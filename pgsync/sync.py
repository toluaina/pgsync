# -*- coding: utf-8 -*-

"""Main module."""
import collections
import itertools
import json
import logging
import os
import pprint
import re
import select
import sys
import time
from datetime import datetime, timedelta

import click
import psycopg2
import sqlalchemy as sa

from . import __version__
from .base import Base, compiled_query
from .constants import (
    DELETE,
    INSERT,
    META,
    PRIMARY_KEY_DELIMITER,
    TG_OP,
    TRUNCATE,
    UPDATE,
)
from .elastichelper import ElasticHelper
from .exc import RDSError, SchemaError, SuperUserError
from .node import get_node, traverse_breadth_first, traverse_post_order, Tree
from .plugin import Plugins
from .querybuilder import QueryBuilder
from .redisqueue import RedisQueue
from .settings import (
    POLL_TIMEOUT,
    REDIS_POLL_INTERVAL,
    REPLICATION_SLOT_CLEANUP_INTERVAL,
)
from .transform import get_private_keys, transform
from .utils import get_config, show_settings, threaded, Timer

logger = logging.getLogger(__name__)


class Sync(Base):
    """Main application class for Sync."""

    def __init__(
        self,
        document,
        verbose=False,
        params=None,
        validate=True,
        repl_slots=True,
    ):
        """Constructor."""
        params = params or {}
        self.index = document["index"]
        self.pipeline = document.get("pipeline")
        self.plugins = document.get("plugins", [])
        self.nodes = document.get("nodes", {})
        self.setting = document.get("setting")
        self.routing = document.get("routing")
        super().__init__(
            document.get("database", self.index), verbose=verbose, **params
        )
        self.es = ElasticHelper()
        self.__name = re.sub(
            "[^0-9a-zA-Z_]+", "", f"{self.database}_{self.index}"
        )
        self._checkpoint = None
        self._plugins = None
        self._truncate = False
        self._checkpoint_file = f".{self.__name}"
        self.redis = RedisQueue(self.__name)
        self.tree = Tree(self)
        self._last_truncate_timestamp = datetime.now()
        if validate:
            self.validate(repl_slots=repl_slots)
            self.create_setting()
        self.query_builder = QueryBuilder(self, verbose=self.verbose)

    def validate(self, repl_slots=True):
        """Perform all validation right away."""

        # ensure v2 compatible schema
        if not isinstance(self.nodes, dict):
            raise SchemaError(
                "Incompatible schema. Please run v2 schema migration"
            )

        self.connect()
        if self.plugins:
            self._plugins = Plugins("plugins", self.plugins)

        max_replication_slots = self.pg_settings("max_replication_slots")
        try:
            if int(max_replication_slots) < 1:
                raise TypeError
        except TypeError:
            raise RuntimeError(
                "Ensure there is at least one replication slot defined "
                "by setting max_replication_slots=1"
            )

        wal_level = self.pg_settings("wal_level")
        if not wal_level or wal_level.lower() != "logical":
            raise RuntimeError(
                "Enable logical decoding by setting wal_level=logical"
            )

        rds_logical_replication = self.pg_settings("rds.logical_replication")

        if rds_logical_replication:
            if rds_logical_replication.lower() == "off":
                raise RDSError("rds.logical_replication is not enabled")
        else:
            if not (
                self.has_permission(
                    self.engine.url.username,
                    "usesuper",
                )
                or self.has_permission(
                    self.engine.url.username,
                    "userepl",
                )
            ):
                raise SuperUserError(
                    f'PG_USER "{self.engine.url.username}" needs to be '
                    f"superuser or have replication role permission to "
                    f"perform this action. "
                    f"Ensure usesuper or userepl is True in pg_user"
                )

        if self.index is None:
            raise ValueError("Index is missing for document")

        # ensure we have bootstrapped and the replication slot exists
        if repl_slots:
            if not self.replication_slots(self.__name):
                raise RuntimeError(
                    f'Replication slot "{self.__name}" does not exist.\n'
                    f'Make sure you have run "bootstrap".'
                )

        root = self.tree.build(self.nodes)
        root.display()
        for node in traverse_breadth_first(root):
            pass

    def create_setting(self):
        """Create Elasticsearch setting and mapping if required."""
        root = self.tree.build(self.nodes)
        self.es._create_setting(
            self.index,
            root,
            setting=self.setting,
            routing=self.routing,
        )

    def setup(self):
        """Create the database triggers and replication slot."""
        self.teardown(drop_view=False)

        for schema in self.schemas:
            tables = set([])
            # tables with user defined foreign keys
            user_defined_fkey_tables = {}

            root = self.tree.build(self.nodes)
            for node in traverse_breadth_first(root):
                if node.schema != schema:
                    continue
                tables |= set(node.relationship.through_tables)
                tables |= set([node.table])
                # we want to get both the parent and the child keys here
                # even though only one of them is the foreign_key.
                # this is because we define both in the schema but
                # do not specify which table is the foreign key.
                columns = []
                if node.relationship.foreign_key.parent:
                    columns.extend(node.relationship.foreign_key.parent)
                if node.relationship.foreign_key.child:
                    columns.extend(node.relationship.foreign_key.child)
                if columns:
                    user_defined_fkey_tables.setdefault(node.table, set([]))
                    user_defined_fkey_tables[node.table] |= set(columns)
            if tables:
                self.create_triggers(schema, tables=tables)
                self.create_view(schema, tables, user_defined_fkey_tables)
        self.create_replication_slot(self.__name)

    def teardown(self, drop_view=True):
        """Drop the database triggers and replication slot."""

        try:
            os.unlink(self._checkpoint_file)
        except OSError:
            pass

        for schema in self.schemas:
            tables = set([])
            root = self.tree.build(self.nodes)
            for node in traverse_breadth_first(root):
                tables |= set(node.relationship.through_tables)
                tables |= set([node.table])
            self.drop_triggers(schema=schema, tables=tables)
            if drop_view:
                self.drop_view(schema=schema)
        self.drop_replication_slot(self.__name)

    def get_doc_id(self, primary_keys):
        """Get the Elasticsearch document id from the primary keys."""
        return f"{PRIMARY_KEY_DELIMITER}".join(map(str, primary_keys))

    def logical_slot_changes(self, txmin=None, txmax=None):
        """
        Process changes from the db logical replication logs.

        Here, we are grouping all rows of the same table and tg_op
        and processing them as a group in bulk.
        This is more efficient.
        e.g [
            {'tg_op': INSERT, 'table': A, ...},
            {'tg_op': INSERT, 'table': A, ...},
            {'tg_op': INSERT, 'table': A, ...},
            {'tg_op': DELETE, 'table': A, ...},
            {'tg_op': DELETE, 'table': A, ...},
            {'tg_op': INSERT, 'table': A, ...},
            {'tg_op': INSERT, 'table': A, ...},
        ]

        We will have 3 groups being synced together in one execution
        First 3 INSERT, Next 2 DELETE and then the next 2 INSERT.
        Perhaps this could be improved but this is the best approach so far.

        TODO: We can also process all INSERTS together and rearrange
        them as done below
        """
        rows = self.logical_slot_peek_changes(
            self.__name,
            txmin=txmin,
            txmax=txmax,
            upto_nchanges=None,
        )

        rows = rows or []
        payloads = []
        _rows = []

        for row in rows:
            if re.search(r"^BEGIN", row.data) or re.search(
                r"^COMMIT", row.data
            ):
                continue
            _rows.append(row)

        for i, row in enumerate(_rows):

            logger.debug(f"txid: {row.xid}")
            logger.debug(f"data: {row.data}")
            # TODO: optimize this so we are not parsing the same row twice
            try:
                payload = self.parse_logical_slot(row.data)
            except Exception as e:
                logger.exception(
                    f"Error parsing row: {e}\nRow data: {row.data}"
                )
                raise
            payloads.append(payload)

            j = i + 1
            if j < len(_rows):
                try:
                    payload2 = self.parse_logical_slot(_rows[j].data)
                except Exception as e:
                    logger.exception(
                        f"Error parsing row: {e}\nRow data: {_rows[j].data}"
                    )
                    raise

                if (
                    payload["tg_op"] != payload2["tg_op"]
                    or payload["table"] != payload2["table"]
                ):
                    self.sync_payloads(payloads)
                    payloads = []
            elif j == len(_rows):
                self.sync_payloads(payloads)
                payloads = []

        if rows:
            self.logical_slot_get_changes(
                self.__name,
                txmin=txmin,
                txmax=txmax,
                upto_nchanges=len(rows),
            )

    def _payload_data(self, payload):
        """Extract the payload data from the payload."""
        payload_data = payload.get("new")
        if payload["tg_op"] == DELETE:
            if payload.get("old"):
                payload_data = payload.get("old")
        return payload_data

    def _insert(self, node, root, filters, payloads):

        if node.table in self.tree.nodes:

            if node.table == root.table:

                for payload in payloads:
                    payload_data = self._payload_data(payload)
                    primary_values = [
                        payload_data[key] for key in node.model.primary_keys
                    ]
                    primary_fields = dict(
                        zip(node.model.primary_keys, primary_values)
                    )
                    filters[node.table].append(
                        {key: value for key, value in primary_fields.items()}
                    )

            else:

                if not node.parent:
                    logger.exception(
                        f"Could not get parent from node: {node.name}"
                    )
                    raise

                # set the parent as the new entity that has changed
                filters[node.parent.table] = []
                foreign_keys = self.query_builder._get_foreign_keys(
                    node.parent,
                    node,
                )

                for payload in payloads:
                    payload_data = self._payload_data(payload)
                    for i, key in enumerate(foreign_keys[node.name]):
                        value = payload_data[key]
                        filters[node.parent.table].append(
                            {foreign_keys[node.parent.name][i]: value}
                        )

        else:

            # handle case where we insert into a through table
            # set the parent as the new entity that has changed
            filters[node.parent.table] = []
            foreign_keys = self.query_builder._get_foreign_keys(
                node.parent,
                node,
            )

            for payload in payloads:
                payload_data = self._payload_data(payload)
                for i, key in enumerate(foreign_keys[node.name]):
                    value = payload_data[key]
                    filters[node.parent.table].append(
                        {foreign_keys[node.parent.name][i]: value}
                    )

        return filters

    def _update(self, node, root, filters, payloads, extra):

        if node.table == root.table:

            # Here, we are performing two operations:
            # 1) Build a filter to sync the updated record(s)
            # 2) Delete the old record(s) in Elasticsearch if the
            #    primary key has changed
            #   2.1) This is crucial otherwise we can have the old
            #        and new document in Elasticsearch at the same time
            docs = []
            for payload in payloads:
                payload_data = self._payload_data(payload)
                primary_values = [
                    payload_data[key] for key in node.model.primary_keys
                ]
                primary_fields = dict(
                    zip(node.model.primary_keys, primary_values)
                )
                filters[node.table].append(
                    {key: value for key, value in primary_fields.items()}
                )

                old_values = []
                for key in root.model.primary_keys:
                    if key in payload.get("old").keys():
                        old_values.append(payload.get("old")[key])

                new_values = [
                    payload.get("new")[key] for key in root.model.primary_keys
                ]

                if (
                    len(old_values) == len(new_values)
                    and old_values != new_values
                ):
                    doc = {
                        "_id": self.get_doc_id(old_values),
                        "_index": self.index,
                        "_op_type": "delete",
                    }
                    if self.routing:
                        doc["_routing"] = old_values[self.routing]
                    if self.es.version[0] < 7:
                        doc["_type"] = "_doc"
                    docs.append(doc)

            if docs:
                self.es.bulk(self.index, docs)

        else:

            # update the child tables
            for payload in payloads:
                _filters = []
                fields = collections.defaultdict(list)

                payload_data = self._payload_data(payload)

                primary_values = [
                    payload_data[key] for key in node.model.primary_keys
                ]
                primary_fields = dict(
                    zip(node.model.primary_keys, primary_values)
                )

                for key, value in primary_fields.items():
                    fields[key].append(value)
                    if None in payload["new"].values():
                        extra["table"] = node.table
                        extra["column"] = key

                if None in payload["old"].values():
                    for key, value in primary_fields.items():
                        fields[key].append(0)

                for doc_id in self.es._search(self.index, node.table, fields):
                    where = {}
                    params = doc_id.split(PRIMARY_KEY_DELIMITER)
                    for i, key in enumerate(root.model.primary_keys):
                        where[key] = params[i]
                    _filters.append(where)

                # also handle foreign_keys
                if node.parent:
                    fields = collections.defaultdict(list)
                    foreign_keys = self.query_builder._get_foreign_keys(
                        node.parent,
                        node,
                    )
                    foreign_values = [
                        payload.get("new", {}).get(k)
                        for k in foreign_keys[node.name]
                    ]

                    for key in [key.name for key in node.primary_keys]:
                        for value in foreign_values:
                            if value:
                                fields[key].append(value)
                    # TODO: we should combine this with the filter above
                    # so we only hit Elasticsearch once
                    for doc_id in self.es._search(
                        self.index,
                        node.parent.table,
                        fields,
                    ):
                        where = {}
                        params = doc_id.split(PRIMARY_KEY_DELIMITER)
                        for i, key in enumerate(root.model.primary_keys):
                            where[key] = params[i]
                        _filters.append(where)

                if _filters:
                    filters[root.table].extend(_filters)

        return filters

    def _delete(self, node, root, filters, payloads):

        # when deleting a root node, just delete the doc in Elasticsearch
        if node.table == root.table:

            docs = []
            for payload in payloads:
                payload_data = self._payload_data(payload)
                root_primary_values = [
                    payload_data[key] for key in root.model.primary_keys
                ]
                doc = {
                    "_id": self.get_doc_id(root_primary_values),
                    "_index": self.index,
                    "_op_type": "delete",
                }
                if self.routing:
                    doc["_routing"] = payload_data[self.routing]
                if self.es.version[0] < 7:
                    doc["_type"] = "_doc"
                docs.append(doc)
            if docs:
                self.es.bulk(self.index, docs)

        else:

            # when deleting the child node, find the doc _id where
            # the child keys match in private, then get the root doc_id and
            # re-sync the child tables
            for payload in payloads:
                payload_data = self._payload_data(payload)
                primary_values = [
                    payload_data[key] for key in node.model.primary_keys
                ]
                primary_fields = dict(
                    zip(node.model.primary_keys, primary_values)
                )
                fields = collections.defaultdict(list)

                _filters = []
                for key, value in primary_fields.items():
                    fields[key].append(value)

                for doc_id in self.es._search(self.index, node.table, fields):
                    where = {}
                    params = doc_id.split(PRIMARY_KEY_DELIMITER)
                    for i, key in enumerate(root.model.primary_keys):
                        where[key] = params[i]

                    _filters.append(where)

                if _filters:
                    filters[root.table].extend(_filters)

        return filters

    def _truncate(self, node, root, filters):

        if node.table == root.table:

            docs = []
            for doc_id in self.es._search(self.index, node.table):
                doc = {
                    "_id": doc_id,
                    "_index": self.index,
                    "_op_type": "delete",
                }
                if self.es.version[0] < 7:
                    doc["_type"] = "_doc"
                docs.append(doc)
            if docs:
                self.es.bulk(self.index, docs)

        else:

            _filters = []
            for doc_id in self.es._search(self.index, node.table):
                where = {}
                params = doc_id.split(PRIMARY_KEY_DELIMITER)
                for i, key in enumerate(root.model.primary_keys):
                    where[key] = params[i]
                _filters.append(where)
            if _filters:
                filters[root.table].extend(_filters)

        return filters

    def _payloads(self, payloads):
        """
        The "payloads" is a list of payload operations to process together.

        The basic assumption is that all payloads in the list have the
        same tg_op and table name.

        e.g:
        [
            {
                'tg_op': 'INSERT',
                'table': 'book',
                'old': {'id': 1}, 'new': {'id': 4}
            },
            {
                'tg_op': 'INSERT',
                'table': 'book',
                'old': {'id': 2}, 'new': {'id': 5}
            },
            {   'tg_op': 'INSERT',
                'table': 'book',
                'old': {'id': 3}, 'new': {'id': 6},
            }
            ...
        ]

        """
        payload = payloads[0]
        tg_op = payload["tg_op"]
        table = payload["table"]
        if tg_op not in TG_OP:
            logger.exception(f"Unknown tg_op {tg_op}")
            raise

        # we might receive an event triggered for a table
        # that is not in the tree node.
        # e.g a through table which we need to react to.
        # in this case, we find the parent of the through
        # table and force a re-sync.
        if (
            table not in self.tree.nodes
            and table not in self.tree.through_nodes
        ):
            return

        node = get_node(self.tree, table, self.nodes)
        root = get_node(self.tree, self.nodes["table"], self.nodes)

        for payload in payloads:
            payload_data = self._payload_data(payload)
            # this is only required for the non truncate tg_ops
            if payload_data:
                if not set(node.model.primary_keys).issubset(
                    set(payload_data.keys())
                ):
                    logger.exception(
                        f"Primary keys {node.model.primary_keys} not subset "
                        f"of payload data {payload_data.keys()} for table "
                        f"{payload['schema']}.{payload['table']}"
                    )
                    raise

        logger.debug(f"tg_op: {tg_op} table: {node.name}")

        filters = {node.table: [], root.table: []}
        extra = {}

        if tg_op == INSERT:

            filters = self._insert(
                node,
                root,
                filters,
                payloads,
            )

        if tg_op == UPDATE:

            filters = self._update(
                node,
                root,
                filters,
                payloads,
                extra,
            )

        if tg_op == DELETE:

            filters = self._delete(
                node,
                root,
                filters,
                payloads,
            )

        if tg_op == TRUNCATE:

            filters = self._truncate(node, root, filters)

        # If there are no filters, then don't execute the sync query
        # otherwise we would end up performing a full query
        # and sync the entire db!
        if not any(filters.values()):
            logger.warning("No filters supplied")
            yield {}
            return

        yield self._sync(filters=filters, extra=extra)

    def _build_filters(self, filters, node):
        """
        Build SQLAlchemy filters.

        NB:
        assumption dictionary is an AND and list is an OR

        filters['book'] = [
            {'id': 1, 'uid': '001'},
            {'id': 2, 'uid': '002'}
        ]
        """
        _filters = []
        if filters.get(node.table):

            for _filter in filters.get(node.table):
                where = []
                for key, value in _filter.items():
                    where.append(getattr(node.model.c, key) == value)
                _filters.append(sa.and_(*where))

            node._filters.append(sa.or_(*_filters))

    def _sync(
        self,
        filters=None,
        txmin=None,
        txmax=None,
        extra=None,
    ):
        if filters is None:
            filters = {}

        root = self.tree.build(self.nodes)

        self.query_builder.isouter = True

        for node in traverse_post_order(root):

            self._build_filters(filters, node)

            if node.is_root:
                if txmin:
                    node._filters.append(
                        sa.cast(
                            sa.cast(
                                node.model.c.xmin,
                                sa.Text,
                            ),
                            sa.BigInteger,
                        )
                        >= txmin
                    )
                if txmax:
                    node._filters.append(
                        sa.cast(
                            sa.cast(
                                node.model.c.xmin,
                                sa.Text,
                            ),
                            sa.BigInteger,
                        )
                        < txmax
                    )

            try:
                self.query_builder.build_queries(node)
            except Exception as e:
                logger.exception(f"Exception {e}")
                raise

        if self.verbose:
            compiled_query(node._subquery, "Query")

        with click.progressbar(
            length=self.fetchcount(node._subquery),
            show_pos=True,
            show_percent=True,
            show_eta=True,
            fill_char="=",
            empty_char="-",
            width=50,
        ) as bar:

            for i, (keys, row, primary_keys) in enumerate(
                self.fetchmany(node._subquery)
            ):

                bar.update(1)

                row = transform(row, self.nodes)
                row[META] = get_private_keys(keys)
                if extra:
                    if extra["table"] not in row[META]:
                        row[META][extra["table"]] = {}
                    if extra["column"] not in row[META][extra["table"]]:
                        row[META][extra["table"]][extra["column"]] = []
                    row[META][extra["table"]][extra["column"]].append(0)

                if self.verbose:
                    print(f"{(i+1)})")
                    print(f"Pkeys: {primary_keys}")
                    pprint.pprint(row)
                    print("-" * 10)

                doc = {
                    "_id": self.get_doc_id(primary_keys),
                    "_index": self.index,
                    "_source": row,
                }

                if self.routing:
                    doc["_routing"] = row[self.routing]

                if self.es.version[0] < 7:
                    doc["_type"] = "_doc"

                if self._plugins:
                    doc = next(self._plugins.transform([doc]))

                if self.pipeline:
                    doc["pipeline"] = self.pipeline

                yield doc

    def sync(self, txmin=None, txmax=None):
        """
        Pull sync all data from database.

        main entry point.
        sync all tables as docs to Elasticsearch
        document contains -> node:
        """
        docs = self._sync(txmin=txmin, txmax=txmax)
        try:
            self.es.bulk(self.index, docs)
        except Exception as e:
            logger.exception(f"Exception {e}")
            raise
        self.checkpoint = txmax or self.txid_current

    def sync_payloads(self, payloads):
        """Sync payload when an event is emitted."""
        docs = []
        for doc in self._payloads(payloads):
            docs.append(doc)
        try:
            self.es.bulk(self.index, itertools.chain(*docs))
        except Exception as e:
            logger.exception(f"Exception: {e}")
            raise

    @property
    def checkpoint(self):
        """Save the current txid as the checkpoint."""
        if os.path.exists(self._checkpoint_file):
            with open(self._checkpoint_file, "r") as fp:
                self._checkpoint = int(fp.read().split()[0])
        return self._checkpoint

    @checkpoint.setter
    def checkpoint(self, value=None):
        if value is None:
            raise ValueError("Cannot assign a None value to checkpoint")
        with open(self._checkpoint_file, "w+") as fp:
            fp.write(f"{value}\n")
        self._checkpoint = value

    @threaded
    def poll_redis(self):
        """Consumer which polls Redis continuously."""
        while True:
            payloads = self.redis.bulk_pop()
            if payloads:
                logger.debug(f"poll_redis: {payloads}")
                self.on_publish(payloads)
            time.sleep(REDIS_POLL_INTERVAL)

    @threaded
    def poll_db(self):
        """
        Producer which polls Postgres continuously.

        Receive a notification message from the channel we are listening on
        """
        conn = self.engine.connect().connection
        conn.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
        )
        cursor = conn.cursor()
        channel = self.database
        cursor.execute(f"LISTEN {channel}")
        logger.debug(f'Listening for notifications on channel "{channel}"')

        i = 0
        j = 0

        while True:
            # NB: consider reducing POLL_TIMEOUT to increase throughout
            if select.select([conn], [], [], POLL_TIMEOUT) == ([], [], []):
                if i % 10 == 0:
                    sys.stdout.write(f"Polling db {channel}: {j:,} item(s)\n")
                    sys.stdout.flush()
                i += 1
                continue

            try:
                conn.poll()
            except psycopg2.OperationalError as e:
                logger.fatal(f"OperationalError: {e}")
                os._exit(-1)

            while conn.notifies:
                notification = conn.notifies.pop(0)
                payload = json.loads(notification.payload)
                self.redis.push(payload)
                logger.debug(f"on_notify: {payload}")
                j += 1
            i = 0

    def on_publish(self, payloads):
        """
        Redis publish event handler.

        This is triggered by poll_redis.
        It is called when an event is received from Redis.
        Deserialize the payload from Redis and sync to Elasticsearch.
        """
        logger.debug(f"on_publish len {len(payloads)}")

        # Safe inserts are insert operations that can be performed in any order
        # Optimize the safe INSERTS
        # TODO repeat this for the other place too
        # if all payload operations are INSERTS
        if set(map(lambda x: x["tg_op"], payloads)) == set([INSERT]):

            _payloads = collections.defaultdict(list)

            for payload in payloads:
                _payloads[payload["table"]].append(payload)

            for _payload in _payloads.values():
                self.sync_payloads(_payload)

        else:

            _payloads = []
            for i, payload in enumerate(payloads):
                _payloads.append(payload)
                j = i + 1
                if j < len(payloads):
                    payload2 = payloads[j]
                    if (
                        payload["tg_op"] != payload2["tg_op"]
                        or payload["table"] != payload2["table"]
                    ):
                        self.sync_payloads(_payloads)
                        _payloads = []
                elif j == len(payloads):
                    self.sync_payloads(_payloads)
                    _payloads = []

        txids = set(map(lambda x: x["xmin"], payloads))
        # for truncate, tg_op txids is None so skip setting the checkpoint
        if txids != set([None]):
            txmin = min(min(txids), self.txid_current) - 1
            self.checkpoint = txmin

    def pull(self):
        """Pull data from db."""
        txmin = self.checkpoint
        txmax = self.txid_current
        logger.debug(f"pull txmin: {txmin} txmax: {txmax}")
        # forward pass sync
        self.sync(txmin=txmin, txmax=txmax)
        # now sync up to txmax to capture everything we may have missed
        self.logical_slot_changes(txmin=txmin, txmax=txmax)
        self._truncate = True

    @threaded
    def truncate_slots(self):
        """Truncate the logical replication slot."""
        while True:
            if self._truncate and (
                datetime.now()
                >= self._last_truncate_timestamp
                + timedelta(seconds=REPLICATION_SLOT_CLEANUP_INTERVAL)
            ):
                logger.debug(f"Truncating replication slot: {self.__name}")
                self.logical_slot_get_changes(self.__name, upto_nchanges=None)
                self._last_truncate_timestamp = datetime.now()
            time.sleep(0.1)

    def receive(self):
        """
        Receive events from db.

        NB: pulls as well as receives in order to avoid missing data.

        1. Buffer all ongoing changes from db to Redis.
        2. Pull everything so far and also replay replication logs.
        3. Consume all changes from Redis.
        """
        # start a background worker producer thread to poll the db and populate
        # the Redis cache
        self.poll_db()

        # sync up to current transaction_id
        self.pull()

        # start a background worker consumer thread to
        # poll Redis and populate Elasticsearch
        self.poll_redis()

        # start a background worker thread to cleanup the replication slot
        self.truncate_slots()


@click.command()
@click.option(
    "--config",
    "-c",
    help="Schema config",
    type=click.Path(exists=True),
)
@click.option("--daemon", "-d", is_flag=True, help="Run as a daemon")
@click.option("--host", "-h", help="PG_HOST override")
@click.option("--password", is_flag=True, help="Prompt for database password")
@click.option("--port", "-p", help="PG_PORT override", type=int)
@click.option(
    "--sslmode",
    help="PG_SSLMODE override",
    type=click.Choice(
        [
            "allow",
            "disable",
            "prefer",
            "require",
            "verify-ca",
            "verify-full",
        ],
        case_sensitive=False,
    ),
)
@click.option(
    "--sslrootcert",
    help="PG_SSLROOTCERT override",
    type=click.Path(exists=True),
)
@click.option("--user", "-u", help="PG_USER override")
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    default=False,
    help="Turn on verbosity",
)
@click.option(
    "--version",
    is_flag=True,
    default=False,
    help="Show version info",
)
def main(
    config,
    daemon,
    host,
    password,
    port,
    sslmode,
    sslrootcert,
    user,
    verbose,
    version,
):
    """
    main application syncer
    """
    if version:
        sys.stdout.write(f"Version: {__version__}\n")
        return

    params = {
        "user": user,
        "host": host,
        "port": port,
        "sslmode": sslmode,
        "sslrootcert": sslrootcert,
    }
    if password:
        params["password"] = click.prompt(
            "Password",
            type=str,
            hide_input=True,
        )
    params = {key: value for key, value in params.items() if value is not None}

    config = get_config(config)

    show_settings(config, params)

    with Timer():
        for document in json.load(open(config)):
            sync = Sync(
                document,
                verbose=verbose,
                params=params,
            )
            sync.pull()
            if daemon:
                sync.receive()


if __name__ == "__main__":
    main()
