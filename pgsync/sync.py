"""Sync module."""

import asyncio
import json
import logging
import os
import pprint
import re
import select
import sys
import time
import typing as t
from collections import defaultdict

import click
import sqlalchemy as sa
import sqlparse
from psycopg2 import OperationalError
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from . import __version__, settings
from .base import Base, Payload
from .constants import (
    DELETE,
    INSERT,
    MATERIALIZED_VIEW,
    MATERIALIZED_VIEW_COLUMNS,
    META,
    PRIMARY_KEY_DELIMITER,
    TG_OP,
    TRUNCATE,
    UPDATE,
)
from .exc import (
    ForeignKeyError,
    InvalidSchemaError,
    InvalidTGOPError,
    PrimaryKeyNotFoundError,
    RDSError,
    SchemaError,
)
from .node import Node, Tree
from .plugin import Plugins
from .querybuilder import QueryBuilder
from .redisqueue import RedisQueue
from .search_client import SearchClient
from .singleton import Singleton
from .transform import Transform
from .utils import (
    chunks,
    compiled_query,
    config_loader,
    exception,
    get_config,
    MutuallyExclusiveOption,
    show_settings,
    threaded,
    Timer,
)

logger = logging.getLogger(__name__)


class Sync(Base, metaclass=Singleton):
    """Main application class for Sync."""

    def __init__(
        self,
        doc: dict,
        verbose: bool = False,
        validate: bool = True,
        repl_slots: bool = True,
        num_workers: int = 1,
        producer: bool = True,
        consumer: bool = True,
        **kwargs,
    ) -> None:
        """Constructor."""
        self.index: str = doc.get("index") or doc["database"]
        self.pipeline: str = doc.get("pipeline")
        self.plugins: list = doc.get("plugins", [])
        self.nodes: dict = doc.get("nodes", {})
        self.setting: dict = doc.get("setting")
        self.mapping: dict = doc.get("mapping")
        self.mappings: dict = doc.get("mappings")
        self.routing: str = doc.get("routing")
        super().__init__(
            doc.get("database", self.index), verbose=verbose, **kwargs
        )
        self.search_client: SearchClient = SearchClient()
        self.__name: str = re.sub(
            "[^0-9a-zA-Z_]+", "", f"{self.database.lower()}_{self.index}"
        )
        self._checkpoint: int = None
        self._plugins: Plugins = None
        self._truncate: bool = False
        self.producer = producer
        self.consumer = consumer
        self.num_workers: int = num_workers
        self._checkpoint_file: str = os.path.join(
            settings.CHECKPOINT_PATH, f".{self.__name}"
        )
        self.redis: RedisQueue = RedisQueue(self.__name)
        self.tree: Tree = Tree(self.models, nodes=self.nodes)
        if validate:
            self.validate(repl_slots=repl_slots)
            self.create_setting()
        if self.plugins:
            self._plugins: Plugins = Plugins("plugins", self.plugins)
        self.query_builder: QueryBuilder = QueryBuilder(verbose=verbose)
        self.count: dict = dict(xlog=0, db=0, redis=0)
        self.tasks: t.List[asyncio.Task] = []

    def validate(self, repl_slots: bool = True) -> None:
        """Perform all validation right away."""

        # ensure v2 compatible schema
        if not isinstance(self.nodes, dict):
            raise SchemaError(
                "Incompatible schema. Please run v2 schema migration"
            )

        self.connect()

        max_replication_slots: t.Optional[str] = self.pg_settings(
            "max_replication_slots"
        )
        try:
            if int(max_replication_slots) < 1:
                raise TypeError
        except TypeError:
            raise RuntimeError(
                "Ensure there is at least one replication slot defined "
                "by setting max_replication_slots = 1"
            )

        wal_level: t.Optional[str] = self.pg_settings("wal_level")
        if not wal_level or wal_level.lower() != "logical":
            raise RuntimeError(
                "Enable logical decoding by setting wal_level = logical"
            )

        self._can_create_replication_slot("_tmp_")

        rds_logical_replication: t.Optional[str] = self.pg_settings(
            "rds.logical_replication"
        )
        if (
            rds_logical_replication
            and rds_logical_replication.lower() == "off"
        ):
            raise RDSError("rds.logical_replication is not enabled")

        if self.index is None:
            raise ValueError("Index is missing for doc")

        # ensure we have run bootstrap and the replication slot exists
        if repl_slots and not self.replication_slots(self.__name):
            raise RuntimeError(
                f'Replication slot "{self.__name}" does not exist.\n'
                f'Make sure you have run the "bootstrap" command.'
            )

        # ensure the checkpoint dirpath is valid
        if not os.path.exists(settings.CHECKPOINT_PATH):
            raise RuntimeError(
                f"Ensure the checkpoint directory exists "
                f'"{settings.CHECKPOINT_PATH}" and is readable.'
            )

        if not os.access(settings.CHECKPOINT_PATH, os.W_OK | os.R_OK):
            raise RuntimeError(
                f'Ensure the checkpoint directory "{settings.CHECKPOINT_PATH}"'
                f" is read/writable"
            )

        self.tree.display()

        for node in self.tree.traverse_breadth_first():
            # ensure internal materialized view compatibility
            if MATERIALIZED_VIEW in self._materialized_views(node.schema):
                if MATERIALIZED_VIEW_COLUMNS != self.columns(
                    node.schema, MATERIALIZED_VIEW
                ):
                    raise RuntimeError(
                        f"Required materialized view columns not present on "
                        f"{MATERIALIZED_VIEW}. Please re-run bootstrap."
                    )

            if node.schema not in self.schemas:
                raise InvalidSchemaError(
                    f"Unknown schema name(s): {node.schema}"
                )

            # ensure all base tables have at least one primary_key
            for table in node.base_tables:
                model: sa.sql.Alias = self.models(table, node.schema)
                if not model.primary_keys:
                    raise PrimaryKeyNotFoundError(
                        f"No primary key(s) for base table: {table}"
                    )

    def analyze(self) -> None:
        for node in self.tree.traverse_breadth_first():
            if node.is_root:
                continue

            primary_keys: list = [
                str(primary_key.name) for primary_key in node.primary_keys
            ]

            foreign_keys: dict
            if node.relationship.throughs:
                through: Node = node.relationship.throughs[0]
                foreign_keys = self.query_builder.get_foreign_keys(
                    node,
                    through,
                )
            else:
                foreign_keys = self.query_builder.get_foreign_keys(
                    node.parent,
                    node,
                )

            columns: list
            for index in self.indices(node.table, node.schema):
                columns = foreign_keys.get(node.name, [])
                if set(columns).issubset(index.get("column_names", [])) or set(
                    columns
                ).issubset(primary_keys):
                    sys.stdout.write(
                        f'Found index "{index.get("name")}" for table '
                        f'"{node.table}" for columns: {columns}: OK \n'
                    )
                    break
            else:
                columns = foreign_keys.get(node.name, [])
                sys.stdout.write(
                    f'Missing index on table "{node.table}" for columns: '
                    f"{columns}\n"
                )
                query: str = sqlparse.format(
                    f'CREATE INDEX idx_{node.table}_{"_".join(columns)} ON '
                    f'{node.table} ({", ".join(columns)})',
                    reindent=True,
                    keyword_case="upper",
                )
                sys.stdout.write(f'Create one with: "\033[4m{query}\033[0m"\n')
                sys.stdout.write("-" * 80)
                sys.stdout.write("\n")
                sys.stdout.flush()

    def create_setting(self) -> None:
        """Create Elasticsearch/OpenSearch setting and mapping if required."""
        self.search_client._create_setting(
            self.index,
            self.tree,
            setting=self.setting,
            mapping=self.mapping,
            mappings=self.mappings,
            routing=self.routing,
        )

    def setup(self) -> None:
        """Create the database triggers and replication slot."""

        join_queries: bool = settings.JOIN_QUERIES
        self.teardown(drop_view=False)

        for schema in self.schemas:
            self.create_function(schema)
            tables: t.Set = set()
            # tables with user defined foreign keys
            user_defined_fkey_tables: dict = {}

            for node in self.tree.traverse_breadth_first():
                if node.schema != schema:
                    continue
                tables |= set(
                    [through.table for through in node.relationship.throughs]
                )
                tables |= set([node.table])
                # we also need to bootstrap the base tables
                tables |= set(node.base_tables)

                # we want to get both the parent and the child keys here
                # even though only one of them is the foreign_key.
                # this is because we define both in the schema but
                # do not specify which table is the foreign key.
                columns: list = []
                if node.relationship.foreign_key.parent:
                    columns.extend(node.relationship.foreign_key.parent)
                if node.relationship.foreign_key.child:
                    columns.extend(node.relationship.foreign_key.child)
                if columns:
                    user_defined_fkey_tables.setdefault(node.table, set())
                    user_defined_fkey_tables[node.table] |= set(columns)
            if tables:
                self.create_view(
                    self.index, schema, tables, user_defined_fkey_tables
                )
                self.create_triggers(
                    schema, tables=tables, join_queries=join_queries
                )
        self.create_replication_slot(self.__name)

    def teardown(self, drop_view: bool = True) -> None:
        """Drop the database triggers and replication slot."""

        join_queries: bool = settings.JOIN_QUERIES

        try:
            os.unlink(self._checkpoint_file)
        except (OSError, FileNotFoundError):
            logger.warning(
                f"Checkpoint file not found: {self._checkpoint_file}"
            )

        self.redis.delete()

        for schema in self.schemas:
            tables: t.Set = set()
            for node in self.tree.traverse_breadth_first():
                tables |= set(
                    [through.table for through in node.relationship.throughs]
                )
                tables |= set([node.table])
                # we also need to teardown the base tables
                tables |= set(node.base_tables)
            self.drop_triggers(
                schema=schema, tables=tables, join_queries=join_queries
            )
            if drop_view:
                self.drop_view(schema)
                self.drop_function(schema)

        self.drop_replication_slot(self.__name)

    def get_doc_id(self, primary_keys: t.List[str], table: str) -> str:
        """
        Get the Elasticsearch/OpenSearch doc id from the primary keys.
        """  # noqa D200
        if not primary_keys:
            raise PrimaryKeyNotFoundError(
                f"No primary key found on table: {table}"
            )
        return f"{PRIMARY_KEY_DELIMITER}".join(map(str, primary_keys))

    def logical_slot_changes(
        self,
        txmin: t.Optional[int] = None,
        txmax: t.Optional[int] = None,
        upto_nchanges: t.Optional[int] = None,
        upto_lsn: t.Optional[str] = None,
    ) -> None:
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
        # minimize the tmp file disk usage when calling
        # PG_LOGICAL_SLOT_PEEK_CHANGES and PG_LOGICAL_SLOT_GET_CHANGES
        # by limiting to a smaller batch size.
        while True:
            changes: int = self.logical_slot_peek_changes(
                self.__name,
                txmin=txmin,
                txmax=txmax,
                upto_nchanges=upto_nchanges,
                upto_lsn=upto_lsn,
            )
            if not changes:
                break

            rows: list = []
            for row in changes:
                if re.search(r"^BEGIN", row.data) or re.search(
                    r"^COMMIT", row.data
                ):
                    continue
                rows.append(row)

            payloads: t.List[Payload] = []
            for i, row in enumerate(rows):
                logger.debug(f"txid: {row.xid}")
                logger.debug(f"data: {row.data}")
                # TODO: optimize this so we are not parsing the same row twice
                try:
                    payload: Payload = self.parse_logical_slot(row.data)
                except Exception as e:
                    logger.exception(
                        f"Error parsing row: {e}\nRow data: {row.data}"
                    )
                    raise

                # filter out unknown schemas
                if payload.schema not in self.tree.schemas:
                    continue

                payloads.append(payload)

                j: int = i + 1
                if j < len(rows):
                    try:
                        payload2: Payload = self.parse_logical_slot(
                            rows[j].data
                        )
                    except Exception as e:
                        logger.exception(
                            f"Error parsing row: {e}\nRow data: {rows[j].data}"
                        )
                        raise

                    if (
                        payload.tg_op != payload2.tg_op
                        or payload.table != payload2.table
                    ):
                        self.search_client.bulk(
                            self.index, self._payloads(payloads)
                        )
                        payloads: list = []
                elif j == len(rows):
                    self.search_client.bulk(
                        self.index, self._payloads(payloads)
                    )
                    payloads: list = []
            self.logical_slot_get_changes(
                self.__name,
                txmin=txmin,
                txmax=txmax,
                upto_nchanges=upto_nchanges,
                upto_lsn=upto_lsn,
            )
            self.count["xlog"] += len(rows)

    def _root_primary_key_resolver(
        self, node: Node, payload: Payload, filters: list
    ) -> list:
        fields: dict = defaultdict(list)
        primary_values: list = [
            payload.data[key] for key in node.model.primary_keys
        ]
        primary_fields: dict = dict(
            zip(node.model.primary_keys, primary_values)
        )
        for key, value in primary_fields.items():
            fields[key].append(value)
        for doc_id in self.search_client._search(
            self.index, node.table, fields
        ):
            where: dict = {}
            params: dict = doc_id.split(PRIMARY_KEY_DELIMITER)
            for i, key in enumerate(self.tree.root.model.primary_keys):
                where[key] = params[i]
            filters.append(where)

        return filters

    def _root_foreign_key_resolver(
        self, node: Node, payload: Payload, foreign_keys: dict, filters: list
    ) -> list:
        """
        Foreign key resolver logic:

        This resolver handles n-tiers relationships (n > 3) where we
        insert/update a new leaf node.
        For the node's parent, get the primary keys values from the
        incoming payload.
        Lookup this value in the meta section of Elasticsearch/OpenSearch
        Then get the root node returned and re-sync that root record.
        Essentially, we want to lookup the root node affected by
        our insert/update operation and sync the tree branch for that root.
        """
        fields: dict = defaultdict(list)
        foreign_values: list = [
            payload.new.get(key) for key in foreign_keys[node.name]
        ]
        for key in [key.name for key in node.primary_keys]:
            for value in foreign_values:
                if value:
                    fields[key].append(value)
        # TODO: we should combine this with the filter above
        # so we only hit Elasticsearch/OpenSearch once
        for doc_id in self.search_client._search(
            self.index,
            node.parent.table,
            fields,
        ):
            where: dict = {}
            params: dict = doc_id.split(PRIMARY_KEY_DELIMITER)
            for i, key in enumerate(self.tree.root.model.primary_keys):
                where[key] = params[i]
            filters.append(where)

        return filters

    def _through_node_resolver(
        self, node: Node, payload: Payload, filters: list
    ) -> list:
        """Handle where node is a through table with a direct references to
        root
        """
        foreign_key_constraint = payload.foreign_key_constraint(node.model)
        if self.tree.root.name in foreign_key_constraint:
            for constraint in foreign_key_constraint[self.tree.root.name]:
                filters.append(
                    {
                        constraint["remote"]: constraint["value"]
                    }
                )
        return filters

    def _insert_op(
        self, node: Node, filters: dict, payloads: t.List[Payload]
    ) -> dict:
        if node.table in self.tree.tables:
            if node.is_root:
                for payload in payloads:
                    primary_values = [
                        payload.data[key]
                        for key in self.tree.root.model.primary_keys
                    ]
                    primary_fields = dict(
                        zip(self.tree.root.model.primary_keys, primary_values)
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

                try:
                    foreign_keys = self.query_builder.get_foreign_keys(
                        node.parent,
                        node,
                    )
                except ForeignKeyError:
                    foreign_keys = self.query_builder._get_foreign_keys(
                        node.parent,
                        node,
                    )

                _filters: list = []
                for payload in payloads:
                    for node_key in foreign_keys[node.name]:
                        for parent_key in foreign_keys[node.parent.name]:
                            if node_key == parent_key:
                                filters[node.parent.table].append(
                                    {parent_key: payload.data[node_key]}
                                )

                    _filters = self._root_foreign_key_resolver(
                        node, payload, foreign_keys, _filters
                    )

                    # through table with a direct references to root
                    if not _filters:
                        _filters = self._through_node_resolver(
                            node, payload, _filters
                        )

                if _filters:
                    filters[self.tree.root.table].extend(_filters)

        else:
            # handle case where we insert into a through table
            # set the parent as the new entity that has changed
            foreign_keys = self.query_builder.get_foreign_keys(
                node.parent,
                node,
            )

            for payload in payloads:
                for i, key in enumerate(foreign_keys[node.name]):
                    filters[node.parent.table].append(
                        {foreign_keys[node.parent.name][i]: payload.data[key]}
                    )

        return filters

    def _update_op(
        self,
        node: Node,
        filters: dict,
        payloads: t.List[dict],
    ) -> dict:
        if node.is_root:
            # Here, we are performing two operations:
            # 1) Build a filter to sync the updated record(s)
            # 2) Delete the old record(s) in Elasticsearch/OpenSearch if the
            #    primary key has changed
            #   2.1) This is crucial otherwise we can have the old and new
            #        doc in Elasticsearch/OpenSearch at the same time
            docs: list = []
            for payload in payloads:
                primary_values: list = [
                    payload.data[key] for key in node.model.primary_keys
                ]
                primary_fields: dict = dict(
                    zip(node.model.primary_keys, primary_values)
                )
                filters[node.table].append(
                    {key: value for key, value in primary_fields.items()}
                )

                old_values: list = []
                for key in self.tree.root.model.primary_keys:
                    if key in payload.old.keys():
                        old_values.append(payload.old[key])

                new_values = [
                    payload.new[key]
                    for key in self.tree.root.model.primary_keys
                ]

                if (
                    len(old_values) == len(new_values)
                    and old_values != new_values
                ):
                    doc: dict = {
                        "_id": self.get_doc_id(
                            old_values, self.tree.root.table
                        ),
                        "_index": self.index,
                        "_op_type": "delete",
                    }
                    if self.routing:
                        doc["_routing"] = old_values[self.routing]
                    if (
                        self.search_client.major_version < 7
                        and not self.search_client.is_opensearch
                    ):
                        doc["_type"] = "_doc"
                    docs.append(doc)

            if docs:
                self.search_client.bulk(self.index, docs)

        else:
            # update the child tables
            for payload in payloads:
                _filters: list = []
                _filters = self._root_primary_key_resolver(
                    node, payload, _filters
                )
                # also handle foreign_keys
                if node.parent:
                    try:
                        foreign_keys = self.query_builder.get_foreign_keys(
                            node.parent,
                            node,
                        )
                    except ForeignKeyError:
                        foreign_keys = self.query_builder._get_foreign_keys(
                            node.parent,
                            node,
                        )

                    _filters = self._root_foreign_key_resolver(
                        node, payload, foreign_keys, _filters
                    )

                if _filters:
                    filters[self.tree.root.table].extend(_filters)

        return filters

    def _delete_op(
        self, node: Node, filters: dict, payloads: t.List[dict]
    ) -> dict:
        # when deleting a root node, just delete the doc in
        # Elasticsearch/OpenSearch
        if node.is_root:
            docs: list = []
            for payload in payloads:
                primary_values: list = [
                    payload.data[key]
                    for key in self.tree.root.model.primary_keys
                ]
                doc: dict = {
                    "_id": self.get_doc_id(
                        primary_values, self.tree.root.table
                    ),
                    "_index": self.index,
                    "_op_type": "delete",
                }
                if self.routing:
                    doc["_routing"] = payload.data[self.routing]
                if (
                    self.search_client.major_version < 7
                    and not self.search_client.is_opensearch
                ):
                    doc["_type"] = "_doc"
                docs.append(doc)
            if docs:
                raise_on_exception: t.Optional[bool] = (
                    False if settings.USE_ASYNC else None
                )
                raise_on_error: t.Optional[bool] = (
                    False if settings.USE_ASYNC else None
                )
                self.search_client.bulk(
                    self.index,
                    docs,
                    raise_on_exception=raise_on_exception,
                    raise_on_error=raise_on_error,
                )

        else:
            # when deleting the child node, find the doc _id where
            # the child keys match in private, then get the root doc_id and
            # re-sync the child tables
            for payload in payloads:
                _filters: list = []
                _filters = self._root_primary_key_resolver(
                    node, payload, _filters
                )
                if _filters:
                    filters[self.tree.root.table].extend(_filters)

        return filters

    def _truncate_op(self, node: Node, filters: dict) -> dict:
        if node.is_root:
            docs: list = []
            for doc_id in self.search_client._search(self.index, node.table):
                doc: dict = {
                    "_id": doc_id,
                    "_index": self.index,
                    "_op_type": "delete",
                }
                if (
                    self.search_client.major_version < 7
                    and not self.search_client.is_opensearch
                ):
                    doc["_type"] = "_doc"
                docs.append(doc)
            if docs:
                self.search_client.bulk(self.index, docs)

        else:
            _filters: list = []
            for doc_id in self.search_client._search(self.index, node.table):
                where: dict = {}
                params = doc_id.split(PRIMARY_KEY_DELIMITER)
                for i, key in enumerate(self.tree.root.model.primary_keys):
                    where[key] = params[i]
                _filters.append(where)
            if _filters:
                filters[self.tree.root.table].extend(_filters)

        return filters

    def _payloads(self, payloads: t.List[Payload]) -> None:
        """
        The "payloads" is a list of payload operations to process together.

        The basic assumption is that all payloads in the list have the
        same tg_op and table name.

        e.g:
        [
            Payload(
                tg_op='INSERT',
                table='book',
                old={'id': 1},
                new={'id': 4},
            ),
            Payload(
                tg_op='INSERT',
                table='book',
                old={'id': 2},
                new={'id': 5},
            ),
            Payload(
                tg_op='INSERT',
                table='book',
                old={'id': 3},
                new={'id': 6},
            ),
            ...
        ]

        """
        payload: Payload = payloads[0]
        if payload.tg_op not in TG_OP:
            logger.exception(f"Unknown tg_op {payload.tg_op}")
            raise InvalidTGOPError(f"Unknown tg_op {payload.tg_op}")

        # we might receive an event triggered for a table
        # that is not in the tree node.
        # e.g a through table which we need to react to.
        # in this case, we find the parent of the through
        # table and force a re-sync.
        if (
            payload.table not in self.tree.tables
            or payload.schema not in self.tree.schemas
        ):
            return

        node: Node = self.tree.get_node(payload.table, payload.schema)

        for payload in payloads:
            # this is only required for the non truncate tg_ops
            if payload.data:
                if not set(node.model.primary_keys).issubset(
                    set(payload.data.keys())
                ):
                    logger.exception(
                        f"Primary keys {node.model.primary_keys} not subset "
                        f"of payload data {payload.data.keys()} for table "
                        f"{payload.schema}.{payload.table}"
                    )
                    raise

        logger.debug(f"tg_op: {payload.tg_op} table: {node.name}")

        filters: dict = {
            node.table: [],
            self.tree.root.table: [],
        }
        if not node.is_root:
            filters[node.parent.table] = []

        if payload.tg_op == INSERT:
            filters = self._insert_op(
                node,
                filters,
                payloads,
            )

        if payload.tg_op == UPDATE:
            filters = self._update_op(
                node,
                filters,
                payloads,
            )

        if payload.tg_op == DELETE:
            filters = self._delete_op(
                node,
                filters,
                payloads,
            )

        if payload.tg_op == TRUNCATE:
            filters = self._truncate_op(node, filters)

        # If there are no filters, then don't execute the sync query
        # otherwise we would end up performing a full query
        # and sync the entire db!
        if any(filters.values()):
            """
            Filters are applied when an insert, update or delete operation
            occurs. For a large table update, this normally results
            in a large SQL query with multiple OR clauses.

            Filters is a dict of tables where each key is a list of id's
            {
                'city': [
                    {'id': '1'},
                    {'id': '4'},
                    {'id': '5'},
                ],
                'book': [
                    {'id': '1'},
                    {'id': '2'},
                    {'id': '7'},
                    ...
                ]
            }
            """
            for l1 in chunks(
                filters.get(self.tree.root.table), settings.FILTER_CHUNK_SIZE
            ):
                if filters.get(node.table):
                    for l2 in chunks(
                        filters.get(node.table), settings.FILTER_CHUNK_SIZE
                    ):
                        if not node.is_root and filters.get(node.parent.table):
                            for l3 in chunks(
                                filters.get(node.parent.table),
                                settings.FILTER_CHUNK_SIZE,
                            ):
                                yield from self.sync(
                                    filters={
                                        self.tree.root.table: l1,
                                        node.table: l2,
                                        node.parent.table: l3,
                                    },
                                )
                        else:
                            yield from self.sync(
                                filters={
                                    self.tree.root.table: l1,
                                    node.table: l2,
                                },
                            )
                else:
                    yield from self.sync(
                        filters={self.tree.root.table: l1},
                    )

    def sync(
        self,
        filters: t.Optional[dict] = None,
        txmin: t.Optional[int] = None,
        txmax: t.Optional[int] = None,
        ctid: t.Optional[dict] = None,
    ) -> t.Generator:
        """
        Synchronizes data from PostgreSQL to Elasticsearch.

        Args:
            filters (Optional[dict]): A dictionary of filters to apply to the data.
            txmin (Optional[int]): The minimum transaction ID to include in the synchronization.
            txmax (Optional[int]): The maximum transaction ID to include in the synchronization.
            ctid (Optional[dict]): A dictionary of ctid values to include in the synchronization.

        Yields:
            dict: A dictionary representing a doc to be indexed in Elasticsearch.
        """
        self.query_builder.isouter = True
        self.query_builder.from_obj = None

        for node in self.tree.traverse_post_order():
            node._subquery = None
            node._filters = []
            node.setup()

            try:
                self.query_builder.build_queries(
                    node, filters=filters, txmin=txmin, txmax=txmax, ctid=ctid
                )
            except Exception as e:
                logger.exception(f"Exception {e}")
                raise

        if self.verbose:
            compiled_query(node._subquery, "Query")

        count: int = self.fetchcount(node._subquery)

        with click.progressbar(
            length=count,
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

                row: dict = Transform.transform(row, self.nodes)

                row[META] = Transform.get_primary_keys(keys)

                if node.is_root:
                    primary_key_values: t.List[str] = list(
                        map(str, primary_keys)
                    )
                    primary_key_names: t.List[str] = [
                        primary_key.name for primary_key in node.primary_keys
                    ]
                    # TODO: add support for composite pkeys
                    row[META][node.table] = {
                        primary_key_names[0]: [primary_key_values[0]],
                    }

                if self.verbose:
                    print(f"{(i+1)})")
                    print(f"pkeys: {primary_keys}")
                    pprint.pprint(row)
                    print("-" * 10)

                doc: dict = {
                    "_id": self.get_doc_id(primary_keys, node.table),
                    "_index": self.index,
                    "_source": row,
                }

                if self.routing:
                    doc["_routing"] = row[self.routing]

                if (
                    self.search_client.major_version < 7
                    and not self.search_client.is_opensearch
                ):
                    doc["_type"] = "_doc"

                if self._plugins:
                    doc = next(self._plugins.transform([doc]))
                    if not doc:
                        continue

                if self.pipeline:
                    doc["pipeline"] = self.pipeline

                yield doc

    @property
    def checkpoint(self) -> int:
        """
        Gets the current checkpoint value.

        :return: The current checkpoint value.
        :rtype: int
        """
        if os.path.exists(self._checkpoint_file):
            with open(self._checkpoint_file, "r") as fp:
                self._checkpoint: int = int(fp.read().split()[0])
        return self._checkpoint

    @checkpoint.setter
    def checkpoint(self, value: t.Optional[str] = None) -> None:
        """
        Sets the checkpoint value.

        :param value: The new checkpoint value.
        :type value: Optional[str]
        :raises ValueError: If the value is None.
        """
        if value is None:
            raise ValueError("Cannot assign a None value to checkpoint")
        with open(self._checkpoint_file, "w+") as fp:
            fp.write(f"{value}\n")
        self._checkpoint: int = value

    def _poll_redis(self) -> None:
        payloads: list = self.redis.pop()
        if payloads:
            logger.debug(f"_poll_redis: {payloads}")
            self.count["redis"] += len(payloads)
            self.refresh_views()
            self.on_publish(
                list(map(lambda payload: Payload(**payload), payloads))
            )
        time.sleep(settings.REDIS_POLL_INTERVAL)

    @threaded
    @exception
    def poll_redis(self) -> None:
        """Consumer which polls Redis continuously."""
        while True:
            self._poll_redis()

    async def _async_poll_redis(self) -> None:
        payloads: list = self.redis.pop()
        if payloads:
            logger.debug(f"_async_poll_redis: {payloads}")
            self.count["redis"] += len(payloads)
            await self.async_refresh_views()
            await self.async_on_publish(
                list(map(lambda payload: Payload(**payload), payloads))
            )
        await asyncio.sleep(settings.REDIS_POLL_INTERVAL)

    @exception
    async def async_poll_redis(self) -> None:
        """Consumer which polls Redis continuously."""
        while True:
            await self._async_poll_redis()

    @threaded
    @exception
    def poll_db(self) -> None:
        """
        Producer which polls Postgres continuously.

        Receive a notification message from the channel we are listening on
        """
        conn = self.engine.connect().connection
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        cursor.execute(f'LISTEN "{self.database}"')
        logger.debug(
            f'Listening to notifications on channel "{self.database}"'
        )
        payloads: list = []

        while True:
            # NB: consider reducing POLL_TIMEOUT to increase throughput
            if select.select([conn], [], [], settings.POLL_TIMEOUT) == (
                [],
                [],
                [],
            ):
                # Catch any hanging items from the last poll
                if payloads:
                    self.redis.push(payloads)
                    payloads = []
                continue

            try:
                conn.poll()
            except OperationalError as e:
                logger.fatal(f"OperationalError: {e}")
                os._exit(-1)

            while conn.notifies:
                if len(payloads) >= settings.REDIS_WRITE_CHUNK_SIZE:
                    self.redis.push(payloads)
                    payloads = []
                notification: t.AnyStr = conn.notifies.pop(0)
                if notification.channel == self.database:
                    payload = json.loads(notification.payload)
                    if (
                        payload["indices"]
                        and self.index in payload["indices"]
                        and payload["schema"] in self.tree.schemas
                    ):
                        payloads.append(payload)
                        logger.debug(f"poll_db: {payload}")
                        self.count["db"] += 1

    @exception
    def async_poll_db(self) -> None:
        """
        Producer which polls Postgres continuously.

        Receive a notification message from the channel we are listening on
        """
        try:
            self.conn.poll()
        except OperationalError as e:
            logger.fatal(f"OperationalError: {e}")
            os._exit(-1)

        while self.conn.notifies:
            notification: t.AnyStr = self.conn.notifies.pop(0)
            if notification.channel == self.database:
                payload = json.loads(notification.payload)
                if (
                    payload["indices"]
                    and self.index in payload["indices"]
                    and payload["schema"] in self.tree.schemas
                ):
                    self.redis.push([payload])
                    logger.debug(f"async_poll: {payload}")
                    self.count["db"] += 1

    def refresh_views(self) -> None:
        self._refresh_views()

    async def async_refresh_views(self) -> None:
        self._refresh_views()

    def _refresh_views(self) -> None:
        for node in self.tree.traverse_breadth_first():
            if node.table in self.views(node.schema):
                if node.table in self._materialized_views(node.schema):
                    self.refresh_view(node.table, node.schema)

    def on_publish(self, payloads: t.List[Payload]) -> None:
        self._on_publish(payloads)

    async def async_on_publish(self, payloads: t.List[Payload]) -> None:
        self._on_publish(payloads)

    def _on_publish(self, payloads: t.List[Payload]) -> None:
        """
        Redis publish event handler.

        This is triggered by poll_redis.
        It is called when an event is received from Redis.
        Deserialize the payload from Redis and sync to Elasticsearch/OpenSearch
        """
        # this is used for the views.
        # we substitute the views for the base table here
        for i, payload in enumerate(payloads):
            for node in self.tree.traverse_breadth_first():
                if payload.table in node.base_tables:
                    payloads[i].table = node.table

        logger.debug(f"on_publish len {len(payloads)}")
        # Safe inserts are insert operations that can be performed in any order
        # Optimize the safe INSERTS
        # TODO repeat this for the other place too
        # if all payload operations are INSERTS
        if set(map(lambda x: x.tg_op, payloads)) == set([INSERT]):
            _payloads: dict = defaultdict(list)

            for payload in payloads:
                _payloads[payload.table].append(payload)

            for _payload in _payloads.values():
                self.search_client.bulk(self.index, self._payloads(_payload))

        else:
            _payloads: t.List[Payload] = []
            for i, payload in enumerate(payloads):
                _payloads.append(payload)
                j: int = i + 1
                if j < len(payloads):
                    payload2 = payloads[j]
                    if (
                        payload.tg_op != payload2.tg_op
                        or payload.table != payload2.table
                    ):
                        self.search_client.bulk(
                            self.index, self._payloads(_payloads)
                        )
                        _payloads = []
                elif j == len(payloads):
                    self.search_client.bulk(
                        self.index, self._payloads(_payloads)
                    )
                    _payloads: list = []

        txids: t.Set = set(map(lambda x: x.xmin, payloads))
        # for truncate, tg_op txids is None so skip setting the checkpoint
        if txids != set([None]):
            self.checkpoint: int = min(min(txids), self.txid_current) - 1

    def pull(self) -> None:
        """Pull data from db."""
        txmin: int = self.checkpoint
        txmax: int = self.txid_current
        # this is the max lsn we should go upto
        upto_lsn: str = self.current_wal_lsn
        upto_nchanges: int = settings.LOGICAL_SLOT_CHUNK_SIZE

        logger.debug(f"pull txmin: {txmin} - txmax: {txmax}")
        # forward pass sync
        self.search_client.bulk(
            self.index, self.sync(txmin=txmin, txmax=txmax)
        )
        # now sync up to txmax to capture everything we may have missed
        self.logical_slot_changes(
            txmin=txmin,
            txmax=txmax,
            upto_nchanges=upto_nchanges,
            upto_lsn=upto_lsn,
        )
        self.checkpoint: int = txmax or self.txid_current
        self._truncate = True

    @threaded
    @exception
    def truncate_slots(self) -> None:
        """Truncate the logical replication slot."""
        while True:
            self._truncate_slots()
            time.sleep(settings.REPLICATION_SLOT_CLEANUP_INTERVAL)

    @exception
    async def async_truncate_slots(self) -> None:
        while True:
            self._truncate_slots()
            await asyncio.sleep(settings.REPLICATION_SLOT_CLEANUP_INTERVAL)

    def _truncate_slots(self) -> None:
        if self._truncate:
            logger.debug(f"Truncating replication slot: {self.__name}")
            self.logical_slot_get_changes(self.__name, upto_nchanges=None)

    @threaded
    @exception
    def status(self) -> None:
        while True:
            self._status(label="Sync")
            time.sleep(settings.LOG_INTERVAL)

    @exception
    async def async_status(self) -> None:
        while True:
            self._status(label="Async")
            await asyncio.sleep(settings.LOG_INTERVAL)

    def _status(self, label: str) -> None:
        # TODO: indicate if we are processing logical logs or not
        sys.stdout.write(
            f"{label} {self.database}:{self.index} "
            f"Xlog: [{self.count['xlog']:,}] => "
            f"Db: [{self.count['db']:,}] => "
            f"Redis: [{self.redis.qsize:,}] => "
            f"{self.search_client.name}: [{self.search_client.doc_count:,}]"
            f"...\n"
        )
        sys.stdout.flush()

    def receive(self) -> None:
        """
        Receive events from db.

        NB: pulls as well as receives in order to avoid missing data.

        1. Buffer all ongoing changes from db to Redis.
        2. Pull everything so far and also replay replication logs.
        3. Consume all changes from Redis.
        """
        if settings.USE_ASYNC:
            self._conn = self.engine.connect().connection
            self._conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = self.conn.cursor()
            cursor.execute(f'LISTEN "{self.database}"')
            event_loop = asyncio.get_event_loop()
            event_loop.add_reader(self.conn, self.async_poll_db)
            self.tasks: t.List[asyncio.Task] = [
                event_loop.create_task(self.async_poll_redis()),
                event_loop.create_task(self.async_truncate_slots()),
                event_loop.create_task(self.async_status()),
            ]

        else:
            # sync up to and produce items in the Redis cache
            if self.producer:
                self.poll_db()
                # sync up to current transaction_id
                self.pull()

            # start a background worker consumer thread to
            # poll Redis and populate Elasticsearch/OpenSearch
            if self.consumer:
                for _ in range(self.num_workers):
                    self.poll_redis()

            # start a background worker thread to cleanup the replication slot
            self.truncate_slots()
            # start a background worker thread to show status
            self.status()


@click.command()
@click.option(
    "--config",
    "-c",
    help="Schema config",
    type=click.Path(exists=True),
)
@click.option(
    "--daemon",
    "-d",
    is_flag=True,
    help="Run as a daemon (Incompatible with --polling)",
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["polling"],
)
@click.option(
    "--producer",
    is_flag=True,
    default=None,
    help="Run as a producer only",
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["consumer"],
)
@click.option(
    "--consumer",
    is_flag=True,
    help="Run as a consumer only",
    default=None,
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["producer"],
)
@click.option(
    "--polling",
    is_flag=True,
    help="Polling mode (Incompatible with -d)",
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["daemon"],
)
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
@click.option(
    "--analyze",
    "-a",
    is_flag=True,
    default=False,
    help="Analyse database",
    cls=MutuallyExclusiveOption,
    mutually_exclusive=["daemon", "polling"],
)
@click.option(
    "--num_workers",
    "-n",
    help="Number of workers to spawn for handling events",
    type=int,
    default=settings.NUM_WORKERS,
)
def main(
    config: str,
    daemon: bool,
    host: str,
    password: bool,
    port: int,
    sslmode: str,
    sslrootcert: str,
    user: str,
    verbose: bool,
    version: bool,
    analyze: bool,
    num_workers: int,
    polling: bool,
    producer: bool,
    consumer: bool,
) -> None:
    """Main application syncer."""
    if version:
        sys.stdout.write(f"Version: {__version__}\n")
        return

    kwargs: dict = {
        "user": user,
        "host": host,
        "port": port,
        "sslmode": sslmode,
        "sslrootcert": sslrootcert,
    }
    if password:
        kwargs["password"] = click.prompt(
            "Password",
            type=str,
            hide_input=True,
        )
    kwargs: dict = {
        key: value for key, value in kwargs.items() if value is not None
    }

    config: str = get_config(config)

    show_settings(config)

    if producer:
        consumer = False
    elif consumer:
        producer = False
    else:
        consumer = producer = True

    with Timer():
        if analyze:
            for doc in config_loader(config):
                sync: Sync = Sync(doc, verbose=verbose, **kwargs)
                sync.analyze()

        elif polling:
            while True:
                for doc in config_loader(config):
                    sync: Sync = Sync(doc, verbose=verbose, **kwargs)
                    sync.pull()
                time.sleep(settings.POLL_INTERVAL)

        else:
            tasks: t.List[asyncio.Task] = []
            for doc in config_loader(config):
                sync: Sync = Sync(
                    doc,
                    verbose=verbose,
                    num_workers=num_workers,
                    producer=producer,
                    consumer=consumer,
                    **kwargs,
                )
                sync.pull()
                if daemon:
                    sync.receive()
                    tasks.extend(sync.tasks)

            if settings.USE_ASYNC:
                event_loop = asyncio.get_event_loop()
                event_loop.run_until_complete(asyncio.gather(*tasks))
                event_loop.close()


if __name__ == "__main__":
    main()
