"""Sync tests."""

import importlib
import os
import typing as t
from collections import namedtuple

import pytest
from mock import ANY, call, patch

from pgsync.base import Base, Payload
from pgsync.exc import (
    InvalidTGOPError,
    PrimaryKeyNotFoundError,
    RDSError,
    SchemaError,
)
from pgsync.node import Node
from pgsync.singleton import Singleton
from pgsync.sync import settings, Sync

from .testing_utils import override_env_var

ROW = namedtuple("Row", ["data", "xid"])


@pytest.fixture(scope="function")
def sync():
    with override_env_var(ELASTICSEARCH="True", OPENSEARCH="False"):
        importlib.reload(settings)
        _sync = Sync(
            {
                "index": "testdb",
                "database": "testdb",
                "nodes": {
                    "table": "book",
                    "columns": ["isbn", "title", "description"],
                    "children": [
                        {
                            "table": "publisher",
                            "columns": ["id", "name"],
                            "relationship": {
                                "variant": "object",
                                "type": "one_to_one",
                                "foreign_key": {
                                    "child": ["id"],
                                    "parent": ["publisher_id"],
                                },
                            },
                        },
                    ],
                },
            },
        )
        Singleton._instances = {}
        yield _sync
        _sync.logical_slot_get_changes(
            f"{_sync.database}_testdb",
            upto_nchanges=None,
        )
        _sync.engine.connect().close()
        _sync.engine.dispose()
        _sync.session.close()
        _sync.search_client.close()


@pytest.mark.usefixtures("table_creator")
class TestSync(object):
    """Sync tests."""

    @patch("pgsync.sync.logger")
    def test_logical_slot_changes(self, mock_logger, sync):
        with patch("pgsync.sync.Sync.logical_slot_peek_changes") as mock_peek:
            mock_peek.side_effect = [
                [ROW("BEGIN: blah", 1234)],
                [],
            ]
            with patch("pgsync.sync.Sync.sync") as mock_sync:
                sync.logical_slot_changes()
                mock_peek.assert_any_call(
                    "testdb_testdb",
                    txmin=None,
                    txmax=None,
                    upto_nchanges=None,
                    upto_lsn=None,
                )
                mock_sync.assert_not_called()

        with patch("pgsync.sync.Sync.logical_slot_peek_changes") as mock_peek:
            mock_peek.side_effect = [
                [ROW("COMMIT: blah", 1234)],
                [],
            ]
            with patch("pgsync.sync.Sync.sync") as mock_sync:
                sync.logical_slot_changes()
                mock_peek.assert_any_call(
                    "testdb_testdb",
                    txmin=None,
                    txmax=None,
                    upto_nchanges=None,
                    upto_lsn=None,
                )
                mock_sync.assert_not_called()

        with patch("pgsync.sync.Sync.logical_slot_peek_changes") as mock_peek:
            mock_peek.side_effect = [
                [
                    ROW(
                        "table public.book: INSERT: id[integer]:10 isbn[character "  # noqa E501
                        "varying]:'888' title[character varying]:'My book title' "  # noqa E501
                        "description[character varying]:null copyright[character "  # noqa E501
                        "varying]:null tags[jsonb]:null publisher_id[integer]:null",  # noqa E501
                        1234,
                    ),
                ],
                [],
            ]

            with patch(
                "pgsync.sync.Sync.logical_slot_get_changes"
            ) as mock_get:
                with patch("pgsync.sync.Sync.sync") as mock_sync:
                    sync.logical_slot_changes()
                    mock_peek.assert_any_call(
                        "testdb_testdb",
                        txmin=None,
                        txmax=None,
                        upto_nchanges=None,
                        upto_lsn=None,
                    )
                    mock_get.assert_called_once()
                    mock_sync.assert_called_once()
                    calls = [
                        call("txid: 1234"),
                        call(ANY),
                        call("tg_op: INSERT table: public.book"),
                    ]
                    assert mock_logger.debug.call_args_list == calls

        with pytest.raises(Exception) as excinfo:
            with patch(
                "pgsync.sync.Sync.logical_slot_peek_changes"
            ) as mock_peek:
                mock_peek.side_effect = [
                    [
                        ROW(
                            "table public.book: INSERT: id[integer]:10 isbn[character "  # noqa E501
                            "varying]:'888' title[character varying]:'My book title' "  # noqa E501
                            "description[character varying]:null copyright[character "  # noqa E501
                            "varying]:null tags[jsonb]:null publisher_id[integer]:null",  # noqa E501
                            1234,
                        ),
                    ],
                    [],
                ]

                with patch(
                    "pgsync.sync.Sync.logical_slot_get_changes"
                ) as mock_get:
                    with patch(
                        "pgsync.sync.Sync.parse_logical_slot",
                        side_effect=Exception,
                    ):
                        with patch("pgsync.sync.Sync.sync") as mock_sync:
                            sync.logical_slot_changes()
            assert "Error parsing row" in str(excinfo.value)

    @patch("pgsync.sync.SearchClient")
    def test_sync_validate(self, mock_es):
        with pytest.raises(SchemaError) as excinfo:
            Sync(
                doc={
                    "index": "testdb",
                    "database": "testdb",
                    "nodes": ["foo"],
                },
                verbose=False,
                validate=True,
                repl_slots=False,
            )
        assert "Incompatible schema. Please run v2 schema migration" in str(
            excinfo.value
        )

        Sync(
            doc={
                "index": "testdb",
                "database": "testdb",
                "nodes": {"table": "book"},
                "plugins": ["Hero"],
            },
            verbose=False,
            validate=True,
            repl_slots=False,
        )

        def _side_effect(*args, **kwargs):
            if args[0] == 0:
                return 0
            elif args[0] == "max_replication_slots":
                raise RuntimeError(
                    "Ensure there is at least one replication slot defined "
                    "by setting max_replication_slots = 1"
                )

            elif args[0] == "wal_level":
                raise RuntimeError(
                    "Enable logical decoding by setting wal_level=logical"
                )
            elif args[0] == "rds_logical_replication":
                raise RDSError("rds.logical_replication is not enabled")
            else:
                return args[0]

        with pytest.raises(RuntimeError) as excinfo:
            with patch(
                "pgsync.base.Base.pg_settings",
                side_effects=_side_effect("max_replication_slots"),
            ):
                Sync(
                    doc={
                        "index": "testdb",
                        "database": "testdb",
                        "nodes": {"table": "book"},
                        "plugins": ["Hero"],
                    },
                )
        assert (
            "Ensure there is at least one replication slot defined "
            "by setting max_replication_slots = 1" in str(excinfo.value)
        )

        with pytest.raises(RuntimeError) as excinfo:
            with patch(
                "pgsync.base.Base.pg_settings",
                return_value=-1,
            ):
                Sync(
                    doc={
                        "index": "testdb",
                        "database": "testdb",
                        "nodes": {"table": "book"},
                        "plugins": ["Hero"],
                    },
                )
            assert (
                "Ensure there is at least one replication slot defined "
                "by setting max_replication_slots = 1" in str(excinfo.value)
            )

        with pytest.raises(RuntimeError) as excinfo:
            with patch(
                "pgsync.base.Base.pg_settings",
                side_effects=_side_effect("wal_level"),
            ):
                Sync(
                    doc={
                        "index": "testdb",
                        "database": "testdb",
                        "nodes": {"table": "book"},
                        "plugins": ["Hero"],
                    },
                )
        assert "Enable logical decoding by setting wal_level=logical" in str(
            excinfo.value
        )

        with pytest.raises(RDSError) as excinfo:
            with patch(
                "pgsync.base.Base.pg_settings",
                side_effects=_side_effect("rds_logical_replication"),
            ):
                Sync(
                    doc={
                        "index": "testdb",
                        "database": "testdb",
                        "nodes": {"table": "book"},
                        "plugins": ["Hero"],
                    },
                )
        assert "rds.logical_replication is not enabled" in str(excinfo.value)

        with pytest.raises(RuntimeError) as excinfo:
            with patch(
                "pgsync.base.Base.replication_slots",
                return_value=None,
            ):
                Sync(
                    doc={
                        "index": "testdb",
                        "database": "testdb",
                        "nodes": {"table": "book"},
                        "plugins": ["Hero"],
                    },
                )
            assert 'Replication slot "testdb_testdb" does not exist' in str(
                excinfo.value
            )

        with patch(
            "pgsync.base.Base._materialized_views",
            side_effect=RuntimeError("hey there"),
        ):
            with pytest.raises(RuntimeError) as excinfo:
                Sync(
                    doc={
                        "index": "testdb",
                        "database": "testdb",
                        "nodes": {"table": "book"},
                        "plugins": ["Hero"],
                    },
                )
                assert "hey there" in str(excinfo.value)

        Sync(
            doc={
                "index": "testdb",
                "database": "testdb",
                "nodes": {"table": "book"},
                "plugins": ["Hero"],
            },
        )
        # raise

    def test_status(self, sync):
        with patch("pgsync.sync.sys") as mock_sys:
            sync._status("mydb")
            mock_sys.stdout.write.assert_called_once_with(
                "mydb testdb:testdb "
                "Xlog: [0] => "
                "Db: [0] => "
                "Redis: [0] => "
                "Elasticsearch: [0]...\n"
            )

    @patch("pgsync.sync.logger")
    def test_truncate_slots(self, mock_logger, sync):
        with patch("pgsync.sync.Sync.logical_slot_get_changes") as mock_get:
            sync._truncate = True
            sync._truncate_slots()
            mock_get.assert_called_once_with(
                "testdb_testdb", upto_nchanges=None
            )
            mock_logger.debug.assert_called_once_with(
                "Truncating replication slot: testdb_testdb"
            )

    @patch("pgsync.sync.SearchClient.bulk")
    @patch("pgsync.sync.logger")
    def test_pull(self, mock_logger, mock_es, sync):
        with patch("pgsync.sync.Sync.logical_slot_changes") as mock_get:
            sync.pull()
            txmin = None
            txmax = sync.txid_current - 1
            mock_get.assert_called_once_with(
                txmin=txmin,
                txmax=txmax,
                upto_nchanges=settings.LOGICAL_SLOT_CHUNK_SIZE,
                upto_lsn=ANY,
            )
            mock_logger.debug.assert_called_once_with(
                f"pull txmin: {txmin} - txmax: {txmax}"
            )
            assert sync.checkpoint == txmax
            assert sync._truncate is True
            mock_es.assert_called_once_with("testdb", ANY)

    @patch("pgsync.sync.SearchClient.bulk")
    @patch("pgsync.sync.logger")
    def test__on_publish(self, mock_logger, mock_es, sync):
        payloads = [
            Payload(
                schema="public",
                tg_op="INSERT",
                table="book",
                old={"isbn": "001"},
                new={"isbn": "0001"},
                xmin=1234,
            ),
            Payload(
                schema="public",
                tg_op="INSERT",
                table="book",
                old={"isbn": "002"},
                new={"isbn": "0002"},
                xmin=1234,
            ),
            Payload(
                schema="public",
                tg_op="INSERT",
                table="book",
                old={"isbn": "003"},
                new={"isbn": "0003"},
                xmin=1234,
            ),
        ]
        sync._on_publish(payloads)
        mock_logger.debug.assert_any_call("on_publish len 3")
        assert sync.checkpoint is not None
        mock_es.assert_called_once_with("testdb", ANY)

    @patch("pgsync.sync.SearchClient.bulk")
    @patch("pgsync.sync.logger")
    def test__on_publish_mixed_ops(self, mock_logger, mock_es, sync):
        payloads = [
            Payload(
                schema="public",
                tg_op="INSERT",
                table="book",
                old={"isbn": "001"},
                new={"isbn": "0001"},
                xmin=1234,
            ),
            Payload(
                schema="public",
                tg_op="UPDATE",
                table="book",
                old={"isbn": "002"},
                new={"isbn": "0002"},
                xmin=1234,
            ),
            Payload(
                schema="public",
                tg_op="DELETE",
                table="book",
                old={"isbn": "003"},
                new={"isbn": "0003"},
                xmin=1234,
            ),
        ]
        sync._on_publish(payloads)
        mock_logger.debug.assert_any_call("on_publish len 3")
        assert sync.checkpoint is not None
        mock_es.debug.call_count == 3
        mock_es.assert_any_call("testdb", ANY)

    @patch("pgsync.sync.Sync._on_publish")
    def test_on_publish(self, mock_on_publish, sync):
        payloads = [
            Payload(
                schema="public",
                tg_op="DELETE",
                table="book",
                old={"isbn": "003"},
                new={"isbn": "0003"},
                xmin=1234,
            ),
        ]
        sync.on_publish(payloads)
        mock_on_publish.assert_called_once_with(payloads)

    def test_sync_analyze(self, sync):
        with patch("pgsync.sync.sys") as mock_sys:
            sync.analyze()
            mock_sys.stdout.write.assert_called_once_with(
                'Found index "publisher_name_key" for table "publisher" for '
                "columns: ['id']: OK \n"
            )

        with patch("pgsync.sync.sys") as mock_sys:
            with patch("pgsync.sync.Sync.indices") as mock_indices:
                mock_indices.return_value = []
                sync.analyze()
                assert mock_sys.stdout.write.call_count == 4
                assert mock_sys.stdout.write.call_args_list == [
                    call(
                        'Missing index on table "publisher" for '
                        "columns: ['id']\n"
                    ),
                    call(
                        'Create one with: "\x1b[4mCREATE INDEX '
                        'idx_publisher_id ON publisher (id)\x1b[0m"\n'
                    ),
                    call(
                        "-----------------------------------------------------"
                        "---------------------------"
                    ),
                    call("\n"),
                ]

    def test__update_op(self, sync, connection):
        pg_base = Base(connection.engine.url.database)
        node = Node(
            models=pg_base.models,
            table="book",
            schema="public",
        )
        filters: dict = {"book": []}
        payloads: t.List[Payload] = [
            Payload(
                tg_op="UPDATE",
                table="book",
                old={"isbn": "001"},
                new={"isbn": "aa1"},
            )
        ]
        assert sync.search_client.doc_count == 0
        _filters = sync._update_op(node, filters, payloads)
        sync.search_client.refresh("testdb")
        assert _filters == {"book": [{"isbn": "aa1"}]}
        docs = sync.search_client.search(
            "testdb", body={"query": {"match_all": {}}}
        )
        assert len(docs["hits"]["hits"]) == 0

    def test__insert_op(self, sync, connection):
        pg_base = Base(connection.engine.url.database)
        node = Node(
            models=pg_base.models,
            table="book",
            schema="public",
        )
        filters: dict = {"book": []}
        payloads: t.List[Payload] = [
            Payload(
                tg_op="INSERT",
                table="book",
                new={"isbn": "001"},
            )
        ]
        _filters = sync._insert_op(node, filters, payloads)
        assert _filters == {"book": [{"isbn": "001"}]}
        sync.search_client.refresh("testdb")
        assert sync.search_client.doc_count == 0
        docs = sync.search_client.search(
            "testdb", body={"query": {"match_all": {}}}
        )
        assert len(docs["hits"]["hits"]) == 0

        node = Node(
            models=pg_base.models,
            table="publisher",
            schema="public",
        )
        payloads: t.List[Payload] = [
            Payload(
                tg_op="INSERT",
                table="publisher",
                new={"id": 1},
            )
        ]
        node.parent = None
        with pytest.raises(Exception):
            with patch("pgsync.sync.logger") as mock_logger:
                _filters = sync._insert_op(node, {"publisher": []}, payloads)
            mock_logger.exception.assert_called_once_with(
                "Could not get parent from node: public.publisher"
            )

    @patch("pgsync.search_client.SearchClient.bulk")
    def test__delete_op(self, mock_es, sync, connection):
        pg_base = Base(connection.engine.url.database)
        node = Node(
            models=pg_base.models,
            table="book",
            schema="public",
        )
        filters: dict = {"book": []}
        payloads: t.List[Payload] = [
            Payload(
                tg_op="DELETE",
                table="book",
                new={"isbn": "001"},
                old=None,
            )
        ]

        _filters = sync._delete_op(node, filters, payloads)
        assert _filters == {"book": []}
        sync.search_client.refresh("testdb")
        mock_es.assert_called_once_with(
            "testdb",
            [{"_id": "001", "_index": "testdb", "_op_type": "delete"}],
            raise_on_exception=None,
            raise_on_error=None,
        )

    @patch("pgsync.sync.SearchClient")
    def test__truncate_op(self, mock_es, sync, connection):
        pg_base = Base(connection.engine.url.database)
        node: Node = Node(
            models=pg_base.models,
            table="book",
            schema="public",
        )
        filters: dict = {"book": []}
        _filters = sync._truncate_op(node, filters)
        assert _filters == {"book": []}

        # truncate a non root table
        node: Node = Node(
            models=pg_base.models,
            table="publisher",
            schema="public",
        )
        _filters = sync._truncate_op(node, filters)
        assert _filters == {"book": []}

    def test__payload(self, sync):
        with patch("pgsync.sync.Sync._insert_op") as mock_op:
            with patch("pgsync.sync.logger") as mock_logger:
                for _ in sync._payloads(
                    [
                        Payload(
                            tg_op="INSERT",
                            table="book",
                            schema="public",
                            new={"isbn": "001"},
                            old={"isbn": "002"},
                        ),
                    ]
                ):
                    pass
                mock_logger.exception.assert_not_called()
                mock_logger.debug.assert_called_once_with(
                    "tg_op: INSERT table: public.book"
                )
                mock_op.assert_called_once()

        with patch("pgsync.sync.Sync._update_op") as mock_op:
            with patch("pgsync.sync.logger") as mock_logger:
                for _ in sync._payloads(
                    [
                        Payload(
                            tg_op="UPDATE",
                            table="book",
                            schema="public",
                            new={"isbn": "001"},
                            old={"isbn": "002"},
                        ),
                    ]
                ):
                    pass
                mock_logger.exception.assert_not_called()
                mock_logger.debug.assert_called_once_with(
                    "tg_op: UPDATE table: public.book"
                )
                mock_op.assert_called_once()

        with patch("pgsync.sync.Sync._delete_op") as mock_op:
            with patch("pgsync.sync.logger") as mock_logger:
                for _ in sync._payloads(
                    [
                        Payload(
                            tg_op="DELETE",
                            table="book",
                            schema="public",
                            new={"isbn": "001"},
                            old={"isbn": "002"},
                        ),
                    ]
                ):
                    pass
                mock_logger.exception.assert_not_called()
                mock_logger.debug.assert_called_once_with(
                    "tg_op: DELETE table: public.book"
                )
                mock_op.assert_called_once()

        with patch("pgsync.sync.Sync._truncate_op") as mock_op:
            with patch("pgsync.sync.logger") as mock_logger:
                for _ in sync._payloads(
                    [
                        Payload(
                            tg_op="TRUNCATE",
                            table="book",
                            schema="public",
                            new={"isbn": "001"},
                            old={"isbn": "002"},
                        ),
                    ]
                ):
                    pass
                mock_logger.exception.assert_not_called()
                mock_logger.debug.assert_called_once_with(
                    "tg_op: TRUNCATE table: public.book"
                )
                mock_op.assert_called_once()

    def test__build_filters(self, sync, connection):
        pg_base = Base(connection.engine.url.database)
        node = Node(
            models=pg_base.models,
            table="book",
            schema="public",
        )
        filters = {
            "book": [
                {"isbn": "001", "title": "this"},
                {"isbn": "002", "title": "that"},
            ]
        }
        assert len(node._filters) == 0
        filters = sync.query_builder._build_filters(filters, node)
        assert len(filters) == 2
        assert str(filters) == (
            "book_1.isbn = :isbn_1 AND book_1.title = :title_1 OR "
            "book_1.isbn = :isbn_2 AND book_1.title = :title_2"
        )

    def test_checkpoint(self, sync):
        os.unlink(sync._checkpoint_file)
        assert os.path.exists(sync._checkpoint_file) is False
        sync.checkpoint = 1234
        with open(sync._checkpoint_file, "r") as fp:
            value: int = int(fp.read().split()[0])
            assert value == 1234

        with pytest.raises(ValueError) as excinfo:
            sync.checkpoint = None
            assert "Cannot assign a None value to checkpoint" == str(
                excinfo.value
            )

    def test__payload_data(self, sync):
        payload = Payload(
            tg_op="INSERT",
            table="book",
            old={"id": 1},
            new={"id": 4},
        )
        assert payload.data == {"id": 4}
        assert payload.table == "book"
        assert payload.tg_op == "INSERT"

        payload = Payload(
            tg_op="DELETE",
            table="book",
            old={"id": 1},
            new={"id": 4},
        )

        assert payload.data == {"id": 1}
        assert payload.table == "book"
        assert payload.tg_op == "DELETE"

        payload = Payload(
            tg_op="UPDATE",
            table="book",
            new=None,
            old={"id": 1},
        )

        assert payload.data == {}

        payload = Payload(
            tg_op="INSERT",
            table="book",
            new={"id": 1},
        )

        assert payload.data == {"id": 1}

    def test_get_doc_id(self, sync):
        doc_id = sync.get_doc_id(["column_1", "column_2"], "book")
        assert doc_id == "column_1|column_2"

        with pytest.raises(PrimaryKeyNotFoundError) as excinfo:
            sync.get_doc_id([], "book")
            assert "No primary key found on table: book" == str(excinfo.value)

    @patch("pgsync.sync.SearchClient._create_setting")
    def test_create_setting(self, mock_es, sync):
        sync.create_setting()
        mock_es.assert_called_once_with(
            "testdb",
            ANY,
            setting=None,
            mapping=None,
            mappings=None,
            routing=None,
        )

    @patch("pgsync.sync.Sync.teardown")
    def test_setup(self, mock_teardown, sync):
        with patch("pgsync.sync.Base.create_function") as mock_create_function:
            with patch("pgsync.sync.Base.create_view") as mock_create_view:
                with patch(
                    "pgsync.sync.Base.create_triggers"
                ) as mock_create_triggers:
                    with patch(
                        "pgsync.sync.Base.create_replication_slot"
                    ) as mock_create_replication_slot:
                        sync.setup()
                        mock_create_replication_slot.assert_called_once_with(
                            "testdb_testdb"
                        )
                    mock_create_triggers.assert_called_once_with(
                        "public",
                        tables={"publisher", "book"},
                        join_queries=True,
                    )
                mock_create_view.assert_called_once_with(
                    "testdb",
                    "public",
                    {"publisher", "book"},
                    {"publisher": {"publisher_id", "id"}},
                )
            mock_create_function.assert_called_once_with("public")
        mock_teardown.assert_called_once_with(drop_view=False)

    @patch("pgsync.redisqueue.RedisQueue.delete")
    def test_teardown(self, mock_redis, sync):
        with patch("pgsync.sync.Base.drop_function") as mock_drop_function:
            with patch("pgsync.sync.Base.drop_view") as mock_drop_view:
                with patch(
                    "pgsync.sync.Base.drop_triggers"
                ) as mock_drop_triggers:
                    with patch(
                        "pgsync.sync.Base.drop_replication_slot"
                    ) as mock_drop_replication_slot:
                        sync.teardown()
                        mock_drop_replication_slot.assert_called_once_with(
                            "testdb_testdb"
                        )
                    mock_drop_triggers.assert_called_once_with(
                        schema="public",
                        tables={"publisher", "book"},
                        join_queries=True,
                    )
                mock_drop_view.assert_called_once_with("public")
            mock_drop_function.assert_called_once_with("public")
        mock_redis.assert_called_once()
        assert os.path.exists(sync._checkpoint_file) is False

        with patch("pgsync.sync.logger") as mock_logger:
            with patch("pgsync.sync.Base.drop_replication_slot"):
                self._checkpoint_file = "foo"
                sync.teardown()
                mock_logger.warning.assert_called_once_with(
                    "Checkpoint file not found: ./.testdb_testdb"
                )

    def test_root(self, sync):
        root = sync.tree.root
        assert root is not None
        assert str(root) == "Node: public.book"

    def test_payloads(self, sync):
        payloads: t.List[Payload] = [
            Payload(
                tg_op="INSERT",
                table="book",
                new={"isbn": "002"},
                schema="public",
            ),
        ]
        for _ in sync._payloads(payloads):
            pass

    def test_payloads_invalid_tg_op(self, mocker, sync):
        payloads: t.List[Payload] = [
            Payload(
                tg_op="FOO",
                table="book",
                old={"isbn": "001"},
                new={"isbn": "002"},
                schema="public",
            ),
        ]
        with patch("pgsync.sync.logger") as mock_logger:
            with pytest.raises(InvalidTGOPError):
                for _ in sync._payloads(payloads):
                    pass
            mock_logger.exception.assert_called_once_with("Unknown tg_op FOO")

    def test_payloads_in_batches(self, mocker, sync):
        # inserting a root node
        payloads: t.List[Payload] = [
            Payload(
                tg_op="INSERT",
                table="book",
                new={"isbn": "002"},
                schema="public",
            )
        ] * 20
        with patch("pgsync.sync.Sync.sync") as mock_sync:
            with override_env_var(FILTER_CHUNK_SIZE="4"):
                importlib.reload(settings)
                for _ in sync._payloads(payloads):
                    pass
        assert mock_sync.call_count == 25
        assert mock_sync.call_args_list[-1] == call(
            filters={
                "book": [
                    {"isbn": "002"},
                    {"isbn": "002"},
                    {"isbn": "002"},
                    {"isbn": "002"},
                ]
            },
        )

        # updating a child table
        payloads: t.List[Payload] = [
            Payload(
                tg_op="UPDATE",
                table="publisher",
                new={"id": 1, "name": "foo"},
                old={"id": 1},
                schema="public",
            )
        ]
        filters: dict = {
            "book": [
                {"isbn": "001"},
            ],
            "publisher": [
                {"id": 1},
            ],
        }
        with patch("pgsync.sync.Sync._update_op", return_value=filters):
            with patch("pgsync.sync.Sync.sync") as mock_sync:
                with override_env_var(FILTER_CHUNK_SIZE="1"):
                    importlib.reload(settings)
                    for _ in sync._payloads(payloads):
                        pass
                mock_sync.assert_called_once_with(filters=filters)

    @patch("pgsync.sync.compiled_query")
    def test_sync(self, mock_compiled_query, sync):
        sync.verbose = True
        for _ in sync.sync():
            pass
        mock_compiled_query.assert_called_once_with(ANY, "Query")

    @patch("pgsync.sync.logger")
    @patch("pgsync.sync.Sync.refresh_views")
    @patch("pgsync.sync.Sync.on_publish")
    @patch("pgsync.sync.time")
    def test_poll_redis(
        self, mock_time, mock_on_publish, mock_refresh_views, mock_logger, sync
    ):
        items = [{"tg_op": "INSERT"}, {"tg_op": "UPDATE"}]
        sync.redis.push(items)
        sync._poll_redis()
        mock_on_publish.assert_called_once_with([ANY, ANY])
        mock_refresh_views.assert_called_once()
        mock_logger.debug.assert_called_once_with(f"_poll_redis: {items}")
        mock_time.sleep.assert_called_once_with(settings.REDIS_POLL_INTERVAL)
        assert sync.count["redis"] == 2
