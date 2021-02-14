"""Base tests."""

import pytest
from mock import patch

from pgsync.base import (
    Base,
    create_database,
    create_extension,
    create_materialized_view,
    drop_database,
    drop_extension,
    drop_materialized_view,
    refresh_materialized_view,
)
from pgsync.exc import TableNotFoundError


@pytest.mark.usefixtures('table_creator')
class TestBase(object):
    """Base tests."""

    def test_pg_settings(self, connection):
        pg_base = Base(connection.engine.url.database)
        pg_base.verbose = False
        value = pg_base.pg_settings('max_replication_slots')
        assert int(value) > 0
        assert pg_base.pg_settings('xyz') is None

    def test_has_permission(self, connection):
        pg_base = Base(connection.engine.url.database)
        pg_base.verbose = False
        value = pg_base.has_permission(
            connection.engine.url.username,
            'usesuper',
        )
        assert value is True
        with pytest.raises(RuntimeError) as excinfo:
            value = pg_base.has_permission(
                connection.engine.url.username,
                'sudo',
            )
            assert 'Invalid user permission sudo' in str(excinfo.value)

    def test_model(self, connection):
        pg_base = Base(connection.engine.url.database)
        model = pg_base.model('book', 'public')
        assert str(model.original) == 'public.book'
        assert pg_base.models['public.book'] == model
        with pytest.raises(TableNotFoundError) as excinfo:
            pg_base.model('book', 'bar')
            assert 'Table "bar.book" not found in registry' in str(
                excinfo.value
            )

    def test_database(self, connection):
        pg_base = Base(connection.engine.url.database)
        assert pg_base.database == 'testdb'

    def test_schemas(self, connection):
        pg_base = Base(connection.engine.url.database)
        assert pg_base.schemas == ['public']

    def test_tables(self, connection):
        pg_base = Base(connection.engine.url.database)
        tables = [
            'public.continent',
            'public.country',
            'public.publisher',
            'public.book',
            'public.city',
            'public.book_subject',
            'public.subject',
            'public.book_language',
            'public.language',
            'public.book_shelf',
            'public.shelf',
            'public.author',
            'public.book_author',
            'public.rating',
        ]
        assert sorted(pg_base.tables('public')) == sorted(tables)

    def test_replication_slots(self, connection):
        pg_base = Base(connection.engine.url.database)
        assert pg_base.replication_slots('noob') == []
        replication_slots = pg_base.replication_slots(
            f'{connection.engine.url.database}_testdb'
        )
        assert 'testdb_testdb' == replication_slots[0][0]

    def test_create_replication_slot(self, connection):
        pg_base = Base(connection.engine.url.database)
        row = pg_base.create_replication_slot('slot_name')
        assert row[0] == 'slot_name'
        assert row[1] is not None
        pg_base.drop_replication_slot('slot_name')

    def test_drop_replication_slot(self, connection):
        pg_base = Base(connection.engine.url.database)
        pg_base.create_replication_slot('slot_name')
        pg_base.drop_replication_slot('slot_name')

    def test_get_schema(self, connection):
        pg_base = Base(connection.engine.url.database)

        pair1, pair2 = pg_base._get_schema('public', 'public1.my_table')
        assert pair1 == 'public1'
        assert pair2 == 'my_table'

        pair1, pair2 = pg_base._get_schema('public', 'my_table')
        assert pair1 == 'public'
        assert pair2 == 'my_table'

        with pytest.raises(ValueError) as excinfo:
            pg_base._get_schema('public', 'public1.my_table.foo')
            assert 'Invalid definition public1.my_table for public' == str(
                excinfo.value
            )

    def test_get_table(self, connection):
        pg_base = Base(connection.engine.url.database)

        table = pg_base._get_table('public', 'public.my_table')
        assert table == 'public.my_table'

        table = pg_base._get_table('public', 'my_table')
        assert table == 'public.my_table'

    @patch('pgsync.base.pg_execute')
    @patch('pgsync.base.pg_engine')
    @patch('pgsync.base.logger')
    def test_create_database(
        self,
        mock_logger,
        mock_pg_engine,
        mock_pg_execute,
        connection,
    ):
        database = connection.engine.url.database
        mock_pg_engine.return_value = connection.engine
        create_database(database, echo=True)
        assert mock_logger.debug.call_count == 2
        mock_logger.debug.assert_any_call(f'Creating database: {database}')
        mock_logger.debug.assert_any_call(f'Created database: {database}')
        mock_pg_engine.assert_any_call(database='postgres', echo=True)
        mock_pg_execute.assert_any_call(
            connection.engine,
            f'CREATE DATABASE {database}',
        )

    @patch('pgsync.base.pg_execute')
    @patch('pgsync.base.pg_engine')
    @patch('pgsync.base.logger')
    def test_drop_database(
        self,
        mock_logger,
        mock_pg_engine,
        mock_pg_execute,
        connection,
    ):
        database = connection.engine.url.database
        mock_pg_engine.return_value = connection.engine
        drop_database(database, echo=True)
        assert mock_logger.debug.call_count == 2
        mock_logger.debug.assert_any_call(f'Dropping database: {database}')
        mock_logger.debug.assert_any_call(f'Dropped database: {database}')
        mock_pg_engine.assert_any_call(database='postgres', echo=True)
        mock_pg_execute.assert_any_call(
            connection.engine,
            f'DROP DATABASE IF EXISTS {database}',
        )

    @patch('pgsync.base.pg_execute')
    @patch('pgsync.base.pg_engine')
    @patch('pgsync.base.logger')
    def test_create_extension(
        self,
        mock_logger,
        mock_pg_engine,
        mock_pg_execute,
        connection,
    ):
        database = connection.engine.url.database
        mock_pg_engine.return_value = connection.engine
        create_extension(database, 'my_ext', echo=True)
        assert mock_logger.debug.call_count == 2
        mock_logger.debug.assert_any_call('Creating extension: my_ext')
        mock_logger.debug.assert_any_call('Created extension: my_ext')
        mock_pg_engine.assert_any_call(database=database, echo=True)
        mock_pg_execute.assert_any_call(
            connection.engine,
            'CREATE EXTENSION IF NOT EXISTS "my_ext"',
        )

    @patch('pgsync.base.pg_execute')
    @patch('pgsync.base.pg_engine')
    @patch('pgsync.base.logger')
    def test_drop_extension(
        self,
        mock_logger,
        mock_pg_engine,
        mock_pg_execute,
        connection,
    ):
        database = connection.engine.url.database
        mock_pg_engine.return_value = connection.engine
        drop_extension(database, 'my_ext', echo=True)
        assert mock_logger.debug.call_count == 2
        mock_logger.debug.assert_any_call('Dropping extension: my_ext')
        mock_logger.debug.assert_any_call('Dropped extension: my_ext')
        mock_pg_engine.assert_any_call(database=database, echo=True)
        mock_pg_execute.assert_any_call(
            connection.engine,
            'DROP EXTENSION IF EXISTS "my_ext"',
        )

    @patch('pgsync.base.pg_execute')
    @patch('pgsync.base.pg_engine')
    @patch('pgsync.base.logger')
    def test_create_materialized_view(
        self,
        mock_logger,
        mock_pg_engine,
        mock_pg_execute,
        connection,
    ):
        database = connection.engine.url.database
        mock_pg_engine.return_value = connection.engine
        create_materialized_view(database, 'my_view', 'SELECT 1', echo=True)
        assert mock_logger.debug.call_count == 2
        mock_logger.debug.assert_any_call(
            'Creating materialized view: my_view with SELECT 1'
        )
        mock_logger.debug.assert_any_call(
            'Created materialized view: my_view'
        )
        mock_pg_engine.assert_any_call(database=database, echo=True)
        mock_pg_execute.assert_any_call(
            connection.engine,
            'CREATE MATERIALIZED VIEW my_view AS SELECT 1',
        )

    @patch('pgsync.base.pg_execute')
    @patch('pgsync.base.pg_engine')
    @patch('pgsync.base.logger')
    def test_refresh_materialized_view(
        self,
        mock_logger,
        mock_pg_engine,
        mock_pg_execute,
        connection,
    ):
        database = connection.engine.url.database
        mock_pg_engine.return_value = connection.engine
        refresh_materialized_view(database, 'my_view')
        assert mock_logger.debug.call_count == 2
        mock_logger.debug.assert_any_call(
            'Refreshing materialized view: my_view'
        )
        mock_logger.debug.assert_any_call(
            'Refreshed materialized view: my_view'
        )
        mock_pg_engine.assert_any_call(database=database, echo=False)
        mock_pg_execute.assert_any_call(
            connection.engine,
            'REFRESH MATERIALIZED VIEW my_view',
        )

    @patch('pgsync.base.pg_execute')
    @patch('pgsync.base.pg_engine')
    @patch('pgsync.base.logger')
    def test_drop_materialized_view(
        self,
        mock_logger,
        mock_pg_engine,
        mock_pg_execute,
        connection,
    ):
        database = connection.engine.url.database
        mock_pg_engine.return_value = connection.engine
        drop_materialized_view(database, 'my_view')
        assert mock_logger.debug.call_count == 2
        mock_logger.debug.assert_any_call(
            'Dropping materialized view: my_view'
        )
        mock_logger.debug.assert_any_call(
            'Dropped materialized view: my_view'
        )
        mock_pg_engine.assert_any_call(database=database, echo=False)
        mock_pg_execute.assert_any_call(
            connection.engine,
            'DROP MATERIALIZED VIEW IF EXISTS my_view',
        )
