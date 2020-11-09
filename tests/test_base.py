"""Base tests."""
import pytest

from pgsync.base import Base
from pgsync.exc import TableNotFoundError


@pytest.mark.usefixtures('table_creator')
class TestBase(object):
    """Base tests."""

    def test_pg_settings(self, connection):
        pg_base = Base(connection.engine.url.database)
        pg_base.verbose = False
        value = pg_base.pg_settings('max_replication_slots')
        assert int(value) > 0
        assert pg_base.pg_settings('xyz') == None

    def test_has_permission(self, connection):
        pg_base = Base(connection.engine.url.database)
        pg_base.verbose = False
        value = pg_base.has_permission(
            connection.engine.url.username,
            'usesuper',
        )
        assert value == True
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
            'public.bookshelf',
            'public.shelf',
            'public.author',
            'public.book_author',
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
        assert row[1] != None
        pg_base.drop_replication_slot('slot_name')

    def test_drop_replication_slot(self, connection):
        pg_base = Base(connection.engine.url.database)
        pg_base.create_replication_slot('slot_name')
        pg_base.drop_replication_slot('slot_name')
