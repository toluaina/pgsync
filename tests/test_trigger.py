"""Trigger tests."""

import pytest
import sqlalchemy as sa

from pgsync.base import Base
from pgsync.settings import IS_MYSQL_COMPAT
from pgsync.trigger import CREATE_TRIGGER_TEMPLATE


@pytest.mark.skipif(
    IS_MYSQL_COMPAT,
    reason="Skipped because IS_MYSQL_COMPAT env var is set",
)
@pytest.mark.usefixtures("table_creator")
class TestTrigger(object):
    """Trigger tests."""

    def test_trigger_template(self):
        expected = """
CREATE OR REPLACE FUNCTION table_notify() RETURNS TRIGGER AS $$
DECLARE
  channel TEXT;
  old_row JSON;
  new_row JSON;
  notification JSON;
  xmin BIGINT;
  _indices TEXT [];
  _primary_keys TEXT [];
  _foreign_keys TEXT [];
  _columns TEXT [];
  _changed BOOLEAN;

BEGIN
    -- database is also the channel name.
    channel := CURRENT_DATABASE();

    IF TG_OP = 'DELETE' THEN

        SELECT primary_keys INTO _primary_keys
        FROM _view
        WHERE table_name = TG_TABLE_NAME;

        old_row = ROW_TO_JSON(OLD);
        old_row := (
            SELECT JSONB_OBJECT_AGG(key, value)
            FROM JSON_EACH(old_row)
            WHERE key = ANY(_primary_keys)
        );
        xmin := OLD.xmin;
    ELSE
        IF TG_OP <> 'TRUNCATE' THEN

            SELECT primary_keys, foreign_keys, indices, columns
            INTO _primary_keys, _foreign_keys, _indices, _columns
            FROM _view
            WHERE table_name = TG_TABLE_NAME;

            -- normalize null to empty array
            _columns := COALESCE(_columns, ARRAY[]::TEXT[]);

            -- Only react if any _columns actually changed
            IF TG_OP = 'UPDATE' THEN
                SELECT EXISTS (
                    SELECT 1
                    FROM JSONB_EACH(TO_JSONB(NEW.*)) n
                    JOIN JSONB_EACH(TO_JSONB(OLD.*)) o USING (key)
                    WHERE n.key = ANY(_columns)
                    AND n.value IS DISTINCT FROM o.value
                )
                INTO _changed;

                IF NOT _changed THEN
                    RETURN NEW;  -- skip notification; nothing relevant changed
                END IF;
            END IF;

            new_row = ROW_TO_JSON(NEW);
            new_row := (
                SELECT JSONB_OBJECT_AGG(key, value)
                FROM JSON_EACH(new_row)
                WHERE key = ANY(_primary_keys || _foreign_keys)
            );
            IF TG_OP = 'UPDATE' THEN
                old_row = ROW_TO_JSON(OLD);
                old_row := (
                    SELECT JSONB_OBJECT_AGG(key, value)
                    FROM JSON_EACH(old_row)
                    WHERE key = ANY(_primary_keys || _foreign_keys)
                );
            END IF;
            xmin := NEW.xmin;
        END IF;
    END IF;

    -- construct the notification as a JSON object.
    notification = JSON_BUILD_OBJECT(
        'xmin', xmin,
        'new', new_row,
        'old', old_row,
        'indices', _indices,
        'tg_op', TG_OP,
        'table', TG_TABLE_NAME,
        'schema', TG_TABLE_SCHEMA
    );

    -- Notify/Listen updates occur asynchronously,
    -- so this doesn't block the Postgres trigger procedure.
    PERFORM PG_NOTIFY(channel, notification::TEXT);

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""
        assert CREATE_TRIGGER_TEMPLATE == expected

    def test_trigger_primary_key_function(self, connection):
        tables = {
            "book": ["isbn"],
            "publisher": ["id"],
            "book_language": ["id"],
            "author": ["id"],
            "language": ["id"],
            "subject": ["id"],
            "city": ["id"],
            "country": ["id"],
            "continent": ["id"],
        }
        pg_base = Base(connection.engine.url.database)
        for table_name, primary_keys in tables.items():
            query = (
                f"SELECT ARRAY_AGG(attname) "
                f"FROM pg_index "
                f"JOIN pg_attribute ON attrelid = indrelid AND attnum = ANY(indkey) "  # noqa E501
                f"WHERE indrelid = '{table_name}'::regclass AND indisprimary"
            )
            rows = pg_base.fetchall(sa.text(query))[0]
            assert list(rows)[0] == primary_keys

    def test_trigger_foreign_key_function(self, connection):
        tables = {
            "book": ["publisher_id", "buyer_id", "seller_id"],
            "publisher": None,
            "book_language": ["book_isbn", "language_id"],
            "author": ["city_id"],
            "language": None,
            "subject": None,
            "city": ["country_id"],
            "country": ["continent_id"],
            "continent": None,
        }
        pg_base = Base(connection.engine.url.database)
        for table_name, foreign_keys in tables.items():
            query = (
                f"SELECT ARRAY_AGG(column_name::TEXT) FROM information_schema.key_column_usage "  # noqa E501
                f"WHERE constraint_catalog=current_catalog AND "
                f"table_name='{table_name}' AND position_in_unique_constraint NOTNULL "  # noqa E501
            )
            rows = pg_base.fetchall(sa.text(query))[0]
            if rows[0]:
                assert sorted(rows[0]) == sorted(foreign_keys)
            else:
                assert rows[0] == foreign_keys
