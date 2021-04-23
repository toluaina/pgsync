"""PGSync Trigger template."""
from .constants import FOREIGN_KEY_VIEW, PRIMARY_KEY_VIEW, TRIGGER_FUNC

CREATE_TRIGGER_TEMPLATE = f"""
CREATE OR REPLACE FUNCTION {TRIGGER_FUNC}() RETURNS TRIGGER AS $$
DECLARE
  channel TEXT;
  old_row JSON;
  new_row JSON;
  notification JSON;
  xmin BIGINT;

  primary_keys TEXT [] := (
      SELECT primary_keys
      FROM {PRIMARY_KEY_VIEW}
      WHERE table_name = TG_TABLE_NAME
  );
  foreign_keys TEXT [] := (
      SELECT foreign_keys
      FROM {FOREIGN_KEY_VIEW}
      WHERE table_name = TG_TABLE_NAME
  );

BEGIN
    -- database is also the channel name.
    channel := CURRENT_DATABASE();

    IF TG_OP = 'DELETE' THEN
        old_row = ROW_TO_JSON(OLD);
        old_row := (
            SELECT JSONB_OBJECT_AGG(key, value)
            FROM JSON_EACH(old_row)
            WHERE key = ANY(primary_keys)
        );
        xmin := OLD.xmin;
    ELSE
        IF TG_OP <> 'TRUNCATE' THEN
            new_row = ROW_TO_JSON(NEW);
            new_row := (
                SELECT JSONB_OBJECT_AGG(key, value)
                FROM JSON_EACH(new_row)
                WHERE key = ANY(primary_keys || foreign_keys)
            );
            IF TG_OP = 'UPDATE' THEN
                old_row = ROW_TO_JSON(OLD);
                old_row := (
                    SELECT JSONB_OBJECT_AGG(key, value)
                    FROM JSON_EACH(old_row)
                    WHERE key = ANY(primary_keys || foreign_keys)
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
