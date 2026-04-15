"""
Reproduce the two bugs (advisory lock leak, Redis meta overwrite)
using the OLD logic inline, then verify the NEW code fixes them.

Run:  python -m pytest tests/test_before_after.py -xvs
"""

import json

import pytest
import sqlalchemy as sa
from redis import Redis

from pgsync.base import Base
from pgsync.redisqueue import RedisQueue
from pgsync.settings import IS_MYSQL_COMPAT
from pgsync.urls import get_redis_url


# ── Advisory lock ────────────────────────────────────────────────────


@pytest.fixture
def pg_base(connection):
    return Base(connection.engine.url.database)


@pytest.mark.skipif(
    IS_MYSQL_COMPAT,
    reason="Skipped because IS_MYSQL_COMPAT env var is set",
)
@pytest.mark.usefixtures("table_creator")
class TestAdvisoryLockBeforeAfter:
    """Show the lock leak with old code and its absence with the fix."""

    def _count_advisory_locks(self, connection, slot):
        row = connection.execute(
            sa.text(
                "SELECT COUNT(*) FROM pg_locks "
                "WHERE locktype = 'advisory' AND granted "
                "AND objid = hashtext(:slot)::int"
            ).bindparams(slot=slot)
        ).fetchone()
        return row[0]

    def test_old_code_leaks_lock(self, connection, pg_base):
        """Simulate the OLD behaviour: lock and unlock on separate connections."""
        slot = "leak_demo"
        key = pg_base.advisory_key(slot)

        # -- acquire on connection A --
        conn_a = pg_base.engine.connect()
        conn_a.execute(
            sa.text("SELECT PG_TRY_ADVISORY_LOCK(:key)").bindparams(key=key)
        )

        # -- "unlock" on connection B  (what the old code did) --
        conn_b = pg_base.engine.connect()
        row = conn_b.execute(
            sa.text("SELECT PG_ADVISORY_UNLOCK(:key)").bindparams(key=key)
        ).fetchone()

        # unlock returns FALSE because conn_b never held the lock
        assert row[0] is False

        # lock is STILL held (leaked on conn_a)
        assert self._count_advisory_locks(connection, slot) == 1

        # cleanup
        conn_a.execute(
            sa.text("SELECT PG_ADVISORY_UNLOCK(:key)").bindparams(key=key)
        )
        conn_a.close()
        conn_b.close()

        assert self._count_advisory_locks(connection, slot) == 0

    def test_new_code_no_leak(self, connection, pg_base):
        """The fixed advisory_lock context manager holds a single connection."""
        slot = "no_leak_demo"

        with pg_base.advisory_lock(slot):
            # lock is held
            assert self._count_advisory_locks(connection, slot) == 1

        # lock is released - no leak
        assert self._count_advisory_locks(connection, slot) == 0


# ── Redis meta ───────────────────────────────────────────────────────


@pytest.fixture
def redis_db():
    url = get_redis_url()
    return Redis.from_url(url)


@pytest.fixture
def meta_key():
    return "queue:before_after_demo:meta"


class TestRedisMetaBeforeAfter:
    """Show the overwrite with old code and field independence with the fix."""

    def test_old_code_overwrites_fields(self, redis_db, meta_key):
        """Simulate the OLD behaviour: plain SET clobbers the previous value."""
        redis_db.delete(meta_key)

        # Checkpoint setter writes:
        redis_db.set(meta_key, json.dumps({"checkpoint": 42}))

        # Status thread writes moments later:
        redis_db.set(meta_key, json.dumps({"txid_current": 100}))

        # Checkpoint is GONE
        stored = json.loads(redis_db.get(meta_key))
        assert "checkpoint" not in stored  # <-- the bug
        assert stored == {"txid_current": 100}

        redis_db.delete(meta_key)

    def test_new_code_preserves_both(self, redis_db, meta_key):
        """The fixed HSET-based set_meta preserves independent fields."""
        redis_db.delete(meta_key)

        queue = RedisQueue("before_after_demo")

        queue.set_meta({"checkpoint": 42})
        queue.set_meta({"txid_current": 100})

        meta = queue.get_meta()
        assert meta["checkpoint"] == 42
        assert meta["txid_current"] == 100

        queue.delete()
