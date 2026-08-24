"""Tests for epoch-aware xid handling (32-bit xid wraparound)."""

import random
from unittest.mock import patch, PropertyMock

import sqlalchemy as sa

from pgsync.base import (
    Base,
    epoch_extended_xid,
    epoch_extended_xid_column,
    XID_MODULUS,
)


def compile_str(expression: sa.sql.elements.ColumnElement) -> str:
    return str(
        expression.compile(
            dialect=sa.dialects.postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )


class TestEpochExtendedXid:
    """Python-side reconstruction of 64-bit txids from raw 32-bit xids."""

    def test_epoch_zero_is_identity(self):
        # before the first wraparound raw xid and txid_current agree
        assert epoch_extended_xid(1000, txid_anchor=5000) == 1000

    def test_current_epoch(self):
        # anchor in epoch 2, raw xid below the anchor's raw counter
        anchor = 2 * XID_MODULUS + 5000
        assert epoch_extended_xid(1000, anchor) == 2 * XID_MODULUS + 1000

    def test_previous_epoch(self):
        # raw xid above the anchor's raw counter was assigned before
        # the last wraparound
        anchor = 2 * XID_MODULUS + 5000
        raw = XID_MODULUS - 10
        assert epoch_extended_xid(raw, anchor) == XID_MODULUS + raw

    def test_already_extended_is_passthrough(self):
        value = 3 * XID_MODULUS + 42
        assert epoch_extended_xid(value, txid_anchor=value) == value

    def test_ordering_across_wraparound(self):
        # a row written just before the wrap must sort below a row
        # written just after it
        anchor = XID_MODULUS + 100
        before_wrap = epoch_extended_xid(XID_MODULUS - 5, anchor)
        after_wrap = epoch_extended_xid(50, anchor)
        assert before_wrap < after_wrap

    def test_checkpoint_no_longer_regresses(self):
        # the on_publish checkpoint math: min(payload xmins, txid_current)
        # used to compare a raw 32-bit xmin against the 64-bit txid and
        # always picked the raw value after a wraparound
        txid_current = XID_MODULUS + 200
        payload_xmin = 150  # written after the wrap
        extended = epoch_extended_xid(payload_xmin, txid_current)
        checkpoint = min(extended, txid_current) - 1
        assert checkpoint == XID_MODULUS + 150 - 1


class TestEpochExtendedXidColumn:
    """SQL-side expression for the same reconstruction."""

    def test_no_anchor_falls_back_to_raw_cast(self):
        sql = compile_str(
            epoch_extended_xid_column(sa.column("xid"), txid_anchor=None)
        )
        assert "CAST" in sql
        assert "CASE" not in sql

    def test_epoch_zero_falls_back_to_raw_cast(self):
        sql = compile_str(
            epoch_extended_xid_column(sa.column("xid"), txid_anchor=5000)
        )
        assert "CASE" not in sql

    def test_nonzero_epoch_emits_case_expression(self):
        anchor = 2 * XID_MODULUS + 5000
        sql = compile_str(
            epoch_extended_xid_column(sa.column("xid"), txid_anchor=anchor)
        )
        assert "CASE" in sql
        # raw counter of the anchor
        assert "5000" in sql
        # current epoch offset
        assert str(2 * XID_MODULUS) in sql
        # previous epoch offset
        assert str(XID_MODULUS) in sql

    def test_filter_matches_post_wrap_rows(self):
        # checkpoint taken in epoch 1, row written after the wrap has a
        # small raw xmin: the raw comparison would exclude it, the
        # epoch-extended one must include it
        anchor = XID_MODULUS + 500  # txid_current
        txmin = XID_MODULUS + 100  # checkpoint
        expression = epoch_extended_xid_column(sa.column("xid"), anchor)
        sql = compile_str(expression >= txmin)
        assert str(txmin) in sql
        assert "CASE" in sql


class TestLogicalSlotTxminOnlyAnchor:
    """txmin-only slot reads must fetch an epoch anchor themselves."""

    def compile_slot_statement(self, statement) -> str:
        return str(
            statement.compile(
                dialect=sa.dialects.postgresql.dialect(),
                compile_kwargs={"literal_binds": True},
            )
        )

    def test_txmin_only_fetches_txid_current(self):
        base = Base.__new__(Base)  # no database connection needed
        anchor = 2 * XID_MODULUS + 5000
        with patch.object(
            Base, "txid_current", new_callable=PropertyMock
        ) as mock_txid:
            mock_txid.return_value = anchor
            statement = base._logical_slot_changes(
                "slot",
                sa.func.PG_LOGICAL_SLOT_PEEK_CHANGES,
                txmin=2 * XID_MODULUS + 100,
            )
        mock_txid.assert_called_once()
        assert "CASE" in self.compile_slot_statement(statement)

    def test_with_txmax_nothing_is_fetched(self):
        base = Base.__new__(Base)
        with patch.object(
            Base, "txid_current", new_callable=PropertyMock
        ) as mock_txid:
            statement = base._logical_slot_changes(
                "slot",
                sa.func.PG_LOGICAL_SLOT_PEEK_CHANGES,
                txmin=2 * XID_MODULUS + 100,
                txmax=2 * XID_MODULUS + 5000,
            )
        mock_txid.assert_not_called()
        assert "CASE" in self.compile_slot_statement(statement)

    def test_no_bounds_no_fetch_no_filter(self):
        base = Base.__new__(Base)
        with patch.object(
            Base, "txid_current", new_callable=PropertyMock
        ) as mock_txid:
            statement = base._logical_slot_changes(
                "slot",
                sa.func.PG_LOGICAL_SLOT_PEEK_CHANGES,
            )
        mock_txid.assert_not_called()
        assert "WHERE" not in self.compile_slot_statement(statement)


class TestWraparoundPullCycle:
    """Simulated pull cycles across the 2**32 wrap boundary.

    Rows are written at monotonically increasing 64-bit txids but only
    store the raw 32-bit value, exactly like table xmin. Each pull
    filters [checkpoint, txmax) and then advances the checkpoint, as
    Sync.pull does. The epoch-extended filter must sync every committed
    row exactly once; the pre-fix raw filter loses everything after the
    wrap.
    """

    def test_no_rows_missed_across_wraparound(self):
        rng = random.Random(7)
        txid: int = XID_MODULUS - 5_000  # just before the first wraparound
        rows: list = []  # (true_txid, raw_xmin)
        raw_synced: list = []
        extended_synced: list = []
        raw_checkpoint: int = txid
        extended_checkpoint: int = txid

        for _ in range(25):
            # transactions commit between pulls, crossing the boundary
            for _ in range(rng.randrange(50, 300)):
                txid += rng.randrange(1, 10)
                rows.append((txid, txid % XID_MODULUS))
            txmax: int = txid  # txid_current at pull time

            for true, raw in rows:
                if raw_checkpoint <= raw < txmax:
                    raw_synced.append(true)
            raw_checkpoint = txmax

            for true, raw in rows:
                extended: int = epoch_extended_xid(raw, txmax)
                if extended_checkpoint <= extended < txmax:
                    extended_synced.append(true)
            extended_checkpoint = txmax

        # rows at the final txmax are picked up by the next pull
        eligible: set = {
            true for true, _ in rows if true < extended_checkpoint
        }
        post_wrap: set = {true for true in eligible if true >= XID_MODULUS}

        assert post_wrap, "the simulation must cross the wrap boundary"
        # epoch-extended filter: nothing missed, nothing duplicated
        assert eligible - set(extended_synced) == set()
        assert len(extended_synced) == len(set(extended_synced))
        # control: the pre-fix raw filter misses the post-wrap rows
        assert post_wrap <= eligible - set(raw_synced)


class TestFrozenRowAliasing:
    """Documented limitation: frozen historical rows alias as over-sync.

    Freezing preserves a row's original raw xmin, and there is no
    SQL-visible frozen flag, so when the counter sweeps past a frozen
    row's historical raw xmin one epoch later, the row is spuriously
    re-included in the pull window. The guarantee tested here is the
    direction of the error: aliasing may cause redundant re-syncs
    (transform plugins must be idempotent) but can never cause a row,
    live or frozen, to be missed.
    """

    def test_aliasing_is_oversync_never_loss(self):
        rng = random.Random(11)
        # frozen rows created in epoch 0, raw xmins 1000..1499, never
        # modified again: with correct behavior they would match nothing
        frozen: list = list(range(1000, 1500, 7))

        txid: int = XID_MODULUS + 950  # epoch 1, nearing the frozen band
        checkpoint: int = txid
        live_rows: list = []
        live_synced: list = []
        spurious: set = set()

        for _ in range(30):
            for _ in range(rng.randrange(20, 80)):
                txid += rng.randrange(1, 5)
                live_rows.append((txid, txid % XID_MODULUS))
            txmax: int = txid

            for true, raw in live_rows:
                extended: int = epoch_extended_xid(raw, txmax)
                if checkpoint <= extended < txmax:
                    live_synced.append(true)
            for raw in frozen:
                extended = epoch_extended_xid(raw, txmax)
                if checkpoint <= extended < txmax:
                    spurious.add(raw)
            checkpoint = txmax

        # every eligible live row synced exactly once: aliasing frozen
        # rows never displace or block real changes
        eligible: set = {true for true, _ in live_rows if true < checkpoint}
        assert eligible - set(live_synced) == set()
        assert len(live_synced) == len(set(live_synced))

        # the limitation is real: exactly the frozen rows whose raw
        # xmin lies in the swept band get spuriously re-synced
        swept_lo: int = (XID_MODULUS + 950) % XID_MODULUS
        swept_hi: int = checkpoint % XID_MODULUS
        expected_spurious: set = {
            raw for raw in frozen if swept_lo <= raw < swept_hi
        }
        assert spurious == expected_spurious
        assert spurious, "the simulation must exercise the aliasing band"


class TestLateCommitCheckpoint:
    """Snapshot-xmin checkpointing survives late-committing transactions.

    Transactions get their xid at first write, not at commit. A pull
    that checkpoints at txid_current (the pre-fix behavior) skips any
    transaction that was still open during the pull and commits later.
    Checkpointing at the snapshot's oldest in-progress xid keeps such
    transactions above the checkpoint until they resolve.
    """

    def test_open_transactions_are_never_skipped(self):
        rng = random.Random(3)
        transactions: list = []  # (xid, start_tick, commit_tick)
        xid: int = 100

        # transactions acquire their xid at start_tick (first write)
        # and commit 0 to 3 ticks later
        for tick in range(20):
            for _ in range(rng.randrange(5, 15)):
                xid += 1
                transactions.append((xid, tick, tick + rng.randrange(0, 4)))
        last_tick: int = max(commit for _, _, commit in transactions)

        def run(checkpoint_strategy: str) -> set:
            synced: set = set()
            checkpoint: int = 0
            for tick in range(last_tick + 2):
                started: list = [
                    (x, start, commit)
                    for x, start, commit in transactions
                    if start <= tick
                ]
                if not started:
                    continue
                # the puller's own fresh txid sits above all data xids
                txmax: int = max(x for x, _, _ in started) + 1
                open_xids: list = [
                    x for x, _, commit in started if commit > tick
                ]
                snapshot_xmin: int = min(open_xids, default=txmax)
                # forward scan sees committed rows only
                visible: list = [
                    x for x, _, commit in started if commit <= tick
                ]
                for x in visible:
                    if checkpoint <= x < txmax:
                        synced.add(x)
                if checkpoint_strategy == "snapshot_xmin":
                    checkpoint = snapshot_xmin
                else:
                    checkpoint = txmax
            return synced

        all_xids: set = {x for x, _, _ in transactions}

        # snapshot-xmin checkpointing: every transaction synced
        assert run("snapshot_xmin") == all_xids

        # control (pre-fix): checkpointing at txmax loses transactions
        # that were open during a pull and committed after it
        missed: set = all_xids - run("txmax")
        assert missed, "the simulation must produce late commits"
        delayed: set = {
            x for x, start, commit in transactions if commit > start
        }
        assert missed <= delayed
