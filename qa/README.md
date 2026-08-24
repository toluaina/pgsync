# PGSync data integrity stress harness

A QA layer that answers one question: under sustained concurrent write
traffic, does the search index end up exactly matching the database?

The harness builds a temporary copy of the book example topology (deep
nested one_to_one chains, one_to_many objects, through tables and
scalar aggregations), runs pgsync in polling mode against it, and
hammers the database with concurrent writers while pgsync syncs. At the
end of each cycle it stops the world, drains the sync pipeline and
verifies Elasticsearch against the database using independent SQL, not
pgsync's own query builder:

- no data loss: every book in the database has a document in the index
- no stale documents: every document in the index has a live book
- field integrity: root fields and every nested entity match, including
  publisher, authors with their city, country and continent, scalar
  language and subject lists and the rating

## Traffic model

Each writer thread applies a weighted random operation mix in its own
transactions: inserts with nested children, root and nested updates,
deletes (with cascades through the through tables), author relinking,
through-table churn, deep nested moves (an author changing city shifts
city, country and continent in every one of their books), and
occasional single-transaction bulk loads. A dedicated writer holds a
transaction open across multiple pull cycles to exercise the
snapshot-xmin checkpointing and the pinned-checkpoint warning.

Unique-constraint conflicts and delete races between writers are
expected and counted, not failed.

## Usage

```bash
python qa/stress_harness.py --cycles 3 --duration 10 --writers 6
```

Options:

- `--cycles`: number of traffic and verify rounds (default 3)
- `--duration`: seconds of traffic per cycle (default 10)
- `--writers`: concurrent writer threads (default 6)
- `--think-time`: mean pause between writer operations (default 0.02s)
- `--pull-interval`: pause between pgsync polling pulls (default 0.25s)
- `--seed`: RNG seed for reproducible traffic (default 42)
- `--soak`: run cycles until interrupted; Ctrl+C stops traffic, runs a
  final verification and exits with its result
- `--restart-sync`: mid-cycle, kill the running Sync and start a fresh
  one while writers keep committing, proving that checkpoint recovery
  and the replication slot lose nothing across a crash and restart
- `--report PATH`: write a JSON report (config, per-cycle results,
  summary) for CI trending; on failure without `--report`, the full
  diff artifacts are written to `qa/.failure_<name>.json`
- `--keep`: keep the database, index and slot after the run

## Performance metrics

Each cycle records, and the end-of-run table shows:

- `ops` and `ops/s`: writer throughput during the traffic window
- `p95 ms`: writer operation latency (p50/p95/max in the JSON report)
- `backlog`: replication slot bytes accumulated when traffic stopped
- `drain s` and `sync/s`: how long pgsync took to replay the backlog
  and its document throughput while doing so
- `books`: database size at verification (always equal to the index
  document count on a passing cycle)

The summary line adds total docs synced, mean ops/s, index size on
disk and the pinned-checkpoint warning count.

## CI integration

An opt-in pytest wrapper runs a short cycle with the restart
simulation:

```bash
PGSYNC_STRESS=1 pytest tests/test_stress_qa.py -v
```

Everything the harness creates is temporary and uniquely named
(`qa_book_stress_<hex>`): the database, the index, the replication
slot, the checkpoint file and the schema JSON. On success it tears all
of it down; on failure (or with `--keep`) it leaves them in place for
inspection and exits nonzero.

## Notes

- The drain after each cycle replays the accumulated replication slot
  entries one interleaved group at a time, so drains dominate the wall
  clock. Size `--duration`, `--writers` and `--think-time` to the soak
  time you want.
- `Document failed to index ... result: not_found` errors from
  pgsync's search client are idempotence noise: a delete replayed for a
  document that is already gone (for example a book inserted and
  deleted between two pulls). The verifier is the arbiter of
  correctness, not that log line.
- Requires local Postgres, Elasticsearch and Redis as configured by
  `.env`.
