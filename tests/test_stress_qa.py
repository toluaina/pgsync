"""Opt-in smoke run of the data integrity stress harness.

Set PGSYNC_STRESS=1 to enable, e.g. in a nightly CI job:

    PGSYNC_STRESS=1 pytest tests/test_stress_qa.py -v

The harness exercises the full pipeline end to end: temporary database,
bootstrap, concurrent traffic against nested entities, polling sync,
stop-the-world verification against independent SQL, and teardown.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

from pgsync.settings import IS_MYSQL_COMPAT

REPO: Path = Path(__file__).resolve().parents[1]


@pytest.mark.skipif(
    not os.environ.get("PGSYNC_STRESS"),
    reason="set PGSYNC_STRESS=1 to run the stress harness",
)
@pytest.mark.skipif(
    IS_MYSQL_COMPAT,
    reason="Skipped because IS_MYSQL_COMPAT env var is set",
)
def test_stress_harness_smoke():
    result = subprocess.run(
        [
            sys.executable,
            str(REPO / "qa" / "stress_harness.py"),
            "--cycles",
            "1",
            "--duration",
            "4",
            "--writers",
            "3",
            "--restart-sync",
        ],
        capture_output=True,
        text=True,
        cwd=REPO,
        timeout=600,
    )
    assert result.returncode == 0, (
        result.stdout[-4000:] + result.stderr[-4000:]
    )
    assert "no data loss" in result.stdout
