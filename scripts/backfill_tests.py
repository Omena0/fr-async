#!/usr/bin/env python3
"""Back-populate .test_data.json with metrics from the last N commits.

For each of the last N commits:
  1. Stash any uncommitted changes
  2. Checkout the commit
  3. Copy the current fr_async/test.py into that commit's tree
  4. Run the test suite and capture the score + metrics
  5. Store results under that commit hash in .test_data.json
  6. Restore the working tree
"""

import contextlib
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, UTC
from typing import Any, cast

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
TEST_FILE = os.path.join(REPO_ROOT, "fr_async", "test.py")
DATA_FILE = os.path.join(REPO_ROOT, ".test_data.json")
NUM_COMMITS = 20

KWARGS = dict[str, Any]


def run(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, capture_output=True, text=True, cwd=REPO_ROOT, check=False, **kwargs)


def get_last_commits(n: int) -> list[str]:
    result = run(["git", "log", "--oneline", f"-n{n}", "--format=%H"])
    return result.stdout.strip().splitlines()


def load_data() -> dict[str, Any]:
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE) as f:
            return cast("dict[str, Any]", json.load(f))
    return {"commits": {}}


def save_data(data: dict[str, Any]) -> None:
    tmp = f"{DATA_FILE}.tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, DATA_FILE)


def stash_changes() -> bool:
    result = run(["git", "diff", "--quiet"])
    if result.returncode == 0:
        return False  # no changes to stash
    run(["git", "stash", "push", "-m", "backfill-stash"])
    return True


def pop_stash(stashed: bool) -> None:
    if stashed:
        run(["git", "stash", "pop"])


def checkout(commit: str) -> None:
    run(["git", "checkout", commit])


def restore_main() -> None:
    run(["git", "checkout", "main"])


def run_tests() -> tuple[float, dict[str, Any], bool]:
    """Run the test suite and return (score, metrics_dict, passed)."""
    result = run([sys.executable, "-m", "fr_async.test"])
    output = result.stdout

    # Parse score
    score = 0.0
    for line in output.splitlines():
        if "General score:" in line:
            with contextlib.suppress(Exception):
                score = float(line.split("General score:")[1].split("%")[0].strip())
            break

    passed = "OVERALL: PASSED" in output

    # Parse metrics from the output (they show overhead in ms)
    metrics: dict[str, Any] = {}
    for line in output.splitlines():
        if "overhead=" in line and "scale=" in line:
            # Parse overhead and scale from the metrics line
            name = line.split(":")[0].strip()
            overhead_part = [p for p in line.split() if p.startswith("overhead=")]
            scale_part = [p for p in line.split() if p.startswith("scale=")]
            if overhead_part and scale_part:
                with contextlib.suppress(Exception):
                    overhead = float(overhead_part[0].split("=")[1].rstrip("ms"))
                    scale = float(scale_part[0].split("=")[1].rstrip("ms"))
                    metrics[name] = {
                        "value": overhead,
                        "target": 0.0,
                        "scale": scale,
                        "weight": 1.0,
                    }

    return score, metrics, passed


def main() -> None:
    commits = get_last_commits(NUM_COMMITS)
    data = load_data()
    stashed = stash_changes()

    # Save current test.py from main so we can copy it into each commit's tree
    current_test_path = os.path.join(REPO_ROOT, "fr_async", "test.py.current")

    try:
        shutil.copy2(TEST_FILE, current_test_path)

        for commit in commits:
            print(f"Processing {commit[:12]}...", flush=True)
            checkout(commit)

            # Copy the current test.py into this commit's tree
            shutil.copy2(current_test_path, TEST_FILE)

            try:
                score, metrics, passed = run_tests()
            except Exception as exc:
                print(f"  Failed: {exc}")
                score, metrics, passed = 0.0, {}, False

            entry = data["commits"].setdefault(commit, {"local": None, "gh_actions": None})
            entry["local"] = {
                "timestamp": datetime.now(UTC).isoformat(),
                "score": score,
                "passed": passed,
                "tests_passed": 40 if passed else 0,
                "total_tests": 40,
                "metrics": {
                    name: {
                        "value": m["value"],
                        "target": m["target"],
                        "scale": m["scale"],
                        "weight": m["weight"],
                    }
                    for name, m in metrics.items()
                },
            }

            print(f"  score={score:.3f}% passed={passed} metrics={len(metrics)}")

        save_data(data)
        print(f"\nBackfilled {len(commits)} commits into {DATA_FILE}")

    finally:
        # Clean up any temp file
        if os.path.exists(current_test_path):
            os.remove(current_test_path)
        restore_main()
        pop_stash(stashed)


if __name__ == "__main__":
    main()
