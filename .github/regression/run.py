"""Runs DuckDB's regression runner and fails when any benchmark regresses in two runs in a row"""

import argparse
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

RUNNER = Path(__file__).resolve().parents[2] / "duckdb" / "scripts" / "regression" / "test_runner.py"
# With verbose output the runner ends each confirmed verdict with the outcome
REGRESSED = re.compile(r"^confirm: (\S+): .*\| regression$", re.MULTILINE)
COLOR = re.compile(r"\x1b\[[0-9;]*m")


def run(passthrough, benchmarks):
    command = [sys.executable, str(RUNNER), *passthrough, "--verbose", "--benchmarks", benchmarks]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    output = []
    for line in process.stdout:
        print(line, end="", flush=True)
        output.append(line)
    code = process.wait()
    return code, set(REGRESSED.findall(COLOR.sub("", "".join(output))))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--benchmarks", required=True)
    known, passthrough = parser.parse_known_args()

    # The runner itself fails only on errors and on the geometric mean
    code, regressed = run(passthrough, known.benchmarks)
    if code != 0 or not regressed:
        return code

    # Runner noise rarely hits the same benchmark twice, so only a repeated regression fails
    print("Rerunning the regressed benchmarks " + ", ".join(sorted(regressed)), flush=True)
    with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False) as rerun:
        rerun.write("\n".join(sorted(regressed)) + "\n")
    try:
        code, again = run(passthrough, rerun.name)
    finally:
        os.unlink(rerun.name)
    if code != 0:
        return code
    repeated = regressed & again
    for benchmark in sorted(repeated):
        print(f"::error::{benchmark} is 10% or more slower in two runs in a row", flush=True)
    return 1 if repeated else 0


if __name__ == "__main__":
    sys.exit(main())
