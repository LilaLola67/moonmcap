"""End-to-end CLI checks including independent outputs and failure atomicity."""
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "_interop_deps"))
from mcap.reader import make_reader
from mcap.stream_reader import StreamReader


def main():
    work = ROOT / "_interop/cli"
    work.mkdir(parents=True, exist_ok=True)
    subprocess.run([os.environ.get("MOON", "moon"), "build", "--target", "js"], cwd=ROOT, check=True, capture_output=True)
    cli = ROOT / "_build/js/debug/build/cmd/mcap/mcap.js"
    rows = []

    def run(*args, success=True):
        result = subprocess.run([os.environ.get("NODE", "node"), str(cli), *map(str, args)],
                                cwd=ROOT, capture_output=True, text=True, encoding="utf-8")
        assert (result.returncode == 0) == success, result.stdout + result.stderr
        return result

    def messages(path):
        data = path.read_bytes()
        list(StreamReader(io.BytesIO(data), validate_crcs=True).records)
        reader = make_reader(io.BytesIO(data), validate_crcs=True)
        result = list(reader.iter_messages())
        assert reader.get_summary().statistics.message_count == len(result)
        return result

    source = ROOT / "examples/files/zstd-summary.mcap"
    info = json.loads(run("inspect", source).stdout)
    assert info["messages"] == 100 and info["attachments"] == 1
    rows.append("inspect")
    query = ["query", source, "--topic", "/imu", "--start", "20000000000", "--end", "30000000000"]
    report = json.loads(run(*query, "--descending", "--offset", "1", "--limit", "2").stdout)
    assert [m["log_time"] for m in report["messages"]] == ["27000000000", "25000000000"]
    assert not report["diagnostics"]["full_validation"]
    assert json.loads(run(*query, "--full").stdout)["diagnostics"]["full_validation"]
    rows.append("query-json-and-full")
    table = list(csv.DictReader(io.StringIO(run(*query, "--format", "csv").stdout)))
    assert [int(m["log_time"]) // 10**9 for m in table] == [21, 23, 25, 27, 29]
    rows.append("query-csv")
    for operation, expected in (("slice", 5), ("rewrite", 100), ("merge", 200), ("recover", 48)):
        output = work / (operation + " with space.mcap")
        output.unlink(missing_ok=True)
        inputs = [source] if operation != "recover" else [ROOT / "examples/files/truncated-zstd.mcap"]
        if operation == "merge":
            inputs.append(ROOT / "examples/files/lz4-no-summary.mcap")
        options = ["--topic", "/imu", "--start", "20000000000", "--end", "30000000000"] if operation == "slice" else []
        command = [operation, *inputs, "--output", output, "--compression", "lz4", "--chunk-messages", "3", *options]
        run(*command)
        actual = messages(output)
        assert len(actual) == expected
        if operation == "slice":
            assert [m.log_time // 10**9 for _, _, m in actual] == [21, 23, 25, 27, 29]
        digest = hashlib.sha256(output.read_bytes()).hexdigest()
        run(*command, success=False)
        assert hashlib.sha256(output.read_bytes()).hexdigest() == digest
        rows.append(operation + "-and-no-clobber")
    missing = work / "must-not-exist.mcap"
    missing.unlink(missing_ok=True)
    error = run("slice", source, "--output", missing, "--start", "9", "--end", "1", success=False)
    assert json.loads(error.stderr)["error"] and not missing.exists()
    assert not list(work.glob(".*.tmp"))
    rows.append("invalid-arguments-no-output")
    (work / "report.json").write_text(json.dumps({"result": "passed", "cases": rows}, indent=2) + "\n", encoding="utf-8")
    print(f"CLI PASS: {len(rows)} command/export/failure workflows")


if __name__ == "__main__":
    main()
