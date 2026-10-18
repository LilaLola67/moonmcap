"""Verify actual async host I/O, event-loop progress and rotated file contents."""
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
    work = ROOT / "_interop/async"
    work.mkdir(parents=True, exist_ok=True)
    subprocess.run([os.environ.get("MOON", "moon"), "build", "--target", "js"], cwd=ROOT, check=True, capture_output=True)
    cli = ROOT / "_build/js/debug/build/cmd/async/async.js"

    def run(*args):
        p = subprocess.run([os.environ.get("NODE", "node"), str(cli), *map(str, args)], cwd=ROOT, capture_output=True, text=True, encoding="utf-8")
        assert p.returncode == 0, p.stderr
        return json.loads(p.stdout)

    def messages(path):
        data = path.read_bytes()
        list(StreamReader(io.BytesIO(data), validate_crcs=True).records)
        reader = make_reader(io.BytesIO(data), validate_crcs=True)
        values = list(reader.iter_messages())
        assert reader.get_summary().statistics.message_count == len(values)
        return values

    rows = []
    for mode in ("none", "lz4", "zstd"):
        output = work / f"record-{mode}.mcap"
        output.unlink(missing_ok=True)
        report = run("record", output, mode)
        assert [m.sequence for _, _, m in messages(output)] == list(range(1000))
        assert report["event_loop_ticks"] > 0 and report["peak_queued_bytes"] <= 32 * 1024 * 1024
        rows.append({"case": "record-" + mode, "result": "passed", **report})
    query = run("query", ROOT / "examples/files/zstd-summary.mcap", "/imu", "20000000000", "30000000000")
    assert [int(m["log_time"]) // 10**9 for m in query["messages"]] == [21, 23, 25, 27, 29]
    rows.append({"case": "async-indexed-query", "result": "passed"})
    prefix = work / "segment"
    for i in range(4):
        (work / f"segment.{i}.mcap").unlink(missing_ok=True)
    report = run("rotate", prefix, "zstd")
    assert [int(s["messages"]) for s in report["segments"]] == [32, 32, 32, 4]
    combined = [m.sequence for i in range(4) for _, _, m in messages(work / f"segment.{i}.mcap")]
    assert combined == list(range(100))
    rows.append({"case": "rotated-files", "result": "passed", **report})
    (work / "report.json").write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")
    print(f"Async PASS: {len(rows)} real I/O, event-loop and rotation workflows")


if __name__ == "__main__":
    main()
