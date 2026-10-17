"""Check actual sink output with the official MCAP reader and CRC validation."""
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
    work = ROOT / "_interop/stream"
    work.mkdir(parents=True, exist_ok=True)
    subprocess.run([os.environ.get("MOON", "moon"), "build", "--target", "js"], cwd=ROOT, check=True, capture_output=True)
    cli = ROOT / "_build/js/debug/build/cmd/stream/stream.js"
    rows = []
    for mode in ("none", "lz4", "zstd"):
        output = work / f"{mode}.mcap"
        output.unlink(missing_ok=True)
        command = [os.environ.get("NODE", "node"), str(cli), str(output), mode]
        run = subprocess.run(command, cwd=ROOT, check=True, capture_output=True, text=True, encoding="utf-8")
        data = output.read_bytes()
        list(StreamReader(io.BytesIO(data), validate_crcs=True).records)
        reader = make_reader(io.BytesIO(data), validate_crcs=True)
        messages = list(reader.iter_messages())
        assert len(messages) == 1000
        assert [m.log_time for _, _, m in messages] == list(range(1000))
        assert all(m.data == b"x" * 1024 and c.topic == "/stream" for _, c, m in messages)
        assert reader.get_summary().statistics.message_count == 1000
        assert len(list(reader.iter_metadata())) == 1
        report = json.loads(run.stdout)
        assert report["peak_buffered_bytes"] <= 16384
        assert report["retained_summary_bytes"] < 8 * 1024 * 1024
        assert int(report["bytes_written"]) == len(data)
        blocked = subprocess.run(command, cwd=ROOT, capture_output=True)
        assert blocked.returncode != 0 and output.read_bytes() == data
        rows.append({"compression": mode, "result": "passed", **report})
        print(f"Streaming {mode}: 1000 messages, CRCs/indexes and exclusive output PASS")
    (work / "report.json").write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
