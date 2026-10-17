"""Verify batch transformations through the public CLI and official reader."""
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
    work = ROOT / "_interop/batch"
    work.mkdir(parents=True, exist_ok=True)
    subprocess.run([os.environ.get("MOON", "moon"), "build", "--target", "js"], cwd=ROOT, check=True, capture_output=True)
    cli = ROOT / "_build/js/debug/build/cmd/mcap/mcap.js"
    source = ROOT / "examples/files/zstd-summary.mcap"
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    rows = []
    for mode in ("none", "lz4", "zstd"):
        output = work / f"mapped-{mode}.mcap"
        output.unlink(missing_ok=True)
        args = ["transform", str(source), "--output", str(output), "--topic", "/imu",
                "--start", "20000000000", "--end", "30000000000", "--sample-period", "4000000000",
                "--sample-origin", "20000000000", "--align-start", "100000000000",
                "--rename", "/imu=/sensors/imu", "--attachment-name", "calibration.json",
                "--attachment-media-type", "application/json", "--metadata-name", "run",
                "--metadata-match", "source=synthetic-reference", "--compression", mode]
        run = subprocess.run([os.environ.get("NODE", "node"), str(cli), *args], cwd=ROOT,
                             check=True, capture_output=True, text=True, encoding="utf-8")
        data = output.read_bytes()
        list(StreamReader(io.BytesIO(data), validate_crcs=True).records)
        reader = make_reader(io.BytesIO(data), validate_crcs=True)
        messages = list(reader.iter_messages())
        assert [m.log_time for _, _, m in messages] == [100_000_000_000, 104_000_000_000, 108_000_000_000]
        assert all(c.topic == "/sensors/imu" for _, c, _ in messages)
        assert [json.loads(m.data)["sequence"] for _, _, m in messages] == [20, 24, 28]
        assert reader.get_summary().statistics.message_count == 3
        attachments = list(reader.iter_attachments())
        assert len(attachments) == 1 and attachments[0].log_time == 79_000_000_002
        assert attachments[0].create_time == 79_000_000_001
        assert list(reader.iter_metadata())[0].metadata == {"source": "synthetic-reference"}
        rows.append({"compression": mode, "result": "passed", "messages": 3})
        print(f"Batch {mode}: mapping/sampling/alignment/attachment/metadata PASS")
    failed = work / "must-not-exist.mcap"
    failed.unlink(missing_ok=True)
    run = subprocess.run([os.environ.get("NODE", "node"), str(cli), "transform", str(source),
                          "--output", str(failed), "--rename", "/missing=/new"], cwd=ROOT, capture_output=True)
    assert run.returncode == 2 and not failed.exists()
    assert hashlib.sha256(source.read_bytes()).hexdigest() == digest
    rows.append({"case": "invalid-mapping-preserves-input", "result": "passed"})
    (work / "report.json").write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
