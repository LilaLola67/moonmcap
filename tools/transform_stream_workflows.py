"""Disk-backed transform against an independent stable sampling/clock oracle."""
import io
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "_interop_deps"))
from mcap.writer import Writer, CompressionType
from mcap.reader import NonSeekingReader, make_reader
from mcap.stream_reader import StreamReader


def main():
    work = ROOT / "_interop/transform-stream"
    temp = work / "runs"
    temp.mkdir(parents=True, exist_ok=True)
    subprocess.run([os.environ.get("MOON", "moon"), "build", "--target", "js"], cwd=ROOT, check=True, capture_output=True)
    cli = ROOT / "_build/js/debug/build/cmd/mcap/mcap.js"
    rows = []

    def command(*args):
        return subprocess.run([os.environ.get("NODE", "node"), str(cli), *map(str, args)], cwd=ROOT, capture_output=True,
                              text=True, encoding="utf-8", timeout=120)

    for mode in CompressionType:
        stream = io.BytesIO()
        writer = Writer(stream, compression=mode, chunk_size=2048, enable_crcs=True, enable_data_crcs=True)
        writer.start()
        # Same content but separate channel IDs must retain independent sampling.
        channels = [writer.register_channel("/same", "raw", 0) for _ in range(3)]
        for i in range(180):
            time = i * 37 % 83
            writer.add_message(channels[i % 3], time, bytes([i])*256, time+2, i)
        writer.add_attachment(4, 6, "keep", "raw", b"calibration")
        writer.add_attachment(5, 8, "drop", "raw", b"unused")
        writer.add_metadata("keep", {"k": "v"})
        writer.add_metadata("drop", {"k": "no"})
        writer.finish()
        source = work / f"{mode.name}.mcap"
        source.write_bytes(stream.getvalue())
        physical = list(NonSeekingReader(io.BytesIO(stream.getvalue())).iter_messages(log_time_order=False))
        ordered = sorted(physical, key=lambda row: row[2].log_time)
        sampled, buckets = [], {}
        for _, c, m in ordered:
            bucket = m.log_time // 10
            if buckets.get(c.id) == bucket:
                continue
            buckets[c.id] = bucket
            sampled.append((c, m))
        for descending in (False, True):
            output = work / f"{mode.name}-{descending}.mcap"
            output.unlink(missing_ok=True)
            args = ["stream-transform", source, "--temp-dir", temp, "--output", output, "--compression", "zstd",
                    "--sample-period", 10, "--offset", 2, "--limit", 7, "--align-start", 1000,
                    "--rename", "/same=/renamed", "--attachment-name", "keep", "--metadata-name", "keep",
                    "--run-bytes", 4096, "--merge-bytes", 4096, "--fan-in", 2]
            if descending:
                args.append("--descending")
            run = command(*args)
            assert run.returncode == 0, run.stderr
            report = json.loads(run.stdout)
            expected = (list(reversed(sampled)) if descending else sampled)[2:9]
            delta = 1000 - min(m.log_time for _, m in expected)
            data = output.read_bytes()
            list(StreamReader(io.BytesIO(data), validate_crcs=True).records)
            actual = list(NonSeekingReader(io.BytesIO(data), validate_crcs=True).iter_messages(log_time_order=False))
            assert [(c.id, c.topic, m.sequence, m.log_time, m.publish_time, m.data) for _, c, m in actual] == [
                (c.id, "/renamed", m.sequence, m.log_time+delta, m.publish_time+delta, m.data) for c, m in expected]
            reader = make_reader(io.BytesIO(data), validate_crcs=True)
            attachments = list(reader.iter_attachments())
            assert len(attachments) == 1 and attachments[0].log_time == 6+delta and attachments[0].create_time == 4+delta
            assert [m.name for m in reader.iter_metadata()] == ["keep"]
            assert report["output_messages"] == 7 and report["sampled_messages"] == len(sampled)
            assert not list(temp.iterdir())
            old = output.read_bytes()
            assert command(*args).returncode != 0 and output.read_bytes() == old
            rows.append({"case": f"{mode.name}-{descending}", "result": "passed", **report})
        ndjson = command("query", source, "--format", "ndjson", "--limit", 11)
        assert ndjson.returncode == 0, ndjson.stderr
        records = [json.loads(line) for line in ndjson.stdout.splitlines()]
        assert len(records) == 11 and all(m["topic"] == "/same" for m in records)
        rows.append({"case": f"{mode.name}-ndjson", "result": "passed"})
        failed = work / "never-created.mcap"
        failed.unlink(missing_ok=True)
        run = command("stream-transform", source, "--temp-dir", temp, "--output", failed, "--spool-bytes", 100)
        assert run.returncode != 0 and not failed.exists() and not list(temp.iterdir())
        assert not list(work.glob(".*.part"))
        rows.append({"case": f"{mode.name}-budget-cleanup", "result": "passed"})
    (work / "report.json").write_text(json.dumps(rows, indent=2)+"\n", encoding="utf-8")
    print(f"Streaming transformation workflows: {len(rows)} passed")


if __name__ == "__main__":
    main()
