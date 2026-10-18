"""Cross-file queries against physical-order independent reference, without merged output."""
import hashlib
import io
import json
import os
from pathlib import Path
import random
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "_interop_deps"))
from mcap.writer import Writer, CompressionType
from mcap.reader import NonSeekingReader


def main():
    work = ROOT / "_interop/dataset"
    work.mkdir(parents=True, exist_ok=True)
    subprocess.run([os.environ.get("MOON", "moon"), "build", "--target", "js"], cwd=ROOT, check=True, capture_output=True)
    cli = ROOT / "_build/js/debug/build/cmd/mcap/mcap.js"
    sources, oracle = [], []
    for source_index, mode in enumerate(CompressionType):
        stream = io.BytesIO()
        writer = Writer(stream, compression=mode, chunk_size=512, enable_crcs=True, enable_data_crcs=True)
        writer.start()
        # Conflicting local IDs: same IDs, differing schema content and topic.
        schema = writer.register_schema("Sample", "raw", bytes([source_index]))
        channel = writer.register_channel("/shared" if source_index < 2 else "/other", "raw", schema)
        rng = random.Random(20261018+source_index)
        for i in range(90):
            time = 2**64-1 if i % 37 == 0 else rng.randrange(32)
            writer.add_message(channel, time, bytes([source_index, i]), time, i)
        writer.finish()
        source = work / f"{mode.name}.mcap"
        source.write_bytes(stream.getvalue())
        sources.append(source)
        for _, c, m in NonSeekingReader(io.BytesIO(stream.getvalue())).iter_messages(log_time_order=False):
            oracle.append((source_index, c.topic, m))
    hashes = [hashlib.sha256(p.read_bytes()).hexdigest() for p in sources]
    rows = []
    for descending in (False, True):
        for topic in (None, "/shared", "/other", "/missing"):
            for limit in (0, 13):
                expected = [r for r in oracle if topic is None or r[1] == topic]
                expected.sort(key=lambda r: (r[2].log_time, r[0], r[2].sequence), reverse=descending)
                expected = expected[7:7+limit]
                args = ["dataset-query", *map(str, sources), "--offset", "7", "--limit", str(limit), "--format", "ndjson"]
                if descending:
                    args.append("--descending")
                if topic:
                    args.extend(["--topic", topic])
                run = subprocess.run([os.environ.get("NODE", "node"), str(cli), *args], cwd=ROOT, capture_output=True, text=True, encoding="utf-8", timeout=30)
                assert run.returncode == 0, run.stderr
                actual = [json.loads(line) for line in run.stdout.splitlines()]
                assert [(r["source_index"], r["topic"], int(r["log_time"]), r["sequence"], r["data_hex"]) for r in actual] == [
                    (source, topic, m.log_time, m.sequence, m.data.hex()) for source, topic, m in expected]
                rows.append({"descending": descending, "topic": topic, "limit": limit, "result": "passed"})
    assert [hashlib.sha256(p.read_bytes()).hexdigest() for p in sources] == hashes
    (work / "report.json").write_text(json.dumps(rows, indent=2)+"\n", encoding="utf-8")
    print(f"Dataset workflows: {len(rows)} passed")


if __name__ == "__main__":
    main()
