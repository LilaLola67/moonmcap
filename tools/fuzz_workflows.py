"""Fixed-seed official-file differential queries and deterministic malformed inputs."""
import io
import json
import os
from pathlib import Path
import random
import struct
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "_interop_deps"))
from mcap.writer import Writer, CompressionType
from mcap.reader import NonSeekingReader


def main():
    work = ROOT / "_interop/fuzz"
    work.mkdir(parents=True, exist_ok=True)
    subprocess.run([os.environ.get("MOON", "moon"), "build", "--target", "js"], cwd=ROOT, check=True, capture_output=True)
    cli = ROOT / "_build/js/debug/build/cmd/mcap/mcap.js"
    rows = []

    def command(*args):
        return subprocess.run([os.environ.get("NODE", "node"), str(cli), *map(str, args)], cwd=ROOT,
                              capture_output=True, text=True, encoding="utf-8", timeout=30)

    for seed in range(12):
        rng = random.Random(20261018 + seed)
        buffer = io.BytesIO()
        writer = Writer(buffer, compression=list(CompressionType)[seed % 3], chunk_size=rng.randrange(100, 900),
                        enable_crcs=True, enable_data_crcs=True)
        writer.start()
        channels = [writer.register_channel(topic, "raw", 0) for topic in ("/a", "/a", "/b")]
        for i in range(80):
            timestamp = (2**64-1) if i % 29 == 0 else rng.randrange(32)
            writer.add_message(rng.choice(channels), timestamp, rng.randbytes(rng.randrange(1, 80)), timestamp, i)
        writer.finish()
        data = buffer.getvalue()
        source = work / f"seed-{seed}.mcap"
        source.write_bytes(data)
        oracle = list(NonSeekingReader(io.BytesIO(data), validate_crcs=True).iter_messages(log_time_order=False))
        for query in range(8):
            start, end = sorted([rng.randrange(32), rng.randrange(32)])
            if query == 7:
                start, end = 0, 2**64-1
            topic = rng.choice([None, "/a", "/b", "/missing"])
            descending = bool(query % 2)
            offset, limit = rng.randrange(8), rng.randrange(1, 18)
            expected = [(c, m) for _, c, m in oracle if start <= m.log_time <= end and (topic is None or c.topic == topic)]
            # Existing API reverses the complete (time, physical position) order.
            # Sequence equals physical position in this fixture, regardless of channel.
            expected.sort(key=lambda pair: (pair[1].log_time, pair[1].sequence), reverse=descending)
            expected = expected[offset:offset+limit]
            args = ["query", source, "--start", start, "--end", end, "--offset", offset, "--limit", limit]
            if topic:
                args.extend(["--topic", topic])
            if descending:
                args.append("--descending")
            run = command(*args)
            assert run.returncode == 0, run.stderr
            actual = json.loads(run.stdout)["messages"]
            assert [(m["channel_id"], int(m["log_time"]), m["sequence"], m["data_hex"]) for m in actual] == [
                (m.channel_id, m.log_time, m.sequence, m.data.hex()) for _, m in expected], (seed, query, actual)
            rows.append({"seed": seed, "query": query, "result": "passed"})
        # CRC-targeted mutation is guaranteed invalid; arbitrary flips could remain legal.
        position = 8
        chunks = []
        while position < len(data) - 8:
            opcode, size = struct.unpack_from("<BQ", data, position)
            if opcode == 6:
                chunks.append(position)
            position += 9 + size
        damaged = bytearray(data)
        damaged[rng.choice(chunks) + 9 + 24] ^= 1
        broken = work / f"seed-{seed}-bad.mcap"
        broken.write_bytes(damaged)
        assert command("validate", broken).returncode == 1
        rows.append({"seed": seed, "mutation": "chunk-crc", "result": "passed"})
    (work / "report.json").write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")
    print(f"Fixed-seed differential workflows: {len(rows)} passed")


if __name__ == "__main__":
    main()
