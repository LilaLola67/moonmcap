"""Disk pages versus physical-order reference; corruption, budgets and cleanup."""
import hashlib
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
from mcap.writer import Writer, CompressionType, IndexType
from mcap.reader import NonSeekingReader


def main():
    work = ROOT / "_interop/disk-index"
    temporary = work / "temporary"
    temporary.mkdir(parents=True, exist_ok=True)
    subprocess.run([os.environ.get("MOON", "moon"), "build", "--target", "js"], cwd=ROOT, check=True, capture_output=True)
    cli = ROOT / "_build/js/debug/build/cmd/paged/paged.js"
    rows = []
    def command(*args):
        return subprocess.run([os.environ.get("NODE", "node"), str(cli), *map(str, args)], cwd=ROOT, capture_output=True, text=True, encoding="utf-8", timeout=180)
    for mode in CompressionType:
        buffer = io.BytesIO()
        writer = Writer(buffer, compression=mode, chunk_size=2048, index_types=IndexType.NONE, enable_data_crcs=True)
        writer.start()
        channels = [writer.register_channel(f"/topic/{i}", "raw", 0) for i in range(5)]
        rng = random.Random(20261026)
        for i in range(2048):
            writer.add_message(channels[i % 5], rng.randrange(37), bytes([i % 251]) * 31, i, i)
        writer.finish()
        source = work / f"{mode.name}.mcap"
        index = work / f"{mode.name}.midx2"
        source.write_bytes(buffer.getvalue()); index.unlink(missing_ok=True)
        oracle = [(i, c.topic, m) for i, (_, c, m) in enumerate(NonSeekingReader(io.BytesIO(buffer.getvalue())).iter_messages(log_time_order=False))]
        result = command("build", source, index, temporary, "--page-entries", 17, "--run-bytes", 4096, "--fan-in", 8)
        assert result.returncode == 0, result.stderr
        built = json.loads(result.stdout)
        assert built["entries"] == "2048" and not list(temporary.iterdir())
        result = command("check", source, index, "--cache-pages", 1)
        assert result.returncode == 0, result.stderr
        for descending in (False, True):
            for topic in (None, "/topic/2"):
                extra = ["--start", 3, "--end", 27, "--offset", 11, "--limit", 31, "--cache-pages", 2]
                if descending: extra.append("--descending")
                if topic: extra += ["--topic", topic]
                result = command("query", source, index, *extra)
                assert result.returncode == 0, result.stderr
                report = json.loads(result.stdout)
                expected = sorted((r for r in oracle if 3 <= r[2].log_time <= 27 and (topic is None or r[1] == topic)), key=lambda r: (r[2].log_time, r[0]), reverse=descending)[11:42]
                assert [(r["topic"], r["sequence"], int(r["log_time"]), bytes.fromhex(r["data_hex"])) for r in report["messages"]] == [(t, m.sequence, m.log_time, m.data) for _, t, m in expected]
                assert report["index"]["peak_cached_pages"] <= 2 and report["index"]["content_verified"]
        original = index.read_bytes()
        start = 16 + struct.unpack_from("<I", original, 8)[0]
        span = 17 * 36 + 4
        for damage in ("header", "page", "order", "truncation"):
            bad = bytearray(original)
            if damage == "order": bad[start:start + span*2] = bad[start+span:start+span*2] + bad[start:start+span]
            elif damage == "truncation": bad = bad[:-1]
            else: bad[12 if damage == "header" else start] ^= 1
            index.write_bytes(bad)
            assert command("query", source, index, "--limit", 1).returncode == 2
        index.write_bytes(original)
        failed = work / f"{mode.name}-budget.midx2"; failed.unlink(missing_ok=True)
        result = command("build", source, failed, temporary, "--spool-bytes", 512)
        assert result.returncode == 2 and not failed.exists() and not list(temporary.iterdir())
        assert hashlib.sha256(source.read_bytes()).digest() == hashlib.sha256(buffer.getvalue()).digest()
        rows.append({"compression": mode.name, "entries": 2048, "index_bytes": len(original), "index_sha256": hashlib.sha256(original).hexdigest(), "result": "passed", "build": built})
    (work / "report.json").write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")
    print("Disk index workflows: 3 compression matrices passed")


if __name__ == "__main__": main()
