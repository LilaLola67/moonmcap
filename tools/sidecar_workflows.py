"""Persistent sidecars: SHA-256, unindexed inputs, independent queries and staleness."""
import hashlib
import io
import json
import os
from pathlib import Path
import random
import struct
import subprocess
import sys
import zlib

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "_interop_deps"))
from mcap.writer import Writer, CompressionType, IndexType
from mcap.reader import NonSeekingReader


def main():
    work = ROOT / "_interop/sidecar"
    work.mkdir(parents=True, exist_ok=True)
    subprocess.run([os.environ.get("MOON", "moon"), "build", "--target", "js"], cwd=ROOT, check=True, capture_output=True)
    cli = ROOT / "_build/js/debug/build/cmd/mcap/mcap.js"
    rows, inputs, indexes, all_messages = [], [], [], []

    def command(*args):
        return subprocess.run([os.environ.get("NODE", "node"), str(cli), *map(str, args)], cwd=ROOT,
                              capture_output=True, text=True, encoding="utf-8", timeout=60)

    shapes = [(mode.name, mode, True, False) for mode in CompressionType] + [
        ("sequential", CompressionType.NONE, False, False), ("indexed", CompressionType.ZSTD, True, True)]
    for source_number, (name, mode, chunked, indexed) in enumerate(shapes):
        buffer = io.BytesIO()
        settings = {} if indexed else dict(index_types=IndexType.NONE, repeat_channels=False, repeat_schemas=False, use_statistics=False, use_summary_offsets=False)
        writer = Writer(buffer, compression=mode, chunk_size=1024, use_chunking=chunked, enable_crcs=True, enable_data_crcs=True, **settings)
        writer.start()
        channel = writer.register_channel("/shared", "raw", 0)
        rng = random.Random(20261018+source_number)
        for i in range(180):
            time = 2**64-1 if i % 37 == 0 else rng.randrange(32)
            writer.add_message(channel, time, bytes([i])*128, time, i)
        writer.finish()
        baseline = buffer.getvalue()
        source, index = work / f"{name}.mcap", work / f"{name}.midx"
        source.write_bytes(baseline)
        index.unlink(missing_ok=True)
        build = command("index-build", source, "--output", index)
        assert build.returncode == 0, build.stderr
        built = json.loads(build.stdout)
        assert built["build_content_hashed"] and built["index"]["source_sha256"] == hashlib.sha256(baseline).hexdigest()
        assert built["index"]["entries"] == 180
        old_index = index.read_bytes()
        assert command("index-build", source, "--output", index).returncode != 0 and index.read_bytes() == old_index
        check = command("index-check", source, "--index", index)
        assert check.returncode == 0 and json.loads(check.stdout)["content_verified"] is True, check.stderr
        rows.append({"case": name+"-build-check", "result": "passed"})
        physical = [(c, m) for _, c, m in NonSeekingReader(io.BytesIO(baseline)).iter_messages(log_time_order=False)]
        all_messages.extend((source_number, c, m) for c, m in physical)
        for descending in (False, True):
            for topic in (None, "/shared", "/missing"):
                expected = [p for p in physical if topic is None or p[0].topic == topic]
                expected.sort(key=lambda p: (p[1].log_time, p[1].sequence), reverse=descending)
                expected = expected[3:20]
                args = ["index-query", source, "--index", index, "--offset", 3, "--limit", 17]
                if descending:
                    args.append("--descending")
                if topic:
                    args.extend(["--topic", topic])
                result = command(*args)
                assert result.returncode == 0, result.stderr
                report = json.loads(result.stdout)
                assert report["index"]["content_verified"]
                assert [(m["sequence"], int(m["log_time"]), m["data_hex"]) for m in report["messages"]] == [
                    (m.sequence, m.log_time, m.data.hex()) for _, m in expected]
                rows.append({"case": f"{name}-query-{descending}-{topic}", "result": "passed"})
        fast = command("index-query", source, "--index", index, "--fast-index", "--limit", 1)
        assert fast.returncode == 0, fast.stderr
        report = json.loads(fast.stdout)
        assert report["index"]["content_verified"] is False
        assert int(report["index"]["source_bytes_read"]) < len(baseline)
        rows.append({"case": name+"-fast-range", "result": "passed", "bytes_read": report["index"]["source_bytes_read"], "source_bytes": len(baseline)})
        damaged = bytearray(baseline)
        damaged[30] ^= 1
        source.write_bytes(damaged)  # Equal length; SHA-256 must still reject it.
        assert command("index-query", source, "--index", index).returncode != 0
        assert command("index-query", source, "--index", index, "--fast-index").returncode != 0
        source.write_bytes(baseline)
        for version in (0, 1, 2):
            broken = work / f"{name}-broken.midx"
            damaged_index = bytearray(old_index)
            if version == 1:
                struct.pack_into("<I", damaged_index, 8, 2)
                struct.pack_into("<I", damaged_index, len(damaged_index)-4, zlib.crc32(damaged_index[:-4]))
            elif version == 2:
                position = 8+4+8
                for _ in range(3):
                    size = struct.unpack_from("<I", damaged_index, position)[0]
                    position += 4+size
                position += 4  # Entry count precedes first entry's time.
                old_time = struct.unpack_from("<Q", damaged_index, position)[0]
                struct.pack_into("<Q", damaged_index, position, old_time ^ 1)
                struct.pack_into("<I", damaged_index, len(damaged_index)-4, zlib.crc32(damaged_index[:-4]))
            else:
                damaged_index[-1] ^= 1
            broken.write_bytes(damaged_index)
            assert command("index-check", source, "--index", broken).returncode != 0
        rows.append({"case": name+"-stale-and-corrupt", "result": "passed"})
        rebuilt = work / f"{name}-rebuilt.midx"
        rebuilt.unlink(missing_ok=True)
        result = command("index-rebuild", source, "--output", rebuilt)
        assert result.returncode == 0, result.stderr
        assert command("index-check", source, "--index", rebuilt, "--fast-index").returncode == 0
        inputs.append(source)
        indexes.append(rebuilt)
        rows.append({"case": name+"-rebuild", "result": "passed"})
    args = ["dataset-query", *inputs, "--format", "ndjson", "--descending", "--limit", 23, "--fast-index"]
    for index in indexes:
        args.extend(["--index", index])
    run = command(*args)
    assert run.returncode == 0, run.stderr
    actual = [json.loads(line) for line in run.stdout.splitlines()]
    expected = sorted(all_messages, key=lambda r: (r[2].log_time, r[0], r[2].sequence), reverse=True)[:23]
    assert [(r["source_index"], r["sequence"], int(r["log_time"]), r["data_hex"]) for r in actual] == [
        (i, m.sequence, m.log_time, m.data.hex()) for i, _, m in expected]
    rows.append({"case": "sidecar-dataset", "result": "passed"})
    too_small = work / "must-not-exist.midx"
    too_small.unlink(missing_ok=True)
    assert command("index-build", inputs[0], "--output", too_small, "--index-bytes", 128).returncode != 0
    assert not too_small.exists() and not list(work.glob(".*.part"))
    rows.append({"case": "budget-cleanup", "result": "passed"})
    (work / "report.json").write_text(json.dumps(rows, indent=2)+"\n", encoding="utf-8")
    print(f"Sidecar workflows: {len(rows)} passed")


if __name__ == "__main__":
    main()
