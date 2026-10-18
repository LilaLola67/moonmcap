"""Real disk external sort: independent stable oracle, budgets and cleanup failures."""
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
from mcap.reader import make_reader, NonSeekingReader
from mcap.stream_reader import StreamReader
from mcap.writer import Writer, CompressionType


def main():
    work = ROOT / "_interop/external"
    temp = work / "temporary runs"
    temp.mkdir(parents=True, exist_ok=True)
    sentinel = temp / "keep.txt"
    sentinel.write_text("unrelated file remains", encoding="utf-8")
    sources = []
    expected = []
    for source_index in range(2):
        data = io.BytesIO()
        writer = Writer(data, compression=CompressionType.NONE if source_index == 0 else CompressionType.ZSTD,
                        chunk_size=16 * 1024, enable_crcs=True, enable_data_crcs=True)
        writer.start(profile="test", library="official-python-external-fixture")
        schema = writer.register_schema("Sample" + str(source_index), "raw", bytes([source_index]))
        channel = writer.register_channel("/shared", "raw", schema)
        rng = random.Random(20261018 + source_index)
        for i in range(100):
            time = (2**64 - 1) if i % 47 == 0 else rng.randrange(50) * 10**9
            payload = struct.pack("<II", source_index, i) + rng.randbytes(1016)
            writer.add_message(channel, time, payload, time, sequence=i)
        writer.add_attachment(1, 2, "calibration", "raw", bytes([source_index]))
        writer.add_metadata("run", {"source": str(source_index)})
        writer.finish()
        source = work / f"input-{source_index}.mcap"
        source.write_bytes(data.getvalue())
        sources.append(source)
        for s, c, m in NonSeekingReader(io.BytesIO(data.getvalue()), validate_crcs=True).iter_messages(log_time_order=False):
            expected.append((s.name, c.topic, m.sequence, m.log_time, m.publish_time, m.data))
    expected.sort(key=lambda m: m[3])
    originals = [hashlib.sha256(p.read_bytes()).hexdigest() for p in sources]
    subprocess.run([os.environ.get("MOON", "moon"), "build", "--target", "js"], cwd=ROOT, check=True, capture_output=True)
    cli = ROOT / "_build/js/debug/build/cmd/mcap/mcap.js"
    base = ["external-merge", *map(str, sources), "--temp-dir", str(temp), "--run-bytes", "4096", "--merge-bytes", "4096", "--fan-in", "2", "--chunk-messages", "8"]

    def run(args):
        return subprocess.run([os.environ.get("NODE", "node"), str(cli), *args], cwd=ROOT,
                              capture_output=True, text=True, encoding="utf-8", timeout=60)

    def clean():
        assert sentinel.read_text(encoding="utf-8") == "unrelated file remains"
        assert not list(temp.glob("moonmcap-sort-*"))
        assert not list(work.glob(".*.part"))

    rows = []
    for mode in ("none", "lz4", "zstd"):
        output = work / f"merged-{mode}.mcap"
        output.unlink(missing_ok=True)
        args = [*base, "--output", str(output), "--compression", mode]
        result = run(args)
        assert result.returncode == 0, result.stderr
        report = json.loads(result.stdout)
        data = output.read_bytes()
        list(StreamReader(io.BytesIO(data), validate_crcs=True).records)
        reader = make_reader(io.BytesIO(data), validate_crcs=True)
        actual = [(s.name, c.topic, m.sequence, m.log_time, m.publish_time, m.data) for s, c, m in reader.iter_messages()]
        assert actual == expected
        assert reader.get_summary().statistics.message_count == 200
        assert len(list(reader.iter_attachments())) == len(list(reader.iter_metadata())) == 2
        assert report["merge_passes"] > 1 and report["runs_created"] > 10
        assert report["peak_run_bytes"] <= 4096 and report["peak_merge_bytes"] <= 4096
        old = hashlib.sha256(data).hexdigest()
        assert run(args).returncode == 2 and hashlib.sha256(output.read_bytes()).hexdigest() == old
        clean()
        rows.append({"case": mode, "result": "passed", **report})
    failed = work / "must-not-exist.mcap"
    failed.unlink(missing_ok=True)
    result = run([*base, "--output", str(failed), "--spool-bytes", "32"])
    assert result.returncode == 2 and not failed.exists()
    clean()
    rows.append({"case": "disk-budget-cleanup", "result": "passed"})
    damaged = bytearray(sources[0].read_bytes())
    position = 8
    while position < len(damaged) - 8:
        opcode, length = struct.unpack_from("<BQ", damaged, position)
        if opcode == 15:
            damaged[position + 9] ^= 128
            break
        position += 9 + length
    broken = work / "bad-input.mcap"
    broken.write_bytes(damaged)
    result = run(["external-merge", str(broken), str(sources[1]), "--temp-dir", str(temp), "--output", str(failed), "--run-bytes", "4096"])
    assert result.returncode == 2 and not failed.exists()
    clean()
    rows.append({"case": "bad-input-cleanup", "result": "passed"})
    assert [hashlib.sha256(p.read_bytes()).hexdigest() for p in sources] == originals
    (work / "report.json").write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")
    print(f"External sort PASS: {len(rows)} stable disk merge/budget/cleanup workflows")


if __name__ == "__main__":
    main()
