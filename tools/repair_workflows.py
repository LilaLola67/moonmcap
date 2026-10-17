"""Distinguish repairable indexes/tails from damaged data, using independent CRC checks."""
import hashlib
import io
import json
import os
from pathlib import Path
import struct
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "_interop_deps"))
from mcap.reader import make_reader
from mcap.stream_reader import StreamReader


def records(data):
    position = 8
    while position < len(data) - 8:
        opcode, length = struct.unpack_from("<BQ", data, position)
        yield position, opcode, length
        position += 9 + length


def semantic(data):
    list(StreamReader(io.BytesIO(data), validate_crcs=True).records)
    reader = make_reader(io.BytesIO(data), validate_crcs=True)
    messages = [(s.name if s else None, c.topic, m.log_time, m.publish_time, m.sequence, m.data)
                for s, c, m in reader.iter_messages()]
    attachments = [(a.name, a.log_time, a.create_time, a.data) for a in reader.iter_attachments()]
    metadata = [(m.name, m.metadata) for m in reader.iter_metadata()]
    return messages, attachments, metadata


def main():
    work = ROOT / "_interop/repair"
    work.mkdir(parents=True, exist_ok=True)
    subprocess.run([os.environ.get("MOON", "moon"), "build", "--target", "js"], cwd=ROOT, check=True, capture_output=True)
    cli = ROOT / "_build/js/debug/build/cmd/mcap/mcap.js"
    rows = []

    def command(*args):
        return subprocess.run([os.environ.get("NODE", "node"), str(cli), *map(str, args)], cwd=ROOT,
                              capture_output=True, text=True, encoding="utf-8")

    def check_case(name, data, repairable, expected_code=None):
        source, output = work / (name + ".mcap"), work / (name + "-repaired.mcap")
        source.write_bytes(data)
        output.unlink(missing_ok=True)
        digest = hashlib.sha256(data).hexdigest()
        diagnosis = command("validate", source)
        assert diagnosis.returncode == 1
        report = json.loads(diagnosis.stdout)
        assert report["data_valid"] == repairable and not report["valid"]
        if expected_code:
            assert any(d["code"] == expected_code for d in report["issues"]), report
        result = command("reindex", source, "--output", output, "--compression", "zstd")
        if repairable:
            assert result.returncode == 0, result.stderr
            fixed = json.loads(result.stdout)
            assert fixed["result"]["valid"]
            assert semantic(output.read_bytes()) == reference
        else:
            assert result.returncode == 2 and not output.exists()
        assert hashlib.sha256(source.read_bytes()).hexdigest() == digest
        rows.append({"case": name, "result": "passed", "repairable": repairable, "diagnosis": report})
        print(f"Repair {name}: {'rebuilt' if repairable else 'refused damaged data'} PASS")

    baseline = (ROOT / "examples/files/none-summary.mcap").read_bytes()
    reference = semantic(baseline)
    for mode in ("none", "lz4", "zstd"):
        data = bytearray((ROOT / f"examples/files/{mode}-summary.mcap").read_bytes())
        index = next(p for p, op, _ in records(data) if op == 8)
        chunk_start, = struct.unpack_from("<Q", data, index + 25)
        struct.pack_into("<Q", data, index + 25, chunk_start + 1)
        struct.pack_into("<I", data, len(data) - 12, 0)  # Summary CRC unavailable; data CRC is unchanged.
        check_case("bad-index-" + mode, bytes(data), True, "bad_index")
    data_end = next(p for p, op, _ in records(baseline) if op == 15)
    summary_start = data_end + 13
    damaged_summary = bytearray(baseline)
    damaged_summary[summary_start + 10] ^= 128
    check_case("summary-crc", bytes(damaged_summary), True, "bad_checksum")
    check_case("missing-tail", baseline[:data_end + 13], True)
    check_case("truncated-data", (ROOT / "examples/files/truncated-zstd.mcap").read_bytes(), False, "truncated_input")
    bad_crc = bytearray(baseline)
    bad_crc[data_end + 9] ^= 128
    check_case("bad-data-crc", bytes(bad_crc), False, "bad_checksum")
    assert command("validate", ROOT / "examples/files/zstd-summary.mcap").returncode == 0
    rows.append({"case": "valid-file", "result": "passed"})
    (work / "report.json").write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
