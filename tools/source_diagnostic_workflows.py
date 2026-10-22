"""Independent bounded-source diagnosis and recovery evidence, including faults."""
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
from mcap.writer import Writer, CompressionType, IndexType
from mcap.reader import NonSeekingReader, make_reader
from mcap.stream_reader import StreamReader


def frames(data):
    position = 8
    while position < len(data) - 8:
        opcode, length = struct.unpack_from("<BQ", data, position)
        yield position, opcode, length
        position += length + 9


def main():
    work = ROOT / "_interop/source-diagnostics"
    work.mkdir(parents=True, exist_ok=True)
    subprocess.run([os.environ.get("MOON", "moon"), "build", "--target", "js"], cwd=ROOT, check=True, capture_output=True)
    cli = ROOT / "_build/js/debug/build/cmd/diagnose/diagnose.js"
    rows = []

    def command(*args):
        return subprocess.run([os.environ.get("NODE", "node"), str(cli), *map(str, args)], cwd=ROOT, capture_output=True, text=True, encoding="utf-8", timeout=60)

    for mode in CompressionType:
        for layout in ("indexed", "unindexed", "sequential"):
            buffer = io.BytesIO()
            writer = Writer(buffer, compression=mode, chunk_size=200, use_chunking=layout != "sequential",
                            index_types=IndexType.ALL if layout == "indexed" else IndexType.NONE,
                            repeat_channels=layout == "indexed", use_statistics=layout == "indexed",
                            use_summary_offsets=layout == "indexed", enable_crcs=True, enable_data_crcs=True)
            writer.start()
            channel = writer.register_channel("/events", "raw", 0)
            for i in range(40):
                writer.add_message(channel, i * 10, bytes([i]) * 64, i * 10, i)
            writer.finish()
            original = buffer.getvalue()
            source = work / f"{mode.name}-{layout}.mcap"
            source.write_bytes(original)
            result = command("validate", source, "--read-bytes", "63")
            assert result.returncode == 0, result.stderr + result.stdout
            report = json.loads(result.stdout)
            assert report["valid"] and report["data_valid"] and report["data_crc_verified"], report
            assert report["observed_messages"] == "40" and report["observed_time_start"] == "0" and report["observed_time_end"] == "390"
            assert report["io"]["peak_read_bytes"] <= 63 and report["unchecked_range"] is None
            assert sum(1 for _ in NonSeekingReader(io.BytesIO(original), validate_crcs=True).iter_messages()) == 40
            rows.append({"case": f"{mode.name}-{layout}-valid", "report": report})
            if layout != "indexed":
                continue
            records = list(frames(original))
            chunks = [(p, n) for p, op, n in records if op == 6]
            middle, length = chunks[len(chunks) // 2]
            data_end = next(p for p, op, _ in records if op == 15)
            faults = {"chunk-crc": middle + 9 + 24, "chunk-frame": middle + 1,
                      "data-crc": data_end + 9, "summary-crc": len(original) - 12,
                      "leading-magic": 0}
            for fault, point in faults.items():
                damaged = bytearray(original)
                damaged[point] ^= 0x40
                source.write_bytes(damaged)
                digest = hashlib.sha256(damaged).hexdigest()
                result = command("validate", source, "--read-bytes", "63")
                assert result.returncode == 1, result.stderr + result.stdout
                report = json.loads(result.stdout)
                assert not report["valid"] and not report["source_complete"] and len(report["issues"]) == 1
                assert report["unknown_lost_messages"] is None and not report["prefix_is_integrity_proof"]
                if fault == "summary-crc" or fault == "leading-magic":
                    assert report["data_valid"] is None and report["observed_messages"] == "0"
                if fault == "chunk-crc":
                    assert int(report["failed_record_range"]["start"]) == middle
                    assert int(report["failed_record_range"]["end"]) == middle + length + 9
                if fault == "data-crc":
                    issue = report["issues"][0]
                    assert issue["scope"] == "whole data section" and issue["expected_crc"] != issue["actual_crc"]
                if fault in ("chunk-crc", "chunk-frame"):
                    output = work / f"{mode.name}-{fault}-recovered.mcap"
                    output.unlink(missing_ok=True)
                    result = command("recover", source, "--output", output)
                    assert result.returncode == 0, result.stderr
                    evidence = json.loads(result.stdout)
                    skipped = [row for row in evidence["ranges"] if not row["recovered"]]
                    assert len(skipped) == 1 and int(skipped[0]["start"]) == middle
                    assert skipped[0]["actual_time_start"] is None and skipped[0]["actual_time_end"] is None
                    assert skipped[0]["unknown_lost_messages"] is None
                    list(StreamReader(io.BytesIO(output.read_bytes()), validate_crcs=True).records)
                    messages = [m for _, _, m in make_reader(io.BytesIO(output.read_bytes()), validate_crcs=True).iter_messages()]
                    assert len(messages) == evidence["summary"]["messages"] < 40
                    for row in evidence["ranges"]:
                        if row["recovered"]:
                            times = [m.log_time for m in messages if int(row["actual_time_start"]) <= m.log_time <= int(row["actual_time_end"])]
                            assert len(times) == row["recovered_messages"] and min(times) == int(row["actual_time_start"]) and max(times) == int(row["actual_time_end"])
                    output.unlink()
                    failed = command("recover", source, "--output", output, "--max-ranges", "1")
                    assert failed.returncode == 2 and not output.exists() and not list(work.glob("*.part"))
                    rows.append({"case": f"{mode.name}-{fault}-recovery", "report": evidence})
                assert hashlib.sha256(source.read_bytes()).hexdigest() == digest
                rows.append({"case": f"{mode.name}-{fault}", "report": report})
            for option, value in (("--record-limit", "3"), ("--record-bytes", "32"), ("--decoded-bytes", "80"), ("--summary-bytes", "16")):
                source.write_bytes(original)
                result = command("validate", source, option, value)
                assert result.returncode == 1, result.stderr + result.stdout
                report = json.loads(result.stdout)
                assert not report["valid"]
                rows.append({"case": f"{mode.name}-{option}", "report": report})
            source.write_bytes(original[:-19])
            result = command("validate", source)
            assert result.returncode == 1 and not json.loads(result.stdout)["valid"]
            rows.append({"case": f"{mode.name}-truncation", "result": "passed"})
    (work / "report.json").write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")
    print(f"source diagnostic workflows: {len(rows)} passed")


if __name__ == "__main__":
    main()
