"""Independent-block recovery: damaged middle framing/CRC and explicit gap manifests."""
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
from mcap.writer import Writer, CompressionType
from mcap.stream_reader import StreamReader
from mcap.reader import make_reader


def main():
    work = ROOT / "_interop/blocks"
    work.mkdir(parents=True, exist_ok=True)
    subprocess.run([os.environ.get("MOON", "moon"), "build", "--target", "js"], cwd=ROOT, check=True, capture_output=True)
    cli = ROOT / "_build/js/debug/build/cmd/mcap/mcap.js"
    rows = []
    for mode in CompressionType:
        buffer = io.BytesIO()
        writer = Writer(buffer, compression=mode, chunk_size=160, enable_crcs=True, enable_data_crcs=True)
        writer.start()
        channel = writer.register_channel("/events", "raw", 0)
        for i in range(40):
            writer.add_message(channel, i, bytes([i]) * 64, i, i)
        writer.add_metadata("intentionally-omitted", {"key": "value"})
        writer.finish()
        baseline = buffer.getvalue()
        chunks = []
        position = 8
        while position < len(baseline) - 8:
            opcode, length = struct.unpack_from("<BQ", baseline, position)
            if opcode == 6:
                chunks.append((position, length))
            position += 9 + length
        middle, length = chunks[len(chunks) // 2]
        for damage in ("crc", "framing", "summary"):
            data = bytearray(baseline)
            point = middle + (9 + 24 if damage == "crc" else 1) if damage != "summary" else len(data) - 12
            data[point] ^= 0x40
            source = work / f"{mode.name}-{damage}.mcap"
            output = work / f"{mode.name}-{damage}-recovered.mcap"
            source.write_bytes(data)
            output.unlink(missing_ok=True)
            digest = hashlib.sha256(data).hexdigest()
            result = subprocess.run([os.environ.get("NODE", "node"), str(cli), "recover-blocks", str(source),
                                     "--output", str(output), "--compression", "zstd"], cwd=ROOT,
                                    capture_output=True, text=True, encoding="utf-8")
            if damage == "summary":
                assert result.returncode != 0 and not output.exists(), result.stdout
            else:
                assert result.returncode == 0, result.stderr
                report = json.loads(result.stdout)
                assert report["source_complete"] is False and report["source_data_crc_checked"] is False
                assert len(report["skipped"]) == 1 and int(report["skipped"][0]["start"]) == middle
                assert int(report["skipped"][0]["end"]) >= middle + 9 + length
                list(StreamReader(io.BytesIO(output.read_bytes()), validate_crcs=True).records)
                recovered = list(make_reader(io.BytesIO(output.read_bytes()), validate_crcs=True).iter_messages())
                assert 0 < len(recovered) < 40 and len(recovered) == report["messages"]
                times = [m.log_time for _, _, m in recovered]
                assert 0 in times and 39 in times
                for _, channel, message in recovered:
                    assert channel.topic == "/events" and message.data == bytes([message.log_time]) * 64
                assert not list(make_reader(io.BytesIO(output.read_bytes())).iter_metadata())
                (work / f"{mode.name}-{damage}-manifest.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
            assert hashlib.sha256(source.read_bytes()).hexdigest() == digest
            assert not list(work.glob("*.part"))
            rows.append({"case": f"{mode.name}-{damage}", "result": "passed"})
    (work / "report.json").write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")
    print(f"independent block workflows: {len(rows)} passed")


if __name__ == "__main__":
    main()
