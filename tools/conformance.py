"""Synthetic MCAP container/compression interoperability and workflow checks."""
import hashlib
import io
import json
import os
import random
from pathlib import Path
import struct
import subprocess
import sys
import zlib

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "_interop_deps"))
from mcap.reader import make_reader, NonSeekingReader
from mcap.stream_reader import StreamReader
from mcap.writer import Writer, CompressionType, IndexType

MOON = os.environ.get("MOON", "moon")
NODE = os.environ.get("NODE", "node")
WORK = ROOT / "_interop" / "conformance"
WORK.mkdir(parents=True, exist_ok=True)
rows = []


def checked(command):
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, encoding="utf-8")
    if result.returncode:
        raise RuntimeError(result.stdout[-4000:] + result.stderr[-4000:])
    return result.stdout


checked([MOON, "build", "--target", "js"])
CLI = ROOT / "_build/js/debug/build/cmd/interop/interop.js"
assert CLI.is_file()


def transcode(source, output, compression, operation="rewrite", second=None):
    command = [NODE, str(CLI), str(source), str(output), compression, operation]
    if second is not None:
        command.append(str(second))
    return checked(command)


def inspect(path):
    data = path.read_bytes()
    # Full sequential validation checks the data-section CRC too.
    list(StreamReader(io.BytesIO(data), validate_crcs=True).records)
    reader = make_reader(io.BytesIO(data), validate_crcs=True)
    messages = [(s.name if s else "", c.topic, m.sequence, m.log_time, m.publish_time, m.data)
                for s, c, m in NonSeekingReader(io.BytesIO(data), validate_crcs=True).iter_messages()]
    attachments = [(a.name, a.media_type, a.log_time, a.create_time, a.data) for a in reader.iter_attachments()]
    metadata = [(m.name, m.metadata) for m in reader.iter_metadata()]
    summary = reader.get_summary()
    if summary and summary.statistics:
        assert summary.statistics.message_count == len(messages)
        assert summary.statistics.attachment_count == len(attachments)
        assert summary.statistics.metadata_count == len(metadata)
    return messages, attachments, metadata


def fixture(compression, summary, chunked=True, indexes=None, offsets=None):
    stream = io.BytesIO()
    writer = Writer(stream, compression=compression, chunk_size=16384,
                    index_types=(IndexType.ALL if summary else IndexType.NONE) if indexes is None else indexes,
                    repeat_channels=summary, repeat_schemas=summary, use_chunking=chunked,
                    use_statistics=summary, use_summary_offsets=summary if offsets is None else offsets,
                    enable_crcs=True, enable_data_crcs=True)
    writer.start(library="official-python-mcap/1.3.0-reference")
    schema = writer.register_schema("Sample", "jsonschema", b'{"type":"object"}')
    channels = [writer.register_channel(t, "json", schema) for t in ("/imu", "/camera")]
    for i in range(100):
        payload = json.dumps({"sequence": i, "sample": "abcdef" * 700}, separators=(",", ":")).encode()
        writer.add_message(channels[i % 2], (i + 1) * 10**9, payload, (i + 1) * 10**9, sequence=i // 2)
    writer.add_attachment(1, 2, "calibration.json", "application/json", b'{"unit":"test"}')
    writer.add_metadata("run", {"source": "synthetic-reference"})
    writer.finish()
    return stream.getvalue()


modes = {"none": CompressionType.NONE, "lz4": CompressionType.LZ4, "zstd": CompressionType.ZSTD}
inputs = {}
for mode, compression in modes.items():
    for summary in (True, False):
        name = f"{mode}-{'summary' if summary else 'no-summary'}"
        source = WORK / f"{name}.mcap"
        source.write_bytes(fixture(compression, summary))
        expected = inspect(source)
        assert len(expected[0]) == 100
        inputs[mode, summary] = source
        for output_mode in modes:
            output = WORK / f"{name}-to-{output_mode}.mcap"
            transcode(source, output, output_mode)
            assert inspect(output) == expected
            rows.append({"input": name, "output_compression": output_mode, "result": "passed",
                         "input_sha256": hashlib.sha256(source.read_bytes()).hexdigest()})
        print(f"verified {name}: all three output modes", flush=True)

# Sequential files and chunk indexes without per-message indexes are both legal.
for name, chunked, indexes, offsets in (("sequential", False, IndexType.ALL, True),
                               ("no-message-index", True, IndexType.CHUNK | IndexType.ATTACHMENT | IndexType.METADATA, True),
                               ("no-summary-offset", True, IndexType.ALL, False)):
    source = WORK / f"{name}.mcap"
    source.write_bytes(fixture(CompressionType.NONE, True, chunked, indexes, offsets))
    output = WORK / f"{name}-output.mcap"
    transcode(source, output, "zstd")
    assert inspect(output) == inspect(source)
    rows.append({"input": name, "result": "passed"})

def physical_records(data):
    position = 8
    while position < len(data) - 8:
        opcode = data[position]
        length, = struct.unpack_from("<Q", data, position + 1)
        yield opcode, data[position + 9:position + 9 + length]
        position += 9 + length


blocks = {mode: [body for op, body in physical_records(path.read_bytes()) if op == 6]
          for (mode, summary), path in inputs.items() if not summary}
mixed = bytearray(inputs["none", False].read_bytes()[:8])
chunk_number = 0
for opcode, body in physical_records(inputs["none", False].read_bytes()):
    if opcode == 6:
        body = blocks[list(modes)[chunk_number % 3]][chunk_number]
        chunk_number += 1
    if opcode == 15:
        body = struct.pack("<I", zlib.crc32(mixed))
    if opcode == 2:
        body = struct.pack("<QQI", 0, 0, 0)
    mixed.extend(struct.pack("<BQ", opcode, len(body)))
    mixed.extend(body)
mixed.extend(inputs["none", False].read_bytes()[-8:])
mixed_input, mixed_output = WORK / "mixed.mcap", WORK / "mixed-output.mcap"
mixed_input.write_bytes(mixed)
transcode(mixed_input, mixed_output, "zstd")
assert inspect(mixed_output) == inspect(inputs["none", False])
rows.append({"input": "mixed-compression-chunks", "result": "passed"})

# Opaque binary payloads include zero-length, noisy, and multi-block messages.
random_source = random.Random(20261016)
stream = io.BytesIO()
writer = Writer(stream, compression=CompressionType.ZSTD, chunk_size=16384,
                enable_crcs=True, enable_data_crcs=True)
writer.start(library="official-python-binary-reference")
channel = writer.register_channel("/binary", "binary", 0)
for i, length in enumerate((0, 1, 31, 128, 4096, 65536, 131072, 262144)):
    payload = random_source.randbytes(length) if i % 2 else (b"pattern" * ((length + 6) // 7))[:length]
    writer.add_message(channel, i + 1, payload, i + 1, sequence=i)
writer.finish()
binary_input = WORK / "binary-multiblock.mcap"
binary_input.write_bytes(stream.getvalue())
for mode in modes:
    output = WORK / f"binary-multiblock-{mode}.mcap"
    transcode(binary_input, output, mode)
    assert inspect(output) == inspect(binary_input)
    rows.append({"input": "binary-multiblock", "output_compression": mode, "result": "passed"})

# The three end-to-end workflows use full-size inputs, not only tiny fixtures.
sliced = WORK / "slice.mcap"
transcode(inputs["zstd", True], sliced, "lz4", "slice")
messages, attachments, metadata = inspect(sliced)
assert [m[3] for m in messages] == [t * 10**9 for t in (21, 23, 25, 27, 29)]
assert len(attachments) == 1 and len(metadata) == 1
merged = WORK / "merged.mcap"
transcode(inputs["lz4", False], merged, "zstd", "merge", inputs["none", True])
messages, attachments, metadata = inspect(merged)
assert len(messages) == 200 and len(attachments) == 2 and len(metadata) == 2
for mode in modes:
    data = inputs[mode, True].read_bytes()
    position, chunks = 8, []
    while position < len(data) - 8:
        opcode = data[position]
        length, = struct.unpack_from("<Q", data, position + 1)
        if opcode == 6:
            chunks.append((position, length))
        position += 9 + length
    stop, length = chunks[12]
    truncated = WORK / f"truncated-{mode}.mcap"
    truncated.write_bytes(data[:stop + 9 + length // 2])
    repaired = WORK / f"repaired-{mode}.mcap"
    output = transcode(truncated, repaired, mode, "recover")
    messages, attachments, metadata = inspect(repaired)
    assert len(messages) == 48 and attachments == [] and metadata == []
    assert f"recovery_stop={stop}" in output
rows.extend({"workflow": name, "result": "passed"} for name in ("slice", "merge", "recover-none", "recover-lz4", "recover-zstd"))
report = {"reference": "mcap==1.3.0", "format": "MCAP 0", "cases": rows}
(WORK / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
print(f"Passed: {len(rows)} interoperability and workflow cases", flush=True)
