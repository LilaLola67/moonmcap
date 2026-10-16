"""Generate small official compression fixtures usable on Wasm and JS."""
from pathlib import Path
import struct
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "_interop_deps"))
import lz4.frame
import zstandard

payload = b"abcdef" * 30000
tests = ["// Generated reference frames; not handwritten implementation source.\n"]
for linked in (False, True):
    for checksum in (False, True):
        frame = lz4.frame.compress(payload, block_linked=linked, block_checksum=checksum,
                                   content_checksum=checksum, store_size=checksum,
                                   block_size=lz4.frame.BLOCKSIZE_MAX64KB)
        assert lz4.frame.decompress(frame) == payload
        tests.append(f'''///|
test "reference LZ4 linked={linked} checksum={checksum}" {{
  let bytes = @moonmcap.decode_hex("{frame.hex()}")
  let expected = Bytes::makei(180000, fn(i) {{ b"abcdef"[i % 6] }})
  assert_eq(@moonmcap.decompress_payload(bytes, "lz4", 180000UL), expected)
}}
''')

for level in (1, 3, 19):
    for checksum in (False, True):
        frame = zstandard.ZstdCompressor(level=level, write_checksum=checksum,
                                         write_content_size=checksum).compress(payload)
        assert zstandard.ZstdDecompressor().decompress(frame, max_output_size=len(payload)) == payload
        tests.append(f'''///|
test "reference Zstd level={level} known_size={checksum} checksum={checksum}" {{
  let bytes = @moonmcap.decode_hex("{frame.hex()}")
  let expected = Bytes::makei(180000, fn(i) {{ b"abcdef"[i % 6] }})
  assert_eq(@moonmcap.decompress_payload(bytes, "zstd", 180000UL), expected)
}}
''')

for kind, data in ((0, b"raw literals"), (1, b"R" * 500)):
    header = b"\x28\xb5\x2f\xfd\xa0" + struct.pack("<I", len(data))
    block = struct.pack("<I", (len(data) << 3) | (kind << 1) | 1)[:3]
    frame = header + block + (data if kind == 0 else data[:1])
    assert zstandard.ZstdDecompressor().decompress(frame) == data
    expected = 'b"raw literals"' if kind == 0 else 'Bytes::make(500, b\'R\')'
    tests.append(f'''///|
test "reference Zstd block kind {kind}" {{
  let bytes = @moonmcap.decode_hex("{frame.hex()}")
  assert_eq(@moonmcap.decompress_payload(bytes, "zstd", {len(data)}UL), {expected})
}}
''')

(ROOT / "compression_fixture_test.mbt").write_text("\n".join(tests), encoding="utf-8")
print("Generated 12 compression fixture tests")
