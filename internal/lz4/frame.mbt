// Derived from bikallem/compress 0.3.4, Apache-2.0.
// Adapted into a portable internal package; see third_party/README.md.

///|
/// Writes an LZ4 frame header to the output buffer.
/// Returns the number of bytes written.
fn write_frame_header(
  dst : FixedArray[Byte],
  pos_ : Int32,
  content_size : Int32?,
) -> Int32 {
  let mut pos = pos_
  let start = pos
  // Magic number (little-endian)
  dst.unsafe_set(pos, (LZ4_FRAME_MAGIC & 0xFFU).to_byte())
  dst.unsafe_set(pos + 1, ((LZ4_FRAME_MAGIC >> 8) & 0xFFU).to_byte())
  dst.unsafe_set(pos + 2, ((LZ4_FRAME_MAGIC >> 16) & 0xFFU).to_byte())
  dst.unsafe_set(pos + 3, ((LZ4_FRAME_MAGIC >> 24) & 0xFFU).to_byte())
  pos += 4
  // FLG byte: version=01, block_independence=1, block_checksum=0,
  //           content_size=depends, content_checksum=1, reserved=0, dict_id=0
  let has_content_size = content_size is Some(_)
  let flg = 0b01_1_0_0_1_0_0 |
    (if has_content_size { 0b00_0_0_1_0_0_0 } else { 0 })
  dst.unsafe_set(pos, flg.to_byte())
  pos += 1
  // BD byte: block_max_size=4 (64KB)
  let bd = DEFAULT_BLOCK_MAX_SIZE_ID << 4
  dst.unsafe_set(pos, bd.to_byte())
  pos += 1
  // Optional content size (8 bytes, little-endian)
  match content_size {
    Some(size) => {
      for i in 0..<8 {
        dst.unsafe_set(pos + i, ((size >> (i * 8)) & 0xFF).to_byte())
      }
      pos += 8
    }
    None => ()
  }
  // Header checksum: (xxh32(header_bytes) >> 8) & 0xFF
  let header_start = start + 4 // after magic
  let header_len = pos - header_start
  let header_bytes = Bytes::makei(header_len, fn(i) {
    dst.unsafe_get(header_start + i)
  })
  let hc = @checksum.xxh32(header_bytes[:])
  dst.unsafe_set(pos, ((hc >> 8) & 0xFFU).to_byte())
  pos += 1
  pos - start
}

///|
/// Writes the end mark (4 zero bytes) and optional content checksum.
/// Returns bytes written.
fn write_frame_footer(
  dst : FixedArray[Byte],
  pos_ : Int32,
  content_checksum : UInt32?,
) -> Int32 {
  let mut pos = pos_
  let start = pos
  // End mark: 0x00000000
  dst.unsafe_set(pos, b'\x00')
  dst.unsafe_set(pos + 1, b'\x00')
  dst.unsafe_set(pos + 2, b'\x00')
  dst.unsafe_set(pos + 3, b'\x00')
  pos += 4
  // Content checksum (XXH32 of original data)
  match content_checksum {
    Some(cs) => {
      dst.unsafe_set(pos, (cs & 0xFFU).to_byte())
      dst.unsafe_set(pos + 1, ((cs >> 8) & 0xFFU).to_byte())
      dst.unsafe_set(pos + 2, ((cs >> 16) & 0xFFU).to_byte())
      dst.unsafe_set(pos + 3, ((cs >> 24) & 0xFFU).to_byte())
      pos += 4
    }
    None => ()
  }
  pos - start
}
