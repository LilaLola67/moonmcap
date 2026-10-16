///|
/// Bounded MCAP LZ4 adapter. Decode into the declared, prechecked chunk size.
fn decode_lz4_frame(bytes : Bytes, expected : Int) -> Bytes raise McapError {
  let input = cursor(bytes, default_limits())
  if input.u32() != 0x184d2204U { raise Invalid("invalid LZ4 standard frame magic") }
  let flags = input.take(1)[0].to_int()
  let bd = input.take(1)[0].to_int()
  if flags >> 6 != 1 || (flags & 2) != 0 || (bd & 0x8f) != 0 { raise Invalid("reserved LZ4 frame bits") }
  let independent = (flags & 0x20) != 0
  let block_checksum = (flags & 0x10) != 0
  let content_checksum = (flags & 4) != 0
  let block_max = match (bd >> 4) & 7 {
    4 => 65536
    5 => 262144
    6 => 1048576
    7 => 4194304
    _ => raise Invalid("invalid LZ4 maximum block size")
  }
  if (flags & 8) != 0 && input.u64() != expected.to_uint64() { raise Invalid("LZ4 content size differs from Chunk") }
  if (flags & 1) != 0 && input.u32() != 0U { raise Invalid("LZ4 external dictionary is unsupported") }
  let header_end = input.pos
  let header_crc = input.take(1)[0].to_uint()
  if ((@checksum.xxh32(bytes[4:header_end]) >> 8) & 255U) != header_crc { raise Invalid("LZ4 header checksum mismatch") }
  let out : FixedArray[Byte] = FixedArray::make(expected, 0)
  let mut written = 0
  while true {
    let raw_size = input.u32()
    if raw_size == 0U { break }
    let raw = (raw_size & 0x80000000U) != 0U
    let size = bounded_length((raw_size & 0x7fffffffU).to_uint64(), block_max, "LZ4 block")
    let block = input.take(size)
    if block_checksum && input.u32() != @checksum.xxh32(block[:]) { raise Invalid("LZ4 block checksum mismatch") }
    if raw {
      if size > expected - written { raise Invalid("LZ4 output exceeds declared size") }
      for byte in block { out[written] = byte; written += 1 }
    } else {
      written = decode_lz4_block(block, out, written, block_max, independent)
    }
  }
  if written != expected { raise Invalid("LZ4 output size mismatch") }
  let result = Bytes::makei(written, fn(i) { out[i] })
  if content_checksum && input.u32() != @checksum.xxh32(result[:]) { raise Invalid("LZ4 content checksum mismatch") }
  input.finish()
  result
}

///|
fn lz4_length(input : Cursor, nibble : Int, maximum : Int) -> Int raise McapError {
  let mut length = nibble
  if length > maximum { raise Invalid("LZ4 sequence exceeds output bound") }
  if nibble == 15 {
    while true {
      let byte = input.take(1)[0].to_int()
      if byte > maximum - length { raise Invalid("LZ4 extended length exceeds output bound") }
      length += byte
      if byte != 255 { break }
    }
  }
  length
}

///|
fn decode_lz4_block(bytes : Bytes, out : FixedArray[Byte], start : Int,
  block_max : Int, independent : Bool) -> Int raise McapError {
  let input = cursor(bytes, default_limits())
  let mut written = start
  let end = start + block_max.min(out.length() - start)
  while input.pos < bytes.length() {
    let token = input.take(1)[0].to_int()
    let literals = input.take(lz4_length(input, token >> 4, end - written))
    for byte in literals { out[written] = byte; written += 1 }
    if input.pos == bytes.length() { break }
    let distance = input.u16()
    if distance == 0 || distance > written - (if independent { start } else { 0 }) {
      raise Invalid("LZ4 match references unavailable history")
    }
    if end - written < 4 { raise Invalid("LZ4 match exceeds output bound") }
    let count = lz4_length(input, token & 15, end - written - 4) + 4
    for j = 0; j < count; j = j + 1 {
      out[written] = out[written - distance]
      written += 1
    }
  }
  written
}
