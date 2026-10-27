///|
pub(all) struct MessageIndex {
  channel_id : Int
  entries : Array[(UInt64, UInt64)]
}

///|
pub(all) struct ChunkIndex {
  start_time : UInt64
  end_time : UInt64
  chunk_start : UInt64
  chunk_length : UInt64
  message_index_offsets : Array[(Int, UInt64)]
  message_index_length : UInt64
  compression : String
  compressed_size : UInt64
  uncompressed_size : UInt64
}

///|
pub(all) struct SummaryOffset {
  opcode : Byte
  start : UInt64
  length : UInt64
}

///|
pub fn encode_message_index(value : MessageIndex) -> Bytes raise McapError {
  let out = @buffer.Buffer::Buffer()
  put_u16(out, value.channel_id)
  let entries = @buffer.Buffer::Buffer()
  for entry in value.entries {
    entries.write_uint64_le(entry.0)
    entries.write_uint64_le(entry.1)
  }
  put_blob(out, entries.to_bytes())
  out.to_bytes()
}

///|
pub fn decode_message_index(
  bytes : Bytes,
  limits? : Limits = default_limits(),
) -> MessageIndex raise McapError {
  let input = cursor(bytes, limits)
  let channel_id = input.u16()
  let records = cursor(input.blob(), limits)
  if records.data.length() % 16 != 0 {
    raise Invalid("misaligned MessageIndex entries")
  }
  if records.data.length() / 16 > limits.max_records {
    raise Invalid("MessageIndex entry budget exceeded")
  }
  let entries = []
  while records.pos < records.data.length() {
    entries.push((records.u64(), records.u64()))
  }
  { channel_id, entries, }
}

///|
pub fn encode_chunk_index(value : ChunkIndex) -> Bytes raise McapError {
  if value.start_time > value.end_time {
    raise Invalid("inverted ChunkIndex time range")
  }
  let out = @buffer.Buffer::Buffer()
  out.write_uint64_le(value.start_time)
  out.write_uint64_le(value.end_time)
  out.write_uint64_le(value.chunk_start)
  out.write_uint64_le(value.chunk_length)
  let entries = @buffer.Buffer::Buffer()
  let seen : Map[Int, Bool] = {}
  for entry in value.message_index_offsets {
    if seen.contains(entry.0) {
      raise Invalid("duplicate ChunkIndex channel")
    }
    seen[entry.0] = true
    put_u16(entries, entry.0)
    entries.write_uint64_le(entry.1)
  }
  put_blob(out, entries.to_bytes())
  out.write_uint64_le(value.message_index_length)
  put_text(out, value.compression)
  out.write_uint64_le(value.compressed_size)
  out.write_uint64_le(value.uncompressed_size)
  out.to_bytes()
}

///|
pub fn decode_chunk_index(
  bytes : Bytes,
  limits? : Limits = default_limits(),
) -> ChunkIndex raise McapError {
  let input = cursor(bytes, limits)
  let start_time = input.u64()
  let end_time = input.u64()
  let chunk_start = input.u64()
  let chunk_length = input.u64()
  let map_input = cursor(input.blob(), limits)
  if map_input.data.length() % 10 != 0 {
    raise Invalid("misaligned ChunkIndex map")
  }
  let message_index_offsets = []
  let seen : Map[Int, Bool] = {}
  while map_input.pos < map_input.data.length() {
    let id = map_input.u16()
    let offset = map_input.u64()
    if seen.contains(id) {
      raise Invalid("duplicate ChunkIndex channel")
    }
    seen[id] = true
    message_index_offsets.push((id, offset))
  }
  let message_index_length = input.u64()
  let compression = input.text()
  let compressed_size = input.u64()
  let uncompressed_size = input.u64()
  if start_time > end_time {
    raise Invalid("inverted ChunkIndex time range")
  }
  {
    start_time,
    end_time,
    chunk_start,
    chunk_length,
    message_index_offsets,
    message_index_length,
    compression,
    compressed_size,
    uncompressed_size,
  }
}

///|
pub fn encode_summary_offset(value : SummaryOffset) -> Bytes {
  let out = @buffer.Buffer::Buffer()
  out.write_byte(value.opcode)
  out.write_uint64_le(value.start)
  out.write_uint64_le(value.length)
  out.to_bytes()
}

///|
pub fn decode_summary_offset(bytes : Bytes) -> SummaryOffset raise McapError {
  let input = cursor(bytes, default_limits())
  let opcode = input.take(1)[0]
  let start = input.u64()
  let length = input.u64()
  { opcode, start, length, }
}

///|
/// Verify index tuples refer to actual Message records inside the same chunk.
pub fn validate_message_index(
  index : MessageIndex,
  chunk : Chunk,
) -> Unit raise McapError {
  let seen : Map[UInt64, Bool] = {}
  for entry in index.entries {
    if seen.contains(entry.1) {
      raise Invalid("duplicate MessageIndex record offset")
    }
    seen[entry.1] = true
    match
      chunk.records
      .iter()
      .find_first(fn(record) { record.offset.to_uint64() == entry.1 }) {
      None => raise Invalid("MessageIndex offset is not a record boundary")
      Some(record) => {
        if record.opcode != 0x05 {
          raise Invalid("MessageIndex points to non-Message")
        }
        let message = decode_message(record.body)
        if message.channel_id != index.channel_id || message.log_time != entry.0 {
          raise Invalid("MessageIndex timestamp or channel mismatch")
        }
      }
    }
  }
  let expected = chunk.records
    .filter(fn(record) {
      if record.opcode != 0x05 {
        false
      } else {
        let message = Result::Ok(decode_message(record.body)) catch {
          error => Result::Err(error)
        }
        match message {
          Ok(value) => value.channel_id == index.channel_id
          Err(_) => false
        }
      }
    })
    .length()
  if expected != index.entries.length() {
    raise Invalid("incomplete MessageIndex")
  }
}
