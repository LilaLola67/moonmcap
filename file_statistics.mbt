///|
/// MCAP Statistics record (opcode 0x0b), distinct from timing diagnostics.
pub(all) struct FileStatistics {
  message_count : UInt64
  schema_count : Int
  channel_count : UInt
  attachment_count : UInt
  metadata_count : UInt
  chunk_count : UInt
  start_time : UInt64
  end_time : UInt64
  channel_message_counts : Array[(Int, UInt64)]
} derive(Eq, @debug.Debug)

///|
pub extend FileStatistics with Eq::{equal, not_equal}

///|
pub extend FileStatistics with @debug.Debug::{to_repr}

///|
pub fn encode_file_statistics(value : FileStatistics) -> Bytes raise McapError {
  let out = @buffer.Buffer::Buffer()
  out.write_uint64_le(value.message_count)
  put_u16(out, value.schema_count)
  out.write_uint_le(value.channel_count)
  out.write_uint_le(value.attachment_count)
  out.write_uint_le(value.metadata_count)
  out.write_uint_le(value.chunk_count)
  out.write_uint64_le(value.start_time)
  out.write_uint64_le(value.end_time)
  let pairs = @buffer.Buffer::Buffer()
  let seen : Map[Int, Bool] = Map([])
  for (id, count) in value.channel_message_counts {
    if seen.contains(id) { raise Invalid("duplicate Statistics channel id") }
    seen[id] = true
    put_u16(pairs, id)
    pairs.write_uint64_le(count)
  }
  put_blob(out, pairs.to_bytes())
  out.to_bytes()
}

///|
pub fn decode_file_statistics(bytes : Bytes, limits? : Limits = default_limits()) -> FileStatistics raise McapError {
  let input = cursor(bytes, limits)
  let message_count = input.u64()
  let schema_count = input.u16()
  let channel_count = input.u32()
  let attachment_count = input.u32()
  let metadata_count = input.u32()
  let chunk_count = input.u32()
  let start_time = input.u64()
  let end_time = input.u64()
  let pairs = cursor(input.blob(), limits)
  let channel_message_counts = []
  let seen : Map[Int, Bool] = Map([])
  while pairs.pos < pairs.data.length() {
    let id = pairs.u16()
    let count = pairs.u64()
    if seen.contains(id) { raise Invalid("duplicate Statistics channel id") }
    seen[id] = true
    channel_message_counts.push((id, count))
  }
  { message_count, schema_count, channel_count, attachment_count, metadata_count,
    chunk_count, start_time, end_time, channel_message_counts }
}

///|
pub fn compute_file_statistics(log : Log, chunk_count : UInt) -> FileStatistics {
  let counts : Map[Int, UInt64] = Map([])
  let mut start_time = 0UL
  let mut end_time = 0UL
  for i = 0; i < log.messages.length(); i = i + 1 {
    let message = log.messages[i]
    if i == 0 || message.log_time < start_time { start_time = message.log_time }
    if i == 0 || message.log_time > end_time { end_time = message.log_time }
    counts[message.channel_id] = counts.get(message.channel_id).unwrap_or(0UL) + 1UL
  }
  let ids = counts.keys().to_array()
  ids.sort()
  { message_count: log.messages.length().to_uint64(), schema_count: log.schemas.length(),
    channel_count: log.channels.length().to_uint(), attachment_count: log.attachments.length().to_uint(),
    metadata_count: log.metadata.length().to_uint(), chunk_count, start_time, end_time,
    channel_message_counts: ids.map(fn(id) { (id, counts[id]) }) }
}

///|
pub fn validate_file_statistics(log : Log, value : FileStatistics, chunk_count : UInt) -> Unit raise McapError {
  let expected = compute_file_statistics(log, chunk_count)
  if value.message_count != expected.message_count || value.schema_count != expected.schema_count ||
    value.channel_count != expected.channel_count || value.attachment_count != expected.attachment_count ||
    value.metadata_count != expected.metadata_count || value.chunk_count != expected.chunk_count ||
    value.start_time != expected.start_time || value.end_time != expected.end_time {
    raise Invalid("Statistics counts or timestamps disagree with data section")
  }
  if value.channel_message_counts.is_empty() { return }
  let actual : Map[Int, UInt64] = Map([])
  for (id, count) in value.channel_message_counts {
    if actual.contains(id) { raise Invalid("duplicate Statistics channel id") }
    if !log.channels.any(fn(c) { c.id == id }) { raise Invalid("Statistics references undeclared channel") }
    actual[id] = count
  }
  for (id, count) in expected.channel_message_counts {
    if actual.get(id) != Some(count) { raise Invalid("Statistics channel count mismatch") }
  }
  for (id, count) in value.channel_message_counts {
    if !expected.channel_message_counts.any(fn(pair) { pair.0 == id }) && count != 0UL {
      raise Invalid("Statistics counts messages on unused channel")
    }
  }
}
