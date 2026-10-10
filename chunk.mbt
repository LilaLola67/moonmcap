///|
pub(all) struct Chunk {
  start_time : UInt64
  end_time : UInt64
  records : Array[Record]
}

///|
pub fn encode_chunk(records : Array[Record], start_time : UInt64, end_time : UInt64, checksums? : Bool = true) -> Bytes raise McapError {
  if start_time > end_time { raise Invalid("inverted chunk time range") }
  let payload = @buffer.Buffer::Buffer()
  for record in records {
    if record.opcode != 0x03 && record.opcode != 0x04 && record.opcode != 0x05 && record.opcode < 0x80 {
      raise Invalid("record type not permitted inside Chunk")
    }
    payload.write_bytes(frame(record.opcode, record.body)[:])
  }
  let bytes = payload.to_bytes()
  let out = @buffer.Buffer::Buffer()
  out.write_uint64_le(start_time)
  out.write_uint64_le(end_time)
  out.write_uint64_le(bytes.length().to_uint64())
  out.write_uint_le(if checksums { crc32(bytes) } else { 0U })
  put_text(out, "")
  out.write_uint64_le(bytes.length().to_uint64())
  out.write_bytes(bytes[:])
  out.to_bytes()
}

///|
pub fn decode_chunk(bytes : Bytes, limits? : Limits = default_limits()) -> Chunk raise McapError {
  let input = cursor(bytes, limits)
  let start_time = input.u64()
  let end_time = input.u64()
  let expected_size = input.u64()
  let checksum = input.u32()
  let compression = input.text()
  let size = bounded_length(input.u64(), limits.max_record_bytes, "chunk")
  let payload = input.take(size)
  if compression != "" { raise Invalid("unsupported chunk compression: \{compression}") }
  if expected_size != payload.length().to_uint64() { raise Invalid("chunk uncompressed size mismatch") }
  if start_time > end_time { raise Invalid("inverted chunk time range") }
  verify_crc(payload, checksum, label="chunk")
  let records = decode_records(payload, limits~)
  for record in records {
    if record.opcode != 0x03 && record.opcode != 0x04 && record.opcode != 0x05 && record.opcode < 0x80 {
      raise Invalid("record type not permitted inside Chunk")
    }
    if record.opcode == 0x05 {
      let message = decode_message(record.body, limits~)
      if message.log_time < start_time || message.log_time > end_time {
        raise Invalid("message timestamp outside chunk range")
      }
    }
  }
  { start_time, end_time, records }
}

///|
pub fn write_chunked(log : Log, messages_per_chunk? : Int = 256, checksums? : Bool = true, limits? : Limits = default_limits()) -> Bytes raise McapError {
  validate_log(log, limits~)
  if messages_per_chunk <= 0 { raise Invalid("chunk message count must be positive") }
  let out = @buffer.Buffer::Buffer()
  out.write_bytes(magic()[:])
  out.write_bytes(frame(0x01, encode_header(log.header))[:])
  for schema in log.schemas { out.write_bytes(frame(0x03, encode_schema(schema))[:]) }
  for channel in log.channels { out.write_bytes(frame(0x04, encode_channel(channel))[:]) }
  let mut start = 0
  while start < log.messages.length() {
    let end = start + messages_per_chunk.min(log.messages.length() - start)
    let mut first = log.messages[start].log_time
    let mut last = first
    let records = []
    for i = start; i < end; i = i + 1 {
      let message = log.messages[i]
      if message.log_time < first { first = message.log_time }
      if message.log_time > last { last = message.log_time }
      records.push({ opcode: 0x05, offset: 0, body: encode_message(message) })
    }
    let body = encode_chunk(records, first, last, checksums~)
    if body.length() > limits.max_record_bytes { raise Invalid("chunk exceeds record budget; reduce messages_per_chunk") }
    out.write_bytes(frame(0x06, body)[:])
    start = end
  }
  for metadata in log.metadata { out.write_bytes(frame(0x0c, encode_metadata(metadata))[:]) }
  let checksum = if checksums { crc32(out.to_bytes()) } else { 0U }
  out.write_bytes(frame(0x0f, checksum.to_le_bytes())[:])
  out.write_bytes(frame(0x02, encode_footer({summary_start: 0UL, summary_offset_start: 0UL, summary_crc: 0U}))[:])
  out.write_bytes(magic()[:])
  let bytes = out.to_bytes()
  ignore(scan_file(bytes, limits~))
  bytes
}
