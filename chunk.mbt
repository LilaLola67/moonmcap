///|
pub(all) struct Chunk {
  start_time : UInt64
  end_time : UInt64
  records : Array[Record]
}

///|
pub fn encode_chunk(
  records : Array[Record],
  start_time : UInt64,
  end_time : UInt64,
  checksums~ : Bool = true,
  compression~ : Compression = Uncompressed,
) -> Bytes raise McapError {
  if start_time > end_time {
    raise Invalid("inverted chunk time range")
  }
  let payload = @buffer.Buffer::Buffer()
  for record in records {
    if record.opcode != 0x03 &&
      record.opcode != 0x04 &&
      record.opcode != 0x05 &&
      record.opcode < 0x80 {
      raise Invalid("record type not permitted inside Chunk")
    }
    payload.write_bytes(frame(record.opcode, record.body)[:])
  }
  let bytes = payload.to_bytes()
  let compressed = compress_payload(bytes, compression)
  let out = @buffer.Buffer::Buffer()
  out.write_uint64_le(start_time)
  out.write_uint64_le(end_time)
  out.write_uint64_le(bytes.length().to_uint64())
  out.write_uint_le(if checksums { crc32(bytes) } else { 0U })
  put_text(out, compression.name())
  out.write_uint64_le(compressed.length().to_uint64())
  out.write_bytes(compressed[:])
  out.to_bytes()
}

///|
fn decode_chunk_internal(
  bytes : Bytes,
  limits : Limits,
  wire_limit : Int32,
) -> Chunk raise McapError {
  let input = cursor(bytes, { ..limits, max_record_bytes: wire_limit, })
  let start_time = input.u64()
  let end_time = input.u64()
  let expected_size = input.u64()
  let checksum = input.u32()
  let compression = input.text()
  let size = bounded_length(input.u64(), wire_limit, "chunk")
  let payload = decompress_payload(
    input.take(size),
    compression,
    expected_size,
    limits={ ..limits, max_record_bytes: wire_limit, },
  )
  input.finish()
  if start_time > end_time {
    raise Invalid("inverted chunk time range")
  }
  verify_crc(payload, checksum, label="chunk")
  let nested_limits = {
    ..limits,
    max_record_bytes: limits.max_record_bytes.min(limits.max_decompressed_bytes),
  }
  let records = decode_records(payload, limits=nested_limits)
  for record in records {
    if record.opcode != 0x03 &&
      record.opcode != 0x04 &&
      record.opcode != 0x05 &&
      record.opcode < 0x80 {
      raise Invalid("record type not permitted inside Chunk")
    }
    if record.opcode == 0x05 {
      let message = decode_message(record.body, limits~)
      if message.log_time < start_time || message.log_time > end_time {
        raise Invalid("message timestamp outside chunk range")
      }
    }
  }
  { start_time, end_time, records, }
}

///|
pub fn write_chunked(
  log : Log,
  messages_per_chunk~ : Int32 = 256,
  checksums~ : Bool = true,
  compression~ : Compression = Uncompressed,
  limits~ : Limits = default_limits(),
) -> Bytes raise McapError {
  validate_log(log, limits~)
  if messages_per_chunk <= 0 {
    raise Invalid("chunk message count must be positive")
  }
  let out = @buffer.Buffer::Buffer()
  out.write_bytes(magic()[:])
  out.write_bytes(frame(0x01, encode_header(log.header))[:])
  for schema in log.schemas {
    out.write_bytes(frame(0x03, encode_schema(schema))[:])
  }
  for channel in log.channels {
    out.write_bytes(frame(0x04, encode_channel(channel))[:])
  }
  let mut start = 0
  while start < log.messages.length() {
    let end = start + messages_per_chunk.min(log.messages.length() - start)
    let mut first = log.messages[start].log_time
    let mut last = first
    let records = []
    for i = start; i < end; i = i + 1 {
      let message = log.messages[i]
      if message.log_time < first {
        first = message.log_time
      }
      if message.log_time > last {
        last = message.log_time
      }
      records.push({ opcode: 0x05, offset: 0, body: encode_message(message), })
    }
    let raw_size = records.fold(init=0UL, fn(n, r) {
      n + (r.body.length() + 9).to_uint64()
    })
    ignore(
      bounded_length(raw_size, limits.max_decompressed_bytes, "chunk output"),
    )
    let body = encode_chunk(records, first, last, checksums~, compression~)
    if body.length() > limits.max_record_bytes {
      raise Invalid("chunk exceeds record budget; reduce messages_per_chunk")
    }
    out.write_bytes(frame(0x06, body)[:])
    start = end
  }
  for metadata in log.metadata {
    out.write_bytes(frame(0x0c, encode_metadata(metadata))[:])
  }
  for attachment in log.attachments {
    out.write_bytes(frame(0x09, encode_attachment(attachment, checksums~))[:])
  }
  let checksum = if checksums { crc32(out.to_bytes()) } else { 0U }
  out.write_bytes(frame(0x0f, checksum.mcap_le_bytes())[:])
  out.write_bytes(
    frame(
      0x02,
      encode_footer({
        summary_start: 0UL,
        summary_offset_start: 0UL,
        summary_crc: 0U,
      }),
    )[:],
  )
  out.write_bytes(magic()[:])
  let bytes = out.to_bytes()
  ignore(scan_file(bytes, limits~))
  bytes
}
