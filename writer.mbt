///|
pub fn validate_log(
  log : Log,
  limits? : Limits = default_limits(),
) -> Unit raise McapError {
  if limits.max_record_bytes < 22 ||
    limits.max_string_bytes < 0 ||
    limits.max_records < 0 ||
    limits.max_decompressed_bytes < 0 {
    raise Invalid("invalid resource limits")
  }
  let schema_ids : Map[Int, Bool] = {}
  for schema in log.schemas {
    if schema_ids.contains(schema.id) {
      raise Invalid("duplicate schema id")
    }
    schema_ids[schema.id] = true
    let body = encode_schema(schema)
    if body.length() > limits.max_record_bytes {
      raise Invalid("schema record exceeds limit")
    }
    ignore(decode_schema(body, limits~))
  }
  let channel_ids : Map[Int, Bool] = {}
  for channel in log.channels {
    if channel_ids.contains(channel.id) {
      raise Invalid("duplicate channel id")
    }
    channel_ids[channel.id] = true
    if channel.schema_id != 0 && !schema_ids.contains(channel.schema_id) {
      raise Invalid("channel references missing schema")
    }
    let body = encode_channel(channel)
    if body.length() > limits.max_record_bytes {
      raise Invalid("channel record exceeds limit")
    }
    ignore(decode_channel(body, limits~))
  }
  if log.messages.length() > limits.max_records {
    raise Invalid("message budget exceeded")
  }
  for message in log.messages {
    if !channel_ids.contains(message.channel_id) {
      raise Invalid("message references missing channel")
    }
    if message.data.length() > limits.max_record_bytes - 22 {
      raise Invalid("message record exceeds limit")
    }
  }
  let header = encode_header(log.header)
  if header.length() > limits.max_record_bytes {
    raise Invalid("Header record exceeds limit")
  }
  ignore(decode_header(header, limits~))
  for metadata in log.metadata {
    let body = encode_metadata(metadata)
    if body.length() > limits.max_record_bytes {
      raise Invalid("Metadata record exceeds limit")
    }
    ignore(decode_metadata(body, limits~))
  }
  for attachment in log.attachments {
    let body = encode_attachment(attachment)
    if body.length() > limits.max_record_bytes {
      raise Invalid("Attachment exceeds record budget")
    }
    ignore(decode_attachment(body, limits~))
  }
  let total = log.attachments.length() +
    log.schemas.length() +
    log.channels.length() +
    log.messages.length() +
    log.metadata.length() +
    3
  if total > limits.max_records {
    raise Invalid("total record budget exceeded")
  }
}

///|
/// Minimal interoperable layout; zero summary pointers mean a sequential file.
pub fn write_log(
  log : Log,
  checksums? : Bool = true,
  limits? : Limits = default_limits(),
) -> Bytes raise McapError {
  validate_log(log, limits~)
  let out = @buffer.Buffer::Buffer()
  out.write_bytes(magic()[:])
  out.write_bytes(frame(0x01, encode_header(log.header))[:])
  for schema in log.schemas {
    out.write_bytes(frame(0x03, encode_schema(schema))[:])
  }
  for channel in log.channels {
    out.write_bytes(frame(0x04, encode_channel(channel))[:])
  }
  for message in log.messages {
    out.write_bytes(frame(0x05, encode_message(message))[:])
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
  out.to_bytes()
}
