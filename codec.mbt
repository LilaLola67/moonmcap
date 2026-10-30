///|
pub fn encode_header(value : Header) -> Bytes {
  let out = @buffer.Buffer::Buffer()
  put_text(out, value.profile)
  put_text(out, value.library)
  out.to_bytes()
}

///|
pub fn decode_header(
  bytes : Bytes,
  limits~ : Limits = default_limits(),
) -> Header raise McapError {
  let input = cursor(bytes, limits)
  { profile: input.text(), library: input.text(), }
}

///|
pub fn encode_schema(value : Schema) -> Bytes raise McapError {
  if value.id == 0 {
    raise Invalid("schema id must be nonzero")
  }
  let out = @buffer.Buffer::Buffer()
  put_u16(out, value.id)
  put_text(out, value.name)
  put_text(out, value.encoding)
  put_blob(out, value.data)
  out.to_bytes()
}

///|
pub fn decode_schema(
  bytes : Bytes,
  limits~ : Limits = default_limits(),
) -> Schema raise McapError {
  let input = cursor(bytes, limits)
  let id = input.u16()
  if id == 0 {
    raise Invalid("schema id must be nonzero")
  }
  let name = input.text()
  let encoding = input.text()
  let data = input.blob()
  if encoding == "" && !data.is_empty() {
    raise Invalid("schema without encoding has nonempty data")
  }
  { id, name, encoding, data, }
}

///|
pub fn encode_channel(value : Channel) -> Bytes raise McapError {
  let out = @buffer.Buffer::Buffer()
  put_u16(out, value.id)
  put_u16(out, value.schema_id)
  put_text(out, value.topic)
  put_text(out, value.message_encoding)
  put_pairs(out, value.metadata)
  out.to_bytes()
}

///|
pub fn decode_channel(
  bytes : Bytes,
  limits~ : Limits = default_limits(),
) -> Channel raise McapError {
  let input = cursor(bytes, limits)
  let id = input.u16()
  let schema_id = input.u16()
  let topic = input.text()
  let message_encoding = input.text()
  let metadata = input.pairs()
  { id, schema_id, topic, message_encoding, metadata, }
}

///|
pub fn encode_message(value : Message) -> Bytes raise McapError {
  let out = @buffer.Buffer::Buffer()
  put_u16(out, value.channel_id)
  out.write_uint_le(value.sequence)
  out.write_uint64_le(value.log_time)
  out.write_uint64_le(value.publish_time)
  out.write_bytes(value.data[:])
  out.to_bytes()
}

///|
pub fn decode_message(
  bytes : Bytes,
  limits~ : Limits = default_limits(),
) -> Message raise McapError {
  let input = cursor(bytes, limits)
  let channel_id = input.u16()
  let sequence = input.u32()
  let log_time = input.u64()
  let publish_time = input.u64()
  let data = input.take(bytes.length() - input.pos)
  { channel_id, sequence, log_time, publish_time, data, }
}

///|
pub fn encode_footer(value : Footer) -> Bytes {
  let out = @buffer.Buffer::Buffer()
  out.write_uint64_le(value.summary_start)
  out.write_uint64_le(value.summary_offset_start)
  out.write_uint_le(value.summary_crc)
  out.to_bytes()
}

///|
pub fn decode_footer(bytes : Bytes) -> Footer raise McapError {
  let input = cursor(bytes, default_limits())
  let summary_start = input.u64()
  let summary_offset_start = input.u64()
  let summary_crc = input.u32()
  input.finish()
  { summary_start, summary_offset_start, summary_crc, }
}

///|
pub fn encode_metadata(value : Metadata) -> Bytes raise McapError {
  let out = @buffer.Buffer::Buffer()
  put_text(out, value.name)
  put_pairs(out, value.entries)
  out.to_bytes()
}

///|
pub fn decode_metadata(
  bytes : Bytes,
  limits~ : Limits = default_limits(),
) -> Metadata raise McapError {
  let input = cursor(bytes, limits)
  { name: input.text(), entries: input.pairs(), }
}
