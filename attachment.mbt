///|
pub(all) struct Attachment {
  log_time : UInt64
  create_time : UInt64
  name : String
  media_type : String
  data : Bytes
} derive(Eq, @debug.Debug)

///|
pub extend Attachment with Eq::{equal, not_equal}

///|
pub extend Attachment with @debug.Debug::{to_repr}

///|
pub fn encode_attachment(value : Attachment, checksums~ : Bool = true) -> Bytes {
  let out = @buffer.Buffer::Buffer()
  out.write_uint64_le(value.log_time)
  out.write_uint64_le(value.create_time)
  put_text(out, value.name)
  put_text(out, value.media_type)
  out.write_uint64_le(value.data.length().to_uint64())
  out.write_bytes(value.data[:])
  out.write_uint_le(if checksums { crc32(out.to_bytes()) } else { 0U })
  out.to_bytes()
}

///|
pub fn decode_attachment(
  bytes : Bytes,
  limits~ : Limits = default_limits(),
) -> Attachment raise McapError {
  let input = cursor(bytes, limits)
  let log_time = input.u64()
  let create_time = input.u64()
  let name = input.text()
  let media_type = input.text()
  let size = bounded_length(input.u64(), limits.max_record_bytes, "attachment")
  let data = input.take(size)
  let crc_end = input.pos
  let checksum = input.u32()
  verify_crc(bytes[0:crc_end].to_owned(), checksum, label="attachment")
  { log_time, create_time, name, media_type, data, }
}

///|
pub(all) struct AttachmentIndex {
  offset : UInt64
  length : UInt64
  log_time : UInt64
  create_time : UInt64
  data_size : UInt64
  name : String
  media_type : String
}

///|
pub fn encode_attachment_index(value : AttachmentIndex) -> Bytes {
  let out = @buffer.Buffer::Buffer()
  out.write_uint64_le(value.offset)
  out.write_uint64_le(value.length)
  out.write_uint64_le(value.log_time)
  out.write_uint64_le(value.create_time)
  out.write_uint64_le(value.data_size)
  put_text(out, value.name)
  put_text(out, value.media_type)
  out.to_bytes()
}

///|
pub fn decode_attachment_index(
  bytes : Bytes,
  limits~ : Limits = default_limits(),
) -> AttachmentIndex raise McapError {
  let input = cursor(bytes, limits)
  let offset = input.u64()
  let length = input.u64()
  let log_time = input.u64()
  let create_time = input.u64()
  let data_size = input.u64()
  let name = input.text()
  let media_type = input.text()
  { offset, length, log_time, create_time, data_size, name, media_type, }
}

///|
pub fn find_attachments(
  log : Log,
  name~ : String = "",
  media_type~ : String = "",
) -> Array[Attachment] {
  log.attachments
  .filter(fn(a) {
    (name == "" || a.name == name) &&
    (media_type == "" || a.media_type == media_type)
  })
  .map(fn(a) { { ..a, data: a.data.copy(), } })
}

///|
pub fn validate_attachment_indexes(
  bytes : Bytes,
  limits~ : Limits = default_limits(),
) -> Int32 raise McapError {
  let records = scan_file(bytes, limits~)
  let physical : Map[UInt64, Record] = {}
  for record in records {
    physical[record.offset.to_uint64()] = record
  }
  let seen : Map[UInt64, Bool] = {}
  let mut count = 0
  for record in records {
    if record.opcode != 0x0a {
      continue
    }
    let index = decode_attachment_index(record.body, limits~)
    if seen.contains(index.offset) {
      raise Invalid("duplicate AttachmentIndex")
    }
    seen[index.offset] = true
    let target = match physical.get(index.offset) {
      Some(value) => value
      None => raise Invalid("AttachmentIndex points outside record boundary")
    }
    if target.opcode != 0x09 ||
      index.length != (target.body.length() + 9).to_uint64() {
      raise Invalid("AttachmentIndex type or length mismatch")
    }
    let attachment = decode_attachment(target.body, limits~)
    if index.log_time != attachment.log_time ||
      index.create_time != attachment.create_time ||
      index.name != attachment.name ||
      index.media_type != attachment.media_type ||
      index.data_size != attachment.data.length().to_uint64() {
      raise Invalid("AttachmentIndex properties disagree with Attachment")
    }
    count += 1
  }
  if count != 0 && count != records.filter(fn(r) { r.opcode == 0x09 }).length() {
    raise Invalid("AttachmentIndex set does not cover attachments")
  }
  count
}
