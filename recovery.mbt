///|
pub(all) struct RecoveryReport {
  log : Log
  recovered_records : Int
  stop_offset : Int
  complete : Bool
  reason : String
}

///|
/// Recover only a verified contiguous prefix. Never resynchronize through corrupt bytes.
pub fn recover_prefix(bytes : Bytes, limits? : Limits = default_limits()) -> RecoveryReport raise McapError {
  if bytes.length() < 8 || bytes[0:8].to_owned() != magic() {
    raise Invalid("recovery requires leading MCAP magic")
  }
  if limits.max_records < 0 || limits.max_record_bytes < 0 {
    raise Invalid("invalid recovery limits")
  }
  let log = empty_log()
  let mut header : Header? = None
  let mut position = 8
  let mut count = 0
  let mut stopped = "end of input without DataEnd"
  let mut ended = false
  while position < bytes.length() {
    if count >= limits.max_records {
      stopped = "record budget exceeded"
      break
    }
    if bytes.length() - position < 9 {
      stopped = "truncated record header"
      break
    }
    let input = cursor(bytes[position:position + 9].to_owned(), limits)
    let opcode = input.take(1)[0]
    let raw_size = input.u64()
    if raw_size > limits.max_record_bytes.to_uint64() {
      stopped = "record size exceeds configured limit"
      break
    }
    let size = raw_size.to_int()
    if size > bytes.length() - position - 9 {
      stopped = "truncated record body"
      break
    }
    let body = bytes[position + 9:position + 9 + size].to_owned()
    let accepted = try {
      if count == 0 && opcode != 0x01 {
        raise Invalid("first record is not Header")
      }
      match opcode {
        0x01 => {
          if header is Some(_) { raise Invalid("duplicate Header") }
          header = Some(decode_header(body, limits~))
        }
        0x03 => add_schema(log, decode_schema(body, limits~))
        0x04 => add_channel(log, decode_channel(body, limits~))
        0x05 => add_message(log, decode_message(body, limits~), limits)
        0x06 => {
          // Validate a whole chunk before adding anything from it.
          let chunk = decode_chunk(body, limits~)
          let candidate = copy_log(log)
          for record in chunk.records {
            match record.opcode {
              0x03 => add_schema(candidate, decode_schema(record.body, limits~))
              0x04 => add_channel(candidate, decode_channel(record.body, limits~))
              0x05 => add_message(candidate, decode_message(record.body, limits~), limits)
              _ => ()
            }
          }
          log.schemas.clear()
          log.channels.clear()
          log.messages.clear()
          for schema in candidate.schemas { log.schemas.push(schema) }
          for channel in candidate.channels { log.channels.push(channel) }
          for message in candidate.messages { log.messages.push(message) }
        }
        0x09 => log.attachments.push(decode_attachment(body, limits~))
        0x0c => log.metadata.push(decode_metadata(body, limits~))
        0x0f => {
          let input = cursor(body, limits)
          let expected = input.u32()
          input.finish()
          verify_crc(bytes[0:position].to_owned(), expected, label="recovery DataEnd")
          ended = true
        }
        0x02 => raise Invalid("Footer before DataEnd")
        0x00 => raise Invalid("reserved opcode zero")
        _ => ()
      }
      true
    } catch {
      Invalid(reason) => { stopped = reason; false }
    }
    if !accepted { break }
    position += 9 + size
    count += 1
    if ended { break }
  }
  let header = match header {
    Some(value) => value
    None => raise Invalid("no complete valid Header to recover")
  }
  let log = { ..log, header }
  let complete = if ended {
    try {
      ignore(read_log(bytes, limits~))
      true
    } catch { _ => false }
  } else { false }
  if ended {
    stopped = if complete { "complete file" } else { "valid data section; summary or footer incomplete" }
  }
  { log, recovered_records: count, stop_offset: position, complete, reason: stopped }
}

///|
/// Rewrites a recoverable prefix as a fresh valid sequential MCAP file.
pub fn repair_prefix(bytes : Bytes, limits? : Limits = default_limits()) -> (Bytes, RecoveryReport) raise McapError {
  let report = recover_prefix(bytes, limits~)
  (write_log(report.log, limits~), report)
}
