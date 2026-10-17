///|
pub(all) enum DiagnosticCode {
  BadEnvelope
  BadRecord
  BadDeclaration
  BadCompression
  BadChecksum
  BadIndex
  BadSummary
  TruncatedInput
  LimitExceeded
} derive(Eq, @debug.Debug)

///|
pub(all) struct Diagnostic {
  code : DiagnosticCode
  offset : UInt64?
  opcode : Byte?
  message : String
  expected : UInt64?
  actual : UInt64?
} derive(Eq, @debug.Debug)

///|
pub(all) struct FileDiagnosis {
  valid : Bool
  data_valid : Bool
  data_crc_verified : Bool
  messages : Int
  chunks : Int
  data_end : UInt64?
  issues : Array[Diagnostic]
} derive(Eq, @debug.Debug)

///|
struct DataInspection {
  log : Log
  chunks : Int
  end : Int?
  issue : Diagnostic?
}

///|
fn diagnostic(
  code : DiagnosticCode,
  message : String,
  offset? : UInt64? = None,
  opcode? : Byte? = None,
  expected? : UInt64? = None,
  actual? : UInt64? = None,
) -> Diagnostic {
  { code, offset, opcode, message, expected, actual, }
}

///|
fn inspect_data(bytes : Bytes, limits : Limits) -> DataInspection {
  let mut log = empty_log()
  let mut chunks = 0
  let mut position = 8
  let mut records = 0
  let limits_issue = try {
    validate_log(log, limits~)
    None
  } catch {
    Invalid(reason) => Some(diagnostic(LimitExceeded, reason))
  }
  if limits_issue is Some(_) {
    return { log, chunks, end: None, issue: limits_issue, }
  }
  if bytes.length() < 8 || bytes[0:8].to_owned() != magic() {
    return {
      log,
      chunks,
      end: None,
      issue: Some(
        diagnostic(BadEnvelope, "missing leading magic", offset=Some(0UL)),
      ),
    }
  }
  while position < bytes.length() {
    let at = Some(position.to_uint64())
    if records >= limits.max_records {
      return {
        log,
        chunks,
        end: None,
        issue: Some(
          diagnostic(LimitExceeded, "record count exceeds budget", offset=at),
        ),
      }
    }
    if bytes.length() - position < 9 {
      return {
        log,
        chunks,
        end: None,
        issue: Some(
          diagnostic(
            TruncatedInput,
            "truncated record header",
            offset=at,
            expected=Some(9UL),
            actual=Some((bytes.length() - position).to_uint64()),
          ),
        ),
      }
    }
    let header = cursor(bytes[position:position + 9].to_owned(), limits)
    let (opcode, length) = (header.take(1)[0], header.u64()) catch {
      _ =>
        return {
          log,
          chunks,
          end: None,
          issue: Some(diagnostic(BadRecord, "invalid record header", offset=at)),
        }
    }
    if length > limits.max_record_bytes.to_uint64() {
      return {
        log,
        chunks,
        end: None,
        issue: Some(
          diagnostic(
            LimitExceeded,
            "record length exceeds budget",
            offset=at,
            opcode=Some(opcode),
            expected=Some(limits.max_record_bytes.to_uint64()),
            actual=Some(length),
          ),
        ),
      }
    }
    if length > (bytes.length() - position - 9).to_uint64() {
      return {
        log,
        chunks,
        end: None,
        issue: Some(
          diagnostic(
            TruncatedInput,
            "truncated record body",
            offset=at,
            opcode=Some(opcode),
            expected=Some(length),
            actual=Some((bytes.length() - position - 9).to_uint64()),
          ),
        ),
      }
    }
    let size = length.to_int()
    let body = bytes[position + 9:position + 9 + size].to_owned()
    let issue = try {
      if records == 0 && opcode != 0x01 {
        raise Invalid("first record must be Header")
      }
      match opcode {
        0x01 => {
          if records != 0 {
            raise Invalid("duplicate Header")
          }
          log = { ..log, header: decode_header(body, limits~), }
        }
        0x03 => add_schema(log, decode_schema(body, limits~))
        0x04 => add_channel(log, decode_channel(body, limits~))
        0x05 => add_message(log, decode_message(body, limits~), limits)
        0x06 => {
          let chunk = decode_chunk(body, limits~)
          if chunk.records.length() > limits.max_records - records - 1 {
            raise Invalid("nested record budget exceeded")
          }
          let candidate = {
            ..log,
            schemas: log.schemas.copy(),
            channels: log.channels.copy(),
            messages: [],
          }
          for nested in chunk.records {
            match nested.opcode {
              0x03 => add_schema(candidate, decode_schema(nested.body, limits~))
              0x04 =>
                add_channel(candidate, decode_channel(nested.body, limits~))
              0x05 =>
                add_message(
                  candidate,
                  decode_message(nested.body, limits~),
                  limits,
                )
              _ => ()
            }
          }
          for message in candidate.messages {
            log.messages.push(message)
          }
          log = { ..candidate, messages: log.messages, }
          records += chunk.records.length()
          chunks += 1
        }
        0x09 => log.attachments.push(decode_attachment(body, limits~))
        0x0c => log.metadata.push(decode_metadata(body, limits~))
        0x0f => {
          let input = cursor(body, limits)
          let expected = input.u32()
          input.finish()
          let actual = crc32(bytes[0:position].to_owned())
          if expected != 0U && expected != actual {
            return {
              log,
              chunks,
              end: None,
              issue: Some(
                diagnostic(
                  BadChecksum,
                  "data section CRC mismatch",
                  offset=at,
                  opcode=Some(opcode),
                  expected=Some(expected.to_uint64()),
                  actual=Some(actual.to_uint64()),
                ),
              ),
            }
          }
          validate_log(log, limits~)
          return { log, chunks, end: Some(position), issue: None, }
        }
        0x00 | 0x02 | 0x08 | 0x0a | 0x0b | 0x0d | 0x0e =>
          raise Invalid("record not allowed in data section")
        _ => ()
      }
      None
    } catch {
      Invalid(reason) => {
        let code = if reason.contains("CRC mismatch") {
          BadChecksum
        } else if reason.contains("budget") || reason.contains("limit") {
          LimitExceeded
        } else if reason.contains("schema") ||
          reason.contains("channel") ||
          reason.contains("Channel") ||
          reason.contains("Schema") {
          BadDeclaration
        } else if reason.contains("Zstd") ||
          reason.contains("LZ4") ||
          reason.contains("compression") ||
          reason.contains("frame") {
          BadCompression
        } else if opcode == 0x03 || opcode == 0x04 || opcode == 0x05 {
          BadDeclaration
        } else {
          BadRecord
        }
        Some(diagnostic(code, reason, offset=at, opcode=Some(opcode)))
      }
    }
    if issue is Some(_) {
      return { log, chunks, end: None, issue, }
    }
    position += 9 + size
    records += 1
  }
  {
    log,
    chunks,
    end: None,
    issue: Some(
      diagnostic(
        TruncatedInput,
        "missing DataEnd",
        offset=Some(position.to_uint64()),
      ),
    ),
  }
}

///|
/// Full validation; precise offsets are supplied only when directly known.
pub fn diagnose_file(
  bytes : Bytes,
  limits? : Limits = default_limits(),
) -> FileDiagnosis {
  let data = inspect_data(bytes, limits)
  let issues = []
  if data.issue is Some(issue) {
    issues.push(issue)
  }
  if data.end is Some(_) {
    let envelope_issue = try {
      ignore(scan_file(bytes, limits~))
      None
    } catch {
      Invalid(reason) =>
        Some(
          diagnostic(
            if reason.contains("CRC mismatch") {
              BadChecksum
            } else {
              BadEnvelope
            },
            reason,
          ),
        )
    }
    if envelope_issue is Some(issue) {
      issues.push(issue)
    } else {
      let summary_issue = try {
        ignore(validate_summary(bytes, limits~))
        None
      } catch {
        Invalid(reason) => Some(diagnostic(BadSummary, reason))
      }
      if summary_issue is Some(issue) {
        issues.push(issue)
      }
      let index_issue = try {
        ignore(validate_file_indexes(bytes, limits~))
        ignore(validate_attachment_indexes(bytes, limits~))
        ignore(validate_metadata_indexes(bytes, limits~))
        None
      } catch {
        Invalid(reason) => Some(diagnostic(BadIndex, reason))
      }
      if index_issue is Some(issue) {
        issues.push(issue)
      }
      if issues.is_empty() {
        ignore(read_log(bytes, limits~)) catch {
          Invalid(reason) => issues.push(diagnostic(BadSummary, reason))
        }
      }
    }
  }
  {
    valid: issues.is_empty(),
    data_valid: data.end is Some(_),
    data_crc_verified: match data.end {
      Some(position) =>
        bytes[position + 9:position + 13].to_owned() != b"\x00\x00\x00\x00"
      None => false
    },
    messages: data.log.messages.length(),
    chunks: data.chunks,
    data_end: data.end.map(fn(offset) { offset.to_uint64() }),
    issues,
  }
}

///|
pub(all) struct IndexRepair {
  bytes : Bytes
  original : FileDiagnosis
  result : FileDiagnosis
}

///|
/// Rebuild only from a complete, checked data section. Never salvage across damage.
pub fn rebuild_indexes(
  bytes : Bytes,
  compression? : Compression = Uncompressed,
  messages_per_chunk? : Int = 256,
  limits? : Limits = default_limits(),
) -> IndexRepair raise McapError {
  let data = inspect_data(bytes, limits)
  if data.end is None || data.issue is Some(_) {
    raise Invalid(
      "index rebuild requires a complete valid data section; use explicit prefix recovery for damaged data",
    )
  }
  let output = write_indexed(
    data.log,
    compression~,
    messages_per_chunk~,
    limits~,
  )
  let result = diagnose_file(output, limits~)
  if !result.valid {
    raise Invalid("rebuilt output did not pass validation")
  }
  { bytes: output, original: diagnose_file(bytes, limits~), result, }
}

///|
pub fn DiagnosticCode::name(self : DiagnosticCode) -> String {
  match self {
    BadEnvelope => "bad_envelope"
    BadRecord => "bad_record"
    BadDeclaration => "bad_declaration"
    BadCompression => "bad_compression"
    BadChecksum => "bad_checksum"
    BadIndex => "bad_index"
    BadSummary => "bad_summary"
    TruncatedInput => "truncated_input"
    LimitExceeded => "limit_exceeded"
  }
}

///|
pub fn diagnosis_json(report : FileDiagnosis) -> Json {
  {
    "valid": report.valid,
    "data_valid": report.data_valid,
    "data_crc_verified": report.data_crc_verified,
    "messages": report.messages,
    "chunks": report.chunks,
    "data_end": report.data_end.map(fn(v) { v.to_string() }),
    "issues": report.issues.map(fn(d) -> Json {
      {
        "code": d.code.name(),
        "offset": d.offset.map(fn(v) { v.to_string() }),
        "opcode": d.opcode.map(fn(v) { v.to_int() }),
        "message": d.message,
        "expected": d.expected.map(fn(v) { v.to_string() }),
        "actual": d.actual.map(fn(v) { v.to_string() }),
      }
    }),
  }
}
