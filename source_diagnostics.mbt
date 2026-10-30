///|
fn evidence_time(value : UInt64?) -> Json {
  match value {
    Some(value) => Json::string(value.to_string())
    None => Json::null()
  }
}

///|
fn source_issue_code(reason : String) -> DiagnosticCode {
  if reason.contains("magic") ||
    reason.contains("footer") ||
    reason.contains("file too short") {
    BadEnvelope
  } else if reason.contains("budget") || reason.contains("limit") {
    LimitExceeded
  } else if reason.contains("CRC") || reason.contains("checksum") {
    BadChecksum
  } else if reason.contains("compression") ||
    reason.contains("LZ4") ||
    reason.contains("Zstd") {
    BadCompression
  } else if reason.contains("truncat") || reason.contains("short read") {
    TruncatedInput
  } else if reason.contains("summary") || reason.contains("Summary") {
    BadSummary
  } else if reason.contains("index") || reason.contains("Index") {
    BadIndex
  } else if reason.contains("schema") || reason.contains("channel") {
    BadDeclaration
  } else {
    BadRecord
  }
}

///|
/// Strict bounded-record validation. Stops at the first failure; prefix parsing
/// is not a proof of whole-file integrity. Metadata is validated before data.
pub fn diagnose_source(
  source : ReadSource,
  limits~ : Limits = default_limits(),
  summary_bytes~ : Int32 = 8 * 1024 * 1024,
  read_bytes~ : Int32 = 1024 * 1024,
  cancelled~ : () -> Bool = fn() { false },
) -> Json raise McapError {
  if read_bytes <= 0 ||
    summary_bytes < 0 ||
    limits.max_record_bytes > 2147483638 {
    raise Invalid("invalid source diagnostic read or summary budget")
  }
  validate_log(empty_log(), limits~)
  let mut calls = 0UL
  let mut bytes = 0UL
  let mut peak_read = 0
  let mut peak_record = 0
  let bounded = new_read_source(source.size(), fn(offset, length) {
    if cancelled() {
      raise Invalid("source diagnosis cancelled")
    }
    let length = length.min(read_bytes)
    calls += 1UL
    peak_read = peak_read.max(length)
    let result = source.read_exact(offset, length)
    bytes += result.length().to_uint64()
    result
  })
  let mut phase = "metadata"
  let mut current : UInt64? = None
  let mut span : Int32? = None
  let mut opcode : Byte? = None
  let mut parsed = 0UL
  let mut messages = 0UL
  let mut chunks = 0UL
  let mut first : UInt64? = None
  let mut last : UInt64? = None
  let mut data_valid : Bool? = None
  let mut crc_verified = false
  let mut expected_crc : UInt64? = None
  let mut actual_crc : UInt64? = None
  let mut scope = "metadata or envelope"
  let failure = try {
    ignore(
      scan_source(
        bounded,
        source_visitor(message=fn(message, _) {
          messages += 1UL
          first = Some(
            match first {
              Some(value) =>
                if value < message.log_time {
                  value
                } else {
                  message.log_time
                }
              None => message.log_time
            },
          )
          last = Some(
            match last {
              Some(value) =>
                if value > message.log_time {
                  value
                } else {
                  message.log_time
                }
              None => message.log_time
            },
          )
        }),
        limits~,
        declaration_bytes=summary_bytes,
        cancelled~,
        trace=fn(event) {
          match event {
            RecordStart(at) => {
              phase = "data"
              scope = "physical record"
              current = Some(at)
              span = None
              opcode = None
            }
            RecordRead(_, op, length) => {
              opcode = Some(op)
              span = Some(length)
              peak_record = peak_record.max(length)
            }
            RecordAccepted(at, op, length) => {
              parsed = at + length.to_uint64()
              if op == 0x06 {
                chunks += 1UL
              }
            }
            DataChecksum(_, expected, actual) =>
              if expected != 0U && expected != actual {
                scope = "whole data section"
                expected_crc = Some(expected.to_uint64())
                actual_crc = Some(actual.to_uint64())
              }
            DataValidated(at, verified) => {
              data_valid = Some(true)
              crc_verified = verified
              parsed = at + 13UL
            }
            SummaryStart(_) => {
              phase = "summary"
              scope = "summary semantics"
              current = None
              opcode = None
              span = None
            }
          }
        },
      ),
    )
    None
  } catch {
    Invalid(reason) => Some(reason)
  }
  if cancelled() {
    raise Invalid("source diagnosis cancelled")
  }
  if failure is Some(_) && phase == "data" && data_valid is None {
    data_valid = Some(false)
  }
  let issues : Array[Json] = []
  if failure is Some(reason) {
    issues.push({
      "code": source_issue_code(reason).name(),
      "message": if reason.length() > 1024 {
        reason[0:1024].to_string()
      } else {
        reason
      },
      "phase": phase,
      "scope": scope,
      "offset": evidence_time(current),
      "opcode": match opcode {
        Some(value) => Json::number(value.to_int().to_double())
        None => Json::null()
      },
      "expected_crc": evidence_time(expected_crc),
      "actual_crc": evidence_time(actual_crc),
    })
  }
  let failed_range : Json = match (current, span) {
    (Some(start), Some(length)) if failure is Some(_) =>
      {
        "start": start.to_string(),
        "end": (start + length.to_uint64()).to_string(),
        "basis": "physical frame read; does not locate the exact corrupt byte",
      }
    _ => Json::null()
  }
  {
    "valid": failure is None,
    "source_complete": failure is None,
    "source_bytes": source.size().to_string(),
    "data_valid": match data_valid {
      Some(value) => Json::boolean(value)
      None => Json::null()
    },
    "data_crc_verified": crc_verified,
    "observed_messages": messages.to_string(),
    "accepted_chunks": chunks.to_string(),
    "observed_time_start": evidence_time(first),
    "observed_time_end": evidence_time(last),
    "parsed_data_prefix_end": parsed.to_string(),
    "prefix_is_integrity_proof": false,
    "failed_record_range": failed_range,
    "unchecked_range": if failure is Some(_) {
      Json::object({
        "start": parsed.to_string(),
        "end": source.size().to_string(),
        "basis": "scan stopped at first failure; metadata may have been read earlier",
      })
    } else {
      Json::null()
    },
    "unknown_lost_messages": Json::null(),
    "issues": issues,
    "io": {
      "bytes_read": bytes.to_string(),
      "read_calls": calls.to_string(),
      "peak_read_bytes": peak_read,
      "peak_record_span": peak_record,
    },
    "budgets": {
      "read_bytes": read_bytes,
      "record_bytes": limits.max_record_bytes,
      "decoded_chunk_bytes": limits.max_decompressed_bytes,
      "summary_bytes": summary_bytes,
      "total_records": limits.max_records,
    },
    "memory_mode": "bounded records, decoded chunks and declaration tables; not an RSS cap",
  }
}
