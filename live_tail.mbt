///|
pub(all) struct LiveBatch {
  declarations : Log
  messages : Array[Message]
  evidence : Json
}

///|
pub struct GrowingTail {
  identity : String
  fingerprint : (ReadSource) -> String raise McapError
  limits : Limits
  batch_bytes : Int32
  mut offset : UInt64
  mut digest : String
  mut declarations : Log
  mut header_seen : Bool
  mut data_end : Bool
  mut complete : Bool
}

///|
fn live_prefix(source : ReadSource, size : UInt64) -> ReadSource {
  new_read_source(size, fn(offset, length) { source.read_exact(offset, length) })
}

///|
fn live_envelope(value : Json) -> Bytes raise McapError {
  let bytes = @utf8.encode(value.stringify()[:])
  if bytes.length() > 8 * 1024 * 1024 - 12 {
    raise Invalid("live snapshot exceeds 8 MiB")
  }
  b"MLIV1\r\n\x00" + bytes + crc32(bytes).mcap_le_bytes()
}

///|
fn live_value(bytes : Bytes) -> Json raise McapError {
  if bytes.length() < 12 ||
    bytes.length() > 8 * 1024 * 1024 ||
    bytes[0:8].to_owned() != b"MLIV1\r\n\x00" {
    raise Invalid("invalid live snapshot")
  }
  let data = bytes[8:bytes.length() - 4].to_owned()
  if crc32(data) !=
    cursor(bytes[bytes.length() - 4:].to_owned(), default_limits()).u32() {
    raise Invalid("live snapshot CRC mismatch")
  }
  parse_json_document(data, 8 * 1024 * 1024, 32)
}

///|
pub fn growing_tail(
  identity : String,
  fingerprint : (ReadSource) -> String raise McapError,
  snapshot? : Bytes,
  limits~ : Limits = default_limits(),
  batch_bytes~ : Int32 = 64 * 1024 * 1024,
) -> GrowingTail raise McapError {
  if identity == "" ||
    identity.length() > 256 ||
    batch_bytes < 1 ||
    limits.max_record_bytes < 1 ||
    limits.max_records < 1 {
    raise Invalid("invalid growing tail identity/budgets")
  }
  let tail : GrowingTail = {
    identity,
    fingerprint,
    limits,
    batch_bytes,
    offset: 0UL,
    digest: "",
    declarations: empty_log(),
    header_seen: false,
    data_end: false,
    complete: false,
  }
  if snapshot is Some(bytes) {
    let state = field_object(live_value(bytes))
    if field_text(field_get(state, "kind")) != "growing" ||
      field_text(field_get(state, "identity")) != identity {
      raise Invalid("live snapshot identity mismatch")
    }
    tail.offset = parse_decimal(field_text(field_get(state, "offset")))
    tail.digest = field_text(field_get(state, "digest"))
    if tail.offset > 0UL {
      ignore(checked_digest(tail.digest))
    }
    tail.declarations = read_log(
      decode_hex(field_text(field_get(state, "declarations"))),
    )
    tail.header_seen = field_bool(field_get(state, "header_seen"))
    tail.data_end = field_bool(field_get(state, "data_end"))
    tail.complete = field_bool(field_get(state, "complete"))
  }
  tail
}

///|
pub fn GrowingTail::snapshot(self : GrowingTail) -> Bytes raise McapError {
  live_envelope({
    "kind": "growing",
    "identity": self.identity,
    "offset": self.offset.to_string(),
    "digest": self.digest,
    "declarations": encode_hex(write_log(self.declarations)),
    "header_seen": self.header_seen,
    "data_end": self.data_end,
    "complete": self.complete,
  })
}

///|
/// Poll a stable size snapshot of an append-only file. Committed prefix hashes
/// are rechecked; incomplete record headers/bodies wait without advancing.
/// Returned early messages have only available per-record/chunk verification.
pub fn GrowingTail::poll(
  self : GrowingTail,
  source : ReadSource,
  units~ : Int32 = 32,
) -> LiveBatch raise McapError {
  if units < 1 || units > 4096 {
    raise Invalid("live poll units must be 1..4096")
  }
  if source.size() < self.offset {
    raise Invalid("growing source was truncated")
  }
  if self.offset > 0UL &&
    checked_digest((self.fingerprint)(live_prefix(source, self.offset))) !=
    self.digest {
    raise Invalid("growing source committed prefix changed")
  }
  if self.complete && source.size() != self.offset {
    raise Invalid("completed MCAP was extended")
  }
  let mut log = copy_log(self.declarations)
  let mut position = self.offset
  let mut header_seen = self.header_seen
  let mut data_end = self.data_end
  let mut complete = self.complete
  let mut used = 0
  let mut chunks = 0
  let mut checked_chunks = 0
  let mut output_bytes = 0
  if position == 0UL && source.size() >= 8UL {
    if source.read_exact(0UL, 8) != magic() {
      raise Invalid("growing source magic mismatch")
    }
    position = 8UL
  }
  while position > 0UL &&
        !data_end &&
        used < units &&
        source.size() - position >= 9UL {
    let header = cursor(source.read_exact(position, 9), self.limits)
    let opcode = header.take(1)[0]
    let length = header.u64()
    if length > self.limits.max_record_bytes.to_uint64() {
      raise Invalid("growing record exceeds byte limit")
    }
    if length > source.size() - position - 9UL {
      break
    }
    let record = source.record(position, source.size(), self.limits)
    if !header_seen && (position != 8UL || opcode != 0x01) {
      raise Invalid("growing source requires Header first")
    }
    let records = if opcode == 0x06 {
      let decoded = decode_chunk_internal(
        record.body,
        self.limits,
        self.limits.max_record_bytes,
      )
      chunks += 1
      if cursor(record.body[24:28].to_owned(), self.limits).u32() != 0U {
        checked_chunks += 1
      }
      decoded.records
    } else {
      [{ opcode, offset: 0, body: record.body, }]
    }
    for record in records {
      match record.opcode {
        0x01 => {
          if header_seen || opcode == 0x06 {
            raise Invalid("duplicate growing Header")
          }
          log = { ..log, header: decode_header(record.body), }
          header_seen = true
        }
        0x03 => add_schema(log, decode_schema(record.body, limits=self.limits))
        0x04 =>
          add_channel(log, decode_channel(record.body, limits=self.limits))
        0x05 => {
          let message = decode_message(record.body, limits=self.limits)
          if message.data.length() > self.batch_bytes - output_bytes - 31 {
            raise Invalid(
              "growing poll message byte budget exceeded; reduce units or chunk size",
            )
          }
          add_message(log, message, self.limits)
          output_bytes += message.data.length() + 31
        }
        0x0f => {
          let body = cursor(record.body, self.limits)
          ignore(body.u32())
          body.finish()
          data_end = true
        }
        0x02 | 0x0e => raise Invalid("footer/summary before DataEnd")
        0x06 => raise Invalid("nested Chunk")
        _ => ()
      }
    }
    let declaration_cost = encode_header(log.header).length().to_uint64() +
      log.schemas.fold(init=0UL, fn(total, s) {
        total +
        s.data.length().to_uint64() +
        ((s.name.length() + s.encoding.length()) * 4 + 32).to_uint64()
      }) +
      log.channels.fold(init=0UL, fn(total, c) {
        total + encode_channel(c).length().to_uint64()
      })
    if declaration_cost > 3UL * 1024UL * 1024UL {
      raise Invalid("growing declaration budget exceeded")
    }
    position += 9UL + length
    used += 1
  }
  if data_end &&
    source.size() >= position + 37UL &&
    source.read_exact(source.size() - 8UL, 8) == magic() {
    ignore(
      scan_source(source, source_visitor(), limits={
        ..self.limits,
        max_records: 2147483647,
      }),
    )
    position = source.size()
    complete = true
  }
  let digest = if position > 0UL {
    checked_digest((self.fingerprint)(live_prefix(source, position)))
  } else {
    ""
  }
  let messages = log.messages.copy()
  let declarations = { ..log, messages: [], }
  // Do not mutate the cursor until all decoding, checks and prefix hashing pass.
  self.offset = position
  self.digest = digest
  self.declarations = declarations
  self.header_seen = header_seen
  self.data_end = data_end
  self.complete = complete
  {
    declarations: copy_log(declarations),
    messages,
    evidence: {
      "verification": if complete {
        "complete-container"
      } else {
        "partial-prefix"
      },
      "offset": position.to_string(),
      "size": source.size().to_string(),
      "waiting_for_data": !complete && used < units,
      "chunks": chunks,
      "chunks_with_crc": checked_chunks,
      "complete": complete,
      "prefix_sha256": digest,
    },
  }
}

///|
pub struct RecordingTail {
  identity : String
  mut config : Json?
  accepted : Array[Json]
}

///|
pub fn recording_tail(
  identity : String,
  snapshot? : Bytes,
) -> RecordingTail raise McapError {
  if identity == "" || identity.length() > 256 {
    raise Invalid("invalid recording tail identity")
  }
  let tail : RecordingTail = { identity, config: None, accepted: [], }
  if snapshot is Some(bytes) {
    let state = field_object(live_value(bytes))
    if field_text(field_get(state, "kind")) != "recording" ||
      field_text(field_get(state, "identity")) != identity {
      raise Invalid("recording tail snapshot identity mismatch")
    }
    tail.config = match field_get(state, "config") {
      Null => None
      value => Some(value)
    }
    tail.accepted.append(field_array(field_get(state, "accepted")))
    if tail.accepted.length() > 4096 {
      raise Invalid("too many recording tail segments")
    }
  }
  tail
}

///|
pub fn RecordingTail::snapshot(self : RecordingTail) -> Bytes raise McapError {
  live_envelope({
    "kind": "recording",
    "identity": self.identity,
    "config": self.config.unwrap_or(Json::null()),
    "accepted": self.accepted,
  })
}

///|
/// One complete, manifest-committed segment per poll; ignores active .part data.
/// The open callback must provide immutable published segment snapshots.
pub fn RecordingTail::poll(
  self : RecordingTail,
  manifest : Bytes,
  open : (Int32) -> ReadSource raise McapError,
  fingerprint : (ReadSource) -> String raise McapError,
  batch_bytes~ : Int32 = 64 * 1024 * 1024,
) -> LiveBatch? raise McapError {
  if batch_bytes < 1 {
    raise Invalid("invalid recording tail byte limit")
  }
  let value = field_object(inspect_recording_manifest(manifest))
  let config = field_get(value, "config")
  if self.config is Some(old) && old != config {
    raise Invalid("recording tail configuration changed")
  }
  let segments = field_array(field_get(value, "segments"))
  if segments.length() < self.accepted.length() || segments.length() > 4096 {
    raise Invalid("recording committed prefix shrank or exceeds limit")
  }
  for i = 0; i < self.accepted.length(); i = i + 1 {
    if segments[i] != self.accepted[i] {
      raise Invalid("recording committed prefix changed")
    }
  }
  if segments.length() == self.accepted.length() {
    return None
  }
  let row = segments[self.accepted.length()]
  let fields = field_object(row)
  let ordinal_value = parse_decimal(field_get(fields, "ordinal").stringify())
  if ordinal_value > 4095UL {
    raise Invalid("recording ordinal exceeds limit")
  }
  let ordinal = ordinal_value.to_int()
  let source = open(ordinal)
  let size = parse_decimal(field_text(field_get(fields, "bytes")))
  if size != source.size() ||
    checked_digest(fingerprint(source)) !=
    field_text(field_get(fields, "sha256")) {
    raise Invalid("committed segment size/hash mismatch")
  }
  let log = empty_log()
  let stored = [0]
  ignore(
    scan_source(
      source,
      source_visitor(
        schema=fn(s) { add_schema(log, s) },
        channel=fn(c) { add_channel(log, c) },
        message=fn(m, _) {
          if m.data.length() > batch_bytes - stored[0] - 31 {
            raise Invalid("recording tail batch byte budget exceeded")
          }
          log.messages.push(m)
          stored[0] += m.data.length() + 31
        },
      ),
      limits={ ..default_limits(), max_records: 2147483647, },
    ),
  )
  if log.messages.length().to_uint64() !=
    parse_decimal(field_text(field_get(fields, "messages"))) {
    raise Invalid("recording segment count mismatch")
  }
  let header = source.record(8UL, source.size(), default_limits())
  let log = { ..log, header: decode_header(header.body), }
  self.config = Some(config)
  self.accepted.push(row)
  {
    declarations: { ..log, messages: [], },
    messages: log.messages,
    evidence: {
      "verification": "committed-segment",
      "ordinal": ordinal,
      "sha256": field_get(fields, "sha256"),
      "bytes": size.to_string(),
    },
  }
  |> Some
}
