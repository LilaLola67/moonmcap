///|
pub(all) enum SequencePolicy {
  PreserveSequence
  ContinueSequence
} derive(Eq, @debug.Debug)

///|
pub(all) struct RecordingOptions {
  stream : StreamOptions
  segment_messages : Int32
  segment_bytes_target : UInt64
  segment_span : UInt64
  segment_limit : Int32
  sequences : SequencePolicy
  source_identity : String
} derive(Eq, @debug.Debug)

///|
pub fn recording_options() -> RecordingOptions {
  {
    stream: stream_options(),
    segment_messages: 10000,
    segment_bytes_target: 0UL,
    segment_span: 0UL,
    segment_limit: 4096,
    sequences: PreserveSequence,
    source_identity: "",
  }
}

///|
/// save/seal must return only after their durable atomic publication. create
/// owns a new empty segment; quarantine preserves its bytes outside the set.
pub struct RecordingStorage {
  create : (Int32) -> WriteSink raise McapError
  open : (Int32) -> ReadSource raise McapError
  exists : (Int32) -> Bool raise McapError
  seal : (Int32) -> Unit raise McapError
  save : (Bytes) -> Unit raise McapError
  quarantine : (Int32) -> Unit raise McapError
  list : () -> Array[Int32] raise McapError
  fingerprint : (ReadSource) -> String raise McapError
}

///|
pub fn recording_storage(
  create : (Int32) -> WriteSink raise McapError,
  open : (Int32) -> ReadSource raise McapError,
  exists : (Int32) -> Bool raise McapError,
  seal : (Int32) -> Unit raise McapError,
  save : (Bytes) -> Unit raise McapError,
  quarantine : (Int32) -> Unit raise McapError,
  list : () -> Array[Int32] raise McapError,
  fingerprint : (ReadSource) -> String raise McapError,
) -> RecordingStorage {
  { create, open, exists, seal, save, quarantine, list, fingerprint, }
}

///|
pub struct RecordingSession {
  template : Log
  config : Json
  options : RecordingOptions
  storage : RecordingStorage
  completed : Array[Json]
  mut next_ordinal : Int32
  mut active : Int32?
  mut writer : StreamWriter?
  mut next_sequence : Map[Int32, UInt64]
  mut committed_next : Map[Int32, UInt64]
  mut total : UInt64
  mut start_time : UInt64
  mut end_time : UInt64
  mut state : WriterState
  mut adopted : Int32
  mut quarantined : Int32
}

///|
fn recording_config(
  template : Log,
  options : RecordingOptions,
) -> Json raise McapError {
  validate_log(template)
  if !template.messages.is_empty() ||
    !template.attachments.is_empty() ||
    !template.metadata.is_empty() ||
    template.channels.length() > 1024 ||
    template.schemas.length() > 1024 ||
    options.segment_messages < 1 ||
    options.segment_messages > 50000 ||
    options.segment_limit < 1 ||
    options.segment_limit > 4096 ||
    options.source_identity.length() > 256 {
    raise Invalid("invalid recording declarations or segment settings")
  }
  let bytes = write_log(template)
  if bytes.length() > 1024 * 1024 {
    raise Invalid("recording declarations exceed budget")
  }
  let check = new_stream_writer(
    new_write_sink(fn(_) { () }),
    options=options.stream,
  )
  check.cancel()
  {
    "declarations": encode_hex(bytes),
    "chunk_bytes": options.stream.chunk_bytes,
    "chunk_messages": options.stream.chunk_messages,
    "chunk_span": options.stream.chunk_span.to_string(),
    "summary_bytes": options.stream.summary_bytes,
    "compression": match options.stream.compression {
      Uncompressed => ""
      Lz4 => "lz4"
      Zstd => "zstd"
    },
    "segment_messages": options.segment_messages,
    "segment_bytes_target": options.segment_bytes_target.to_string(),
    "segment_span": options.segment_span.to_string(),
    "segment_limit": options.segment_limit,
    "sequence_policy": if options.sequences == ContinueSequence {
      "continue"
    } else {
      "preserve"
    },
    "source_identity": options.source_identity,
  }
}

///|
pub fn inspect_recording_manifest(bytes : Bytes) -> Json raise McapError {
  if bytes.length() < 16 ||
    bytes.length() > 8 * 1024 * 1024 ||
    bytes[0:8].to_owned() != b"MRSC1\r\n\x00" {
    raise Invalid("invalid recording manifest envelope")
  }
  let length = cursor(bytes[8:12].to_owned(), default_limits()).u32().to_int()
  if length < 0 || length != bytes.length() - 16 {
    raise Invalid("recording manifest length mismatch")
  }
  let data = bytes[12:12 + length].to_owned()
  if crc32(data) !=
    cursor(bytes[12 + length:].to_owned(), default_limits()).u32() {
    raise Invalid("recording manifest CRC mismatch")
  }
  let json = parse_json_document(data, 8 * 1024 * 1024, 32)
  let object = field_object(json)
  if field_get(object, "version") != Json::number(1) {
    raise Invalid("unsupported recording manifest version")
  }
  json
}

///|
fn RecordingSession::save(
  self : RecordingSession,
  clean : Bool,
) -> Unit raise McapError {
  let value : Json = {
    "version": 1,
    "config": self.config,
    "next_ordinal": self.next_ordinal,
    "active": match self.active {
      None => Json::null()
      Some(id) => Json::number(id.to_double())
    },
    "committed_messages": self.total.to_string(),
    "clean_stop": clean,
    "segments": Json::array(self.completed.copy()),
  }
  let bytes = @utf8.encode(value.stringify()[:])
  if bytes.length() > 8 * 1024 * 1024 - 16 {
    raise Invalid("recording manifest byte budget exceeded")
  }
  let envelope = b"MRSC1\r\n\x00" +
    bytes.length().to_uint().mcap_le_bytes() +
    bytes +
    crc32(bytes).mcap_le_bytes()
  ignore(inspect_recording_manifest(envelope))
  (self.storage.save)(envelope)
}

///|
fn sequence_json(sequences : Map[Int32, UInt64]) -> Json {
  let ids = sequences.keys().to_array()
  ids.sort()
  Json::array(
    ids.map(fn(id) -> Json {
      [Json::number(id.to_double()), Json::string(sequences[id].to_string())]
    }),
  )
}

///|
/// Revalidate the complete container and derive counters from its actual data.
fn RecordingSession::summarize(
  self : RecordingSession,
  id : Int32,
) -> (Json, Map[Int32, UInt64]) raise McapError {
  let source = (self.storage.open)(id)
  let header = source.record(8UL, source.size(), default_limits())
  if header.opcode != 0x01 ||
    decode_header(header.body).profile != self.template.header.profile {
    raise Invalid("recording segment profile mismatch")
  }
  let schemas : Map[Int32, Bool] = Map([])
  let channels : Map[Int32, Bool] = Map([])
  let next = self.committed_next.copy()
  let count = [0UL]
  let times = [0UL, 0UL]
  ignore(
    scan_source(
      source,
      source_visitor(
        schema=fn(s) {
          if !self.template.schemas.contains(s) {
            raise Invalid("recording segment schema changed")
          }
          schemas[s.id] = true
        },
        channel=fn(c) {
          if !self.template.channels.contains(c) {
            raise Invalid("recording segment channel changed")
          }
          channels[c.id] = true
        },
        message=fn(m, _) {
          let sequence = next.get(m.channel_id).unwrap_or(0UL)
          if self.options.sequences == ContinueSequence &&
            (sequence > 4294967295UL || m.sequence.to_uint64() != sequence) {
            raise Invalid("recording continuation sequence mismatch")
          }
          let following = m.sequence.to_uint64() + 1UL
          next[m.channel_id] = if following > sequence {
            following
          } else {
            sequence
          }
          if count[0] == 0UL {
            times[0] = m.log_time
            times[1] = m.log_time
          }
          if m.log_time < times[0] {
            times[0] = m.log_time
          }
          if m.log_time > times[1] {
            times[1] = m.log_time
          }
          count[0] += 1UL
        },
        attachment=fn(_) {
          raise Invalid("recording segment has unexpected attachments")
        },
        metadata=fn(_) {
          raise Invalid("recording segment has unexpected metadata")
        },
      ),
    ),
  )
  if schemas.length() != self.template.schemas.length() ||
    channels.length() != self.template.channels.length() ||
    count[0] == 0UL ||
    count[0] > self.options.segment_messages.to_uint64() {
    raise Invalid("recording segment declarations or count mismatch")
  }
  let digest = checked_digest((self.storage.fingerprint)(source))
  (
    {
      "ordinal": id,
      "bytes": source.size().to_string(),
      "sha256": digest,
      "messages": count[0].to_string(),
      "start_time": times[0].to_string(),
      "end_time": times[1].to_string(),
      "next_sequence": sequence_json(next),
    },
    next,
  )
}

///|
fn RecordingSession::accept_segment(
  self : RecordingSession,
  row : Json,
  next : Map[Int32, UInt64],
) -> Unit raise McapError {
  let messages = parse_decimal(
    field_text(field_get(field_object(row), "messages")),
  )
  if messages > 18446744073709551615UL - self.total {
    raise Invalid("recording message total overflow")
  }
  self.completed.push(row)
  self.total += messages
  self.committed_next = next
  self.next_sequence = next.copy()
  self.active = None
}

///|
pub fn open_recording_session(
  template : Log,
  storage : RecordingStorage,
  manifest? : Bytes,
  options~ : RecordingOptions = recording_options(),
) -> RecordingSession raise McapError {
  let config = recording_config(template, options)
  let session : RecordingSession = {
    template: copy_log(template),
    config,
    options,
    storage,
    completed: [],
    next_ordinal: 0,
    active: None,
    writer: None,
    next_sequence: Map([]),
    committed_next: Map([]),
    total: 0UL,
    start_time: 0UL,
    end_time: 0UL,
    state: WriterOpen,
    adopted: 0,
    quarantined: 0,
  }
  if manifest is Some(bytes) {
    let saved = field_object(inspect_recording_manifest(bytes))
    if field_get(saved, "config") != config {
      raise Invalid("recording configuration or source identity mismatch")
    }
    session.next_ordinal = bounded_length(
      parse_decimal(field_get(saved, "next_ordinal").stringify()),
      options.segment_limit,
      "recording ordinal",
    )
    let active = field_get(saved, "active")
    if active != Json::null() {
      let id = bounded_length(
        parse_decimal(active.stringify()),
        options.segment_limit - 1,
        "active segment",
      )
      if id + 1 != session.next_ordinal {
        raise Invalid("invalid active recording ordinal")
      }
      session.active = Some(id)
    }
    let completed = field_array(field_get(saved, "segments"))
    if completed.length() > options.segment_limit {
      raise Invalid("recording segment count exceeded")
    }
    let ids : Map[Int32, Bool] = Map([])
    let mut previous = -1
    for row in completed {
      let entry = field_object(row)
      let id = bounded_length(
        parse_decimal(field_get(entry, "ordinal").stringify()),
        options.segment_limit - 1,
        "segment ordinal",
      )
      if id <= previous ||
        id >= session.next_ordinal ||
        session.active == Some(id) {
        raise Invalid("recording segment ordinal order mismatch")
      }
      previous = id
      ids[id] = true
      let (actual, next) = session.summarize(id)
      if actual != row {
        raise Invalid("committed recording segment integrity mismatch")
      }
      // accept_segment normally clears active; preserve its reserved ordinal here.
      let active = session.active
      session.accept_segment(actual, next)
      session.active = active
    }
    if parse_decimal(field_text(field_get(saved, "committed_messages"))) !=
      session.total {
      raise Invalid("recording committed total mismatch")
    }
    for id in (storage.list)() {
      if !ids.contains(id) && session.active != Some(id) {
        raise Invalid("unreserved file in recording session")
      }
    }
    if session.active is Some(id) {
      if (storage.exists)(id) {
        let result = Some(session.summarize(id)) catch { _ => None }
        match result {
          Some((row, next)) => {
            (storage.seal)(id)
            session.accept_segment(row, next)
            session.adopted += 1
          }
          None => {
            (storage.quarantine)(id)
            session.active = None
            session.quarantined += 1
          }
        }
      } else {
        session.active = None
      }
    }
  } else if !(storage.list)().is_empty() {
    raise Invalid("new recording storage is not empty")
  }
  session.save(false)
  session
}

///|
fn RecordingSession::start(self : RecordingSession) -> Unit raise McapError {
  if self.next_ordinal >= self.options.segment_limit {
    raise Invalid("recording segment limit exceeded")
  }
  let id = self.next_ordinal
  self.next_ordinal += 1
  self.active = Some(id)
  self.save(false) // reserve durably before creating the first byte
  let writer = new_stream_writer(
    (self.storage.create)(id),
    profile=self.template.header.profile,
    options=self.options.stream,
  )
  self.writer = Some(writer)
  for schema in self.template.schemas {
    writer.add_schema(schema)
  }
  for channel in self.template.channels {
    writer.add_channel(channel)
  }
}

///|
fn RecordingSession::seal(self : RecordingSession) -> Unit raise McapError {
  if self.writer is Some(writer) {
    ignore(writer.finish())
    let id = self.active.unwrap()
    (self.storage.seal)(id)
    let (row, next) = self.summarize(id)
    let prior_total = self.total
    let prior_next = self.committed_next
    let prior_active = self.active
    self.accept_segment(row, next)
    self.writer = None
    self.save(false) catch {
      error => {
        ignore(self.completed.pop())
        self.total = prior_total
        self.committed_next = prior_next
        self.next_sequence = prior_next.copy()
        self.active = prior_active
        raise error
      }
    }
  }
}

///|
pub fn RecordingSession::append(
  self : RecordingSession,
  message : Message,
) -> Unit raise McapError {
  if self.state != WriterOpen {
    raise Invalid("recording session is closed")
  }
  try {
    if !self.template.channels.any(fn(c) { c.id == message.channel_id }) ||
      message.data.length() > self.options.stream.chunk_bytes - 31 {
      raise Invalid("recording message channel or size invalid")
    }
    if self.writer is Some(writer) {
      let p = writer.progress()
      let first = if message.log_time < self.start_time {
        message.log_time
      } else {
        self.start_time
      }
      let last = if message.log_time > self.end_time {
        message.log_time
      } else {
        self.end_time
      }
      if p.messages >= self.options.segment_messages.to_uint64() ||
        (
          self.options.segment_bytes_target != 0UL &&
          p.bytes_written >= self.options.segment_bytes_target
        ) ||
        (
          self.options.segment_span != 0UL &&
          last - first > self.options.segment_span
        ) {
        self.seal()
      }
    }
    if self.writer is None {
      self.start()
      self.start_time = message.log_time
      self.end_time = message.log_time
    }
    let next = self.next_sequence.get(message.channel_id).unwrap_or(0UL)
    let message = if self.options.sequences == ContinueSequence {
      if next > 4294967295UL {
        raise Invalid("recording channel sequence overflow")
      }
      { ..message, sequence: next.to_uint(), }
    } else {
      message
    }
    self.writer.unwrap().append(message)
    let following = message.sequence.to_uint64() + 1UL
    self.next_sequence[message.channel_id] = if following > next {
      following
    } else {
      next
    }
    if message.log_time < self.start_time {
      self.start_time = message.log_time
    }
    if message.log_time > self.end_time {
      self.end_time = message.log_time
    }
  } catch {
    error => {
      self.state = WriterFailed
      if self.writer is Some(w) {
        w.cancel()
      }
      raise error
    }
  }
}

///|
pub fn RecordingSession::finish(
  self : RecordingSession,
) -> Json raise McapError {
  if self.state != WriterOpen {
    raise Invalid("recording session is closed")
  }
  try {
    self.seal()
    self.save(true)
    self.state = WriterFinished
    self.info()
  } catch {
    error => {
      self.state = WriterFailed
      raise error
    }
  }
}

///|
/// Leaves a reserved, possibly incomplete tail for inspection on reopen.
pub fn RecordingSession::cancel(self : RecordingSession) -> Unit {
  if self.state == WriterOpen {
    self.state = WriterCancelled
    if self.writer is Some(w) {
      w.cancel()
    }
  }
}

///|
pub fn RecordingSession::info(self : RecordingSession) -> Json {
  {
    "committed_messages": self.total.to_string(),
    "active_messages": self.writer
    .map(fn(w) { w.progress().messages.to_string() })
    .unwrap_or("0"),
    "completed_segments": self.completed.length(),
    "next_ordinal": self.next_ordinal,
    "active": match self.active {
      None => Json::null()
      Some(n) => Json::number(n.to_double())
    },
    "next_sequence": sequence_json(self.committed_next),
    "adopted_segments": self.adopted,
    "quarantined_segments": self.quarantined,
    "finished": self.state == WriterFinished,
  }
}

///|
pub fn RecordingSession::committed_messages(self : RecordingSession) -> UInt64 {
  self.total
}
