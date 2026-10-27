///|
pub(all) enum IndexVerification {
  VerifyContent((ReadSource) -> String raise McapError)
  TrustIdentity(String)
}

///|
struct SidecarEntry {
  time : UInt64
  channel : Int
  location : MessageLocation
}

///|
pub struct SidecarReader {
  source : ReadSource
  declarations : Log
  entries : Array[SidecarEntry]
  positions : Array[Int]
  limits : Limits
  digest : String
  content_verified : Bool
  index_bytes : Int
  cache_budget : Int
  mut cached_offset : UInt64?
  mut cached_span : Int
  mut cached : Chunk?
  mut cached_bytes : Int
}

///|
fn checked_digest(text : String) -> String raise McapError {
  if text.length() != 64 {
    raise Invalid("index requires a 64-digit SHA-256 fingerprint")
  }
  encode_hex(decode_hex(text, max_bytes=32))
}

///|
pub fn build_sidecar(
  source : ReadSource,
  fingerprint : (ReadSource) -> String raise McapError,
  identity? : String = "",
  limits? : Limits = default_limits(),
  index_bytes? : Int = 32 * 1024 * 1024,
  declaration_bytes? : Int = 8 * 1024 * 1024,
  cancelled? : () -> Bool = fn() { false },
) -> Bytes raise McapError {
  if index_bytes < 128 || identity.length() > 256 {
    raise Invalid("invalid sidecar budget or identity token")
  }
  let reader = open_reader(
    source,
    limits~,
    cache_bytes=0,
    summary_bytes=declaration_bytes,
    fallback_bytes=0,
  )
  let declarations = { ..empty_log(), header: reader.declarations.header, }
  let entries : Array[SidecarEntry] = []
  let retained = [
    128 + identity.length() + encode_header(declarations.header).length(),
  ]
  let reserve = fn(size : Int) -> Unit raise McapError {
    if size > index_bytes - retained[0] {
      raise Invalid("sidecar index byte budget exceeded")
    }
    retained[0] += size
  }
  ignore(
    scan_source(
      source,
      source_visitor(
        schema=fn(value) {
          reserve(encode_schema(value).length() + 9)
          add_schema(declarations, value)
        },
        channel=fn(value) {
          reserve(encode_channel(value).length() + 9)
          add_channel(declarations, value)
        },
        message=fn(message, location) {
          reserve(26)
          entries.push({
            time: message.log_time,
            channel: message.channel_id,
            location,
          })
        },
      ),
      limits~,
      declaration_bytes~,
      cancelled~,
    ),
  )
  if cancelled() {
    raise Invalid("sidecar build cancelled")
  }
  let digest = checked_digest(fingerprint(source))
  let body = @buffer.Buffer::Buffer()
  body.write_uint_le(1U)
  body.write_uint64_le(source.size)
  put_text(body, digest)
  put_text(body, identity)
  put_blob(body, write_log(declarations, limits~))
  body.write_uint_le(entries.length().to_uint())
  for entry in entries {
    body.write_uint64_le(entry.time)
    put_u16(body, entry.channel)
    body.write_uint64_le(entry.location.offset)
    body.write_uint_le(entry.location.span.to_uint())
    body.write_uint_le(
      entry.location.inner.map(fn(n) { n.to_uint() }).unwrap_or(4294967295U),
    )
  }
  let framed = b"MIDX1\r\n\x00" + body.to_bytes()
  if framed.length() > index_bytes - 4 {
    raise Invalid("sidecar encoded byte budget exceeded")
  }
  framed + crc32(framed).mcap_le_bytes()
}

///|
pub fn open_sidecar(
  source : ReadSource,
  bytes : Bytes,
  verification : IndexVerification,
  limits? : Limits = default_limits(),
  index_bytes? : Int = 32 * 1024 * 1024,
  cache_bytes? : Int = 8 * 1024 * 1024,
) -> SidecarReader raise McapError {
  validate_log(empty_log(), limits~)
  if source.size < 50UL ||
    bytes.length() < 12 ||
    bytes.length() > index_bytes ||
    cache_bytes < 0 ||
    bytes[0:8].to_owned() != b"MIDX1\r\n\x00" {
    raise Invalid("invalid sidecar envelope or budget")
  }
  if source.read_exact(0UL, 8) != magic() ||
    source.read_exact(source.size - 8UL, 8) != magic() {
    raise Invalid("sidecar source is not an MCAP envelope")
  }
  let ending = cursor(bytes[bytes.length() - 4:].to_owned(), limits)
  if crc32(bytes[0:bytes.length() - 4].to_owned()) != ending.u32() {
    raise Invalid("sidecar CRC mismatch")
  }
  let input = cursor(bytes[8:bytes.length() - 4].to_owned(), {
    ..limits,
    max_record_bytes: index_bytes,
  })
  if input.u32() != 1U {
    raise Invalid("unsupported sidecar version")
  }
  if input.u64() != source.size {
    raise Invalid("sidecar source size changed")
  }
  let digest = checked_digest(input.text())
  let identity = input.text()
  if identity.length() > 256 {
    raise Invalid("invalid sidecar identity token")
  }
  let declarations = read_log(input.blob(), limits~)
  if !declarations.messages.is_empty() ||
    !declarations.attachments.is_empty() ||
    !declarations.metadata.is_empty() {
    raise Invalid("sidecar has non-declaration content")
  }
  let count = bounded_length(
    input.u32().to_uint64(),
    limits.max_records,
    "sidecar entries",
  )
  if count > (bytes.length() - input.pos) / 26 {
    raise Invalid("truncated sidecar entries")
  }
  let entries : Array[SidecarEntry] = []
  let channels : Map[Int, Bool] = Map([])
  for channel in declarations.channels {
    channels[channel.id] = true
  }
  for _i = 0; _i < count; _i = _i + 1 {
    let time = input.u64()
    let channel = input.u16()
    let offset = input.u64()
    let span = bounded_length(
      input.u32().to_uint64(),
      limits.max_record_bytes + 9,
      "sidecar record span",
    )
    let raw_inner = input.u32()
    let inner = if raw_inner == 4294967295U {
      None
    } else {
      Some(
        bounded_length(
          raw_inner.to_uint64(),
          limits.max_decompressed_bytes,
          "sidecar inner offset",
        ),
      )
    }
    if !channels.contains(channel) ||
      offset < 8UL ||
      offset > source.size ||
      span < 9 ||
      span.to_uint64() > source.size - offset {
      raise Invalid("sidecar entry outside source or missing declaration")
    }
    if !entries.is_empty() {
      let previous = entries[entries.length() - 1].location
      if offset == previous.offset {
        match (previous.inner, inner) {
          (Some(a), Some(b)) =>
            if b <= a || span != previous.span {
              raise Invalid("sidecar duplicate or reversed inner position")
            }
          _ => raise Invalid("sidecar duplicate physical position")
        }
      } else if offset < previous.offset + previous.span.to_uint64() {
        raise Invalid("sidecar overlapping or reversed physical position")
      }
    }
    entries.push({ time, channel, location: { offset, span, inner, }, })
  }
  input.finish()
  let content_verified = match verification {
    VerifyContent(fingerprint) => {
      if checked_digest(fingerprint(source)) != digest {
        raise Invalid("sidecar source content fingerprint changed")
      }
      true
    }
    TrustIdentity(token) => {
      if token == "" || identity == "" || token != identity {
        raise Invalid("sidecar source identity changed")
      }
      false
    }
  }
  let positions = Array::makei(entries.length(), fn(i) { i })
  positions.sort_by(fn(a, b) {
    if entries[a].time == entries[b].time {
      a.compare(b)
    } else {
      entries[a].time.compare(entries[b].time)
    }
  })
  {
    source,
    declarations,
    entries,
    positions,
    limits,
    digest,
    content_verified,
    index_bytes: bytes.length(),
    cache_budget: cache_bytes,
    cached_offset: None,
    cached_span: 0,
    cached: None,
    cached_bytes: 0,
  }
}

///|
fn SidecarReader::load(
  self : SidecarReader,
  entry : SidecarEntry,
  expanded? : Bytes,
) -> Message raise McapError {
  let location = entry.location
  let message = match location.inner {
    None => {
      if expanded is Some(_) {
        raise Invalid("expanded direct Message is invalid")
      }
      let record = self.source.record(
        location.offset,
        self.source.size,
        self.limits,
      )
      if record.opcode != 0x05 || record.body.length() + 9 != location.span {
        raise Invalid("sidecar direct location mismatch")
      }
      decode_message(record.body, limits=self.limits)
    }
    Some(inner) => {
      let chunk = if self.cached_offset == Some(location.offset) &&
        self.cached_span == location.span {
        self.cached.unwrap()
      } else {
        let record = self.source.record(
          location.offset,
          self.source.size,
          self.limits,
        )
        if record.opcode != 0x06 || record.body.length() + 9 != location.span {
          raise Invalid("sidecar chunk location mismatch")
        }
        let chunk = match expanded {
          None => decode_chunk(record.body, limits=self.limits)
          Some(bytes) => decode_prepared_chunk(record.body, bytes, self.limits)
        }
        let size = chunk.records.fold(init=0, fn(sum, record) {
          sum + record.body.length() + 9
        })
        self.cached = None
        self.cached_offset = None
        self.cached_bytes = 0
        if size <= self.cache_budget {
          self.cached = Some(chunk)
          self.cached_offset = Some(location.offset)
          self.cached_span = location.span
          self.cached_bytes = size
        }
        chunk
      }
      let mut low = 0
      let mut high = chunk.records.length()
      while low < high {
        let middle = low + (high - low) / 2
        if chunk.records[middle].offset < inner {
          low = middle + 1
        } else {
          high = middle
        }
      }
      if low >= chunk.records.length() ||
        chunk.records[low].offset != inner ||
        chunk.records[low].opcode != 0x05 {
        raise Invalid("sidecar inner location is not a Message")
      }
      let nested = chunk.records[low]
      decode_message(nested.body, limits=self.limits)
    }
  }
  if message.channel_id != entry.channel || message.log_time != entry.time {
    raise Invalid("sidecar message identity mismatch")
  }
  message
}

///|
pub fn SidecarReader::validate_entries(
  self : SidecarReader,
) -> Unit raise McapError {
  for entry in self.entries {
    ignore(self.load(entry))
  }
}

///|
pub fn SidecarReader::clear_cache(self : SidecarReader) -> Unit {
  self.cached = None
  self.cached_offset = None
  self.cached_bytes = 0
  self.cached_span = 0
}

///|
pub fn SidecarReader::channels(self : SidecarReader) -> Array[Channel] {
  self.declarations.channels.map(fn(c) { { ..c, metadata: c.metadata.copy(), } })
}

///|
pub fn SidecarReader::info(self : SidecarReader) -> Json {
  {
    "version": 1,
    "source_bytes": self.source.size.to_string(),
    "entries": self.entries.length(),
    "index_bytes": self.index_bytes,
    "source_sha256": self.digest,
    "content_verified": self.content_verified,
    "cached_bytes": self.cached_bytes,
    "source_bytes_read": self.source.bytes_read.to_string(),
    "source_read_calls": self.source.calls,
  }
}

///|
pub struct SidecarCursor {
  reader : SidecarReader
  original : Query
  allowed : Map[Int, Bool]
  mut query : Query
  mut state : FileCursorState
  mut position : Int
  mut remaining : Int
  mut offset_left : Int
  mut emitted : Int
  mut skipped : Int
  mut waiting : ReadSpan?
  mut ready : Message?
}

///|
fn SidecarCursor::reset(self : SidecarCursor, query : Query) -> Unit {
  self.waiting = None
  self.ready = None
  self.query = query
  self.remaining = query.limit
  self.offset_left = query.offset
  self.emitted = 0
  self.skipped = 0
  self.state = if query.limit == 0 { FileExhausted } else { FileActive }
  let mut low = 0
  let mut high = self.reader.positions.length()
  let time = if query.descending { query.end_time } else { query.start_time }
  while low < high {
    let middle = low + (high - low) / 2
    let value = self.reader.entries[self.reader.positions[middle]].time
    if value < time || (query.descending && value == time) {
      low = middle + 1
    } else {
      high = middle
    }
  }
  self.position = if query.descending { low - 1 } else { low }
}

///|
pub fn SidecarReader::cursor(
  self : SidecarReader,
  query : Query,
) -> SidecarCursor raise McapError {
  if query.start_time > query.end_time || query.offset < 0 || query.limit < 0 {
    raise Invalid("invalid sidecar query")
  }
  let query = { ..query, topics: query.topics.copy(), }
  let allowed : Map[Int, Bool] = Map([])
  for channel in self.declarations.channels {
    if query.topics.is_empty() || query.topics.contains(channel.topic) {
      allowed[channel.id] = true
    }
  }
  let cursor : SidecarCursor = {
    reader: self,
    original: query,
    query,
    allowed,
    state: FileActive,
    position: 0,
    remaining: 0,
    offset_left: 0,
    emitted: 0,
    skipped: 0,
    waiting: None,
    ready: None,
  }
  cursor.reset(query)
  cursor
}

///|
pub fn SidecarCursor::poll(self : SidecarCursor) -> FilePoll raise McapError {
  if self.state == FileFailed {
    raise Invalid("sidecar cursor failed")
  }
  if self.state != FileActive {
    return FileDone
  }
  try {
    while self.position >= 0 && self.position < self.reader.positions.length() {
      let entry = self.reader.entries[self.reader.positions[self.position]]
      if entry.time < self.query.start_time || entry.time > self.query.end_time {
        break
      }
      if !self.allowed.contains(entry.channel) {
        self.position += if self.query.descending { -1 } else { 1 }
        continue
      }
      if self.offset_left > 0 {
        self.offset_left -= 1
        self.skipped += 1
        self.position += if self.query.descending { -1 } else { 1 }
        continue
      }
      let message = match self.ready {
        Some(message) => message
        None =>
          if entry.location.inner is Some(_) &&
            self.reader.cached_offset == Some(entry.location.offset) &&
            self.reader.cached_span == entry.location.span {
            self.reader.load(entry)
          } else {
            let span : ReadSpan = {
              offset: entry.location.offset,
              length: entry.location.span,
            }
            self.waiting = Some(span)
            return FileRead(span)
          }
      }
      self.ready = None
      self.waiting = None
      self.position += if self.query.descending { -1 } else { 1 }
      self.emitted += 1
      self.remaining -= 1
      if self.remaining == 0 {
        self.state = FileExhausted
      }
      return FileMessage(message)
    }
    self.state = FileExhausted
    FileDone
  } catch {
    Invalid(reason) => {
      self.state = FileFailed
      raise Invalid(reason)
    }
  }
}

///|
pub fn SidecarCursor::feed(
  self : SidecarCursor,
  bytes : Bytes,
) -> Unit raise McapError {
  try {
    guard self.state == FileActive &&
      self.waiting is Some(span) &&
      bytes.length() == span.length else {
      raise Invalid("unexpected sidecar feed")
    }
    let entry = self.reader.entries[self.reader.positions[self.position]]
    let mirror = new_read_source(self.reader.source.size, fn(offset, length) {
      if offset < span.offset ||
        offset - span.offset > bytes.length().to_uint64() {
        raise Invalid("sidecar feed range mismatch")
      }
      let start = (offset - span.offset).to_int()
      if length > bytes.length() - start {
        raise Invalid("sidecar feed length mismatch")
      }
      bytes[start:start + length].to_owned()
    })
    let staged = { ..self.reader, source: mirror, }
    self.ready = Some(staged.load(entry))
    self.reader.cached = staged.cached
    self.reader.cached_offset = staged.cached_offset
    self.reader.cached_span = staged.cached_span
    self.reader.cached_bytes = staged.cached_bytes
    self.waiting = None
  } catch {
    error => {
      self.state = FileFailed
      self.waiting = None
      self.ready = None
      raise error
    }
  }
}

///|
pub fn SidecarCursor::next(self : SidecarCursor) -> Message? raise McapError {
  try {
    while true {
      match self.poll() {
        FileDone => return None
        FileMessage(message) => return Some(message)
        FileRead(span) =>
          self.feed(self.reader.source.read_exact(span.offset, span.length))
      }
    }
    None
  } catch {
    error => {
      self.state = FileFailed
      self.waiting = None
      self.ready = None
      raise error
    }
  }
}

///|
pub fn SidecarCursor::pending_bytes(self : SidecarCursor) -> Int {
  self.ready.map(fn(m) { m.data.length() + 31 }).unwrap_or(0)
}

///|
pub fn SidecarReader::declaration_log(self : SidecarReader) -> Log {
  copy_log(self.declarations)
}

///|
pub fn SidecarCursor::next_page(
  self : SidecarCursor,
  size : Int,
) -> Array[Message] raise McapError {
  if size <= 0 {
    raise Invalid("sidecar page size must be positive")
  }
  let page = []
  for _i = 0; _i < size; _i = _i + 1 {
    match self.next() {
      Some(message) => page.push(message)
      None => break
    }
  }
  page
}

///|
pub fn SidecarCursor::seek(
  self : SidecarCursor,
  time : UInt64,
) -> Unit raise McapError {
  if self.state == FileCancelled || self.state == FileFailed {
    raise Invalid("sidecar cursor is closed")
  }
  if time < self.original.start_time || time > self.original.end_time {
    raise Invalid("sidecar seek outside original interval")
  }
  self.reset({
    ..self.original,
    offset: 0,
    start_time: if self.original.descending {
      self.original.start_time
    } else {
      time
    },
    end_time: if self.original.descending {
      time
    } else {
      self.original.end_time
    },
  })
}

///|
pub fn SidecarCursor::cancel(self : SidecarCursor) -> Unit {
  self.ready = None
  self.waiting = None
  if self.state != FileFailed {
    self.state = FileCancelled
  }
}

///|
pub fn SidecarCursor::progress(self : SidecarCursor) -> Json {
  {
    "state": match self.state {
      FileActive => "active"
      FileExhausted => "exhausted"
      FileFailed => "failed"
      FileCancelled => "cancelled"
    },
    "indexed_messages": self.reader.entries.length(),
    "emitted_messages": self.emitted,
    "skipped_messages": self.skipped,
  }
}

///|
pub fn SidecarReader::query_source(
  self : SidecarReader,
) -> QuerySource raise McapError {
  query_source(self.declarations, fn(query, _) {
    let cursor = self.cursor(query)
    message_stream(fn() { cursor.next() }, fn() { cursor.cancel() })
  })
}

///|
pub fn SidecarCursor::write_ndjson(
  self : SidecarCursor,
  sink : WriteSink,
) -> Int raise McapError {
  let mut count = 0
  while self.next() is Some(message) {
    let text = messages_json([message], self.reader.declarations.channels).stringify()
    sink.write(@utf8.encode((text[1:text.length() - 1].to_string() + "\n")[:]))
    count += 1
  }
  count
}
