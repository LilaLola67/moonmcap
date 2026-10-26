///|
/// One shared write buffer for all live runs; opening a run flushes it first.
fn buffered_sort_storage(inner : SortStorage, maximum : Int) -> SortStorage {
  let sinks : Map[Int, WriteSink] = Map([])
  let buffer = @buffer.Buffer::Buffer()
  let active : Array[Int?] = [None]
  let flush = fn() -> Unit raise McapError {
    if active[0] is Some(id) {
      if buffer.length() > 0 {
        sinks[id].write(buffer.to_bytes())
      }
      buffer.reset()
      active[0] = None
    }
  }
  sort_storage(
    fn(id) {
      flush()
      let sink = (inner.create)(id)
      sinks[id] = sink
      new_write_sink(fn(bytes) {
        if active[0] != Some(id) {
          flush()
        }
        if bytes.length() >= maximum {
          flush()
          sink.write(bytes)
        } else {
          if bytes.length() > maximum - buffer.length() {
            flush()
          }
          active[0] = Some(id)
          buffer.write_bytes(bytes[:])
        }
      })
    },
    fn(id) {
      flush()
      (inner.open)(id)
    },
    fn(id) {
      if active[0] == Some(id) {
        active[0] = None
        buffer.reset()
      }
      (inner.remove)(id)
      sinks.remove(id)
    },
    finish=fn() {
      flush()
      (inner.finish)()
    },
  )
}

///|
struct DiskIndexEntry {
  entry : SidecarEntry
  ordinal : UInt64
}

///|
fn disk_entry_bytes(value : DiskIndexEntry) -> Bytes raise McapError {
  let out = @buffer.Buffer::Buffer()
  out.write_uint64_le(value.entry.time)
  out.write_uint64_le(value.ordinal)
  out.write_uint_le(value.entry.channel.to_uint())
  out.write_uint64_le(value.entry.location.offset)
  out.write_uint_le(value.entry.location.span.to_uint())
  out.write_uint_le(
    value.entry.location.inner.map(fn(n) { n.to_uint() }).unwrap_or(4294967295U),
  )
  out.to_bytes()
}

///|
fn read_disk_entry(
  bytes : Bytes,
  limits : Limits,
) -> DiskIndexEntry raise McapError {
  let input = cursor(bytes, limits)
  let time = input.u64()
  let ordinal = input.u64()
  let channel = bounded_length(
    input.u32().to_uint64(),
    65535,
    "disk index channel",
  )
  let offset = input.u64()
  let span = bounded_length(
    input.u32().to_uint64(),
    limits.max_record_bytes + 9,
    "disk index span",
  )
  let raw_inner = input.u32()
  let inner = if raw_inner == 4294967295U {
    None
  } else {
    Some(
      bounded_length(
        raw_inner.to_uint64(),
        limits.max_decompressed_bytes,
        "disk index inner",
      ),
    )
  }
  input.finish()
  { entry: { time, channel, location: { offset, span, inner, }, }, ordinal, }
}

///|
fn disk_entry_before(left : DiskIndexEntry, right : DiskIndexEntry) -> Bool {
  left.entry.time < right.entry.time ||
  (left.entry.time == right.entry.time && left.ordinal < right.ordinal)
}

///|
/// Write MIDX2 pages using external sorting. Storage is dedicated to this job.
/// Output is provisional until success; caller owns atomic publication.
pub fn build_disk_sidecar(
  source : ReadSource,
  storage : SortStorage,
  output : WriteSink,
  fingerprint : (ReadSource) -> String raise McapError,
  identity? : String = "",
  page_entries? : Int = 128,
  options? : ExternalOptions = external_options(),
  cancelled? : () -> Bool = fn() { false },
) -> Json raise McapError {
  let sizes : Map[Int, UInt64] = Map([])
  let mut spool = 0UL
  let mut peak_spool = 0UL
  let backing = buffered_sort_storage(storage, 65536)
  let job = sort_storage(
    fn(id) {
      if sizes.contains(id) {
        raise Invalid("disk index temporary ID collision")
      }
      let sink = (backing.create)(id)
      sizes[id] = 0UL
      new_write_sink(fn(bytes) {
        if cancelled() {
          raise Invalid("disk index build cancelled")
        }
        if bytes.length().to_uint64() > options.spool_bytes - spool {
          raise Invalid("disk index total spool budget exceeded")
        }
        sink.write(bytes)
        sizes[id] = sizes[id] + bytes.length().to_uint64()
        spool += bytes.length().to_uint64()
        if spool > peak_spool {
          peak_spool = spool
        }
      })
    },
    fn(id) { (backing.open)(id) },
    fn(id) {
      (backing.remove)(id)
      spool -= sizes[id]
      sizes.remove(id)
    },
  )
  let result = try {
    if identity.length() > 256 ||
      page_entries < 1 ||
      page_entries > 8192 ||
      options.limits.max_record_bytes > 2147483638 {
      raise Invalid("invalid disk index identity, page size or record budget")
    }
    let reader = open_reader(
      source,
      limits=options.limits,
      cache_bytes=0,
      summary_bytes=options.declaration_bytes,
      fallback_bytes=0,
    )
    let declarations = { ..empty_log(), header: reader.declarations.header, }
    let staged = new_stream_writer(
      (job.create)(-1),
      options={
        ..options.output,
        compression: Uncompressed,
        chunk_messages: page_entries,
      },
      limits=options.limits,
    )
    staged.add_channel({
      id: 1,
      schema_id: 0,
      topic: "/index",
      message_encoding: "raw",
      metadata: [],
    })
    let mut count = 0UL
    ignore(
      scan_source(
        source,
        source_visitor(
          schema=fn(value) { add_schema(declarations, value) },
          channel=fn(value) { add_channel(declarations, value) },
          message=fn(message, location) {
            let data = disk_entry_bytes({
              entry: {
                time: message.log_time,
                channel: message.channel_id,
                location,
              },
              ordinal: count,
            })
            staged.append({
              channel_id: 1,
              sequence: 0U,
              log_time: message.log_time,
              publish_time: message.log_time,
              data,
            })
            count += 1UL
          },
        ),
        limits=options.limits,
        declaration_bytes=options.declaration_bytes,
        cancelled~,
      ),
    )
    ignore(staged.finish())
    let sorted = (job.create)(-2)
    let sorted_report = external_merge(
      [(job.open)(-1)],
      job,
      sorted,
      options~,
      cancelled~,
    )
    let header = @buffer.Buffer::Buffer()
    header.write_uint_le(2U)
    header.write_uint64_le(source.size())
    let digest = checked_digest(fingerprint(source))
    put_text(header, digest)
    put_text(header, identity)
    put_blob(header, write_log(declarations, limits=options.limits))
    header.write_uint64_le(count)
    header.write_uint_le(page_entries.to_uint())
    let metadata = header.to_bytes()
    output.write(
      b"MIDX2\r\n\x00" +
      metadata.length().to_uint().to_le_bytes() +
      metadata +
      crc32(metadata).to_le_bytes(),
    )
    let page = @buffer.Buffer::Buffer()
    let mut rows = 0UL
    let flush = fn() -> Unit raise McapError {
      if page.length() > 0 {
        while page.length() < page_entries * 36 {
          page.write_byte(0x00)
        }
        let bytes = page.to_bytes()
        output.write(bytes + crc32(bytes).to_le_bytes())
        page.reset()
      }
    }
    ignore(
      scan_source(
        (job.open)(-2),
        source_visitor(message=fn(message, _) {
          if cancelled() {
            raise Invalid("disk index build cancelled")
          }
          if message.data.length() != 36 {
            raise Invalid("disk index sorted row size mismatch")
          }
          page.write_bytes(message.data[:])
          rows += 1UL
          if page.length() == page_entries * 36 {
            flush()
          }
        }),
        limits=options.limits,
        declaration_bytes=options.declaration_bytes,
        cancelled~,
      ),
    )
    flush()
    if rows != count {
      raise Invalid("disk index sorted row count mismatch")
    }
    let report : Json = {
      "version": 2,
      "entries": count.to_string(),
      "page_entries": page_entries,
      "index_bytes": output.position().to_string(),
      "source_sha256": digest,
      "peak_spool_bytes": peak_spool.to_string(),
      "sort": external_report_json(sorted_report),
    }
    Ok(report)
  } catch {
    error => Err(error)
  }
  let errors = []
  for id in sizes.keys().to_array() {
    (job.remove)(id) catch {
      Invalid(reason) => errors.push(reason)
    }
  }
  (storage.finish)() catch {
    Invalid(reason) => errors.push(reason)
  }
  match result {
    Err(error) => raise error
    Ok(report) => {
      if !errors.is_empty() {
        raise Invalid("disk index cleanup failed: \{errors[0]}")
      }
      report
    }
  }
}

///|
pub struct DiskSidecarReader {
  index : ReadSource
  base : SidecarReader
  count : Int
  width : Int
  start : UInt64
  cache_limit : Int
  pages : Array[(Int, Array[DiskIndexEntry])]
  mut page_reads : Int
  mut cache_hits : Int
  mut peak_pages : Int
}

///|
fn DiskSidecarReader::page(
  self : DiskSidecarReader,
  number : Int,
) -> Array[DiskIndexEntry] raise McapError {
  for i = 0; i < self.pages.length(); i = i + 1 {
    if self.pages[i].0 == number {
      let page = self.pages.remove(i)
      self.pages.push(page)
      self.cache_hits += 1
      return page.1
    }
  }
  let span = self.width * 36 + 4
  let bytes = self.index.read_exact(
    self.start + number.to_uint64() * span.to_uint64(),
    span,
  )
  self.accept_page(number, bytes)
}

///|
fn DiskSidecarReader::accept_page(
  self : DiskSidecarReader,
  number : Int,
  bytes : Bytes,
) -> Array[DiskIndexEntry] raise McapError {
  let span = self.width * 36 + 4
  if number < 0 ||
    number.to_uint64() * self.width.to_uint64() >= self.count.to_uint64() ||
    bytes.length() != span {
    raise Invalid("unexpected disk index page feed")
  }
  let data = bytes[0:span - 4].to_owned()
  let ending = cursor(bytes[span - 4:].to_owned(), self.base.limits)
  if crc32(data) != ending.u32() {
    raise Invalid("disk index page CRC mismatch")
  }
  let rows = (self.count - number * self.width).min(self.width)
  let entries = []
  for i = 0; i < rows; i = i + 1 {
    let value = read_disk_entry(
      data[i * 36:(i + 1) * 36].to_owned(),
      self.base.limits,
    )
    let location = value.entry.location
    if value.ordinal >= self.count.to_uint64() ||
      location.span < 9 ||
      location.offset > self.base.source.size ||
      location.span.to_uint64() > self.base.source.size - location.offset ||
      !self.base.declarations.channels.any(fn(c) { c.id == value.entry.channel }) {
      raise Invalid("disk index row bounds or channel mismatch")
    }
    if !entries.is_empty() &&
      !disk_entry_before(entries[entries.length() - 1], value) {
      raise Invalid("disk index page order mismatch")
    }
    entries.push(value)
  }
  for i = rows * 36; i < data.length(); i = i + 1 {
    if data[i] != 0x00 {
      raise Invalid("disk index padding is not zero")
    }
  }
  self.page_reads += 1
  if self.cache_limit > 0 {
    if self.pages.length() >= self.cache_limit {
      ignore(self.pages.remove(0))
    }
    self.pages.push((number, entries))
    self.peak_pages = self.peak_pages.max(self.pages.length())
  }
  entries
}

///|
fn DiskSidecarReader::entry(
  self : DiskSidecarReader,
  position : Int,
) -> DiskIndexEntry raise McapError {
  if position < 0 || position >= self.count {
    raise Invalid("disk index position outside bounds")
  }
  self.page(position / self.width)[position % self.width]
}

///|
/// Full index ordering/CRC audit uses bounded pages. Source content verification
/// remains explicit and does not replace per-message location checks.
fn open_disk_sidecar_internal(
  source : ReadSource,
  index : ReadSource,
  verification : IndexVerification,
  limits? : Limits = default_limits(),
  declaration_bytes? : Int = 8 * 1024 * 1024,
  cache_pages? : Int = 4,
  chunk_cache_bytes? : Int = 8 * 1024 * 1024,
  index_bytes? : UInt64 = 1099511627776UL,
  audit? : Bool = true,
) -> DiskSidecarReader raise McapError {
  validate_log(empty_log(), limits~)
  if index.size() < 16UL ||
    index.size() > index_bytes ||
    declaration_bytes < 0 ||
    declaration_bytes > 2147482623 ||
    cache_pages < 0 ||
    cache_pages > 64 ||
    chunk_cache_bytes < 0 ||
    limits.max_record_bytes > 2147483638 {
    raise Invalid("invalid disk index budgets or envelope")
  }
  let prefix = index.read_exact(0UL, 12)
  if prefix[0:8].to_owned() != b"MIDX2\r\n\x00" {
    raise Invalid("disk index requires MIDX2")
  }
  let size = cursor(prefix[8:12].to_owned(), limits).u32()
  let length = bounded_length(
    size.to_uint64(),
    declaration_bytes + 1024,
    "disk index metadata",
  )
  let metadata = index.read_exact(12UL, length)
  if crc32(metadata) !=
    cursor(index.read_exact(12UL + length.to_uint64(), 4), limits).u32() {
    raise Invalid("disk index header CRC mismatch")
  }
  let input = cursor(metadata, {
    ..limits,
    max_record_bytes: declaration_bytes + 1024,
  })
  if input.u32() != 2U || input.u64() != source.size() {
    raise Invalid("disk index version or source size changed")
  }
  let digest = checked_digest(input.text())
  let identity = input.text()
  if identity.length() > 256 {
    raise Invalid("disk index identity too large")
  }
  let declarations = read_log(input.blob(), limits~)
  if !declarations.messages.is_empty() ||
    !declarations.attachments.is_empty() ||
    !declarations.metadata.is_empty() {
    raise Invalid("disk index metadata contains nondeclarations")
  }
  let count = bounded_length(
    input.u64(),
    limits.max_records,
    "disk index entries",
  )
  let width = bounded_length(
    input.u32().to_uint64(),
    8192,
    "disk index page entries",
  )
  input.finish()
  if width < 1 ||
    source.size() < 50UL ||
    source.read_exact(0UL, 8) != magic() ||
    source.read_exact(source.size() - 8UL, 8) != magic() {
    raise Invalid("invalid disk index source envelope or page size")
  }
  let start = 16UL + length.to_uint64()
  let page_count = (count.to_uint64() + width.to_uint64() - 1UL) /
    width.to_uint64()
  if start + page_count * (width * 36 + 4).to_uint64() != index.size() {
    raise Invalid("disk index length or page count mismatch")
  }
  let verified = match verification {
    VerifyContent(callback) => {
      if checked_digest(callback(source)) != digest {
        raise Invalid("disk index source fingerprint changed")
      }
      true
    }
    TrustIdentity(token) => {
      if token == "" || token != identity {
        raise Invalid("disk index source identity changed")
      }
      false
    }
  }
  let base : SidecarReader = {
    source,
    declarations,
    entries: [],
    positions: [],
    limits,
    digest,
    content_verified: verified,
    index_bytes: 0,
    cache_budget: chunk_cache_bytes,
    cached_offset: None,
    cached_span: 0,
    cached: None,
    cached_bytes: 0,
  }
  let reader : DiskSidecarReader = {
    index,
    base,
    count,
    width,
    start,
    cache_limit: cache_pages,
    pages: [],
    page_reads: 0,
    cache_hits: 0,
    peak_pages: 0,
  }
  if !audit {
    return reader
  }
  let mut previous : DiskIndexEntry? = None
  for page = 0; page < page_count.to_int(); page = page + 1 {
    let entries = reader.page(page)
    if previous is Some(value) && !disk_entry_before(value, entries[0]) {
      raise Invalid("disk index cross-page order mismatch")
    }
    previous = entries.last()
  }
  reader.pages.clear()
  reader
}

///|
pub fn DiskSidecarReader::info(self : DiskSidecarReader) -> Json {
  {
    "version": 2,
    "entries": self.count,
    "page_entries": self.width,
    "page_reads": self.page_reads,
    "cache_hits": self.cache_hits,
    "cached_pages": self.pages.length(),
    "peak_cached_pages": self.peak_pages,
    "page_cache_budget": self.cache_limit,
    "index_bytes": self.index.size().to_string(),
    "content_verified": self.base.content_verified,
    "source_sha256": self.base.digest,
    "index_audit": "all page CRCs and global order; bounded memory",
  }
}

///|
pub fn DiskSidecarReader::channels(self : DiskSidecarReader) -> Array[Channel] {
  self.base.channels()
}

///|
pub fn DiskSidecarReader::validate_entries(
  self : DiskSidecarReader,
) -> Unit raise McapError {
  for i = 0; i < self.count; i = i + 1 {
    ignore(self.base.load(self.entry(i).entry))
  }
}

///|
pub struct DiskSidecarCursor {
  reader : DiskSidecarReader
  original : Query
  allowed : Map[Int, Bool]
  mut query : Query
  mut position : Int
  mut offset : Int
  mut remaining : Int
  mut state : FileCursorState
}

///|
fn DiskSidecarCursor::reset(
  self : DiskSidecarCursor,
  query : Query,
) -> Unit raise McapError {
  let mut low = 0
  let mut high = self.reader.count
  let time = if query.descending { query.end_time } else { query.start_time }
  while low < high {
    let middle = low + (high - low) / 2
    let value = self.reader.entry(middle).entry.time
    if value < time || (query.descending && value == time) {
      low = middle + 1
    } else {
      high = middle
    }
  }
  self.query = query
  self.position = if query.descending { low - 1 } else { low }
  self.offset = query.offset
  self.remaining = query.limit
  self.state = if query.limit == 0 { FileExhausted } else { FileActive }
}

///|
pub fn DiskSidecarReader::cursor(
  self : DiskSidecarReader,
  query : Query,
) -> DiskSidecarCursor raise McapError {
  if query.start_time > query.end_time || query.offset < 0 || query.limit < 0 {
    raise Invalid("invalid disk index query")
  }
  let query = { ..query, topics: query.topics.copy(), }
  let allowed : Map[Int, Bool] = Map([])
  for channel in self.base.declarations.channels {
    if query.topics.is_empty() || query.topics.contains(channel.topic) {
      allowed[channel.id] = true
    }
  }
  let cursor : DiskSidecarCursor = {
    reader: self,
    original: query,
    allowed,
    query,
    position: 0,
    offset: 0,
    remaining: 0,
    state: FileActive,
  }
  if query.limit != 0 {
    cursor.reset(query)
  } else {
    cursor.state = FileExhausted
  }
  cursor
}

///|
pub fn DiskSidecarCursor::next(
  self : DiskSidecarCursor,
) -> Message? raise McapError {
  if self.state == FileFailed {
    raise Invalid("disk index cursor failed")
  }
  if self.state != FileActive {
    return None
  }
  try {
    while self.remaining > 0 &&
          self.position >= 0 &&
          self.position < self.reader.count {
      let entry = self.reader.entry(self.position).entry
      self.position += if self.query.descending { -1 } else { 1 }
      if entry.time < self.query.start_time || entry.time > self.query.end_time {
        break
      }
      if !self.allowed.contains(entry.channel) {
        continue
      }
      if self.offset > 0 {
        self.offset -= 1
        continue
      }
      let message = self.reader.base.load(entry)
      self.remaining -= 1
      return Some(message)
    }
    self.state = FileExhausted
    None
  } catch {
    error => {
      self.state = FileFailed
      raise error
    }
  }
}

///|
pub fn DiskSidecarCursor::next_page(
  self : DiskSidecarCursor,
  size : Int,
) -> Array[Message] raise McapError {
  if size <= 0 || size > self.reader.base.limits.max_records {
    raise Invalid("invalid disk index page request")
  }
  let messages = []
  while messages.length() < size {
    match self.next() {
      Some(message) => messages.push(message)
      None => break
    }
  }
  messages
}

///|
pub fn DiskSidecarCursor::seek(
  self : DiskSidecarCursor,
  time : UInt64,
) -> Unit raise McapError {
  if self.state == FileCancelled ||
    self.state == FileFailed ||
    time < self.original.start_time ||
    time > self.original.end_time {
    raise Invalid("invalid disk index seek or state")
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
pub fn DiskSidecarCursor::cancel(self : DiskSidecarCursor) -> Unit {
  self.state = FileCancelled
}

///|
pub fn DiskSidecarReader::query_source(
  self : DiskSidecarReader,
) -> QuerySource raise McapError {
  query_source(self.base.declarations, fn(query, _) {
    let cursor = self.cursor(query)
    message_stream(fn() { cursor.next() }, fn() { cursor.cancel() })
  })
}
