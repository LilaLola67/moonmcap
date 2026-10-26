///|
pub fn open_disk_sidecar(
  source : ReadSource,
  index : ReadSource,
  verification : IndexVerification,
  limits? : Limits = default_limits(),
  declaration_bytes? : Int = 8 * 1024 * 1024,
  cache_pages? : Int = 4,
  chunk_cache_bytes? : Int = 8 * 1024 * 1024,
  index_bytes? : UInt64 = 1099511627776UL,
) -> DiskSidecarReader raise McapError {
  open_disk_sidecar_internal(
    source,
    index,
    verification,
    limits~,
    declaration_bytes~,
    cache_pages~,
    chunk_cache_bytes~,
    index_bytes~,
  )
}

///|
pub struct DiskSidecarAudit {
  reader : DiskSidecarReader
  mut page : Int
  mut previous : DiskIndexEntry?
  mut failed : Bool
}

///|
/// Headers/source envelopes are read synchronously here; pages are fed by poll.
pub fn start_disk_sidecar_audit(
  source : ReadSource,
  index : ReadSource,
  verification : IndexVerification,
  limits? : Limits = default_limits(),
  declaration_bytes? : Int = 8 * 1024 * 1024,
  cache_pages? : Int = 4,
  chunk_cache_bytes? : Int = 8 * 1024 * 1024,
  index_bytes? : UInt64 = 1099511627776UL,
) -> DiskSidecarAudit raise McapError {
  {
    reader: open_disk_sidecar_internal(
      source,
      index,
      verification,
      limits~,
      declaration_bytes~,
      cache_pages~,
      chunk_cache_bytes~,
      index_bytes~,
      audit=false,
    ),
    page: 0,
    previous: None,
    failed: false,
  }
}

///|
pub fn DiskSidecarAudit::poll(
  self : DiskSidecarAudit,
) -> ReadSpan? raise McapError {
  if self.failed {
    raise Invalid("disk index audit failed")
  }
  if self.page.to_uint64() * self.reader.width.to_uint64() >=
    self.reader.count.to_uint64() {
    return None
  }
  let length = self.reader.width * 36 + 4
  Some({
    offset: self.reader.start + self.page.to_uint64() * length.to_uint64(),
    length,
  })
}

///|
pub fn DiskSidecarAudit::feed(
  self : DiskSidecarAudit,
  bytes : Bytes,
) -> Unit raise McapError {
  try {
    guard self.poll() is Some(_) else {
      raise Invalid("disk index audit already complete")
    }
    let entries = self.reader.accept_page(self.page, bytes)
    if self.previous is Some(previous) &&
      !disk_entry_before(previous, entries[0]) {
      raise Invalid("disk index cross-page order mismatch")
    }
    self.previous = entries.last()
    self.page += 1
  } catch {
    error => {
      self.failed = true
      raise error
    }
  }
}

///|
pub fn DiskSidecarAudit::finish(
  self : DiskSidecarAudit,
) -> DiskSidecarReader raise McapError {
  if self.poll() is Some(_) {
    raise Invalid("disk index audit is incomplete")
  }
  self.reader.pages.clear()
  self.reader
}

///|
pub fn DiskSidecarReader::declaration_log(self : DiskSidecarReader) -> Log {
  copy_log(self.base.declarations)
}

///|
pub(all) enum DiskSidecarPoll {
  DiskDone
  DiskMessage(Message)
  DiskIndexRead(ReadSpan)
  DiskSourceRead(ReadSpan)
}

///|
pub struct DiskSidecarPollCursor {
  reader : DiskSidecarReader
  original : Query
  allowed : Map[Int, Bool]
  mut query : Query
  mut low : Int
  mut high : Int
  mut searching : Bool
  mut position : Int
  mut offset : Int
  mut remaining : Int
  mut state : FileCursorState
  mut current : (Int, Array[DiskIndexEntry])?
  mut waiting_page : Int?
  mut waiting_entry : SidecarEntry?
  mut ready : Message?
}

///|
pub fn DiskSidecarReader::poll_cursor(
  self : DiskSidecarReader,
  query : Query,
) -> DiskSidecarPollCursor raise McapError {
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
  {
    reader: self,
    original: query,
    allowed,
    query,
    low: 0,
    high: self.count,
    searching: true,
    position: 0,
    offset: query.offset,
    remaining: query.limit,
    state: if query.limit == 0 {
      FileExhausted
    } else {
      FileActive
    },
    current: None,
    waiting_page: None,
    waiting_entry: None,
    ready: None,
  }
}

///|
fn DiskSidecarPollCursor::entry(
  self : DiskSidecarPollCursor,
  position : Int,
) -> SidecarEntry? {
  let page = position / self.reader.width
  if self.current is Some((number, entries)) && number == page {
    return Some(entries[position % self.reader.width].entry)
  }
  for i = 0; i < self.reader.pages.length(); i = i + 1 {
    if self.reader.pages[i].0 == page {
      let found = self.reader.pages.remove(i)
      self.reader.pages.push(found)
      self.reader.cache_hits += 1
      self.current = Some(found)
      return Some(found.1[position % self.reader.width].entry)
    }
  }
  self.current = None
  self.waiting_page = Some(page)
  None
}

///|
pub fn DiskSidecarPollCursor::poll(
  self : DiskSidecarPollCursor,
) -> DiskSidecarPoll raise McapError {
  if self.state == FileFailed {
    raise Invalid("disk index cursor failed")
  }
  if self.state != FileActive {
    return DiskDone
  }
  try {
    while self.searching {
      if self.low == self.high {
        self.position = if self.query.descending {
          self.low - 1
        } else {
          self.low
        }
        self.searching = false
        break
      }
      let middle = self.low + (self.high - self.low) / 2
      guard self.entry(middle) is Some(entry) else {
        return self.index_request()
      }
      let time = if self.query.descending {
        self.query.end_time
      } else {
        self.query.start_time
      }
      if entry.time < time || (self.query.descending && entry.time == time) {
        self.low = middle + 1
      } else {
        self.high = middle
      }
    }
    while self.remaining > 0 &&
          self.position >= 0 &&
          self.position < self.reader.count {
      guard self.entry(self.position) is Some(entry) else {
        return self.index_request()
      }
      if entry.time < self.query.start_time || entry.time > self.query.end_time {
        break
      }
      if !self.allowed.contains(entry.channel) || self.offset > 0 {
        if self.allowed.contains(entry.channel) {
          self.offset -= 1
        }
        self.position += if self.query.descending { -1 } else { 1 }
        continue
      }
      let message = match self.ready {
        Some(message) => message
        None =>
          if entry.location.inner is Some(_) &&
            self.reader.base.cached_offset == Some(entry.location.offset) &&
            self.reader.base.cached_span == entry.location.span {
            self.reader.base.load(entry)
          } else {
            self.waiting_entry = Some(entry)
            return DiskSourceRead({
              offset: entry.location.offset,
              length: entry.location.span,
            })
          }
      }
      self.ready = None
      self.waiting_entry = None
      self.position += if self.query.descending { -1 } else { 1 }
      self.remaining -= 1
      if self.remaining == 0 {
        self.state = FileExhausted
      }
      return DiskMessage(message)
    }
    self.state = FileExhausted
    DiskDone
  } catch {
    error => {
      self.state = FileFailed
      raise error
    }
  }
}

///|
fn DiskSidecarPollCursor::index_request(
  self : DiskSidecarPollCursor,
) -> DiskSidecarPoll raise McapError {
  guard self.waiting_page is Some(page) else {
    raise Invalid("missing index page request")
  }
  let length = self.reader.width * 36 + 4
  DiskIndexRead({
    offset: self.reader.start + page.to_uint64() * length.to_uint64(),
    length,
  })
}

///|
pub fn DiskSidecarPollCursor::feed_index(
  self : DiskSidecarPollCursor,
  bytes : Bytes,
) -> Unit raise McapError {
  try {
    guard self.state == FileActive &&
      self.waiting_page is Some(page) &&
      self.waiting_entry is None else {
      raise Invalid("unexpected index page feed")
    }
    self.current = Some((page, self.reader.accept_page(page, bytes)))
    self.waiting_page = None
  } catch {
    error => {
      self.state = FileFailed
      raise error
    }
  }
}

///|
pub fn DiskSidecarPollCursor::feed_source(
  self : DiskSidecarPollCursor,
  bytes : Bytes,
) -> Unit raise McapError {
  try {
    guard self.state == FileActive &&
      self.waiting_entry is Some(entry) &&
      self.waiting_page is None &&
      self.ready is None &&
      bytes.length() == entry.location.span else {
      raise Invalid("unexpected indexed source feed")
    }
    let span = entry.location
    let mirror = new_read_source(self.reader.base.source.size, fn(
      offset,
      length,
    ) {
      if offset < span.offset ||
        offset - span.offset > bytes.length().to_uint64() {
        raise Invalid("disk index feed offset mismatch")
      }
      let start = (offset - span.offset).to_int()
      if length > bytes.length() - start {
        raise Invalid("disk index feed length mismatch")
      }
      bytes[start:start + length].to_owned()
    })
    let staged = { ..self.reader.base, source: mirror, }
    self.ready = Some(staged.load(entry))
    self.reader.base.cached = staged.cached
    self.reader.base.cached_offset = staged.cached_offset
    self.reader.base.cached_span = staged.cached_span
    self.reader.base.cached_bytes = staged.cached_bytes
    self.waiting_entry = None
  } catch {
    error => {
      self.state = FileFailed
      self.ready = None
      raise error
    }
  }
}

///|
pub fn DiskSidecarPollCursor::pending_bytes(
  self : DiskSidecarPollCursor,
) -> Int {
  self.ready.map(fn(m) { m.data.length() + 31 }).unwrap_or(0)
}

///|
pub fn DiskSidecarPollCursor::cancel(self : DiskSidecarPollCursor) -> Unit {
  self.state = FileCancelled
  self.ready = None
  self.waiting_entry = None
  self.waiting_page = None
  self.current = None
}

///|
pub fn DiskSidecarPollCursor::seek(
  self : DiskSidecarPollCursor,
  time : UInt64,
) -> Unit raise McapError {
  if self.state == FileFailed ||
    self.state == FileCancelled ||
    time < self.original.start_time ||
    time > self.original.end_time {
    raise Invalid("invalid disk index seek or state")
  }
  self.query = {
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
  }
  self.low = 0
  self.high = self.reader.count
  self.searching = true
  self.position = 0
  self.offset = 0
  self.remaining = self.query.limit
  self.ready = None
  self.waiting_entry = None
  self.waiting_page = None
  self.current = None
  self.state = if self.remaining == 0 { FileExhausted } else { FileActive }
}
