///|
pub(all) struct ReadSpan {
  offset : UInt64
  length : Int
} derive(Eq, @debug.Debug)

///|
pub(all) enum FilePoll {
  FileMessage(Message)
  FileRead(ReadSpan)
  FileDone
}

///|
pub(all) enum FileCursorState {
  FileActive
  FileExhausted
  FileCancelled
  FileFailed
} derive(Eq, @debug.Debug)

///|
pub(all) struct FileProgress {
  state : FileCursorState
  candidate_chunks : Int
  loaded_chunks : Int
  emitted_messages : Int
  skipped_messages : Int
  pending_messages : Int
  pending_bytes : Int
  peak_pending_bytes : Int
  used_fallback : Bool
} derive(Eq, @debug.Debug)

///|
struct FileItem {
  message : Message
  chunk_position : UInt64
  record_position : Int
}

///|
pub struct FileCursor {
  reader : RangeReader
  original : Query
  candidates : Array[ChunkIndex]
  heap : Array[FileItem]
  allowed : Map[Int, Bool]
  pending_budget : Int
  span_budget : Int
  mut active : Query
  mut fallback : QueryCursor?
  mut state : FileCursorState
  mut loaded : Int
  mut visited : Int
  mut emitted : Int
  mut skipped : Int
  mut offset_left : Int
  mut remaining : Int
  mut pending_bytes : Int
  mut peak_pending : Int
}

///|
fn FileCursor::before(self : FileCursor, a : FileItem, b : FileItem) -> Bool {
  let order = if a.message.log_time != b.message.log_time {
    a.message.log_time.compare(b.message.log_time)
  } else if a.chunk_position != b.chunk_position {
    a.chunk_position.compare(b.chunk_position)
  } else {
    a.record_position.compare(b.record_position)
  }
  if self.active.descending {
    order > 0
  } else {
    order < 0
  }
}

///|
fn FileCursor::push(self : FileCursor, item : FileItem) -> Unit {
  self.heap.push(item)
  let mut child = self.heap.length() - 1
  while child > 0 {
    let parent = (child - 1) / 2
    if !self.before(self.heap[child], self.heap[parent]) {
      break
    }
    let old = self.heap[parent]
    self.heap[parent] = self.heap[child]
    self.heap[child] = old
    child = parent
  }
}

///|
fn FileCursor::pop(self : FileCursor) -> Message {
  let item = self.heap[0]
  let last = self.heap.pop().unwrap()
  if !self.heap.is_empty() {
    self.heap[0] = last
  }
  let mut parent = 0
  while parent < self.heap.length() / 2 {
    let left = parent * 2 + 1
    let right = left + 1
    let child = if right < self.heap.length() &&
      self.before(self.heap[right], self.heap[left]) {
      right
    } else {
      left
    }
    if !self.before(self.heap[child], self.heap[parent]) {
      break
    }
    let old = self.heap[parent]
    self.heap[parent] = self.heap[child]
    self.heap[child] = old
    parent = child
  }
  self.pending_bytes -= item.message.data.length() + 31
  item.message
}

///|
fn FileCursor::reset(self : FileCursor, query : Query) -> Unit raise McapError {
  self.active = query
  self.heap.clear()
  self.candidates.clear()
  self.allowed.clear()
  self.loaded = 0
  self.visited = 0
  self.emitted = 0
  self.skipped = 0
  self.offset_left = query.offset
  self.remaining = query.limit
  self.pending_bytes = 0
  self.peak_pending = 0
  self.state = if query.limit == 0 { FileExhausted } else { FileActive }
  self.fallback = None
  if query.limit == 0 {
    return
  }
  if self.reader.indexes.is_empty() {
    let size = bounded_length(
      self.reader.source.size,
      self.reader.fallback_budget,
      "cursor fallback",
    )
    let log = read_log(
      self.reader.source.read_exact(0UL, size),
      limits=self.reader.limits,
    )
    self.reader.full_validation = true
    self.fallback = Some(build_query_index(log).cursor(query))
    return
  }
  self.fallback = None
  for channel in self.reader.declarations.channels {
    if query.topics.is_empty() || query.topics.contains(channel.topic) {
      self.allowed[channel.id] = true
    }
  }
  if self.allowed.is_empty() {
    self.state = FileExhausted
    return
  }
  for index in self.reader.indexes {
    if index.end_time < query.start_time || index.start_time > query.end_time {
      continue
    }
    if !index.message_index_offsets.is_empty() &&
      !index.message_index_offsets.any(fn(p) { self.allowed.contains(p.0) }) {
      continue
    }
    self.candidates.push(index)
  }
  self.candidates.sort_by(fn(a, b) {
    let order = if query.descending {
      b.end_time.compare(a.end_time)
    } else {
      a.start_time.compare(b.start_time)
    }
    if order != 0 {
      order
    } else if query.descending {
      b.chunk_start.compare(a.chunk_start)
    } else {
      a.chunk_start.compare(b.chunk_start)
    }
  })
}

///|
pub fn RangeReader::cursor(
  self : RangeReader,
  query : Query,
  pending_bytes? : Int = 64 * 1024 * 1024,
  span_bytes? : Int = 32 * 1024 * 1024,
) -> FileCursor raise McapError {
  if query.start_time > query.end_time ||
    query.offset < 0 ||
    query.limit < 0 ||
    pending_bytes < 0 ||
    span_bytes < 9 {
    raise Invalid("invalid file cursor query or budgets")
  }
  let copied = { ..query, topics: query.topics.copy(), }
  let cursor : FileCursor = {
    reader: self,
    original: copied,
    active: copied,
    candidates: [],
    heap: [],
    allowed: Map([]),
    pending_budget: pending_bytes,
    span_budget: span_bytes,
    fallback: None,
    state: FileActive,
    loaded: 0,
    visited: 0,
    emitted: 0,
    skipped: 0,
    offset_left: 0,
    remaining: 0,
    pending_bytes: 0,
    peak_pending: 0,
  }
  cursor.reset(copied)
  cursor
}

///|
fn FileCursor::needed(self : FileCursor) -> Bool {
  if self.loaded >= self.candidates.length() {
    return false
  }
  if self.heap.is_empty() {
    return true
  }
  let index = self.candidates[self.loaded]
  let time = self.heap[0].message.log_time
  // Equal bounds must load too, preserving physical ties across overlapping chunks.
  if self.active.descending {
    index.end_time >= time
  } else {
    index.start_time <= time
  }
}

///|
fn FileCursor::accept(self : FileCursor, chunk : Chunk) -> Unit raise McapError {
  let index = self.candidates[self.loaded]
  let items : Array[FileItem] = []
  let mut size = 0
  if chunk.records.length() > self.reader.limits.max_records - self.visited {
    raise Invalid("cursor visited record budget exceeded")
  }
  for record in chunk.records {
    if record.opcode != 0x05 {
      continue
    }
    let message = decode_message(record.body, limits=self.reader.limits)
    if self.allowed.contains(message.channel_id) &&
      message.log_time >= self.active.start_time &&
      message.log_time <= self.active.end_time {
      let bytes = message.data.length() + 31
      if bytes > self.pending_budget - self.pending_bytes - size {
        raise Invalid(
          "overlapping cursor candidates exceed pending byte budget",
        )
      }
      size += bytes
      items.push({
        message,
        chunk_position: index.chunk_start,
        record_position: record.offset,
      })
    }
  }
  for item in items {
    self.push(item)
  }
  self.pending_bytes += size
  self.peak_pending = self.peak_pending.max(self.pending_bytes)
  self.visited += chunk.records.length()
  self.loaded += 1
}

///|
fn FileCursor::fail(self : FileCursor) -> Unit {
  self.heap.clear()
  self.pending_bytes = 0
  self.state = FileFailed
}

///|
/// Nonblocking protocol: perform FileRead externally, then feed exactly its bytes.
pub fn FileCursor::poll(self : FileCursor) -> FilePoll raise McapError {
  if self.state == FileFailed {
    raise Invalid("file cursor is failed")
  }
  if self.state != FileActive {
    return FileDone
  }
  if self.fallback is Some(cursor) {
    let message = cursor.next()
    self.skipped = cursor.progress().skipped_messages
    return match message {
      Some(message) => {
        self.emitted += 1
        if cursor.progress().state == Exhausted {
          self.state = FileExhausted
        }
        FileMessage(message)
      }
      None => {
        self.state = FileExhausted
        FileDone
      }
    }
  }
  try {
    while self.remaining > 0 {
      if self.needed() {
        let index = self.candidates[self.loaded]
        if self.reader.cache.any(fn(c) { c.offset == index.chunk_start }) {
          self.accept(self.reader.load_chunk(index))
          continue
        }
        let total = index.chunk_length + index.message_index_length
        let length = bounded_length(total, self.span_budget, "cursor read span")
        return FileRead({ offset: index.chunk_start, length, })
      }
      if self.heap.is_empty() {
        break
      }
      let message = self.pop()
      if self.offset_left > 0 {
        self.offset_left -= 1
        self.skipped += 1
        continue
      }
      self.remaining -= 1
      self.emitted += 1
      if self.remaining == 0 {
        self.heap.clear()
        self.pending_bytes = 0
        self.state = FileExhausted
      }
      return FileMessage(message)
    }
    self.state = FileExhausted
    FileDone
  } catch {
    Invalid(reason) => {
      self.fail()
      raise Invalid(reason)
    }
  }
}

///|
pub fn FileCursor::feed(
  self : FileCursor,
  bytes : Bytes,
) -> Unit raise McapError {
  if self.state != FileActive || !self.needed() {
    raise Invalid("file cursor has no pending read")
  }
  try {
    let index = self.candidates[self.loaded]
    let length = bounded_length(
      index.chunk_length + index.message_index_length,
      self.span_budget,
      "cursor read span",
    )
    if bytes.length() != length {
      raise Invalid("cursor supplied span length mismatch")
    }
    self.accept(self.reader.load_chunk(index, provided=Some(bytes)))
  } catch {
    Invalid(reason) => {
      self.fail()
      raise Invalid(reason)
    }
  }
}

///|
pub fn FileCursor::next(self : FileCursor) -> Message? raise McapError {
  while true {
    match self.poll() {
      FileDone => return None
      FileMessage(message) => return Some(message)
      FileRead(span) => {
        let bytes = self.reader.source.read_exact(span.offset, span.length) catch {
          Invalid(reason) => {
            self.fail()
            raise Invalid(reason)
          }
        }
        self.feed(bytes)
      }
    }
  }
  None
}

///|
pub fn FileCursor::next_page(
  self : FileCursor,
  size : Int,
) -> Array[Message] raise McapError {
  if size <= 0 {
    raise Invalid("page size must be positive")
  }
  let messages = []
  for _i = 0; _i < size; _i = _i + 1 {
    match self.next() {
      Some(message) => messages.push(message)
      None => break
    }
  }
  messages
}

///|
pub fn FileCursor::seek(
  self : FileCursor,
  time : UInt64,
) -> Unit raise McapError {
  if self.state == FileCancelled || self.state == FileFailed {
    raise Invalid("file cursor is closed")
  }
  if time < self.original.start_time || time > self.original.end_time {
    raise Invalid("seek outside original interval")
  }
  let query = {
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
  self.reset(query) catch {
    Invalid(reason) => {
      self.fail()
      raise Invalid(reason)
    }
  }
}

///|
pub fn FileCursor::cancel(self : FileCursor) -> Unit {
  self.heap.clear()
  self.pending_bytes = 0
  if self.fallback is Some(cursor) {
    cursor.cancel()
  }
  if self.state != FileFailed {
    self.state = FileCancelled
  }
}

///|
pub fn FileCursor::progress(self : FileCursor) -> FileProgress {
  {
    state: self.state,
    candidate_chunks: self.candidates.length(),
    loaded_chunks: self.loaded,
    emitted_messages: self.emitted,
    skipped_messages: self.skipped,
    pending_messages: self.heap.length(),
    pending_bytes: self.pending_bytes,
    peak_pending_bytes: self.peak_pending,
    used_fallback: self.fallback is Some(_),
  }
}
