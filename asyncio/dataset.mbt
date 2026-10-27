///|
pub struct AsyncMessageStream {
  next : async () -> @mcap.Message?
  stop : () -> Unit
  pending : () -> Int
}

///|
pub fn async_message_stream(
  next : async () -> @mcap.Message?,
  cancel : () -> Unit,
  pending_bytes? : () -> Int = fn() { 0 },
) -> AsyncMessageStream {
  { next, stop: cancel, pending: pending_bytes, }
}

///|
pub struct AsyncQuerySource {
  declarations : @mcap.Log
  open : (@mcap.Query, Int) -> AsyncMessageStream raise @mcap.McapError
  materialized : Bool
}

///|
pub fn async_query_source(
  declarations : @mcap.Log,
  open : (@mcap.Query, Int) -> AsyncMessageStream raise @mcap.McapError,
  materialized? : Bool = false,
) -> AsyncQuerySource raise @mcap.McapError {
  let checked = @mcap.query_source(declarations, fn(_, _) {
    @mcap.message_stream(fn() { None }, fn() { () })
  })
  { declarations: checked.declaration_log(), open, materialized, }
}

///|
pub fn AsyncReader::query_source(
  self : AsyncReader,
  abort_source_on_cancel? : Bool = false,
  select_region? : async (UInt64, Int) -> Bool = async fn(_, _) { true },
) -> AsyncQuerySource raise @mcap.McapError {
  let declarations = self.reader.query_source().declaration_log()
  async_query_source(
    declarations,
    fn(query, budget) {
      let cursor = self.cursor(
        query,
        pending_bytes=budget,
        span_bytes=budget,
        select_region~,
      )
      async_message_stream(
        async fn() { cursor.next() },
        fn() {
          cursor.cancel()
          if abort_source_on_cancel {
            self.source.cancel()
          }
        },
        pending_bytes=fn() { cursor.progress().pending_bytes },
      )
    },
    materialized=!self.reader.diagnostics().indexed,
  )
}

///|
pub fn AsyncSidecarReader::query_source(
  self : AsyncSidecarReader,
  abort_source_on_cancel? : Bool = false,
) -> AsyncQuerySource raise @mcap.McapError {
  async_query_source(self.reader.declaration_log(), fn(query, budget) {
    let cursor = self.cursor(query, pending_bytes=budget)
    async_message_stream(
      async fn() { cursor.next() },
      fn() {
        cursor.cancel()
        if abort_source_on_cancel {
          self.source.cancel()
        }
      },
      pending_bytes=fn() { cursor.cursor.pending_bytes() },
    )
  })
}

///|
pub fn AsyncQuerySource::filter_json(
  self : AsyncQuerySource,
  filter : @mcap.JsonFilter,
) -> AsyncQuerySource raise @mcap.McapError {
  async_query_source(
    self.declarations,
    fn(query, budget) {
      let inner = (self.open)(
        { ..query, offset: 0, limit: 2147483647, },
        budget,
      )
      let remaining = [query.limit]
      let offset = [query.offset]
      let closed = [false]
      async_message_stream(
        async fn() {
          if closed[0] || remaining[0] == 0 {
            (inner.stop)()
            closed[0] = true
            return None
          }
          try {
            while (inner.next)() is Some(message) {
              if closed[0] {
                return None
              }
              if !self.declarations.channels.any(fn(c) {
                  c.id == message.channel_id && c.message_encoding == "json"
                }) ||
                !filter.matches(message.data) {
                continue
              }
              if offset[0] > 0 {
                offset[0] -= 1
                continue
              }
              remaining[0] -= 1
              if remaining[0] == 0 {
                closed[0] = true
                (inner.stop)()
              }
              return Some(message)
            }
            closed[0] = true
            (inner.stop)()
            None
          } catch {
            error => {
              closed[0] = true
              (inner.stop)()
              raise error
            }
          }
        },
        fn() {
          closed[0] = true
          (inner.stop)()
        },
        pending_bytes=inner.pending,
      )
    },
    materialized=self.materialized,
  )
}

///|
pub struct AsyncDataset {
  providers : Array[AsyncQuerySource]
  metadata : @mcap.Dataset
  limits : @mcap.Limits
}

///|
pub fn open_dataset(
  providers : Array[AsyncQuerySource],
  limits? : @mcap.Limits = @mcap.default_limits(),
  declaration_bytes? : Int = 8 * 1024 * 1024,
  max_sources? : Int = 64,
) -> AsyncDataset raise @mcap.McapError {
  let sources = providers.map(fn(provider) {
    @mcap.query_source(
      provider.declarations,
      fn(_, _) { @mcap.message_stream(fn() { None }, fn() { () }) },
      materialized=provider.materialized,
    )
  })
  {
    providers: providers.copy(),
    metadata: @mcap.open_dataset(
      sources,
      limits~,
      declaration_bytes~,
      max_sources~,
    ),
    limits,
  }
}

///|
pub fn AsyncDataset::channels(self : AsyncDataset) -> Array[@mcap.Channel] {
  self.metadata.channels()
}

///|
pub struct AsyncDatasetCursor {
  dataset : AsyncDataset
  original : @mcap.Query
  budget : Int
  streams : Array[AsyncMessageStream]
  heap : Array[@mcap.DatasetMessage]
  previous : Array[UInt64?]
  mut query : @mcap.Query
  mut state : @mcap.FileCursorState
  mut initialized : Bool
  mut advance : Int?
  mut remaining : Int
  mut offset : Int
  mut emitted : Int
  mut skipped : Int
  mut peak : Int
  mut busy : Bool
}

///|
fn AsyncDatasetCursor::close(
  self : AsyncDatasetCursor,
  state : @mcap.FileCursorState,
) -> Unit {
  self.state = state
  for stream in self.streams {
    (stream.stop)()
  }
  self.heap.clear()
  self.advance = None
}

///|
fn AsyncDatasetCursor::reset(
  self : AsyncDatasetCursor,
  query : @mcap.Query,
) -> Unit raise @mcap.McapError {
  self.close(@mcap.FileActive)
  self.streams.clear()
  self.query = query
  self.initialized = false
  self.remaining = query.limit
  self.offset = query.offset
  self.previous.clear()
  self.emitted = 0
  self.skipped = 0
  self.peak = 0
  if query.limit == 0 {
    self.state = @mcap.FileExhausted
    return
  }
  let share = self.budget / self.dataset.providers.length() / 2
  if share < 1 {
    self.state = @mcap.FileFailed
    raise @mcap.io_error("async dataset budget too small")
  }
  try {
    for source in self.dataset.providers {
      self.streams.push(
        (source.open)(
          { ..query, offset: 0, limit: self.dataset.limits.max_records, },
          share,
        ),
      )
      self.previous.push(None)
    }
  } catch {
    error => {
      self.close(@mcap.FileFailed)
      raise error
    }
  }
}

///|
pub fn AsyncDataset::cursor(
  self : AsyncDataset,
  query : @mcap.Query,
  pending_bytes? : Int = 64 * 1024 * 1024,
) -> AsyncDatasetCursor raise @mcap.McapError {
  if query.start_time > query.end_time ||
    query.offset < 0 ||
    query.limit < 0 ||
    pending_bytes < 1 {
    raise @mcap.io_error("invalid async dataset query or budget")
  }
  let query = { ..query, topics: query.topics.copy(), }
  let cursor : AsyncDatasetCursor = {
    dataset: self,
    original: query,
    budget: pending_bytes,
    streams: [],
    heap: [],
    previous: [],
    query,
    state: @mcap.FileActive,
    initialized: false,
    advance: None,
    remaining: 0,
    offset: 0,
    emitted: 0,
    skipped: 0,
    peak: 0,
    busy: false,
  }
  cursor.reset(query)
  cursor
}

///|
fn AsyncDatasetCursor::before(
  self : AsyncDatasetCursor,
  a : @mcap.DatasetMessage,
  b : @mcap.DatasetMessage,
) -> Bool {
  let order = if a.message.log_time != b.message.log_time {
    a.message.log_time.compare(b.message.log_time)
  } else {
    a.source_index.compare(b.source_index)
  }
  if self.query.descending {
    order > 0
  } else {
    order < 0
  }
}

///|
fn AsyncDatasetCursor::push(
  self : AsyncDatasetCursor,
  value : @mcap.DatasetMessage,
) -> Unit {
  self.heap.push(value)
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
fn AsyncDatasetCursor::pop(self : AsyncDatasetCursor) -> @mcap.DatasetMessage {
  let head = self.heap[0]
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
  head
}

///|
fn AsyncDatasetCursor::account(
  self : AsyncDatasetCursor,
) -> Int raise @mcap.McapError {
  let mut sum = 0
  for head in self.heap {
    let bytes = head.message.data.length() + 31
    if bytes > self.budget - sum {
      raise @mcap.io_error("async dataset heads exceed budget")
    }
    sum += bytes
  }
  for stream in self.streams {
    let bytes = (stream.pending)()
    if bytes < 0 || bytes > self.budget - sum {
      raise @mcap.io_error("async dataset providers exceed budget")
    }
    sum += bytes
  }
  self.peak = self.peak.max(sum)
  sum
}

///|
async fn AsyncDatasetCursor::load(
  self : AsyncDatasetCursor,
  index : Int,
) -> Unit {
  let value = (self.streams[index].next)()
  if self.state != @mcap.FileActive {
    return
  }
  if value is Some(message) {
    if message.log_time < self.query.start_time ||
      message.log_time > self.query.end_time {
      raise @mcap.io_error("async provider time outside query")
    }
    if self.previous[index] is Some(time) &&
      (if self.query.descending {
        message.log_time > time
      } else {
        message.log_time < time
      }) {
      raise @mcap.io_error("unordered async provider")
    }
    self.previous[index] = Some(message.log_time)
    let mapped = self.dataset.metadata.remap_message(index, message)
    if !self.query.topics.is_empty() &&
      !self.dataset.metadata
      .channels()
      .any(fn(c) {
        c.id == mapped.channel_id && self.query.topics.contains(c.topic)
      }) {
      raise @mcap.io_error("async provider topic outside query")
    }
    self.push({ source_index: index, message: mapped, })
  }
  ignore(self.account())
}

///|
async fn AsyncDatasetCursor::take(
  self : AsyncDatasetCursor,
) -> @mcap.DatasetMessage? {
  if self.state == @mcap.FileFailed {
    raise @mcap.io_error("async dataset cursor failed")
  }
  if self.state != @mcap.FileActive {
    return None
  }
  try {
    if !self.initialized {
      for i = 0; i < self.streams.length(); i = i + 1 {
        self.load(i)
        if self.state != @mcap.FileActive {
          return None
        }
      }
      self.initialized = true
    }
    while true {
      if self.advance is Some(index) {
        self.advance = None
        self.load(index)
      }
      if self.state != @mcap.FileActive {
        return None
      }
      if self.heap.is_empty() {
        self.close(@mcap.FileExhausted)
        return None
      }
      if self.emitted + self.skipped >= self.dataset.limits.max_records {
        raise @mcap.io_error("async dataset record limit exceeded")
      }
      let head = self.pop()
      self.advance = Some(head.source_index)
      if self.offset > 0 {
        self.offset -= 1
        self.skipped += 1
        continue
      }
      self.emitted += 1
      self.remaining -= 1
      if self.remaining == 0 {
        self.close(@mcap.FileExhausted)
      }
      return Some(head)
    }
    None
  } catch {
    error => {
      if self.state == @mcap.FileCancelled {
        return None
      }
      self.close(@mcap.FileFailed)
      raise error
    }
  }
}

///|
pub async fn AsyncDatasetCursor::next(
  self : AsyncDatasetCursor,
) -> @mcap.DatasetMessage? {
  if self.busy {
    raise @mcap.io_error("concurrent async dataset operation")
  }
  self.busy = true
  defer {
    self.busy = false
  }
  self.take()
}

///|
pub async fn AsyncDatasetCursor::next_page(
  self : AsyncDatasetCursor,
  size : Int,
) -> Array[@mcap.DatasetMessage] {
  if size < 1 || self.busy {
    raise @mcap.io_error("invalid page size or concurrent dataset operation")
  }
  self.busy = true
  defer {
    self.busy = false
  }
  let page = []
  for i = 0; i < size; i = i + 1 {
    match self.take() {
      None => break
      Some(message) => page.push(message)
    }
  }
  page
}

///|
pub fn AsyncDatasetCursor::seek(
  self : AsyncDatasetCursor,
  time : UInt64,
) -> Unit raise @mcap.McapError {
  if self.busy ||
    [@mcap.FileCancelled, @mcap.FileFailed].contains(self.state) ||
    time < self.original.start_time ||
    time > self.original.end_time {
    raise @mcap.io_error("invalid async dataset seek")
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
pub fn AsyncDatasetCursor::cancel(self : AsyncDatasetCursor) -> Unit {
  if self.state != @mcap.FileFailed {
    self.close(@mcap.FileCancelled)
  }
}

///|
pub fn AsyncDatasetCursor::progress(
  self : AsyncDatasetCursor,
) -> @mcap.DatasetProgress {
  {
    state: self.state,
    sources: self.dataset.providers.length(),
    materialized_sources: self.dataset.providers
    .filter(fn(p) { p.materialized })
    .length(),
    loaded_sources: if self.initialized {
      self.streams.length()
    } else {
      self.heap.length()
    },
    emitted_messages: self.emitted,
    skipped_messages: self.skipped,
    pending_bytes: self.account() catch {
      _ => self.budget
    },
    peak_pending_bytes: self.peak,
  }
}

///|
pub async fn AsyncDatasetCursor::write_ndjson(
  self : AsyncDatasetCursor,
  sink : AsyncSink,
  content? : @mcap.JsonFilter,
) -> Int {
  if self.busy {
    raise @mcap.io_error("concurrent async dataset export")
  }
  self.busy = true
  defer {
    self.busy = false
  }
  let mut count = 0
  try {
    while self.take() is Some(record) {
      let value = match content {
        None =>
          @mcap.dataset_message_json(record, self.dataset.metadata.channels())
        Some(filter) =>
          @mcap.json_query_record(
            record,
            self.dataset.metadata.channels(),
            filter,
          )
      }
      sink.write(@utf8.encode((value.stringify() + "\n")[:]))
      count += 1
    }
    count
  } catch {
    error => {
      self.cancel()
      raise error
    }
  }
}
