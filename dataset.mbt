///|
pub struct MessageStream {
  next_message : () -> Message? raise McapError
  stop : () -> Unit
  pending : () -> Int
}

///|
pub fn message_stream(
  next : () -> Message? raise McapError,
  cancel : () -> Unit,
  pending_bytes? : () -> Int = fn() { 0 },
) -> MessageStream {
  { next_message: next, stop: cancel, pending: pending_bytes, }
}

///|
pub struct QuerySource {
  declarations : Log
  open : (Query, Int) -> MessageStream raise McapError
  materialized : Bool
}

///|
/// Provider contract: stable snapshot, ordered messages, exact declarations,
/// and truthful nonnegative pending payload accounting.
pub fn query_source(
  declarations : Log,
  open : (Query, Int) -> MessageStream raise McapError,
  materialized? : Bool = false,
) -> QuerySource raise McapError {
  if !declarations.messages.is_empty() ||
    !declarations.attachments.is_empty() ||
    !declarations.metadata.is_empty() {
    raise Invalid("query source requires declarations only")
  }
  validate_log(declarations)
  { declarations: copy_log(declarations), open, materialized, }
}

///|
pub fn QuerySource::declaration_log(self : QuerySource) -> Log {
  copy_log(self.declarations)
}

///|
pub fn RangeReader::query_source(
  self : RangeReader,
) -> QuerySource raise McapError {
  if self.indexes.is_empty() {
    let size = bounded_length(
      self.source.size,
      self.fallback_budget,
      "dataset unindexed fallback",
    )
    let log = read_log(self.source.read_exact(0UL, size), limits=self.limits)
    self.full_validation = true
    let index = build_query_index(log)
    return query_source(
      { ..log, messages: [], attachments: [], metadata: [], },
      fn(query, _) {
        let cursor = index.cursor(query)
        message_stream(fn() { cursor.next() }, fn() { cursor.cancel() })
      },
      materialized=true,
    )
  }
  query_source(self.declarations, fn(query, budget) {
    let cursor = self.cursor(query, pending_bytes=budget)
    message_stream(fn() { cursor.next() }, fn() { cursor.cancel() }, pending_bytes=fn() {
      cursor.progress().pending_bytes
    })
  })
}

///|
pub struct Dataset {
  sources : Array[QuerySource]
  declarations : Log
  remaps : Array[Map[Int, Int]]
  limits : Limits
}

///|
pub fn open_dataset(
  sources : Array[QuerySource],
  limits? : Limits = default_limits(),
  declaration_bytes? : Int = 8 * 1024 * 1024,
  max_sources? : Int = 64,
) -> Dataset raise McapError {
  validate_log(empty_log(), limits~)
  if sources.is_empty() ||
    sources.length() > max_sources ||
    declaration_bytes < 0 {
    raise Invalid("invalid dataset source count or budget")
  }
  let mut retained = 0
  for source in sources {
    for schema in source.declarations.schemas {
      let size = encode_schema(schema).length()
      if size > declaration_bytes - retained {
        raise Invalid("dataset declarations exceed budget")
      }
      retained += size
    }
    for channel in source.declarations.channels {
      let size = encode_channel(channel).length()
      if size > declaration_bytes - retained {
        raise Invalid("dataset declarations exceed budget")
      }
      retained += size
    }
  }
  let declarations = merge_logs(sources.map(fn(source) { source.declarations }))
  validate_log(declarations, limits~)
  let remaps = sources.map(fn(source) {
    let schema_ids : Map[Int, Int] = Map([(0, 0)])
    for schema in source.declarations.schemas {
      let mapped = declarations.schemas
        .iter()
        .find_first(fn(s) {
          s.name == schema.name &&
          s.encoding == schema.encoding &&
          s.data == schema.data
        })
        .unwrap()
      schema_ids[schema.id] = mapped.id
    }
    let ids : Map[Int, Int] = Map([])
    for channel in source.declarations.channels {
      let mapped = declarations.channels
        .iter()
        .find_first(fn(c) {
          c.topic == channel.topic &&
          c.schema_id == schema_ids[channel.schema_id] &&
          c.message_encoding == channel.message_encoding &&
          c.metadata == channel.metadata
        })
        .unwrap()
      ids[channel.id] = mapped.id
    }
    ids
  })
  { sources: sources.copy(), declarations, remaps, limits, }
}

///|
pub fn Dataset::channels(self : Dataset) -> Array[Channel] {
  self.declarations.channels.map(fn(c) { { ..c, metadata: c.metadata.copy(), } })
}

///|
pub fn Dataset::schemas(self : Dataset) -> Array[Schema] {
  self.declarations.schemas.copy()
}

///|
pub(all) struct DatasetMessage {
  source_index : Int
  message : Message
} derive(Eq, @debug.Debug)

///|
pub(all) struct DatasetProgress {
  state : FileCursorState
  sources : Int
  materialized_sources : Int
  loaded_sources : Int
  emitted_messages : Int
  skipped_messages : Int
  pending_bytes : Int
  peak_pending_bytes : Int
} derive(Eq, @debug.Debug)

///|
pub struct DatasetCursor {
  dataset : Dataset
  original : Query
  budget : Int
  streams : Array[MessageStream]
  heap : Array[DatasetMessage]
  mut query : Query
  mut state : FileCursorState
  mut initialized : Bool
  mut loaded : Int
  mut advance : Int?
  mut remaining : Int
  mut offset_left : Int
  mut emitted : Int
  mut skipped : Int
  mut peak : Int
}

///|
fn DatasetCursor::close(self : DatasetCursor, state : FileCursorState) -> Unit {
  for stream in self.streams {
    (stream.stop)()
  }
  self.heap.clear()
  self.advance = None
  self.state = state
}

///|
fn DatasetCursor::pending_bytes(self : DatasetCursor) -> Int raise McapError {
  let mut sum = 0
  for head in self.heap {
    let bytes = head.message.data.length() + 31
    if bytes > self.budget - sum {
      raise Invalid("dataset pending byte budget exceeded")
    }
    sum += bytes
  }
  for stream in self.streams {
    let bytes = (stream.pending)()
    if bytes < 0 || bytes > self.budget - sum {
      raise Invalid("dataset pending byte budget exceeded")
    }
    sum += bytes
  }
  sum
}

///|
fn DatasetCursor::before(
  self : DatasetCursor,
  a : DatasetMessage,
  b : DatasetMessage,
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
fn DatasetCursor::push(self : DatasetCursor, head : DatasetMessage) -> Unit {
  self.heap.push(head)
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
fn DatasetCursor::pop(self : DatasetCursor) -> DatasetMessage {
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
fn DatasetCursor::reset(
  self : DatasetCursor,
  query : Query,
) -> Unit raise McapError {
  self.close(FileActive)
  self.streams.clear()
  self.query = query
  self.initialized = false
  self.loaded = 0
  self.remaining = query.limit
  self.offset_left = query.offset
  self.emitted = 0
  self.skipped = 0
  self.peak = 0
  if query.limit == 0 {
    self.state = FileExhausted
    return
  }
  try {
    let child_query = {
      ..query,
      offset: 0,
      limit: self.dataset.limits.max_records,
    }
    let share = self.budget / self.dataset.sources.length() / 2
    for source in self.dataset.sources {
      self.streams.push((source.open)(child_query, share))
    }
  } catch {
    Invalid(reason) => {
      self.close(FileFailed)
      raise Invalid(reason)
    }
  }
}

///|
pub fn Dataset::cursor(
  self : Dataset,
  query : Query,
  pending_bytes? : Int = 64 * 1024 * 1024,
) -> DatasetCursor raise McapError {
  if query.start_time > query.end_time ||
    query.offset < 0 ||
    query.limit < 0 ||
    pending_bytes < 0 {
    raise Invalid("invalid dataset cursor query or budget")
  }
  let copied = { ..query, topics: query.topics.copy(), }
  let cursor : DatasetCursor = {
    dataset: self,
    original: copied,
    query: copied,
    budget: pending_bytes,
    streams: [],
    heap: [],
    state: FileActive,
    initialized: false,
    loaded: 0,
    advance: None,
    remaining: 0,
    offset_left: 0,
    emitted: 0,
    skipped: 0,
    peak: 0,
  }
  cursor.reset(copied)
  cursor
}

///|
fn DatasetCursor::read_head(
  self : DatasetCursor,
  source : Int,
) -> Unit raise McapError {
  if (self.streams[source].next_message)() is Some(message) {
    if message.log_time < self.query.start_time ||
      message.log_time > self.query.end_time ||
      !self.dataset.remaps[source].contains(message.channel_id) {
      raise Invalid("dataset provider returned invalid message")
    }
    self.push({
      source_index: source,
      message: {
        ..message,
        channel_id: self.dataset.remaps[source][message.channel_id],
      },
    })
  }
  self.peak = self.peak.max(self.pending_bytes())
}

///|
pub fn DatasetCursor::next_record(
  self : DatasetCursor,
) -> DatasetMessage? raise McapError {
  if self.state == FileFailed {
    raise Invalid("dataset cursor failed")
  }
  if self.state != FileActive {
    return None
  }
  try {
    if !self.initialized {
      for i = 0; i < self.streams.length(); i = i + 1 {
        self.read_head(i)
        self.loaded += 1
      }
      self.initialized = true
    }
    while self.remaining > 0 {
      if self.advance is Some(source) {
        self.advance = None
        self.read_head(source)
      }
      if self.heap.is_empty() {
        self.close(FileExhausted)
        return None
      }
      let head = self.pop()
      self.advance = Some(head.source_index)
      if self.offset_left > 0 {
        self.offset_left -= 1
        self.skipped += 1
        continue
      }
      if self.emitted >= self.dataset.limits.max_records {
        raise Invalid("dataset output record budget exceeded")
      }
      self.remaining -= 1
      self.emitted += 1
      if self.remaining == 0 {
        self.close(FileExhausted)
      }
      return Some(head)
    }
    None
  } catch {
    Invalid(reason) => {
      self.close(FileFailed)
      raise Invalid(reason)
    }
  }
}

///|
pub fn DatasetCursor::next(self : DatasetCursor) -> Message? raise McapError {
  self.next_record().map(fn(record) { record.message })
}

///|
pub fn DatasetCursor::next_page(
  self : DatasetCursor,
  size : Int,
) -> Array[DatasetMessage] raise McapError {
  if size <= 0 {
    raise Invalid("dataset page size must be positive")
  }
  let page = []
  for _i = 0; _i < size; _i = _i + 1 {
    match self.next_record() {
      Some(record) => page.push(record)
      None => break
    }
  }
  page
}

///|
pub fn DatasetCursor::seek(
  self : DatasetCursor,
  time : UInt64,
) -> Unit raise McapError {
  if self.state == FileCancelled || self.state == FileFailed {
    raise Invalid("dataset cursor is closed")
  }
  if time < self.original.start_time || time > self.original.end_time {
    raise Invalid("dataset seek outside original interval")
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
pub fn DatasetCursor::cancel(self : DatasetCursor) -> Unit {
  self.close(if self.state == FileFailed { FileFailed } else { FileCancelled })
}

///|
pub fn DatasetCursor::progress(self : DatasetCursor) -> DatasetProgress {
  {
    state: self.state,
    sources: self.dataset.sources.length(),
    materialized_sources: self.dataset.sources
    .filter(fn(s) { s.materialized })
    .length(),
    loaded_sources: self.loaded,
    emitted_messages: self.emitted,
    skipped_messages: self.skipped,
    pending_bytes: self.pending_bytes() catch {
      _ => self.budget
    },
    peak_pending_bytes: self.peak,
  }
}

///|
pub fn dataset_message_json(
  record : DatasetMessage,
  channels : Array[Channel],
) -> Json {
  let m = record.message
  {
    "source_index": record.source_index,
    "channel_id": m.channel_id,
    "topic": channels
    .iter()
    .find_first(fn(c) { c.id == m.channel_id })
    .map(fn(c) { c.topic })
    .unwrap_or(""),
    "sequence": m.sequence,
    "log_time": m.log_time.to_string(),
    "publish_time": m.publish_time.to_string(),
    "data_hex": encode_hex(m.data),
  }
}

///|
pub fn DatasetCursor::write_ndjson(
  self : DatasetCursor,
  sink : WriteSink,
) -> Int raise McapError {
  let mut count = 0
  while self.next_record() is Some(record) {
    sink.write(
      @utf8.encode(
        (dataset_message_json(record, self.dataset.declarations.channels).stringify() +
        "\n")[:],
      ),
    )
    count += 1
  }
  count
}
