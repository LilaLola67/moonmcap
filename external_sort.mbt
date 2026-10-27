///|
pub struct SortStorage {
  create : (Int) -> WriteSink raise McapError
  open : (Int) -> ReadSource raise McapError
  remove : (Int) -> Unit raise McapError
  finish : () -> Unit raise McapError
}

///|
pub fn sort_storage(
  create : (Int) -> WriteSink raise McapError,
  open : (Int) -> ReadSource raise McapError,
  remove : (Int) -> Unit raise McapError,
  finish? : () -> Unit raise McapError = fn() { () },
) -> SortStorage {
  { create, open, remove, finish, }
}

///|
pub(all) struct ExternalOptions {
  run_bytes : Int
  merge_bytes : Int
  fan_in : Int
  spool_bytes : UInt64
  run_limit : Int
  declaration_bytes : Int
  limits : Limits
  output : StreamOptions
}

///|
pub fn external_options() -> ExternalOptions {
  {
    run_bytes: 2 * 1024 * 1024,
    merge_bytes: 16 * 1024 * 1024,
    fan_in: 8,
    spool_bytes: 512UL * 1024UL * 1024UL,
    run_limit: 32768,
    declaration_bytes: 8 * 1024 * 1024,
    limits: default_limits(),
    output: stream_options(),
  }
}

///|
pub(all) struct ExternalReport {
  input_files : Int
  messages : UInt64
  runs_created : Int
  merge_passes : Int
  peak_run_bytes : Int
  peak_merge_bytes : Int
  peak_spool_bytes : UInt64
  output_bytes : UInt64
}

///|
struct SortRow {
  message : Message
  ordinal : UInt64
}

///|
fn compare_rows(a : SortRow, b : SortRow) -> Int {
  if a.message.log_time != b.message.log_time {
    a.message.log_time.compare(b.message.log_time)
  } else {
    a.ordinal.compare(b.ordinal)
  }
}

///|
struct SortSession {
  observer : SourceVisitor?
  trace : ((SourceScanEvent) -> Unit)?
  preserve_ids : Bool
  storage : SortStorage
  options : ExternalOptions
  live : Map[Int, UInt64]
  cancelled : () -> Bool
  buffer : Array[SortRow]
  mut buffered : Int
  mut ordinal : UInt64
  mut next_id : Int
  mut spool : UInt64
  mut peak_spool : UInt64
  mut peak_run : Int
  mut peak_merge : Int
  mut passes : Int
  mut declared : Int
}

///|
fn SortSession::trace_event(
  self : SortSession,
  event : SourceScanEvent,
) -> Unit {
  if self.trace is Some(callback) {
    callback(event)
  }
}

///|
fn SortSession::check_cancel(self : SortSession) -> Unit raise McapError {
  if (self.cancelled)() {
    raise Invalid("external merge cancelled")
  }
}

///|
fn SortSession::create(
  self : SortSession,
  count : UInt64,
) -> (Int, WriteSink) raise McapError {
  self.check_cancel()
  if self.next_id >= self.options.run_limit {
    raise Invalid("temporary run count budget exceeded")
  }
  let id = self.next_id
  self.next_id += 1
  self.live[id] = 0UL
  let target = (self.storage.create)(id)
  let sink = new_write_sink(fn(bytes) {
    self.check_cancel()
    if bytes.length().to_uint64() > self.options.spool_bytes - self.spool {
      raise Invalid("temporary disk byte budget exceeded")
    }
    target.write(bytes)
    let size = bytes.length().to_uint64()
    self.live[id] = self.live[id] + size
    self.spool += size
    if self.spool > self.peak_spool {
      self.peak_spool = self.spool
    }
  })
  let header = @buffer.Buffer::Buffer()
  header.write_bytes(b"MSRT0\r\n\x00"[:])
  header.write_uint64_le(count)
  sink.write(header.to_bytes())
  (id, sink)
}

///|
fn write_sort_row(sink : WriteSink, row : SortRow) -> Unit raise McapError {
  let body = row.ordinal.to_le_bytes() + encode_message(row.message)
  sink.write(frame(0x80, crc32(body).mcap_le_bytes() + body))
}

///|
fn SortSession::flush_run(self : SortSession) -> Int? raise McapError {
  if self.buffer.is_empty() {
    return None
  }
  self.buffer.sort_by(compare_rows)
  let (id, sink) = self.create(self.buffer.length().to_uint64())
  for row in self.buffer {
    write_sort_row(sink, row)
  }
  self.buffer.clear()
  self.buffered = 0
  Some(id)
}

///|
fn SortSession::add(
  self : SortSession,
  message : Message,
  runs : Array[Int],
) -> Unit raise McapError {
  self.check_cancel()
  if message.data.length() > self.options.run_bytes - 43 {
    raise Invalid("message cannot fit external sort run budget")
  }
  let size = message.data.length() + 43
  if size > self.options.run_bytes - self.buffered {
    if self.flush_run() is Some(id) {
      runs.push(id)
    }
  }
  self.buffer.push({ message, ordinal: self.ordinal, })
  self.ordinal += 1UL
  self.buffered += size
  self.peak_run = self.peak_run.max(self.buffered)
}

///|
struct RunInput {
  source : ReadSource
  limits : Limits
  mut position : UInt64
  mut remaining : UInt64
  count : UInt64
  mut previous : (UInt64, UInt64)?
}

///|
fn SortSession::open_run(
  self : SortSession,
  id : Int,
) -> RunInput raise McapError {
  let source = (self.storage.open)(id)
  if source.size != self.live[id] || source.size < 16UL {
    raise Invalid("temporary run size changed")
  }
  let header = cursor(source.read_exact(0UL, 16), self.options.limits)
  if header.take(8) != b"MSRT0\r\n\x00" {
    raise Invalid("invalid temporary run magic")
  }
  let count = header.u64()
  if count > self.ordinal {
    raise Invalid("temporary run count exceeds ingested messages")
  }
  {
    source,
    limits: { ..self.options.limits, max_record_bytes: self.options.run_bytes, },
    position: 16UL,
    remaining: count,
    count,
    previous: None,
  }
}

///|
fn RunInput::next(self : RunInput) -> SortRow? raise McapError {
  if self.remaining == 0UL {
    if self.position != self.source.size {
      raise Invalid("temporary run has trailing or extra records")
    }
    return None
  }
  let record = self.source.record(self.position, self.source.size, self.limits)
  if record.opcode != 0x80 {
    raise Invalid("invalid temporary run record")
  }
  let input = cursor(record.body, self.limits)
  let checksum = input.u32()
  let body = input.take(record.body.length() - 4)
  if crc32(body) != checksum {
    raise Invalid("temporary run checksum mismatch")
  }
  let value = cursor(body, self.limits)
  let ordinal = value.u64()
  let message = decode_message(
    value.take(body.length() - 8),
    limits=self.limits,
  )
  if self.previous is Some((time, order)) {
    if message.log_time < time || (message.log_time == time && ordinal <= order) {
      raise Invalid("temporary run is not stably ordered")
    }
  }
  self.previous = Some((message.log_time, ordinal))
  self.position += (record.body.length() + 9).to_uint64()
  self.remaining -= 1UL
  Some({ message, ordinal, })
}

///|
struct RunHead {
  row : SortRow
  input : Int
}

///|
fn push_run_head(heap : Array[RunHead], head : RunHead) -> Unit {
  heap.push(head)
  let mut child = heap.length() - 1
  while child > 0 {
    let parent = (child - 1) / 2
    if compare_rows(heap[child].row, heap[parent].row) >= 0 {
      break
    }
    let old = heap[parent]
    heap[parent] = heap[child]
    heap[child] = old
    child = parent
  }
}

///|
fn pop_run_head(heap : Array[RunHead]) -> RunHead {
  let head = heap[0]
  let last = heap.pop().unwrap()
  if !heap.is_empty() {
    heap[0] = last
  }
  let mut parent = 0
  while parent < heap.length() / 2 {
    let left = parent * 2 + 1
    let right = left + 1
    let child = if right < heap.length() &&
      compare_rows(heap[right].row, heap[left].row) < 0 {
      right
    } else {
      left
    }
    if compare_rows(heap[child].row, heap[parent].row) >= 0 {
      break
    }
    let old = heap[parent]
    heap[parent] = heap[child]
    heap[child] = old
    parent = child
  }
  head
}

///|
fn SortSession::merge_group(
  self : SortSession,
  ids : Array[Int],
  emit : (SortRow) -> Unit raise McapError,
) -> Unit raise McapError {
  let inputs = []
  let heap : Array[RunHead] = []
  let mut bytes = 0
  for id in ids {
    let input = self.open_run(id)
    inputs.push(input)
    if input.next() is Some(row) {
      let size = row.message.data.length() + 43
      if size > self.options.merge_bytes - bytes {
        raise Invalid("merge heads exceed memory budget")
      }
      bytes += size
      push_run_head(heap, { row, input: inputs.length() - 1, })
    }
  }
  self.peak_merge = self.peak_merge.max(bytes)
  while !heap.is_empty() {
    self.check_cancel()
    let head = pop_run_head(heap)
    bytes -= head.row.message.data.length() + 43
    emit(head.row)
    if inputs[head.input].next() is Some(row) {
      let size = row.message.data.length() + 43
      if size > self.options.merge_bytes - bytes {
        raise Invalid("merge heads exceed memory budget")
      }
      bytes += size
      push_run_head(heap, { row, input: head.input, })
      self.peak_merge = self.peak_merge.max(bytes)
    }
  }
}

///|
fn SortSession::remove(self : SortSession, id : Int) -> Unit raise McapError {
  (self.storage.remove)(id)
  self.spool -= self.live[id]
  self.live.remove(id)
}

///|
fn SortSession::sort_to(
  self : SortSession,
  runs : Array[Int],
  writer : StreamWriter,
) -> Unit raise McapError {
  let mut current = runs.copy()
  while current.length() > self.options.fan_in {
    self.passes += 1
    let next = []
    let mut start = 0
    while start < current.length() {
      let end = start + self.options.fan_in.min(current.length() - start)
      let group = current[start:end].to_array()
      let mut count = 0UL
      for id in group {
        count += self.open_run(id).count
      }
      let (id, output) = self.create(count)
      self.merge_group(group, fn(row) { write_sort_row(output, row) })
      next.push(id)
      for old in group {
        self.remove(old)
      }
      start = end
    }
    current = next
  }
  if !current.is_empty() {
    self.passes += 1
    self.merge_group(current, fn(row) { writer.append(row.message) })
  }
  for id in current {
    self.remove(id)
  }
}
