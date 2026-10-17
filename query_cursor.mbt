///|
pub(all) enum QueryCursorState {
  Active
  Exhausted
  Cancelled
} derive(Eq, @debug.Debug)

///|
/// Counts refer to the current traversal, reset by seek. Remaining respects limit.
pub(all) struct QueryProgress {
  state : QueryCursorState
  matching_messages : Int
  skipped_messages : Int
  emitted_messages : Int
  remaining_messages : Int
  buffered_heads : Int
} derive(Eq, @debug.Debug)

///|
struct CursorRun {
  positions : Array[Int]
  current : Int
  start : Int
  end : Int
}

///|
/// A cursor shares the index's immutable snapshot, retaining one head per run.
/// It does not retain a second array of all matching messages or reread files.
pub struct QueryCursor {
  index : QueryIndex
  query : Query
  heap : Array[CursorRun]
  mut state : QueryCursorState
  mut matching : Int
  mut skipped : Int
  mut emitted : Int
  mut remaining : Int
}

///|
fn QueryCursor::precedes(
  self : QueryCursor,
  a : CursorRun,
  b : CursorRun,
) -> Bool {
  let pa = a.positions[a.current]
  let pb = b.positions[b.current]
  let ta = self.index.log.messages[pa].log_time
  let tb = self.index.log.messages[pb].log_time
  if ta == tb {
    if self.query.descending {
      pa > pb
    } else {
      pa < pb
    }
  } else if self.query.descending {
    ta > tb
  } else {
    ta < tb
  }
}

///|
fn QueryCursor::push_head(self : QueryCursor, head : CursorRun) -> Unit {
  self.heap.push(head)
  let mut child = self.heap.length() - 1
  while child > 0 {
    let parent = (child - 1) / 2
    if !self.precedes(self.heap[child], self.heap[parent]) {
      break
    }
    let value = self.heap[parent]
    self.heap[parent] = self.heap[child]
    self.heap[child] = value
    child = parent
  }
}

///|
fn QueryCursor::sift_head(self : QueryCursor) -> Unit {
  let mut parent = 0
  while parent < self.heap.length() / 2 {
    let left = parent * 2 + 1
    let right = left + 1
    let child = if right < self.heap.length() &&
      self.precedes(self.heap[right], self.heap[left]) {
      right
    } else {
      left
    }
    if !self.precedes(self.heap[child], self.heap[parent]) {
      break
    }
    let value = self.heap[parent]
    self.heap[parent] = self.heap[child]
    self.heap[child] = value
    parent = child
  }
}

///|
fn QueryCursor::take_position(self : QueryCursor) -> Int {
  let head = self.heap[0]
  let position = head.positions[head.current]
  let next = if self.query.descending {
    head.current - 1
  } else {
    head.current + 1
  }
  if next >= head.start && next < head.end {
    self.heap[0] = { ..head, current: next, }
  } else {
    let last = self.heap.pop().unwrap()
    if !self.heap.is_empty() {
      self.heap[0] = last
    }
  }
  self.sift_head()
  position
}

///|
fn QueryCursor::add_run(
  self : QueryCursor,
  positions : Array[Int],
  start_time : UInt64,
  end_time : UInt64,
) -> Unit {
  let start = lower_bound(self.index.log, positions, start_time, false)
  let end = lower_bound(self.index.log, positions, end_time, true)
  self.matching += end - start
  if start < end {
    self.push_head({
      positions,
      current: if self.query.descending {
        end - 1
      } else {
        start
      },
      start,
      end,
    })
  }
}

///|
fn QueryCursor::reset(
  self : QueryCursor,
  start_time : UInt64,
  end_time : UInt64,
  offset : Int,
) -> Unit {
  self.heap.clear()
  self.matching = 0
  self.skipped = 0
  self.emitted = 0
  if self.query.topics.is_empty() {
    // The global index already supplies the stable order, so only one run is needed.
    self.add_run(self.index.all_messages, start_time, end_time)
  } else {
    for channel in self.index.log.channels {
      if self.query.topics.contains(channel.topic) {
        if self.index.by_channel.get(channel.id) is Some(positions) {
          self.add_run(positions, start_time, end_time)
        }
      }
    }
  }
  let skipped = offset.min(self.matching)
  self.remaining = self.query.limit.min(self.matching - skipped)
  self.skipped = skipped
  // There is no reason to walk the offset if the requested result is empty.
  if self.remaining == 0 {
    self.heap.clear()
    self.state = Exhausted
    return
  }
  for _i = 0; _i < skipped; _i = _i + 1 {
    ignore(self.take_position())
  }
  self.state = Active
}

///|
/// Offset/limit apply once across all pages. Duplicate topic filters are harmless.
/// The snapshot is shared with the index; returned message values and Bytes are immutable.
pub fn QueryIndex::cursor(
  self : QueryIndex,
  query : Query,
) -> QueryCursor raise McapError {
  if query.start_time > query.end_time || query.offset < 0 || query.limit < 0 {
    raise Invalid("invalid query range or pagination")
  }
  let cursor : QueryCursor = {
    index: self,
    query: { ..query, topics: query.topics.copy(), },
    heap: [],
    state: Exhausted,
    matching: 0,
    skipped: 0,
    emitted: 0,
    remaining: 0,
  }
  cursor.reset(query.start_time, query.end_time, query.offset)
  cursor
}

///|
/// Return the next message, or None after exhaustion/cancellation.
pub fn QueryCursor::next(self : QueryCursor) -> Message? {
  if self.state != Active {
    return None
  }
  let position = self.take_position()
  self.emitted += 1
  self.remaining -= 1
  if self.remaining == 0 {
    self.heap.clear()
    self.state = Exhausted
  }
  let message = self.index.log.messages[position]
  Some(message)
}

///|
/// Page size must be positive. An invalid size leaves cursor position unchanged.
pub fn QueryCursor::next_page(
  self : QueryCursor,
  size : Int,
) -> Array[Message] raise McapError {
  if size <= 0 {
    raise Invalid("cursor page size must be positive")
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
/// Restart inclusively at time within the original interval, in query direction.
/// Offset is not reapplied; limit and progress restart. Cancelled cursors stay closed.
/// Seeking outside the original interval fails without changing traversal state.
pub fn QueryCursor::seek(
  self : QueryCursor,
  time : UInt64,
) -> Unit raise McapError {
  if self.state == Cancelled {
    raise Invalid("cursor is cancelled")
  }
  if time < self.query.start_time || time > self.query.end_time {
    raise Invalid("cursor seek is outside the original query interval")
  }
  if self.query.descending {
    self.reset(self.query.start_time, time, 0)
  } else {
    self.reset(time, self.query.end_time, 0)
  }
}

///|
/// Cancellation is terminal and idempotent, releasing pending run references.
pub fn QueryCursor::cancel(self : QueryCursor) -> Unit {
  self.heap.clear()
  self.remaining = 0
  self.state = Cancelled
}

///|
pub fn QueryCursor::progress(self : QueryCursor) -> QueryProgress {
  {
    state: self.state,
    matching_messages: self.matching,
    skipped_messages: self.skipped,
    emitted_messages: self.emitted,
    remaining_messages: self.remaining,
    buffered_heads: self.heap.length(),
  }
}
