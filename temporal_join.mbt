///|
pub(all) enum JoinMode {
  JoinAsOf
  JoinNearest
} derive(Eq, @debug.Debug)

///|
pub(all) enum MissingMatch {
  JoinKeep
  JoinDrop
} derive(Eq, @debug.Debug)

///|
pub(all) struct JoinOptions {
  mode : JoinMode
  tolerance : UInt64
  missing : MissingMatch
  buffer_bytes : Int
  buffer_messages : Int
} derive(Eq, @debug.Debug)

///|
pub fn join_options() -> JoinOptions {
  {
    mode: JoinNearest,
    tolerance: 0UL,
    missing: JoinKeep,
    buffer_bytes: 64 * 1024 * 1024,
    buffer_messages: 3,
  }
}

///|
pub(all) struct TemporalMatch {
  left : Message
  right : Message?
  distance : UInt64?
  right_is_earlier : Bool
} derive(Eq, @debug.Debug)

///|
pub(all) enum JoinPoll {
  JoinReadLeft
  JoinReadRight
  JoinRow(TemporalMatch)
  JoinDone
}

///|
pub struct TemporalJoinCursor {
  options : JoinOptions
  mut left : Message?
  mut before : Message?
  mut after : Message?
  mut left_done : Bool
  mut right_done : Bool
  mut waiting : Int
  mut last_left : UInt64?
  mut last_right : UInt64?
  mut state : FileCursorState
  mut left_read : UInt64
  mut right_read : UInt64
  mut matched : UInt64
  mut unmatched : UInt64
  mut peak_bytes : Int
}

///|
pub fn temporal_join_cursor(
  options? : JoinOptions = join_options(),
) -> TemporalJoinCursor raise McapError {
  if options.buffer_bytes < 31 ||
    options.buffer_messages < 1 ||
    options.buffer_messages > 3 {
    raise Invalid("invalid temporal join buffer budget")
  }
  {
    options,
    left: None,
    before: None,
    after: None,
    left_done: false,
    right_done: false,
    waiting: 0,
    last_left: None,
    last_right: None,
    state: FileActive,
    left_read: 0UL,
    right_read: 0UL,
    matched: 0UL,
    unmatched: 0UL,
    peak_bytes: 0,
  }
}

///|
fn TemporalJoinCursor::clear(self : TemporalJoinCursor) -> Unit {
  self.left = None
  self.before = None
  self.after = None
  self.waiting = 0
}

///|
fn TemporalJoinCursor::check_buffer(
  self : TemporalJoinCursor,
) -> Unit raise McapError {
  let mut bytes = 0UL
  let mut count = 0
  for message in [self.left, self.before, self.after] {
    if message is Some(m) {
      bytes += m.data.length().to_uint64() + 31UL
      count += 1
    }
  }
  if bytes > self.options.buffer_bytes.to_uint64() ||
    count > self.options.buffer_messages {
    raise Invalid("temporal join retained payload budget exceeded")
  }
  self.peak_bytes = self.peak_bytes.max(bytes.to_int())
}

///|
pub fn TemporalJoinCursor::feed_left(
  self : TemporalJoinCursor,
  message : Message?,
) -> Unit raise McapError {
  try {
    if self.state != FileActive ||
      self.waiting != 1 ||
      self.left is Some(_) ||
      self.left_done {
      raise Invalid("unexpected temporal left feed")
    }
    if message is Some(m) {
      if self.last_left is Some(time) && m.log_time < time {
        raise Invalid("temporal left stream is not ascending")
      }
      self.last_left = Some(m.log_time)
      self.left_read += 1UL
    } else {
      self.left_done = true
    }
    self.left = message
    self.waiting = 0
    self.check_buffer()
  } catch {
    error => {
      self.state = FileFailed
      self.clear()
      raise error
    }
  }
}

///|
pub fn TemporalJoinCursor::feed_right(
  self : TemporalJoinCursor,
  message : Message?,
) -> Unit raise McapError {
  try {
    if self.state != FileActive ||
      self.waiting != 2 ||
      self.after is Some(_) ||
      self.right_done {
      raise Invalid("unexpected temporal right feed")
    }
    if message is Some(m) {
      if self.last_right is Some(time) && m.log_time < time {
        raise Invalid("temporal right stream is not ascending")
      }
      self.last_right = Some(m.log_time)
      self.right_read += 1UL
    } else {
      self.right_done = true
    }
    self.after = message
    self.waiting = 0
    self.check_buffer()
  } catch {
    error => {
      self.state = FileFailed
      self.clear()
      raise error
    }
  }
}

///|
/// Ascending many-to-one left join. Equal-distance ties prefer the earlier
/// timestamp; duplicate right timestamps prefer the first stable input record.
pub fn TemporalJoinCursor::poll(
  self : TemporalJoinCursor,
) -> JoinPoll raise McapError {
  if self.state == FileFailed {
    raise Invalid("temporal join cursor failed")
  }
  if self.state != FileActive {
    return JoinDone
  }
  while true {
    guard self.left is Some(left) else {
      if self.left_done {
        self.state = FileExhausted
        self.clear()
        return JoinDone
      }
      self.waiting = 1
      return JoinReadLeft
    }
    if self.after is None && !self.right_done {
      self.waiting = 2
      return JoinReadRight
    }
    if self.after is Some(right) && right.log_time <= left.log_time {
      if self.before is None ||
        (self.before is Some(old) && right.log_time > old.log_time) {
        self.before = Some(right)
      }
      self.after = None
      continue
    }
    let chosen = match (self.before, self.after) {
      (before, _) if self.options.mode == JoinAsOf => before
      (None, after) => after
      (before, None) => before
      (Some(before), Some(after)) =>
        if left.log_time - before.log_time <= after.log_time - left.log_time {
          Some(before)
        } else {
          Some(after)
        }
    }
    let distance = chosen.map(fn(right) {
      if right.log_time <= left.log_time {
        left.log_time - right.log_time
      } else {
        right.log_time - left.log_time
      }
    })
    let accepted = distance is Some(d) && d <= self.options.tolerance
    self.left = None
    if accepted {
      self.matched += 1UL
    } else {
      self.unmatched += 1UL
    }
    if !accepted && self.options.missing == JoinDrop {
      continue
    }
    return JoinRow({
      left,
      right: if accepted {
        chosen
      } else {
        None
      },
      distance: if accepted {
        distance
      } else {
        None
      },
      right_is_earlier: accepted &&
      chosen is Some(r) &&
      r.log_time <= left.log_time,
    })
  }
  JoinDone
}

///|
pub fn TemporalJoinCursor::cancel(self : TemporalJoinCursor) -> Unit {
  if self.state == FileActive {
    self.state = FileCancelled
  }
  self.clear()
}

///|
pub fn TemporalJoinCursor::info(self : TemporalJoinCursor) -> Json {
  {
    "left_read": self.left_read.to_string(),
    "right_read": self.right_read.to_string(),
    "matched": self.matched.to_string(),
    "unmatched": self.unmatched.to_string(),
    "peak_retained_bytes": self.peak_bytes,
    "buffer_bytes": self.options.buffer_bytes,
    "buffer_messages": self.options.buffer_messages,
    "finished": self.state == FileExhausted,
  }
}

///|
pub struct TemporalJoinStream {
  cursor : TemporalJoinCursor
  left : MessageStream
  right : MessageStream
  mut closed : Bool
}

///|
pub fn temporal_join(
  left : MessageStream,
  right : MessageStream,
  options? : JoinOptions = join_options(),
) -> TemporalJoinStream raise McapError {
  { cursor: temporal_join_cursor(options~), left, right, closed: false, }
}

///|
fn TemporalJoinStream::close(self : TemporalJoinStream) -> Unit {
  if !self.closed {
    self.closed = true
    (self.left.stop)()
    (self.right.stop)()
  }
}

///|
pub fn TemporalJoinStream::next(
  self : TemporalJoinStream,
) -> TemporalMatch? raise McapError {
  if self.closed {
    return None
  }
  try {
    while true {
      match self.cursor.poll() {
        JoinReadLeft => self.cursor.feed_left((self.left.next_message)())
        JoinReadRight => self.cursor.feed_right((self.right.next_message)())
        JoinRow(row) => return Some(row)
        JoinDone => {
          self.close()
          return None
        }
      }
    }
    None
  } catch {
    error => {
      self.cursor.cancel()
      self.close()
      raise error
    }
  }
}

///|
pub fn TemporalJoinStream::cancel(self : TemporalJoinStream) -> Unit {
  self.cursor.cancel()
  self.close()
}

///|
pub fn TemporalJoinStream::info(self : TemporalJoinStream) -> Json {
  self.cursor.info()
}
