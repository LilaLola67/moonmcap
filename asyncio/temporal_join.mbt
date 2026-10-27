///|
pub struct AsyncTemporalJoin {
  cursor : @mcap.TemporalJoinCursor
  left : AsyncMessageStream
  right : AsyncMessageStream
  mut closed : Bool
  mut busy : Bool
}

///|
pub fn temporal_join(
  left : AsyncMessageStream,
  right : AsyncMessageStream,
  options? : @mcap.JoinOptions = @mcap.join_options(),
) -> AsyncTemporalJoin raise @mcap.McapError {
  {
    cursor: @mcap.temporal_join_cursor(options~),
    left,
    right,
    closed: false,
    busy: false,
  }
}

///|
pub fn AsyncTemporalJoin::cancel(self : AsyncTemporalJoin) -> Unit {
  self.cursor.cancel()
  if !self.closed {
    self.closed = true
    (self.left.stop)()
    (self.right.stop)()
  }
}

///|
pub async fn AsyncTemporalJoin::next(
  self : AsyncTemporalJoin,
) -> @mcap.TemporalMatch? {
  if self.busy {
    raise @mcap.io_error("concurrent temporal join operation")
  }
  if self.closed {
    return None
  }
  self.busy = true
  defer {
    self.busy = false
  }
  try {
    while true {
      match self.cursor.poll() {
        @mcap.JoinReadLeft => {
          let value = (self.left.next)()
          if self.closed {
            return None
          }
          self.cursor.feed_left(value)
        }
        @mcap.JoinReadRight => {
          let value = (self.right.next)()
          if self.closed {
            return None
          }
          self.cursor.feed_right(value)
        }
        @mcap.JoinRow(row) => return Some(row)
        @mcap.JoinDone => {
          self.cancel()
          return None
        }
      }
    }
    None
  } catch {
    error => {
      self.cancel()
      raise error
    }
  }
}

///|
pub fn AsyncTemporalJoin::info(self : AsyncTemporalJoin) -> Json {
  self.cursor.info()
}
