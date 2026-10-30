///|
pub(all) enum ReplayState {
  ReplayReady
  ReplayRunning
  ReplayPaused
  ReplayDone
  ReplayCancelled
  ReplayFailed
} derive(Eq, @debug.Debug)

///|
pub(all) enum ReplayPoll {
  ReplayWait(UInt64)
  ReplayIdle
  ReplayEvent(Message)
  ReplayEnd
}

///|
pub(all) struct ReplayProgress {
  state : ReplayState
  position : UInt64
  emitted : Int32
  rate_numerator : Int32
  rate_denominator : Int32
  generation : Int32
}

///|
pub struct Replay {
  source : QuerySource
  query : Query
  budget : Int32
  mut stream : MessageStream
  mut pending : Message?
  mut state : ReplayState
  mut position : UInt64
  mut wall : UInt64
  mut last_clock : UInt64?
  mut numerator : Int32
  mut denominator : Int32
  mut emitted : Int32
  mut generation : Int32
  mut previous : UInt64?
}

///|
fn replay_rate(numerator : Int32, denominator : Int32) -> Unit raise McapError {
  if numerator < 1 ||
    denominator < 1 ||
    numerator > 1000000 ||
    denominator > 1000000 {
    raise Invalid("replay rate must be a positive ratio up to 1000000/1000000")
  }
}

///|
pub fn replay(
  source : QuerySource,
  query~ : Query = all_query(),
  rate_numerator~ : Int32 = 1,
  rate_denominator~ : Int32 = 1,
  pending_bytes~ : Int32 = 64 * 1024 * 1024,
) -> Replay raise McapError {
  replay_rate(rate_numerator, rate_denominator)
  if query.descending ||
    query.start_time > query.end_time ||
    query.offset < 0 ||
    query.limit < 0 ||
    pending_bytes < 1 {
    raise Invalid("replay needs an ascending bounded query")
  }
  let stream = (source.open)(query, pending_bytes)
  let pending = (stream.next_message)() catch {
    error => {
      (stream.stop)()
      raise error
    }
  }
  {
    source,
    query: { ..query, topics: query.topics.copy(), },
    budget: pending_bytes,
    stream,
    pending,
    state: ReplayReady,
    position: pending.map(fn(m) { m.log_time }).unwrap_or(query.start_time),
    wall: 0UL,
    last_clock: None,
    numerator: rate_numerator,
    denominator: rate_denominator,
    emitted: 0,
    generation: 0,
    previous: None,
  }
}

///|
fn Replay::clock(self : Replay, now : UInt64) -> Unit raise McapError {
  if self.last_clock is Some(previous) && now < previous {
    raise Invalid("replay requires a monotonic clock")
  }
  if self.last_clock is None {
    self.wall = now
  }
  self.last_clock = Some(now)
}

///|
fn Replay::at(self : Replay, now : UInt64) -> UInt64 {
  if self.state != ReplayRunning {
    return self.position
  }
  let value = @bigint.BigInt::from_uint64(self.position) +
    @bigint.BigInt::from_uint64(now - self.wall) *
    @bigint.BigInt::from_int(self.numerator) /
    @bigint.BigInt::from_int(self.denominator)
  let maximum = @bigint.BigInt::from_uint64(18446744073709551615UL)
  if value > maximum {
    18446744073709551615UL
  } else {
    value.to_uint64()
  }
}

///|
fn Replay::take(self : Replay) -> Message? raise McapError {
  if self.pending is None {
    self.pending = (self.stream.next_message)()
  }
  let message = self.pending
  if message is Some(m) {
    if self.previous is Some(time) && m.log_time < time {
      raise Invalid("replay provider returned unordered messages")
    }
    self.previous = Some(m.log_time)
    self.pending = None
    self.emitted += 1
  } else {
    self.position = self.at(self.last_clock.unwrap_or(self.wall))
    self.state = ReplayDone
    (self.stream.stop)()
  }
  message
}

///|
pub fn Replay::poll(self : Replay, now : UInt64) -> ReplayPoll raise McapError {
  if [ReplayCancelled, ReplayFailed, ReplayDone].contains(self.state) {
    return ReplayEnd
  }
  try {
    self.clock(now)
    if self.state == ReplayPaused {
      return ReplayIdle
    }
    if self.state == ReplayReady {
      self.state = ReplayRunning
      self.wall = now
    }
    if self.pending is None {
      self.pending = (self.stream.next_message)()
    }
    guard self.pending is Some(message) else {
      self.position = self.at(now)
      self.state = ReplayDone
      (self.stream.stop)()
      return ReplayEnd
    }
    let position = self.at(now)
    if message.log_time > position {
      let distance = @bigint.BigInt::from_uint64(
          message.log_time - self.position,
        ) *
        @bigint.BigInt::from_int(self.denominator) -
        @bigint.BigInt::from_uint64(now - self.wall) *
        @bigint.BigInt::from_int(self.numerator)
      let divisor = @bigint.BigInt::from_int(self.numerator)
      let delay = (distance + divisor - @bigint.BigInt::from_int(1)) / divisor
      let maximum = @bigint.BigInt::from_uint64(18446744073709551615UL)
      return ReplayWait(
        if delay > maximum {
          18446744073709551615UL
        } else {
          delay.to_uint64()
        },
      )
    }
    match self.take() {
      Some(message) => ReplayEvent(message)
      None => ReplayEnd
    }
  } catch {
    error => {
      self.state = ReplayFailed
      (self.stream.stop)()
      raise error
    }
  }
}

///|
pub fn Replay::pause(self : Replay, now : UInt64) -> Unit raise McapError {
  if ![ReplayReady, ReplayRunning, ReplayPaused].contains(self.state) {
    raise Invalid("replay cannot pause in terminal state")
  }
  self.clock(now)
  self.position = self.at(now)
  self.wall = now
  self.state = ReplayPaused
}

///|
pub fn Replay::resume(self : Replay, now : UInt64) -> Unit raise McapError {
  if self.state != ReplayPaused {
    raise Invalid("replay resume requires paused state")
  }
  self.clock(now)
  self.wall = now
  self.state = ReplayRunning
}

///|
pub fn Replay::set_rate(
  self : Replay,
  numerator : Int32,
  denominator : Int32,
  now : UInt64,
) -> Unit raise McapError {
  replay_rate(numerator, denominator)
  if ![ReplayReady, ReplayRunning, ReplayPaused].contains(self.state) {
    raise Invalid("replay cannot change rate in terminal state")
  }
  self.clock(now)
  self.position = self.at(now)
  self.wall = now
  self.numerator = numerator
  self.denominator = denominator
}

///|
pub fn Replay::step(self : Replay, now : UInt64) -> Message? raise McapError {
  if self.state != ReplayPaused {
    raise Invalid("single-step requires paused replay")
  }
  self.clock(now)
  try {
    let message = self.take()
    if message is Some(m) {
      self.position = m.log_time
      self.wall = now
    }
    message
  } catch {
    error => {
      self.state = ReplayFailed
      (self.stream.stop)()
      raise error
    }
  }
}

///|
pub fn Replay::seek(
  self : Replay,
  time : UInt64,
  now : UInt64,
) -> Unit raise McapError {
  if [ReplayCancelled, ReplayFailed].contains(self.state) ||
    time < self.query.start_time ||
    time > self.query.end_time {
    raise Invalid("replay seek outside query or terminal state")
  }
  self.clock(now)
  let paused = self.state == ReplayPaused
  (self.stream.stop)()
  try {
    self.stream = (self.source.open)(
      { ..self.query, start_time: time, offset: 0, },
      self.budget,
    )
    self.pending = None
    self.previous = None
    self.position = time
    self.wall = now
    self.emitted = 0
    self.generation += 1
    self.state = if paused { ReplayPaused } else { ReplayRunning }
  } catch {
    error => {
      self.state = ReplayFailed
      raise error
    }
  }
}

///|
pub fn Replay::cancel(self : Replay) -> Unit {
  if self.state != ReplayCancelled {
    self.position = self.at(self.last_clock.unwrap_or(self.wall))
    (self.stream.stop)()
    self.pending = None
    self.state = ReplayCancelled
  }
}

///|
pub fn Replay::progress(
  self : Replay,
  now : UInt64,
) -> ReplayProgress raise McapError {
  self.clock(now)
  {
    state: self.state,
    position: self.at(now),
    emitted: self.emitted,
    rate_numerator: self.numerator,
    rate_denominator: self.denominator,
    generation: self.generation,
  }
}
