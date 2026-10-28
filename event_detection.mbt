///|
pub(all) struct EventRule {
  topic : String
  enter : String
  leave : String?
  duration : UInt64
  max_gap : UInt64
  cooldown : UInt64
  pre : UInt64
  post : UInt64
}

///|
pub fn event_rule(topic : String, enter : String) -> EventRule {
  {
    topic,
    enter,
    leave: None,
    duration: 0UL,
    max_gap: 1000000000UL,
    cooldown: 0UL,
    pre: 0UL,
    post: 0UL,
  }
}

///|
pub(all) struct EventSpan {
  start : UInt64
  confirmed : UInt64
  end : UInt64
  closed : Bool
} derive(Eq, @debug.Debug)

///|
pub(all) enum EventSignal {
  EventStarted(UInt64, UInt64)
  EventClosed(EventSpan)
}

///|
pub struct EventDetector {
  rule : EventRule
  enter : JsonFilter
  leave : JsonFilter?
  mut previous : UInt64?
  mut candidate : UInt64?
  mut confirmed : UInt64?
  mut ended : UInt64?
  mut finished : Bool
}

///|
pub fn event_detector(rule : EventRule) -> EventDetector raise McapError {
  if rule.topic == "" || rule.max_gap == 0UL {
    raise Invalid("event rule requires a topic and positive continuity gap")
  }
  {
    rule,
    enter: json_filter(rule.enter),
    leave: rule.leave.map(fn(text) { json_filter(text) }),
    previous: None,
    candidate: None,
    confirmed: None,
    ended: None,
    finished: false,
  }
}

///|
fn EventDetector::close_span(
  self : EventDetector,
  end : UInt64,
  closed : Bool,
) -> EventSpan? {
  let span = match (self.candidate, self.confirmed) {
    (Some(start), Some(confirmed)) => Some({ start, confirmed, end, closed, })
    _ => None
  }
  if span is Some(_) {
    self.ended = Some(end)
  }
  self.candidate = None
  self.confirmed = None
  span
}

///|
/// Feed only this rule's topic, in ascending log-time order. Continuity is
/// sampled evidence; a long gap closes an active event as incomplete.
pub fn EventDetector::feed(
  self : EventDetector,
  message : Message,
) -> Array[EventSignal] raise McapError {
  if self.finished || (self.previous is Some(t) && message.log_time < t) {
    raise Invalid("event detector is finished or time moved backwards")
  }
  // Decode both predicates before changing state: malformed payload is retryable.
  let enter = self.enter.matches(message.data)
  let leave = match self.leave {
    Some(predicate) => predicate.matches(message.data)
    None => !enter
  }
  let signals = []
  if self.previous is Some(t) && message.log_time - t > self.rule.max_gap {
    if self.close_span(t, false) is Some(span) {
      signals.push(EventClosed(span))
    }
  }
  if self.confirmed is Some(_) {
    if leave {
      if self.close_span(message.log_time, true) is Some(span) {
        signals.push(EventClosed(span))
      }
    }
  } else if enter && !leave {
    let cooling = self.ended is Some(t) &&
      message.log_time - t < self.rule.cooldown
    if !cooling {
      if self.candidate is None {
        self.candidate = Some(message.log_time)
      }
      let start = self.candidate.unwrap()
      if message.log_time - start >= self.rule.duration {
        self.confirmed = Some(message.log_time)
        signals.push(EventStarted(start, message.log_time))
      }
    }
  } else {
    self.candidate = None
  }
  self.previous = Some(message.log_time)
  signals
}

///|
pub fn EventDetector::finish(self : EventDetector) -> EventSpan? {
  if self.finished {
    return None
  }
  self.finished = true
  self.close_span(self.previous.unwrap_or(0UL), false)
}

///|
pub(all) struct EventWindow {
  start : UInt64
  end : UInt64
  events : Array[EventSpan]
}

///|
fn time_before(time : UInt64, span : UInt64) -> UInt64 {
  if time < span {
    0UL
  } else {
    time - span
  }
}

///|
fn time_after(time : UInt64, span : UInt64) -> UInt64 {
  if span > 18446744073709551615UL - time {
    18446744073709551615UL
  } else {
    time + span
  }
}

///|
/// Overlapping/nearby closed windows merge; event evidence remains separate.
pub fn event_windows(
  events : Array[EventSpan],
  pre : UInt64,
  post : UInt64,
  merge_gap? : UInt64 = 0UL,
) -> Array[EventWindow] raise McapError {
  let windows : Array[EventWindow] = []
  let mut previous : UInt64? = None
  for event in events {
    if event.start > event.confirmed ||
      event.confirmed > event.end ||
      (previous is Some(t) && event.start < t) {
      raise Invalid("invalid or unordered event spans")
    }
    previous = Some(event.start)
    let start = time_before(event.start, pre)
    let end = time_after(event.end, post)
    if !windows.is_empty() &&
      start <= time_after(windows[windows.length() - 1].end, merge_gap) {
      let old = windows[windows.length() - 1]
      old.events.push(event)
      windows[windows.length() - 1] = {
        ..old,
        end: if old.end > end {
          old.end
        } else {
          end
        },
      }
    } else {
      windows.push({ start, end, events: [event], })
    }
  }
  windows
}

///|
pub fn detect_events(
  source : QuerySource,
  rule : EventRule,
  start? : UInt64 = 0UL,
  end? : UInt64 = 18446744073709551615UL,
  event_limit? : Int = 1024,
  pending_bytes? : Int = 64 * 1024 * 1024,
  cancelled? : () -> Bool = fn() { false },
) -> Array[EventSpan] raise McapError {
  if start > end || event_limit < 1 || event_limit > 100000 || pending_bytes < 1 {
    raise Invalid("invalid event bounds or budgets")
  }
  if !source.declarations.channels.any(fn(c) { c.topic == rule.topic }) {
    raise Invalid("event topic absent")
  }
  let detector = event_detector(rule)
  let stream = (source.open)(
    { ..all_query(), topics: [rule.topic], start_time: start, end_time: end, },
    pending_bytes,
  )
  defer (stream.stop)()
  let events = []
  fn add(event : EventSpan) -> Unit raise McapError {
    if events.length() >= event_limit {
      raise Invalid("event count budget exceeded")
    }
    events.push(event)
  }
  while (stream.next_message)() is Some(message) {
    if cancelled() {
      raise Invalid("event detection cancelled")
    }
    for signal in detector.feed(message) {
      if signal is EventClosed(event) {
        add(event)
      }
    }
  }
  if detector.finish() is Some(event) {
    add(event)
  }
  events
}

///|
pub fn event_span_json(event : EventSpan) -> Json {
  {
    "start": event.start.to_string(),
    "confirmed": event.confirmed.to_string(),
    "end": event.end.to_string(),
    "closed": event.closed,
  }
}
