///|
struct LiveQualityItem {
  rule : Int
  channel : Int
  sequence : UInt
  time : UInt64
  matched : Bool
}

///|
pub struct LiveQuality {
  states : Array[QualityState]
  settings : Json
  start : UInt64
  lateness : UInt64
  pending_limit : Int
  pending : Array[LiveQualityItem]
  mut maximum : UInt64?
  mut watermark : UInt64?
  mut late : UInt64
  mut inputs : UInt64
  mut finished : Bool
}

///|
pub fn live_quality(
  rules : Array[QualityRule],
  start : UInt64,
  lateness? : UInt64 = 0UL,
  pending_limit? : Int = 4096,
  snapshot? : Bytes,
) -> LiveQuality raise McapError {
  if rules.is_empty() ||
    rules.length() > 128 ||
    pending_limit < 1 ||
    pending_limit > 16384 {
    raise Invalid("invalid live quality rules/buffer")
  }
  let names : Map[String, Bool] = {}
  let states = rules.map(fn(rule) {
    if rule.topic == "" ||
      rule.minimum_messages < 0 ||
      names.contains(rule.topic) {
      raise Invalid("invalid/duplicate live quality topic")
    }
    names[rule.topic] = true
    QualityState::{
      rule,
      predicate: rule.condition.map(fn(c) { json_filter(c) }),
      count: 0UL,
      previous: None,
      sequences: {},
      violations: 0UL,
      details: [],
    }
  })
  let settings : Json = {
    "start": start.to_string(),
    "lateness": lateness.to_string(),
    "pending_limit": pending_limit,
    "rules": rules.map(fn(r) -> Json {
      {
        "topic": r.topic,
        "minimum": r.minimum_messages,
        "gap": r.max_gap
        .map(fn(t) { Json::string(t.to_string()) })
        .unwrap_or(Json::null()),
        "sequence": r.check_sequence,
        "condition": r.condition.map(Json::string).unwrap_or(Json::null()),
        "warning": r.warning,
      }
    }),
  }
  let quality : LiveQuality = {
    states,
    settings,
    start,
    lateness,
    pending_limit,
    pending: [],
    maximum: None,
    watermark: None,
    late: 0UL,
    inputs: 0UL,
    finished: false,
  }
  fn optional_time(value : Json) -> UInt64? raise McapError {
    match value {
      Null => None
      _ => Some(parse_decimal(field_text(value)))
    }
  }
  if snapshot is Some(bytes) {
    let value = field_object(live_value(bytes))
    if field_get(value, "kind") != Json::string("quality") ||
      field_get(value, "settings") != settings {
      raise Invalid("live quality snapshot rules mismatch")
    }
    quality.maximum = optional_time(field_get(value, "maximum"))
    quality.watermark = optional_time(field_get(value, "watermark"))
    quality.late = parse_decimal(field_text(field_get(value, "late")))
    quality.inputs = parse_decimal(field_text(field_get(value, "inputs")))
    quality.finished = field_bool(field_get(value, "finished"))
    let saved = field_array(field_get(value, "states"))
    if saved.length() != states.length() {
      raise Invalid("live quality snapshot state count mismatch")
    }
    for i = 0; i < states.length(); i = i + 1 {
      let row = field_object(saved[i])
      let state = states[i]
      state.count = parse_decimal(field_text(field_get(row, "count")))
      state.previous = optional_time(field_get(row, "previous"))
      state.violations = parse_decimal(field_text(field_get(row, "violations")))
      let details = field_array(field_get(row, "details"))
      if details.length() > 64 || state.count > quality.inputs {
        raise Invalid("invalid live quality counters/details")
      }
      state.details.append(details)
      for entry in field_array(field_get(row, "sequences")) {
        let pair = field_array(entry)
        if pair.length() != 2 {
          raise Invalid("invalid sequence state")
        }
        let channel = parse_decimal(pair[0].stringify())
        let sequence = parse_decimal(pair[1].stringify())
        if channel > 65535UL ||
          sequence > 4294967295UL ||
          state.sequences.contains(channel.to_int()) {
          raise Invalid("invalid sequence value")
        }
        state.sequences[channel.to_int()] = sequence.to_uint()
      }
      if state.sequences.length() > 1024 {
        raise Invalid("live quality sequence channel budget exceeded")
      }
    }
    for entry in field_array(field_get(value, "pending")) {
      let row = field_object(entry)
      let rule = parse_decimal(field_get(row, "rule").stringify())
      let channel = parse_decimal(field_get(row, "channel").stringify())
      let sequence = parse_decimal(field_get(row, "sequence").stringify())
      let time = parse_decimal(field_text(field_get(row, "time")))
      if rule >= states.length().to_uint64() ||
        channel > 65535UL ||
        sequence > 4294967295UL ||
        quality.pending.length() >= pending_limit ||
        time < start ||
        (quality.watermark is Some(w) && time <= w) {
        raise Invalid("invalid pending live quality state")
      }
      quality.pending.push({
        rule: rule.to_int(),
        channel: channel.to_int(),
        sequence: sequence.to_uint(),
        time,
        matched: field_bool(field_get(row, "matched")),
      })
    }
  }
  quality
}

///|
pub fn LiveQuality::snapshot(self : LiveQuality) -> Bytes raise McapError {
  fn time(value : UInt64?) -> Json {
    value.map(fn(t) { Json::string(t.to_string()) }).unwrap_or(Json::null())
  }
  live_envelope({
    "kind": "quality",
    "settings": self.settings,
    "maximum": time(self.maximum),
    "watermark": time(self.watermark),
    "late": self.late.to_string(),
    "inputs": self.inputs.to_string(),
    "finished": self.finished,
    "states": self.states.map(fn(s) -> Json {
      {
        "count": s.count.to_string(),
        "previous": time(s.previous),
        "violations": s.violations.to_string(),
        "details": s.details,
        "sequences": s.sequences
        .to_array()
        .map(fn(pair) -> Json {
          [Json::number(pair.0.to_double()), Json::number(pair.1.to_double())]
        }),
      }
    }),
    "pending": self.pending.map(fn(p) -> Json {
      {
        "rule": p.rule,
        "channel": p.channel,
        "sequence": p.sequence,
        "time": p.time.to_string(),
        "matched": p.matched,
      }
    }),
  })
}

///|
fn LiveQuality::flush(self : LiveQuality, watermark : UInt64) -> Unit {
  // Stable sort preserves arrival order for equal timestamps.
  let ordered = self.pending.map(fn(item) { item })
  let indices = Array::makei(ordered.length(), fn(i) { i })
  indices.sort_by(fn(a, b) {
    let c = ordered[a].time.compare(ordered[b].time)
    if c == 0 {
      a.compare(b)
    } else {
      c
    }
  })
  self.pending.clear()
  for i in indices {
    let item = ordered[i]
    if item.time > watermark {
      self.pending.push(item)
      continue
    }
    let state = self.states[item.rule]
    let previous = state.previous.unwrap_or(self.start)
    if state.rule.max_gap is Some(gap) && item.time - previous > gap {
      state.issue("gap", previous, item.time, 64)
    }
    if state.rule.check_sequence {
      if state.sequences.get(item.channel) is Some(previous) &&
        item.sequence != previous + 1U {
        state.issue("sequence_discontinuity", item.time, item.time, 64)
      }
      state.sequences[item.channel] = item.sequence
    }
    if !item.matched {
      state.issue("condition", item.time, item.time, 64)
    }
    state.previous = Some(item.time)
    state.count += 1UL
  }
  self.watermark = Some(watermark)
}

///|
/// False means an explicitly counted late observation was excluded. Watermarks
/// are closed: a later arrival at or below the watermark cannot retract results.
pub fn LiveQuality::feed(
  self : LiveQuality,
  topic : String,
  message : Message,
) -> Bool raise McapError {
  if self.finished {
    raise Invalid("live quality is finished")
  }
  let mut index = None
  for i = 0; i < self.states.length(); i = i + 1 {
    if self.states[i].rule.topic == topic {
      index = Some(i)
      break
    }
  }
  guard index is Some(index) else { return true }
  if message.channel_id < 0 || message.channel_id > 65535 {
    raise Invalid("invalid live quality channel")
  }
  if message.log_time < self.start {
    raise Invalid("live quality observation precedes start")
  }
  if self.inputs == 18446744073709551615UL ||
    self.late == 18446744073709551615UL {
    raise Invalid("live quality counters exhausted")
  }
  if self.watermark is Some(w) && message.log_time <= w {
    self.late += 1UL
    self.inputs += 1UL
    return false
  }
  let matched = match self.states[index].predicate {
    None => true
    Some(predicate) => predicate.matches(message.data)
  }
  if self.states[index].rule.check_sequence {
    let channels : Map[Int, Bool] = {}
    for channel in self.states[index].sequences.keys() {
      channels[channel] = true
    }
    for item in self.pending {
      if item.rule == index {
        channels[item.channel] = true
      }
    }
    channels[message.channel_id] = true
    if channels.length() > 1024 {
      raise Invalid("live quality sequence channel budget exceeded")
    }
  }
  if self.pending.length() >= self.pending_limit {
    raise Invalid("live quality reorder buffer exceeded")
  }
  self.pending.push({
    rule: index,
    channel: message.channel_id,
    sequence: message.sequence,
    time: message.log_time,
    matched,
  })
  self.inputs += 1UL
  let old = self.maximum.unwrap_or(message.log_time)
  let maximum = if old > message.log_time { old } else { message.log_time }
  self.maximum = Some(maximum)
  if maximum >= self.lateness &&
    maximum - self.lateness >= self.start &&
    (
      self.watermark is None ||
      (self.watermark is Some(w) && maximum - self.lateness > w)
    ) {
    self.flush(maximum - self.lateness)
  }
  true
}

///|
pub fn LiveQuality::advance(
  self : LiveQuality,
  watermark : UInt64,
) -> Unit raise McapError {
  if self.finished ||
    watermark < self.start ||
    (self.watermark is Some(w) && watermark < w) {
    raise Invalid("invalid/decreasing live watermark")
  }
  self.flush(watermark)
}

///|
pub fn LiveQuality::finish(
  self : LiveQuality,
  end : UInt64,
) -> Unit raise McapError {
  if self.finished ||
    end < self.start ||
    (self.maximum is Some(time) && time > end) ||
    (self.watermark is Some(time) && time > end) {
    raise Invalid("invalid live quality end")
  }
  self.flush(end)
  for state in self.states {
    if state.count < state.rule.minimum_messages.to_uint64() {
      state.issue("minimum_messages", self.start, end, 64)
    }
    if state.rule.max_gap is Some(gap) &&
      end - state.previous.unwrap_or(self.start) > gap {
      state.issue("gap", state.previous.unwrap_or(self.start), end, 64)
    }
  }
  self.finished = true
}

///|
pub fn LiveQuality::report(self : LiveQuality) -> Json {
  {
    "finished": self.finished,
    "watermark": self.watermark
    .map(fn(t) { Json::string(t.to_string()) })
    .unwrap_or(Json::null()),
    "late_excluded": self.late.to_string(),
    "inputs": self.inputs.to_string(),
    "pending": self.pending.length(),
    "rules": self.states.map(fn(s) -> Json {
      {
        "topic": s.rule.topic,
        "messages": s.count.to_string(),
        "violations": s.violations.to_string(),
        "details": s.details,
        "status": if s.violations > 0UL {
          if s.rule.warning {
            "warning"
          } else {
            "failed"
          }
        } else if self.finished {
          "passed"
        } else {
          "monitoring"
        },
      }
    }),
  }
}
