///|
pub(all) enum SyncClock {
  SyncLogTime
  SyncPublishTime
  SyncJsonNanoseconds(String)
  SyncJsonStamp(String, String)
}

///|
pub fn message_timestamp(
  message : Message,
  clock : SyncClock,
  max_bytes~ : Int32 = 1024 * 1024,
) -> UInt64 raise McapError {
  match clock {
    SyncLogTime => message.log_time
    SyncPublishTime => message.publish_time
    SyncJsonNanoseconds(path) => {
      let value = parse_json_document(message.data, max_bytes, 64)
      guard pointer_value(value, json_pointer(path)) is Some(value) else {
        raise Invalid("message clock path absent")
      }
      payload_unsigned(value, 64)
    }
    SyncJsonStamp(seconds, nanos) => {
      let value = parse_json_document(message.data, max_bytes, 64)
      guard pointer_value(value, json_pointer(seconds)) is Some(sec) &&
        pointer_value(value, json_pointer(nanos)) is Some(ns) else {
        raise Invalid("message timestamp fields absent")
      }
      let seconds = payload_unsigned(sec, 64)
      let nanos = payload_unsigned(ns, 32)
      if nanos >= 1000000000UL ||
        seconds > (18446744073709551615UL - nanos) / 1000000000UL {
        raise Invalid("message timestamp outside UInt64 nanoseconds")
      }
      seconds * 1000000000UL + nanos
    }
  }
}

///|
pub(all) enum SyncMode {
  SyncNearest
  SyncPrevious
  SyncNext
  SyncLinear
} derive(Eq)

///|
pub(all) enum SyncReuse {
  SyncAllowReuse
  SyncConsume
} derive(Eq)

///|
pub(all) struct SyncOptions {
  mode : SyncMode
  reuse : SyncReuse
  tolerance : UInt64
  keep_missing : Bool
  interpolation_paths : Array[String]
  buffer_bytes : Int32
  json_bytes : Int32
  output_rows : UInt64
  input_messages : UInt64
}

///|
pub fn sync_options() -> SyncOptions {
  {
    mode: SyncNearest,
    reuse: SyncAllowReuse,
    tolerance: 0UL,
    keep_missing: true,
    interpolation_paths: [],
    buffer_bytes: 8 * 1024 * 1024,
    json_bytes: 1024 * 1024,
    output_rows: 1000000UL,
    input_messages: 10000000UL,
  }
}

///|
pub struct SyncInput {
  name : String
  stream : MessageStream
  clock : (Message) -> UInt64 raise McapError
  interpolation_paths : Array[String]?
  synthetic : Bool
}

///|
pub fn sync_input(
  name : String,
  stream : MessageStream,
  clock~ : (Message) -> UInt64 raise McapError = fn(m) { m.log_time },
  interpolation_paths? : Array[String],
) -> SyncInput {
  { name, stream, clock, interpolation_paths, synthetic: false, }
}

///|
struct TimedSyncMessage {
  message : Message
  time : UInt64
}

///|
struct SyncLane {
  input : SyncInput
  paths : Array[Array[String]]
  mut before : TimedSyncMessage?
  mut after : TimedSyncMessage?
  mut last : UInt64?
  mut done : Bool
}

///|
pub(all) struct SyncCell {
  name : String
  synthetic : Bool
  message : Message?
  before : Message?
  after : Message?
  source_time : UInt64?
  before_time : UInt64?
  after_time : UInt64?
  distance : UInt64?
  interpolated : Json?
}

///|
pub(all) struct SyncRow {
  time : UInt64
  cells : Array[SyncCell]
}

///|
pub struct SyncStream {
  lanes : Array[SyncLane]
  options : SyncOptions
  cancelled : () -> Bool
  mut state : FileCursorState
  mut read : UInt64
  mut examined : UInt64
  mut emitted : UInt64
  mut missing : UInt64
  mut peak_bytes : Int32
  mut anchor : TimedSyncMessage?
}

///|
/// Inputs must be monotonic in their selected clocks. The first input supplies
/// anchor rows. Consume is greedy and prevents reuse, not a global assignment.
pub fn synchronize(
  inputs : Array[SyncInput],
  options~ : SyncOptions = sync_options(),
  cancelled~ : () -> Bool = fn() { false },
) -> SyncStream raise McapError {
  if inputs.length() < 2 ||
    inputs.length() > 33 ||
    options.buffer_bytes < 31 ||
    options.json_bytes < 1 ||
    options.output_rows == 0UL ||
    options.input_messages == 0UL ||
    (options.mode == SyncLinear && options.reuse == SyncConsume) {
    raise Invalid("invalid synchronization configuration")
  }
  let names : Map[String, Bool] = Map([])
  for input in inputs {
    if input.name == "" ||
      input.name.length() > 128 ||
      names.contains(input.name) ||
      (input.name == "$grid" && !input.synthetic) {
      raise Invalid("duplicate, reserved or invalid sync input name")
    }
    names[input.name] = true
  }
  let lanes = inputs.mapi(fn(i, input) {
    let paths = input.interpolation_paths.unwrap_or(options.interpolation_paths)
    if i > 0 && options.mode == SyncLinear && paths.is_empty() {
      raise Invalid(
        "linear synchronization requires paths for each sampled input",
      )
    }
    ignore(
      payload_edit_paths(paths.map(fn(path) { { path, replacement: None, } })),
    )
    let paths = paths.map(json_pointer)
    if paths.any(fn(p) { p.is_empty() }) {
      raise Invalid("interpolation paths must address object members")
    }
    { input, paths, before: None, after: None, last: None, done: false, }
  })
  {
    lanes,
    options: {
      ..options,
      interpolation_paths: options.interpolation_paths.copy(),
    },
    cancelled,
    state: FileActive,
    read: 0UL,
    examined: 0UL,
    emitted: 0UL,
    missing: 0UL,
    peak_bytes: 0,
    anchor: None,
  }
}

///|
fn SyncStream::close(self : SyncStream, state : FileCursorState) -> Unit {
  if self.state == FileActive {
    self.state = state
    self.anchor = None
    for lane in self.lanes {
      lane.before = None
      lane.after = None
      lane.input.stream.cancel()
    }
  }
}

///|
pub fn SyncStream::cancel(self : SyncStream) -> Unit {
  self.close(FileCancelled)
}

///|
fn SyncStream::check_buffer(self : SyncStream) -> Unit raise McapError {
  let mut bytes = match self.anchor {
    Some(m) => m.message.data.length().to_uint64() + 31UL
    None => 0UL
  }
  for lane in self.lanes {
    for message in [lane.before, lane.after] {
      if message is Some(m) {
        bytes += m.message.data.length().to_uint64() + 31UL
      }
    }
  }
  if bytes > self.options.buffer_bytes.to_uint64() {
    raise Invalid("synchronization retained payload budget exceeded")
  }
  self.peak_bytes = self.peak_bytes.max(bytes.to_int())
}

///|
fn SyncStream::pull(
  self : SyncStream,
  lane : SyncLane,
) -> TimedSyncMessage? raise McapError {
  if (self.cancelled)() {
    raise Invalid("synchronization cancelled")
  }
  if lane.done {
    return None
  }
  match lane.input.stream.next() {
    None => {
      lane.done = true
      None
    }
    Some(message) => {
      if self.read >= self.options.input_messages {
        raise Invalid("synchronization input budget exceeded")
      }
      self.read += 1UL
      let time = (lane.input.clock)(message)
      if lane.last is Some(previous) && time < previous {
        raise Invalid(
          "synchronization input not monotonic in selected clock: " +
          lane.input.name,
        )
      }
      lane.last = Some(time)
      Some({ message, time, })
    }
  }
}

///|
fn sync_distance(a : UInt64, b : UInt64) -> UInt64 {
  if a <= b {
    b - a
  } else {
    a - b
  }
}

///|
fn sync_cell(
  name : String,
  sample : TimedSyncMessage?,
  time : UInt64,
  synthetic~ : Bool = false,
) -> SyncCell {
  {
    name,
    synthetic,
    message: sample.map(fn(s) { s.message }),
    before: None,
    after: None,
    source_time: sample.map(fn(s) { s.time }),
    before_time: None,
    after_time: None,
    distance: sample.map(fn(s) { sync_distance(s.time, time) }),
    interpolated: None,
  }
}

///|
fn SyncStream::interpolate(
  self : SyncStream,
  lane : SyncLane,
  time : UInt64,
) -> SyncCell raise McapError {
  if lane.before is Some(b) && b.time == time {
    return sync_cell(lane.input.name, Some(b), time)
  }
  guard lane.before is Some(b) &&
    lane.after is Some(a) &&
    a.time > b.time &&
    time - b.time <= self.options.tolerance &&
    a.time - time <= self.options.tolerance else {
    return sync_cell(lane.input.name, None, time)
  }
  let left = parse_json_document(b.message.data, self.options.json_bytes, 64)
  let right = parse_json_document(a.message.data, self.options.json_bytes, 64)
  let numerator = time - b.time
  let denominator = a.time - b.time
  let fraction = numerator.to_double() / denominator.to_double()
  let mut value = Json::object(Map([]))
  for path in lane.paths {
    guard pointer_value(left, path) is Some(Number(x, ..) as xv) &&
      pointer_value(right, path) is Some(Number(y, ..) as yv) &&
      !x.is_nan() &&
      !x.is_inf() &&
      !y.is_nan() &&
      !y.is_inf() else {
      raise Invalid("interpolation requires finite numeric fields")
    }
    if compare_decimal(xv.stringify(), x.to_string()) != 0 ||
      compare_decimal(yv.stringify(), y.to_string()) != 0 {
      raise Invalid("interpolation would round input fields")
    }
    let n = (1.0 - fraction) * x + fraction * y
    if n.is_nan() || n.is_inf() {
      raise Invalid("interpolation overflow")
    }
    value = normalized_insert(value, path, 0, Json::number(n))
  }
  {
    name: lane.input.name,
    synthetic: false,
    message: None,
    before: Some(b.message),
    after: Some(a.message),
    source_time: None,
    before_time: Some(b.time),
    after_time: Some(a.time),
    distance: None,
    interpolated: Some({
      "value": value,
      "numerator": numerator.to_string(),
      "denominator": denominator.to_string(),
      "method": "linear IEEE Double; exact UInt64 bracket times",
    }),
  }
}

///|
pub fn SyncStream::next(self : SyncStream) -> SyncRow? raise McapError {
  if self.state != FileActive {
    return None
  }
  try {
    if self.emitted >= self.options.output_rows {
      self.close(FileExhausted)
      return None
    }
    while true {
      guard self.pull(self.lanes[0]) is Some(anchor) else {
        self.close(FileExhausted)
        return None
      }
      self.anchor = Some(anchor)
      self.check_buffer()
      self.examined += 1UL
      let cells = [
        sync_cell(
          self.lanes[0].input.name,
          Some(anchor),
          anchor.time,
          synthetic=self.lanes[0].input.synthetic,
        ),
      ]
      let chosen : Array[Int32] = [0]
      let mut complete = true
      for i = 1; i < self.lanes.length(); i = i + 1 {
        let lane = self.lanes[i]
        if lane.after is None && !lane.done {
          lane.after = self.pull(lane)
          self.check_buffer()
        }
        while lane.after is Some(a) && a.time <= anchor.time {
          lane.before = Some(a)
          lane.after = None
          self.check_buffer()
          lane.after = self.pull(lane)
          self.check_buffer()
        }
        let selected = if self.options.mode == SyncLinear {
          None
        } else {
          match (self.options.mode, lane.before, lane.after) {
            (SyncPrevious, b, _) => b
            (SyncNext, Some(b), _) if b.time == anchor.time => Some(b)
            (SyncNext, _, a) => a
            (_, None, a) => a
            (_, b, None) => b
            (_, Some(b), Some(a)) =>
              if anchor.time - b.time <= a.time - anchor.time {
                Some(b)
              } else {
                Some(a)
              }
          }
        }
        let accepted = selected.filter(fn(s) {
          sync_distance(s.time, anchor.time) <= self.options.tolerance
        })
        let cell = if self.options.mode == SyncLinear {
          self.interpolate(lane, anchor.time)
        } else {
          sync_cell(lane.input.name, accepted, anchor.time)
        }
        if cell.message is None && cell.interpolated is None {
          complete = false
        }
        chosen.push(
          match accepted {
            None => 0
            Some(s) =>
              if lane.before is Some(b) && b.time == s.time {
                1
              } else {
                2
              }
          },
        )
        cells.push(cell)
      }
      self.anchor = None
      if !complete {
        self.missing += 1UL
      }
      if !complete && !self.options.keep_missing {
        continue
      }
      if self.options.reuse == SyncConsume {
        for i = 1; i < self.lanes.length(); i = i + 1 {
          if chosen[i] == 1 {
            self.lanes[i].before = None
          } else if chosen[i] == 2 {
            self.lanes[i].before = None
            self.lanes[i].after = None
          }
        }
      }
      self.emitted += 1UL
      return Some({ time: anchor.time, cells, })
    }
    None
  } catch {
    error => {
      self.close(FileFailed)
      raise error
    }
  }
}

///|
pub fn SyncStream::info(self : SyncStream) -> Json {
  {
    "inputs": self.lanes.length(),
    "messages_read": self.read.to_string(),
    "anchors_examined": self.examined.to_string(),
    "rows": self.emitted.to_string(),
    "incomplete_rows": self.missing.to_string(),
    "peak_retained_bytes": self.peak_bytes,
    "buffer_bytes": self.options.buffer_bytes,
    "finished": self.state == FileExhausted,
    "limit_reached": self.emitted >= self.options.output_rows,
    "anchor_exhausted": self.lanes[0].done,
  }
}

///|
/// Uniform anchors are synthetic observations, identified by the reserved $grid
/// input name. Endpoints and grid arithmetic stay in UInt64 nanoseconds.
pub fn resample(
  inputs : Array[SyncInput],
  start : UInt64,
  end : UInt64,
  period : UInt64,
  options~ : SyncOptions = sync_options(),
  cancelled~ : () -> Bool = fn() { false },
) -> SyncStream raise McapError {
  if inputs.is_empty() ||
    inputs.length() > 32 ||
    start > end ||
    period == 0UL ||
    inputs.any(fn(i) { i.name == "$grid" }) {
    raise Invalid("invalid resampling grid")
  }
  let next = [Some(start)]
  let ordinal = [0U]
  let grid = message_stream(
    fn() {
      guard next[0] is Some(time) else { return None }
      next[0] = if end - time < period { None } else { Some(time + period) }
      let message : Message = {
        channel_id: 0,
        sequence: ordinal[0],
        log_time: time,
        publish_time: time,
        data: b"{}",
      }
      ordinal[0] += 1U
      Some(message)
    },
    fn() { next[0] = None },
  )
  synchronize(
    [{ ..sync_input("$grid", grid), synthetic: true, }] + inputs,
    options~,
    cancelled~,
  )
}

///|
fn sync_envelope(message : Message) -> Json {
  {
    "channel_id": message.channel_id,
    "sequence": message.sequence.to_string(),
    "log_time": message.log_time.to_string(),
    "publish_time": message.publish_time.to_string(),
  }
}

///|
/// The default payload representation is Base64. A JSON-decoded stream can
/// supply a strict JSON callback. Interpolation brackets retain source envelopes.
pub fn sync_row_json(
  row : SyncRow,
  payload~ : (Message) -> Json raise McapError = fn(m) {
    Json::string(@base64.encode(m.data[:]))
  },
) -> Json raise McapError {
  {
    "time": row.time.to_string(),
    "cells": row.cells.map(fn(cell) {
      let sample = match cell.message {
        None => Json::null()
        Some(message) =>
          (
            { "envelope": sync_envelope(message), "value": payload(message) } :
            Json)
      }
      (
        {
          "name": cell.name,
          "kind": if cell.synthetic {
            "grid"
          } else if cell.message is Some(_) {
            "sample"
          } else if cell.interpolated is Some(_) {
            "interpolated"
          } else {
            "missing"
          },
          "source_time": cell.source_time
          .map(fn(t) { Json::string(t.to_string()) })
          .unwrap_or(Json::null()),
          "distance": cell.distance
          .map(fn(t) { Json::string(t.to_string()) })
          .unwrap_or(Json::null()),
          "before_time": cell.before_time
          .map(fn(t) { Json::string(t.to_string()) })
          .unwrap_or(Json::null()),
          "after_time": cell.after_time
          .map(fn(t) { Json::string(t.to_string()) })
          .unwrap_or(Json::null()),
          "before": cell.before.map(sync_envelope).unwrap_or(Json::null()),
          "after": cell.after.map(sync_envelope).unwrap_or(Json::null()),
          "message": sample,
          "interpolation": cell.interpolated.unwrap_or(Json::null()),
        } : Json)
    }),
  }
}
