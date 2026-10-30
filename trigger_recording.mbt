///|
pub(all) struct TriggerOptions {
  pre : UInt64
  post : UInt64
  buffer_bytes : Int32
  buffer_messages : Int32
  clip_bytes : Int32
  clip_messages : Int32
  event_limit : Int32
}

///|
pub fn trigger_options() -> TriggerOptions {
  {
    pre: 5000000000UL,
    post: 10000000000UL,
    buffer_bytes: 16 * 1024 * 1024,
    buffer_messages: 10000,
    clip_bytes: 64 * 1024 * 1024,
    clip_messages: 100000,
    event_limit: 1024,
  }
}

///|
pub struct TriggerRecorder {
  template : Log
  options : TriggerOptions
  emit : (Log, Json) -> Unit raise McapError
  mut ring : Array[Message?]
  mut head : Int32
  mut buffered_bytes : Int32
  mut first : UInt64?
  mut last : UInt64?
  mut capacity_evicted_through : UInt64?
  mut evicted : UInt64
  mut active : Array[Message]?
  mut active_bytes : Int32
  mut start : UInt64
  mut deadline : UInt64
  mut pre_complete : Bool
  labels : Array[String]
  mut completed : Int32
  mut state : WriterState
  mut delivering : Bool
}

///|
/// The callback is synchronous backpressure: feed/trigger/finish do not return
/// until output is accepted. Buffers are volatile, unlike RecordingSession.
pub fn trigger_recorder(
  template : Log,
  emit : (Log, Json) -> Unit raise McapError,
  options~ : TriggerOptions = trigger_options(),
) -> TriggerRecorder raise McapError {
  validate_log(template)
  if !template.messages.is_empty() ||
    !template.attachments.is_empty() ||
    !template.metadata.is_empty() ||
    write_log(template).length() > 1024 * 1024 ||
    options.buffer_bytes < 31 ||
    options.buffer_messages < 1 ||
    options.clip_bytes < 31 ||
    options.clip_messages < 1 ||
    options.event_limit < 1 ||
    options.event_limit > 100000 {
    raise Invalid("invalid trigger declarations or budgets")
  }
  {
    template: copy_log(template),
    options,
    emit,
    ring: [],
    head: 0,
    buffered_bytes: 0,
    first: None,
    last: None,
    capacity_evicted_through: None,
    evicted: 0UL,
    active: None,
    active_bytes: 0,
    start: 0UL,
    deadline: 0UL,
    pre_complete: false,
    labels: [],
    completed: 0,
    state: WriterOpen,
    delivering: false,
  }
}

///|
fn TriggerRecorder::release(self : TriggerRecorder) -> Unit {
  self.ring.clear()
  self.head = 0
  self.buffered_bytes = 0
  self.active = None
  self.active_bytes = 0
  self.labels.clear()
}

///|
fn TriggerRecorder::discard_head(
  self : TriggerRecorder,
  capacity : Bool,
) -> Unit {
  let message = self.ring[self.head].unwrap()
  self.ring[self.head] = None // release the Bytes reference, not just an index
  self.head += 1
  self.buffered_bytes -= message.data.length() + 31
  if capacity {
    self.capacity_evicted_through = Some(message.log_time)
    self.evicted += 1UL
  }
}

///|
fn TriggerRecorder::append_active(
  self : TriggerRecorder,
  message : Message,
) -> Unit raise McapError {
  if self.active is Some(messages) {
    if messages.length() >= self.options.clip_messages ||
      message.data.length() > self.options.clip_bytes - self.active_bytes - 31 {
      raise Invalid("trigger clip budget exceeded")
    }
    messages.push(message)
    self.active_bytes += message.data.length() + 31
  }
}

///|
fn TriggerRecorder::publish(
  self : TriggerRecorder,
  complete_post : Bool,
) -> Unit raise McapError {
  guard self.active is Some(messages) else { return }
  if self.completed >= self.options.event_limit {
    raise Invalid("trigger output count exceeded")
  }
  let report : Json = {
    "ordinal": self.completed,
    "requested_start": self.start.to_string(),
    "requested_end": self.deadline.to_string(),
    "pre_complete": self.pre_complete,
    "post_complete": complete_post,
    "messages": messages.length(),
    "payload_bytes": self.active_bytes,
    "labels": self.labels.copy(),
    "capacity_evicted_messages": self.evicted.to_string(),
    "auxiliary_policy": "declarations and messages only; volatile capture",
  }
  let log = {
    ..self.template,
    messages: messages.copy(),
    metadata: [
      { name: "moonmcap.trigger", entries: [("manifest", report.stringify())], },
    ],
  }
  self.delivering = true
  (self.emit)(log, report) catch {
    error => {
      self.delivering = false
      raise error
    }
  }
  self.delivering = false
  self.completed += 1
  self.active = None
  self.active_bytes = 0
  self.labels.clear()
}

///|
pub fn TriggerRecorder::feed(
  self : TriggerRecorder,
  message : Message,
) -> Unit raise McapError {
  if self.state != WriterOpen || self.delivering {
    raise Invalid("trigger recorder is closed or delivering")
  }
  try {
    if (self.last is Some(time) && message.log_time < time) ||
      !self.template.channels.any(fn(c) { c.id == message.channel_id }) ||
      message.data.length() > self.options.buffer_bytes - 31 {
      raise Invalid(
        "trigger input is unordered, undeclared or larger than buffer",
      )
    }
    if self.active is Some(_) && message.log_time > self.deadline {
      self.publish(true)
    }
    if self.first is None {
      self.first = Some(message.log_time)
    }
    self.last = Some(message.log_time)
    self.append_active(message)
    let cutoff = time_before(message.log_time, self.options.pre)
    while self.head < self.ring.length() &&
          self.ring[self.head].unwrap().log_time < cutoff {
      self.discard_head(false)
    }
    while self.head < self.ring.length() &&
          (
            self.ring.length() - self.head >= self.options.buffer_messages ||
            message.data.length() >
            self.options.buffer_bytes - self.buffered_bytes - 31
          ) {
      self.discard_head(true)
    }
    self.ring.push(Some(message))
    self.buffered_bytes += message.data.length() + 31
    if self.head > 1024 && self.head > self.ring.length() / 2 {
      self.ring = self.ring[self.head:].to_array()
      self.head = 0
    }
  } catch {
    error => {
      self.state = WriterFailed
      self.release()
      raise error
    }
  }
}

///|
/// Trigger at the latest fed message's log time. Repeated triggers extend a
/// single window; output count and label count are independently bounded.
pub fn TriggerRecorder::trigger(
  self : TriggerRecorder,
  label : String,
) -> Unit raise McapError {
  if self.state != WriterOpen || self.delivering {
    raise Invalid("trigger recorder is closed or delivering")
  }
  try {
    guard self.last is Some(time) else {
      raise Invalid("feed a message before triggering")
    }
    if label == "" || label.length() > 256 || self.labels.length() >= 128 {
      raise Invalid("invalid trigger label or label budget exceeded")
    }
    if self.active is None {
      if self.completed >= self.options.event_limit {
        raise Invalid("trigger output count exceeded")
      }
      self.start = time_before(time, self.options.pre)
      self.pre_complete = self.first.unwrap() <= self.start &&
        !(self.capacity_evicted_through is Some(t) && t >= self.start)
      self.active = Some([])
      for i = self.head; i < self.ring.length(); i = i + 1 {
        self.append_active(self.ring[i].unwrap())
      }
    }
    self.deadline = time_after(time, self.options.post)
    self.labels.push(label)
  } catch {
    error => {
      self.state = WriterFailed
      self.release()
      raise error
    }
  }
}

///|
pub fn TriggerRecorder::finish(self : TriggerRecorder) -> Unit raise McapError {
  if self.state == WriterFinished {
    return
  }
  if self.state != WriterOpen || self.delivering {
    raise Invalid("trigger recorder is closed or delivering")
  }
  try {
    self.publish(self.last is Some(time) && time >= self.deadline)
    self.state = WriterFinished
    self.release()
  } catch {
    error => {
      self.state = WriterFailed
      self.release()
      raise error
    }
  }
}

///|
pub fn TriggerRecorder::cancel(self : TriggerRecorder) -> Unit {
  if self.state == WriterOpen && !self.delivering {
    self.state = WriterCancelled
    self.release()
  }
}

///|
pub fn TriggerRecorder::progress(self : TriggerRecorder) -> Json {
  {
    "completed": self.completed,
    "buffered_messages": self.ring.length() - self.head,
    "buffered_bytes": self.buffered_bytes,
    "active_bytes": self.active_bytes,
    "capacity_evicted_messages": self.evicted.to_string(),
    "active": self.active is Some(_),
    "state": match self.state {
      WriterOpen => "open"
      WriterFinished => "finished"
      WriterFailed => "failed"
      WriterCancelled => "cancelled"
    },
  }
}
