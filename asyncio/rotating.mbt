///|
pub(all) struct SegmentInfo {
  ordinal : Int32
  messages : UInt64
  bytes : UInt64
  start_time : UInt64
  end_time : UInt64
}

///|
pub struct RotatingWriter {
  template : @mcap.Log
  create : async (Int32) -> AsyncSink
  options : @mcap.StreamOptions
  queue_bytes : Int32
  message_limit : Int32
  byte_target : UInt64
  time_span : UInt64
  segment_limit : Int32
  reports : Array[SegmentInfo]
  mut current : AsyncWriter?
  mut ordinal : Int32
  mut start_time : UInt64
  mut end_time : UInt64
  mut state : AsyncWriterState
  mut busy : Bool
}

///|
pub fn rotating_writer(
  template : @mcap.Log,
  create : async (Int32) -> AsyncSink,
  options~ : @mcap.StreamOptions = @mcap.stream_options(),
  queue_bytes~ : Int32 = 32 * 1024 * 1024,
  segment_messages~ : Int32 = 10000,
  segment_bytes_target~ : UInt64 = 0UL,
  segment_span~ : UInt64 = 0UL,
  segment_limit~ : Int32 = 10000,
) -> RotatingWriter raise @mcap.McapError {
  @mcap.validate_log(template)
  if !template.messages.is_empty() ||
    !template.attachments.is_empty() ||
    !template.metadata.is_empty() ||
    segment_messages <= 0 ||
    segment_limit <= 0 ||
    queue_bytes < 128 ||
    options.chunk_bytes < 31 ||
    options.chunk_bytes > (queue_bytes - 128) / 2 ||
    options.chunk_messages <= 0 ||
    options.summary_bytes < 0 ||
    options.chunk_bytes > @mcap.default_limits().max_record_bytes - 128 {
    raise @mcap.io_error(
      "rotating writer needs declarations-only template and positive budgets",
    )
  }
  {
    template: @mcap.copy_log(template),
    create,
    options,
    queue_bytes,
    message_limit: segment_messages,
    byte_target: segment_bytes_target,
    time_span: segment_span,
    segment_limit,
    reports: [],
    current: None,
    ordinal: 0,
    start_time: 0UL,
    end_time: 0UL,
    state: AsyncOpen,
    busy: false,
  }
}

///|
async fn RotatingWriter::start_segment(self : RotatingWriter) -> Unit {
  if self.ordinal >= self.segment_limit {
    raise @mcap.io_error("segment count budget exceeded")
  }
  let sink = (self.create)(self.ordinal)
  if self.state != AsyncOpen {
    sink.cancel()
    raise @mcap.io_error("rotation cancelled while creating output")
  }
  let writer = new_writer(
    sink,
    profile=self.template.header.profile,
    options=self.options,
    queue_bytes=self.queue_bytes,
  )
  self.current = Some(writer)
  if self.state != AsyncOpen {
    writer.cancel()
    raise @mcap.io_error("rotation cancelled while opening writer")
  }
  for schema in self.template.schemas {
    writer.add_schema(schema)
  }
  for channel in self.template.channels {
    writer.add_channel(channel)
  }
  self.ordinal += 1
}

///|
async fn RotatingWriter::close_segment(self : RotatingWriter) -> Unit {
  if self.current is Some(writer) {
    let progress = writer.finish()
    self.reports.push({
      ordinal: self.ordinal - 1,
      messages: progress.encoded.messages,
      bytes: progress.bytes_written,
      start_time: self.start_time,
      end_time: self.end_time,
    })
    self.current = None
  }
}

///|
pub async fn RotatingWriter::append(
  self : RotatingWriter,
  message : @mcap.Message,
) -> Unit {
  if self.state != AsyncOpen || self.busy {
    raise @mcap.io_error("rotating writer closed or busy")
  }
  if !self.template.channels.any(fn(c) { c.id == message.channel_id }) ||
    message.data.length() > self.options.chunk_bytes - 31 {
    raise @mcap.io_error("invalid rotating message channel or size")
  }
  self.busy = true
  defer {
    self.busy = false
  }
  try {
    if self.current is Some(writer) {
      let p = writer.progress()
      let first = if message.log_time < self.start_time {
        message.log_time
      } else {
        self.start_time
      }
      let last = if message.log_time > self.end_time {
        message.log_time
      } else {
        self.end_time
      }
      if p.encoded.messages >= self.message_limit.to_uint64() ||
        (self.byte_target != 0UL && p.bytes_written >= self.byte_target) ||
        (self.time_span != 0UL && last - first > self.time_span) {
        self.close_segment()
      }
    }
    if self.current is None {
      self.start_segment()
      self.start_time = message.log_time
      self.end_time = message.log_time
    }
    let writer = self.current.unwrap()
    writer.append(message)
    if message.log_time < self.start_time {
      self.start_time = message.log_time
    }
    if message.log_time > self.end_time {
      self.end_time = message.log_time
    }
  } catch {
    error => {
      if self.state == AsyncOpen {
        self.state = if @async.is_cancellation_error(error) {
          AsyncCancelled
        } else {
          AsyncFailed
        }
      }
      if self.current is Some(writer) {
        writer.cancel()
      }
      raise error
    }
  }
}

///|
pub async fn RotatingWriter::finish(
  self : RotatingWriter,
) -> Array[SegmentInfo] {
  if self.state != AsyncOpen || self.busy {
    raise @mcap.io_error("rotating writer closed or busy")
  }
  self.busy = true
  defer {
    self.busy = false
  }
  try {
    self.close_segment()
    self.state = AsyncFinished
  } catch {
    error => {
      if self.state == AsyncOpen {
        self.state = AsyncFailed
      }
      raise error
    }
  }
  self.reports.copy()
}

///|
pub fn RotatingWriter::cancel(self : RotatingWriter) -> Unit {
  if self.state == AsyncOpen {
    self.state = AsyncCancelled
    if self.current is Some(writer) {
      writer.cancel()
    }
  }
}

///|
pub fn RotatingWriter::segments(self : RotatingWriter) -> Array[SegmentInfo] {
  self.reports.copy()
}
