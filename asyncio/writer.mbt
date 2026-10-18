///|
pub struct AsyncSink {
  write_all : async (Bytes) -> Unit
  abort : () -> Unit
  mut cancelled : Bool
  mut bytes_written : UInt64
  mut claimed : Bool
}

///|
pub fn new_sink(
  write_all : async (Bytes) -> Unit,
  abort? : () -> Unit = fn() { () },
) -> AsyncSink {
  { write_all, abort, cancelled: false, bytes_written: 0UL, claimed: false, }
}

///|
pub fn AsyncSink::cancel(self : AsyncSink) -> Unit {
  if !self.cancelled {
    self.cancelled = true
    (self.abort)()
  }
}

///|
pub async fn AsyncSink::write(self : AsyncSink, bytes : Bytes) -> Unit {
  if self.cancelled {
    raise @mcap.io_error("async sink cancelled")
  }
  (self.write_all)(bytes)
  self.bytes_written += bytes.length().to_uint64()
  if self.cancelled {
    raise @mcap.io_error("async sink cancelled")
  }
}

///|
pub(all) enum AsyncWriterState {
  AsyncOpen
  AsyncFinished
  AsyncFailed
  AsyncCancelled
} derive(Eq)

///|
pub(all) struct AsyncWriteProgress {
  state : AsyncWriterState
  bytes_written : UInt64
  queued_bytes : Int
  peak_queued_bytes : Int
  encoded : @mcap.WriteProgress
}

///|
struct OutputQueue {
  chunks : Array[Bytes]
  budget : Int
  mut bytes : Int
  mut peak : Int
}

///|
pub struct AsyncWriter {
  sink : AsyncSink
  writer : @mcap.StreamWriter
  queue : OutputQueue
  mut state : AsyncWriterState
  mut busy : Bool
}

///|
pub async fn new_writer(
  sink : AsyncSink,
  profile? : String = "",
  options? : @mcap.StreamOptions = @mcap.stream_options(),
  limits? : @mcap.Limits = @mcap.default_limits(),
  queue_bytes? : Int = 32 * 1024 * 1024,
) -> AsyncWriter {
  if sink.claimed || sink.cancelled || sink.bytes_written != 0UL {
    raise @mcap.io_error("async sink already owned or closed")
  }
  if queue_bytes < 128 || options.chunk_bytes > (queue_bytes - 128) / 2 {
    raise @mcap.io_error(
      "async output queue cannot hold a complete chunk and indexes",
    )
  }
  let queue : OutputQueue = {
    chunks: [],
    budget: queue_bytes,
    bytes: 0,
    peak: 0,
  }
  let writer = @mcap.new_stream_writer(
    @mcap.new_write_sink(fn(bytes) {
      if bytes.length() > queue.budget - queue.bytes {
        raise @mcap.io_error("async output queue budget exceeded")
      }
      queue.chunks.push(bytes)
      queue.bytes += bytes.length()
      queue.peak = queue.peak.max(queue.bytes)
    }),
    profile~,
    options~,
    limits~,
  )
  sink.claimed = true
  let result : AsyncWriter = {
    sink,
    writer,
    queue,
    state: AsyncOpen,
    busy: false,
  }
  result.drain()
  result
}

///|
fn AsyncWriter::fail(self : AsyncWriter) -> Unit {
  if self.state == AsyncOpen {
    self.state = AsyncFailed
  }
  self.writer.cancel()
  self.queue.chunks.clear()
  self.queue.bytes = 0
}

///|
pub fn AsyncWriter::cancel(self : AsyncWriter) -> Unit {
  if self.state == AsyncOpen {
    self.state = AsyncCancelled
    self.writer.cancel()
    self.queue.chunks.clear()
    self.queue.bytes = 0
    self.sink.cancel()
  }
}

///|
async fn AsyncWriter::drain(self : AsyncWriter) -> Unit {
  try {
    while !self.queue.chunks.is_empty() {
      if self.state != AsyncOpen {
        raise @mcap.io_error("async writer closed during output")
      }
      let bytes = self.queue.chunks[0]
      self.sink.write(bytes)
      if self.state != AsyncOpen {
        raise @mcap.io_error("async writer closed during output")
      }
      ignore(self.queue.chunks.remove(0))
      self.queue.bytes -= bytes.length()
    }
  } catch {
    error => {
      if @async.is_cancellation_error(error) {
        self.cancel()
      } else {
        self.fail()
      }
      raise error
    }
  }
}

///|
async fn AsyncWriter::operation(
  self : AsyncWriter,
  action : (@mcap.StreamWriter) -> Unit raise @mcap.McapError,
) -> Unit {
  if self.state != AsyncOpen || self.busy {
    raise @mcap.io_error("async writer closed or operation already in progress")
  }
  self.busy = true
  defer {
    self.busy = false
  }
  action(self.writer) catch {
    error => {
      if self.writer.progress().state != @mcap.WriterOpen {
        self.fail()
      }
      raise error
    }
  }
  self.drain()
}

///|
pub async fn AsyncWriter::add_schema(
  self : AsyncWriter,
  schema : @mcap.Schema,
) -> Unit {
  self.operation(fn(writer) { writer.add_schema(schema) })
}

///|
pub async fn AsyncWriter::add_channel(
  self : AsyncWriter,
  channel : @mcap.Channel,
) -> Unit {
  self.operation(fn(writer) { writer.add_channel(channel) })
}

///|
pub async fn AsyncWriter::append(
  self : AsyncWriter,
  message : @mcap.Message,
) -> Unit {
  self.operation(fn(writer) { writer.append(message) })
}

///|
pub async fn AsyncWriter::add_attachment(
  self : AsyncWriter,
  attachment : @mcap.Attachment,
) -> Unit {
  self.operation(fn(writer) { writer.add_attachment(attachment) })
}

///|
pub async fn AsyncWriter::add_metadata(
  self : AsyncWriter,
  metadata : @mcap.Metadata,
) -> Unit {
  self.operation(fn(writer) { writer.add_metadata(metadata) })
}

///|
pub async fn AsyncWriter::flush(self : AsyncWriter) -> Unit {
  self.operation(fn(writer) { writer.flush() })
}

///|
pub async fn AsyncWriter::finish(self : AsyncWriter) -> AsyncWriteProgress {
  self.operation(fn(writer) { ignore(writer.finish()) })
  self.state = AsyncFinished
  self.progress()
}

///|
pub fn AsyncWriter::progress(self : AsyncWriter) -> AsyncWriteProgress {
  {
    state: self.state,
    bytes_written: self.sink.bytes_written,
    queued_bytes: self.queue.bytes,
    peak_queued_bytes: self.queue.peak,
    encoded: self.writer.progress(),
  }
}
