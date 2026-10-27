///|
pub struct WriteSink {
  write_all : (Bytes) -> Unit raise McapError
  mut position : UInt64
}

///|
/// Synchronous backpressure: callback must accept all bytes or raise.
pub fn new_write_sink(write_all : (Bytes) -> Unit raise McapError) -> WriteSink {
  { write_all, position: 0UL, }
}

///|
pub fn WriteSink::position(self : WriteSink) -> UInt64 {
  self.position
}

///|
/// Write raw bytes to an exclusively owned sink (for temporary run storage).
pub fn WriteSink::write(
  self : WriteSink,
  bytes : Bytes,
) -> Unit raise McapError {
  if bytes.length().to_uint64() > 18446744073709551615UL - self.position {
    raise Invalid("sink offset overflow")
  }
  (self.write_all)(bytes)
  self.position += bytes.length().to_uint64()
}

///|
pub(all) enum WriterState {
  WriterOpen
  WriterFinished
  WriterFailed
  WriterCancelled
} derive(Eq, @debug.Debug)

///|
pub(all) struct StreamOptions {
  compression : Compression
  chunk_bytes : Int
  chunk_messages : Int
  chunk_span : UInt64
  summary_bytes : Int
} derive(Eq, @debug.Debug)

///|
pub fn stream_options() -> StreamOptions {
  {
    compression: Uncompressed,
    chunk_bytes: 256 * 1024,
    chunk_messages: 256,
    chunk_span: 0UL,
    summary_bytes: 8 * 1024 * 1024,
  }
}

///|
pub(all) struct WriteProgress {
  state : WriterState
  bytes_written : UInt64
  messages : UInt64
  chunks : UInt
  buffered_bytes : Int
  peak_buffered_bytes : Int
  retained_summary_bytes : Int
} derive(Eq, @debug.Debug)

///|
pub struct StreamWriter {
  sink : WriteSink
  options : StreamOptions
  limits : Limits
  declarations : Log
  pending : Array[Record]
  summary : Array[(Byte, Bytes)]
  counts : Map[Int, UInt64]
  mut state : WriterState
  mut crc : UInt
  mut records : Int
  mut buffered : Int
  mut peak_buffered : Int
  mut retained : Int
  mut messages : UInt64
  mut chunks : UInt
  mut attachments : UInt
  mut metadata : UInt
  mut start_time : UInt64
  mut end_time : UInt64
  mut chunk_start : UInt64
  mut chunk_end : UInt64
}

///|
pub fn new_stream_writer(
  sink : WriteSink,
  profile? : String = "",
  options? : StreamOptions = stream_options(),
  limits? : Limits = default_limits(),
) -> StreamWriter raise McapError {
  let declarations = empty_log(profile~)
  validate_log(declarations, limits~)
  if sink.position != 0UL ||
    options.chunk_bytes < 31 ||
    options.chunk_messages <= 0 ||
    options.summary_bytes < 0 ||
    options.chunk_bytes > limits.max_decompressed_bytes ||
    options.chunk_bytes > limits.max_record_bytes - 128 {
    raise Invalid("invalid stream budgets or sink is not empty")
  }
  let writer : StreamWriter = {
    sink,
    options,
    limits,
    declarations,
    pending: [],
    summary: [],
    counts: Map([]),
    state: WriterOpen,
    crc: 0U,
    records: 0,
    buffered: 0,
    peak_buffered: 0,
    retained: 0,
    messages: 0UL,
    chunks: 0U,
    attachments: 0U,
    metadata: 0U,
    start_time: 0UL,
    end_time: 0UL,
    chunk_start: 0UL,
    chunk_end: 0UL,
  }
  writer.send(magic(), true)
  writer.emit(0x01, encode_header(declarations.header), true)
  writer
}

///|
fn StreamWriter::require_open(self : StreamWriter) -> Unit raise McapError {
  if self.state != WriterOpen {
    raise Invalid("stream writer is closed")
  }
}

///|
fn StreamWriter::send(
  self : StreamWriter,
  bytes : Bytes,
  data : Bool,
) -> Unit raise McapError {
  if bytes.length().to_uint64() > 18446744073709551615UL - self.sink.position {
    self.state = WriterFailed
    raise Invalid("output offset overflow")
  }
  (self.sink.write_all)(bytes) catch {
    Invalid(reason) => {
      self.state = WriterFailed
      raise Invalid(reason)
    }
  }
  self.sink.position += bytes.length().to_uint64()
  if data {
    self.crc = crc32_extend(self.crc, bytes)
  }
}

///|
fn StreamWriter::emit(
  self : StreamWriter,
  opcode : Byte,
  body : Bytes,
  data : Bool,
) -> Unit raise McapError {
  if body.length() > self.limits.max_record_bytes ||
    self.records >= self.limits.max_records {
    raise Invalid("stream record budget exceeded")
  }
  self.send(frame(opcode, body), data)
  self.records += 1
}

///|
fn StreamWriter::check_summary(
  self : StreamWriter,
  size : Int,
) -> Unit raise McapError {
  if size < 0 || size > self.options.summary_bytes - self.retained {
    raise Invalid("stream summary budget exceeded")
  }
}

///|
fn StreamWriter::retain(
  self : StreamWriter,
  opcode : Byte,
  body : Bytes,
) -> Unit {
  self.summary.push((opcode, body))
  self.retained += body.length() + 9
}

///|
pub fn StreamWriter::add_schema(
  self : StreamWriter,
  schema : Schema,
) -> Unit raise McapError {
  self.require_open()
  if self.declarations.schemas.contains(schema) {
    return
  }
  let candidate = copy_log(self.declarations)
  add_schema(candidate, schema)
  validate_log(candidate, limits=self.limits)
  let body = encode_schema(schema)
  self.check_summary(body.length() + 9)
  self.flush()
  self.emit(0x03, body, true)
  self.declarations.schemas.push(schema)
  self.retain(0x03, body)
}

///|
pub fn StreamWriter::add_channel(
  self : StreamWriter,
  channel : Channel,
) -> Unit raise McapError {
  self.require_open()
  if self.declarations.channels.contains(channel) {
    return
  }
  let candidate = copy_log(self.declarations)
  add_channel(candidate, channel)
  validate_log(candidate, limits=self.limits)
  let copied = { ..channel, metadata: channel.metadata.copy(), }
  let body = encode_channel(copied)
  self.check_summary(body.length() + 9)
  self.flush()
  self.emit(0x04, body, true)
  self.declarations.channels.push(copied)
  self.retain(0x04, body)
}

///|
pub fn StreamWriter::append(
  self : StreamWriter,
  message : Message,
) -> Unit raise McapError {
  self.require_open()
  if !self.declarations.channels.any(fn(c) { c.id == message.channel_id }) {
    raise Invalid("stream Message references missing Channel")
  }
  if message.data.length() > self.options.chunk_bytes - 31 ||
    self.records >= self.limits.max_records {
    raise Invalid("message exceeds stream chunk or record budget")
  }
  let body = encode_message(message)
  let size = body.length() + 9
  let start = if self.pending.is_empty() || message.log_time < self.chunk_start {
    message.log_time
  } else {
    self.chunk_start
  }
  let end = if self.pending.is_empty() || message.log_time > self.chunk_end {
    message.log_time
  } else {
    self.chunk_end
  }
  if !self.pending.is_empty() &&
    (
      size > self.options.chunk_bytes - self.buffered ||
      self.pending.length() >= self.options.chunk_messages ||
      (self.options.chunk_span != 0UL && end - start > self.options.chunk_span)
    ) {
    self.flush()
  }
  self.chunk_start = if self.pending.is_empty() ||
    message.log_time < self.chunk_start {
    message.log_time
  } else {
    self.chunk_start
  }
  self.chunk_end = if self.pending.is_empty() ||
    message.log_time > self.chunk_end {
    message.log_time
  } else {
    self.chunk_end
  }
  self.pending.push({ opcode: 0x05, offset: self.buffered, body, })
  self.buffered += size
  self.peak_buffered = self.peak_buffered.max(self.buffered)
  self.records += 1 // Nested messages also count against the record budget.
  self.counts[message.channel_id] = self.counts
    .get(message.channel_id)
    .unwrap_or(0UL) +
    1UL
  if self.messages == 0UL || message.log_time < self.start_time {
    self.start_time = message.log_time
  }
  if self.messages == 0UL || message.log_time > self.end_time {
    self.end_time = message.log_time
  }
  self.messages += 1UL
}

///|
fn StreamWriter::flush_pending(self : StreamWriter) -> Unit raise McapError {
  if self.pending.is_empty() {
    return
  }
  let body = encode_chunk(
    self.pending,
    self.chunk_start,
    self.chunk_end,
    compression=self.options.compression,
  )
  let entries : Map[Int, Array[(UInt64, UInt64)]] = Map([])
  for record in self.pending {
    let message = decode_message(record.body, limits=self.limits)
    let pair = (message.log_time, record.offset.to_uint64())
    match entries.get(message.channel_id) {
      Some(values) => values.push(pair)
      None => entries[message.channel_id] = [pair]
    }
  }
  let ids = entries.keys().to_array()
  ids.sort()
  let offset = self.sink.position
  let chunk_length = (body.length() + 9).to_uint64()
  let mut next = offset + chunk_length
  let offsets = []
  let message_bodies = []
  for id in ids {
    let body = encode_message_index({ channel_id: id, entries: entries[id], })
    offsets.push((id, next))
    message_bodies.push(body)
    next += (body.length() + 9).to_uint64()
  }
  let index = encode_chunk_index({
    start_time: self.chunk_start,
    end_time: self.chunk_end,
    chunk_start: offset,
    chunk_length,
    message_index_offsets: offsets,
    message_index_length: next - offset - chunk_length,
    compression: self.options.compression.name(),
    compressed_size: (body.length() -
    40 -
    @utf8.encode(self.options.compression.name()[:]).length()).to_uint64(),
    uncompressed_size: self.buffered.to_uint64(),
  })
  self.check_summary(index.length() + 9)
  self.emit(0x06, body, true)
  for body in message_bodies {
    self.emit(0x07, body, true)
  }
  self.retain(0x08, index)
  self.chunks += 1U
  self.pending.clear()
  self.buffered = 0
}

///|
/// Manual flush; automatic flush also occurs before byte/count/time thresholds.
pub fn StreamWriter::flush(self : StreamWriter) -> Unit raise McapError {
  self.require_open()
  self.flush_pending() catch {
    Invalid(reason) => {
      self.state = WriterFailed
      raise Invalid(reason)
    }
  }
}

///|
pub fn StreamWriter::add_attachment(
  self : StreamWriter,
  value : Attachment,
) -> Unit raise McapError {
  self.require_open()
  let body = encode_attachment(value)
  ignore(decode_attachment(body, limits=self.limits))
  self.flush()
  let index = encode_attachment_index({
    offset: self.sink.position,
    length: (body.length() + 9).to_uint64(),
    log_time: value.log_time,
    create_time: value.create_time,
    data_size: value.data.length().to_uint64(),
    name: value.name,
    media_type: value.media_type,
  })
  self.check_summary(index.length() + 9)
  self.emit(0x09, body, true)
  self.retain(0x0a, index)
  self.attachments += 1U
}

///|
pub fn StreamWriter::add_metadata(
  self : StreamWriter,
  value : Metadata,
) -> Unit raise McapError {
  self.require_open()
  let body = encode_metadata(value)
  ignore(decode_metadata(body, limits=self.limits))
  self.flush()
  let index = encode_metadata_index({
    offset: self.sink.position,
    length: (body.length() + 9).to_uint64(),
    name: value.name,
  })
  self.check_summary(index.length() + 9)
  self.emit(0x0c, body, true)
  self.retain(0x0d, index)
  self.metadata += 1U
}

///|
fn StreamWriter::finish_output(self : StreamWriter) -> Unit raise McapError {
  self.flush()
  self.emit(0x0f, self.crc.mcap_le_bytes(), false)
  let start = self.sink.position
  let out = @buffer.Buffer::Buffer()
  let groups = []
  let ids = self.counts.keys().to_array()
  ids.sort()
  let statistics = encode_file_statistics({
    message_count: self.messages,
    schema_count: self.declarations.schemas.length(),
    channel_count: self.declarations.channels.length().reinterpret_as_uint(),
    attachment_count: self.attachments,
    metadata_count: self.metadata,
    chunk_count: self.chunks,
    start_time: self.start_time,
    end_time: self.end_time,
    channel_message_counts: ids.map(fn(id) { (id, self.counts[id]) }),
  })
  self.check_summary(statistics.length() + 9)
  let opcodes : Array[Byte] = [0x03, 0x04, 0x08, 0x0a, 0x0b, 0x0d]
  for opcode in opcodes {
    let begin = out.length()
    for (op, body) in self.summary {
      if op == opcode {
        out.write_bytes(frame(op, body)[:])
        self.records += 1
      }
    }
    if opcode == 0x0b {
      out.write_bytes(frame(opcode, statistics)[:])
      self.records += 1
    }
    if out.length() > begin {
      groups.push({
        opcode,
        start: start + begin.to_uint64(),
        length: (out.length() - begin).to_uint64(),
      })
    }
  }
  let offset_start = start + out.length().to_uint64()
  for group in groups {
    out.write_bytes(frame(0x0e, encode_summary_offset(group))[:])
    self.records += 1
  }
  if out.length() > self.options.summary_bytes ||
    self.records >= self.limits.max_records {
    raise Invalid("final summary budget exceeded")
  }
  let footer : Footer = {
    summary_start: start,
    summary_offset_start: offset_start,
    summary_crc: 0U,
  }
  let provisional = frame(0x02, encode_footer(footer))
  let checksum = crc32(out.to_bytes() + provisional[0:25].to_owned())
  self.send(out.to_bytes(), false)
  self.emit(0x02, encode_footer({ ..footer, summary_crc: checksum, }), false)
  self.send(magic(), false)
  self.state = WriterFinished
}

///|
pub fn StreamWriter::finish(
  self : StreamWriter,
) -> WriteProgress raise McapError {
  self.require_open()
  self.finish_output() catch {
    Invalid(reason) => {
      self.state = WriterFailed
      raise Invalid(reason)
    }
  }
  self.progress()
}

///|
/// Leaves an unfinished output prefix; does not fabricate a successful footer.
pub fn StreamWriter::cancel(self : StreamWriter) -> Unit {
  if self.state == WriterOpen {
    self.pending.clear()
    self.buffered = 0
    self.state = WriterCancelled
  }
}

///|
pub fn StreamWriter::progress(self : StreamWriter) -> WriteProgress {
  {
    state: self.state,
    bytes_written: self.sink.position,
    messages: self.messages,
    chunks: self.chunks,
    buffered_bytes: self.buffered,
    peak_buffered_bytes: self.peak_buffered,
    retained_summary_bytes: self.retained,
  }
}
