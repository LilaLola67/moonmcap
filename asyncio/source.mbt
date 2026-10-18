///|
pub struct AsyncSource {
  size : UInt64
  read_at : async (UInt64, Int) -> Bytes
  abort : () -> Unit
  mut cancelled : Bool
  mut read_calls : Int
  mut bytes_read : UInt64
}

///|
pub fn new_source(
  size : UInt64,
  read_at : async (UInt64, Int) -> Bytes,
  abort? : () -> Unit = fn() { () },
) -> AsyncSource {
  { size, read_at, abort, cancelled: false, read_calls: 0, bytes_read: 0UL, }
}

///|
pub fn AsyncSource::size(self : AsyncSource) -> UInt64 {
  self.size
}

///|
pub fn AsyncSource::cancel(self : AsyncSource) -> Unit {
  if !self.cancelled {
    self.cancelled = true
    (self.abort)()
  }
}

///|
pub async fn AsyncSource::read_exact(
  self : AsyncSource,
  offset : UInt64,
  length : Int,
) -> Bytes {
  if self.cancelled {
    raise @mcap.io_error("async source cancelled")
  }
  if length < 0 || offset > self.size || length.to_uint64() > self.size - offset {
    raise @mcap.io_error("async read span outside source")
  }
  let out = @buffer.Buffer::Buffer()
  while out.length() < length {
    if self.cancelled {
      raise @mcap.io_error("async source cancelled")
    }
    self.read_calls += 1
    let bytes = (self.read_at)(
      offset + out.length().to_uint64(),
      length - out.length(),
    )
    if self.cancelled {
      raise @mcap.io_error("async source cancelled")
    }
    self.bytes_read += bytes.length().to_uint64()
    if bytes.is_empty() || bytes.length() > length - out.length() {
      raise @mcap.io_error("async read made no progress or over-read")
    }
    out.write_bytes(bytes[:])
  }
  out.to_bytes()
}

///|
pub(all) struct AsyncReadMetrics {
  read_calls : Int
  bytes_read : UInt64
  cancelled : Bool
}

///|
pub fn AsyncSource::metrics(self : AsyncSource) -> AsyncReadMetrics {
  {
    read_calls: self.read_calls,
    bytes_read: self.bytes_read,
    cancelled: self.cancelled,
  }
}

///|
pub struct AsyncReader {
  source : AsyncSource
  reader : @mcap.RangeReader
}

///|
pub async fn open_reader(
  source : AsyncSource,
  cache_bytes? : Int = 8 * 1024 * 1024,
  summary_bytes? : Int = 8 * 1024 * 1024,
  fallback_bytes? : Int = 64 * 1024 * 1024,
  limits? : @mcap.Limits = @mcap.default_limits(),
) -> AsyncReader {
  if source.size < 50UL ||
    cache_bytes < 0 ||
    summary_bytes < 0 ||
    fallback_bytes < 0 ||
    limits.max_record_bytes < 22 {
    raise @mcap.io_error("invalid async reader budgets or short file")
  }
  let spans : Array[(@mcap.ReadSpan, Bytes)] = []
  let leading = source.read_exact(0UL, 8)
  if leading != @mcap.magic() {
    raise @mcap.io_error("missing async source magic")
  }
  spans.push(({ offset: 0UL, length: 8, }, leading))
  let tail_start = source.size - 37UL
  let tail = source.read_exact(tail_start, 37)
  if tail[29:37].to_owned() != @mcap.magic() {
    raise @mcap.io_error("missing async source trailing magic")
  }
  spans.push(({ offset: tail_start, length: 37, }, tail))
  let records = @mcap.decode_records(tail[0:29].to_owned(), limits~)
  if records.length() != 1 || records[0].opcode != 0x02 {
    raise @mcap.io_error("invalid async Footer")
  }
  let footer = @mcap.decode_footer(records[0].body)
  let summary_start = if footer.summary_start == 0UL {
    tail_start
  } else {
    footer.summary_start
  }
  if summary_start < 21UL ||
    summary_start > tail_start ||
    tail_start - summary_start > summary_bytes.to_uint64() {
    raise @mcap.io_error("async summary boundary or budget invalid")
  }
  let ending = source.read_exact(summary_start - 13UL, 13)
  spans.push(({ offset: summary_start - 13UL, length: 13, }, ending))
  let header = source.read_exact(8UL, 9)
  let mut size = 0UL
  for i = 0; i < 8; i = i + 1 {
    size = size | (header[i + 1].to_uint64() << (i * 8))
  }
  if summary_start < 30UL ||
    size > limits.max_record_bytes.to_uint64() ||
    size > summary_start - 30UL {
    raise @mcap.io_error("async Header size invalid")
  }
  spans.push(({ offset: 8UL, length: 9, }, header))
  let body = source.read_exact(17UL, size.to_int())
  spans.push(({ offset: 17UL, length: size.to_int(), }, body))
  let summary = source.read_exact(
    summary_start,
    (tail_start - summary_start).to_int(),
  )
  spans.push(({ offset: summary_start, length: summary.length(), }, summary))
  let mirror = @mcap.new_read_source(source.size, fn(offset, length) {
    for (span, bytes) in spans {
      if offset >= span.offset &&
        offset - span.offset <= span.length.to_uint64() {
        let begin = (offset - span.offset).to_int()
        if length <= bytes.length() - begin {
          return bytes[begin:begin + length].to_owned()
        }
      }
    }
    raise @mcap.io_error("async metadata mirror received an unplanned read")
  })
  let reader = @mcap.open_reader(
    mirror,
    cache_bytes~,
    summary_bytes~,
    fallback_bytes~,
    limits~,
  )
  if !reader.diagnostics().indexed {
    if source.size > fallback_bytes.to_uint64() {
      raise @mcap.io_error("async unindexed fallback exceeds budget")
    }
    let all = source.read_exact(0UL, source.size.to_int())
    spans.clear()
    spans.push(({ offset: 0UL, length: all.length(), }, all))
  }
  { source, reader, }
}

///|
pub struct AsyncCursor {
  source : AsyncSource
  cursor : @mcap.FileCursor
  mut busy : Bool
}

///|
pub fn AsyncReader::cursor(
  self : AsyncReader,
  query : @mcap.Query,
  pending_bytes? : Int = 64 * 1024 * 1024,
  span_bytes? : Int = 32 * 1024 * 1024,
) -> AsyncCursor raise @mcap.McapError {
  {
    source: self.source,
    cursor: self.reader.cursor(query, pending_bytes~, span_bytes~),
    busy: false,
  }
}

///|
pub async fn AsyncCursor::next(self : AsyncCursor) -> @mcap.Message? {
  if self.busy {
    raise @mcap.io_error("concurrent async cursor operation")
  }
  self.busy = true
  defer {
    self.busy = false
  }
  try {
    while true {
      match self.cursor.poll() {
        @mcap.FileDone => return None
        @mcap.FileMessage(message) => return Some(message)
        @mcap.FileRead(span) =>
          self.cursor.feed(self.source.read_exact(span.offset, span.length))
      }
    }
    None
  } catch {
    error => {
      self.cursor.cancel()
      raise error
    }
  }
}

///|
pub async fn AsyncCursor::next_page(
  self : AsyncCursor,
  size : Int,
) -> Array[@mcap.Message] {
  if size <= 0 {
    raise @mcap.io_error("async page size must be positive")
  }
  let page = []
  for _i = 0; _i < size; _i = _i + 1 {
    match self.next() {
      Some(message) => page.push(message)
      None => break
    }
  }
  page
}

///|
pub fn AsyncCursor::seek(
  self : AsyncCursor,
  time : UInt64,
) -> Unit raise @mcap.McapError {
  if self.busy {
    raise @mcap.io_error("async cursor operation in progress")
  }
  self.cursor.seek(time)
}

///|
pub fn AsyncCursor::cancel(self : AsyncCursor) -> Unit {
  self.cursor.cancel()
}

///|
pub fn AsyncCursor::progress(self : AsyncCursor) -> @mcap.FileProgress {
  self.cursor.progress()
}

///|
pub fn AsyncReader::channels(self : AsyncReader) -> Array[@mcap.Channel] {
  self.reader.channels()
}
