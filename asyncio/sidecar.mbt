///|
pub(all) enum AsyncIndexVerification {
  AsyncVerifyContent(async (AsyncSource) -> String)
  AsyncTrustIdentity(String)
}

///|
pub struct AsyncSidecarReader {
  source : AsyncSource
  reader : @mcap.SidecarReader
}

///|
pub async fn open_sidecar(
  source : AsyncSource,
  bytes : Bytes,
  verification : AsyncIndexVerification,
  limits? : @mcap.Limits = @mcap.default_limits(),
  index_bytes? : Int = 32 * 1024 * 1024,
  cache_bytes? : Int = 8 * 1024 * 1024,
) -> AsyncSidecarReader {
  if source.size < 50UL {
    raise @mcap.io_error("short async sidecar source")
  }
  let leading = source.read_exact(0UL, 8)
  let trailing = source.read_exact(source.size - 8UL, 8)
  let mode = match verification {
    AsyncTrustIdentity(token) => @mcap.TrustIdentity(token)
    AsyncVerifyContent(hash) => {
      let digest = hash(source)
      @mcap.VerifyContent(fn(_) { digest })
    }
  }
  let mirror = @mcap.new_read_source(source.size, fn(offset, length) {
    if offset == 0UL && length == 8 {
      leading
    } else if offset == source.size - 8UL && length == 8 {
      trailing
    } else {
      raise @mcap.io_error(
        "async sidecar open requested unplanned source bytes",
      )
    }
  })
  {
    source,
    reader: @mcap.open_sidecar(
      mirror,
      bytes,
      mode,
      limits~,
      index_bytes~,
      cache_bytes~,
    ),
  }
}

///|
pub struct AsyncSidecarCursor {
  source : AsyncSource
  cursor : @mcap.SidecarCursor
  budget : Int
  mut busy : Bool
}

///|
pub fn AsyncSidecarReader::cursor(
  self : AsyncSidecarReader,
  query : @mcap.Query,
  pending_bytes? : Int = 32 * 1024 * 1024,
) -> AsyncSidecarCursor raise @mcap.McapError {
  if pending_bytes < 1 {
    raise @mcap.io_error("invalid sidecar span budget")
  }
  {
    source: self.source,
    cursor: self.reader.cursor(query),
    budget: pending_bytes,
    busy: false,
  }
}

///|
pub async fn AsyncSidecarCursor::next(
  self : AsyncSidecarCursor,
) -> @mcap.Message? {
  if self.busy {
    raise @mcap.io_error("concurrent async sidecar operation")
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
        @mcap.FileRead(span) => {
          if span.length > self.budget {
            raise @mcap.io_error("async sidecar span exceeds budget")
          }
          self.cursor.feed(self.source.read_exact(span.offset, span.length))
        }
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
pub fn AsyncSidecarCursor::seek(
  self : AsyncSidecarCursor,
  time : UInt64,
) -> Unit raise @mcap.McapError {
  if self.busy {
    raise @mcap.io_error("sidecar operation in progress")
  }
  self.cursor.seek(time)
}

///|
pub fn AsyncSidecarCursor::cancel(self : AsyncSidecarCursor) -> Unit {
  self.cursor.cancel()
}

///|
pub fn AsyncSidecarReader::info(self : AsyncSidecarReader) -> Json {
  let value = self.reader.info()
  if value is Object(fields) {
    fields["async_source_bytes_read"] = Json::string(
      self.source.metrics().bytes_read.to_string(),
    )
  }
  value
}
