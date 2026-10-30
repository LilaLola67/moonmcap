///|
pub struct AsyncDiskSidecarReader {
  source : AsyncSource
  index : AsyncSource
  reader : @mcap.DiskSidecarReader
  pool : WorkerPool?
  limits : @mcap.Limits
  select_region : async (UInt64, Int32) -> Bool
}

///|
pub async fn open_disk_sidecar(
  source : AsyncSource,
  index : AsyncSource,
  verification : AsyncIndexVerification,
  limits~ : @mcap.Limits = @mcap.default_limits(),
  declaration_bytes~ : Int32 = 8 * 1024 * 1024,
  cache_pages~ : Int32 = 4,
  chunk_cache_bytes~ : Int32 = 8 * 1024 * 1024,
  index_bytes~ : UInt64 = 1099511627776UL,
  pool? : WorkerPool,
  select_region~ : async (UInt64, Int32) -> Bool = async fn(_, _) { true },
) -> AsyncDiskSidecarReader {
  if source.size() < 50UL ||
    index.size() < 16UL ||
    index.size() > index_bytes ||
    declaration_bytes < 0 ||
    declaration_bytes > 2147482623 {
    raise @mcap.io_error("invalid async disk index envelope or budget")
  }
  let prefix = index.read_exact(0UL, 12)
  if prefix[0:8].to_owned() != b"MIDX2\r\n\x00" {
    raise @mcap.io_error("async disk index requires MIDX2")
  }
  let mut length = 0UL
  for i = 0; i < 4; i = i + 1 {
    length = length | (prefix[8 + i].to_uint64() << (i * 8))
  }
  if length > (declaration_bytes + 1024).to_uint64() ||
    length + 16UL > index.size() {
    raise @mcap.io_error("async disk index metadata exceeds budget")
  }
  let header = prefix + index.read_exact(12UL, length.to_int() + 4)
  let leading = source.read_exact(0UL, 8)
  let trailing = source.read_exact(source.size() - 8UL, 8)
  let mode = match verification {
    AsyncVerifyContent(hash) => {
      let digest = hash(source)
      @mcap.VerifyContent(fn(_) { digest })
    }
    AsyncTrustIdentity(token) => @mcap.TrustIdentity(token)
  }
  let source_mirror = @mcap.new_read_source(source.size(), fn(offset, count) {
    if offset == 0UL && count == 8 {
      leading
    } else if offset == source.size() - 8UL && count == 8 {
      trailing
    } else {
      raise @mcap.io_error(
        "unexpected synchronous source read in async disk index",
      )
    }
  })
  let index_mirror = @mcap.new_read_source(index.size(), fn(offset, count) {
    if offset > header.length().to_uint64() ||
      count < 0 ||
      count.to_uint64() > header.length().to_uint64() - offset {
      raise @mcap.io_error(
        "unexpected synchronous index read in async disk index",
      )
    }
    header[offset.to_int():offset.to_int() + count].to_owned()
  })
  let audit = @mcap.start_disk_sidecar_audit(
    source_mirror,
    index_mirror,
    mode,
    limits~,
    declaration_bytes~,
    cache_pages~,
    chunk_cache_bytes~,
    index_bytes~,
  )
  while audit.poll() is Some(span) {
    audit.feed(index.read_exact(span.offset, span.length))
  }
  { source, index, reader: audit.finish(), pool, limits, select_region, }
}

///|
pub struct AsyncDiskSidecarCursor {
  owner : AsyncDiskSidecarReader
  cursor : @mcap.DiskSidecarPollCursor
  budget : Int32
  mut busy : Bool
}

///|
pub fn AsyncDiskSidecarReader::cursor(
  self : AsyncDiskSidecarReader,
  query : @mcap.Query,
  pending_bytes~ : Int32 = 32 * 1024 * 1024,
) -> AsyncDiskSidecarCursor raise @mcap.McapError {
  if pending_bytes < 1 {
    raise @mcap.io_error("invalid async indexed read budget")
  }
  {
    owner: self,
    cursor: self.reader.poll_cursor(query),
    budget: pending_bytes,
    busy: false,
  }
}

///|
pub async fn AsyncDiskSidecarCursor::next(
  self : AsyncDiskSidecarCursor,
) -> @mcap.Message? {
  if self.busy {
    raise @mcap.io_error("concurrent async indexed cursor operation")
  }
  self.busy = true
  defer {
    self.busy = false
  }
  try {
    while true {
      match self.cursor.poll() {
        @mcap.DiskDone => return None
        @mcap.DiskMessage(message) => return Some(message)
        @mcap.DiskIndexRead(span) => {
          if span.length > self.budget {
            raise @mcap.io_error("index page exceeds async span budget")
          }
          self.cursor.feed_index(
            self.owner.index.read_exact(span.offset, span.length),
          )
        }
        @mcap.DiskSourceRead(span) => {
          if !(self.owner.select_region)(span.offset, span.length) {
            self.cursor.skip_source()
            continue
          }
          if span.length > self.budget {
            raise @mcap.io_error("source record exceeds async span budget")
          }
          let bytes = self.owner.source.read_exact(span.offset, span.length)
          let expanded = match self.owner.pool {
            None => None
            Some(pool) => {
              let records = @mcap.decode_records(
                bytes,
                limits=self.owner.limits,
              )
              if records.length() == 1 && records[0].opcode == 0x06 {
                let (expanded_bytes, compression) = @mcap.worker_chunk_properties(
                  records[0].body,
                  limits=self.owner.limits,
                )
                if compression == "" ||
                  expanded_bytes - 40 < pool.options.min_chunk_bytes {
                  None
                } else {
                  Some(
                    pool.expand_chunk(
                      records[0].body,
                      expanded_bytes,
                      limits=self.owner.limits,
                    ),
                  )
                }
              } else {
                None
              }
            }
          }
          self.cursor.feed_source(bytes, expanded?)
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
pub fn AsyncDiskSidecarCursor::seek(
  self : AsyncDiskSidecarCursor,
  time : UInt64,
) -> Unit raise @mcap.McapError {
  if self.busy {
    raise @mcap.io_error("async indexed cursor operation in progress")
  }
  self.cursor.seek(time)
}

///|
pub fn AsyncDiskSidecarCursor::cancel(self : AsyncDiskSidecarCursor) -> Unit {
  self.cursor.cancel()
}

///|
pub fn AsyncDiskSidecarReader::info(self : AsyncDiskSidecarReader) -> Json {
  let value = self.reader.info()
  if value is Object(fields) {
    fields["async_source_bytes_read"] = Json::string(
      self.source.metrics().bytes_read.to_string(),
    )
    fields["async_index_bytes_read"] = Json::string(
      self.index.metrics().bytes_read.to_string(),
    )
  }
  value
}

///|
pub fn AsyncDiskSidecarReader::query_source(
  self : AsyncDiskSidecarReader,
  abort_sources_on_cancel~ : Bool = false,
) -> AsyncQuerySource raise @mcap.McapError {
  async_query_source(self.reader.declaration_log(), fn(query, budget) {
    let cursor = self.cursor(query, pending_bytes=budget)
    async_message_stream(
      async fn() { cursor.next() },
      fn() {
        cursor.cancel()
        if abort_sources_on_cancel {
          self.source.cancel()
          self.index.cancel()
        }
      },
      pending_bytes=fn() { cursor.cursor.pending_bytes() },
    )
  })
}
