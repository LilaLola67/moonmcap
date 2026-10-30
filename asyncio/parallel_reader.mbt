///|
struct PreparedChunk {
  skipped : Bool
  original : Bytes
  expanded : Bytes?
  failure : String?
}

///|
pub struct ParallelReader {
  base : AsyncReader
  pool : WorkerPool
  limits : @mcap.Limits
  select_region : async (UInt64, Int32) -> Bool
  mut batch_peak : Int32
}

///|
pub async fn open_parallel_reader(
  source : AsyncSource,
  pool : WorkerPool,
  cache_bytes~ : Int32 = 8 * 1024 * 1024,
  summary_bytes~ : Int32 = 8 * 1024 * 1024,
  fallback_bytes~ : Int32 = 64 * 1024 * 1024,
  limits~ : @mcap.Limits = @mcap.default_limits(),
  select_region~ : async (UInt64, Int32) -> Bool = async fn(_, _) { true },
) -> ParallelReader {
  {
    base: open_reader(
      source,
      cache_bytes~,
      summary_bytes~,
      fallback_bytes~,
      limits~,
    ),
    pool,
    limits,
    select_region,
    batch_peak: 0,
  }
}

///|
pub struct ParallelCursor {
  owner : ParallelReader
  cursor : @mcap.FileCursor
  prepared : Map[UInt64, PreparedChunk]
  mut busy : Bool
}

///|
pub fn ParallelReader::cursor(
  self : ParallelReader,
  query : @mcap.Query,
  pending_bytes~ : Int32 = 64 * 1024 * 1024,
  span_bytes~ : Int32 = 32 * 1024 * 1024,
) -> ParallelCursor raise @mcap.McapError {
  {
    owner: self,
    cursor: self.base.reader.cursor(query, pending_bytes~, span_bytes~),
    prepared: Map([]),
    busy: false,
  }
}

///|
async fn ParallelCursor::fill(self : ParallelCursor) -> Unit {
  let config = self.owner.pool.options
  // fill is called only when the currently demanded span is absent; discard
  // obsolete lookahead (e.g. another cursor populated the shared Chunk cache).
  self.prepared.clear()
  let requests = self.cursor.upcoming_reads(config.prefetch)
  let selected = []
  let mut bytes = 0UL
  for request in requests {
    if self.prepared.contains(request.span.offset) {
      continue
    }
    // Include conservative hexadecimal request/response transport overhead.
    let charge = 2UL *
      (request.span.length.to_uint64() + request.expanded_bytes.to_uint64()) +
      4096UL
    if charge > config.buffer_bytes.to_uint64() - bytes {
      if !selected.is_empty() {
        break
      }
      self.prepared.clear()
      bytes = 0UL
      if charge > config.buffer_bytes.to_uint64() {
        raise @mcap.io_error("parallel chunk batch exceeds buffer budget")
      }
    }
    bytes += charge
    selected.push(request)
  }
  if selected.is_empty() {
    raise @mcap.io_error("parallel cursor made no read progress")
  }
  self.owner.batch_peak = self.owner.batch_peak.max(bytes.to_int())
  let outcomes = @async.all(
    selected.map(fn(request) {
      async fn() {
        try {
          // ChunkReadRequest includes following MessageIndexes; recover the
          // Chunk's exact physical span from the source header before pruning.
          // The indexed request exposes this without additional source IO.
          if !(self.owner.select_region)(
              request.span.offset,
              request.chunk_bytes,
            ) {
            return PreparedChunk::{
              skipped: true,
              original: b"",
              expanded: None,
              failure: None,
            }
          }
          let original = self.owner.base.source.read_exact(
            request.span.offset,
            request.span.length,
          )
          if request.compression == "" ||
            request.expanded_bytes - 40 < config.min_chunk_bytes {
            return PreparedChunk::{
              skipped: false,
              original,
              expanded: None,
              failure: None,
            }
          }
          let records = @mcap.decode_records(original, limits=self.owner.limits)
          guard records.length() > 0 && records[0].opcode == 0x06 else {
            raise @mcap.io_error("parallel span does not start with Chunk")
          }
          let limits = self.owner.limits
          let job : Json = {
            "op": "expand",
            "data": @mcap.encode_hex(records[0].body),
            "output_bytes": request.expanded_bytes,
            "limits": {
              "record": limits.max_record_bytes.min(config.input_bytes),
              "string": limits.max_string_bytes,
              "records": limits.max_records,
              "decompressed": request.expanded_bytes - 40,
            },
          }
          let expanded = self.owner.pool.call(job, request.expanded_bytes)
          PreparedChunk::{
            skipped: false,
            original,
            expanded: Some(expanded),
            failure: None,
          }
        } catch {
          error =>
            PreparedChunk::{
              skipped: false,
              original: b"",
              expanded: None,
              failure: Some(
                match error {
                  @mcap.Invalid(reason) => reason
                  _ => "\{error}"
                },
              ),
            }
        }
      }
    }),
    max_concurrent=config.workers,
  )
  for i = 0; i < selected.length(); i = i + 1 {
    self.prepared[selected[i].span.offset] = outcomes[i]
  }
}

///|
pub async fn ParallelCursor::next(self : ParallelCursor) -> @mcap.Message? {
  if self.busy {
    raise @mcap.io_error("concurrent parallel cursor operation")
  }
  self.busy = true
  defer {
    self.busy = false
  }
  try {
    while true {
      match self.cursor.poll() {
        @mcap.FileDone => {
          self.prepared.clear()
          return None
        }
        @mcap.FileMessage(message) => return Some(message)
        @mcap.FileRead(span) => {
          if !self.prepared.contains(span.offset) {
            self.fill()
          }
          guard self.prepared.get(span.offset) is Some(prepared) else {
            raise @mcap.io_error("missing prepared Chunk")
          }
          self.prepared.remove(span.offset)
          if prepared.failure is Some(reason) {
            raise @mcap.io_error(reason)
          }
          if prepared.skipped {
            self.cursor.skip_chunk()
            continue
          }
          match prepared.expanded {
            None => self.cursor.feed(prepared.original)
            Some(expanded) =>
              self.cursor.feed_expanded(prepared.original, expanded)
          }
        }
      }
    }
    None
  } catch {
    error => {
      self.cursor.cancel()
      self.prepared.clear()
      raise error
    }
  }
}

///|
pub fn ParallelCursor::seek(
  self : ParallelCursor,
  time : UInt64,
) -> Unit raise @mcap.McapError {
  if self.busy {
    raise @mcap.io_error("parallel cursor operation in progress")
  }
  self.prepared.clear()
  self.cursor.seek(time)
}

///|
pub fn ParallelCursor::cancel(self : ParallelCursor) -> Unit {
  self.cursor.cancel()
  self.prepared.clear()
}

///|
pub fn ParallelCursor::progress(self : ParallelCursor) -> @mcap.FileProgress {
  self.cursor.progress()
}

///|
pub fn ParallelReader::query_source(
  self : ParallelReader,
) -> AsyncQuerySource raise @mcap.McapError {
  if !self.base.reader.diagnostics().indexed {
    return self.base.query_source(select_region=self.select_region)
  }
  async_query_source(
    self.base.reader.declaration_log(),
    fn(query, budget) {
      let cursor = self.cursor(query, pending_bytes=budget)
      async_message_stream(
        async fn() { cursor.next() },
        fn() { cursor.cancel() },
        pending_bytes=fn() { cursor.progress().pending_bytes },
      )
    },
    materialized=!self.base.reader.diagnostics().indexed,
  )
}

///|
pub fn ParallelReader::info(self : ParallelReader) -> Json {
  {
    "peak_cursor_batch_bytes": self.batch_peak,
    "cursor_batch_budget": self.pool.options.buffer_bytes,
    "decoded_chunks": self.base.reader.diagnostics().decoded_chunks,
    "async_source_bytes_read": self.base.source.metrics().bytes_read.to_string(),
    "indexed": self.base.reader.diagnostics().indexed,
  }
}

///|
struct ParallelPayload {
  message : @mcap.Message?
  data : Bytes?
  failure : String?
}

///|
pub fn AsyncQuerySource::decode_parallel(
  self : AsyncQuerySource,
  pool : WorkerPool,
  max_bytes~ : Int32 = 1024 * 1024,
) -> AsyncQuerySource raise @mcap.McapError {
  if max_bytes < 1 || max_bytes > pool.options.output_bytes {
    raise @mcap.io_error("invalid parallel decoded JSON budget")
  }
  let context = @mcap.write_log(self.declarations)
  if context.length() > pool.options.context_bytes {
    raise @mcap.io_error("parallel declaration context exceeds budget")
  }
  let encoded = @mcap.encode_hex(context)
  let decoder = @mcap.standard_payload_decoder()
  let declarations = {
    ..@mcap.copy_log(self.declarations),
    schemas: [],
    channels: self.declarations.channels.map(fn(c) {
      { ..c, schema_id: 0, message_encoding: "json", }
    }),
  }
  async_query_source(
    declarations,
    fn(query, budget) {
      let inner = (self.open)(query, budget)
      let pending : Array[ParallelPayload] = []
      let ended = [false]
      let closed = [false]
      async_message_stream(
        async fn() {
          if closed[0] {
            return None
          }
          try {
            if pending.is_empty() && !ended[0] {
              let inputs = []
              let failure : Array[String] = []
              let mut charged = 0UL
              for i = 0; i < pool.options.prefetch; i = i + 1 {
                if !inputs.is_empty() &&
                  pool.options.input_bytes.to_uint64() +
                  max_bytes.to_uint64() +
                  31UL >
                  budget.to_uint64() - charged {
                  break
                }
                let next = (inner.next)() catch {
                  error => {
                    failure.push(
                      match error {
                        @mcap.Invalid(reason) => reason
                        _ => "\{error}"
                      },
                    )
                    ended[0] = true
                    None
                  }
                }
                guard next is Some(message) else {
                  ended[0] = true
                  break
                }
                let charge = message.data.length().to_uint64() +
                  max_bytes.to_uint64() +
                  31UL
                if charge > budget.to_uint64() - charged {
                  failure.push(
                    "parallel payload exceeds provider buffer budget",
                  )
                  ended[0] = true
                  break
                }
                charged += charge
                inputs.push(message)
              }
              let results = @async.all(
                inputs.map(fn(message) {
                  async fn() {
                    try {
                      let data = if message.data.length() <
                        pool.options.min_payload_bytes {
                        decoder.decode_message(
                          message,
                          self.declarations,
                          max_bytes~,
                        ).data
                      } else {
                        let job : Json = {
                          "op": "decode",
                          "data": @mcap.encode_hex(message.data),
                          "declarations": encoded,
                          "channel": message.channel_id,
                          "output_bytes": max_bytes,
                        }
                        pool.call(job, max_bytes)
                      }
                      ParallelPayload::{
                        message: Some(message),
                        data: Some(data),
                        failure: None,
                      }
                    } catch {
                      error =>
                        ParallelPayload::{
                          message: Some(message),
                          data: None,
                          failure: Some(
                            match error {
                              @mcap.Invalid(reason) => reason
                              _ => "\{error}"
                            },
                          ),
                        }
                    }
                  }
                }),
                max_concurrent=pool.options.workers,
              )
              for result in results {
                pending.push(result)
              }
              if !failure.is_empty() {
                pending.push({
                  message: None,
                  data: None,
                  failure: Some(failure[0]),
                })
              }
            }
            if pending.is_empty() {
              closed[0] = true
              (inner.stop)()
              return None
            }
            let row = pending.remove(0)
            if row.failure is Some(reason) {
              raise @mcap.io_error(reason)
            }
            guard row.message is Some(message) && row.data is Some(data) else {
              raise @mcap.io_error("invalid parallel payload result")
            }
            Some({ ..message, data, })
          } catch {
            error => {
              closed[0] = true
              pending.clear()
              (inner.stop)()
              raise error
            }
          }
        },
        fn() {
          closed[0] = true
          pending.clear()
          (inner.stop)()
        },
        pending_bytes=fn() {
          let mut total = (inner.pending)().to_uint64()
          for row in pending {
            if row.data is Some(data) {
              total += data.length().to_uint64() + 31UL
            }
          }
          if total > 2147483647UL {
            2147483647
          } else {
            total.to_int()
          }
        },
      )
    },
    materialized=self.materialized,
  )
}
