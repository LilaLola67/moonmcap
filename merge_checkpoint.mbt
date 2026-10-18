///|
struct MergeState {
  runs : Array[Int]
  hashes : Map[Int, String]
  inputs : Array[String]
  auxiliary : String
}

///|
fn checkpoint_settings(options : ExternalOptions) -> Bytes {
  let b = @buffer.Buffer::Buffer()
  for
    n in [
      options.run_bytes,
      options.merge_bytes,
      options.fan_in,
      options.run_limit,
      options.declaration_bytes,
      options.limits.max_record_bytes,
      options.limits.max_string_bytes,
      options.limits.max_records,
      options.limits.max_decompressed_bytes,
      options.output.chunk_bytes,
      options.output.chunk_messages,
      options.output.summary_bytes,
    ] {
    b.write_uint_le(n.to_uint())
  }
  b.write_uint64_le(options.spool_bytes)
  b.write_uint64_le(options.output.chunk_span)
  b.write_byte(
    match options.output.compression {
      Uncompressed => 0
      Lz4 => 1
      Zstd => 2
    },
  )
  b.to_bytes()
}

///|
fn checkpoint_text(b : @buffer.Buffer, text : String) -> Unit {
  let bytes = @utf8.encode(text[:])
  b.write_uint_le(bytes.length().to_uint())
  b.write_bytes(bytes[:])
}

///|
fn merge_checkpoint(
  session : SortSession,
  state : MergeState,
) -> Bytes raise McapError {
  let b = @buffer.Buffer::Buffer()
  b.write_bytes(b"MJOB1\r\n\x00"[:])
  b.write_bytes(checkpoint_settings(session.options)[:])
  b.write_uint_le(state.inputs.length().to_uint())
  for fingerprint in state.inputs {
    checkpoint_text(b, fingerprint)
  }
  checkpoint_text(b, state.auxiliary)
  b.write_uint64_le(session.ordinal)
  b.write_uint_le(session.next_id.to_uint())
  b.write_uint_le(session.passes.to_uint())
  b.write_uint_le(session.peak_run.to_uint())
  b.write_uint_le(session.peak_merge.to_uint())
  b.write_uint64_le(session.peak_spool)
  b.write_uint_le(state.runs.length().to_uint())
  for id in state.runs {
    b.write_uint_le(id.to_uint())
    b.write_uint64_le(session.live[id])
    checkpoint_text(b, state.hashes[id])
  }
  let body = b.to_bytes()
  if body.length() > 8 * 1024 * 1024 - 4 {
    raise Invalid("merge checkpoint exceeds 8 MiB")
  }
  body + crc32(body).to_le_bytes()
}

///|
fn checkpoint_cursor(bytes : Bytes) -> Cursor raise McapError {
  if bytes.length() < 12 || bytes.length() > 8 * 1024 * 1024 {
    raise Invalid("merge checkpoint size invalid")
  }
  let body = bytes[0:bytes.length() - 4].to_owned()
  if cursor(bytes[bytes.length() - 4:].to_owned(), default_limits()).u32() !=
    crc32(body) {
    raise Invalid("merge checkpoint checksum mismatch")
  }
  let c = cursor(body, { ..default_limits(), max_string_bytes: 256, })
  if c.take(8) != b"MJOB1\r\n\x00" {
    raise Invalid("unsupported merge checkpoint version")
  }
  c
}

///|
fn restore_merge(
  session : SortSession,
  bytes : Bytes,
  fingerprints : Array[String],
) -> MergeState raise McapError {
  let c = checkpoint_cursor(bytes)
  let settings = checkpoint_settings(session.options)
  if c.take(settings.length()) != settings {
    raise Invalid("merge checkpoint options changed")
  }
  let count = bounded_length(c.u32().to_uint64(), 64, "checkpoint input count")
  let inputs = Array::makei(count, fn(_) { c.text() })
  if inputs != fingerprints {
    raise Invalid("merge checkpoint input fingerprint/order changed")
  }
  let auxiliary = c.text()
  session.ordinal = c.u64()
  session.next_id = bounded_length(
    c.u32().to_uint64(),
    session.options.run_limit,
    "checkpoint next run",
  )
  session.passes = bounded_length(
    c.u32().to_uint64(),
    session.options.run_limit,
    "checkpoint passes",
  )
  session.peak_run = bounded_length(
    c.u32().to_uint64(),
    session.options.run_bytes,
    "checkpoint run memory",
  )
  session.peak_merge = bounded_length(
    c.u32().to_uint64(),
    session.options.merge_bytes,
    "checkpoint merge memory",
  )
  session.peak_spool = c.u64()
  if session.ordinal > session.options.limits.max_records.to_uint64() ||
    session.peak_spool > session.options.spool_bytes {
    raise Invalid("checkpoint counters exceed budgets")
  }
  let count = bounded_length(
    c.u32().to_uint64(),
    session.next_id,
    "checkpoint live run count",
  )
  let runs = []
  let hashes : Map[Int, String] = Map([])
  for i = 0; i < count; i = i + 1 {
    let id = bounded_length(
      c.u32().to_uint64(),
      session.next_id - 1,
      "checkpoint run id",
    )
    let size = c.u64()
    if hashes.contains(id) || size > session.options.spool_bytes - session.spool {
      raise Invalid("checkpoint duplicate run or spool overflow")
    }
    session.spool += size
    session.live[id] = size
    hashes[id] = c.text()
    runs.push(id)
  }
  c.finish()
  { runs, hashes, inputs, auxiliary, }
}

///|
/// Read owned run IDs for a storage adapter's orphan cleanup. No path strings.
pub fn checkpoint_run_ids(bytes : Bytes) -> Array[Int] raise McapError {
  let c = checkpoint_cursor(bytes)
  ignore(c.take(65))
  let inputs = bounded_length(c.u32().to_uint64(), 64, "checkpoint input count")
  for i = 0; i < inputs; i = i + 1 {
    ignore(c.text())
  }
  ignore(c.text())
  ignore(c.take(32))
  let count = bounded_length(c.u32().to_uint64(), 32768, "checkpoint run count")
  let ids = [-1]
  for i = 0; i < count; i = i + 1 {
    ids.push(
      bounded_length(c.u32().to_uint64(), 2147483647, "checkpoint run id"),
    )
    ignore(c.u64())
    ignore(c.text())
  }
  c.finish()
  ids
}

///|
/// Resume only durable ingestion/merge boundaries. Caller atomically saves and
/// fsyncs checkpoints and runs, owns a job lock, and removes unreferenced runs.
/// Final output may be retried; committed runs remain until caller cleanup.
pub fn resumable_merge(
  sources : Array[ReadSource],
  storage : SortStorage,
  output : WriteSink,
  fingerprint : (ReadSource) -> String raise McapError,
  save : (Bytes) -> Unit raise McapError,
  checkpoint? : Bytes,
  options? : ExternalOptions = external_options(),
  cancelled? : () -> Bool = fn() { false },
) -> ExternalReport raise McapError {
  if sources.is_empty() ||
    sources.length() > 64 ||
    options.run_bytes < 43 ||
    options.merge_bytes < 43 ||
    options.fan_in < 2 ||
    options.fan_in > 64 ||
    options.run_limit < 1 ||
    options.run_limit > 32768 ||
    options.declaration_bytes < 0 {
    raise Invalid("invalid resumable merge sources or budgets")
  }
  validate_log(empty_log(), limits=options.limits)
  let session : SortSession = {
    observer: None,
    preserve_ids: false,
    storage,
    options,
    live: Map([]),
    cancelled,
    buffer: [],
    buffered: 0,
    ordinal: 0UL,
    next_id: 0,
    spool: 0UL,
    peak_spool: 0UL,
    peak_run: 0,
    peak_merge: 0,
    passes: 0,
    declared: 0,
  }
  let fingerprints = sources.map(fn(source) {
    session.check_cancel()
    fingerprint(source)
  })
  let mut state = match checkpoint {
    Some(bytes) => restore_merge(session, bytes, fingerprints)
    None => {
      let first = open_reader(
        sources[0],
        limits=options.limits,
        cache_bytes=0,
        fallback_bytes=0,
        summary_bytes=options.declaration_bytes,
      )
      let global = { ..empty_log(), header: first.declarations.header, }
      let raw = (storage.create)(-1)
      // Non-message records also consume the physical temporary-space budget.
      let auxiliary = new_stream_writer(
        new_write_sink(fn(data) {
          if data.length().to_uint64() > options.spool_bytes - session.spool {
            raise Invalid("auxiliary MCAP exceeds spool budget")
          }
          raw.write(data)
          session.spool += data.length().to_uint64()
          if session.spool > session.peak_spool {
            session.peak_spool = session.spool
          }
        }),
        profile=global.header.profile,
        options=options.output,
        limits=options.limits,
      )
      let runs = []
      for source in sources {
        session.ingest(source, global, auxiliary, runs)
      }
      if session.flush_run() is Some(id) {
        runs.push(id)
      }
      ignore(auxiliary.finish())
      if session.spool > session.peak_spool {
        session.peak_spool = session.spool
      }
      let hashes : Map[Int, String] = Map([])
      for id in runs {
        hashes[id] = fingerprint((storage.open)(id))
      }
      let state : MergeState = {
        runs,
        hashes,
        inputs: fingerprints,
        auxiliary: fingerprint((storage.open)(-1)),
      }
      save(merge_checkpoint(session, state))
      state
    }
  }
  let auxiliary = (storage.open)(-1)
  if fingerprint(auxiliary) != state.auxiliary {
    raise Invalid("merge auxiliary fingerprint changed")
  }
  // Restored counters cover runs; add auxiliary bytes exactly once.
  if checkpoint is Some(_) {
    if auxiliary.size > options.spool_bytes - session.spool {
      raise Invalid("merge auxiliary exceeds spool budget")
    }
    session.spool += auxiliary.size
  }
  let mut total = 0UL
  for id in state.runs {
    session.check_cancel()
    if fingerprint((storage.open)(id)) != state.hashes[id] {
      raise Invalid("merge run fingerprint changed")
    }
    let input = session.open_run(id)
    total += input.count
    while input.next() is Some(row) {
      if row.ordinal >= session.ordinal {
        raise Invalid("merge run ordinal out of bounds")
      }
    }
  }
  if total != session.ordinal {
    raise Invalid("merge checkpoint message count mismatch")
  }
  while state.runs.length() > options.fan_in {
    let group = state.runs[0:options.fan_in].to_array()
    let mut count = 0UL
    for id in group {
      count += session.open_run(id).count
    }
    let (id, sink) = session.create(count)
    session.merge_group(group, fn(row) { write_sort_row(sink, row) })
    state.hashes[id] = fingerprint((storage.open)(id))
    let runs = state.runs[options.fan_in:].to_array()
    runs.push(id)
    let next = { ..state, runs, }
    session.passes += 1
    save(merge_checkpoint(session, next))
    for old in group {
      session.remove(old)
      state.hashes.remove(old)
    }
    state = next
  }
  let first = open_reader(
    auxiliary,
    limits=options.limits,
    cache_bytes=0,
    fallback_bytes=0,
  )
  let writer = new_stream_writer(
    output,
    profile=first.declarations.header.profile,
    options=options.output,
    limits=options.limits,
  )
  ignore(
    scan_source(
      auxiliary,
      source_visitor(
        schema=fn(s) { writer.add_schema(s) },
        channel=fn(c) { writer.add_channel(c) },
        attachment=fn(a) { writer.add_attachment(a) },
        metadata=fn(m) { writer.add_metadata(m) },
        message=fn(_, _) { raise Invalid("merge auxiliary contains messages") },
      ),
      limits=options.limits,
      declaration_bytes=options.declaration_bytes,
      cancelled~,
    ),
  )
  session.merge_group(state.runs, fn(row) { writer.append(row.message) })
  let result = writer.finish()
  {
    input_files: sources.length(),
    messages: session.ordinal,
    runs_created: session.next_id,
    merge_passes: session.passes + 1,
    peak_run_bytes: session.peak_run,
    peak_merge_bytes: session.peak_merge,
    peak_spool_bytes: session.peak_spool,
    output_bytes: result.bytes_written,
  }
}
