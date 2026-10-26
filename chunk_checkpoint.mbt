///|
struct ChunkPart {
  input : Int
  chunk : Int
  messages : UInt64
  bytes : UInt64
  digest : String
}

///|
struct ChunkJobState {
  settings : Bytes
  inputs : Array[String]
  parts : Array[ChunkPart]
  crcs : Array[Bool]
  mut completed : Int
  mut next_chunk : Int
}

///|
fn encode_chunk_job(state : ChunkJobState) -> Bytes raise McapError {
  let out = @buffer.Buffer::Buffer()
  out.write_bytes(b"CJOB1\r\n\x00"[:])
  out.write_bytes(state.settings[:])
  out.write_uint_le(state.inputs.length().to_uint())
  for digest in state.inputs {
    checkpoint_text(out, digest)
  }
  out.write_uint_le(state.completed.to_uint())
  out.write_uint_le(state.next_chunk.to_uint())
  for verified in state.crcs {
    out.write_byte(if verified { 0x01 } else { 0x00 })
  }
  out.write_uint_le(state.parts.length().to_uint())
  for part in state.parts {
    out.write_uint_le(part.input.to_uint())
    out.write_int_le(part.chunk)
    out.write_uint64_le(part.messages)
    out.write_uint64_le(part.bytes)
    checkpoint_text(out, part.digest)
  }
  let body = out.to_bytes()
  if body.length() > 8 * 1024 * 1024 - 4 {
    raise Invalid("chunk checkpoint exceeds 8 MiB")
  }
  body + crc32(body).to_le_bytes()
}

///|
fn decode_chunk_job(bytes : Bytes) -> ChunkJobState raise McapError {
  if bytes.length() < 12 ||
    bytes.length() > 8 * 1024 * 1024 ||
    bytes[0:8].to_owned() != b"CJOB1\r\n\x00" {
    raise Invalid("invalid chunk checkpoint envelope")
  }
  let limits = { ..default_limits(), max_string_bytes: 256, }
  let body = bytes[0:bytes.length() - 4].to_owned()
  if crc32(body) != cursor(bytes[bytes.length() - 4:].to_owned(), limits).u32() {
    raise Invalid("chunk checkpoint checksum mismatch")
  }
  let value = cursor(body, limits)
  ignore(value.take(8))
  let settings = value.take(69)
  let maximum = cursor(settings[65:69].to_owned(), limits).u32()
  if maximum == 0U || maximum > 32768U {
    raise Invalid("invalid chunk checkpoint part budget")
  }
  let count = bounded_length(
    value.u32().to_uint64(),
    64,
    "chunk checkpoint inputs",
  )
  if count == 0 {
    raise Invalid("chunk checkpoint has no inputs")
  }
  let inputs = Array::makei(count, fn(_) { checked_digest(value.text()) })
  let completed = bounded_length(
    value.u32().to_uint64(),
    count,
    "chunk checkpoint completed inputs",
  )
  let next_chunk = bounded_length(
    value.u32().to_uint64(),
    maximum.to_int(),
    "chunk checkpoint next chunk",
  )
  let crcs = Array::makei(completed, fn(_) {
    match value.take(1)[0] {
      0x00 => false
      0x01 => true
      _ => raise Invalid("invalid chunk checkpoint CRC flag")
    }
  })
  let size = bounded_length(
    value.u32().to_uint64(),
    maximum.to_int(),
    "chunk checkpoint parts",
  )
  let parts = []
  let mut expected_input = 0
  let mut expected_chunk = 0
  for i = 0; i < size; i = i + 1 {
    let input = bounded_length(
      value.u32().to_uint64(),
      count - 1,
      "chunk checkpoint input ID",
    )
    let chunk = value.u32().to_int()
    let messages = value.u64()
    let bytes = value.u64()
    let digest = checked_digest(value.text())
    if input != expected_input || bytes < 50UL || messages > 2147483647UL {
      raise Invalid("chunk checkpoint part bounds mismatch")
    }
    if chunk == -1 {
      if messages != 0UL {
        raise Invalid("chunk auxiliary part contains messages")
      }
      expected_input += 1
      expected_chunk = 0
    } else {
      if chunk != expected_chunk {
        raise Invalid("chunk checkpoint part order mismatch")
      }
      expected_chunk += 1
    }
    parts.push({ input, chunk, messages, bytes, digest, })
  }
  value.finish()
  if expected_input != completed ||
    expected_chunk != next_chunk ||
    (completed == count && next_chunk != 0) {
    raise Invalid("chunk checkpoint progress mismatch")
  }
  { settings, inputs, parts, crcs, completed, next_chunk, }
}

///|
fn chunk_job_ids(bytes : Bytes) -> Array[Int] raise McapError {
  let state = decode_chunk_job(bytes)
  Array::makei(state.parts.length(), fn(i) { i })
}

///|
fn chunk_job_progress(bytes : Bytes) -> Json raise McapError {
  let state = decode_chunk_job(bytes)
  let mut messages = 0UL
  for part in state.parts {
    messages += part.messages
  }
  {
    "engine": "indexed-chunk-ingest",
    "version": 1,
    "input_files": state.inputs.length(),
    "completed_inputs": state.completed,
    "next_chunk": state.next_chunk,
    "committed_parts": state.parts.length(),
    "committed_messages": messages.to_string(),
    "source_data_crc_verified": state.crcs,
    "stage": if state.completed == state.inputs.length() {
      "final-merge"
    } else {
      "ingest"
    },
  }
}

///|
/// Indexed Chunk staging with a durable save point after each verified Chunk.
/// Committed parts persist on failure. Caller owns locking, pruning and saves.
fn resumable_chunk_merge_internal(
  sources : Array[ReadSource],
  parts_storage : SortStorage,
  merge_storage : SortStorage,
  output : WriteSink,
  fingerprint : (ReadSource) -> String raise McapError,
  save : (Bytes) -> Unit raise McapError,
  checkpoint? : Bytes,
  options? : ExternalOptions = external_options(),
  max_parts? : Int = 10000,
  cancelled? : () -> Bool = fn() { false },
  finalize? : (ChunkJobState, UInt64) -> Json raise McapError,
) -> Json raise McapError {
  let result = try {
    if sources.is_empty() ||
      sources.length() > 64 ||
      max_parts < 1 ||
      max_parts > 32768 {
      raise Invalid("invalid chunk merge inputs or part budget")
    }
    validate_log(empty_log(), limits=options.limits)
    let settings = checkpoint_settings(options) +
      max_parts.to_uint().to_le_bytes()
    let inputs = sources.map(fn(source) {
      if cancelled() {
        raise Invalid("chunk merge cancelled")
      }
      checked_digest(fingerprint(source))
    })
    let state = match checkpoint {
      None =>
        (
          {
            settings,
            inputs,
            parts: [],
            crcs: [],
            completed: 0,
            next_chunk: 0,
          } : ChunkJobState)
      Some(bytes) => {
        let state = decode_chunk_job(bytes)
        if state.settings != settings {
          raise Invalid("chunk merge options changed")
        }
        if state.inputs != inputs {
          raise Invalid("chunk merge input fingerprint/order changed")
        }
        state
      }
    }
    let mut spool = 0UL
    let mut messages = 0UL
    for i = 0; i < state.parts.length(); i = i + 1 {
      if cancelled() {
        raise Invalid("chunk merge cancelled")
      }
      let part = state.parts[i]
      let source = (parts_storage.open)(i)
      if source.size() != part.bytes ||
        checked_digest(fingerprint(source)) != part.digest {
        raise Invalid("chunk part fingerprint changed")
      }
      if part.bytes > options.spool_bytes - spool {
        raise Invalid("chunk parts exceed spool budget")
      }
      spool += part.bytes
      messages += part.messages
    }
    if messages > options.limits.max_records.to_uint64() {
      raise Invalid("chunk merge message count exceeds budget")
    }
    let sink_for = fn(id : Int) -> WriteSink raise McapError {
      if id >= max_parts {
        raise Invalid("chunk part count budget exceeded")
      }
      let raw = (parts_storage.create)(id)
      new_write_sink(fn(bytes) {
        if cancelled() {
          raise Invalid("chunk merge cancelled")
        }
        if bytes.length().to_uint64() > options.spool_bytes - spool {
          raise Invalid("chunk parts exceed spool budget")
        }
        raw.write(bytes)
        spool += bytes.length().to_uint64()
      })
    }
    for input = state.completed; input < sources.length(); input = input + 1 {
      let source = sources[input]
      let reader = open_reader(
        source,
        limits=options.limits,
        cache_bytes=0,
        summary_bytes=options.declaration_bytes,
        fallback_bytes=0,
      )
      let indexes = reader.indexes.copy()
      indexes.sort_by(fn(a, b) { a.chunk_start.compare(b.chunk_start) })
      let footer = decode_footer(source.read_exact(source.size() - 28UL, 20))
      if footer.summary_crc == 0U || reader.indexes.is_empty() {
        raise Invalid(
          "chunk checkpoint requires indexed input with nonzero summary CRC",
        )
      }
      if state.next_chunk > indexes.length() {
        raise Invalid("chunk checkpoint beyond input index")
      }
      let seed = fn(sink : WriteSink) -> StreamWriter raise McapError {
        let writer = new_stream_writer(
          sink,
          profile=reader.declarations.header.profile,
          options={ ..options.output, compression: Uncompressed, },
          limits=options.limits,
        )
        for schema in reader.declarations.schemas {
          writer.add_schema(schema)
        }
        for channel in reader.declarations.channels {
          writer.add_channel(channel)
        }
        writer
      }
      for chunk_id = state.next_chunk
          chunk_id < indexes.length()
          chunk_id = chunk_id + 1 {
        if cancelled() {
          raise Invalid("chunk merge cancelled")
        }
        let index = indexes[chunk_id]
        let frame = source.record(
          index.chunk_start,
          reader.data_end,
          options.limits,
        )
        let properties = cursor(frame.body, options.limits)
        ignore(properties.take(24))
        if properties.u32() == 0U {
          raise Invalid("chunk checkpoint requires nonzero Chunk CRC")
        }
        let chunk = reader.load_chunk(index)
        let id = state.parts.length()
        let sink = sink_for(id)
        let writer = seed(sink)
        let mut count = 0UL
        for record in chunk.records {
          match record.opcode {
            0x03 =>
              writer.add_schema(
                decode_schema(record.body, limits=options.limits),
              )
            0x04 =>
              writer.add_channel(
                decode_channel(record.body, limits=options.limits),
              )
            0x05 => {
              if messages >= options.limits.max_records.to_uint64() {
                raise Invalid("chunk merge message count exceeds budget")
              }
              writer.append(decode_message(record.body, limits=options.limits))
              messages += 1UL
              count += 1UL
            }
            _ => ()
          }
        }
        ignore(writer.finish())
        state.parts.push({
          input,
          chunk: chunk_id,
          messages: count,
          bytes: sink.position(),
          digest: checked_digest(fingerprint((parts_storage.open)(id))),
        })
        state.next_chunk = chunk_id + 1
        save(encode_chunk_job(state))
      }
      // Full original validation still gates final output, including unchunked
      // records, whole-data CRC, declarations and summary semantics.
      let id = state.parts.length()
      let sink = sink_for(id)
      let writer = seed(sink)
      let mut verified_crc = false
      let count = scan_source(
        source,
        source_visitor(
          schema=fn(value) { writer.add_schema(value) },
          channel=fn(value) { writer.add_channel(value) },
          attachment=fn(value) { writer.add_attachment(value) },
          metadata=fn(value) { writer.add_metadata(value) },
        ),
        limits=options.limits,
        declaration_bytes=options.declaration_bytes,
        cancelled~,
        trace=fn(event) {
          if event is DataValidated(_, value) {
            verified_crc = value
          }
        },
      )
      let mut staged_count = 0UL
      for part in state.parts {
        if part.input == input {
          staged_count += part.messages
        }
      }
      if count != staged_count {
        raise Invalid("chunk merge staged/source count mismatch")
      }
      ignore(writer.finish())
      state.parts.push({
        input,
        chunk: -1,
        messages: 0UL,
        bytes: sink.position(),
        digest: checked_digest(fingerprint((parts_storage.open)(id))),
      })
      state.completed = input + 1
      state.next_chunk = 0
      state.crcs.push(verified_crc)
      save(encode_chunk_job(state))
    }
    if cancelled() {
      raise Invalid("chunk merge cancelled")
    }
    if spool >= options.spool_bytes {
      raise Invalid("chunk merge has no space left for final sorting")
    }
    if finalize is Some(callback) {
      Ok(callback(state, spool))
    } else {
      let sources = Array::makei(state.parts.length(), fn(i) {
        (parts_storage.open)(i)
      })
      let temporary = sort_storage(
        merge_storage.create,
        merge_storage.open,
        merge_storage.remove,
      )
      let report = external_merge(
        sources,
        buffered_sort_storage(temporary, 65536),
        output,
        options={ ..options, spool_bytes: options.spool_bytes - spool, },
        cancelled~,
      )
      let value : Json = {
        "engine": "indexed-chunk-ingest",
        "input_files": inputs.length(),
        "committed_parts": state.parts.length(),
        "messages": report.messages.to_string(),
        "retained_part_bytes": spool.to_string(),
        "source_data_crc_verified": state.crcs,
        "source_complete": true,
        "final_merge": external_report_json(report),
      }
      Ok(value)
    }
  } catch {
    error => Err(error)
  }
  let cleanup = try {
    (merge_storage.finish)()
    None
  } catch {
    Invalid(reason) => Some(reason)
  }
  match result {
    Err(Invalid(reason)) => {
      if cleanup is Some(detail) {
        raise Invalid("\{reason}; cleanup failed: \{detail}")
      }
      raise Invalid(reason)
    }
    Ok(report) => {
      if cleanup is Some(reason) {
        raise Invalid("chunk merge cleanup failed: \{reason}")
      }
      report
    }
  }
}
