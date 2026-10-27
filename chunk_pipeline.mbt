///|
pub fn resumable_chunk_merge(
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
) -> Json raise McapError {
  resumable_chunk_merge_internal(
    sources,
    parts_storage,
    merge_storage,
    output,
    fingerprint,
    save,
    checkpoint?,
    options~,
    max_parts~,
    cancelled~,
  )
}

///|
fn encode_chunk_pipeline(chunk : Bytes, merge : Bytes) -> Bytes raise McapError {
  let buffer = @buffer.Buffer::Buffer()
  buffer.write_bytes(b"CJOB2\r\n\x00")
  for bytes in [chunk, merge] {
    buffer.write_uint_le(bytes.length().to_uint())
    buffer.write_bytes(bytes[:])
  }
  let body = buffer.to_bytes()
  if body.length() > 8 * 1024 * 1024 - 4 {
    raise Invalid("combined chunk pipeline checkpoint exceeds 8 MiB")
  }
  body + crc32(body).mcap_le_bytes()
}

///|
fn decode_chunk_pipeline(bytes : Bytes) -> (Bytes, Bytes) raise McapError {
  if bytes.length() >= 8 && bytes[0:8].to_owned() == b"CJOB1\r\n\x00" {
    ignore(decode_chunk_job(bytes))
    return (bytes, b"")
  }
  if bytes.length() < 20 ||
    bytes.length() > 8 * 1024 * 1024 ||
    bytes[0:8].to_owned() != b"CJOB2\r\n\x00" {
    raise Invalid("invalid combined chunk pipeline checkpoint")
  }
  let body = bytes[0:bytes.length() - 4].to_owned()
  if crc32(body) !=
    cursor(bytes[bytes.length() - 4:].to_owned(), default_limits()).u32() {
    raise Invalid("combined chunk checkpoint CRC mismatch")
  }
  let input = cursor(body, {
    ..default_limits(),
    max_record_bytes: 8 * 1024 * 1024,
  })
  ignore(input.take(8))
  let chunk = input.blob()
  let merge = input.blob()
  input.finish()
  let state = decode_chunk_job(chunk)
  if !merge.is_empty() {
    if state.completed != state.inputs.length() {
      raise Invalid("final merge exists before chunk ingestion completed")
    }
    if merge.length() < 8 || merge[0:8].to_owned() != b"MJOB2\r\n\x00" {
      raise Invalid("combined pipeline requires an MJOB2 final checkpoint")
    }
    ignore(checkpoint_run_ids(merge))
  }
  (chunk, merge)
}

///|
fn chunk_pipeline_run(
  id : Int,
  maximum : Int,
  run_limit : Int,
) -> Int raise McapError {
  if id >= 0 {
    if id >= run_limit {
      raise Invalid("pipeline final run ID exceeds configured limit")
    }
    maximum + id
  } else {
    if id < -65 {
      raise Invalid("pipeline final auxiliary ID outside input bound")
    }
    maximum + run_limit + (-id - 1)
  }
}

///|
fn chunk_pipeline_ids(bytes : Bytes) -> Array[Int] raise McapError {
  let (chunk, merge) = decode_chunk_pipeline(bytes)
  let state = decode_chunk_job(chunk)
  let maximum = cursor(state.settings[65:69].to_owned(), default_limits())
    .u32()
    .to_int()
  let run_limit = cursor(state.settings[12:16].to_owned(), default_limits())
    .u32()
    .to_int()
  if run_limit < 1 || run_limit > 32768 {
    raise Invalid("pipeline run limit outside configured bound")
  }
  let ids = Array::makei(state.parts.length(), fn(i) { i })
  if !merge.is_empty() {
    for id in checkpoint_run_ids(merge) {
      ids.push(chunk_pipeline_run(id, maximum, run_limit))
    }
  }
  ids
}

///|
fn chunk_pipeline_progress(bytes : Bytes) -> Json raise McapError {
  let (chunk, merge) = decode_chunk_pipeline(bytes)
  let state = decode_chunk_job(chunk)
  let final = if merge.is_empty() {
    Json::null()
  } else {
    checkpoint_progress(merge)
  }
  {
    "version": 2,
    "engine": "indexed-chunk-pipeline",
    "stage": if merge.is_empty() {
      "chunk_ingest"
    } else {
      "final_sort_merge"
    },
    "input_files": state.inputs.length(),
    "completed_inputs": state.completed,
    "committed_parts": state.parts.length(),
    "chunk": chunk_job_progress(chunk),
    "final_sort": final,
  }
}

///|
struct ChunkSegment {
  start : UInt64
  size : UInt64
  source : ReadSource
  offset : UInt64
}

///|
/// Join only validated part data sections from the same original source.
/// This virtual MCAP has no summary or data CRC, and is never published.
fn chunk_virtual_source(
  parts : Array[ReadSource],
  limits : Limits,
) -> ReadSource raise McapError {
  if parts.is_empty() {
    raise Invalid("chunk virtual source has no parts")
  }
  let first = parts[0].record(8UL, parts[0].size(), limits)
  if first.opcode != 0x01 {
    raise Invalid("chunk virtual source lacks Header")
  }
  let header = decode_header(first.body, limits~)
  let prefix = magic() +
    frame(
      0x01,
      encode_header({
        profile: header.profile,
        library: "moonmcap-chunk-pipeline-v1",
      }),
    )
  let segments : Array[ChunkSegment] = [
    {
      start: 0UL,
      size: prefix.length().to_uint64(),
      source: memory_source(prefix),
      offset: 0UL,
    },
  ]
  let mut position = prefix.length().to_uint64()
  for source in parts {
    let record = source.record(8UL, source.size(), limits)
    if record.opcode != 0x01 ||
      decode_header(record.body, limits~).profile != header.profile {
      raise Invalid("chunk virtual part profile mismatch")
    }
    let start = 17UL + record.body.length().to_uint64()
    let mut at = start
    while true {
      if at > source.size() - 9UL {
        raise Invalid("chunk virtual part lacks DataEnd")
      }
      let raw = source.read_exact(at, 9)
      let c = cursor(raw[1:].to_owned(), limits)
      let size = c.u64()
      if size > source.size() - at - 9UL {
        raise Invalid("chunk virtual part record exceeds boundary")
      }
      if raw[0] == 0x0f {
        if size != 4UL {
          raise Invalid("invalid chunk virtual DataEnd")
        }
        break
      }
      if raw[0] == 0x01 || raw[0] == 0x02 {
        raise Invalid("unexpected Header/Footer in virtual data section")
      }
      at += 9UL + size
    }
    let size = at - start
    if size > 0UL {
      segments.push({ start: position, size, source, offset: start, })
      if size > 18446744073709551615UL - position {
        raise Invalid("chunk virtual source size overflow")
      }
      position += size
    }
  }
  let suffix = frame(0x0f, b"\x00\x00\x00\x00") +
    frame(
      0x02,
      encode_footer({
        summary_start: 0UL,
        summary_offset_start: 0UL,
        summary_crc: 0U,
      }),
    ) +
    magic()
  segments.push({
    start: position,
    size: suffix.length().to_uint64(),
    source: memory_source(suffix),
    offset: 0UL,
  })
  position += suffix.length().to_uint64()
  new_read_source(position, fn(offset, length) {
    let mut low = 0
    let mut high = segments.length()
    while low + 1 < high {
      let middle = (low + high) / 2
      if segments[middle].start <= offset {
        low = middle
      } else {
        high = middle
      }
    }
    let segment = segments[low]
    if offset < segment.start || offset - segment.start >= segment.size {
      raise Invalid("chunk virtual read outside segment")
    }
    let within = offset - segment.start
    let available = segment.size - within
    let count = if length.to_uint64() > available {
      available.to_int()
    } else {
      length
    }
    segment.source.read_exact(segment.offset + within, count)
  })
}

///|
pub fn resumable_chunk_pipeline(
  sources : Array[ReadSource],
  storage : SortStorage,
  output : WriteSink,
  fingerprint : (ReadSource) -> String raise McapError,
  save : (Bytes) -> Unit raise McapError,
  checkpoint? : Bytes,
  options? : ExternalOptions = external_options(),
  max_parts? : Int = 10000,
  cancelled? : () -> Bool = fn() { false },
) -> Json raise McapError {
  if options.run_limit < 1 ||
    options.run_limit > 32768 ||
    max_parts < 1 ||
    max_parts > 32768 {
    raise Invalid("invalid combined pipeline storage bounds")
  }
  let (chunk_checkpoint, merge_checkpoint) = match checkpoint {
    None => (None, b"")
    Some(bytes) => {
      let (chunk, merge) = decode_chunk_pipeline(bytes)
      (Some(chunk), merge)
    }
  }
  let saved = [chunk_checkpoint.unwrap_or(b"")]
  let final_storage = sort_storage(
    fn(id) {
      (storage.create)(chunk_pipeline_run(id, max_parts, options.run_limit))
    },
    fn(id) {
      (storage.open)(chunk_pipeline_run(id, max_parts, options.run_limit))
    },
    fn(id) {
      (storage.remove)(chunk_pipeline_run(id, max_parts, options.run_limit))
    },
  )
  resumable_chunk_merge_internal(
    sources,
    storage,
    final_storage,
    output,
    fingerprint,
    fn(bytes) {
      saved[0] = bytes
      save(encode_chunk_pipeline(bytes, b""))
    },
    checkpoint?=chunk_checkpoint,
    options~,
    max_parts~,
    cancelled~,
    finalize=fn(state, spool) {
      saved[0] = encode_chunk_job(state)
      let virtuals = Array::makei(state.inputs.length(), fn(input) {
        let parts = []
        for i = 0; i < state.parts.length(); i = i + 1 {
          if state.parts[i].input == input {
            parts.push((storage.open)(i))
          }
        }
        chunk_virtual_source(parts, options.limits)
      })
      let report = resumable_merge(
        virtuals,
        buffered_sort_storage(final_storage, 65536),
        output,
        fingerprint,
        fn(bytes) { save(encode_chunk_pipeline(saved[0], bytes)) },
        checkpoint?=if merge_checkpoint.is_empty() {
          None
        } else {
          Some(merge_checkpoint)
        },
        options={ ..options, spool_bytes: options.spool_bytes - spool, },
        cancelled~,
      )
      {
        "engine": "indexed-chunk-pipeline",
        "input_files": sources.length(),
        "committed_parts": state.parts.length(),
        "messages": report.messages.to_string(),
        "retained_part_bytes": spool.to_string(),
        "source_data_crc_verified": state.crcs,
        "source_complete": true,
        "final_merge": external_report_json(report),
      }
    },
  )
}
