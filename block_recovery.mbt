///|
pub(all) struct RecoveredBlock {
  start : UInt64
  end : UInt64
  messages : Int
} derive(Eq, @debug.Debug)

///|
pub(all) struct SkippedBlock {
  start : UInt64
  end : UInt64
  reason : String
} derive(Eq, @debug.Debug)

///|
pub(all) struct BlockRecoveryReport {
  recovered : Array[RecoveredBlock]
  skipped : Array[SkippedBlock]
  messages : Int
  output_bytes : UInt64
  source_complete : Bool
}

///|
/// Explicit partial extraction, never a claim that all source records survived.
/// Requires a nonzero verified summary CRC and a CRC on each accepted Chunk.
pub fn recover_blocks(
  source : ReadSource,
  sink : WriteSink,
  limits? : Limits = default_limits(),
  options? : StreamOptions = stream_options(),
  summary_bytes? : Int = 8 * 1024 * 1024,
) -> BlockRecoveryReport raise McapError {
  let reader = open_reader(
    source,
    limits~,
    cache_bytes=0,
    summary_bytes~,
    fallback_bytes=0,
  )
  let tail = source.read_exact(source.size - 37UL, 29)
  let footer = decode_footer(tail[9:29].to_owned())
  if footer.summary_crc == 0U || reader.indexes.is_empty() {
    raise Invalid(
      "independent recovery requires CRC-checked summary and indexed chunks; use prefix recovery otherwise",
    )
  }
  validate_log(reader.declarations, limits~)
  let writer = new_stream_writer(
    sink,
    profile=reader.declarations.header.profile,
    options~,
    limits~,
  )
  for schema in reader.declarations.schemas {
    writer.add_schema(schema)
  }
  for channel in reader.declarations.channels {
    writer.add_channel(channel)
  }
  let recovered = []
  let skipped = []
  let mut messages = 0
  let mut records = 0
  for index in reader.indexes {
    let end = index.chunk_start +
      index.chunk_length +
      index.message_index_length
    let candidate = try {
      let record = source.record(index.chunk_start, reader.data_end, limits)
      let properties = cursor(record.body, limits)
      ignore(properties.take(24))
      if properties.u32() == 0U {
        raise Invalid(
          "Chunk CRC absent; block cannot be independently verified",
        )
      }
      Some(reader.load_chunk(index))
    } catch {
      Invalid(reason) => {
        skipped.push({ start: index.chunk_start, end, reason, })
        None
      }
    }
    if candidate is Some(chunk) {
      if chunk.records.length() > limits.max_records - records {
        raise Invalid("recovery total record budget exceeded")
      }
      records += chunk.records.length()
      let mut count = 0
      for record in chunk.records {
        if record.opcode == 0x05 {
          writer.append(decode_message(record.body, limits~))
          count += 1
        }
      }
      messages += count
      recovered.push({ start: index.chunk_start, end, messages: count, })
    }
  }
  if recovered.is_empty() {
    raise Invalid("no independently verified chunks recovered")
  }
  ignore(writer.finish())
  {
    recovered,
    skipped,
    messages,
    output_bytes: sink.position(),
    source_complete: false,
  }
}

///|
pub fn block_recovery_json(report : BlockRecoveryReport) -> Json {
  {
    "scope": "CRC-verified indexed chunks only",
    "boundary_basis": "CRC-checked summary ChunkIndex",
    "source_complete": report.source_complete,
    "source_data_crc_checked": false,
    "omitted": [
      "unchunked messages", "attachments", "metadata", "unknown records",
    ],
    "messages": report.messages,
    "output_bytes": report.output_bytes.to_string(),
    "recovered": report.recovered.map(fn(block) -> Json {
      {
        "start": block.start.to_string(),
        "end": block.end.to_string(),
        "messages": block.messages,
      }
    }),
    "skipped": report.skipped.map(fn(block) -> Json {
      {
        "start": block.start.to_string(),
        "end": block.end.to_string(),
        "reason": block.reason,
      }
    }),
    "gap_time_and_message_counts": "unknown; summary time bounds are not proof of lost content",
  }
}
