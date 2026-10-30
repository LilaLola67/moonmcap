///|
pub(all) struct BlockRecoveryEvidence {
  start : UInt64
  end : UInt64
  recovered : Bool
  messages : Int32
  actual_start : UInt64?
  actual_end : UInt64?
  advertised_start : UInt64
  advertised_end : UInt64
  reason : String?
}

///|
pub(all) struct RecoveredBlock {
  start : UInt64
  end : UInt64
  messages : Int32
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
  messages : Int32
  output_bytes : UInt64
  source_complete : Bool
}

///|
/// Explicit partial extraction, never a claim that all source records survived.
/// Requires a nonzero verified summary CRC and a CRC on each accepted Chunk.
pub fn recover_blocks(
  source : ReadSource,
  sink : WriteSink,
  limits~ : Limits = default_limits(),
  options~ : StreamOptions = stream_options(),
  summary_bytes~ : Int32 = 8 * 1024 * 1024,
  on_block~ : (BlockRecoveryEvidence) -> Unit raise McapError = fn(_) { () },
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
        on_block({
          start: index.chunk_start,
          end,
          recovered: false,
          messages: 0,
          actual_start: None,
          actual_end: None,
          advertised_start: index.start_time,
          advertised_end: index.end_time,
          reason: Some(reason),
        })
        None
      }
    }
    if candidate is Some(chunk) {
      if chunk.records.length() > limits.max_records - records {
        raise Invalid("recovery total record budget exceeded")
      }
      records += chunk.records.length()
      let mut count = 0
      let mut first : UInt64? = None
      let mut last : UInt64? = None
      for record in chunk.records {
        if record.opcode == 0x05 {
          let message = decode_message(record.body, limits~)
          writer.append(message)
          first = Some(
            match first {
              Some(value) =>
                if value < message.log_time {
                  value
                } else {
                  message.log_time
                }
              None => message.log_time
            },
          )
          last = Some(
            match last {
              Some(value) =>
                if value > message.log_time {
                  value
                } else {
                  message.log_time
                }
              None => message.log_time
            },
          )
          count += 1
        }
      }
      messages += count
      recovered.push({ start: index.chunk_start, end, messages: count, })
      on_block({
        start: index.chunk_start,
        end,
        recovered: true,
        messages: count,
        actual_start: first,
        actual_end: last,
        advertised_start: index.start_time,
        advertised_end: index.end_time,
        reason: None,
      })
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
pub fn recover_blocks_detailed(
  source : ReadSource,
  sink : WriteSink,
  limits~ : Limits = default_limits(),
  options~ : StreamOptions = stream_options(),
  summary_bytes~ : Int32 = 8 * 1024 * 1024,
  max_ranges~ : Int32 = 10000,
) -> Json raise McapError {
  if max_ranges <= 0 || limits.max_record_bytes > 2147483638 {
    raise Invalid("invalid recovery range or record budget")
  }
  let evidence : Array[Json] = []
  let report = recover_blocks(source, sink, limits~, options~, summary_bytes~, on_block=fn(
    block,
  ) {
    if evidence.length() >= max_ranges {
      raise Invalid("recovery evidence range budget exceeded")
    }
    evidence.push({
      "start": block.start.to_string(),
      "end": block.end.to_string(),
      "recovered": block.recovered,
      "recovered_messages": block.messages,
      "actual_time_start": evidence_time(block.actual_start),
      "actual_time_end": evidence_time(block.actual_end),
      "advertised_time_start": block.advertised_start.to_string(),
      "advertised_time_end": block.advertised_end.to_string(),
      "unknown_lost_messages": Json::null(),
      "reason": match block.reason {
        Some(value) => Json::string(value)
        None => Json::null()
      },
    })
  })
  {
    "summary": block_recovery_json(report),
    "ranges": evidence,
    "advertised_time_basis": "CRC-checked ChunkIndex claims; not proof of exact missing times or counts",
    "actual_time_basis": "decoded messages in independently verified chunks",
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
