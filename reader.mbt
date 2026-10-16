///|
/// Decode the complete file envelope. Unknown records are available to callers.
pub fn scan_file(
  bytes : Bytes,
  limits? : Limits = default_limits(),
) -> Array[Record] raise McapError {
  if bytes.length() < 16 ||
    bytes[0:8].to_owned() != magic() ||
    bytes[bytes.length() - 8:].to_owned() != magic() {
    raise Invalid("missing MCAP leading or trailing magic")
  }
  let records = decode_records(
    bytes[8:bytes.length() - 8].to_owned(),
    limits~,
    base=8,
  )
  if records.length() < 3 ||
    records[0].opcode != 0x01 ||
    records[records.length() - 1].opcode != 0x02 {
    raise Invalid("file must begin with Header and end with Footer")
  }
  let footer = decode_footer(records[records.length() - 1].body)
  let footer_offset = records[records.length() - 1].offset
  let mut data_end = -1
  for i = 0; i < records.length(); i = i + 1 {
    let record = records[i]
    if record.opcode == 0x01 && i != 0 {
      raise Invalid("duplicate Header")
    }
    if record.opcode == 0x02 && i != records.length() - 1 {
      raise Invalid("premature Footer")
    }
    if record.opcode == 0x0f {
      if data_end != -1 {
        raise Invalid("duplicate DataEnd")
      }
      let input = cursor(record.body, limits)
      let expected = input.u32()
      input.finish()
      verify_crc(
        bytes[0:record.offset].to_owned(),
        expected,
        label="data section",
      )
      data_end = i
    }
  }
  if data_end == -1 {
    raise Invalid("missing DataEnd")
  }
  let summary_minimum = records[data_end].offset + 13
  if footer.summary_start != 0UL {
    if footer.summary_start < summary_minimum.to_uint64() ||
      footer.summary_start >= footer_offset.to_uint64() {
      raise Invalid("summary offset outside summary section")
    }
    let start = footer.summary_start.to_int()
    if !records.any(fn(record) { record.offset == start }) {
      raise Invalid("summary starts inside record")
    }
    verify_crc(
      bytes[start:footer_offset + 25].to_owned(),
      footer.summary_crc,
      label="summary",
    )
  } else if footer.summary_offset_start != 0UL {
    raise Invalid("SummaryOffset without summary")
  }
  if footer.summary_offset_start != 0UL {
    if footer.summary_offset_start < footer.summary_start ||
      footer.summary_offset_start >= footer_offset.to_uint64() {
      raise Invalid("SummaryOffset outside summary")
    }
  }
  records
}

///|
fn add_schema(log : Log, value : Schema) -> Unit raise McapError {
  match log.schemas.iter().find_first(fn(schema) { schema.id == value.id }) {
    Some(old) =>
      if old != value {
        raise Invalid("conflicting schema id \{value.id}")
      }
    None => log.schemas.push(value)
  }
}

///|
fn add_channel(log : Log, value : Channel) -> Unit raise McapError {
  if value.schema_id != 0 &&
    !log.schemas.any(fn(schema) { schema.id == value.schema_id }) {
    raise Invalid("channel references undeclared schema \{value.schema_id}")
  }
  match log.channels.iter().find_first(fn(channel) { channel.id == value.id }) {
    Some(old) =>
      if old != value {
        raise Invalid("conflicting channel id \{value.id}")
      }
    None => log.channels.push(value)
  }
}

///|
fn add_message(
  log : Log,
  value : Message,
  limits : Limits,
) -> Unit raise McapError {
  if !log.channels.any(fn(channel) { channel.id == value.channel_id }) {
    raise Invalid("message references undeclared channel \{value.channel_id}")
  }
  if log.messages.length() >= limits.max_records {
    raise Invalid("message budget exceeded")
  }
  log.messages.push(value)
}

///|
pub fn read_log(
  bytes : Bytes,
  limits? : Limits = default_limits(),
) -> Log raise McapError {
  let records = scan_file(bytes, limits~)
  let header = decode_header(records[0].body, limits~)
  let log : Log = {
    header,
    schemas: [],
    channels: [],
    messages: [],
    metadata: [],
    attachments: [],
  }
  let mut in_summary = false
  let mut statistics : FileStatistics? = None
  let mut chunk_count = 0U
  for record in records {
    match record.opcode {
      0x03 => add_schema(log, decode_schema(record.body, limits~))
      0x04 => add_channel(log, decode_channel(record.body, limits~))
      0x05 => {
        if in_summary {
          raise Invalid("Message in summary section")
        }
        add_message(log, decode_message(record.body, limits~), limits)
      }
      0x06 => {
        if in_summary {
          raise Invalid("Chunk in summary section")
        }
        let chunk = decode_chunk(record.body, limits~)
        chunk_count += 1U
        for nested in chunk.records {
          match nested.opcode {
            0x03 => add_schema(log, decode_schema(nested.body, limits~))
            0x04 => add_channel(log, decode_channel(nested.body, limits~))
            0x05 =>
              add_message(log, decode_message(nested.body, limits~), limits)
            _ => ()
          }
        }
      }
      0x09 => {
        if in_summary {
          raise Invalid("Attachment in summary section")
        }
        log.attachments.push(decode_attachment(record.body, limits~))
      }
      0x0c => {
        if in_summary {
          raise Invalid("Metadata in summary section")
        }
        log.metadata.push(decode_metadata(record.body, limits~))
      }
      0x0b => {
        if !in_summary || statistics is Some(_) { raise Invalid("Statistics must appear once in summary") }
        statistics = Some(decode_file_statistics(record.body, limits~))
      }
      0x0f => in_summary = true
      _ => ()
    }
  }
  validate_log(log, limits~)
  match statistics {
    Some(value) => validate_file_statistics(log, value, chunk_count)
    None => ()
  }
  log
}
