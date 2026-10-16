///|
fn append_group(
  out : @buffer.Buffer,
  opcode : Byte,
  bodies : Array[Bytes],
  groups : Array[SummaryOffset],
) -> Unit raise McapError {
  if bodies.is_empty() {
    return
  }
  let start = out.length()
  for body in bodies {
    out.write_bytes(frame(opcode, body)[:])
  }
  groups.push({
    opcode,
    start: start.to_uint64(),
    length: (out.length() - start).to_uint64(),
  })
}

///|
/// Serialize MessageIndex, ChunkIndex, grouped summary and SummaryOffset records.
pub fn write_indexed(
  log : Log,
  messages_per_chunk? : Int = 256,
  limits? : Limits = default_limits(),
) -> Bytes raise McapError {
  let sequential = write_chunked(log, messages_per_chunk~, limits~)
  let records = scan_file(sequential, limits~)
  let out = @buffer.Buffer::Buffer()
  out.write_bytes(magic()[:])
  let indexes : Array[ChunkIndex] = []
  let attachments : Array[Bytes] = []
  let metadata : Array[Bytes] = []
  for record in records {
    if record.opcode == 0x0f || record.opcode == 0x02 {
      continue
    }
    let offset = out.length()
    let framed = frame(record.opcode, record.body)
    out.write_bytes(framed[:])
    if record.opcode == 0x09 {
      let value = decode_attachment(record.body, limits~)
      attachments.push(
        encode_attachment_index({
          offset: offset.to_uint64(),
          length: framed.length().to_uint64(),
          log_time: value.log_time,
          create_time: value.create_time,
          data_size: value.data.length().to_uint64(),
          name: value.name,
          media_type: value.media_type,
        }),
      )
    }
    if record.opcode == 0x0c {
      let value = decode_metadata(record.body, limits~)
      metadata.push(
        encode_metadata_index({
          offset: offset.to_uint64(),
          length: framed.length().to_uint64(),
          name: value.name,
        }),
      )
    }
    if record.opcode != 0x06 {
      continue
    }
    let chunk = decode_chunk(record.body, limits~)
    let entries : Map[Int, Array[(UInt64, UInt64)]] = {}
    let mut uncompressed_size = 0UL
    for nested in chunk.records {
      uncompressed_size += (nested.body.length() + 9).to_uint64()
      if nested.opcode == 0x05 {
        let message = decode_message(nested.body, limits~)
        let pair = (message.log_time, nested.offset.to_uint64())
        match entries.get(message.channel_id) {
          Some(values) => values.push(pair)
          None => entries[message.channel_id] = [pair]
        }
      }
    }
    let channel_ids = entries.keys().to_array()
    channel_ids.sort()
    let message_index_start = out.length()
    let message_index_offsets = []
    for id in channel_ids {
      message_index_offsets.push((id, out.length().to_uint64()))
      out.write_bytes(
        frame(
          0x07,
          encode_message_index({ channel_id: id, entries: entries[id], }),
        )[:],
      )
    }
    indexes.push({
      start_time: chunk.start_time,
      end_time: chunk.end_time,
      chunk_start: offset.to_uint64(),
      chunk_length: framed.length().to_uint64(),
      message_index_offsets,
      message_index_length: (out.length() - message_index_start).to_uint64(),
      compression: "",
      compressed_size: uncompressed_size,
      uncompressed_size,
    })
  }
  out.write_bytes(frame(0x0f, crc32(out.to_bytes()).to_le_bytes())[:])
  let summary_start = out.length()
  let groups = []
  let schema_bodies = []
  for schema in log.schemas {
    schema_bodies.push(encode_schema(schema))
  }
  append_group(out, 0x03, schema_bodies, groups)
  let channel_bodies = []
  for channel in log.channels {
    channel_bodies.push(encode_channel(channel))
  }
  append_group(out, 0x04, channel_bodies, groups)
  let chunk_bodies = []
  for index in indexes {
    chunk_bodies.push(encode_chunk_index(index))
  }
  append_group(out, 0x08, chunk_bodies, groups)
  append_group(out, 0x0a, attachments, groups)
  append_group(out, 0x0b, [encode_file_statistics(compute_file_statistics(log, indexes.length().to_uint()))], groups)
  append_group(out, 0x0d, metadata, groups)
  let offset_start = out.length()
  for group in groups {
    out.write_bytes(frame(0x0e, encode_summary_offset(group))[:])
  }
  let footer : Footer = {
    summary_start: if groups.is_empty() {
      0UL
    } else {
      summary_start.to_uint64()
    },
    summary_offset_start: if groups.is_empty() {
      0UL
    } else {
      offset_start.to_uint64()
    },
    summary_crc: 0U,
  }
  let provisional = frame(0x02, encode_footer(footer))
  let crc = if groups.is_empty() {
    0U
  } else {
    crc32(
      out.to_bytes()[summary_start:].to_owned() + provisional[0:25].to_owned(),
    )
  }
  out.write_bytes(
    frame(0x02, encode_footer({ ..footer, summary_crc: crc, }))[:],
  )
  out.write_bytes(magic()[:])
  let bytes = out.to_bytes()
  ignore(scan_file(bytes, limits~))
  bytes
}

///|
/// Check physical index pointers against record boundaries and chunk contents.
pub fn validate_file_indexes(
  bytes : Bytes,
  limits? : Limits = default_limits(),
) -> Array[ChunkIndex] raise McapError {
  let records = scan_file(bytes, limits~)
  let by_offset : Map[UInt64, Record] = {}
  for record in records {
    by_offset[record.offset.to_uint64()] = record
  }
  let result = []
  let indexed_chunks : Map[UInt64, Bool] = {}
  for record in records {
    if record.opcode != 0x08 {
      continue
    }
    let index = decode_chunk_index(record.body, limits~)
    if indexed_chunks.contains(index.chunk_start) {
      raise Invalid("duplicate ChunkIndex")
    }
    indexed_chunks[index.chunk_start] = true
    let chunk_record = match by_offset.get(index.chunk_start) {
      Some(value) => value
      None => raise Invalid("ChunkIndex points outside a record boundary")
    }
    if chunk_record.opcode != 0x06 ||
      index.chunk_length != (chunk_record.body.length() + 9).to_uint64() {
      raise Invalid("ChunkIndex type or length mismatch")
    }
    let chunk = decode_chunk(chunk_record.body, limits~)
    if chunk.start_time != index.start_time ||
      chunk.end_time != index.end_time ||
      index.compression != "" {
      raise Invalid("ChunkIndex properties disagree with Chunk")
    }
    let mut payload_size = 0UL
    for nested in chunk.records {
      payload_size += (nested.body.length() + 9).to_uint64()
    }
    if index.compressed_size != payload_size ||
      index.uncompressed_size != payload_size {
      raise Invalid("ChunkIndex payload sizes disagree with Chunk")
    }
    let mut total_index_length = 0UL
    let channel_ids : Map[Int, Bool] = {}
    for pair in index.message_index_offsets {
      let message_record = match by_offset.get(pair.1) {
        Some(value) => value
        None => raise Invalid("MessageIndex pointer is not a record boundary")
      }
      if message_record.opcode != 0x07 {
        raise Invalid("MessageIndex pointer has wrong opcode")
      }
      let index_start = index.chunk_start + index.chunk_length
      if index.message_index_length > bytes.length().to_uint64() - index_start {
        raise Invalid("MessageIndex span exceeds file")
      }
      if pair.1 < index_start ||
        pair.1 >= index_start + index.message_index_length {
        raise Invalid("MessageIndex precedes its Chunk")
      }
      let parsed = decode_message_index(message_record.body, limits~)
      if parsed.channel_id != pair.0 {
        raise Invalid("ChunkIndex channel differs from MessageIndex")
      }
      validate_message_index(parsed, chunk)
      channel_ids[pair.0] = true
      total_index_length += (message_record.body.length() + 9).to_uint64()
    }
    if total_index_length != index.message_index_length {
      raise Invalid("MessageIndex total length mismatch")
    }
    for nested in chunk.records {
      if nested.opcode == 0x05 {
        let message = decode_message(nested.body, limits~)
        if !index.message_index_offsets.is_empty() &&
          !channel_ids.contains(message.channel_id) {
          raise Invalid("missing channel MessageIndex")
        }
      }
    }
    result.push(index)
  }
  if !result.is_empty() {
    for record in records {
      if record.opcode == 0x06 &&
        !indexed_chunks.contains(record.offset.to_uint64()) {
        raise Invalid("missing ChunkIndex")
      }
      if record.opcode == 0x05 {
        raise Invalid("indexed file contains unchunked Message")
      }
    }
  }
  result
}
