///|
pub(all) struct ReadDiagnostics {
  read_calls : Int
  bytes_read : UInt64
  decoded_chunks : Int
  cache_hits : Int
  cache_bytes : Int
  peak_cache_bytes : Int
  indexed : Bool
  full_validation : Bool
} derive(Eq, @debug.Debug)

///|
struct CachedChunk {
  offset : UInt64
  size : Int
  chunk : Chunk
}

///|
/// Summary-checked selective reader. Unread data and DataEnd CRC are not validated.
pub struct RangeReader {
  source : ReadSource
  limits : Limits
  declarations : Log
  indexes : Array[ChunkIndex]
  cache : Array[CachedChunk]
  cache_budget : Int
  fallback_budget : Int
  data_end : UInt64
  mut cache_bytes : Int
  mut peak_cache_bytes : Int
  mut decoded_chunks : Int
  mut cache_hits : Int
  mut full_validation : Bool
}

///|
pub fn open_reader(
  source : ReadSource,
  limits? : Limits = default_limits(),
  cache_bytes? : Int = 8 * 1024 * 1024,
  summary_bytes? : Int = 8 * 1024 * 1024,
  fallback_bytes? : Int = 64 * 1024 * 1024,
) -> RangeReader raise McapError {
  validate_log(empty_log(), limits~)
  if cache_bytes < 0 ||
    summary_bytes < 0 ||
    fallback_bytes < 0 ||
    source.size < 50UL {
    raise Invalid("invalid reader budgets or file too short")
  }
  if source.read_exact(0UL, 8) != magic() {
    raise Invalid("missing leading MCAP magic")
  }
  let footer_start = source.size - 37UL
  let tail = source.read_exact(footer_start, 37)
  if tail[29:37].to_owned() != magic() {
    raise Invalid("missing trailing MCAP magic")
  }
  let footer_record = decode_records(tail[0:29].to_owned(), limits~)
  if footer_record.length() != 1 || footer_record[0].opcode != 0x02 {
    raise Invalid("invalid terminal Footer")
  }
  let footer = decode_footer(footer_record[0].body)
  let summary_start = if footer.summary_start == 0UL {
    footer_start
  } else {
    footer.summary_start
  }
  if summary_start < 21UL ||
    summary_start > footer_start ||
    (footer.summary_start != 0UL && summary_start == footer_start) {
    raise Invalid("invalid summary boundary")
  }
  let data_end = summary_start - 13UL
  let ending = source.record(data_end, summary_start, limits)
  if ending.opcode != 0x0f || ending.body.length() != 4 {
    raise Invalid("missing DataEnd immediately before summary")
  }
  let header = source.record(8UL, data_end, limits)
  if header.opcode != 0x01 {
    raise Invalid("first record must be Header")
  }
  let declarations = {
    ..empty_log(),
    header: decode_header(header.body, limits~),
  }
  let summary_size = bounded_length(
    footer_start - summary_start,
    summary_bytes,
    "summary",
  )
  let summary = source.read_exact(summary_start, summary_size)
  verify_crc(
    summary + tail[0:25].to_owned(),
    footer.summary_crc,
    label="summary",
  )
  let records = decode_records(summary, limits~)
  let indexes = []
  let groups : Map[Byte, (UInt64, UInt64)] = Map([])
  let mut previous : Byte? = None
  let mut in_offsets = false
  let offsets_seen : Map[Byte, Bool] = Map([])
  let indexed_chunks : Map[UInt64, Bool] = Map([])
  let mut statistics : FileStatistics? = None
  for record in records {
    let position = summary_start + record.offset.to_uint64()
    if footer.summary_offset_start != 0UL &&
      position >= footer.summary_offset_start {
      if !in_offsets && position != footer.summary_offset_start {
        raise Invalid("SummaryOffset pointer is not a record boundary")
      }
      in_offsets = true
      if record.opcode != 0x0e {
        raise Invalid("non-offset record in SummaryOffset section")
      }
      let group = decode_summary_offset(record.body)
      if offsets_seen.contains(group.opcode) {
        raise Invalid("duplicate SummaryOffset")
      }
      offsets_seen[group.opcode] = true
      match groups.get(group.opcode) {
        Some((start, length)) =>
          if group.start != start || group.length != length {
            raise Invalid("SummaryOffset span mismatch")
          }
        None =>
          if group.length != 0UL ||
            group.start < summary_start ||
            group.start > position {
            raise Invalid("invalid empty SummaryOffset")
          }
      }
      continue
    }
    if previous != Some(record.opcode) {
      if groups.contains(record.opcode) {
        raise Invalid("summary is not grouped")
      }
      groups[record.opcode] = (position, 0UL)
      previous = Some(record.opcode)
    }
    let group = groups[record.opcode]
    groups[record.opcode] = (
      group.0,
      group.1 + (record.body.length() + 9).to_uint64(),
    )
    match record.opcode {
      0x03 => add_schema(declarations, decode_schema(record.body, limits~))
      0x04 => add_channel(declarations, decode_channel(record.body, limits~))
      0x08 => {
        let index = decode_chunk_index(record.body, limits~)
        if indexed_chunks.contains(index.chunk_start) ||
          index.chunk_start < 8UL + (header.body.length() + 9).to_uint64() ||
          index.chunk_start > data_end ||
          index.chunk_length < 49UL ||
          index.chunk_length > data_end - index.chunk_start ||
          index.chunk_length - 9UL > limits.max_record_bytes.to_uint64() ||
          index.message_index_length >
          data_end - index.chunk_start - index.chunk_length {
          raise Invalid("ChunkIndex outside data section or duplicate")
        }
        ignore(
          bounded_length(
            index.uncompressed_size,
            limits.max_decompressed_bytes,
            "chunk expansion",
          ),
        )
        let begin = index.chunk_start + index.chunk_length
        for (id, pointer) in index.message_index_offsets {
          if !declarations.channels.any(fn(c) { c.id == id }) ||
            pointer < begin ||
            pointer >= begin + index.message_index_length {
            raise Invalid("invalid MessageIndex pointer or channel")
          }
        }
        indexed_chunks[index.chunk_start] = true
        indexes.push(index)
      }
      0x0b => {
        if statistics is Some(_) {
          raise Invalid("duplicate Statistics")
        }
        statistics = Some(decode_file_statistics(record.body, limits~))
      }
      0x0a => ignore(decode_attachment_index(record.body, limits~))
      0x0d => ignore(decode_metadata_index(record.body, limits~))
      _ => raise Invalid("record not allowed in summary")
    }
  }
  if footer.summary_offset_start != 0UL &&
    (!in_offsets || footer.summary_offset_start < summary_start) {
    raise Invalid("invalid SummaryOffset boundary")
  }
  indexes.sort_by(fn(a, b) { a.chunk_start.compare(b.chunk_start) })
  let mut end = 0UL
  for index in indexes {
    if index.chunk_start < end {
      raise Invalid("overlapping indexed chunks")
    }
    end = index.chunk_start + index.chunk_length + index.message_index_length
  }
  if statistics is Some(stats) {
    if !indexes.is_empty() &&
      stats.chunk_count.to_uint64() != indexes.length().to_uint64() {
      raise Invalid("Statistics chunk count differs from index count")
    }
  }
  {
    source,
    limits,
    declarations,
    indexes,
    cache: [],
    cache_budget: cache_bytes,
    fallback_budget: fallback_bytes,
    data_end,
    cache_bytes: 0,
    peak_cache_bytes: 0,
    decoded_chunks: 0,
    cache_hits: 0,
    full_validation: false,
  }
}

///|
fn RangeReader::load_chunk(
  self : RangeReader,
  index : ChunkIndex,
  provided? : Bytes? = None,
  expanded? : Bytes,
) -> Chunk raise McapError {
  for i = 0; i < self.cache.length(); i = i + 1 {
    if self.cache[i].offset == index.chunk_start {
      let cached = self.cache.remove(i)
      self.cache.push(cached)
      self.cache_hits += 1
      return cached.chunk
    }
  }
  let source = match provided {
    None => self.source
    Some(span) =>
      new_read_source(self.source.size, fn(offset, length) {
        if offset < index.chunk_start ||
          offset - index.chunk_start > span.length().to_uint64() {
          raise Invalid("provided chunk span does not cover requested offset")
        }
        let begin = (offset - index.chunk_start).to_int()
        if length < 0 || length > span.length() - begin {
          raise Invalid("provided chunk span is truncated")
        }
        span[begin:begin + length].to_owned()
      })
  }
  let record = source.record(index.chunk_start, self.data_end, self.limits)
  if record.opcode != 0x06 ||
    (record.body.length() + 9).to_uint64() != index.chunk_length {
    raise Invalid("ChunkIndex does not point to a matching Chunk")
  }
  let properties = cursor(record.body, self.limits)
  ignore(properties.take(16))
  let uncompressed_size = properties.u64()
  let checksum = properties.u32()
  let compression = properties.text()
  let compressed_size = properties.u64()
  if index.uncompressed_size != uncompressed_size ||
    index.compression != compression ||
    index.compressed_size != compressed_size {
    raise Invalid("ChunkIndex compression properties mismatch")
  }
  let chunk = match expanded {
    None => decode_chunk(record.body, limits=self.limits)
    Some(bytes) => {
      if self.limits.max_decompressed_bytes > 2147483607 ||
        bytes.length() > self.limits.max_decompressed_bytes + 40 {
        raise Invalid("expanded worker Chunk exceeds budget")
      }
      let metadata = cursor(bytes, {
        ..self.limits,
        max_record_bytes: self.limits.max_decompressed_bytes + 40,
      })
      ignore(metadata.take(16))
      if metadata.u64() != uncompressed_size ||
        metadata.u32() != checksum ||
        metadata.text() != "" ||
        metadata.u64() != uncompressed_size {
        raise Invalid("expanded Chunk differs from original properties")
      }
      decode_chunk_internal(
        bytes,
        self.limits,
        self.limits.max_decompressed_bytes + 40,
      )
    }
  }
  if chunk.start_time != index.start_time || chunk.end_time != index.end_time {
    raise Invalid("ChunkIndex time range mismatch")
  }
  let seen : Map[Int, Bool] = Map([])
  let mut index_length = 0UL
  for (id, pointer) in index.message_index_offsets {
    let record = source.record(
      pointer,
      index.chunk_start + index.chunk_length + index.message_index_length,
      self.limits,
    )
    if record.opcode != 0x07 {
      raise Invalid("MessageIndex pointer opcode mismatch")
    }
    let message_index = decode_message_index(record.body, limits=self.limits)
    if message_index.channel_id != id {
      raise Invalid("MessageIndex channel mismatch")
    }
    validate_message_index(message_index, chunk)
    seen[id] = true
    index_length += (record.body.length() + 9).to_uint64()
  }
  if index_length != index.message_index_length {
    raise Invalid("MessageIndex span mismatch")
  }
  for nested in chunk.records {
    match nested.opcode {
      0x03 => {
        let schema = decode_schema(nested.body, limits=self.limits)
        if !self.declarations.schemas.contains(schema) {
          raise Invalid("Chunk Schema differs from summary")
        }
      }
      0x04 => {
        let channel = decode_channel(nested.body, limits=self.limits)
        if !self.declarations.channels.contains(channel) {
          raise Invalid("Chunk Channel differs from summary")
        }
      }
      0x05 => {
        let message = decode_message(nested.body, limits=self.limits)
        if !self.declarations.channels.any(fn(c) { c.id == message.channel_id }) ||
          (
            !index.message_index_offsets.is_empty() &&
            !seen.contains(message.channel_id)
          ) {
          raise Invalid("missing Channel or MessageIndex")
        }
      }
      _ => ()
    }
  }
  self.decoded_chunks += 1
  let size = bounded_length(
    uncompressed_size,
    self.limits.max_decompressed_bytes,
    "cache entry",
  )
  if size > 0 && size <= self.cache_budget {
    while size > self.cache_budget - self.cache_bytes {
      self.cache_bytes -= self.cache.remove(0).size
    }
    self.cache.push({ offset: index.chunk_start, size, chunk, })
    self.cache_bytes += size
    self.peak_cache_bytes = self.peak_cache_bytes.max(self.cache_bytes)
  }
  chunk
}

///|
pub fn RangeReader::query(
  self : RangeReader,
  query : Query,
) -> Array[Message] raise McapError {
  if query.start_time > query.end_time || query.offset < 0 || query.limit < 0 {
    raise Invalid("invalid query range or pagination")
  }
  if query.limit == 0 {
    return []
  }
  if self.indexes.is_empty() {
    let size = bounded_length(
      self.source.size,
      self.fallback_budget,
      "unindexed fallback",
    )
    let log = read_log(self.source.read_exact(0UL, size), limits=self.limits)
    self.full_validation = true
    return build_query_index(log).query(query)
  }
  let selected : Array[(Message, UInt64, Int)] = []
  let allowed : Map[Int, Bool] = Map([])
  for channel in self.declarations.channels {
    if query.topics.is_empty() || query.topics.contains(channel.topic) {
      allowed[channel.id] = true
    }
  }
  for index in self.indexes {
    if index.end_time < query.start_time || index.start_time > query.end_time {
      continue
    }
    if !index.message_index_offsets.is_empty() &&
      !index.message_index_offsets.any(fn(pair) { allowed.contains(pair.0) }) {
      continue
    }
    let chunk = self.load_chunk(index)
    for record in chunk.records {
      if record.opcode != 0x05 {
        continue
      }
      let message = decode_message(record.body, limits=self.limits)
      if allowed.contains(message.channel_id) &&
        message.log_time >= query.start_time &&
        message.log_time <= query.end_time {
        if selected.length() >= self.limits.max_records {
          raise Invalid("query message budget exceeded")
        }
        selected.push((message, index.chunk_start, record.offset))
      }
    }
  }
  selected.sort_by(fn(a, b) {
    if a.0.log_time != b.0.log_time {
      a.0.log_time.compare(b.0.log_time)
    } else if a.1 != b.1 {
      a.1.compare(b.1)
    } else {
      a.2.compare(b.2)
    }
  })
  let begin = query.offset.min(selected.length())
  let count = query.limit.min(selected.length() - begin)
  Array::makei(count, fn(i) {
    let position = begin + i
    selected[if query.descending {
      selected.length() - 1 - position
    } else {
      position
    }].0
  })
}

///|
pub fn RangeReader::validate_all(self : RangeReader) -> Unit raise McapError {
  let size = bounded_length(
    self.source.size,
    self.fallback_budget,
    "full validation",
  )
  ignore(read_log(self.source.read_exact(0UL, size), limits=self.limits))
  self.full_validation = true
}

///|
pub fn RangeReader::diagnostics(self : RangeReader) -> ReadDiagnostics {
  {
    read_calls: self.source.calls,
    bytes_read: self.source.bytes_read,
    decoded_chunks: self.decoded_chunks,
    cache_hits: self.cache_hits,
    cache_bytes: self.cache_bytes,
    peak_cache_bytes: self.peak_cache_bytes,
    indexed: !self.indexes.is_empty(),
    full_validation: self.full_validation,
  }
}

///|
pub fn RangeReader::clear_cache(self : RangeReader) -> Unit {
  self.cache.clear()
  self.cache_bytes = 0
}

///|
pub fn RangeReader::channels(self : RangeReader) -> Array[Channel] {
  self.declarations.channels.map(fn(c) { { ..c, metadata: c.metadata.copy(), } })
}
