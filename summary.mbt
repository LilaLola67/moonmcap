///|
pub(all) struct MetadataIndex {
  offset : UInt64
  length : UInt64
  name : String
}

///|
pub fn encode_metadata_index(value : MetadataIndex) -> Bytes {
  let out = @buffer.Buffer::Buffer()
  out.write_uint64_le(value.offset)
  out.write_uint64_le(value.length)
  put_text(out, value.name)
  out.to_bytes()
}

///|
pub fn decode_metadata_index(
  bytes : Bytes,
  limits? : Limits = default_limits(),
) -> MetadataIndex raise McapError {
  let input = cursor(bytes, limits)
  let offset = input.u64()
  let length = input.u64()
  let name = input.text()
  { offset, length, name, }
}

///|
/// Check grouped summary records and every declared SummaryOffset span.
pub fn validate_summary(
  bytes : Bytes,
  limits? : Limits = default_limits(),
) -> Int raise McapError {
  let records = scan_file(bytes, limits~)
  let footer = decode_footer(records[records.length() - 1].body)
  let footer_offset = records[records.length() - 1].offset.to_uint64()
  let data_end = records.iter().find_first(fn(r) { r.opcode == 0x0f })
  let data_end = match data_end {
    Some(value) => value
    None => raise Invalid("missing DataEnd")
  }
  let summary : Array[Record] = []
  let offsets : Array[Record] = []
  for record in records {
    let position = record.offset.to_uint64()
    if position <= data_end.offset.to_uint64() || record.opcode == 0x02 {
      continue
    }
    if footer.summary_start == 0UL || position < footer.summary_start {
      raise Invalid("records between DataEnd and summary")
    }
    if footer.summary_offset_start != 0UL &&
      position >= footer.summary_offset_start {
      if record.opcode != 0x0e {
        raise Invalid("non-SummaryOffset in offset section")
      }
      offsets.push(record)
    } else {
      if record.opcode != 0x03 &&
        record.opcode != 0x04 &&
        record.opcode != 0x08 &&
        record.opcode != 0x0a &&
        record.opcode != 0x0b &&
        record.opcode != 0x0d {
        raise Invalid("record opcode not allowed in summary")
      }
      summary.push(record)
    }
  }
  if footer.summary_offset_start != 0UL &&
    !offsets.any(fn(r) { r.offset.to_uint64() == footer.summary_offset_start }) {
    raise Invalid("SummaryOffset pointer is not an offset record boundary")
  }
  let seen : Map[Byte, Bool] = {}
  let groups : Map[Byte, (UInt64, UInt64)] = {}
  let mut previous : Byte? = None
  for record in summary {
    if previous != Some(record.opcode) {
      if seen.contains(record.opcode) {
        raise Invalid("summary records not grouped by opcode")
      }
      seen[record.opcode] = true
      previous = Some(record.opcode)
      groups[record.opcode] = (record.offset.to_uint64(), 0UL)
    }
    let group = groups[record.opcode]
    groups[record.opcode] = (
      group.0,
      group.1 + (record.body.length() + 9).to_uint64(),
    )
  }
  let indexed : Map[Byte, Bool] = {}
  for record in offsets {
    let value = decode_summary_offset(record.body)
    if indexed.contains(value.opcode) {
      raise Invalid("duplicate SummaryOffset opcode")
    }
    indexed[value.opcode] = true
    if value.start > footer_offset || value.length > footer_offset - value.start {
      raise Invalid("SummaryOffset span outside summary")
    }
    match groups.get(value.opcode) {
      Some((start, length)) =>
        if value.start != start || value.length != length {
          raise Invalid("SummaryOffset span mismatch")
        }
      None =>
        if value.length != 0UL ||
          value.start < footer.summary_start ||
          value.start >
          (if footer.summary_offset_start == 0UL {
            footer_offset
          } else {
            footer.summary_offset_start
          }) {
          raise Invalid("SummaryOffset references absent nonempty group")
        }
    }
  }
  groups.length()
}

///|
pub fn validate_metadata_indexes(
  bytes : Bytes,
  limits? : Limits = default_limits(),
) -> Int raise McapError {
  let records = scan_file(bytes, limits~)
  let physical : Map[UInt64, Record] = {}
  for record in records {
    physical[record.offset.to_uint64()] = record
  }
  let seen : Map[UInt64, Bool] = {}
  let mut count = 0
  for record in records {
    if record.opcode != 0x0d {
      continue
    }
    let index = decode_metadata_index(record.body, limits~)
    if seen.contains(index.offset) {
      raise Invalid("duplicate MetadataIndex")
    }
    seen[index.offset] = true
    let target = match physical.get(index.offset) {
      Some(value) => value
      None => raise Invalid("MetadataIndex target outside record boundary")
    }
    if target.opcode != 0x0c ||
      index.length != (target.body.length() + 9).to_uint64() {
      raise Invalid("MetadataIndex type or length mismatch")
    }
    if decode_metadata(target.body, limits~).name != index.name {
      raise Invalid("MetadataIndex name mismatch")
    }
    count += 1
  }
  count
}
