///|
fn FieldBounds::may_match(
  self : FieldBounds,
  op : String,
  expected : Json,
) -> Bool raise McapError {
  if !self.present {
    return false
  }
  if op == "exists" {
    return true
  }
  if op == "!=" || op == "contains" {
    return true
  }
  let (lower, upper) = match expected {
    Number(_, ..) => {
      guard self.number_min is Some(low) && self.number_max is Some(high) else {
        return false
      }
      (
        compare_decimal(low.stringify(), expected.stringify()),
        compare_decimal(high.stringify(), expected.stringify()),
      )
    }
    String(text) => {
      guard self.text_min is Some(low) && self.text_max is Some(high) else {
        return false
      }
      (low.compare(text), high.compare(text))
    }
    _ => return true
  }
  match op {
    "=" => lower <= 0 && upper >= 0
    ">" => upper > 0
    ">=" => upper >= 0
    "<" => lower < 0
    "<=" => lower <= 0
    _ => true
  }
}

///|
fn JsonCondition::may_match_zone(
  self : JsonCondition,
  zone : FieldZone,
  pointers : Array[Array[String]],
) -> Bool raise McapError {
  if zone.unknown {
    return true
  }
  match self {
    Constant(value) => value
    All(children) =>
      children.all(fn(child) { child.may_match_zone(zone, pointers) })
    Any(children) =>
      children.any(fn(child) { child.may_match_zone(zone, pointers) })
    Not(_) => true
    Compare(path, op, value) => {
      for i = 0; i < pointers.length(); i = i + 1 {
        if pointers[i] == path {
          return zone.fields[i].may_match(op, value)
        }
      }
      true
    }
  }
}

///|
pub struct FieldQueryPlan {
  reader : RangeReader
  filter : JsonFilter
  decoder : PayloadDecoder
  all_skipped : Bool
  total : Int
  skipped : Int
  start_bytes : UInt64
}

///|
/// Conservative physical-region predicate. Missing and differently sized regions
/// remain candidates. Always apply the exact payload predicate after pruning.
pub fn FieldIndex::location_filter(
  self : FieldIndex,
  filter : JsonFilter,
) -> ((UInt64, Int) -> Bool raise McapError) raise McapError {
  let pointers = self.paths.map(fn(path) { json_pointer(path) })
  fn(offset, span) {
    match self.zones.get(offset) {
      None => true
      Some(zone) =>
        zone.span != span || filter.condition.may_match_zone(zone, pointers)
    }
  }
}

///|
/// Caller must use the same decoder semantics identified when building the index.
pub fn RangeReader::plan_field_query(
  self : RangeReader,
  index : FieldIndex,
  filter : JsonFilter,
  fingerprint : (ReadSource) -> String raise McapError,
  decoder_identity : String,
  decoder? : PayloadDecoder,
) -> FieldQueryPlan raise McapError {
  if self.source.size() != index.size ||
    decoder_identity != index.decoder_identity ||
    checked_digest(fingerprint(self.source)) != index.digest {
    raise Invalid("field query source or decoder identity mismatch")
  }
  let decoder = match decoder {
    Some(decoder) => decoder
    None => standard_payload_decoder()
  }
  let pointers = index.paths.map(fn(path) { json_pointer(path) })
  let selected : Array[ChunkIndex] = []
  for chunk in self.indexes {
    let keep = match index.zones.get(chunk.chunk_start) {
      None => true
      Some(zone) =>
        zone.span.to_uint64() != chunk.chunk_length ||
        filter.condition.may_match_zone(zone, pointers)
    }
    if keep {
      selected.push(chunk)
    }
  }
  {
    reader: {
      ..self,
      indexes: selected,
      cache: [],
      cache_bytes: 0,
      peak_cache_bytes: 0,
      decoded_chunks: 0,
      cache_hits: 0,
      full_validation: false,
    },
    filter,
    decoder,
    all_skipped: !self.indexes.is_empty() && selected.is_empty(),
    total: self.indexes.length(),
    skipped: self.indexes.length() - selected.length(),
    start_bytes: self.source.bytes_read,
  }
}

///|
pub fn FieldQueryPlan::query_source(
  self : FieldQueryPlan,
) -> QuerySource raise McapError {
  let provider = if self.all_skipped {
    query_source(self.reader.declarations, fn(_, _) {
      message_stream(fn() { None }, fn() { () })
    })
  } else {
    self.reader.query_source()
  }
  provider.decode_payload(self.decoder).filter_json(self.filter)
}

///|
pub fn FieldQueryPlan::info(self : FieldQueryPlan) -> Json {
  {
    "total_chunks": self.total,
    "skipped_chunks": self.skipped,
    "candidate_chunks": self.total - self.skipped,
    "decoded_chunks": self.reader.decoded_chunks,
    "query_source_bytes_read": (self.reader.source.bytes_read - self.start_bytes).to_string(),
    "used_unindexed_fallback": self.total == 0,
    "source_content_verified": true,
  }
}

///|
pub fn FieldIndex::info(self : FieldIndex) -> Json {
  {
    "version": 1,
    "source_size": self.size.to_string(),
    "source_sha256": self.digest,
    "decoder_identity": self.decoder_identity,
    "paths": self.paths,
    "zones": self.zones.length(),
    "unknown_zones": self.zones
    .values()
    .filter(fn(zone) { zone.unknown })
    .count(),
  }
}
