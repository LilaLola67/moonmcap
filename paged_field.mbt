///|
pub(all) struct FieldPageLayout {
  header_bytes : Int
  page_bytes : Int
  pages : Int
} derive(Eq, @debug.Debug)

///|
pub struct PagedFieldHeader {
  template : FieldIndex
  layout : FieldPageLayout
}

///|
pub struct FieldPage {
  index : FieldIndex
  blooms : Map[UInt64, Array[Bytes]]
  first : UInt64
  end : UInt64
}

///|
fn field_bloom_positions(text : String) -> Array[Int] {
  let bytes = @utf8.encode(text[:])
  let a = crc32(bytes)
  let b = crc32(b"MFLD2-string\x00" + bytes) | 1U
  [0, 1, 2, 3].map(fn(i) { ((a + b * i.to_uint()) & 255U).to_int() })
}

///|
fn field_bloom_add(bits : Array[Byte], text : String) -> Unit {
  for bit in field_bloom_positions(text) {
    bits[bit / 8] = (bits[bit / 8].to_int() | (1 << (bit % 8))).to_byte()
  }
}

///|
fn JsonCondition::may_match_bloom(
  self : JsonCondition,
  paths : Array[Array[String]],
  blooms : Array[Bytes],
) -> Bool {
  match self {
    All(items) => items.all(fn(item) { item.may_match_bloom(paths, blooms) })
    Any(items) => items.any(fn(item) { item.may_match_bloom(paths, blooms) })
    Compare(path, "=", String(text)) => {
      for i = 0; i < paths.length(); i = i + 1 {
        if paths[i] == path && blooms[i].length() == 32 {
          return field_bloom_positions(text).all(fn(bit) {
            (blooms[i][bit / 8].to_int() & (1 << (bit % 8))) != 0
          })
        }
      }
      true
    }
    _ => true
  }
}

///|
pub fn PagedFieldHeader::layout(self : PagedFieldHeader) -> FieldPageLayout {
  self.layout
}

///|
pub fn FieldPage::first_offset(self : FieldPage) -> UInt64 {
  self.first
}

///|
pub fn FieldPage::end_offset(self : FieldPage) -> UInt64 {
  self.end
}

///|
pub fn FieldPage::may_match(
  self : FieldPage,
  offset : UInt64,
  span : Int,
  filter : JsonFilter,
) -> Bool raise McapError {
  let check = self.index.location_filter(filter)
  if !check(offset, span) {
    return false
  }
  match (self.index.zones.get(offset), self.blooms.get(offset)) {
    (Some(zone), Some(blooms)) if !zone.unknown && zone.span == span =>
      filter.condition.may_match_bloom(
        self.index.paths.map(fn(p) { json_pointer(p) }),
        blooms,
      )
    _ => true
  }
}

///|
/// Header bytes consist of the 12-byte prefix followed by JSON and its CRC.
pub fn open_paged_field_header(
  source_size : UInt64,
  index_size : UInt64,
  bytes : Bytes,
  digest : String,
  decoder_identity : String,
) -> PagedFieldHeader raise McapError {
  if bytes.length() < 16 ||
    bytes.length() > 4096 ||
    bytes[0:8].to_owned() != b"MFLD2\r\n\x00" {
    raise Invalid("invalid paged field header")
  }
  let length = cursor(bytes[8:12].to_owned(), default_limits()).u32().to_int()
  if length < 0 || length != bytes.length() - 16 {
    raise Invalid("paged field header length mismatch")
  }
  let data = bytes[12:12 + length].to_owned()
  if crc32(data) !=
    cursor(bytes[12 + length:].to_owned(), default_limits()).u32() {
    raise Invalid("paged field header CRC mismatch")
  }
  let object = field_object(parse_json_document(data, 4096, 16))
  let page_bytes = bounded_length(
    parse_decimal(field_get(object, "page_bytes").stringify()),
    1024 * 1024,
    "field page",
  )
  if page_bytes < 4096 ||
    index_size < bytes.length().to_uint64() ||
    (index_size - bytes.length().to_uint64()) % page_bytes.to_uint64() != 0UL {
    raise Invalid("paged field file layout mismatch")
  }
  let count = (index_size - bytes.length().to_uint64()) / page_bytes.to_uint64()
  if count > 2147483647UL || field_get(object, "version") != Json::number(2) {
    raise Invalid("invalid paged field version or page count")
  }
  let size = parse_decimal(field_text(field_get(object, "source_size")))
  let identity = field_text(field_get(object, "decoder_identity"))
  let hash = checked_digest(field_text(field_get(object, "source_sha256")))
  let paths = field_array(field_get(object, "paths")).map(fn(v) {
    field_text(v)
  })
  if size != source_size ||
    identity != decoder_identity ||
    identity == "" ||
    hash != checked_digest(digest) ||
    paths.is_empty() ||
    paths.length() > 16 {
    raise Invalid("paged field identity mismatch")
  }
  let seen : Map[String, Bool] = Map([])
  for path in paths {
    ignore(json_pointer(path))
    if seen.contains(path) {
      raise Invalid("duplicate paged field path")
    }
    seen[path] = true
  }
  {
    template: {
      size,
      digest: hash,
      decoder_identity: identity,
      paths,
      zones: Map([]),
    },
    layout: { header_bytes: bytes.length(), page_bytes, pages: count.to_int(), },
  }
}

///|
pub fn PagedFieldHeader::decode_page(
  self : PagedFieldHeader,
  bytes : Bytes,
) -> FieldPage raise McapError {
  let size = self.layout.page_bytes
  if bytes.length() != size ||
    crc32(bytes[0:size - 4].to_owned()) !=
    cursor(bytes[size - 4:].to_owned(), default_limits()).u32() {
    raise Invalid("field page length or CRC mismatch")
  }
  let wire = cursor(bytes, { ..default_limits(), max_record_bytes: size, })
  let first = wire.u64()
  let end = wire.u64()
  let index_length = bounded_length(
    wire.u32().to_uint64(),
    size - 28,
    "field page index",
  )
  let bloom_length = bounded_length(
    wire.u32().to_uint64(),
    size - 28 - index_length,
    "field page bloom",
  )
  let template = self.template
  let mirror = new_read_source(template.size, fn(_, _) {
    raise Invalid("unexpected paged field source read")
  })
  let index = open_field_index(
    mirror,
    wire.take(index_length),
    fn(_) { template.digest },
    template.decoder_identity,
    max_bytes=size,
    max_zones=1024,
  )
  if index.paths != template.paths || index.zones.is_empty() {
    raise Invalid("invalid field page paths or empty page")
  }
  let offsets = index.zones.keys().to_array()
  offsets.sort()
  let last = offsets[offsets.length() - 1]
  if first != offsets[0] || end != last + index.zones[last].span.to_uint64() {
    raise Invalid("field page bounds mismatch")
  }
  let rows = field_array(parse_json_document(wire.take(bloom_length), size, 16))
  if rows.length() != offsets.length() {
    raise Invalid("field page bloom row count mismatch")
  }
  let blooms : Map[UInt64, Array[Bytes]] = Map([])
  for i = 0; i < rows.length(); i = i + 1 {
    let row = field_array(rows[i])
    if row.length() != template.paths.length() {
      raise Invalid("field page bloom column mismatch")
    }
    blooms[offsets[i]] = row.map(fn(v) {
      let bits = decode_hex(field_text(v), max_bytes=32)
      if bits.length() != 0 && bits.length() != 32 {
        raise Invalid("invalid field bloom size")
      }
      bits
    })
  }
  for i = wire.pos; i < size - 4; i = i + 1 {
    if bytes[i] != 0x00 {
      raise Invalid("nonzero field page padding")
    }
  }
  { index, blooms, first, end, }
}

///|
/// Streaming physical-order build: one region and one fixed-size page in memory.
/// Output must be staged by the caller, since source validation finishes last.
pub fn build_paged_field_index(
  source : ReadSource,
  sink : WriteSink,
  paths : Array[String],
  fingerprint : (ReadSource) -> String raise McapError,
  decoder_identity : String,
  page_bytes? : Int = 65536,
  page_zones? : Int = 32,
  bloom? : Bool = true,
  limits? : Limits = default_limits(),
  cancelled? : () -> Bool = fn() { false },
) -> Json raise McapError {
  if page_bytes < 4096 ||
    page_bytes > 1024 * 1024 ||
    page_zones < 1 ||
    page_zones > 1024 ||
    paths.is_empty() ||
    paths.length() > 16 ||
    decoder_identity == "" ||
    decoder_identity.length() > 256 {
    raise Invalid("invalid paged field settings")
  }
  let pointers = paths.map(fn(p) { json_pointer(p) })
  let seen : Map[String, Bool] = Map([])
  for p in paths {
    if seen.contains(p) {
      raise Invalid("duplicate field path")
    }
    seen[p] = true
  }
  let digest = checked_digest(fingerprint(source))
  let header : Json = {
    "version": 2,
    "page_bytes": page_bytes,
    "source_size": source.size().to_string(),
    "source_sha256": digest,
    "decoder_identity": decoder_identity,
    "paths": paths,
  }
  let data = @utf8.encode(header.stringify()[:])
  if data.length() > 4080 {
    raise Invalid("field header exceeds budget")
  }
  sink.write(
    b"MFLD2\r\n\x00" +
    data.length().to_uint().to_le_bytes() +
    data +
    crc32(data).to_le_bytes(),
  )
  let page_index : FieldIndex = {
    size: source.size(),
    digest,
    decoder_identity,
    paths: paths.copy(),
    zones: Map([]),
  }
  let page_blooms : Map[UInt64, Array[Array[Byte]]] = Map([])
  let current : Array[(UInt64, FieldZone, Array[Array[Byte]])?] = [None]
  let counts = [0, 0, 0] // regions, pages, peak estimated region state
  let write_page = fn() -> Unit raise McapError {
    if page_index.zones.is_empty() {
      return
    }
    let offsets = page_index.zones.keys().to_array()
    offsets.sort()
    let bytes = page_index.to_bytes(max_bytes=page_bytes)
    let rows : Json = Json::array(
      offsets.map(fn(o) {
        Json::array(
          page_blooms[o].map(fn(bits) {
            Json::string(encode_hex(Bytes::from_array(bits)))
          }),
        )
      }),
    )
    let blooms = @utf8.encode(rows.stringify()[:])
    if bytes.length() + blooms.length() > page_bytes - 28 {
      raise Invalid(
        "field page exceeds byte budget; lower page_zones or increase page_bytes",
      )
    }
    let buffer = @buffer.Buffer::Buffer()
    buffer.write_uint64_le(offsets[0])
    let last = offsets[offsets.length() - 1]
    buffer.write_uint64_le(last + page_index.zones[last].span.to_uint64())
    buffer.write_uint_le(bytes.length().to_uint())
    buffer.write_uint_le(blooms.length().to_uint())
    buffer.write_bytes(bytes[:])
    buffer.write_bytes(blooms[:])
    while buffer.length() < page_bytes - 4 {
      buffer.write_byte(0x00)
    }
    buffer.write_uint_le(crc32(buffer.to_bytes()))
    sink.write(buffer.to_bytes())
    page_index.zones.clear()
    page_blooms.clear()
    counts[1] += 1
  }
  let flush_region = fn() -> Unit raise McapError {
    if current[0] is Some((offset, zone, blooms)) {
      if page_index.zones.length() == page_zones {
        write_page()
      }
      page_index.zones[offset] = zone
      page_blooms[offset] = blooms
      counts[0] += 1
      current[0] = None
    }
  }
  let declarations = empty_log()
  let decoder = standard_payload_decoder()
  ignore(
    scan_source(
      source,
      source_visitor(
        schema=fn(s) {
          if !declarations.schemas.any(fn(old) { old.id == s.id }) {
            declarations.schemas.push(s)
          }
        },
        channel=fn(c) {
          if !declarations.channels.any(fn(old) { old.id == c.id }) {
            declarations.channels.push(c)
          }
        },
        message=fn(message, location) {
          if current[0] is Some((offset, _, _)) && offset != location.offset {
            flush_region()
          }
          if current[0] is None {
            current[0] = Some(
              (
                location.offset,
                {
                  span: location.span,
                  fields: paths.map(fn(_) { field_bounds() }),
                  unknown: false,
                },
                paths.map(fn(_) {
                  if bloom {
                    Array::make(32, 0x00)
                  } else {
                    []
                  }
                }),
              ),
            )
          }
          let (_, zone, blooms) = current[0].unwrap()
          if zone.span != location.span {
            raise Invalid("field region span changed")
          }
          let channel = declarations.channels
            .iter()
            .find_first(fn(c) { c.id == message.channel_id })
            .unwrap()
          let schema = declarations.schemas
            .iter()
            .find_first(fn(s) { s.id == channel.schema_id })
          try {
            let value = (decoder.decode_payload)(schema, channel, message.data)
            for i = 0; i < pointers.length(); i = i + 1 {
              if pointer_value(value, pointers[i]) is Some(v) {
                zone.fields[i].add(v)
                if bloom && v is String(text) {
                  field_bloom_add(blooms[i], text)
                }
              }
            }
          } catch {
            _ => zone.unknown = true
          }
          counts[2] = counts[2].max(zone.charge())
          if zone.charge() > page_bytes / page_zones {
            raise Invalid("field region summary exceeds bounded page state")
          }
        },
      ),
      limits~,
      cancelled~,
    ),
  )
  if cancelled() {
    raise Invalid("paged field build cancelled")
  }
  flush_region()
  write_page()
  {
    "version": 2,
    "zones": counts[0],
    "pages": counts[1],
    "page_bytes": page_bytes,
    "peak_region_state_bytes": counts[2],
    "bloom": bloom,
    "source_sha256": digest,
  }
}
