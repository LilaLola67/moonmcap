///|
pub struct PayloadRewriter {
  apply : (Schema?, Channel, Bytes, Array[PayloadEdit]) -> Bytes raise McapError
}

///|
pub fn payload_rewriter(
  apply : (Schema?, Channel, Bytes, Array[PayloadEdit]) -> Bytes raise McapError,
) -> PayloadRewriter {
  { apply, }
}

///|
pub fn PayloadRewriter::rewrite(
  self : PayloadRewriter,
  schema : Schema?,
  channel : Channel,
  payload : Bytes,
  edits : Array[PayloadEdit],
) -> Bytes raise McapError {
  (self.apply)(schema, channel, payload, edits)
}

///|
fn SchemaDecoder::rewrite(
  self : SchemaDecoder,
  name : String,
  bytes : Bytes,
  edits : Array[PayloadEdit],
) -> Bytes raise McapError {
  match self {
    Proto(decoder) => decoder.rewrite(name, bytes, edits)
    Cdr(decoder) => {
      let value = decoder.decode(bytes)
      if edits.is_empty() {
        bytes
      } else {
        decoder.encode(
          edit_payload_json(value, edits),
          little_endian=bytes[1] == 0x01,
        )
      }
    }
    Ros1(decoder) => {
      let value = decoder.decode(bytes)
      if edits.is_empty() {
        bytes
      } else {
        decoder.encode(edit_payload_json(value, edits))
      }
    }
  }
}

///|
/// Typed edits retain the channel's original encoding and declarations. Cache
/// budgets account for retained schema state, not total process memory.
pub fn standard_payload_rewriter(
  options~ : ProtobufOptions = protobuf_options(),
  cdr~ : CdrOptions = cdr_options(),
  cache_bytes~ : Int32 = 8 * 1024 * 1024,
  max_schemas~ : Int32 = 64,
) -> PayloadRewriter raise McapError {
  if options.protojson || cache_bytes < 1 || max_schemas < 1 {
    raise Invalid("invalid payload rewriter options")
  }
  let entries : Array[ProtoCache] = []
  let retained = [0]
  payload_rewriter(fn(schema, channel, bytes, edits) {
    ignore(payload_edit_paths(edits))
    if channel.message_encoding == "json" {
      let original = parse_json_document(
        bytes,
        options.message_bytes,
        options.depth,
      )
      if edits.is_empty() {
        return bytes
      }
      let result = @utf8.encode(
        edit_payload_json(original, edits).stringify()[:],
      )
      ignore(parse_json_document(result, options.message_bytes, options.depth))
      return result
    }
    guard schema is Some(schema) &&
      (
        (
          schema.encoding == "protobuf" &&
          channel.message_encoding == "protobuf"
        ) ||
        (schema.encoding == "ros2msg" && channel.message_encoding == "cdr") ||
        (schema.encoding == "ros1msg" && channel.message_encoding == "ros1")
      ) else {
      raise Invalid(
        "payload rewrite requires a matching supported schema and encoding",
      )
    }
    for i = 0; i < entries.length(); i = i + 1 {
      if entries[i].name == schema.name &&
        entries[i].data == schema.data &&
        entries[i].encoding == schema.encoding {
        let entry = entries.remove(i)
        entries.push(entry)
        return entry.decoder.rewrite(schema.name, bytes, edits)
      }
    }
    let (decoder, fields) = if schema.encoding == "protobuf" {
      let d = protobuf_decoder(schema.data, options~)
      (SchemaDecoder::Proto(d), d.field_count[0])
    } else if schema.encoding == "ros1msg" {
      let d = ros1_decoder(schema.name, schema.data, options=cdr)
      (SchemaDecoder::Ros1(d), d.layout.field_count)
    } else {
      let d = cdr_decoder(schema.name, schema.data, options=cdr)
      (SchemaDecoder::Cdr(d), d.field_count)
    }
    let size = schema.data.length().to_uint64() + fields.to_uint64() * 128UL
    if size > cache_bytes.to_uint64() {
      raise Invalid("single payload schema exceeds rewriter cache budget")
    }
    let charge = size.to_int()
    while entries.length() >= max_schemas || charge > cache_bytes - retained[0] {
      retained[0] -= entries.remove(0).charge
    }
    entries.push({
      name: schema.name,
      data: schema.data,
      encoding: schema.encoding,
      decoder,
      charge,
    })
    retained[0] += charge
    decoder.rewrite(schema.name, bytes, edits)
  })
}

///|
pub fn PayloadRewriter::rewrite_message(
  self : PayloadRewriter,
  message : Message,
  declarations : Log,
  edits : Array[PayloadEdit],
) -> Message raise McapError {
  guard declarations.channels
    .iter()
    .find_first(fn(c) { c.id == message.channel_id })
    is Some(channel) else {
    raise Invalid("rewrite message channel is undeclared")
  }
  let schema = declarations.schemas
    .iter()
    .find_first(fn(s) { s.id == channel.schema_id })
  if channel.schema_id != 0 && schema is None {
    raise Invalid("rewrite schema is undeclared")
  }
  { ..message, data: self.rewrite(schema, channel, message.data, edits), }
}

///|
/// Rewrite message payloads in physical order, retaining timestamps, sequence,
/// declarations and optionally auxiliary records. Caller owns sink publication.
pub fn stream_rewrite_payload(
  source : ReadSource,
  sink : WriteSink,
  edits : Array[PayloadEdit],
  topics~ : Array[String] = [],
  rewriter? : PayloadRewriter,
  copy_auxiliary~ : Bool = true,
  options~ : StreamOptions = stream_options(),
  limits~ : Limits = default_limits(),
  cancelled~ : () -> Bool = fn() { false },
) -> Json raise McapError {
  ignore(payload_edit_paths(edits))
  let rewriter = match rewriter {
    Some(r) => r
    None => standard_payload_rewriter()
  }
  let reader = open_reader(source, limits~, fallback_bytes=0)
  let declarations = { ..empty_log(), header: reader.declarations.header, }
  let writer = new_stream_writer(
    sink,
    profile=declarations.header.profile,
    options~,
    limits~,
  )
  defer writer.cancel()
  let mut total = 0UL
  let mut changed = 0UL
  let mut rewritten = 0UL
  ignore(
    scan_source(
      source,
      source_visitor(
        schema=fn(schema) {
          declarations.schemas.push(schema)
          writer.add_schema(schema)
        },
        channel=fn(channel) {
          declarations.channels.push(channel)
          writer.add_channel(channel)
        },
        message=fn(message, _) {
          guard declarations.channels
            .iter()
            .find_first(fn(c) { c.id == message.channel_id })
            is Some(channel) else {
            raise Invalid("rewrite source has undeclared channel")
          }
          if topics.is_empty() || topics.contains(channel.topic) {
            let result = rewriter.rewrite_message(message, declarations, edits)
            if result.data != message.data {
              changed += 1UL
            }
            rewritten += 1UL
            writer.append(result)
          } else {
            writer.append(message)
          }
          total += 1UL
        },
        attachment=fn(a) { if copy_auxiliary { writer.add_attachment(a) } },
        metadata=fn(m) { if copy_auxiliary { writer.add_metadata(m) } },
      ),
      limits~,
      cancelled~,
    ),
  )
  for topic in topics {
    if !declarations.channels.any(fn(c) { c.topic == topic }) {
      raise Invalid("rewrite topic absent: " + topic)
    }
  }
  let progress = writer.finish()
  {
    "messages": total.to_string(),
    "rewritten": rewritten.to_string(),
    "changed": changed.to_string(),
    "output_bytes": progress.bytes_written.to_string(),
    "peak_chunk_bytes": progress.peak_buffered_bytes,
    "auxiliary_copied": copy_auxiliary,
    "order": "original physical order",
  }
}
