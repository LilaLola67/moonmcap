///|
pub struct PayloadDecoder {
  decode_payload : (Schema?, Channel, Bytes) -> Json raise McapError
}

///|
pub fn payload_decoder(
  decode : (Schema?, Channel, Bytes) -> Json raise McapError,
) -> PayloadDecoder {
  { decode_payload: decode, }
}

///|
enum SchemaDecoder {
  Proto(ProtobufDecoder)
  Cdr(CdrDecoder)
  Ros1(Ros1Decoder)
}

///|
fn SchemaDecoder::decode(
  self : SchemaDecoder,
  name : String,
  bytes : Bytes,
  selection? : DecodeSelection,
) -> Json raise McapError {
  match self {
    Proto(decoder) =>
      match selection {
        None => decoder.decode(name, bytes)
        Some(s) => decoder.decode_selected(name, bytes, s)
      }
    Cdr(decoder) =>
      match selection {
        None => decoder.decode(bytes)
        Some(s) => decoder.view(bytes, selection=s).json()
      }
    Ros1(decoder) =>
      match selection {
        None => decoder.decode(bytes)
        Some(s) => decoder.view(bytes, selection=s).json()
      }
  }
}

///|
struct ProtoCache {
  name : String
  data : Bytes
  encoding : String
  decoder : SchemaDecoder
  charge : Int
}

///|
/// Schema identity includes descriptor bytes, not just a source-local schema ID.
pub fn standard_payload_decoder(
  options? : ProtobufOptions = protobuf_options(),
  cdr? : CdrOptions = cdr_options(),
  cache_bytes? : Int = 8 * 1024 * 1024,
  max_schemas? : Int = 64,
  selection? : DecodeSelection,
) -> PayloadDecoder raise McapError {
  if cache_bytes < 1 || max_schemas < 1 {
    raise Invalid("invalid payload decoder cache budget")
  }
  let entries : Array[ProtoCache] = []
  if selection is Some(s) {
    ignore(selection_paths(s))
  }
  let retained = [0]
  payload_decoder(fn(schema, channel, bytes) {
    if channel.message_encoding == "json" {
      let value = parse_json_document(
        bytes,
        options.message_bytes,
        options.depth,
      )
      return match selection {
        None => value
        Some(s) => selected_json(value, selection_paths(s), [])
      }
    }
    if channel.message_encoding != "protobuf" &&
      channel.message_encoding != "cdr" &&
      channel.message_encoding != "ros1" {
      raise Invalid(
        "no decoder for message encoding: \{channel.message_encoding}",
      )
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
        "payload channel requires a matching protobuf or ros2msg schema",
      )
    }
    for i = 0; i < entries.length(); i = i + 1 {
      if entries[i].name == schema.name &&
        entries[i].data == schema.data &&
        entries[i].encoding == schema.encoding {
        let entry = entries.remove(i)
        entries.push(entry)
        return entry.decoder.decode(schema.name, bytes, selection?)
      }
    }
    let (decoder, fields) = if schema.encoding == "protobuf" {
      let decoder = protobuf_decoder(schema.data, options~)
      (SchemaDecoder::Proto(decoder), decoder.field_count[0])
    } else if schema.encoding == "ros1msg" {
      let decoder = ros1_decoder(schema.name, schema.data, options=cdr)
      (SchemaDecoder::Ros1(decoder), decoder.layout.field_count)
    } else {
      let decoder = cdr_decoder(schema.name, schema.data, options=cdr)
      (SchemaDecoder::Cdr(decoder), decoder.field_count)
    }
    let size = schema.data.length().to_uint64() + fields.to_uint64() * 128UL
    if size > cache_bytes.to_uint64() {
      raise Invalid("single payload schema exceeds decoder cache budget")
    }
    let charge = size.to_int()
    while entries.length() >= max_schemas || charge > cache_bytes - retained[0] {
      retained[0] -= entries.remove(0).charge
    }
    entries.push({
      name: schema.name,
      encoding: schema.encoding,
      data: schema.data,
      decoder,
      charge,
    })
    retained[0] += charge
    decoder.decode(schema.name, bytes, selection?)
  })
}

///|
pub fn PayloadDecoder::decode_message(
  self : PayloadDecoder,
  message : Message,
  declarations : Log,
  max_bytes? : Int = 1024 * 1024,
) -> Message raise McapError {
  if max_bytes < 1 {
    raise Invalid("invalid decoded JSON byte budget")
  }
  let channel = match
    declarations.channels
    .iter()
    .find_first(fn(c) { c.id == message.channel_id }) {
    Some(c) => c
    None => raise Invalid("decoded message channel is undeclared")
  }
  let schema = if channel.schema_id == 0 {
    None
  } else {
    declarations.schemas.iter().find_first(fn(s) { s.id == channel.schema_id })
  }
  if channel.schema_id != 0 && schema is None {
    raise Invalid("decoded message schema is undeclared")
  }
  let json = (self.decode_payload)(schema, channel, message.data)
  let data = @utf8.encode(json.stringify()[:])
  if data.length() > max_bytes {
    raise Invalid("decoded JSON exceeds byte budget")
  }
  { ..message, data, }
}

///|
pub fn QuerySource::decode_payload(
  self : QuerySource,
  decoder : PayloadDecoder,
  max_bytes? : Int = 1024 * 1024,
) -> QuerySource raise McapError {
  if max_bytes < 1 {
    raise Invalid("invalid decoded JSON byte budget")
  }
  let declarations = {
    ..copy_log(self.declarations),
    schemas: [],
    channels: self.declarations.channels.map(fn(c) {
      { ..c, schema_id: 0, message_encoding: "json", }
    }),
  }
  query_source(
    declarations,
    fn(query, budget) {
      let stream = (self.open)(query, budget)
      message_stream(
        fn() {
          try {
            match (stream.next_message)() {
              None => {
                (stream.stop)()
                None
              }
              Some(message) => {
                let decoded = decoder.decode_message(
                  message,
                  self.declarations,
                  max_bytes~,
                )
                if budget < 31 || decoded.data.length() > budget - 31 {
                  raise Invalid("decoded message exceeds provider budget")
                }
                Some(decoded)
              }
            }
          } catch {
            error => {
              (stream.stop)()
              raise error
            }
          }
        },
        stream.stop,
        pending_bytes=stream.pending,
      )
    },
    materialized=self.materialized,
  )
}
