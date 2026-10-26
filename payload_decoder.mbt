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
struct ProtoCache {
  name : String
  data : Bytes
  decoder : ProtobufDecoder
  charge : Int
}

///|
/// Schema identity includes descriptor bytes, not just a source-local schema ID.
pub fn standard_payload_decoder(
  options? : ProtobufOptions = protobuf_options(),
  cache_bytes? : Int = 8 * 1024 * 1024,
  max_schemas? : Int = 64,
) -> PayloadDecoder raise McapError {
  if cache_bytes < 1 || max_schemas < 1 {
    raise Invalid("invalid payload decoder cache budget")
  }
  let entries : Array[ProtoCache] = []
  let retained = [0]
  payload_decoder(fn(schema, channel, bytes) {
    if channel.message_encoding == "json" {
      return parse_json_document(bytes, options.message_bytes, options.depth)
    }
    if channel.message_encoding != "protobuf" {
      raise Invalid(
        "no decoder for message encoding: \{channel.message_encoding}",
      )
    }
    guard schema is Some(schema) && schema.encoding == "protobuf" else {
      raise Invalid(
        "protobuf channel requires a protobuf FileDescriptorSet schema",
      )
    }
    for i = 0; i < entries.length(); i = i + 1 {
      if entries[i].name == schema.name && entries[i].data == schema.data {
        let entry = entries.remove(i)
        entries.push(entry)
        return entry.decoder.decode(schema.name, bytes)
      }
    }
    let decoder = protobuf_decoder(schema.data, options~)
    let size = schema.data.length().to_uint64() +
      decoder.field_count[0].to_uint64() * 128UL
    if size > cache_bytes.to_uint64() {
      raise Invalid("single protobuf schema exceeds decoder cache budget")
    }
    let charge = size.to_int()
    while entries.length() >= max_schemas || charge > cache_bytes - retained[0] {
      retained[0] -= entries.remove(0).charge
    }
    entries.push({ name: schema.name, data: schema.data, decoder, charge, })
    retained[0] += charge
    decoder.decode(schema.name, bytes)
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
