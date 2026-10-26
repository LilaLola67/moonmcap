///|
/// Bounded pure MoonBit handler for a trusted host worker entry.
pub fn codec_worker_handler(
  input_bytes? : Int = 32 * 1024 * 1024,
  output_bytes? : Int = 128 * 1024 * 1024,
  context_bytes? : Int = 1024 * 1024,
) -> ((String) -> String) raise McapError {
  if input_bytes < 1 ||
    input_bytes > 128 * 1024 * 1024 ||
    output_bytes < 40 ||
    output_bytes > 128 * 1024 * 1024 ||
    context_bytes < 1 ||
    context_bytes > 16 * 1024 * 1024 {
    raise Invalid("invalid codec worker byte budgets")
  }
  let decoder = standard_payload_decoder()
  let cached : Array[(String, Log)] = []
  fn(request) {
    let result = try {
      if request.length() > 2 * (input_bytes + context_bytes) + 32768 {
        raise Invalid("worker request exceeds encoded byte budget")
      }
      let job = field_object(
        @json.parse(request[:]) catch {
          _ => raise Invalid("invalid codec worker request JSON")
        },
      )
      let bytes = decode_hex(
        field_text(field_get(job, "data")),
        max_bytes=input_bytes,
      )
      let maximum = bounded_length(
        parse_decimal(field_get(job, "output_bytes").stringify()),
        output_bytes,
        "worker response",
      )
      let op = field_text(field_get(job, "op"))
      let output = if op == "expand" {
        let limits_object = field_object(field_get(job, "limits"))
        fn option(key : String, cap : Int) -> Int raise McapError {
          bounded_length(
            parse_decimal(field_get(limits_object, key).stringify()),
            cap,
            "worker codec limit",
          )
        }
        if maximum < 40 {
          raise Invalid("worker expansion budget too small")
        }
        let limits : Limits = {
          max_record_bytes: option("record", input_bytes),
          max_string_bytes: option("string", 16 * 1024 * 1024),
          max_records: option("records", 10000000),
          max_decompressed_bytes: option("decompressed", maximum - 40),
        }
        expand_worker_chunk(bytes, limits~)
      } else if op == "decode" {
        let context = field_text(field_get(job, "declarations"))
        let declarations = if cached.length() == 1 && cached[0].0 == context {
          cached[0].1
        } else {
          let log = read_log(decode_hex(context, max_bytes=context_bytes))
          if !log.messages.is_empty() ||
            !log.attachments.is_empty() ||
            !log.metadata.is_empty() {
            raise Invalid("worker context must contain declarations only")
          }
          cached.clear()
          cached.push((context, log))
          log
        }
        let channel_id = bounded_length(
          parse_decimal(field_get(job, "channel").stringify()),
          65535,
          "worker channel",
        )
        guard declarations.channels
          .iter()
          .find_first(fn(c) { c.id == channel_id })
          is Some(channel) else {
          raise Invalid("worker channel is undeclared")
        }
        let schema = declarations.schemas
          .iter()
          .find_first(fn(s) { s.id == channel.schema_id })
        let json = (decoder.decode_payload)(schema, channel, bytes)
        @utf8.encode(json.stringify()[:])
      } else {
        raise Invalid("unknown codec worker operation")
      }
      if output.length() > maximum {
        raise Invalid("codec worker response exceeds budget")
      }
      encode_hex(output)
    } catch {
      Invalid(reason) => "!" + reason
    }
    result
  }
}
