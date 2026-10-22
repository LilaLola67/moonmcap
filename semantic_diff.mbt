///|
pub(all) struct DiffOptions {
  json_payloads : Bool
  bucket_bytes : Int
  auxiliary_bytes : Int
  max_details : Int
  detail_bytes : Int
  fallback_bytes : Int
  limits : Limits
}

///|
pub fn diff_options() -> DiffOptions {
  {
    json_payloads: false,
    bucket_bytes: 8 * 1024 * 1024,
    auxiliary_bytes: 8 * 1024 * 1024,
    max_details: 64,
    detail_bytes: 256 * 1024,
    fallback_bytes: 64 * 1024 * 1024,
    limits: default_limits(),
  }
}

///|
fn sorted_pairs(pairs : Array[(String, String)]) -> Array[(String, String)] {
  let result = pairs.copy()
  result.sort_by(fn(a, b) {
    let c = a.0.compare(b.0)
    if c != 0 {
      c
    } else {
      a.1.compare(b.1)
    }
  })
  result
}

///|
fn semantic_channel(
  channel : Channel,
  schemas : Array[Schema],
) -> String raise McapError {
  let schema = if channel.schema_id == 0 {
    ""
  } else {
    let schema = schemas
      .iter()
      .find_first(fn(s) { s.id == channel.schema_id })
      .unwrap()
    encode_hex(encode_schema({ ..schema, id: 1, }))
  }
  schema +
  ":" +
  encode_hex(
    encode_channel({
      ..channel,
      id: 1,
      schema_id: 0,
      metadata: sorted_pairs(channel.metadata),
    }),
  )
}

///|
struct DiffBag {
  counts : Map[String, Int]
  maximum : Int
  mut bytes : Int
}

///|
fn DiffBag::add(self : DiffBag, key : String) -> Unit raise McapError {
  if !self.counts.contains(key) {
    let size = @utf8.encode(key[:]).length() + 64
    if size > self.maximum - self.bytes {
      raise Invalid("diff retained signatures exceed budget")
    }
    self.bytes += size
  }
  self.counts[key] = self.counts.get(key).unwrap_or(0) + 1
}

///|
fn diff_catalog(
  source : ReadSource,
  options : DiffOptions,
) -> (RangeReader, DiffBag) raise McapError {
  let reader = open_reader(
    source,
    limits=options.limits,
    cache_bytes=0,
    summary_bytes=options.auxiliary_bytes,
    fallback_bytes=options.fallback_bytes,
  )
  let declarations = empty_log(profile=reader.declarations.header.profile)
  let bag : DiffBag = {
    counts: Map([]),
    maximum: options.auxiliary_bytes,
    bytes: 0,
  }
  ignore(
    scan_source(
      source,
      source_visitor(
        schema=fn(value) { add_schema(declarations, value) },
        channel=fn(value) { add_channel(declarations, value) },
        attachment=fn(value) {
          bag.add("attachment:" + encode_hex(encode_attachment(value)))
        },
        metadata=fn(value) {
          bag.add(
            "metadata:" +
            encode_hex(
              encode_metadata({ ..value, entries: sorted_pairs(value.entries), }),
            ),
          )
        },
      ),
      limits=options.limits,
      declaration_bytes=options.auxiliary_bytes,
    ),
  )
  bag.add("profile:" + declarations.header.profile)
  for schema in declarations.schemas {
    bag.add("schema:" + encode_hex(encode_schema({ ..schema, id: 1, })))
  }
  for channel in declarations.channels {
    bag.add("channel:" + semantic_channel(channel, declarations.schemas))
  }
  reader.declarations.schemas.clear()
  reader.declarations.channels.clear()
  for schema in declarations.schemas {
    reader.declarations.schemas.push(schema)
  }
  for channel in declarations.channels {
    reader.declarations.channels.push(channel)
  }
  (reader, bag)
}

///|
struct DiffResult {
  details : Array[Json]
  options : DiffOptions
  mut detail_bytes : Int
  mut omitted : Int
  mut left_only : Int
  mut right_only : Int
  mut matching : Int
  mut auxiliary_changes : Int
}

///|
fn DiffResult::compare(
  self : DiffResult,
  left : DiffBag,
  right : DiffBag,
  time : UInt64?,
) -> Unit {
  let keys = left.counts.keys().to_array()
  for key in right.counts.keys() {
    if !left.counts.contains(key) {
      keys.push(key)
    }
  }
  keys.sort()
  for key in keys {
    let a = left.counts.get(key).unwrap_or(0)
    let b = right.counts.get(key).unwrap_or(0)
    if time is Some(_) {
      self.matching += a.min(b)
      self.left_only += (a - b).max(0)
      self.right_only += (b - a).max(0)
    }
    if a == b {
      continue
    }
    if time is None {
      self.auxiliary_changes += (a - b).abs()
    }
    if self.details.length() >= self.options.max_details {
      self.omitted += 1
      continue
    }
    let preview = if key.length() > 256 { key[0:256].to_string() } else { key }
    let entry : Json = {
      "kind": if time is Some(_) {
        "message"
      } else {
        "declaration-or-auxiliary"
      },
      "log_time": match time {
        Some(t) => Json::string(t.to_string())
        None => Json::null()
      },
      "left_count": a,
      "right_count": b,
      "signature_preview": preview,
      "preview_truncated": preview != key,
    }
    let size = @utf8.encode(entry.stringify()[:]).length()
    if size > self.options.detail_bytes - self.detail_bytes {
      self.omitted += 1
      continue
    }
    self.detail_bytes += size
    self.details.push(entry)
  }
}

///|
fn diff_message_signature(
  message : Message,
  reader : RangeReader,
  options : DiffOptions,
) -> String raise McapError {
  let channel = reader.declarations.channels
    .iter()
    .find_first(fn(c) { c.id == message.channel_id })
    .unwrap()
  let data = if options.json_payloads && channel.message_encoding == "json" {
    canonical_value(
      parse_json_document(message.data, options.limits.max_record_bytes, 64),
    )
  } else {
    encode_hex(message.data)
  }
  semantic_channel(channel, reader.declarations.schemas) +
  ":" +
  message.sequence.to_string() +
  ":" +
  message.publish_time.to_string() +
  ":" +
  data
}

///|
/// Compare validated sources by exact timestamp buckets and message multisets.
/// Ignores compression, physical ID values, indexes and Header.library.
pub fn diff_sources(
  left : ReadSource,
  right : ReadSource,
  options? : DiffOptions = diff_options(),
) -> Json raise McapError {
  if options.bucket_bytes < 1 ||
    options.auxiliary_bytes < 1 ||
    options.max_details < 0 ||
    options.detail_bytes < 0 {
    raise Invalid("invalid diff budgets")
  }
  let (a, left_aux) = diff_catalog(left, options)
  let (b, right_aux) = diff_catalog(right, options)
  let query = { ..all_query(), limit: options.limits.max_records, }
  let ca = a.cursor(query, pending_bytes=options.bucket_bytes)
  let cb = b.cursor(query, pending_bytes=options.bucket_bytes)
  let result : DiffResult = {
    details: [],
    options,
    detail_bytes: 0,
    omitted: 0,
    left_only: 0,
    right_only: 0,
    matching: 0,
    auxiliary_changes: 0,
  }
  try {
    result.compare(left_aux, right_aux, None)
    let mut ma = ca.next()
    let mut mb = cb.next()
    while ma is Some(_) || mb is Some(_) {
      let time = match (ma, mb) {
        (Some(a), Some(b)) =>
          if a.log_time < b.log_time {
            a.log_time
          } else {
            b.log_time
          }
        (Some(a), _) => a.log_time
        (_, Some(b)) => b.log_time
        _ => abort("unreachable")
      }
      let ba : DiffBag = {
        counts: Map([]),
        maximum: options.bucket_bytes,
        bytes: 0,
      }
      let bb : DiffBag = {
        counts: Map([]),
        maximum: options.bucket_bytes,
        bytes: 0,
      }
      while ma is Some(message) && message.log_time == time {
        ba.add(diff_message_signature(message, a, options))
        ma = ca.next()
      }
      while mb is Some(message) && message.log_time == time {
        bb.add(diff_message_signature(message, b, options))
        mb = cb.next()
      }
      result.compare(ba, bb, Some(time))
    }
    {
      "equal": result.left_only == 0 &&
      result.right_only == 0 &&
      result.auxiliary_changes == 0,
      "matching_messages": result.matching,
      "left_only_messages": result.left_only,
      "right_only_messages": result.right_only,
      "auxiliary_changes": result.auxiliary_changes,
      "details": result.details,
      "omitted_details": result.omitted,
      "json_payloads": options.json_payloads,
      "full_validation": true,
    }
  } catch {
    error => {
      ca.cancel()
      cb.cancel()
      raise error
    }
  }
}
