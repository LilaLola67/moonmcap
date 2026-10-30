///|
pub(all) struct TransformOptions {
  query : Query
  topic_map : Array[(String, String)]
  sample_period : UInt64
  sample_origin : UInt64
  align_start : UInt64?
  attachment_names : Array[String]
  attachment_media_types : Array[String]
  attachment_start : UInt64
  attachment_end : UInt64
  metadata_names : Array[String]
  metadata_entries : Array[(String, String)]
}

///|
pub fn transform_options() -> TransformOptions {
  {
    query: all_query(),
    topic_map: [],
    sample_period: 0UL,
    sample_origin: 0UL,
    align_start: None,
    attachment_names: [],
    attachment_media_types: [],
    attachment_start: 0UL,
    attachment_end: 18446744073709551615UL,
    metadata_names: [],
    metadata_entries: [],
  }
}

///|
/// Filters and samples before pagination. Renames are simultaneous and retain channel IDs.
/// Sampling selects the first physical message in each time bucket, separately per channel.
pub fn transform_log(
  log : Log,
  options : TransformOptions,
) -> Log raise McapError {
  validate_log(log)
  if options.attachment_start > options.attachment_end {
    raise Invalid("inverted attachment time interval")
  }
  let mappings : Map[String, String] = Map([])
  for (source, target) in options.topic_map {
    if mappings.contains(source) || target == "" {
      raise Invalid("duplicate topic mapping or empty target")
    }
    if !log.channels.any(fn(c) { c.topic == source }) {
      raise Invalid("topic mapping source is absent: \{source}")
    }
    mappings[source] = target
  }
  let keys : Map[String, Bool] = Map([])
  for (key, _) in options.metadata_entries {
    if keys.contains(key) {
      raise Invalid("duplicate metadata filter key")
    }
    keys[key] = true
  }
  let query = options.query
  if query.offset < 0 || query.limit < 0 {
    raise Invalid("invalid transform pagination")
  }
  let selected = build_query_index(log).query({
    ..query,
    descending: false,
    offset: 0,
    limit: default_limits().max_records,
  })
  let sampled : Array[Message] = []
  let buckets : Map[Int32, UInt64] = Map([])
  for message in selected {
    if options.sample_period != 0UL {
      if message.log_time < options.sample_origin {
        continue
      }
      let bucket = (message.log_time - options.sample_origin) /
        options.sample_period
      if buckets.get(message.channel_id) == Some(bucket) {
        continue
      }
      buckets[message.channel_id] = bucket
    }
    sampled.push(message)
  }
  let start = query.offset.min(sampled.length())
  let count = query.limit.min(sampled.length() - start)
  let messages = Array::makei(count, fn(i) {
    let position = start + i
    sampled[if query.descending {
      sampled.length() - 1 - position
    } else {
      position
    }]
  })
  let used : Map[Int32, Bool] = Map([])
  for message in messages {
    used[message.channel_id] = true
  }
  let channels = log.channels
    .filter(fn(c) { used.contains(c.id) })
    .map(fn(c) {
      {
        ..c,
        topic: mappings.get(c.topic).unwrap_or(c.topic),
        metadata: c.metadata.copy(),
      }
    })
  let schemas = log.schemas.filter(fn(s) {
    channels.any(fn(c) { c.schema_id == s.id })
  })
  let attachments = log.attachments.filter(fn(a) {
    (
      options.attachment_names.is_empty() ||
      options.attachment_names.contains(a.name)
    ) &&
    (
      options.attachment_media_types.is_empty() ||
      options.attachment_media_types.contains(a.media_type)
    ) &&
    a.log_time >= options.attachment_start &&
    a.log_time <= options.attachment_end
  })
  let metadata = log.metadata
    .filter(fn(m) {
      (
        options.metadata_names.is_empty() ||
        options.metadata_names.contains(m.name)
      ) &&
      options.metadata_entries.all(fn(pair) { m.entries.contains(pair) })
    })
    .map(fn(m) { { ..m, entries: m.entries.copy(), } })
  let result : Log = {
    ..log,
    schemas,
    channels,
    messages,
    attachments,
    metadata,
  }
  let aligned = match options.align_start {
    None => result
    Some(target) => {
      if messages.is_empty() {
        raise Invalid("cannot align an empty message selection")
      }
      let first = messages.fold(init=messages[0].log_time, fn(value, m) {
        if m.log_time < value {
          m.log_time
        } else {
          value
        }
      })
      if target >= first {
        shift_time(result, target - first)
      } else {
        shift_time(result, first - target, backwards=true)
      }
    }
  }
  validate_log(aligned)
  aligned
}
