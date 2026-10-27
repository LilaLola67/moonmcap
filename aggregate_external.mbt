///|
fn AggregateState::charge(self : AggregateState, key : String) -> Int {
  @utf8.encode(key[:]).length() * 2 +
  self.coefficient.to_string().length() +
  self.minimum.map(fn(s) { s.length() }).unwrap_or(0) +
  self.maximum.map(fn(s) { s.length() }).unwrap_or(0) +
  1024
}

///|
fn AggregateState::merge(
  self : AggregateState,
  other : AggregateState,
) -> Unit raise McapError {
  if other.count > 2147483647 - self.count {
    raise Invalid("aggregate count overflow")
  }
  self.count += other.count
  self.missing += other.missing
  self.non_numeric += other.non_numeric
  if other.numeric > 0 {
    let exponent = if self.numeric == 0 {
      other.exponent
    } else {
      self.exponent.min(other.exponent)
    }
    let sum = if self.numeric == 0 {
      other.coefficient
    } else {
      self.coefficient * decimal_power(self.exponent - exponent) +
      other.coefficient * decimal_power(other.exponent - exponent)
    }
    if sum.to_string().length() > 4096 {
      raise Invalid("aggregate sum exceeds digit budget")
    }
    self.coefficient = sum
    self.exponent = exponent
    self.numeric += other.numeric
    if other.minimum is Some(value) &&
      (
        self.minimum is None ||
        compare_decimal(value, self.minimum.unwrap()) < 0
      ) {
      self.minimum = Some(value)
    }
    if other.maximum is Some(value) &&
      (
        self.maximum is None ||
        compare_decimal(value, self.maximum.unwrap()) > 0
      ) {
      self.maximum = Some(value)
    }
  }
}

///|
fn aggregate_single(
  message : Message,
  channel : Channel,
  options : AggregateOptions,
  paths : Array[Array[String]],
  value_path : Array[String],
) -> (String, AggregateState)? raise McapError {
  if channel.message_encoding != "json" || message.log_time < options.origin {
    return None
  }
  let document = parse_json_document(message.data, 1024 * 1024, 64) catch {
    Invalid(reason) =>
      if options.invalid == JsonSkip {
        return None
      } else {
        raise Invalid(reason)
      }
  }
  let values : Array[Json] = paths.map(fn(path) {
    match pointer_value(document, path) {
      None => { "missing": true }
      Some(value) => { "value": value }
    }
  })
  let window = if options.window == 0UL {
    None
  } else {
    Some(
      options.origin +
      (message.log_time - options.origin) / options.window * options.window,
    )
  }
  let group : Json = {
    "topic": if options.by_topic {
      Json::string(channel.topic)
    } else {
      Json::null()
    },
    "fields": values,
  }
  let key = canonical_value(group) +
    "/" +
    window.map(fn(n) { n.to_string() }).unwrap_or("")
  let state : AggregateState = {
    group,
    window,
    count: 1,
    numeric: 0,
    missing: 0,
    non_numeric: 0,
    coefficient: @bigint.BigInt::from_int(0),
    exponent: 0,
    minimum: None,
    maximum: None,
  }
  match pointer_value(document, value_path) {
    None => state.missing = 1
    Some(Number(_, ..) as value) => state.number(value.stringify())
    Some(_) => state.non_numeric = 1
  }
  Some((key, state))
}

///|
fn aggregate_encode(key : String, state : AggregateState) -> Bytes {
  let out = @buffer.Buffer::Buffer()
  put_text(out, key)
  put_text(out, state.group.stringify())
  match state.window {
    None => out.write_byte(0x00)
    Some(time) => {
      out.write_byte(0x01)
      out.write_uint64_le(time)
    }
  }
  for count in [state.count, state.numeric, state.missing, state.non_numeric] {
    out.write_uint64_le(count.to_uint64())
  }
  put_text(out, state.coefficient.to_string())
  out.write_int_le(state.exponent)
  for value in [state.minimum, state.maximum] {
    match value {
      None => out.write_byte(0x00)
      Some(value) => {
        out.write_byte(0x01)
        put_text(out, value)
      }
    }
  }
  let bytes = out.to_bytes()
  crc32(bytes).mcap_le_bytes() + bytes
}

///|
struct AggregateRow {
  key : String
  state : AggregateState
  charge : Int
}

///|
fn aggregate_decode(
  bytes : Bytes,
  limits : Limits,
) -> AggregateRow raise McapError {
  let input = cursor(bytes, limits)
  let checksum = input.u32()
  if checksum != crc32(bytes[4:].to_owned()) {
    raise Invalid("aggregate run CRC mismatch")
  }
  let key = input.text()
  let group_text = input.text()
  let group = parse_json_document(
    @utf8.encode(group_text[:]),
    limits.max_record_bytes,
    64,
  )
  let window = match input.take(1)[0] {
    0x00 => None
    0x01 => Some(input.u64())
    _ => raise Invalid("invalid aggregate window tag")
  }
  let count = input.u64()
  let numeric = input.u64()
  let missing = input.u64()
  let non_numeric = input.u64()
  if count > 2147483647UL ||
    numeric + missing + non_numeric != count ||
    numeric > count ||
    missing > count ||
    non_numeric > count {
    raise Invalid("invalid aggregate counters")
  }
  let coefficient = input.text()
  if coefficient.length() > 4096 ||
    coefficient.contains(".") ||
    coefficient.contains("e") ||
    coefficient.contains("E") {
    raise Invalid("invalid aggregate coefficient")
  }
  ignore(decimal_parts(coefficient))
  let exponent = input.u32().to_int()
  if exponent < -1024 || exponent > 1024 {
    raise Invalid("invalid aggregate exponent")
  }
  let extrema : Array[String?] = []
  for i = 0; i < 2; i = i + 1 {
    extrema.push(
      match input.take(1)[0] {
        0x00 => None
        0x01 => {
          let text = input.text()
          ignore(decimal_parts(text))
          Some(text)
        }
        _ => raise Invalid("invalid aggregate extrema tag")
      },
    )
  }
  input.finish()
  if (numeric == 0UL && (extrema[0] is Some(_) || extrema[1] is Some(_))) ||
    (numeric > 0UL && (extrema[0] is None || extrema[1] is None)) {
    raise Invalid("aggregate extrema/count mismatch")
  }
  if numeric > 0UL &&
    compare_decimal(extrema[0].unwrap(), extrema[1].unwrap()) > 0 {
    raise Invalid("aggregate extrema are reversed")
  }
  if key !=
    canonical_value(group) +
    "/" +
    window.map(fn(n) { n.to_string() }).unwrap_or("") {
    raise Invalid("aggregate key/state mismatch")
  }
  let state : AggregateState = {
    group,
    window,
    count: count.to_int(),
    numeric: numeric.to_int(),
    missing: missing.to_int(),
    non_numeric: non_numeric.to_int(),
    coefficient: @bigint.BigInt::from_string(coefficient),
    exponent,
    minimum: extrema[0],
    maximum: extrema[1],
  }
  { key, state, charge: state.charge(key), }
}

///|
struct AggregateRun {
  source : ReadSource
  limits : Limits
  quota : Int
  mut position : UInt64
  mut previous : String?
}

///|
fn AggregateRun::next(self : AggregateRun) -> AggregateRow? raise McapError {
  if self.position == self.source.size {
    return None
  }
  let record = self.source.record(self.position, self.source.size, {
    ..self.limits,
    max_record_bytes: self.limits.max_record_bytes.min(self.quota),
  })
  if record.opcode != 0x83 {
    raise Invalid("invalid aggregate run opcode")
  }
  let row = aggregate_decode(record.body, self.limits)
  if row.charge > self.quota {
    raise Invalid("aggregate merge row exceeds head budget")
  }
  if self.previous is Some(key) && row.key <= key {
    raise Invalid("aggregate run order mismatch")
  }
  self.previous = Some(row.key)
  self.position += (record.body.length() + 9).to_uint64()
  Some(row)
}

///|
struct AggregateSpill {
  storage : SortStorage
  options : ExternalOptions
  cancelled : () -> Bool
  live : Map[Int, UInt64]
  mut next_id : Int
  mut spool : UInt64
  mut peak_spool : UInt64
  mut peak_merge : Int
}

///|
fn AggregateSpill::create(
  self : AggregateSpill,
) -> (Int, WriteSink) raise McapError {
  if self.next_id >= self.options.run_limit {
    raise Invalid("aggregate run count budget exceeded")
  }
  let id = self.next_id
  self.next_id += 1
  let sink = (self.storage.create)(id)
  self.live[id] = 0UL
  let output = new_write_sink(fn(bytes) {
    if (self.cancelled)() {
      raise Invalid("external aggregation cancelled")
    }
    if bytes.length().to_uint64() > self.options.spool_bytes - self.spool {
      raise Invalid("aggregate disk budget exceeded")
    }
    sink.write(bytes)
    self.live[id] = self.live[id] + bytes.length().to_uint64()
    self.spool += bytes.length().to_uint64()
    if self.spool > self.peak_spool {
      self.peak_spool = self.spool
    }
  })
  output.write(b"GAGR1\r\n\x00")
  (id, output)
}

///|
fn AggregateSpill::remove(
  self : AggregateSpill,
  id : Int,
) -> Unit raise McapError {
  (self.storage.remove)(id)
  self.spool -= self.live[id]
  self.live.remove(id)
}

///|
fn AggregateSpill::write_row(
  self : AggregateSpill,
  sink : WriteSink,
  key : String,
  state : AggregateState,
) -> Unit raise McapError {
  let bytes = aggregate_encode(key, state)
  if @utf8.encode(key[:]).length() > self.options.limits.max_string_bytes ||
    @utf8.encode(state.group.stringify()[:]).length() >
    self.options.limits.max_string_bytes {
    raise Invalid("aggregate key exceeds string budget")
  }
  if bytes.length() > self.options.limits.max_record_bytes {
    raise Invalid("aggregate row exceeds record budget")
  }
  sink.write(frame(0x83, bytes))
}

///|
fn AggregateSpill::merge(
  self : AggregateSpill,
  ids : Array[Int],
  emit : (String, AggregateState) -> Unit raise McapError,
) -> Unit raise McapError {
  let inputs = []
  let heads : Array[AggregateRow?] = []
  let quota = self.options.merge_bytes / (ids.length() + 1)
  let mut retained = 0
  for id in ids {
    let source = (self.storage.open)(id)
    if source.size < 8UL || source.read_exact(0UL, 8) != b"GAGR1\r\n\x00" {
      raise Invalid("invalid aggregate run envelope")
    }
    let input : AggregateRun = {
      source,
      limits: self.options.limits,
      quota,
      position: 8UL,
      previous: None,
    }
    let head = input.next()
    retained += head.map(fn(row) { row.charge }).unwrap_or(0)
    inputs.push(input)
    heads.push(head)
  }
  while true {
    if (self.cancelled)() {
      raise Invalid("external aggregation cancelled")
    }
    let mut chosen : String? = None
    for head in heads {
      if head is Some(row) && (chosen is None || row.key < chosen.unwrap()) {
        chosen = Some(row.key)
      }
    }
    guard chosen is Some(key) else { break }
    let mut merged : AggregateState? = None
    for i = 0; i < heads.length(); i = i + 1 {
      if heads[i] is Some(row) && row.key == key {
        retained -= row.charge
        match merged {
          None => merged = Some(row.state)
          Some(state) => state.merge(row.state)
        }
        let next = inputs[i].next()
        retained += next.map(fn(row) { row.charge }).unwrap_or(0)
        heads[i] = next
        let used = retained + merged.unwrap().charge(key)
        if used > self.options.merge_bytes {
          raise Invalid("aggregate merge heads exceed budget")
        }
        self.peak_merge = self.peak_merge.max(used)
      }
    }
    emit(key, merged.unwrap())
  }
}

///|
/// Stream one exact aggregate JSON object per group; failures leave provisional
/// output. Temporary storage is dedicated and always cleaned on return.
pub fn aggregate_external(
  cursor : DatasetCursor,
  channels : Array[Channel],
  storage : SortStorage,
  output : WriteSink,
  aggregate? : AggregateOptions = aggregate_options(),
  external? : ExternalOptions = external_options(),
  cancelled? : () -> Bool = fn() { false },
) -> Json raise McapError {
  let session : AggregateSpill = {
    storage: buffered_sort_storage(storage, 65536),
    options: external,
    cancelled,
    live: Map([]),
    next_id: 0,
    spool: 0UL,
    peak_spool: 0UL,
    peak_merge: 0,
  }
  let result = try {
    validate_log(empty_log(), limits=external.limits)
    if aggregate.max_groups < 1 ||
      aggregate.group_bytes < 1 ||
      aggregate.group_paths.length() > 16 ||
      external.run_bytes < 1 ||
      external.merge_bytes < 1 ||
      external.fan_in < 2 ||
      external.fan_in > 64 ||
      external.limits.max_record_bytes > 2147483638 {
      raise Invalid("invalid external aggregate budgets")
    }
    let maximum = aggregate.group_bytes.min(external.run_bytes)
    let paths = aggregate.group_paths.map(json_pointer)
    let value_path = json_pointer(aggregate.value_path)
    let groups : Map[String, AggregateState] = Map([])
    let runs = []
    let mut retained = 0
    let mut peak = 0
    let mut input_count = 0
    let mut skipped = 0
    let flush = fn() -> Unit raise McapError {
      if !groups.is_empty() {
        let keys = groups.keys().to_array()
        keys.sort()
        let (id, sink) = session.create()
        for key in keys {
          session.write_row(sink, key, groups[key])
        }
        runs.push(id)
        groups.clear()
      }
    }
    while cursor.next() is Some(message) {
      if cancelled() {
        raise Invalid("external aggregation cancelled")
      }
      if input_count >= external.limits.max_records {
        raise Invalid("external aggregate input count exceeds budget")
      }
      input_count += 1
      let channel = channels
        .iter()
        .find_first(fn(c) { c.id == message.channel_id })
        .unwrap()
      guard aggregate_single(message, channel, aggregate, paths, value_path)
        is Some((key, single)) else {
        skipped += 1
        continue
      }
      let old = groups.get(key)
      let candidate = match old {
        None => single
        Some(state) => {
          let merged = { ..state, }
          merged.merge(single)
          merged
        }
      }
      let charge = candidate.charge(key)
      let previous = old.map(fn(s) { s.charge(key) }).unwrap_or(0)
      if (old is None && groups.length() >= aggregate.max_groups) ||
        charge - previous > maximum - retained {
        flush()
        retained = 0
        let charge = single.charge(key)
        if charge > maximum {
          raise Invalid("single aggregate group exceeds run budget")
        }
        groups[key] = single
        retained = charge
      } else {
        groups[key] = candidate
        retained += charge - previous
      }
      peak = peak.max(retained)
    }
    flush()
    let mut passes = 0
    while runs.length() > external.fan_in {
      let ids = runs[0:external.fan_in].to_array()
      let (id, sink) = session.create()
      session.merge(ids, fn(key, state) { session.write_row(sink, key, state) })
      for old in ids {
        session.remove(old)
      }
      for i = 0; i < ids.length(); i = i + 1 {
        ignore(runs.remove(0))
      }
      runs.push(id)
      passes += 1
    }
    let mut count = 0
    session.merge(runs, fn(_, state) {
      output.write(@utf8.encode((state.report().stringify() + "\n")[:]))
      count += 1
    })
    let report : Json = {
      "groups": count,
      "input_messages": input_count,
      "skipped_messages": skipped,
      "runs_created": session.next_id,
      "merge_groups": passes,
      "peak_run_bytes": peak,
      "peak_merge_bytes": session.peak_merge,
      "peak_spool_bytes": session.peak_spool.to_string(),
      "output_bytes": output.position().to_string(),
      "output_format": "NDJSON exact group aggregates",
    }
    Ok(report)
  } catch {
    error => Err(error)
  }
  let errors = []
  for id in session.live.keys().to_array() {
    session.remove(id) catch {
      Invalid(reason) => errors.push(reason)
    }
  }
  (session.storage.finish)() catch {
    Invalid(reason) => errors.push(reason)
  }
  match result {
    Err(Invalid(reason)) => {
      cursor.cancel()
      if !errors.is_empty() {
        raise Invalid("\{reason}; cleanup failed: \{errors[0]}")
      }
      raise Invalid(reason)
    }
    Ok(report) => {
      if !errors.is_empty() {
        raise Invalid("aggregate cleanup failed: \{errors[0]}")
      }
      report
    }
  }
}
