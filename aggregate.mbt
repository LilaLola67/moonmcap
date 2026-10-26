///|
pub(all) struct AggregateOptions {
  value_path : String
  group_paths : Array[String]
  by_topic : Bool
  window : UInt64
  origin : UInt64
  max_groups : Int
  group_bytes : Int
  invalid : JsonInvalidPolicy
}

///|
pub fn aggregate_options() -> AggregateOptions {
  {
    value_path: "",
    group_paths: [],
    by_topic: true,
    window: 0UL,
    origin: 0UL,
    max_groups: 1024,
    group_bytes: 8 * 1024 * 1024,
    invalid: JsonError,
  }
}

///|
fn canonical_value(value : Json) -> String raise McapError {
  match value {
    Number(_, ..) => {
      let d = decimal_parts(value.stringify())
      (if d.negative { "-" } else { "" }) +
      d.digits +
      "e" +
      d.exponent.to_string()
    }
    Array(values) => "[" + values.map(canonical_value).join(",") + "]"
    Object(fields) => {
      let keys = fields.keys().to_array()
      keys.sort()
      "{" +
      keys
      .map(fn(key) {
        Json::string(key).stringify() + ":" + canonical_value(fields[key])
      })
      .join(",") +
      "}"
    }
    _ => value.stringify()
  }
}

///|
struct AggregateState {
  group : Json
  window : UInt64?
  mut count : Int
  mut numeric : Int
  mut missing : Int
  mut non_numeric : Int
  mut coefficient : @bigint.BigInt
  mut exponent : Int
  mut minimum : String?
  mut maximum : String?
}

///|
fn decimal_power(exponent : Int) -> @bigint.BigInt raise McapError {
  if exponent < 0 || exponent > 4096 {
    raise Invalid("aggregate decimal alignment exceeds 4096 digits")
  }
  @bigint.BigInt::pow(
    @bigint.BigInt::from_int(10),
    @bigint.BigInt::from_int(exponent),
  )
}

///|
fn AggregateState::number(
  self : AggregateState,
  text : String,
) -> Unit raise McapError {
  let d = decimal_parts(text)
  if d.exponent < -1024 || d.exponent > 1024 {
    raise Invalid("aggregate exponent outside -1024..1024")
  }
  let coefficient = @bigint.BigInt::from_string(
    (if d.negative { "-" } else { "" }) + d.digits,
  )
  let exponent = if self.numeric == 0 {
    d.exponent
  } else {
    self.exponent.min(d.exponent)
  }
  let sum = if self.numeric == 0 {
    coefficient
  } else {
    self.coefficient * decimal_power(self.exponent - exponent) +
    coefficient * decimal_power(d.exponent - exponent)
  }
  if sum.to_string().length() > 4096 {
    raise Invalid("aggregate sum exceeds digit budget")
  }
  self.coefficient = sum
  self.exponent = exponent
  self.numeric += 1
  if self.minimum is None || compare_decimal(text, self.minimum.unwrap()) < 0 {
    self.minimum = Some(text)
  }
  if self.maximum is None || compare_decimal(text, self.maximum.unwrap()) > 0 {
    self.maximum = Some(text)
  }
}

///|
fn AggregateState::report(self : AggregateState) -> Json raise McapError {
  let sum = self.coefficient.to_string() + "e" + self.exponent.to_string()
  let average = if self.numeric == 0 {
    Json::null()
  } else {
    let mut numerator = self.coefficient
    let mut denominator = @bigint.BigInt::from_int(self.numeric)
    if self.exponent + 18 >= 0 {
      numerator = numerator * decimal_power(self.exponent + 18)
    } else {
      denominator = denominator * decimal_power(-self.exponent - 18)
    }
    let mut rounded = numerator / denominator
    let remainder = numerator % denominator
    let absolute = if remainder < @bigint.BigInt::from_int(0) {
      -remainder
    } else {
      remainder
    }
    if absolute * @bigint.BigInt::from_int(2) >= denominator {
      rounded = rounded +
        @bigint.BigInt::from_int(
          if numerator < @bigint.BigInt::from_int(0) {
            -1
          } else {
            1
          },
        )
    }
    {
      "numerator": sum,
      "denominator": self.numeric,
      "decimal": rounded.to_string() + "e-18",
      "rounding": "half-away-from-zero",
      "decimal_places": 18,
    }
  }
  {
    "group": self.group,
    "window_start": match self.window {
      None => Json::null()
      Some(n) => Json::string(n.to_string())
    },
    "count": self.count,
    "numeric_count": self.numeric,
    "missing_count": self.missing,
    "non_numeric_count": self.non_numeric,
    "sum": if self.numeric == 0 {
      Json::null()
    } else {
      Json::string(sum)
    },
    "min": match self.minimum {
      None => Json::null()
      Some(n) => Json::string(n)
    },
    "max": match self.maximum {
      None => Json::null()
      Some(n) => Json::string(n)
    },
    "avg": average,
  }
}

///|
/// Consume ordered messages without retaining history; returns deterministic,
/// bounded groups. Payload JSON is capped at 1 MiB/64 levels.
pub fn aggregate_json(
  cursor : DatasetCursor,
  channels : Array[Channel],
  options? : AggregateOptions = aggregate_options(),
) -> Json raise McapError {
  try {
    if options.max_groups < 1 ||
      options.group_bytes < 1 ||
      options.group_paths.length() > 16 {
      raise Invalid("invalid aggregate group budgets")
    }
    let value_path = json_pointer(options.value_path)
    let paths = options.group_paths.map(json_pointer)
    let groups : Map[String, AggregateState] = Map([])
    let charges : Map[String, Int] = Map([])
    let mut retained = 0
    let mut peak = 0
    let mut skipped = 0
    while cursor.next() is Some(message) {
      let channel = channels
        .iter()
        .find_first(fn(c) { c.id == message.channel_id })
        .unwrap()
      if channel.message_encoding != "json" || message.log_time < options.origin {
        skipped += 1
        continue
      }
      let document = parse_json_document(message.data, 1024 * 1024, 64) catch {
        Invalid(reason) =>
          if options.invalid == JsonSkip {
            skipped += 1
            continue
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
      let state = match groups.get(key) {
        Some(state) => state
        None => {
          if groups.length() >= options.max_groups {
            raise Invalid("aggregate group count budget exceeded")
          }
          let state : AggregateState = {
            group,
            window,
            count: 0,
            numeric: 0,
            missing: 0,
            non_numeric: 0,
            coefficient: @bigint.BigInt::from_int(0),
            exponent: 0,
            minimum: None,
            maximum: None,
          }
          groups[key] = state
          state
        }
      }
      state.count += 1
      match pointer_value(document, value_path) {
        None => state.missing += 1
        Some(Number(_, ..) as value) => state.number(value.stringify())
        Some(_) => state.non_numeric += 1
      }
      let charge = state.charge(key)
      let old = charges.get(key).unwrap_or(0)
      if charge - old > options.group_bytes - retained {
        raise Invalid("aggregate retained group byte budget exceeded")
      }
      retained += charge - old
      charges[key] = charge
      peak = peak.max(retained)
    }
    let keys = groups.keys().to_array()
    keys.sort()
    {
      "groups": keys.map(fn(key) { groups[key].report() }),
      "skipped_messages": skipped,
      "peak_group_bytes": peak,
    }
  } catch {
    error => {
      cursor.cancel()
      raise error
    }
  }
}
