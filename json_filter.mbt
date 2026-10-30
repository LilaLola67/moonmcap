///|
pub(all) enum JsonInvalidPolicy {
  JsonError
  JsonSkip
} derive(Eq)

///|
enum JsonCondition {
  Constant(Bool)
  All(Array[JsonCondition])
  Any(Array[JsonCondition])
  Not(JsonCondition)
  Compare(Array[String], String, Json)
}

///|
pub struct JsonFilter {
  condition : JsonCondition
  projections : Array[(String, Array[String])]
  policy : JsonInvalidPolicy
  maximum : Int32
  depth : Int32
}

///|
fn json_pointer(text : String) -> Array[String] raise McapError {
  if text == "" {
    return []
  }
  if !text.has_prefix("/") || text.length() > 1024 {
    raise Invalid("invalid JSON Pointer")
  }
  let segments = text[1:].split("/").map(fn(s) { s.to_string() }).to_array()
  if segments.length() > 64 {
    raise Invalid("JSON Pointer depth exceeded")
  }
  segments.map(fn(segment) {
    let mut i = 0
    while i < segment.length() {
      if segment[i].to_int() == 126 {
        if i + 1 >= segment.length() ||
          ![48, 49].contains(segment[i + 1].to_int()) {
          raise Invalid("invalid JSON Pointer escape")
        }
        i += 1
      }
      i += 1
    }
    segment.replace_all(old="~1", new="/").replace_all(old="~0", new="~")
  })
}

///|
fn pointer_value(document : Json, path : Array[String]) -> Json? {
  let mut value = document
  for key in path {
    value = match value {
      Object(fields) =>
        match fields.get(key) {
          Some(value) => value
          None => return None
        }
      Array(values) => {
        if key == "" || (key.length() > 1 && key.has_prefix("0")) {
          return None
        }
        let index = parse_decimal(key) catch { _ => return None }
        if index >= values.length().to_uint64() {
          return None
        }
        values[index.to_int()]
      }
      _ => return None
    }
  }
  Some(value)
}

///|
fn compile_condition(
  value : Json,
  depth : Int32,
  nodes : Array[Int32],
) -> JsonCondition raise McapError {
  nodes[0] += 1
  if depth > 16 || nodes[0] > 128 {
    raise Invalid("JSON condition complexity exceeded")
  }
  match value {
    True => Constant(true)
    False => Constant(false)
    Object(fields) => {
      for name in ["all", "any", "not"] {
        if fields.get(name) is Some(child) {
          if fields.length() != 1 {
            raise Invalid("ambiguous JSON condition")
          }
          if name == "not" {
            return Not(compile_condition(child, depth + 1, nodes))
          }
          guard child is Array(children) && !children.is_empty() else {
            raise Invalid("all/any needs a nonempty array")
          }
          let conditions = children.map(fn(c) {
            compile_condition(c, depth + 1, nodes)
          })
          return if name == "all" { All(conditions) } else { Any(conditions) }
        }
      }
      guard fields.get("path") is Some(String(path)) &&
        fields.get("op") is Some(String(op)) else {
        raise Invalid("JSON condition needs path and op")
      }
      if !["=", "!=", ">", ">=", "<", "<=", "contains", "exists"].contains(op) {
        raise Invalid("unknown JSON condition operator")
      }
      let expected = if op == "exists" {
        if fields.length() != 2 {
          raise Invalid("exists takes no value")
        }
        Json::null()
      } else {
        if fields.length() != 3 {
          raise Invalid("comparison needs exactly path, op and value")
        }
        match fields.get("value") {
          Some(value) => value
          None => raise Invalid("missing comparison value")
        }
      }
      if op == "contains" && !(expected is String(_)) {
        raise Invalid("contains needs a string")
      }
      Compare(json_pointer(path), op, expected)
    }
    _ => raise Invalid("invalid JSON condition")
  }
}

///|
fn json_equal(a : Json, b : Json) -> Bool raise McapError {
  match (a, b) {
    (Number(_, ..), Number(_, ..)) =>
      compare_decimal(a.stringify(), b.stringify()) == 0
    (Null, Null) | (True, True) | (False, False) => true
    (String(a), String(b)) => a == b
    (Array(a), Array(b)) => {
      if a.length() != b.length() {
        return false
      }
      for i = 0; i < a.length(); i = i + 1 {
        if !json_equal(a[i], b[i]) {
          return false
        }
      }
      true
    }
    (Object(a), Object(b)) => {
      if a.length() != b.length() {
        return false
      }
      for (key, value) in a {
        match b.get(key) {
          Some(other) => if !json_equal(value, other) { return false }
          None => return false
        }
      }
      true
    }
    _ => false
  }
}

///|
fn JsonCondition::matches(
  self : JsonCondition,
  document : Json,
) -> Bool raise McapError {
  match self {
    Constant(value) => value
    All(children) => children.all(fn(child) { child.matches(document) })
    Any(children) => children.any(fn(child) { child.matches(document) })
    Not(child) => !child.matches(document)
    Compare(path, op, expected) => {
      let value = match pointer_value(document, path) {
        Some(value) => value
        None => return false
      }
      if op == "exists" {
        return true
      }
      if op == "=" {
        return json_equal(value, expected)
      }
      if op == "!=" {
        return !json_equal(value, expected)
      }
      if op == "contains" {
        return match (value, expected) {
          (String(a), String(b)) => a.contains(b)
          _ => false
        }
      }
      let comparison = match (value, expected) {
        (Number(_, ..), Number(_, ..)) =>
          compare_decimal(value.stringify(), expected.stringify())
        (String(a), String(b)) => a.compare(b)
        _ => return false
      }
      match op {
        ">" => comparison > 0
        ">=" => comparison >= 0
        "<" => comparison < 0
        "<=" => comparison <= 0
        _ => false
      }
    }
  }
}

///|
pub fn json_filter(
  expression : String,
  select~ : Array[String] = [],
  invalid~ : JsonInvalidPolicy = JsonError,
  max_bytes~ : Int32 = 1024 * 1024,
  max_depth~ : Int32 = 64,
) -> JsonFilter raise McapError {
  if max_bytes < 1 || max_depth < 1 || max_depth > 128 || select.length() > 64 {
    raise Invalid("invalid JSON filter budgets")
  }
  let value = parse_json_document(@utf8.encode(expression[:]), 32768, 32)
  let seen : Map[String, Bool] = Map([])
  let projections = select.map(fn(path) {
    if seen.contains(path) {
      raise Invalid("duplicate JSON projection")
    }
    seen[path] = true
    (path, json_pointer(path))
  })
  {
    condition: compile_condition(value, 0, [0]),
    projections,
    policy: invalid,
    maximum: max_bytes,
    depth: max_depth,
  }
}

///|
pub fn JsonFilter::matches(
  self : JsonFilter,
  payload : Bytes,
) -> Bool raise McapError {
  self.condition.matches(parse_json_document(payload, self.maximum, self.depth)) catch {
    Invalid(reason) =>
      if self.policy == JsonSkip {
        false
      } else {
        raise Invalid(reason)
      }
  }
}

///|
pub fn JsonFilter::project(
  self : JsonFilter,
  payload : Bytes,
) -> Json raise McapError {
  let document = parse_json_document(payload, self.maximum, self.depth)
  if self.projections.is_empty() {
    return { "data": document, "missing_fields": [] }
  }
  let fields : Map[String, Json] = Map([])
  let missing = []
  for (name, path) in self.projections {
    match pointer_value(document, path) {
      Some(value) => fields[name] = value
      None => missing.push(name)
    }
  }
  { "data": Json::object(fields), "missing_fields": missing }
}

///|
fn matches_json_message(
  message : Message,
  channels : Array[Channel],
  filter : JsonFilter,
) -> Bool raise McapError {
  channels.any(fn(channel) {
    channel.id == message.channel_id && channel.message_encoding == "json"
  }) &&
  filter.matches(message.data)
}

///|
pub fn QueryIndex::query_source(
  self : QueryIndex,
) -> QuerySource raise McapError {
  query_source(
    { ..self.log, messages: [], attachments: [], metadata: [], },
    fn(query, _) {
      let cursor = self.cursor(query)
      message_stream(fn() { cursor.next() }, fn() { cursor.cancel() })
    },
    materialized=true,
  )
}

///|
pub fn QuerySource::filter_json(
  self : QuerySource,
  filter : JsonFilter,
) -> QuerySource raise McapError {
  query_source(
    self.declarations,
    fn(query, budget) {
      let inner = (self.open)(
        { ..query, offset: 0, limit: 2147483647, },
        budget,
      )
      let remaining = [query.limit]
      let skipped = [query.offset]
      let closed = [false]
      message_stream(
        fn() {
          if closed[0] || remaining[0] == 0 {
            (inner.stop)()
            closed[0] = true
            return None
          }
          try {
            while (inner.next_message)() is Some(message) {
              if !matches_json_message(
                  message,
                  self.declarations.channels,
                  filter,
                ) {
                continue
              }
              if skipped[0] > 0 {
                skipped[0] -= 1
                continue
              }
              remaining[0] -= 1
              if remaining[0] == 0 {
                (inner.stop)()
                closed[0] = true
              }
              return Some(message)
            }
            (inner.stop)()
            closed[0] = true
            None
          } catch {
            error => {
              (inner.stop)()
              closed[0] = true
              raise error
            }
          }
        },
        fn() {
          closed[0] = true
          (inner.stop)()
        },
        pending_bytes=inner.pending,
      )
    },
    materialized=self.materialized,
  )
}

///|
pub fn json_query_record(
  record : DatasetMessage,
  channels : Array[Channel],
  filter : JsonFilter,
) -> Json raise McapError {
  let value = dataset_message_json(record, channels)
  if value is Object(fields) {
    fields["decoded"] = filter.project(record.message.data)
  }
  value
}
