///|
struct SchemaComparison {
  mut old_reads_new : Bool
  mut new_reads_old : Bool
  mut old_queries_new : Bool
  mut total : Int
  maximum : Int
  issues : Array[Json]
  visited : Map[String, Bool]
}

///|
fn SchemaComparison::issue(
  self : SchemaComparison,
  path : String,
  reason : String,
  old_reads_new? : Bool = true,
  new_reads_old? : Bool = true,
  queries? : Bool = true,
) -> Unit {
  self.old_reads_new = self.old_reads_new && old_reads_new
  self.new_reads_old = self.new_reads_old && new_reads_old
  self.old_queries_new = self.old_queries_new && queries
  self.total += 1
  if self.issues.length() < self.maximum {
    self.issues.push({
      "path": path,
      "reason": reason,
      "old_reader_new_data": old_reads_new,
      "new_reader_old_data": new_reads_old,
      "old_query_new_data": queries,
    })
  }
}

///|
fn SchemaComparison::visit(
  self : SchemaComparison,
  a : String,
  b : String,
  depth : Int,
) -> Bool raise McapError {
  if depth > 64 || self.visited.length() > 4096 {
    raise Invalid("schema comparison traversal budget exceeded")
  }
  let key = a + "\x00" + b
  if self.visited.contains(key) {
    return false
  }
  self.visited[key] = true
  true
}

///|
fn proto_oneof_members(model : ProtoMessage, field : ProtoField) -> Array[Int] {
  let members = match field.oneof {
    None => []
    Some(id) =>
      model.fields
      .values()
      .filter(fn(f) { f.oneof == Some(id) })
      .map(fn(f) { f.number })
      .to_array()
  }
  members.sort()
  members
}

///|
fn compare_proto_model(
  report : SchemaComparison,
  old : ProtobufDecoder,
  new : ProtobufDecoder,
  a : String,
  b : String,
  path : String,
  depth : Int,
) -> Unit raise McapError {
  if !report.visit(a, b, depth) {
    return
  }
  guard old.messages.get(a) is Some(left) && new.messages.get(b) is Some(right) else {
    raise Invalid("schema root message not found")
  }
  if left.map_entry != right.map_entry || left.proto3 != right.proto3 {
    report.issue(
      path,
      "map-entry or proto2/proto3 interpretation changed",
      old_reads_new=false,
      new_reads_old=false,
    )
  }
  let numbers = left.fields.keys().to_array()
  numbers.sort()
  for n in numbers {
    let f = left.fields[n]
    let child = path + "/" + n.to_string()
    guard right.fields.get(n) is Some(g) else {
      report.issue(
        child,
        "field removed",
        old_reads_new=f.label != 2,
        queries=false,
      )
      continue
    }
    if f.json_name != g.json_name {
      report.issue(
        child,
        "JSON field name changed: " + f.json_name + " -> " + g.json_name,
        queries=false,
      )
    }
    if f.kind != g.kind || (f.label == 3) != (g.label == 3) {
      report.issue(
        child,
        "field kind or repeated shape changed",
        old_reads_new=false,
        new_reads_old=false,
        queries=false,
      )
      continue
    }
    if f.label != g.label {
      report.issue(
        child,
        "required/optional contract changed",
        old_reads_new=f.label != 2,
        new_reads_old=g.label != 2,
      )
    }
    if f.optional != g.optional ||
      proto_oneof_members(left, f) != proto_oneof_members(right, g) {
      report.issue(
        child,
        "presence or oneof membership changed",
        old_reads_new=false,
        new_reads_old=false,
        queries=false,
      )
    }
    if f.kind == 11 {
      compare_proto_model(
        report,
        old,
        new,
        f.type_name,
        g.type_name,
        child,
        depth + 1,
      )
    } else if f.kind == 14 {
      let av = old.enums[f.type_name].values
      let bv = new.enums[g.type_name].values
      if old.enums[f.type_name].first != new.enums[g.type_name].first {
        report.issue(
          child,
          "enum first/default numeric value changed",
          old_reads_new=false,
          new_reads_old=false,
          queries=false,
        )
      }
      for (value, name) in av {
        match bv.get(value) {
          None =>
            report.issue(
              child,
              "enum numeric value removed: " + value.to_string(),
              new_reads_old=right.proto3,
              queries=false,
            )
          Some(other) =>
            if name != other {
              report.issue(
                child,
                "enum name changed at value " + value.to_string(),
                queries=false,
              )
            }
        }
      }
      for value in bv.keys() {
        if !av.contains(value) {
          report.issue(
            child,
            "enum numeric value added: " + value.to_string(),
            old_reads_new=left.proto3,
          )
        }
      }
    }
  }
  let added = right.fields.keys().to_array()
  added.sort()
  for n in added {
    if !left.fields.contains(n) {
      report.issue(
        path + "/" + n.to_string(),
        "field added",
        new_reads_old=right.fields[n].label != 2,
      )
    }
  }
}

///|
fn compare_ros_model(
  report : SchemaComparison,
  old : CdrDecoder,
  new : CdrDecoder,
  a : String,
  b : String,
  path : String,
  depth : Int,
) -> Unit raise McapError {
  if !report.visit(a, b, depth) {
    return
  }
  let left = old.definitions[a]
  let right = new.definitions[b]
  if left.length() != right.length() {
    report.issue(
      path,
      "ordered field count changed",
      old_reads_new=false,
      new_reads_old=false,
      queries=false,
    )
  }
  for i = 0; i < left.length() && i < right.length(); i = i + 1 {
    let f = left[i]
    let g = right[i]
    let child = path + "/" + i.to_string()
    if f.name != g.name {
      report.issue(
        child,
        "field name/order changed: " + f.name + " -> " + g.name,
        queries=false,
      )
    }
    if f.array != g.array ||
      cdr_primitive(f.base) != cdr_primitive(g.base) ||
      (cdr_primitive(f.base) && f.base != g.base) {
      report.issue(
        child,
        "ordered wire type or array layout changed",
        old_reads_new=false,
        new_reads_old=false,
        queries=false,
      )
      continue
    }
    let old_array = f.bound.unwrap_or(2147483647)
    let new_array = g.bound.unwrap_or(2147483647)
    let old_text = f.string_bound.unwrap_or(2147483647)
    let new_text = g.string_bound.unwrap_or(2147483647)
    if old_array != new_array || old_text != new_text {
      report.issue(
        child,
        "sequence or string acceptance bound changed",
        old_reads_new=new_array <= old_array && new_text <= old_text,
        new_reads_old=old_array <= new_array && old_text <= new_text,
      )
    }
    if !cdr_primitive(f.base) {
      compare_ros_model(report, old, new, f.base, g.base, child, depth + 1)
    }
  }
}

///|
/// Conservative directional comparison of supported payload definitions. This
/// describes wire/schema acceptance, not application meaning or unit equivalence.
pub fn compare_payload_schemas(
  old : Schema,
  new : Schema,
  issue_limit? : Int = 256,
) -> Json raise McapError {
  if issue_limit < 0 || issue_limit > 10000 {
    raise Invalid("invalid schema issue limit")
  }
  let report : SchemaComparison = {
    old_reads_new: true,
    new_reads_old: true,
    old_queries_new: true,
    total: 0,
    maximum: issue_limit,
    issues: [],
    visited: Map([]),
  }
  if old.encoding != new.encoding {
    report.issue(
      "",
      "payload schema encoding changed",
      old_reads_new=false,
      new_reads_old=false,
      queries=false,
    )
  } else {
    match old.encoding {
      "protobuf" =>
        compare_proto_model(
          report,
          protobuf_decoder(old.data),
          protobuf_decoder(new.data),
          if old.name.has_prefix(".") {
            old.name[1:].to_string()
          } else {
            old.name
          },
          if new.name.has_prefix(".") {
            new.name[1:].to_string()
          } else {
            new.name
          },
          "",
          1,
        )
      "ros2msg" => {
        let a = cdr_decoder(old.name, old.data)
        let b = cdr_decoder(new.name, new.data)
        compare_ros_model(report, a, b, a.name, b.name, "", 1)
      }
      "ros1msg" => {
        let a = ros1_decoder(old.name, old.data).layout
        let b = ros1_decoder(new.name, new.data).layout
        compare_ros_model(report, a, b, a.name, b.name, "", 1)
      }
      _ =>
        raise Invalid(
          "schema comparison supports protobuf, ros1msg and ros2msg",
        )
    }
  }
  {
    "encoding": old.encoding,
    "old_name": old.name,
    "new_name": new.name,
    "old_reader_new_data": report.old_reads_new,
    "new_reader_old_data": report.new_reads_old,
    "old_query_new_data": report.old_queries_new,
    "issues": report.issues,
    "issue_count": report.total,
    "truncated": report.total > report.issues.length(),
    "policy": "conservative structural compatibility; application meaning and units require explicit mapping",
    "limitations": [
      "supported decoder models only", "descriptor defaults, custom options and application semantics are not inferred",
      "shared referenced type pairs are reported once",
    ],
  }
}
