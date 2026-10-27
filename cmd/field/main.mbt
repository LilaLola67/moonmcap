///|
extern "js" fn args() -> Array[String] =
  #| ()=>process.argv.slice(2)

///|
extern "js" fn error_exit(text : String) -> Unit =
  #| text=>{process.stderr.write(text+'\n');process.exitCode=2;}

///|
extern "js" fn report(text : String) -> Unit =
  #| text=>process.stderr.write(text+'\n')

///|
fn run(
  values : Array[String],
  scope : @nodeio.FileScope,
) -> Unit raise @mcap.McapError {
  if values.is_empty() || values == ["--help"] {
    println(
      "field build INPUT NEW_INDEX --field /path... | build-paged INPUT NEW_INDEX --field /path... [--page-bytes N --page-zones N --no-bloom] | query INPUT MFLD1 [query flags]; use cmd/query for MFLD2",
    )
    return
  }
  if values.length() < 3 {
    raise @mcap.io_error("field requires mode, input and index path")
  }
  let identity = "moonmcap.std.json-protobuf-cdr.numeric.v1"
  let source = scope.open(values[1])
  if values[0] == "build" || values[0] == "build-paged" {
    let fields = []
    let mut page_bytes = 65536
    let mut page_zones = 32
    let mut bloom = true
    let mut i = 3
    while i < values.length() {
      if values[i] == "--no-bloom" && values[0] == "build-paged" {
        bloom = false
        i += 1
        continue
      }
      if ["--page-bytes", "--page-zones"].contains(values[i]) &&
        values[0] == "build-paged" {
        if i + 1 >= values.length() {
          raise @mcap.io_error("missing page option")
        }
        let n = @mcap.parse_decimal(values[i + 1])
        if n > 2147483647UL {
          raise @mcap.io_error("page option overflow")
        }
        if values[i] == "--page-bytes" {
          page_bytes = n.to_int()
        } else {
          page_zones = n.to_int()
        }
        i += 2
        continue
      }
      if values[i] != "--field" || i + 1 >= values.length() {
        raise @mcap.io_error("build accepts repeated --field paths")
      }
      fields.push(values[i + 1])
      i += 2
    }
    if values[0] == "build-paged" {
      let output = @nodeio.output_transaction(values[2])
      try {
        let info = @mcap.build_paged_field_index(
          source,
          output.sink(),
          fields,
          @nodeio.source_sha256,
          identity,
          page_bytes~,
          page_zones~,
          bloom~,
        )
        output.publish()
        println(info.stringify())
      } catch {
        error => {
          output.abort()
          raise error
        }
      }
      return
    }
    let index = @mcap.build_field_index(
      source, fields, @nodeio.source_sha256, identity,
    )
    @nodeio.write_atomic_new(values[2], index.to_bytes())
    println(index.info().stringify())
    return
  }
  if values[0] != "query" {
    raise @mcap.io_error("unknown field command")
  }
  let rest = values[3:].to_array()
  let parsed = @mcap.parse_command(["dataset-query", values[1]] + rest)
  if parsed.full || !parsed.indexes.is_empty() || parsed.inputs.length() != 1 {
    raise @mcap.io_error("field query requires a single selective source")
  }
  let digest = @nodeio.source_sha256(source)
  let index = @mcap.open_field_index(
    source,
    @nodeio.read_file(values[2], maximum=16 * 1024 * 1024),
    fn(_) { digest },
    identity,
  )
  let filter = match parsed.content {
    Some(filter) => filter
    None => @mcap.json_filter("true")
  }
  let plan = @mcap.open_reader(
    source,
    cache_bytes=parsed.cache_bytes,
    fallback_bytes=parsed.max_file_bytes,
  ).plan_field_query(index, filter, fn(_) { digest }, identity)
  let dataset = @mcap.open_dataset([plan.query_source()])
  let cursor = dataset.cursor(parsed.query)
  let transaction = if parsed.output == "" {
    None
  } else {
    Some(@nodeio.output_transaction(parsed.output))
  }
  try {
    while cursor.next_record() is Some(record) {
      let line = @mcap.json_query_record(record, dataset.channels(), filter).stringify() +
        "\n"
      match transaction {
        None => println(line[0:line.length() - 1])
        Some(output) => output.sink().write(@utf8.encode(line[:]))
      }
    }
    if transaction is Some(output) {
      output.publish()
    }
    report(plan.info().stringify())
  } catch {
    error => {
      cursor.cancel()
      if transaction is Some(output) {
        output.abort()
      }
      raise error
    }
  }
}

///|
fn main {
  let scope = @nodeio.file_scope()
  run(args(), scope) catch {
    @mcap.Invalid(reason) => error_exit(reason)
  }
  scope.close() catch {
    @mcap.Invalid(reason) => error_exit(reason)
  }
}
