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
      "field build INPUT NEW_INDEX --field /path... | query INPUT INDEX [--where JSON --select /path --offset N --limit N --output NEW.ndjson]",
    )
    return
  }
  if values.length() < 3 {
    raise @mcap.io_error("field requires mode, input and index path")
  }
  let identity = "moonmcap.std.json-protobuf-cdr.numeric.v1"
  let source = scope.open(values[1])
  if values[0] == "build" {
    let fields = []
    let mut i = 3
    while i < values.length() {
      if values[i] != "--field" || i + 1 >= values.length() {
        raise @mcap.io_error("build accepts repeated --field paths")
      }
      fields.push(values[i + 1])
      i += 2
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
