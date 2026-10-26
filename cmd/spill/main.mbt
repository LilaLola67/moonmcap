///|
extern "js" fn args() -> Array[String] =
  #| ()=>process.argv.slice(2)

///|
extern "js" fn report_error(message : String) -> Unit =
  #| text=>{process.stderr.write(text+'\n');process.exitCode=2;}

///|
fn run(
  values : Array[String],
  scope : @nodeio.FileScope,
) -> Unit raise @mcap.McapError {
  if values.is_empty() || values[0] == "--help" {
    println(
      "spill INPUT... --output NEW.ndjson --temp-dir DIRECTORY [aggregate options] [--run-bytes N --merge-bytes N --fan-in N --spool-bytes N --run-limit N --record-limit N]",
    )
    return
  }
  let grouping = ["aggregate"]
  let sorting = []
  let mut records = 100000
  let mut seen_records = false
  let mut i = 0
  while i < values.length() {
    let key = values[i]
    if [
        "--temp-dir", "--run-bytes", "--merge-bytes", "--fan-in", "--spool-bytes",
        "--run-limit", "--declaration-bytes", "--record-limit",
      ].contains(key) {
      if i + 1 >= values.length() {
        raise @mcap.io_error("missing external aggregation budget")
      }
      if key == "--record-limit" {
        if seen_records {
          raise @mcap.io_error("duplicate input record budget")
        }
        let count = @mcap.parse_decimal(values[i + 1])
        if count > 2147483647UL {
          raise @mcap.io_error("input count exceeds Int")
        }
        records = count.to_int()
        seen_records = true
      } else {
        sorting.push(key)
        sorting.push(values[i + 1])
      }
      i += 2
    } else {
      if key == "--select" || key == "--format" || key == "--full" {
        raise @mcap.io_error(
          "spill uses raw JSON fields and NDJSON group output; unsupported option",
        )
      }
      grouping.push(key)
      i += 1
    }
  }
  let parsed = @mcap.parse_command(grouping)
  if parsed.output == "" {
    raise @mcap.io_error("external aggregation requires --output")
  }
  let budgets = @mcap.parse_command(
    [
      "external-merge",
      parsed.inputs[0],
      parsed.inputs[0],
      "--output",
      parsed.output,
    ] +
    sorting,
  )
  let limits = { ..@mcap.default_limits(), max_records: records, }
  if !parsed.indexes.is_empty() &&
    parsed.indexes.length() != parsed.inputs.length() {
    raise @mcap.io_error("provide one index per input")
  }
  let providers = []
  for n = 0; n < parsed.inputs.length(); n = n + 1 {
    let source = scope.open(parsed.inputs[n])
    let provider = if parsed.indexes.is_empty() {
      @mcap.open_reader(
        source,
        limits~,
        cache_bytes=parsed.cache_bytes,
        fallback_bytes=parsed.max_file_bytes,
      ).query_source()
    } else {
      let index = scope.open(parsed.indexes[n])
      if index.read_exact(0UL, 8) == b"MIDX2\r\n\x00" {
        @mcap.open_disk_sidecar(
          source,
          index,
          @mcap.VerifyContent(@nodeio.source_sha256),
          limits~,
          chunk_cache_bytes=parsed.cache_bytes,
        ).query_source()
      } else {
        let bytes = @nodeio.read_file(
          parsed.indexes[n],
          maximum=parsed.index_bytes,
        )
        @mcap.open_sidecar(
          source,
          bytes,
          @mcap.VerifyContent(@nodeio.source_sha256),
          limits~,
          cache_bytes=parsed.cache_bytes,
        ).query_source()
      }
    }
    providers.push(
      match parsed.content {
        None => provider
        Some(filter) => provider.filter_json(filter)
      },
    )
  }
  let dataset = @mcap.open_dataset(providers, limits~)
  let transaction = @nodeio.output_transaction(parsed.output)
  try {
    let report = @mcap.aggregate_external(
      dataset.cursor(parsed.query),
      dataset.channels(),
      @nodeio.sort_directory(budgets.temp_dir),
      transaction.sink(),
      aggregate=parsed.aggregate,
      external={ ..budgets.external, limits, },
    )
    transaction.publish()
    println(report.stringify())
  } catch {
    error => {
      transaction.abort()
      raise error
    }
  }
}

///|
fn main {
  let scope = @nodeio.file_scope()
  run(args(), scope) catch {
    @mcap.Invalid(reason) => report_error(reason)
  }
  scope.close() catch {
    @mcap.Invalid(reason) => report_error(reason)
  }
}
