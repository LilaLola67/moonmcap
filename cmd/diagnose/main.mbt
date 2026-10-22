///|
extern "js" fn arguments() -> Array[String] =
  #| ()=>process.argv.slice(2)

///|
extern "js" fn exit_code(code : Int) -> Unit =
  #| code=>{process.exitCode=code;}

///|
extern "js" fn report_error(message : String) -> Unit =
  #| message=>{process.stderr.write(message+'\n');process.exitCode=2;}

///|
fn integer(value : String) -> Int raise @mcap.McapError {
  let number = @mcap.parse_decimal(value)
  if number == 0UL || number > 2147483647UL {
    raise @mcap.io_error("diagnostic budgets must be positive Int values")
  }
  number.to_int()
}

///|
fn run(
  args : Array[String],
  scope : @nodeio.FileScope,
) -> Unit raise @mcap.McapError {
  if args.length() == 0 || args[0] == "--help" {
    println(
      "diagnose validate|recover INPUT [--record-limit N] [--record-bytes N] [--decoded-bytes N] [--summary-bytes N] [--read-bytes N (validate)] [--max-ranges N (recover)] [--output NEW.mcap (recover)]",
    )
    return
  }
  if args.length() < 2 || !["validate", "recover"].contains(args[0]) {
    raise @mcap.io_error("expected diagnose validate|recover INPUT")
  }
  let options : Map[String, String] = Map([])
  let mut i = 2
  while i < args.length() {
    let key = args[i]
    if ![
        "--record-limit", "--record-bytes", "--decoded-bytes", "--summary-bytes",
        "--read-bytes", "--max-ranges", "--output",
      ].contains(key) ||
      i + 1 >= args.length() ||
      options.contains(key) {
      raise @mcap.io_error("unknown, duplicate or incomplete diagnostic option")
    }
    options[key] = args[i + 1]
    if args[i + 1].has_prefix("--") || args[i + 1] == "" {
      raise @mcap.io_error("missing diagnostic option value")
    }
    i += 2
  }
  let limits = {
    ..@mcap.default_limits(),
    max_records: integer(options.get("--record-limit").unwrap_or("100000")),
    max_record_bytes: integer(
      options.get("--record-bytes").unwrap_or("16777216"),
    ),
    max_decompressed_bytes: integer(
      options.get("--decoded-bytes").unwrap_or("67108864"),
    ),
  }
  let summary_bytes = integer(
    options.get("--summary-bytes").unwrap_or("8388608"),
  )
  let source = scope.open(args[1])
  if args[0] == "validate" {
    if options.contains("--output") || options.contains("--max-ranges") {
      raise @mcap.io_error("output and range options require recover")
    }
    let report = @mcap.diagnose_source(
      source,
      limits~,
      summary_bytes~,
      read_bytes=integer(options.get("--read-bytes").unwrap_or("1048576")),
    )
    println(report.stringify())
    if report is { "valid": false, .. } {
      exit_code(1)
    }
  } else {
    if !options.contains("--output") || options.contains("--read-bytes") {
      raise @mcap.io_error(
        "recover requires --output and does not accept --read-bytes",
      )
    }
    let transaction = @nodeio.output_transaction(options["--output"])
    try {
      let report = @mcap.recover_blocks_detailed(
        source,
        transaction.sink(),
        limits~,
        summary_bytes~,
        max_ranges=integer(options.get("--max-ranges").unwrap_or("10000")),
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
}

///|
fn main {
  let scope = @nodeio.file_scope()
  run(arguments(), scope) catch {
    @mcap.Invalid(reason) => report_error(reason)
  }
  scope.close() catch {
    @mcap.Invalid(reason) => report_error(reason)
  }
}
