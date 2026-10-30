///|
extern "js" fn args() -> Array[String] =
  #| ()=>process.argv.slice(2)

///|
extern "js" fn report_error(message : String) -> Unit =
  #| text=>{process.stderr.write(text+'\n');process.exitCode=2;}

///|
fn small(value : String) -> Int32 raise @mcap.McapError {
  let number = @mcap.parse_decimal(value)
  if number > 2147483647UL {
    raise @mcap.io_error("integer option exceeds Int")
  }
  number.to_int()
}

///|
fn run(
  values : Array[String],
  scope : @nodeio.FileScope,
) -> Unit raise @mcap.McapError {
  if values.is_empty() || values[0] == "--help" {
    println(
      "paged build INPUT OUTPUT TEMP_DIR [external-merge budget options] [--page-entries N]\npaged query INPUT INDEX [index-query options] [--cache-pages N]\npaged check INPUT INDEX [--cache-pages N]",
    )
    return
  }
  let build = values[0] == "build"
  let check = values[0] == "check"
  if (!build && !check && values[0] != "query") ||
    values.length() < (if build { 4 } else { 3 }) {
    raise @mcap.io_error("invalid paged command or input paths")
  }
  let extra = []
  let mut pages = 4
  let mut width = 128
  let mut records = 100000
  let mut seen_records = false
  let mut seen_pages = false
  let mut seen_width = false
  let mut i = if build { 4 } else { 3 }
  while i < values.length() {
    if values[i] == "--cache-pages" ||
      values[i] == "--page-entries" ||
      values[i] == "--record-limit" {
      let key = values[i]
      if i + 1 >= values.length() {
        raise @mcap.io_error("missing page option value")
      }
      if key == "--record-limit" {
        if seen_records {
          raise @mcap.io_error("duplicate record budget")
        }
        records = small(values[i + 1])
        seen_records = true
      } else if key == "--cache-pages" {
        if build || seen_pages {
          raise @mcap.io_error("invalid or duplicate cache page option")
        }
        pages = small(values[i + 1])
        seen_pages = true
      } else {
        if !build || seen_width {
          raise @mcap.io_error("invalid or duplicate index page size")
        }
        width = small(values[i + 1])
        seen_width = true
      }
      i += 2
    } else {
      extra.push(values[i])
      i += 1
    }
  }
  let source = scope.open(values[1])
  if build {
    let parsed = @mcap.parse_command(
      [
        "external-merge",
        values[1],
        values[1],
        "--output",
        values[2],
        "--temp-dir",
        values[3],
      ] +
      extra,
    )
    let transaction = @nodeio.output_transaction(values[2])
    try {
      let report = @mcap.build_disk_sidecar(
        source,
        @nodeio.sort_directory(values[3]),
        transaction.sink(),
        @nodeio.source_sha256,
        identity=@nodeio.file_identity_token(values[1]),
        page_entries=width,
        options={
          ..parsed.external,
          limits: { ..parsed.external.limits, max_records: records, },
        },
      )
      transaction.publish()
      println(report.stringify())
    } catch {
      error => {
        transaction.abort()
        raise error
      }
    }
  } else {
    let parsed = @mcap.parse_command(
      [
        if check {
          "index-check"
        } else {
          "index-query"
        },
        values[1],
        "--index",
        values[2],
      ] +
      extra,
    )
    let reader = @mcap.open_disk_sidecar(
      source,
      scope.open(values[2]),
      @mcap.VerifyContent(@nodeio.source_sha256),
      cache_pages=pages,
      chunk_cache_bytes=parsed.cache_bytes,
      limits={ ..@mcap.default_limits(), max_records: records, },
    )
    if check {
      reader.validate_entries()
      println(reader.info().stringify())
    } else {
      if parsed.content is Some(_) {
        raise @mcap.io_error(
          "use the query_source API for JSON filtering of paged indexes",
        )
      }
      let messages = reader
        .cursor(parsed.query)
        .next_page(parsed.query.limit.max(1))
      let report : Json = {
        "messages": @mcap.messages_json(messages, reader.channels()),
        "index": reader.info(),
      }
      println(report.stringify())
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
