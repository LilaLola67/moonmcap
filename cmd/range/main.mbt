///|
extern "js" fn argument(index : Int) -> String =
  #| (index) => process.argv.slice(2)[index] ?? ''

///|
fn main {
  try {
    let reader = @mcap.open_reader(@nodeio.open_file(argument(0)))
    let query = {
      ..@mcap.all_query(),
      topics: [argument(1)],
      start_time: @mcap.parse_decimal(argument(2)),
      end_time: @mcap.parse_decimal(argument(3)),
    }
    let first = reader.query(query)
    let second = reader.query(query)
    if first != second {
      abort("cached query differs")
    }
    let d = reader.diagnostics()
    let report : Json = {
      "messages": first.length(),
      "bytes_read": d.bytes_read.to_string(),
      "source_bytes": @nodeio.open_file(argument(0)).size().to_string(),
      "decoded_chunks": d.decoded_chunks,
      "cache_hits": d.cache_hits,
      "cache_bytes": d.cache_bytes,
      "peak_cache_bytes": d.peak_cache_bytes,
      "full_validation": d.full_validation,
      "times": first.map(fn(m) { m.log_time.to_string() }),
    }
    println(report.stringify())
  } catch {
    @mcap.Invalid(reason) => abort(reason)
  }
}
