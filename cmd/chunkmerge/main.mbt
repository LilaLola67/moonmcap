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
      "chunkmerge INPUT... --job DIRECTORY --output NEW.mcap [--resume --checkpoint-final] [external merge budget options] [--max-parts N --record-limit N]",
    )
    return
  }
  let paths = []
  let mut i = 0
  while i < values.length() && !values[i].has_prefix("--") {
    paths.push(values[i])
    i += 1
  }
  if paths.is_empty() {
    raise @mcap.io_error("chunkmerge requires indexed inputs")
  }
  let options = []
  let mut maximum = 10000
  let mut pipeline = false
  let mut records = 100000
  let seen : Map[String, Bool] = Map([])
  while i < values.length() {
    if values[i] == "--checkpoint-final" {
      if pipeline {
        raise @mcap.io_error("duplicate checkpoint-final option")
      }
      pipeline = true
      i += 1
    } else if values[i] == "--max-parts" || values[i] == "--record-limit" {
      let key = values[i]
      if seen.contains(key) || i + 1 >= values.length() {
        raise @mcap.io_error("duplicate or incomplete chunk budget")
      }
      seen[key] = true
      let number = @mcap.parse_decimal(values[i + 1])
      if number > 2147483647UL {
        raise @mcap.io_error("chunk budget exceeds Int")
      }
      if key == "--max-parts" {
        maximum = number.to_int()
      } else {
        records = number.to_int()
      }
      i += 2
    } else {
      options.push(values[i])
      i += 1
    }
  }
  let parsed = @mcap.parse_command(
    ["external-merge"] +
    paths +
    (if paths.length() == 1 { [paths[0]] } else { [] }) +
    options,
  )
  if parsed.job_dir == "" || parsed.external.run_limit > 2147483647 - maximum {
    raise @mcap.io_error("chunkmerge requires --job and bounded run IDs")
  }
  let job = @nodeio.open_merge_job(parsed.job_dir, resume=parsed.resume_job)
  try {
    let checkpoint = job.checkpoint()
    if checkpoint is Some(bytes) &&
      bytes.length() >= 8 &&
      bytes[0:8].to_owned() == b"CJOB2\r\n\x00" {
      pipeline = true
    }
    let sources = paths.map(fn(path) { scope.open(path) })
    let transaction = job.output(parsed.output)
    try {
      let owned = job.storage()
      let final_sort = @mcap.sort_storage(
        fn(id) { (owned.create)(maximum + id) },
        fn(id) { (owned.open)(maximum + id) },
        fn(id) { (owned.remove)(maximum + id) },
      )
      let report = if pipeline {
        @mcap.resumable_chunk_pipeline(
          sources,
          owned,
          transaction.sink(),
          @nodeio.source_sha256,
          fn(bytes) { job.save(bytes) },
          checkpoint?,
          max_parts=maximum,
          options={
            ..parsed.external,
            limits: { ..parsed.external.limits, max_records: records, },
          },
        )
      } else {
        @mcap.resumable_chunk_merge(
          sources,
          owned,
          final_sort,
          transaction.sink(),
          @nodeio.source_sha256,
          fn(bytes) { job.save(bytes) },
          checkpoint?,
          max_parts=maximum,
          options={
            ..parsed.external,
            limits: { ..parsed.external.limits, max_records: records, },
          },
        )
      }
      transaction.publish()
      job.complete(report)
      println(report.stringify())
    } catch {
      error => {
        transaction.abort()
        raise error
      }
    }
    job.close()
  } catch {
    error => {
      job.close() catch {
        _ => ()
      }
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
