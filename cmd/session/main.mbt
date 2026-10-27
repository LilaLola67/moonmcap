///|
extern "js" fn args() -> Array[String] =
  #| ()=>process.argv.slice(2)

///|
extern "js" fn pause(milliseconds : Int) -> Unit =
  #| ms=>{if(ms>0)Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0,ms);}

///|
extern "js" fn fail_exit(text : String) -> Unit =
  #| text=>{process.stderr.write(text+'\n');process.exitCode=2;}

///|
fn run(
  values : Array[String],
  scope : @nodeio.FileScope,
  opened : Array[@nodeio.RecordingDirectory],
) -> Unit raise @mcap.McapError {
  if values.is_empty() || values == ["--help"] {
    println(
      "session new|resume DIRECTORY INPUT [--segment-messages N --segment-bytes N --segment-span NS --compression none|lz4|zstd --sequence preserve|continue --pace-ms N] | inspect DIRECTORY",
    )
    return
  }
  if values[0] == "inspect" && values.length() == 2 {
    let directory = @nodeio.open_recording_directory(values[1], resume=true)
    opened.push(directory)
    println(directory.inspect().stringify())
    return
  }
  if values.length() < 3 || !["new", "resume"].contains(values[0]) {
    raise @mcap.io_error("invalid session command")
  }
  let mut options = @mcap.recording_options()
  let mut pace = 0
  let seen : Map[String, Bool] = Map([])
  let mut i = 3
  while i < values.length() {
    let flag = values[i]
    if i + 1 >= values.length() || seen.contains(flag) {
      raise @mcap.io_error("missing or duplicate recording option")
    }
    seen[flag] = true
    let value = values[i + 1]
    if flag == "--sequence" {
      options = {
        ..options,
        sequences: match value {
          "preserve" => @mcap.PreserveSequence
          "continue" => @mcap.ContinueSequence
          _ => raise @mcap.io_error("invalid sequence policy")
        },
      }
    } else if flag == "--compression" {
      let compression = match value {
        "none" => @mcap.Uncompressed
        "lz4" => @mcap.Lz4
        "zstd" => @mcap.Zstd
        _ => raise @mcap.io_error("invalid compression")
      }
      options = { ..options, stream: { ..options.stream, compression, }, }
    } else {
      let number = @mcap.parse_decimal(value)
      match flag {
        "--segment-messages" => {
          if number > 50000UL {
            raise @mcap.io_error("segment count exceeds limit")
          }
          options = { ..options, segment_messages: number.to_int(), }
        }
        "--segment-bytes" =>
          options = { ..options, segment_bytes_target: number, }
        "--segment-span" => options = { ..options, segment_span: number, }
        "--pace-ms" => {
          if number > 1000UL {
            raise @mcap.io_error("pace exceeds limit")
          }
          pace = number.to_int()
        }
        _ => raise @mcap.io_error("unknown recording option")
      }
    }
    i += 2
  }
  let source = scope.open(values[2])
  // Validate before producing a recording; the stable source bridge detects
  // changes between validation, fingerprinting and the physical-order replay.
  ignore(@mcap.scan_source(source, @mcap.source_visitor()))
  let template = @mcap.open_reader(source).query_source().declaration_log()
  options = { ..options, source_identity: @nodeio.source_sha256(source), }
  let directory = @nodeio.open_recording_directory(
    values[1],
    resume=values[0] == "resume",
  )
  opened.push(directory)
  let session = @mcap.open_recording_session(
    template,
    directory.storage(),
    manifest?=directory.manifest(),
    options~,
  )
  let committed = session.committed_messages()
  let position = [0UL]
  try {
    ignore(
      @mcap.scan_source(
        source,
        @mcap.source_visitor(message=fn(message, _) {
          if position[0] >= committed {
            session.append(message)
            pause(pace)
          }
          position[0] += 1UL
        }),
      ),
    )
    if position[0] < committed {
      raise @mcap.io_error("recording source shorter than committed prefix")
    }
    println(session.finish().stringify())
  } catch {
    error => {
      session.cancel()
      raise error
    }
  }
}

///|
fn main {
  let scope = @nodeio.file_scope()
  let directories = []
  run(args(), scope, directories) catch {
    @mcap.Invalid(reason) => fail_exit(reason)
  }
  for directory in directories {
    directory.close() catch {
      @mcap.Invalid(reason) => fail_exit(reason)
    }
  }
  scope.close() catch {
    @mcap.Invalid(reason) => fail_exit(reason)
  }
}
