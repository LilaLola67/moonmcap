///|
extern "js" fn args() -> Array[String] =
  #| ()=>process.argv.slice(2)

///|
extern "js" fn fail_exit(text : String) -> Unit =
  #| text=>{process.stderr.write(text+'\n');process.exitCode=2;}

///|
fn run(values : Array[String]) -> Unit raise @mcap.McapError {
  if values.is_empty() || values == ["--help"] {
    println(
      "view ROS2MSG PAYLOAD TYPE [--field /path... --inline N --export /binary NEW_FILE --window /binary START COUNT]",
    )
    return
  }
  if values.length() < 3 {
    raise @mcap.io_error("view requires schema, payload and type")
  }
  let fields = []
  let mut inline = 1024
  let exports = []
  let windows = []
  let mut protobuf = false
  let mut i = 3
  while i < values.length() {
    let flag = values[i]
    if flag == "--protobuf" {
      if protobuf {
        raise @mcap.io_error("duplicate protobuf flag")
      }
      protobuf = true
      i += 1
    } else if flag == "--field" && i + 1 < values.length() {
      fields.push(values[i + 1])
      i += 2
    } else if flag == "--inline" && i + 1 < values.length() {
      let n = @mcap.parse_decimal(values[i + 1])
      if n > 100000UL {
        raise @mcap.io_error("inline budget too large")
      }
      inline = n.to_int()
      i += 2
    } else if flag == "--export" && i + 2 < values.length() {
      exports.push((values[i + 1], values[i + 2]))
      i += 3
    } else if flag == "--window" && i + 3 < values.length() {
      let start = @mcap.parse_decimal(values[i + 2])
      let count = @mcap.parse_decimal(values[i + 3])
      if start > 2147483647UL || count > 1024UL {
        raise @mcap.io_error("window exceeds limit")
      }
      windows.push((values[i + 1], start.to_int(), count.to_int()))
      i += 4
    } else {
      raise @mcap.io_error("invalid view option")
    }
  }
  let schema = @nodeio.read_file(values[0], maximum=4 * 1024 * 1024)
  let bytes = @nodeio.read_file(values[1], maximum=16 * 1024 * 1024)
  let selection = {
    ..@mcap.decode_selection(),
    paths: fields,
    inline_array_items: inline,
  }
  if protobuf {
    if !exports.is_empty() || !windows.is_empty() {
      raise @mcap.io_error(
        "protobuf mode supports field projection; binary windows require CDR",
      )
    }
    let value = @mcap.protobuf_decoder(schema, options={
      ..@mcap.protobuf_options(),
      message_bytes: 16 * 1024 * 1024,
    }).decode_selected(values[2], bytes, selection)
    println(value.stringify())
    return
  }
  let view = @mcap.cdr_decoder(values[2], schema).view(bytes, selection~)
  let output = []
  // Validate every export/window before producing any destination.
  let window_values = windows.map(fn(w) { view.read_elements(w.0, w.1, w.2) })
  for (path, _) in exports {
    ignore(view.read_binary(path, 0, 0))
  }
  try {
    for (path, target) in exports {
      let transaction = @nodeio.output_transaction(target)
      output.push(transaction)
      ignore(view.write_binary(path, transaction.sink()))
    }
    for transaction in output {
      transaction.publish()
    }
    let result : Json = {
      "data": view.json(),
      "windows": Json::array(window_values),
      "metrics": view.info(),
    }
    println(result.stringify())
  } catch {
    error => {
      for transaction in output {
        transaction.abort()
      }
      raise error
    }
  }
}

///|
fn main {
  run(args()) catch {
    @mcap.Invalid(reason) => fail_exit(reason)
  }
}
