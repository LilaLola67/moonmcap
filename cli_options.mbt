///|
pub(all) struct CommandOptions {
  command : String
  inputs : Array[String]
  output : String
  query : Query
  compression : Compression
  format : String
  max_file_bytes : Int
  cache_bytes : Int
  chunk_messages : Int
  full : Bool
}

///|
fn decimal_int(text : String) -> Int raise McapError {
  bounded_length(parse_decimal(text), 2147483647, "integer argument")
}

///|
pub fn parse_command(args : Array[String]) -> CommandOptions raise McapError {
  if args.is_empty() || args[0] == "--help" || args[0] == "help" {
    return {
      command: "help",
      inputs: [],
      output: "",
      query: all_query(),
      compression: Uncompressed,
      format: "json",
      max_file_bytes: 64 * 1024 * 1024,
      cache_bytes: 8 * 1024 * 1024,
      chunk_messages: 256,
      full: false,
    }
  }
  let command = args[0]
  if !["inspect", "query", "slice", "merge", "recover", "rewrite"].contains(
      command,
    ) {
    raise Invalid("unknown command: \{command}")
  }
  let inputs = []
  let topics = []
  let values : Map[String, String] = Map([])
  let flags : Map[String, Bool] = Map([])
  let mut positional = false
  let mut i = 1
  while i < args.length() {
    let arg = args[i]
    if !positional && arg == "--" {
      positional = true
      i += 1
      continue
    }
    if !positional && arg.has_prefix("--") {
      if ["--descending", "--full"].contains(arg) {
        if flags.contains(arg) {
          raise Invalid("duplicate option: \{arg}")
        }
        flags[arg] = true
      } else {
        if ![
            "--topic", "--start", "--end", "--offset", "--limit", "--output", "--compression",
            "--format", "--max-file-bytes", "--cache-bytes", "--chunk-messages",
          ].contains(arg) {
          raise Invalid("unknown option: \{arg}")
        }
        i += 1
        if i >= args.length() || args[i].has_prefix("--") {
          raise Invalid("missing option value: \{arg}")
        }
        if arg == "--topic" {
          topics.push(args[i])
        } else {
          if values.contains(arg) {
            raise Invalid("duplicate option: \{arg}")
          }
          values[arg] = args[i]
        }
      }
    } else {
      inputs.push(arg)
    }
    i += 1
  }
  if (command == "merge" && inputs.length() < 2) ||
    (command != "merge" && inputs.length() != 1) {
    raise Invalid("wrong number of input paths")
  }
  if !["query", "slice"].contains(command) &&
    (
      !topics.is_empty() ||
      flags.contains("--descending") ||
      ["--start", "--end", "--offset", "--limit"].any(fn(key) {
        values.contains(key)
      })
    ) {
    raise Invalid("message filters are only available for query and slice")
  }
  if command != "query" &&
    (flags.contains("--full") || values.contains("--cache-bytes")) {
    raise Invalid("--full and --cache-bytes are query options")
  }
  if ["query", "inspect"].contains(command) &&
    (values.contains("--compression") || values.contains("--chunk-messages")) {
    raise Invalid("compression and chunk sizing are MCAP output options")
  }
  let start_time = parse_decimal(values.get("--start").unwrap_or("0"))
  let end_time = parse_decimal(
    values.get("--end").unwrap_or("18446744073709551615"),
  )
  if start_time > end_time {
    raise Invalid("inverted time interval")
  }
  let compression = match values.get("--compression").unwrap_or("none") {
    "none" => Uncompressed
    "lz4" => Lz4
    "zstd" => Zstd
    _ => raise Invalid("compression must be none, lz4 or zstd")
  }
  let format = values.get("--format").unwrap_or("json")
  if format != "json" && format != "csv" {
    raise Invalid("format must be json or csv")
  }
  if format == "csv" && command != "query" {
    raise Invalid("CSV is available for query")
  }
  let output = values.get("--output").unwrap_or("")
  if !["query", "inspect"].contains(command) && output == "" {
    raise Invalid("--output is required")
  }
  let chunk_messages = decimal_int(
    values.get("--chunk-messages").unwrap_or("256"),
  )
  if chunk_messages == 0 {
    raise Invalid("--chunk-messages must be positive")
  }
  {
    command,
    inputs,
    output,
    compression,
    format,
    query: {
      topics,
      start_time,
      end_time,
      descending: flags.contains("--descending"),
      offset: decimal_int(values.get("--offset").unwrap_or("0")),
      limit: decimal_int(values.get("--limit").unwrap_or("100000")),
    },
    max_file_bytes: decimal_int(
      values.get("--max-file-bytes").unwrap_or("67108864"),
    ),
    cache_bytes: decimal_int(values.get("--cache-bytes").unwrap_or("8388608")),
    chunk_messages,
    full: flags.contains("--full"),
  }
}
