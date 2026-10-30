///|
pub(all) struct CommandOptions {
  command : String
  inputs : Array[String]
  output : String
  query : Query
  compression : Compression
  format : String
  max_file_bytes : Int32
  cache_bytes : Int32
  chunk_messages : Int32
  full : Bool
  transform : TransformOptions
  external : ExternalOptions
  temp_dir : String
  indexes : Array[String]
  fast_index : Bool
  index_bytes : Int32
  content : JsonFilter?
  job_dir : String
  resume_job : Bool
  aggregate : AggregateOptions
  diff : DiffOptions
}

///|
fn decimal_int(text : String) -> Int32 raise McapError {
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
      transform: transform_options(),
      external: external_options(),
      temp_dir: "",
      indexes: [],
      fast_index: false,
      index_bytes: 32 * 1024 * 1024,
      content: None,
      job_dir: "",
      resume_job: false,
      aggregate: aggregate_options(),
      diff: diff_options(),
    }
  }
  let command = args[0]
  if ![
      "inspect", "query", "slice", "merge", "recover", "rewrite", "transform", "validate",
      "reindex", "external-merge", "recover-blocks", "stream-transform", "dataset-query",
      "aggregate", "index-build", "index-rebuild", "index-check", "index-query",
      "job-inspect", "job-clean", "diff",
    ].contains(command) {
    raise Invalid("unknown command: \{command}")
  }
  let inputs = []
  let indexes = []
  let projections = []
  let group_paths = []
  let topics = []
  let renames = []
  let attachment_names = []
  let attachment_types = []
  let metadata_names = []
  let metadata_entries = []
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
      if [
          "--descending", "--full", "--fast-index", "--skip-invalid-json", "--resume",
          "--no-topic-group", "--json-payloads",
        ].contains(arg) {
        if flags.contains(arg) {
          raise Invalid("duplicate option: \{arg}")
        }
        flags[arg] = true
      } else {
        if ![
            "--topic", "--start", "--end", "--offset", "--limit", "--output", "--compression",
            "--format", "--max-file-bytes", "--cache-bytes", "--chunk-messages",
            "--rename", "--sample-period", "--sample-origin", "--align-start", "--attachment-name",
            "--attachment-media-type", "--attachment-start", "--attachment-end",
            "--metadata-name", "--metadata-match", "--temp-dir", "--run-bytes", "--merge-bytes",
            "--fan-in", "--spool-bytes", "--run-limit", "--declaration-bytes", "--index",
            "--index-bytes", "--where", "--select", "--job", "--value", "--group",
            "--window", "--origin", "--max-groups", "--group-bytes", "--bucket-bytes",
            "--aux-bytes", "--max-details", "--detail-bytes",
          ].contains(arg) {
          raise Invalid("unknown option: \{arg}")
        }
        i += 1
        if i >= args.length() || args[i].has_prefix("--") {
          raise Invalid("missing option value: \{arg}")
        }
        if arg == "--group" {
          group_paths.push(args[i])
        } else if arg == "--select" {
          projections.push(args[i])
        } else if arg == "--index" {
          indexes.push(args[i])
        } else if arg == "--topic" {
          topics.push(args[i])
        } else if arg == "--rename" || arg == "--metadata-match" {
          let pair = match args[i].split_once("=") {
            Some((left, right)) => (left.to_string(), right.to_string())
            None => raise Invalid("mapping must use KEY=VALUE")
          }
          if arg == "--rename" {
            renames.push(pair)
          } else {
            metadata_entries.push(pair)
          }
        } else if arg == "--attachment-name" {
          attachment_names.push(args[i])
        } else if arg == "--attachment-media-type" {
          attachment_types.push(args[i])
        } else if arg == "--metadata-name" {
          metadata_names.push(args[i])
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
  if (["merge", "external-merge"].contains(command) && inputs.length() < 2) ||
    (["dataset-query", "aggregate"].contains(command) && inputs.is_empty()) ||
    (command == "diff" && inputs.length() != 2) ||
    (
      !["merge", "external-merge", "dataset-query", "aggregate", "diff"].contains(
        command,
      ) &&
      inputs.length() != 1
    ) {
    raise Invalid("wrong number of input paths")
  }
  if ![
      "query", "slice", "transform", "stream-transform", "dataset-query", "aggregate",
      "index-query",
    ].contains(command) &&
    (
      !topics.is_empty() ||
      flags.contains("--descending") ||
      ["--start", "--end", "--offset", "--limit"].any(fn(key) {
        values.contains(key)
      })
    ) {
    raise Invalid(
      "message filters are only available for query, slice and transform",
    )
  }
  if !["transform", "stream-transform"].contains(command) &&
    (
      !renames.is_empty() ||
      !attachment_names.is_empty() ||
      !attachment_types.is_empty() ||
      !metadata_names.is_empty() ||
      !metadata_entries.is_empty() ||
      [
        "--sample-period", "--sample-origin", "--align-start", "--attachment-start",
        "--attachment-end",
      ].any(fn(key) { values.contains(key) })
    ) {
    raise Invalid("batch options require the transform command")
  }
  if !["query", "dataset-query", "aggregate", "index-query", "index-check"].contains(
      command,
    ) &&
    (flags.contains("--full") || values.contains("--cache-bytes")) {
    raise Invalid("--full and --cache-bytes are query options")
  }
  if !["external-merge", "stream-transform"].contains(command) &&
    [
      "--temp-dir", "--run-bytes", "--merge-bytes", "--fan-in", "--spool-bytes",
      "--run-limit", "--declaration-bytes",
    ].any(fn(key) { values.contains(key) }) {
    raise Invalid("external storage options require external-merge")
  }
  if ["external-merge", "stream-transform"].contains(command) &&
    (
      (
        values.get("--temp-dir").unwrap_or("") == "" &&
        values.get("--job").unwrap_or("") == ""
      ) ||
      values.contains("--max-file-bytes")
    ) {
    raise Invalid(
      "external-merge requires --temp-dir and uses run/merge budgets instead of --max-file-bytes",
    )
  }
  if [
      "query", "dataset-query", "aggregate", "index-query", "index-check", "index-build",
      "index-rebuild", "inspect", "validate",
    ].contains(command) &&
    (values.contains("--compression") || values.contains("--chunk-messages")) {
    raise Invalid("compression and chunk sizing are MCAP output options")
  }
  let start_time = parse_decimal(values.get("--start").unwrap_or("0"))
  if values.contains("--job") || flags.contains("--resume") {
    if command != "external-merge" ||
      values.get("--job").unwrap_or("") == "" ||
      values.contains("--temp-dir") {
      raise Invalid(
        "--job/--resume require external-merge --job DIR without --temp-dir",
      )
    }
  }
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
  if !["json", "csv", "ndjson"].contains(format) {
    raise Invalid("format must be json, csv or ndjson")
  }
  if (format == "csv" && command != "query") ||
    (
      format == "ndjson" &&
      !["query", "dataset-query", "aggregate", "index-query"].contains(command)
    ) {
    raise Invalid("CSV/NDJSON is available for query")
  }
  let output = values.get("--output").unwrap_or("")
  if command != "diff" &&
    (
      flags.contains("--json-payloads") ||
      ["--bucket-bytes", "--aux-bytes", "--max-details", "--detail-bytes"].any(fn(
        k,
      ) {
        values.contains(k)
      })
    ) {
    raise Invalid("diff options require diff command")
  }
  if command == "diff" && format != "json" {
    raise Invalid("diff produces JSON")
  }
  if command != "aggregate" &&
    (
      !group_paths.is_empty() ||
      flags.contains("--no-topic-group") ||
      ["--value", "--window", "--origin", "--max-groups", "--group-bytes"].any(fn(
        key,
      ) {
        values.contains(key)
      })
    ) {
    raise Invalid("aggregate options require aggregate command")
  }
  if command == "aggregate" && (format != "json" || !projections.is_empty()) {
    raise Invalid("aggregate produces JSON groups and does not take --select")
  }
  let content = if values.contains("--where") ||
    !projections.is_empty() ||
    flags.contains("--skip-invalid-json") {
    if ![
        "query", "dataset-query", "aggregate", "index-query", "stream-transform",
      ].contains(command) ||
      format == "csv" {
      raise Invalid(
        "JSON content options require JSON queries or stream-transform",
      )
    }
    if command == "stream-transform" && !projections.is_empty() {
      raise Invalid(
        "projection is for query output; MCAP payloads stay unchanged",
      )
    }
    Some(
      json_filter(
        values.get("--where").unwrap_or("true"),
        select=projections,
        invalid=if flags.contains("--skip-invalid-json") {
          JsonSkip
        } else {
          JsonError
        },
      ),
    )
  } else {
    None
  }
  if ![
      "query", "dataset-query", "aggregate", "index-query", "index-check", "inspect",
      "validate", "job-inspect", "job-clean", "diff",
    ].contains(command) &&
    output == "" {
    raise Invalid("--output is required")
  }
  let chunk_messages = decimal_int(
    values.get("--chunk-messages").unwrap_or("256"),
  )
  if chunk_messages == 0 {
    raise Invalid("--chunk-messages must be positive")
  }
  let query : Query = {
    topics,
    start_time,
    end_time,
    descending: flags.contains("--descending"),
    offset: decimal_int(values.get("--offset").unwrap_or("0")),
    limit: decimal_int(values.get("--limit").unwrap_or("100000")),
  }
  if ["index-query", "index-check"].contains(command) {
    if indexes.length() != 1 {
      raise Invalid("index-query/index-check requires one --index")
    }
  } else if ["dataset-query", "aggregate"].contains(command) {
    if !indexes.is_empty() && indexes.length() != inputs.length() {
      raise Invalid("dataset sidecars must match input count and order")
    }
  } else if !indexes.is_empty() {
    raise Invalid("--index requires index-query, index-check or dataset-query")
  }
  if flags.contains("--fast-index") && indexes.is_empty() {
    raise Invalid("--fast-index requires sidecar inputs")
  }
  if values.contains("--index-bytes") &&
    ![
      "index-build", "index-rebuild", "index-query", "index-check", "dataset-query",
      "aggregate",
    ].contains(command) {
    raise Invalid("--index-bytes requires index command or dataset-query")
  }
  {
    command,
    inputs,
    output,
    compression,
    format,
    query,
    indexes,
    content,
    job_dir: values.get("--job").unwrap_or(""),
    resume_job: flags.contains("--resume"),
    diff: {
      ..diff_options(),
      json_payloads: flags.contains("--json-payloads"),
      bucket_bytes: decimal_int(
        values.get("--bucket-bytes").unwrap_or("8388608"),
      ),
      auxiliary_bytes: decimal_int(
        values.get("--aux-bytes").unwrap_or("8388608"),
      ),
      max_details: decimal_int(values.get("--max-details").unwrap_or("64")),
      detail_bytes: decimal_int(
        values.get("--detail-bytes").unwrap_or("262144"),
      ),
      fallback_bytes: decimal_int(
        values.get("--max-file-bytes").unwrap_or("67108864"),
      ),
    },
    aggregate: {
      ..aggregate_options(),
      value_path: values.get("--value").unwrap_or(""),
      group_paths,
      by_topic: !flags.contains("--no-topic-group"),
      window: parse_decimal(values.get("--window").unwrap_or("0")),
      origin: parse_decimal(values.get("--origin").unwrap_or("0")),
      max_groups: decimal_int(values.get("--max-groups").unwrap_or("1024")),
      group_bytes: decimal_int(values.get("--group-bytes").unwrap_or("8388608")),
      invalid: if flags.contains("--skip-invalid-json") {
        JsonSkip
      } else {
        JsonError
      },
    },
    fast_index: flags.contains("--fast-index"),
    index_bytes: decimal_int(values.get("--index-bytes").unwrap_or("33554432")),
    temp_dir: values.get("--temp-dir").unwrap_or(""),
    external: {
      ..external_options(),
      run_bytes: decimal_int(values.get("--run-bytes").unwrap_or("2097152")),
      merge_bytes: decimal_int(
        values.get("--merge-bytes").unwrap_or("16777216"),
      ),
      fan_in: decimal_int(values.get("--fan-in").unwrap_or("8")),
      spool_bytes: parse_decimal(
        values.get("--spool-bytes").unwrap_or("536870912"),
      ),
      run_limit: decimal_int(values.get("--run-limit").unwrap_or("32768")),
      declaration_bytes: decimal_int(
        values.get("--declaration-bytes").unwrap_or("8388608"),
      ),
      output: { ..stream_options(), compression, chunk_messages, },
    },
    transform: {
      query,
      topic_map: renames,
      sample_period: parse_decimal(values.get("--sample-period").unwrap_or("0")),
      sample_origin: parse_decimal(values.get("--sample-origin").unwrap_or("0")),
      align_start: match values.get("--align-start") {
        Some(value) => Some(parse_decimal(value))
        None => None
      },
      attachment_names,
      attachment_media_types: attachment_types,
      attachment_start: parse_decimal(
        values.get("--attachment-start").unwrap_or("0"),
      ),
      attachment_end: parse_decimal(
        values.get("--attachment-end").unwrap_or("18446744073709551615"),
      ),
      metadata_names,
      metadata_entries,
    },
    max_file_bytes: decimal_int(
      values.get("--max-file-bytes").unwrap_or("67108864"),
    ),
    cache_bytes: decimal_int(values.get("--cache-bytes").unwrap_or("8388608")),
    chunk_messages,
    full: flags.contains("--full"),
  }
}
