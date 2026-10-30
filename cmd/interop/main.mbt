///|
extern "js" fn argument(index : Int32) -> String =
  #| (index) => process.argv.slice(2)[index] ?? ""

///|
extern "js" fn load_hex(path : String) -> String =
  #| (path) => process.getBuiltinModule('node:fs').readFileSync(path).toString('hex')

///|
extern "js" fn save_hex(path : String, hex : String) -> Unit =
  #| (path, hex) => process.getBuiltinModule('node:fs').writeFileSync(path, Buffer.from(hex, 'hex'))

///|
fn main {
  try {
    let input_path = argument(0)
    let output_path = argument(1)
    if input_path == "" || output_path == "" {
      abort(
        "usage: interop INPUT OUTPUT [none|lz4|zstd] [rewrite|slice|merge|recover] [SECOND_INPUT]",
      )
    }
    let compression : @lib.Compression = match argument(2) {
      "" | "none" => @lib.Uncompressed
      "lz4" => @lib.Lz4
      "zstd" => @lib.Zstd
      _ => abort("unknown compression mode")
    }
    let bytes = @lib.decode_hex(
      load_hex(input_path),
      max_bytes=64 * 1024 * 1024,
    )
    let operation = argument(3)
    let log = if operation == "recover" {
      let report = @lib.recover_prefix(bytes)
      println("recovery_stop=\{report.stop_offset}")
      report.log
    } else {
      @lib.read_log(bytes)
    }
    let result = match operation {
      "slice" =>
        @lib.slice_log(log, {
          ..@lib.all_query(),
          topics: ["/imu"],
          start_time: 20000000000UL,
          end_time: 30000000000UL,
        })
      "merge" =>
        @lib.merge_logs([
          log,
          @lib.read_log(
            @lib.decode_hex(load_hex(argument(4)), max_bytes=64 * 1024 * 1024),
          ),
        ])
      "" | "rewrite" | "recover" => log
      _ => abort("unknown operation")
    }
    let output = @lib.write_indexed(result, compression~, messages_per_chunk=4)
    ignore(@lib.validate_file_indexes(output))
    ignore(@lib.validate_summary(output))
    ignore(@lib.validate_attachment_indexes(output))
    ignore(@lib.validate_metadata_indexes(output))
    save_hex(output_path, @lib.encode_hex(output))
    println(
      "messages=\{result.messages.length()} attachments=\{result.attachments.length()} metadata=\{result.metadata.length()}",
    )
  } catch {
    @lib.Invalid(reason) => {
      println("error=\{reason}")
      abort(reason)
    }
  }
}
