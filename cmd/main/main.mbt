///|
/// Entry point for the CLI or runnable program in this template.
/// Keep main focused; move logic into the library package.
/// Run with `moon run cmd/main` from the project root.
fn main {
  try {
    let recorder = @lib.new_builder()
    let schema = recorder.register_schema("Event", "jsonschema", b"{}")
    let channel = recorder.register_channel("/events", "json", schema_id=schema)
    for i = 0; i < 6; i = i + 1 {
      recorder.append(
        channel,
        (i + 1).to_uint64() * 1000000000UL,
        b"{\"value\":1}",
      )
    }
    recorder.add_attachment({
      log_time: 1UL,
      create_time: 1UL,
      name: "calibration",
      media_type: "application/json",
      data: b"{}",
    })
    let bytes = recorder.finish(messages_per_chunk=2)
    let log = @lib.read_log(bytes)
    println(@lib.stats_report(@lib.summarize(log)))
    let query = {
      ..@lib.all_query(),
      topics: ["/events"],
      start_time: 2000000000UL,
      end_time: 4000000000UL,
    }
    let result = @lib.query_file(bytes, query)
    println(
      "query messages=\{result.messages.length()}, candidate chunks=\{result.diagnostics.candidate_chunks}",
    )
    let sliced = @lib.slice_log(log, query)
    println(
      "slice messages=\{sliced.messages.length()}, attachments=\{sliced.attachments.length()}",
    )
    println("MCAP_HEX=\{@lib.encode_hex(bytes)}")
  } catch {
    @lib.Invalid(reason) => abort("demo failed: \{reason}")
  }
}
