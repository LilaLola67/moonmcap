///|
test "worker expansion preserves chunk records and verifies original metadata and CRC" {
  let log = cursor_fixture()
  for compression in [@moonmcap.Uncompressed, @moonmcap.Lz4, @moonmcap.Zstd] {
    let source = @moonmcap.memory_source(
      @moonmcap.write_indexed(log, compression~, messages_per_chunk=11),
    )
    let cursor = @moonmcap.open_reader(source, cache_bytes=0).cursor(
      @moonmcap.all_query(),
    )
    let requests = cursor.upcoming_reads(2)
    assert_true(!requests.is_empty())
    let messages = []
    while true {
      match cursor.poll() {
        @moonmcap.FileDone => break
        @moonmcap.FileMessage(message) => messages.push(message)
        @moonmcap.FileRead(span) => {
          let original = source.read_exact(span.offset, span.length)
          let records = @moonmcap.decode_records(original)
          let expanded = @moonmcap.expand_worker_chunk(records[0].body)
          let before = @moonmcap.decode_chunk(records[0].body)
          let after = @moonmcap.decode_chunk(expanded)
          assert_eq(
            (before.start_time, before.end_time),
            (after.start_time, after.end_time),
          )
          assert_eq(
            before.records.map(fn(r) { (r.opcode, r.offset, r.body) }),
            after.records.map(fn(r) { (r.opcode, r.offset, r.body) }),
          )
          cursor.feed_expanded(original, expanded)
        }
      }
    }
    assert_eq(
      messages,
      @moonmcap.build_query_index(log).query(@moonmcap.all_query()),
    )
  }
}

///|
test "worker codec handler rejects oversized and malformed jobs" {
  let handler = @moonmcap.codec_worker_handler(
    input_bytes=128,
    output_bytes=1024,
    context_bytes=128,
  )
  assert_true(handler("{}").has_prefix("!"))
  let request : Json = {
    "op": "expand",
    "data": "00",
    "output_bytes": 100,
    "limits": {
      "record": 128,
      "string": 128,
      "records": 100,
      "decompressed": 60,
    },
  }
  assert_true(handler(request.stringify()).has_prefix("!"))
}
