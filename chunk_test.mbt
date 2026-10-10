///|
test "chunked files preserve input order across chunk boundaries" {
  let log = example_log()
  let bytes = @moonmcap.write_chunked(log, messages_per_chunk=2)
  assert_eq(@moonmcap.read_log(bytes), log)
  let chunks = @moonmcap.scan_file(bytes).filter(fn(record) { record.opcode == 0x06 })
  assert_eq(chunks.length(), 2)
  let first = @moonmcap.decode_chunk(chunks[0].body)
  assert_eq(first.start_time, 10UL)
  assert_eq(first.end_time, 30UL)
}

///|
test "chunk errors reject forbidden records and timestamps" {
  assert_true((try? @moonmcap.write_chunked(example_log(), messages_per_chunk=0)) is Err(_))
  let forbidden : @moonmcap.Record = {opcode: 0x01, offset: 0, body: b""}
  assert_true((try? @moonmcap.encode_chunk([forbidden], 0UL, 0UL)) is Err(_))
  let message = example_log().messages[0]
  let record : @moonmcap.Record = {opcode: 0x05, offset: 0, body: @moonmcap.encode_message(message)}
  let bad = @moonmcap.encode_chunk([record], 0UL, 10UL)
  assert_true((try? @moonmcap.decode_chunk(bad)) is Err(_))
}
