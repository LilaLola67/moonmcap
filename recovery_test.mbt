///|
test "recovery stops at truncation and repairs retained prefix" {
  let builder = @moonmcap.new_builder()
  let id = builder.register_channel("/sample", "raw")
  builder.append(id, 1UL, b"first")
  builder.append(id, 2UL, b"second")
  let bytes = builder.finish(indexed=false)
  let records = @moonmcap.scan_file(bytes)
  let second = records.filter(fn(r) { r.opcode == 0x05 })[1]
  let truncated = bytes[0:second.offset + 10].to_owned()
  let (repaired, report) = @moonmcap.repair_prefix(truncated)
  assert_false(report.complete)
  assert_eq(report.stop_offset, second.offset)
  assert_eq(@moonmcap.read_log(repaired).messages.length(), 1)
  assert_true(@moonmcap.recover_prefix(bytes).complete)
}

///|
test "recovery chunk is atomic when declaration graph fails" {
  let header = @moonmcap.frame(
    0x01,
    @moonmcap.encode_header({ profile: "", library: "test", }),
  )
  let message : @moonmcap.Message = {
    channel_id: 42,
    sequence: 0U,
    log_time: 1UL,
    publish_time: 1UL,
    data: b"",
  }
  let chunk = @moonmcap.encode_chunk(
    [{ opcode: 0x05, offset: 0, body: @moonmcap.encode_message(message), }],
    1UL,
    1UL,
  )
  let report = @moonmcap.recover_prefix(
    @moonmcap.magic() + header + @moonmcap.frame(0x06, chunk),
  )
  assert_eq(report.log.messages.length(), 0)
  assert_eq(report.recovered_records, 1)
}
