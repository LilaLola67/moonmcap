///|
test "standard Statistics round trip and truthful counts" {
  let builder = @moonmcap.new_builder()
  let id = builder.register_channel("/sample", "raw")
  for t in [7UL, 2UL, 7UL] { builder.append(id, t, b"x") }
  let log = builder.snapshot()
  let bytes = builder.finish(messages_per_chunk=2)
  let record = @moonmcap.scan_file(bytes).filter(fn(r) { r.opcode == 0x0b })[0]
  let stats = @moonmcap.decode_file_statistics(record.body)
  assert_eq(stats.message_count, 3UL)
  assert_eq(stats.start_time, 2UL)
  assert_eq(stats.chunk_count, 2U)
  assert_eq(@moonmcap.decode_file_statistics(@moonmcap.encode_file_statistics(stats)), stats)
  @moonmcap.validate_file_statistics(log, stats, 2U)
  let rejected = try @moonmcap.validate_file_statistics(log, { ..stats, message_count: 4UL }, 2U) catch { _ => true } noraise { _ => false }
  assert_true(rejected)
  assert_eq(@moonmcap.read_log(bytes), log)
}

///|
test "empty indexed files have standard zero Statistics" {
  let bytes = @moonmcap.write_indexed(@moonmcap.empty_log())
  let record = @moonmcap.scan_file(bytes).filter(fn(r) { r.opcode == 0x0b })[0]
  let stats = @moonmcap.decode_file_statistics(record.body)
  assert_eq(stats.message_count, 0UL)
  assert_eq(stats.start_time, 0UL)
  assert_eq(stats.end_time, 0UL)
  ignore(@moonmcap.read_log(bytes))
}
