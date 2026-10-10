///|
fn example_log() -> @moonmcap.Log {
  let log = @moonmcap.empty_log()
  log.schemas.push({
    id: 1,
    name: "Temperature",
    encoding: "jsonschema",
    data: b"{}",
  })
  log.channels.push({
    id: 1,
    schema_id: 1,
    topic: "/temperature",
    message_encoding: "json",
    metadata: [],
  })
  log.channels.push({
    id: 2,
    schema_id: 0,
    topic: "/events",
    message_encoding: "text",
    metadata: [],
  })
  log.messages.push({
    channel_id: 1,
    sequence: 1U,
    log_time: 30UL,
    publish_time: 29UL,
    data: b"21",
  })
  log.messages.push({
    channel_id: 2,
    sequence: 1U,
    log_time: 10UL,
    publish_time: 10UL,
    data: b"start",
  })
  log.messages.push({
    channel_id: 1,
    sequence: 2U,
    log_time: 20UL,
    publish_time: 19UL,
    data: b"22",
  })
  log.metadata.push({ name: "experiment", entries: [("id", "001")], })
  log
}

///|
test "sequential writer round trips out of order nanosecond timestamps" {
  let log = example_log()
  assert_eq(@moonmcap.read_log(@moonmcap.write_log(log)), log)
  assert_eq(@moonmcap.read_log(@moonmcap.write_log(log, checksums=false)), log)
}

///|
test "writer refuses inconsistent graph and duplicate identifiers" {
  let log = example_log()
  log.channels.push(log.channels[0])
  assert_true((try? @moonmcap.write_log(log)) is Err(_))
  let dangling = @moonmcap.empty_log()
  dangling.messages.push({
    channel_id: 4,
    sequence: 0U,
    log_time: 0UL,
    publish_time: 0UL,
    data: b"",
  })
  assert_true((try? @moonmcap.write_log(dangling)) is Err(_))
}

///|
test "data section corruption is detected" {
  let bytes = @moonmcap.write_log(example_log())
  let records = @moonmcap.scan_file(bytes)
  let message = records
    .iter()
    .find_first(fn(record) { record.opcode == 0x05 })
    .unwrap()
  let offset = message.offset + 9 + 22
  let corrupted = bytes[0:offset].to_owned() +
    b"x" +
    bytes[offset + 1:].to_owned()
  assert_true((try? @moonmcap.read_log(corrupted)) is Err(_))
}
