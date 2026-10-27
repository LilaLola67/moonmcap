///|
test "indexed file has valid physical offsets and summary CRC" {
  let log = example_log()
  let bytes = @moonmcap.write_indexed(log, messages_per_chunk=2)
  assert_eq(@moonmcap.read_log(bytes), log)
  let indexes = @moonmcap.validate_file_indexes(bytes)
  assert_eq(indexes.length(), 2)
  assert_eq(indexes[0].message_index_offsets.length(), 2)
  let records = @moonmcap.scan_file(bytes)
  assert_true(records.any(fn(record) { record.opcode == 0x0e }))
}

///|
test "empty indexed writer falls back to valid empty summary" {
  let bytes = @moonmcap.write_indexed(@moonmcap.empty_log())
  assert_eq(@moonmcap.read_log(bytes).messages.length(), 0)
  assert_eq(@moonmcap.validate_file_indexes(bytes).length(), 0)
}

///|
test "Summary CRC catches corruption after DataEnd" {
  let bytes = @moonmcap.write_indexed(example_log())
  let record = @moonmcap.scan_file(bytes)
    .iter()
    .find_first(fn(record) { record.opcode == 0x08 })
    .unwrap()
  let location = record.offset + 9
  let changed = bytes[0:location].to_owned() +
    b"\xff" +
    bytes[location + 1:].to_owned()
  assert_true(
    (Result::Ok(@moonmcap.read_log(changed)) catch {
      error => Result::Err(error)
    })
    is Err(_),
  )
}
