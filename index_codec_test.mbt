///|
test "MessageIndex retains unsigned 64 bit timestamps and offsets" {
  let value : @moonmcap.MessageIndex = {
    channel_id: 65535,
    entries: [(18446744073709551615UL, 12UL), (3UL, 0UL)],
  }
  let parsed = @moonmcap.decode_message_index(
    @moonmcap.encode_message_index(value),
  )
  assert_eq(parsed.channel_id, value.channel_id)
  assert_eq(parsed.entries, value.entries)
  assert_true(
    (try? @moonmcap.decode_message_index(b"\x01\x00\x01\x00\x00\x00x"))
    is Err(_),
  )
}

///|
test "MessageIndex entries are checked against chunk boundaries" {
  let body = @moonmcap.encode_message(example_log().messages[0])
  let chunk = @moonmcap.decode_chunk(
    @moonmcap.encode_chunk([{ opcode: 0x05, offset: 0, body, }], 30UL, 30UL),
  )
  @moonmcap.validate_message_index(
    { channel_id: 1, entries: [(30UL, 0UL)], },
    chunk,
  )
  assert_true(
    (try? @moonmcap.validate_message_index(
      { channel_id: 1, entries: [(30UL, 1UL)], },
      chunk,
    ))
    is Err(_),
  )
  assert_true(
    (try? @moonmcap.validate_message_index(
      { channel_id: 1, entries: [], },
      chunk,
    ))
    is Err(_),
  )
}
