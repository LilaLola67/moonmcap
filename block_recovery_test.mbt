///|
test "independent recovery skips a damaged middle block and retains later blocks" {
  for compression in [@moonmcap.Uncompressed, @moonmcap.Lz4, @moonmcap.Zstd] {
    let builder = @moonmcap.new_builder()
    let channel = builder.register_channel("/recovery", "raw")
    for i = 0; i < 12; i = i + 1 {
      builder.append(channel, i.to_uint64(), b"payload")
    }
    let original = builder.finish(compression~, messages_per_chunk=4)
    let records = @moonmcap.decode_records(
      original[8:original.length() - 8].to_owned(),
    )
    let chunks = records.filter(fn(r) { r.opcode == 0x06 })
    let middle = chunks[1].offset + 8
    for framing in [false, true] {
      let damaged = original.to_array()
      let point = if framing { middle + 1 } else { middle + 9 + 24 }
      damaged[point] = damaged[point] ^ 0x40
      let data = Bytes::from_array(damaged)
      let output = @buffer.Buffer::Buffer()
      let sink = @moonmcap.new_write_sink(fn(bytes) {
        output.write_bytes(bytes[:])
      })
      let report = @moonmcap.recover_blocks(@moonmcap.memory_source(data), sink)
      assert_eq(report.recovered.length(), 2)
      assert_eq(report.skipped.length(), 1)
      assert_eq(report.skipped[0].start, middle.to_uint64())
      assert_eq(report.messages, 8)
      assert_false(report.source_complete)
      let log = @moonmcap.read_log(output.to_bytes())
      assert_eq(log.messages.map(fn(m) { m.log_time }), [
        0UL, 1UL, 2UL, 3UL, 8UL, 9UL, 10UL, 11UL,
      ])
      assert_true(
        @moonmcap.recover_prefix(data).log.messages.length() <
        log.messages.length(),
      )
    }
  }
}

///|
test "independent recovery refuses absent or corrupt summary and absent chunk CRC" {
  let builder = @moonmcap.new_builder()
  let channel = builder.register_channel("/recovery", "raw")
  builder.append(channel, 1UL, b"payload")
  let original = builder.finish(messages_per_chunk=1)
  let records = @moonmcap.decode_records(
    original[8:original.length() - 8].to_owned(),
  )
  let chunk = records.filter(fn(r) { r.opcode == 0x06 })[0]
  for point in [original.length() - 12, chunk.offset + 8 + 9 + 24] {
    let damaged = original.to_array()
    if point == original.length() - 12 {
      damaged[point] = damaged[point] ^ 0x01
    } else {
      for i = 0; i < 4; i = i + 1 {
        damaged[point + i] = 0x00
      }
    }
    try
      @moonmcap.recover_blocks(
        @moonmcap.memory_source(Bytes::from_array(damaged)),
        @moonmcap.new_write_sink(fn(_) { () }),
      )
    catch {
      @moonmcap.Invalid(_) => ()
    } noraise {
      _ => fail("unverified recovery accepted")
    }
  }
  let sequential = @moonmcap.write_log(builder.snapshot())
  try
    @moonmcap.recover_blocks(
      @moonmcap.memory_source(sequential),
      @moonmcap.new_write_sink(fn(_) { () }),
    )
  catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("unindexed recovery accepted")
  }
}
