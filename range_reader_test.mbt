///|
test "range query matches full scan for all compression modes and directions" {
  let log = cursor_fixture()
  let reference = @moonmcap.build_query_index(log)
  for compression in [@moonmcap.Uncompressed, @moonmcap.Lz4, @moonmcap.Zstd] {
    let bytes = @moonmcap.write_indexed(log, compression~, messages_per_chunk=9)
    let reader = @moonmcap.open_reader(@moonmcap.memory_source(bytes))
    for descending in [false, true] {
      for topics in [[], ["/shared", "/shared"], ["/missing"], ["/topic/5"]] {
        let query = {
          ..@moonmcap.all_query(),
          topics,
          start_time: 8UL,
          end_time: 15UL,
          descending,
          offset: 2,
          limit: 19,
        }
        assert_eq(reader.query(query), reference.query(query))
      }
    }
    assert_true(reader.diagnostics().indexed)
    assert_true(!reader.diagnostics().full_validation)
    reader.validate_all()
    assert_true(reader.diagnostics().full_validation)
  }
}

///|
test "range reads narrow windows and LRU reuse avoids additional IO" {
  let log = @moonmcap.empty_log()
  log.channels.push({
    id: 1,
    schema_id: 0,
    topic: "/data",
    message_encoding: "raw",
    metadata: [],
  })
  for i = 0; i < 120; i = i + 1 {
    log.messages.push({
      channel_id: 1,
      sequence: i.reinterpret_as_uint(),
      log_time: i.to_uint64(),
      publish_time: 0UL,
      data: Bytes::make(1024, b'x'),
    })
  }
  let bytes = @moonmcap.write_indexed(log, messages_per_chunk=4)
  let reader = @moonmcap.open_reader(
    @moonmcap.memory_source(bytes),
    cache_bytes=9000,
  )
  let query = { ..@moonmcap.all_query(), start_time: 20UL, end_time: 23UL, }
  assert_eq(reader.query(query).length(), 4)
  let first = reader.diagnostics()
  assert_true(first.bytes_read < bytes.length().to_uint64() / 4UL)
  assert_eq(first.decoded_chunks, 1)
  ignore(reader.query(query))
  assert_eq(reader.diagnostics().bytes_read, first.bytes_read)
  assert_eq(reader.diagnostics().cache_hits, 1)
  ignore(reader.query({ ..query, start_time: 24UL, end_time: 27UL, }))
  ignore(reader.query(query)) // Refresh the older entry, then evict the other one.
  ignore(reader.query({ ..query, start_time: 28UL, end_time: 31UL, }))
  let decoded = reader.diagnostics().decoded_chunks
  ignore(reader.query(query))
  assert_eq(reader.diagnostics().decoded_chunks, decoded)
  ignore(reader.query({ ..query, start_time: 24UL, end_time: 27UL, }))
  assert_eq(reader.diagnostics().decoded_chunks, decoded + 1)
  assert_true(reader.diagnostics().peak_cache_bytes <= 9000)
  reader.clear_cache()
  assert_eq(reader.diagnostics().cache_bytes, 0)
}

///|
test "zero cache and oversized chunks are never retained" {
  let bytes = @moonmcap.write_indexed(example_log(), messages_per_chunk=1)
  for budget in [0, 1] {
    let reader = @moonmcap.open_reader(
      @moonmcap.memory_source(bytes),
      cache_bytes=budget,
    )
    ignore(reader.query(@moonmcap.all_query()))
    ignore(reader.query(@moonmcap.all_query()))
    assert_eq(reader.diagnostics().cache_bytes, 0)
    assert_eq(reader.diagnostics().decoded_chunks, 6)
  }
}

///|
test "read source retries short reads and propagates IO failures" {
  let bytes = @moonmcap.write_indexed(example_log())
  let source = @moonmcap.new_read_source(bytes.length().to_uint64(), fn(
    offset,
    length,
  ) {
    let start = offset.to_int()
    bytes[start:start + length.min(3)].to_owned()
  })
  let reader = @moonmcap.open_reader(source)
  assert_eq(reader.query(@moonmcap.all_query()).length(), 3)
  let stalled = @moonmcap.new_read_source(100UL, fn(_, _) { b"" })
  try stalled.read_exact(0UL, 8) catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("zero progress must fail")
  }
  let failed = @moonmcap.new_read_source(100UL, fn(_, _) {
    raise @moonmcap.io_error("read failed")
  })
  try @moonmcap.open_reader(failed) catch {
    @moonmcap.Invalid(reason) => assert_eq(reason, "I/O: read failed")
  } noraise {
    _ => fail("IO failure must propagate")
  }
  try source.read_exact(1UL, -1) catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("negative read")
  }
  try source.read_exact(bytes.length().to_uint64(), 1) catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("read past EOF")
  }
}

///|
test "unindexed fallback is explicit bounded and fully validated" {
  let bytes = @moonmcap.write_chunked(example_log(), compression=@moonmcap.Zstd)
  let reader = @moonmcap.open_reader(@moonmcap.memory_source(bytes))
  assert_true(!reader.diagnostics().indexed)
  assert_eq(reader.query(@moonmcap.all_query()).length(), 3)
  assert_true(reader.diagnostics().full_validation)
  let disabled = @moonmcap.open_reader(
    @moonmcap.memory_source(bytes),
    fallback_bytes=0,
  )
  try disabled.query(@moonmcap.all_query()) catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("fallback budget must apply")
  }
}

///|
test "range open rejects corrupted summary truncated files and resource excess" {
  let bytes = @moonmcap.write_indexed(example_log())
  let records = @moonmcap.scan_file(bytes)
  let footer = @moonmcap.decode_footer(records[records.length() - 1].body)
  let position = footer.summary_start.to_int() + 10
  let broken = bytes[0:position].to_owned() +
    b"X" +
    bytes[position + 1:].to_owned()
  try @moonmcap.open_reader(@moonmcap.memory_source(broken)) catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("summary corruption must fail")
  }
  try
    @moonmcap.open_reader(
      @moonmcap.memory_source(bytes[0:bytes.length() - 1].to_owned()),
    )
  catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("truncated tail must fail")
  }
  try
    @moonmcap.open_reader(@moonmcap.memory_source(bytes), summary_bytes=1)
  catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("summary budget must apply")
  }
}

///|
test "selective validation detects corruption when affected chunk is loaded" {
  let bytes = @moonmcap.write_indexed(cursor_fixture(), messages_per_chunk=9)
  let chunk = @moonmcap.scan_file(bytes).filter(fn(r) { r.opcode == 0x06 })[0]
  let position = chunk.offset + 9 + chunk.body.length() - 1
  let broken = bytes[0:position].to_owned() +
    b"!" +
    bytes[position + 1:].to_owned()
  let reader = @moonmcap.open_reader(@moonmcap.memory_source(broken))
  try reader.query(@moonmcap.all_query()) catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("corrupt chunk must fail on load")
  }
  try reader.validate_all() catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("full CRC check must fail")
  }
}
