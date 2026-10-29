///|
// Deterministic test digest only. File adapters use actual SHA256.
fn live_test_digest(
  source : @moonmcap.ReadSource,
) -> String raise @moonmcap.McapError {
  let crc = @moonmcap.crc32(source.read_exact(0UL, source.size().to_int()))
  let buffer = @buffer.Buffer::Buffer()
  for _ in 0..<8 {
    buffer.write_uint_le(crc)
  }
  @moonmcap.encode_hex(buffer.to_bytes())
}

///|
test "growing tail waits for whole chunks and resumes without replaying delivered messages" {
  for compression in [@moonmcap.Uncompressed, @moonmcap.Lz4, @moonmcap.Zstd] {
    let builder = @moonmcap.new_builder()
    let channel = builder.register_channel("/n", "json")
    for i = 0; i < 12; i = i + 1 {
      builder.append(channel, i.to_uint64(), b"{}")
    }
    let bytes = builder.finish(compression~, messages_per_chunk=3)
    let records = @moonmcap.scan_file(bytes)
    let chunks = records.filter(fn(r) { r.opcode == 0x06 })
    let first = chunks[0]
    let end = first.offset + 9 + first.body.length()
    let tail = @moonmcap.growing_tail("test", live_test_digest)
    let initial = tail.poll(
      @moonmcap.memory_source(bytes[0:end - 1].to_owned()),
    )
    assert_eq(initial.messages.length(), 0)
    assert_true(
      initial.evidence is Object(f) &&
      f["verification"] == Json::string("partial-prefix"),
    )
    let first = tail.poll(@moonmcap.memory_source(bytes[0:end].to_owned()))
    assert_eq(first.messages.map(fn(m) { m.log_time }), [0UL, 1UL, 2UL])
    let restored = @moonmcap.growing_tail(
      "test",
      live_test_digest,
      snapshot=tail.snapshot(),
    )
    let rest = restored.poll(@moonmcap.memory_source(bytes))
    assert_eq(rest.messages.map(fn(m) { m.log_time }), [
      3UL, 4UL, 5UL, 6UL, 7UL, 8UL, 9UL, 10UL, 11UL,
    ])
    assert_true(
      rest.evidence is Object(f) &&
      f["verification"] == Json::string("complete-container"),
    )
    assert_eq(
      restored.poll(@moonmcap.memory_source(bytes)).messages.length(),
      0,
    )
    try restored.poll(@moonmcap.memory_source(bytes + b"x")) catch {
      _ => ()
    } noraise {
      _ => fail("completed source extended")
    }
    try tail.poll(@moonmcap.memory_source(bytes[0:end - 1].to_owned())) catch {
      _ => ()
    } noraise {
      _ => fail("source truncation accepted")
    }
    let mutated = @buffer.Buffer::Buffer()
    mutated.write_bytes(bytes[0:9])
    mutated.write_byte(bytes[9] ^ 0x01)
    mutated.write_bytes(bytes[10:])
    try tail.poll(@moonmcap.memory_source(mutated.to_bytes())) catch {
      _ => ()
    } noraise {
      _ => fail("prefix mutation accepted")
    }
    try
      @moonmcap.growing_tail(
        "other",
        live_test_digest,
        snapshot=tail.snapshot(),
      )
    catch {
      _ => ()
    } noraise {
      _ => fail("snapshot identity ignored")
    }
  }
}

///|
test "growing tail failed CRC and byte budgets leave cursor unchanged" {
  let builder = @moonmcap.new_builder()
  let channel = builder.register_channel("/n", "json")
  builder.append(channel, 0UL, b"{}")
  let bytes = builder.finish(compression=@moonmcap.Zstd)
  let tail = @moonmcap.growing_tail("test", live_test_digest, batch_bytes=1)
  let snapshot = tail.snapshot()
  try tail.poll(@moonmcap.memory_source(bytes)) catch {
    _ => ()
  } noraise {
    _ => fail("batch budget ignored")
  }
  assert_eq(tail.snapshot(), snapshot)
  let chunk = @moonmcap.scan_file(bytes).filter(fn(r) { r.opcode == 0x06 })[0]
  let offset = chunk.offset + 9 + 24
  let modified = @buffer.Buffer::Buffer()
  modified.write_bytes(bytes[0:offset])
  modified.write_byte(bytes[offset] ^ 0x01)
  modified.write_bytes(bytes[offset + 1:])
  let tail = @moonmcap.growing_tail("test", live_test_digest)
  let snapshot = tail.snapshot()
  try tail.poll(@moonmcap.memory_source(modified.to_bytes())) catch {
    _ => ()
  } noraise {
    _ => fail("chunk CRC accepted")
  }
  assert_eq(tail.snapshot(), snapshot)
  // A final data CRC failure never upgrades earlier partial evidence.
  let end = @moonmcap.scan_file(bytes).filter(fn(r) { r.opcode == 0x0f })[0]
  let prefix = bytes[0:end.offset].to_owned()
  assert_eq(tail.poll(@moonmcap.memory_source(prefix)).messages.length(), 1)
  let snapshot = tail.snapshot()
  let modified = @buffer.Buffer::Buffer()
  let offset = end.offset + 9
  modified.write_bytes(bytes[0:offset])
  modified.write_byte(bytes[offset] ^ 0x01)
  modified.write_bytes(bytes[offset + 1:])
  try tail.poll(@moonmcap.memory_source(modified.to_bytes())) catch {
    _ => ()
  } noraise {
    _ => fail("full data CRC accepted")
  }
  assert_eq(tail.snapshot(), snapshot)
}

///|
test "recording tail ignores unfinished segments and preserves committed prefix" {
  let builder = @moonmcap.new_builder()
  let channel = builder.register_channel("/n", "json")
  let fixture = recording_fixture()
  let session = @moonmcap.open_recording_session(
    builder.snapshot(),
    fixture.storage,
    options={ ..@moonmcap.recording_options(), segment_messages: 2, },
  )
  let tail = @moonmcap.recording_tail("test")
  let digest : (@moonmcap.ReadSource) -> String raise @moonmcap.McapError = fn(
    _,
  ) {
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  }
  let open : (Int) -> @moonmcap.ReadSource raise @moonmcap.McapError = fn(id) {
    @moonmcap.memory_source(fixture.files[id].to_bytes())
  }
  session.append({
    channel_id: channel,
    sequence: 0U,
    log_time: 0UL,
    publish_time: 0UL,
    data: b"{}",
  })
  assert_true(
    tail.poll(fixture.manifests.last().unwrap(), open, digest) is None,
  )
  session.append({
    channel_id: channel,
    sequence: 1U,
    log_time: 1UL,
    publish_time: 1UL,
    data: b"{}",
  })
  assert_true(
    tail.poll(fixture.manifests.last().unwrap(), open, digest) is None,
  )
  session.append({
    channel_id: channel,
    sequence: 2U,
    log_time: 2UL,
    publish_time: 2UL,
    data: b"{}",
  })
  let batch = tail
    .poll(fixture.manifests.last().unwrap(), open, digest)
    .unwrap()
  assert_eq(batch.messages.length(), 2)
  let saved = tail.snapshot()
  let resumed = @moonmcap.recording_tail("test", snapshot=saved)
  assert_true(
    resumed.poll(fixture.manifests.last().unwrap(), open, digest) is None,
  )
  ignore(session.finish())
  let manifest = fixture.manifests.last().unwrap()
  try
    resumed.poll(manifest, open, fn(_) {
      "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    })
  catch {
    _ => ()
  } noraise {
    _ => fail("segment hash ignored")
  }
  assert_eq(resumed.snapshot(), saved)
  assert_eq(resumed.poll(manifest, open, digest).unwrap().messages.length(), 1)
  try resumed.poll(fixture.manifests[0], open, digest) catch {
    _ => ()
  } noraise {
    _ => fail("recording manifest rewind accepted")
  }
}

///|
test "live quality reorders bounded arrivals excludes finalized late data and resumes" {
  let rules = [
    {
      ..@moonmcap.quality_rule("/n"),
      max_gap: Some(5UL),
      check_sequence: true,
      condition: Some("{\"path\":\"/ok\",\"op\":\"=\",\"value\":true}"),
    },
  ]
  let quality = @moonmcap.live_quality(rules, 0UL, lateness=5UL)
  fn message(time : UInt64, sequence : UInt) -> @moonmcap.Message {
    {
      channel_id: 1,
      sequence,
      log_time: time,
      publish_time: time,
      data: b"{\"ok\":true}",
    }
  }
  assert_true(quality.feed("/n", message(4UL, 1U)))
  assert_true(quality.feed("/n", message(1UL, 0U)))
  assert_true(quality.feed("/n", message(8UL, 2U)))
  let resumed = @moonmcap.live_quality(
    rules,
    0UL,
    lateness=5UL,
    snapshot=quality.snapshot(),
  )
  assert_true(!resumed.feed("/n", message(2UL, 99U)))
  assert_true(resumed.feed("/n", message(10UL, 3U)))
  resumed.advance(12UL)
  assert_true(resumed.feed("/n", message(13UL, 4U)))
  assert_true(
    resumed.report() is Object(f) && f["watermark"] == Json::string("12"),
  )
  resumed.finish(14UL)
  assert_true(
    resumed.report() is Object(f) &&
    f["late_excluded"] == Json::string("1") &&
    f["rules"] is Array(rows) &&
    rows[0] is Object(rule) &&
    rule["status"] == Json::string("passed") &&
    rule["messages"] == Json::string("5"),
  )
  try resumed.feed("/n", message(15UL, 5U)) catch {
    _ => ()
  } noraise {
    _ => fail("finished quality accepted observation")
  }
  let restored = @moonmcap.live_quality(
    rules,
    0UL,
    lateness=5UL,
    snapshot=resumed.snapshot(),
  )
  assert_eq(restored.report(), resumed.report())
  try
    @moonmcap.live_quality(
      rules,
      0UL,
      lateness=6UL,
      snapshot=quality.snapshot(),
    )
  catch {
    _ => ()
  } noraise {
    _ => fail("quality rules mismatch accepted")
  }
}
