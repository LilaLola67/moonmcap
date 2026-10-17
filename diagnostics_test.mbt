///|
fn bad_chunk_index(bytes : Bytes) -> Bytes raise @moonmcap.McapError {
  let out = @buffer.Buffer::Buffer()
  out.write_bytes(@moonmcap.magic()[:])
  let records = @moonmcap.scan_file(bytes)
  for record in records {
    let body = if record.opcode == 0x08 {
      let index = @moonmcap.decode_chunk_index(record.body)
      @moonmcap.encode_chunk_index({
        ..index,
        chunk_start: index.chunk_start + 1UL,
      })
    } else if record.opcode == 0x02 {
      @moonmcap.encode_footer({
        ..@moonmcap.decode_footer(record.body),
        summary_crc: 0U,
      })
    } else {
      record.body
    }
    out.write_bytes(@moonmcap.frame(record.opcode, body)[:])
  }
  out.write_bytes(@moonmcap.magic()[:])
  out.to_bytes()
}

///|
test "diagnosis distinguishes valid data from broken indexes and repairs all messages" {
  let log = cursor_fixture()
  for compression in [@moonmcap.Uncompressed, @moonmcap.Lz4, @moonmcap.Zstd] {
    let original = @moonmcap.write_indexed(
      log,
      compression~,
      messages_per_chunk=11,
    )
    assert_true(@moonmcap.diagnose_file(original).valid)
    let broken = bad_chunk_index(original)
    let report = @moonmcap.diagnose_file(broken)
    assert_true(!report.valid && report.data_valid)
    assert_eq(report.messages, 257)
    assert_true(report.issues.any(fn(d) { d.code == @moonmcap.BadIndex }))
    let repaired = @moonmcap.rebuild_indexes(broken, compression~)
    assert_true(!repaired.original.valid && repaired.result.valid)
    assert_eq(@moonmcap.read_log(repaired.bytes).messages, log.messages)
    assert_eq(@moonmcap.diagnose_file(broken), report)
  }
}

///|
test "diagnosis identifies exact truncated record position expected and actual size" {
  let bytes = @moonmcap.write_indexed(example_log())
  let record = @moonmcap.scan_file(bytes).filter(fn(r) { r.opcode == 0x06 })[0]
  let cut = bytes[0:record.offset + 12].to_owned()
  let report = @moonmcap.diagnose_file(cut)
  assert_true(!report.valid && !report.data_valid)
  assert_eq(report.issues[0].code, @moonmcap.TruncatedInput)
  assert_eq(report.issues[0].offset, Some(record.offset.to_uint64()))
  assert_eq(report.issues[0].opcode, Some(0x06))
  assert_eq(report.issues[0].expected, Some(record.body.length().to_uint64()))
  assert_eq(report.issues[0].actual, Some(3UL))
  try @moonmcap.rebuild_indexes(cut) catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("truncated data cannot be reindexed")
  }
}

///|
test "index rebuild accepts incomplete tail but rejects bad data checksum" {
  let bytes = @moonmcap.write_indexed(example_log())
  let records = @moonmcap.scan_file(bytes)
  let ending = records.filter(fn(r) { r.opcode == 0x0f })[0]
  let no_tail = bytes[0:ending.offset + 13].to_owned()
  let repaired = @moonmcap.rebuild_indexes(no_tail)
  assert_true(repaired.original.data_valid && !repaired.original.valid)
  assert_true(repaired.result.valid)
  assert_eq(@moonmcap.read_log(repaired.bytes).messages.length(), 3)
  let broken = Bytes::makei(bytes.length(), fn(i) {
    if i == ending.offset + 9 {
      bytes[i] ^ b'\xff'
    } else {
      bytes[i]
    }
  })
  let report = @moonmcap.diagnose_file(broken)
  assert_true(!report.data_valid)
  assert_eq(report.issues[0].code, @moonmcap.BadChecksum)
  assert_eq(report.issues[0].offset, Some(ending.offset.to_uint64()))
  assert_true(report.issues[0].expected != report.issues[0].actual)
  try @moonmcap.rebuild_indexes(broken) catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("corrupt data cannot be reindexed")
  }
}

///|
test "diagnosis reports bad magic and respects configured resource limits" {
  assert_eq(
    @moonmcap.diagnose_file(b"invalid").issues[0].code,
    @moonmcap.BadEnvelope,
  )
  let bytes = @moonmcap.write_indexed(example_log())
  let report = @moonmcap.diagnose_file(bytes, limits={
    ..@moonmcap.default_limits(),
    max_record_bytes: 22,
  })
  assert_true(!report.valid)
  assert_eq(report.issues[0].code, @moonmcap.LimitExceeded)
  assert_true(report.issues[0].expected is Some(_))
  assert_true(report.issues[0].actual is Some(_))
}

///|
test "diagnosis distinguishes an absent data checksum from verified CRC" {
  let checked = @moonmcap.diagnose_file(@moonmcap.write_log(example_log()))
  assert_true(checked.valid && checked.data_crc_verified)
  let absent = @moonmcap.diagnose_file(
    @moonmcap.write_log(example_log(), checksums=false),
  )
  assert_true(absent.valid && absent.data_valid && !absent.data_crc_verified)
}

///|
test "seeded corruption diagnostics never report damaged checksummed files as valid" {
  let bytes = @moonmcap.write_indexed(example_log(), compression=@moonmcap.Zstd)
  let mut seed = 20261017U
  for _i = 0; _i < 128; _i = _i + 1 {
    seed = seed * 1664525U + 1013904223U
    let at = (seed % bytes.length().reinterpret_as_uint()).reinterpret_as_int()
    let broken = Bytes::makei(bytes.length(), fn(i) {
      if i == at {
        bytes[i] ^ b'\x80'
      } else {
        bytes[i]
      }
    })
    assert_true(!@moonmcap.diagnose_file(broken).valid)
  }
}
