///|
test "memory diagnosis JSON emits null for unknown evidence" {
  let report = @moonmcap.diagnosis_json(@moonmcap.diagnose_file(b"invalid"))
  assert_true(
    report
    is {
      "data_end": Null,
      "issues": [{ "opcode": Null, "expected": Null, "actual": Null, .. }],
      ..
    },
  )
}

///|
test "source diagnosis bounds reads and does not invent validity before data scan" {
  for compression in [@moonmcap.Uncompressed, @moonmcap.Lz4, @moonmcap.Zstd] {
    let builder = @moonmcap.new_builder()
    let channel = builder.register_channel("/diag", "raw")
    for i = 0; i < 12; i = i + 1 {
      builder.append(channel, i.to_uint64(), b"payload")
    }
    let original = builder.finish(compression~, messages_per_chunk=4)
    let mut peak = 0
    let source = @moonmcap.new_read_source(original.length().to_uint64(), fn(
      offset,
      length,
    ) {
      peak = peak.max(length)
      original[offset.to_int():offset.to_int() + length].to_owned()
    })
    let report = @moonmcap.diagnose_source(source, read_bytes=17)
    assert_true(
      report
      is { "valid": true, "data_valid": true, "observed_messages": "12", .. },
    )
    assert_true(peak <= 17)
    let bad = original.to_array()
    bad[original.length() - 12] = bad[original.length() - 12] ^ 0x40
    let report = @moonmcap.diagnose_source(
      @moonmcap.memory_source(Bytes::from_array(bad)),
    )
    assert_true(
      report
      is { "valid": false, "data_valid": Null, "observed_messages": "0", .. },
    )
  }
}

///|
test "source diagnosis reports checksum evidence and cancellation" {
  let builder = @moonmcap.new_builder()
  let channel = builder.register_channel("/diag", "raw")
  builder.append(channel, 5UL, b"payload")
  let original = builder.finish()
  let records = @moonmcap.decode_records(
    original[8:original.length() - 8].to_owned(),
  )
  let end = records.filter(fn(record) { record.opcode == 0x0f })[0].offset + 8
  let bad = original.to_array()
  bad[end + 9] = bad[end + 9] ^ 0x40
  let report = @moonmcap.diagnose_source(
    @moonmcap.memory_source(Bytes::from_array(bad)),
  )
  assert_true(
    report
    is {
      "valid": false,
      "data_valid": false,
      "issues": [{ "scope": "whole data section", .. }],
      ..
    },
  )
  try
    @moonmcap.diagnose_source(@moonmcap.memory_source(original), cancelled=fn() {
      true
    })
  catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("cancel accepted")
  }
}

///|
test "detailed recovery separates observed time bounds from index claims" {
  let builder = @moonmcap.new_builder()
  let channel = builder.register_channel("/diag", "raw")
  for i = 0; i < 12; i = i + 1 {
    builder.append(channel, i.to_uint64(), b"payload")
  }
  let original = builder.finish(messages_per_chunk=4)
  let records = @moonmcap.decode_records(
    original[8:original.length() - 8].to_owned(),
  )
  let chunk = records.filter(fn(record) { record.opcode == 0x06 })[1].offset + 8
  let bad = original.to_array()
  bad[chunk + 9 + 24] = bad[chunk + 9 + 24] ^ 0x40
  let report = @moonmcap.recover_blocks_detailed(
    @moonmcap.memory_source(Bytes::from_array(bad)),
    @moonmcap.new_write_sink(fn(_) { () }),
  )
  assert_true(
    report
    is {
      "ranges": [
        {
          "recovered": true,
          "actual_time_start": "0",
          "actual_time_end": "3",
          ..
        },
        {
          "recovered": false,
          "actual_time_start": Null,
          "actual_time_end": Null,
          "unknown_lost_messages": Null,
          ..
        },
        {
          "recovered": true,
          "actual_time_start": "8",
          "actual_time_end": "11",
          ..
        },
      ],
      ..
    },
  )
}
