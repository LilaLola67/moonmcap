///|
fn alter_body(bytes : Bytes, opcode : Byte, body : Bytes) -> Bytes raise {
  let mut output = @moonmcap.magic()
  for record in @moonmcap.scan_file(bytes) {
    let replacement = if record.opcode == 0x02 {
      @moonmcap.encode_footer({
        ..@moonmcap.decode_footer(record.body),
        summary_crc: 0U,
      })
    } else if record.opcode == 0x0f {
      b"\x00\x00\x00\x00"
    } else if record.opcode == opcode {
      body
    } else {
      record.body
    }
    assert_eq(replacement.length(), record.body.length())
    output = output + @moonmcap.frame(record.opcode, replacement)
  }
  output + @moonmcap.magic()
}

///|
test "read path rejects false statistics and physical chunk index pointers" {
  let bytes = @moonmcap.write_indexed(example_log())
  let records = @moonmcap.scan_file(bytes)
  let stats = @moonmcap.decode_file_statistics(
    records.filter(fn(r) { r.opcode == 0x0b })[0].body,
  )
  let changed = alter_body(
    bytes,
    0x0b,
    @moonmcap.encode_file_statistics({ ..stats, message_count: 99UL, }),
  )
  let rejected = try @moonmcap.read_log(changed) catch {
    _ => true
  } noraise {
    _ => false
  }
  assert_true(rejected)
  let index = @moonmcap.decode_chunk_index(
    records.filter(fn(r) { r.opcode == 0x08 })[0].body,
  )
  let changed = alter_body(
    bytes,
    0x08,
    @moonmcap.encode_chunk_index({
      ..index,
      chunk_start: index.chunk_start + 1UL,
    }),
  )
  let rejected = try @moonmcap.read_log(changed) catch {
    _ => true
  } noraise {
    _ => false
  }
  assert_true(rejected)
}

///|
test "read path validates attachment indexes instead of silently trusting summary" {
  let log = example_log()
  log.attachments.push({
    log_time: 1UL,
    create_time: 1UL,
    name: "cal",
    media_type: "text/plain",
    data: b"x",
  })
  let bytes = @moonmcap.write_indexed(log)
  let body = @moonmcap.scan_file(bytes).filter(fn(r) { r.opcode == 0x0a })[0].body
  let index = @moonmcap.decode_attachment_index(body)
  let changed = alter_body(
    bytes,
    0x0a,
    @moonmcap.encode_attachment_index({ ..index, data_size: 77UL, }),
  )
  let rejected = try @moonmcap.read_log(changed) catch {
    _ => true
  } noraise {
    _ => false
  }
  assert_true(rejected)
}
