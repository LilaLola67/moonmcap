///|
test "recorder deduplicates declarations snapshots and closes atomically" {
  let builder = @moonmcap.new_builder()
  let schema = builder.register_schema("event", "jsonschema", b"{}")
  assert_eq(builder.register_schema("event", "jsonschema", b"{}"), schema)
  let id = builder.register_channel("/event", "json", schema_id=schema)
  builder.append(id, 1UL, b"{}")
  let snapshot = builder.snapshot()
  snapshot.messages.clear()
  assert_eq(builder.message_count(), 1)
  builder.append(id, 2UL, b"{}")
  let bytes = builder.finish()
  assert_eq(@moonmcap.read_log(bytes).messages[1].sequence, 1U)
  let rejected = try builder.append(id, 3UL, b"{}") catch {
    _ => true
  } noraise {
    _ => false
  }
  assert_true(rejected)
}

///|
test "failed recording does not consume sequence or identifiers" {
  let builder = @moonmcap.new_builder()
  let rejected = try
    builder.register_channel("bad", "json", schema_id=99)
  catch {
    _ => true
  } noraise {
    _ => false
  }
  assert_true(rejected)
  assert_eq(builder.register_channel("ok", "json"), 1)
  let rejected = try builder.append(99, 1UL, b"") catch {
    _ => true
  } noraise {
    _ => false
  }
  assert_true(rejected)
  builder.append(1, 1UL, b"")
  assert_eq(builder.snapshot().messages[0].sequence, 0U)
}
