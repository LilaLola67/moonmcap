///|
struct RecordingFixture {
  storage : @moonmcap.RecordingStorage
  files : Map[Int, @buffer.Buffer]
  manifests : Array[Bytes]
  quarantine : Map[Int, Bytes]
  fail_save : Array[Bool]
  fail_after_seal : Array[Bool]
}

///|
fn recording_fixture() -> RecordingFixture {
  let files : Map[Int, @buffer.Buffer] = Map([])
  let manifests = []
  let quarantine : Map[Int, Bytes] = Map([])
  let fail_save = [false]
  let fail_after_seal = [false]
  let storage = @moonmcap.recording_storage(
    fn(id) {
      if files.contains(id) {
        raise @moonmcap.io_error("segment already exists")
      }
      let out = @buffer.Buffer::Buffer()
      files[id] = out
      @moonmcap.new_write_sink(fn(bytes) { out.write_bytes(bytes[:]) })
    },
    fn(id) {
      guard files.get(id) is Some(out) else {
        raise @moonmcap.io_error("missing segment")
      }
      @moonmcap.memory_source(out.to_bytes())
    },
    fn(id) { files.contains(id) },
    fn(_) { if fail_after_seal[0] { fail_save[0] = true } },
    fn(bytes) {
      if fail_save[0] {
        raise @moonmcap.io_error("simulated manifest failure")
      }
      manifests.push(bytes)
    },
    fn(id) {
      quarantine[id] = files[id].to_bytes()
      files.remove(id)
    },
    fn() {
      let ids = files.keys().to_array()
      ids.sort()
      ids
    },
    fn(_) { "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" },
  )
  { storage, files, manifests, quarantine, fail_save, fail_after_seal, }
}

///|
test "recording recovery quarantines unfinished tails and continues per channel in new segments" {
  let builder = @moonmcap.new_builder()
  let a = builder.register_channel("/a", "json")
  let b = builder.register_channel("/b", "json")
  let template = builder.snapshot()
  let fixture = recording_fixture()
  let options = {
    ..@moonmcap.recording_options(),
    segment_messages: 2,
    sequences: @moonmcap.ContinueSequence,
  }
  fn message(channel : Int, time : Int) -> @moonmcap.Message {
    {
      channel_id: channel,
      sequence: 100U,
      log_time: time.to_uint64(),
      publish_time: time.to_uint64(),
      data: b"{}",
    }
  }
  let session = @moonmcap.open_recording_session(
    template,
    fixture.storage,
    options~,
  )
  for i = 0; i < 5; i = i + 1 {
    session.append(message(if i % 2 == 0 { a } else { b }, i))
  }
  assert_eq(session.committed_messages(), 4UL)
  session.cancel()
  let session = @moonmcap.open_recording_session(
    template,
    fixture.storage,
    manifest=fixture.manifests.last().unwrap(),
    options~,
  )
  assert_eq(session.committed_messages(), 4UL)
  assert_eq(fixture.quarantine.length(), 1)
  assert_true(fixture.quarantine.contains(2))
  for i = 4; i < 8; i = i + 1 {
    session.append(message(if i % 2 == 0 { a } else { b }, i))
  }
  ignore(session.finish())
  assert_eq(session.committed_messages(), 8UL)
  let sequences : Map[Int, Array[UInt]] = Map([])
  let ids = fixture.files.keys().to_array()
  ids.sort()
  assert_eq(ids, [0, 1, 3, 4])
  for id in ids {
    for m in @moonmcap.read_log(fixture.files[id].to_bytes()).messages {
      sequences.get_or_init(m.channel_id, fn() { [] }).push(m.sequence)
    }
  }
  assert_eq(sequences[a], [0U, 1U, 2U, 3U])
  assert_eq(sequences[b], [0U, 1U, 2U, 3U])
  let again = @moonmcap.open_recording_session(
    template,
    fixture.storage,
    manifest=fixture.manifests.last().unwrap(),
    options~,
  )
  ignore(again.finish())
  assert_eq(fixture.files.length(), 4)
  assert_eq(again.committed_messages(), 8UL)
  try
    @moonmcap.open_recording_session(
      template,
      fixture.storage,
      manifest=fixture.manifests.last().unwrap(),
      options={ ..options, segment_messages: 3, },
    )
  catch {
    _ => ()
  } noraise {
    _ => fail("recording configuration mismatch accepted")
  }
}

///|
test "sealed orphan adoption repairs the manifest commit window without duplicate records" {
  let builder = @moonmcap.new_builder()
  let id = builder.register_channel("/n", "json")
  let fixture = recording_fixture()
  let session = @moonmcap.open_recording_session(
    builder.snapshot(),
    fixture.storage,
  )
  session.append({
    channel_id: id,
    sequence: 7U,
    log_time: 1UL,
    publish_time: 1UL,
    data: b"{}",
  })
  fixture.fail_after_seal[0] = true
  try session.finish() catch {
    _ => ()
  } noraise {
    _ => fail("manifest failure not propagated")
  }
  fixture.fail_after_seal[0] = false
  assert_eq(session.committed_messages(), 0UL)
  fixture.fail_save[0] = false
  let resumed = @moonmcap.open_recording_session(
    builder.snapshot(),
    fixture.storage,
    manifest=fixture.manifests.last().unwrap(),
  )
  assert_eq(resumed.committed_messages(), 1UL)
  assert_true(resumed.info() is { "adopted_segments": 1, .. })
  ignore(resumed.finish())
  let bytes = fixture.manifests.last().unwrap().to_array()
  bytes[30] = bytes[30] ^ 0x01
  try @moonmcap.inspect_recording_manifest(Bytes::from_array(bytes)) catch {
    _ => ()
  } noraise {
    _ => fail("manifest CRC ignored")
  }
  let bytes = fixture.files[0].to_bytes().to_array()
  bytes[bytes.length() - 1] = 0x00
  let broken = @buffer.Buffer::Buffer()
  broken.write_bytes(Bytes::from_array(bytes)[:])
  fixture.files[0] = broken
  try
    @moonmcap.open_recording_session(
      builder.snapshot(),
      fixture.storage,
      manifest=fixture.manifests.last().unwrap(),
    )
  catch {
    _ => ()
  } noraise {
    _ => fail("committed corrupt segment accepted")
  }
  assert_eq(fixture.quarantine.length(), 0)
}
