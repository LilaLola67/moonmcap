///|
test "stream transform matches sampled paginated aligned memory semantics" {
  let original = cursor_fixture()
  for compression in [@moonmcap.Uncompressed, @moonmcap.Lz4, @moonmcap.Zstd] {
    for descending in [false, true] {
      let options = {
        ..@moonmcap.transform_options(),
        query: {
          ..@moonmcap.all_query(),
          topics: ["/shared"],
          descending,
          offset: 1,
          limit: 5,
        },
        sample_period: 10UL,
        topic_map: [("/shared", "/renamed")],
        align_start: Some(100UL),
      }
      let expected = @moonmcap.transform_log(original, options)
      let files : Map[Int32, @buffer.Buffer] = Map([])
      let closed = [false]
      let out = @buffer.Buffer::Buffer()
      let report = @moonmcap.stream_transform(
        @moonmcap.memory_source(
          @moonmcap.write_indexed(original, compression~, messages_per_chunk=7),
        ),
        memory_sort_storage(files, closed),
        @moonmcap.new_write_sink(fn(bytes) { out.write_bytes(bytes[:]) }),
        options,
        external={
          ..@moonmcap.external_options(),
          run_bytes: 400,
          merge_bytes: 1000,
          fan_in: 2,
        },
      )
      let actual = @moonmcap.read_log(out.to_bytes())
      assert_eq(
        actual.messages.map(fn(m) {
          (m.sequence, m.log_time, m.publish_time, m.data)
        }),
        expected.messages.map(fn(m) {
          (m.sequence, m.log_time, m.publish_time, m.data)
        }),
      )
      assert_true(actual.channels.all(fn(c) { c.topic == "/renamed" }))
      assert_eq(report.output_messages, 5)
      assert_eq(report.sampled_messages, 9)
      assert_true(closed[0] && files.is_empty())
    }
  }
}

///|
test "stream transform sequential input preserves filtered attachments and metadata" {
  let original = example_log()
  original.attachments.push({
    name: "keep",
    media_type: "raw",
    log_time: 1UL,
    create_time: 0UL,
    data: b"blob",
  })
  original.metadata.push({ name: "keep", entries: [("key", "yes")], })
  let files : Map[Int32, @buffer.Buffer] = Map([])
  let closed = [false]
  let output = @buffer.Buffer::Buffer()
  let options = {
    ..@moonmcap.transform_options(),
    attachment_names: ["keep"],
    metadata_names: ["keep"],
  }
  ignore(
    @moonmcap.stream_transform(
      @moonmcap.memory_source(@moonmcap.write_log(original)),
      memory_sort_storage(files, closed),
      @moonmcap.new_write_sink(fn(b) { output.write_bytes(b[:]) }),
      options,
    ),
  )
  let result = @moonmcap.read_log(output.to_bytes())
  assert_eq(result.attachments.length(), 1)
  assert_eq(result.metadata.length(), 1)
  assert_true(files.is_empty() && closed[0])
}

///|
test "stream transform cleanup on disk budget cancellation and empty alignment" {
  for mode in [0, 1, 2] {
    let files : Map[Int32, @buffer.Buffer] = Map([])
    let closed = [false]
    let options = {
      ..@moonmcap.transform_options(),
      query: { ..@moonmcap.all_query(), limit: if mode == 2 { 0 } else { 10 }, },
      align_start: Some(0UL),
    }
    try
      @moonmcap.stream_transform(
        @moonmcap.memory_source(@moonmcap.write_log(example_log())),
        memory_sort_storage(files, closed),
        @moonmcap.new_write_sink(fn(_) { () }),
        options,
        external={
          ..@moonmcap.external_options(),
          spool_bytes: if mode == 0 {
            100UL
          } else {
            1000000UL
          },
        },
        cancelled=fn() { mode == 1 },
      )
    catch {
      @moonmcap.Invalid(_) => ()
    } noraise {
      _ => fail("stream failure expected")
    }
    assert_true(files.is_empty() && closed[0])
  }
}

///|
test "stream NDJSON emits individual objects and sequential topics" {
  let reader = @moonmcap.open_reader(
    @moonmcap.memory_source(@moonmcap.write_log(example_log())),
  )
  let out = @buffer.Buffer::Buffer()
  let count = reader
    .cursor({ ..@moonmcap.all_query(), limit: 2, })
    .write_ndjson(@moonmcap.new_write_sink(fn(b) { out.write_bytes(b[:]) }))
  let text = @utf8.decode(out.to_bytes()[:])
  assert_eq(count, 2)
  assert_eq(text.split("\n").to_array().length(), 3)
  assert_true(text.contains("\"topic\":\"" + reader.channels()[0].topic + "\""))
}
