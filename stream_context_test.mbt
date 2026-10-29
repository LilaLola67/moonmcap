///|
test "streamed clips equal materialized clips without retaining window payloads" {
  let builder = @moonmcap.new_builder()
  let state = builder.register_channel("/state", "json")
  let sensor = builder.register_channel("/sensor", "json")
  builder.append(state, 0UL, b"{}")
  builder.append(state, 1UL, b"{}")
  for i = 0; i < 1000; i = i + 1 {
    builder.append(sensor, i.to_uint64() + 10UL, b"{\"n\":1}")
  }
  let source = @moonmcap.build_query_index(builder.snapshot()).query_source()
  let context = [{ ..@moonmcap.context_rule("/state"), depth: 2, }]
  let expected = @moonmcap.extract_context_clip(source, 10UL, 1009UL, context~)
  for codec in [@moonmcap.Uncompressed, @moonmcap.Lz4, @moonmcap.Zstd] {
    let buffer = @buffer.Buffer::Buffer()
    let _ = @moonmcap.stream_context_clip(
      source,
      @moonmcap.new_write_sink(fn(b) { buffer.write_bytes(b[:]) }),
      10UL,
      1009UL,
      context~,
      options={
        ..@moonmcap.clip_stream_options(),
        writer: {
          ..@moonmcap.stream_options(),
          compression: codec,
          chunk_bytes: 512,
          chunk_messages: 8,
        },
      },
    )
    let actual = @moonmcap.read_log(buffer.to_bytes())
    assert_eq(actual.messages, expected.log.messages)
    assert_eq(actual.schemas, expected.log.schemas)
  }
}

///|
test "streamed clips enforce output and context budgets and cancellation" {
  let builder = @moonmcap.new_builder()
  let id = builder.register_channel("/x", "json")
  builder.append(id, 0UL, b"{}")
  builder.append(id, 1UL, b"{}")
  let source = @moonmcap.build_query_index(builder.snapshot()).query_source()
  for
    options in [
      { ..@moonmcap.clip_stream_options(), output_messages: 1UL, },
      { ..@moonmcap.clip_stream_options(), output_payload_bytes: 3UL, },
    ] {
    try
      @moonmcap.stream_context_clip(
        source,
        @moonmcap.new_write_sink(fn(_) { () }),
        0UL,
        1UL,
        options~,
      )
    catch {
      _ => ()
    } noraise {
      _ => fail("streaming output budget ignored")
    }
  }
  try
    @moonmcap.stream_context_clip(
      source,
      @moonmcap.new_write_sink(fn(_) { () }),
      1UL,
      1UL,
      context=[@moonmcap.context_rule("/x")],
      options={ ..@moonmcap.clip_stream_options(), context_bytes: 1, },
    )
  catch {
    _ => ()
  } noraise {
    _ => fail("context budget ignored")
  }
  try
    @moonmcap.stream_context_clip(
      source,
      @moonmcap.new_write_sink(fn(_) { () }),
      0UL,
      1UL,
      cancelled=fn() { true },
    )
  catch {
    _ => ()
  } noraise {
    _ => fail("cancellation ignored")
  }
}
