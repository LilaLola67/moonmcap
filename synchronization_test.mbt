///|
fn sync_test_stream(
  times : Array[UInt64],
  stopped : Array[Int32],
  values~ : Array[Bytes] = [],
) -> @moonmcap.MessageStream {
  let next = [0]
  @moonmcap.message_stream(
    fn() {
      if next[0] == times.length() {
        return None
      }
      let n = next[0]
      next[0] += 1
      Some({
        channel_id: 1,
        sequence: n.to_uint(),
        log_time: times[n],
        publish_time: times[n],
        data: if values.is_empty() {
          b"{}"
        } else {
          values[n]
        },
      })
    },
    fn() { stopped[0] += 1 },
  )
}

///|
test "multi-topic synchronization preserves missing cells and deterministic nearest choices" {
  let stopped = [0]
  let stream = @moonmcap.synchronize(
    [
      @moonmcap.sync_input(
        "anchor",
        sync_test_stream([0UL, 5UL, 10UL], stopped),
      ),
      @moonmcap.sync_input("b", sync_test_stream([1UL, 4UL, 11UL], stopped)),
      @moonmcap.sync_input("c", sync_test_stream([0UL, 9UL], stopped)),
    ],
    options={ ..@moonmcap.sync_options(), tolerance: 2UL, },
  )
  let rows = []
  while stream.next() is Some(row) {
    rows.push(row)
  }
  assert_eq(rows.length(), 3)
  assert_eq(rows.map(fn(r) { r.cells[1].source_time }), [
    Some(1UL),
    Some(4UL),
    Some(11UL),
  ])
  assert_eq(rows.map(fn(r) { r.cells[2].source_time }), [
    Some(0UL),
    None,
    Some(9UL),
  ])
  assert_eq(stopped[0], 3)
  let tie = @moonmcap.synchronize(
    [
      @moonmcap.sync_input("a", sync_test_stream([5UL], stopped)),
      @moonmcap.sync_input("b", sync_test_stream([4UL, 6UL], stopped)),
    ],
    options={ ..@moonmcap.sync_options(), tolerance: 1UL, },
  )
  assert_true(tie.next() is Some(row) && row.cells[1].source_time == Some(4UL))
  tie.cancel()
}

///|
test "consume prevents reuse and dropped incomplete rows do not consume matches" {
  let stopped = [0]
  let stream = @moonmcap.synchronize(
    [
      @moonmcap.sync_input("a", sync_test_stream([4UL, 5UL], stopped)),
      @moonmcap.sync_input("b", sync_test_stream([4UL], stopped)),
    ],
    options={
      ..@moonmcap.sync_options(),
      tolerance: 1UL,
      reuse: @moonmcap.SyncConsume,
    },
  )
  assert_true(
    stream.next() is Some(row) && row.cells[1].source_time == Some(4UL),
  )
  assert_true(stream.next() is Some(row) && row.cells[1].message is None)
  stream.cancel()
  let stream = @moonmcap.synchronize(
    [
      @moonmcap.sync_input("a", sync_test_stream([4UL, 5UL], stopped)),
      @moonmcap.sync_input("b", sync_test_stream([4UL], stopped)),
      @moonmcap.sync_input("c", sync_test_stream([6UL], stopped)),
    ],
    options={
      ..@moonmcap.sync_options(),
      tolerance: 1UL,
      reuse: @moonmcap.SyncConsume,
      keep_missing: false,
    },
  )
  assert_true(
    stream.next() is Some(row) &&
    row.time == 5UL &&
    row.cells[1].source_time == Some(4UL),
  )
  assert_true(stream.next() is None)
}

///|
test "uniform resampling interpolates selected fields with exact UInt64 bracket provenance" {
  let stopped = [0]
  let end = 18446744073709551615UL
  let stream = @moonmcap.resample(
    [
      @moonmcap.sync_input(
        "x",
        sync_test_stream([end - 100UL, end], stopped, values=[
          b"{\"x\":0}", b"{\"x\":10}",
        ]),
        interpolation_paths=["/x"],
      ),
      @moonmcap.sync_input(
        "y",
        sync_test_stream([end - 100UL, end], stopped, values=[
          b"{\"y\":20}", b"{\"y\":0}",
        ]),
        interpolation_paths=["/y"],
      ),
    ],
    end - 100UL,
    end,
    50UL,
    options={
      ..@moonmcap.sync_options(),
      mode: @moonmcap.SyncLinear,
      tolerance: 100UL,
    },
  )
  assert_true(
    stream.next() is Some(row) &&
    row.time == end - 100UL &&
    row.cells[1].message is Some(_),
  )
  let row = stream.next().unwrap()
  assert_eq(row.time, end - 50UL)
  assert_eq(row.cells[1].before_time, Some(end - 100UL))
  assert_eq(row.cells[1].after_time, Some(end))
  assert_true(
    row.cells[1].interpolated is Some(Object(fields)) &&
    fields["value"] == @json.parse("{\"x\":5}"),
  )
  assert_true(
    row.cells[2].interpolated is Some(Object(fields)) &&
    fields["value"] == @json.parse("{\"y\":10}"),
  )
  assert_true(stream.next() is Some(row) && row.time == end)
  assert_true(stream.next() is None)
  assert_eq(stopped[0], 2)
}

///|
test "explicit clocks preserve timestamp extremes and reject invalid header stamps" {
  let message : @moonmcap.Message = {
    channel_id: 1,
    sequence: 0U,
    log_time: 1UL,
    publish_time: 2UL,
    data: b"{\"stamp\":{\"sec\":\"18446744073\",\"nanosec\":709551615},\"ns\":\"18446744073709551615\"}",
  }
  assert_eq(
    @moonmcap.message_timestamp(message, @moonmcap.SyncPublishTime),
    2UL,
  )
  assert_eq(
    @moonmcap.message_timestamp(
      message,
      @moonmcap.SyncJsonStamp("/stamp/sec", "/stamp/nanosec"),
    ),
    18446744073709551615UL,
  )
  assert_eq(
    @moonmcap.message_timestamp(message, @moonmcap.SyncJsonNanoseconds("/ns")),
    18446744073709551615UL,
  )
  for
    data in [
      b"{\"sec\":-1,\"ns\":0}", b"{\"sec\":1,\"ns\":1000000000}", b"{\"sec\":18446744074,\"ns\":0}",
    ] {
    try
      @moonmcap.message_timestamp(
        { ..message, data, },
        @moonmcap.SyncJsonStamp("/sec", "/ns"),
      )
    catch {
      _ => ()
    } noraise {
      _ => fail("invalid timestamp accepted")
    }
  }
}

///|
test "synchronization rejects observed disorder budgets and cancellation and releases inputs" {
  for scenario = 0; scenario < 4; scenario = scenario + 1 {
    let stopped = [0]
    let stream = @moonmcap.synchronize(
      [
        @moonmcap.sync_input("a", sync_test_stream([3UL], stopped)),
        @moonmcap.sync_input("b", sync_test_stream([2UL, 1UL], stopped)),
      ],
      options={
        ..@moonmcap.sync_options(),
        buffer_bytes: if scenario == 1 {
          31
        } else {
          1024
        },
        input_messages: if scenario == 2 {
          1UL
        } else {
          100UL
        },
      },
      cancelled=fn() { scenario == 3 },
    )
    try stream.next() catch {
      _ => ()
    } noraise {
      _ => fail("unsafe synchronization accepted")
    }
    assert_eq(stopped[0], 2)
    assert_true(stream.next() is None)
  }
}
