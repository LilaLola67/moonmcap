///|
fn join_test_stream(
  times : Array[UInt64],
  stopped : Array[Int32],
) -> @moonmcap.MessageStream {
  let position = [0]
  @moonmcap.message_stream(
    fn() {
      if position[0] == times.length() {
        return None
      }
      let p = position[0]
      position[0] += 1
      Some({
        channel_id: 1,
        sequence: p.to_uint(),
        log_time: times[p],
        publish_time: times[p],
        data: b"x",
      })
    },
    fn() { stopped[0] += 1 },
  )
}

///|
test "nearest/asof joins have stable ties tolerance missing policies and UInt64 boundaries" {
  for mode in [@moonmcap.JoinNearest, @moonmcap.JoinAsOf] {
    let stopped = [0]
    let stream = @moonmcap.temporal_join(
      join_test_stream(
        [0UL, 5UL, 6UL, 10UL, 20UL, 18446744073709551615UL],
        stopped,
      ),
      join_test_stream([2UL, 2UL, 8UL, 20UL, 18446744073709551615UL], stopped),
      options={ ..@moonmcap.join_options(), mode, tolerance: 3UL, },
    )
    let rows = []
    while stream.next() is Some(row) {
      rows.push(row)
    }
    assert_eq(
      rows.map(fn(row) { row.right.map(fn(r) { r.sequence }) }),
      if mode == @moonmcap.JoinNearest {
        [Some(0U), Some(0U), Some(2U), Some(2U), Some(3U), Some(4U)]
      } else {
        [None, Some(0U), None, Some(2U), Some(3U), Some(4U)]
      },
    )
    assert_eq(stopped[0], 2)
    assert_eq(stream.next(), None)
  }
  let stopped = [0]
  let drop = @moonmcap.temporal_join(
    join_test_stream([1UL, 2UL, 3UL], stopped),
    join_test_stream([2UL], stopped),
    options={ ..@moonmcap.join_options(), missing: @moonmcap.JoinDrop, },
  )
  assert_true(drop.next() is Some(row) && row.left.log_time == 2UL)
  assert_eq(drop.next(), None)
}

///|
test "temporal join rejects observed disorder and bounded buffers and propagates cancellation" {
  for (left, right) in [([10UL, 5UL], [1UL, 20UL]), ([10UL], [8UL, 2UL])] {
    let stopped = [0]
    let stream = @moonmcap.temporal_join(
      join_test_stream(left, stopped),
      join_test_stream(right, stopped),
      options={ ..@moonmcap.join_options(), tolerance: 100UL, },
    )
    try {
      while stream.next() is Some(_) {

      }
    } catch {
      _ => ()
    } noraise {
      _ => fail("unordered join stream accepted")
    }
    assert_eq(stopped[0], 2)
  }
  let stopped = [0]
  let stream = @moonmcap.temporal_join(
    join_test_stream([3UL], stopped),
    join_test_stream([2UL, 4UL], stopped),
    options={ ..@moonmcap.join_options(), buffer_bytes: 63, },
  )
  try stream.next() catch {
    _ => ()
  } noraise {
    _ => fail("join payload budget ignored")
  }
  assert_eq(stopped[0], 2)
  let stream = @moonmcap.temporal_join(
    join_test_stream([1UL], stopped),
    join_test_stream([], stopped),
  )
  stream.cancel()
  stream.cancel()
  assert_eq(stopped[0], 4)
  assert_eq(stream.next(), None)
}
