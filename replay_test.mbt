///|
fn replay_fixture() -> @moonmcap.Replay raise @moonmcap.McapError {
  let builder = @moonmcap.new_builder()
  let id = builder.register_channel("/replay", "raw")
  for time in [100UL, 200UL, 300UL, 300UL] {
    builder.append(id, time, b"x")
  }
  @moonmcap.replay(
    @moonmcap.build_query_index(builder.snapshot()).query_source(),
  )
}

///|
test "replay pause resume rate step and seek preserve virtual time" {
  let replay = replay_fixture()
  assert_true(replay.poll(1000UL) is @moonmcap.ReplayEvent(_))
  assert_true(replay.poll(1050UL) is @moonmcap.ReplayWait(50UL))
  replay.pause(1050UL)
  assert_true(replay.poll(5000UL) is @moonmcap.ReplayIdle)
  assert_eq(replay.progress(5000UL).position, 150UL)
  assert_eq(replay.step(5000UL).unwrap().log_time, 200UL)
  replay.set_rate(2, 1, 5000UL)
  replay.resume(5000UL)
  assert_true(replay.poll(5049UL) is @moonmcap.ReplayWait(1UL))
  assert_true(replay.poll(5050UL) is @moonmcap.ReplayEvent(_))
  assert_true(replay.poll(5050UL) is @moonmcap.ReplayEvent(_))
  assert_true(replay.poll(5050UL) is @moonmcap.ReplayEnd)
  replay.seek(200UL, 6000UL)
  assert_true(replay.poll(6000UL) is @moonmcap.ReplayEvent(_))
  assert_eq(replay.progress(6000UL).generation, 1)
  replay.cancel()
  assert_true(replay.poll(6000UL) is @moonmcap.ReplayEnd)
  try replay.seek(100UL, 6000UL) catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("cancelled replay reopened")
  }
}

///|
test "replay fractional deadlines monotonic clock and UInt64 edge" {
  let replay = replay_fixture()
  replay.set_rate(2, 3, 0UL)
  ignore(replay.poll(0UL))
  assert_true(replay.poll(1UL) is @moonmcap.ReplayWait(149UL))
  try replay.poll(0UL) catch {
    @moonmcap.Invalid(_) => ()
  } noraise {
    _ => fail("clock moved backwards")
  }
  assert_eq(replay.progress(1UL).state, @moonmcap.ReplayFailed)
  let builder = @moonmcap.new_builder()
  let id = builder.register_channel("/max", "raw")
  builder.append(id, 18446744073709551614UL, b"a")
  builder.append(id, 18446744073709551615UL, b"b")
  let replay = @moonmcap.replay(
    @moonmcap.build_query_index(builder.snapshot()).query_source(),
  )
  assert_true(replay.poll(0UL) is @moonmcap.ReplayEvent(_))
  assert_true(replay.poll(0UL) is @moonmcap.ReplayWait(1UL))
  assert_true(replay.poll(1UL) is @moonmcap.ReplayEvent(_))
}
