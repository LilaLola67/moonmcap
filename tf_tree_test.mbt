///|
fn tf_test_sample(
  parent : String,
  child : String,
  time : UInt64,
  x : Double,
  is_static? : Bool = false,
) -> @moonmcap.TfStamped {
  {
    parent,
    child,
    time,
    is_static,
    origin: None,
    transform: {
      ..@moonmcap.tf_identity(),
      translation: { x, y: 0.0, z: 0.0, },
    },
  }
}

///|
test "TF historical lookup composes inverse chains and retains semantic context" {
  let tree = @moonmcap.tf_tree()
  tree.add(tf_test_sample("world", "base", 10UL, 0.0))
  tree.add(tf_test_sample("world", "base", 20UL, 10.0))
  tree.add(tf_test_sample("world", "base", 30UL, 20.0))
  tree.add(tf_test_sample("base", "sensor", 0UL, 2.0, is_static=true))
  tree.add(tf_test_sample("unused", "other", 0UL, 99.0, is_static=true))
  let value = tree.lookup("world", "sensor", 15UL)
  assert_eq(value.transform.translation.x, 7.0)
  assert_true(value.interpolated)
  assert_eq(value.evidence.length(), 3)
  assert_eq(tree.lookup("sensor", "world", 15UL).transform.translation.x, -7.0)
  assert_eq(tree.lookup("base", "sensor", 0UL).transform.translation.x, 2.0)
  assert_eq(
    tree.lookup("sensor", "sensor", 18446744073709551615UL).transform,
    @moonmcap.tf_identity(),
  )
  let context = tree.context(["sensor", "base"], 15UL, 25UL)
  assert_eq(context.length(), 4)
  assert_eq(context.filter(fn(s) { s.child == "base" }).map(fn(s) { s.time }), [
    10UL, 20UL, 30UL,
  ])
  assert_eq(tree.context(["sensor"], 20UL, 20UL).length(), 2)
  let clipped = @moonmcap.tf_tree()
  for sample in context {
    clipped.add(sample)
  }
  assert_eq(
    clipped.lookup("world", "sensor", 25UL).transform.translation.x,
    17.0,
  )
}

///|
test "TF quaternion interpolation follows shortest arc and normalizes robustly" {
  let tree = @moonmcap.tf_tree()
  let a = tf_test_sample("world", "base", 0UL, 0.0)
  tree.add(a)
  tree.add({
    ..a,
    time: 10UL,
    transform: { ..a.transform, rotation: { x: 0.0, y: 0.0, z: 1.0, w: 0.0, }, },
  })
  let point = @moonmcap.tf_apply(tree.lookup("world", "base", 5UL).transform, {
    x: 1.0,
    y: 0.0,
    z: 0.0,
  })
  assert_true(point.x.abs() < 0.000000001)
  assert_true((point.y - 1.0).abs() < 0.000000001)
  let inverse = @moonmcap.tf_inverse(
    tree.lookup("world", "base", 5UL).transform,
  )
  let back = @moonmcap.tf_apply(inverse, point)
  assert_true((back.x - 1.0).abs() < 0.000000001 && back.y.abs() < 0.000000001)
  let tree = @moonmcap.tf_tree()
  tree.add({
    ..a,
    transform: {
      ..a.transform,
      rotation: { x: 0.0, y: 0.0, z: 0.0, w: 1.0e300, },
    },
  })
  tree.add({
    ..a,
    time: 10UL,
    transform: {
      ..a.transform,
      rotation: { x: 0.0, y: 0.0, z: 0.0, w: -1.0e300, },
    },
  })
  assert_eq(
    tree.lookup("world", "base", 5UL).transform,
    @moonmcap.tf_identity(),
  )
}

///|
test "TF rejects cycles parent changes extrapolation and excessive gaps without mutation" {
  let tree = @moonmcap.tf_tree(sample_limit=4)
  let a = tf_test_sample("world", "base", 10UL, 1.0)
  tree.add(a)
  tree.add({ ..a, time: 20UL, })
  for
    bad in [
      tf_test_sample("base", "world", 0UL, 0.0),
      { ..a, parent: "other", time: 30UL, },
      tf_test_sample("world", "base", 20UL, 2.0),
      { ..a, is_static: true, time: 30UL, },
      {
        ..a,
        time: 30UL,
        transform: {
          ..a.transform,
          rotation: { x: 0.0, y: 0.0, z: 0.0, w: 0.0, },
        },
      },
    ] {
    try tree.add(bad) catch {
      _ => ()
    } noraise {
      _ => fail("invalid TF accepted")
    }
  }
  for time in [9UL, 21UL] {
    try tree.lookup("world", "base", time) catch {
      _ => ()
    } noraise {
      _ => fail("TF extrapolation accepted")
    }
  }
  try tree.lookup("world", "base", 15UL, max_gap=4UL) catch {
    _ => ()
  } noraise {
    _ => fail("gap accepted")
  }
  try tree.lookup("world", "base", 15UL, interpolate=false) catch {
    _ => ()
  } noraise {
    _ => fail("disabled interpolation accepted")
  }
  try tree.lookup("missing", "missing", 15UL) catch {
    _ => ()
  } noraise {
    _ => fail("unknown frame accepted")
  }
  tree.add({ ..a, time: 30UL, })
  tree.add({ ..a, time: 40UL, })
  try tree.add({ ..a, time: 50UL, }) catch {
    _ => ()
  } noraise {
    _ => fail("history budget exceeded")
  }
  assert_eq(tree.lookup("world", "base", 40UL).transform.translation.x, 1.0)
}

///|
test "TF messages parse exact ROS2 stamps and retain source envelopes" {
  let original : @moonmcap.Message = {
    channel_id: 1,
    sequence: 9U,
    log_time: 100UL,
    publish_time: 101UL,
    data: b"raw",
  }
  let parsed = @moonmcap.parse_tf_message(
    @json.parse(
      "{\"transforms\":[{\"header\":{\"frame_id\":\"world\",\"stamp\":{\"sec\":\"18446744073\",\"nanosec\":709551615}},\"child_frame_id\":\"base\",\"transform\":{\"translation\":{\"x\":1,\"y\":2,\"z\":3},\"rotation\":{\"x\":0,\"y\":0,\"z\":0,\"w\":1}}}]}",
    ),
    false,
    origin=original,
  )
  assert_eq(parsed[0].time, 18446744073709551615UL)
  assert_eq(parsed[0].origin, Some(original))
  let tree = @moonmcap.tf_tree(byte_limit=1)
  try tree.add(parsed[0]) catch {
    _ => ()
  } noraise {
    _ => fail("TF byte budget ignored")
  }
}
