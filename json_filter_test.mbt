///|
test "JSON decimal comparisons preserve integers fractions exponents and zero" {
  for
    (a, op, b, expected) in [
      ("9007199254740993", ">", "9007199254740992", true),
      ("18446744073709551615", "=", "18446744073709551614", false),
      ("0.123456789012345678901", "<", "0.123456789012345678902", true),
      ("-1.20e3", "=", "-1200", true),
      ("-0.0", "=", "0e30", true),
      ("1e1000", ">", "9e999", true),
      ("-1e-999", "<", "0", true),
    ] {
    let expression = "{\"path\":\"/n\",\"op\":\"\{op}\",\"value\":\{b}}"
    let filter = @moonmcap.json_filter(expression)
    assert_eq(filter.matches(@utf8.encode("{\"n\":\{a}}"[:])), expected)
  }
  let payload = b"{\"n\":18446744073709551615}"
  assert_true(
    @moonmcap.json_filter("true")
    .project(payload)
    .stringify()
    .contains("18446744073709551615"),
  )
}

///|
test "JSON Pointer escapes arrays root missing and null remain distinct" {
  let bytes = b"{\"a/b\":{\"~x\":[null,42]},\"\":true}"
  assert_true(
    @moonmcap.json_filter(
      "{\"path\":\"/a~1b/~0x/1\",\"op\":\"=\",\"value\":42}",
    ).matches(bytes),
  )
  assert_true(
    @moonmcap.json_filter("{\"path\":\"/a~1b/~0x/0\",\"op\":\"exists\"}").matches(
      bytes,
    ),
  )
  assert_false(
    @moonmcap.json_filter(
      "{\"path\":\"/missing\",\"op\":\"!=\",\"value\":null}",
    ).matches(bytes),
  )
  assert_false(
    @moonmcap.json_filter("{\"path\":\"/a~1b/~0x/01\",\"op\":\"exists\"}").matches(
      bytes,
    ),
  )
  let result = @moonmcap.json_filter("true", select=[
    "/a~1b/~0x/0", "/missing", "/",
  ]).project(bytes)
  assert_eq(result, {
    "data": { "/a~1b/~0x/0": Json::null(), "/": true },
    "missing_fields": ["/missing"],
  })
  assert_true(
    @moonmcap.json_filter("{\"path\":\"\",\"op\":\"=\",\"value\":[1,null]}").matches(
      b"[1.0,null]",
    ),
  )
}

///|
test "JSON malformed documents strict policy and explicit skip budgets" {
  for
    bytes in [
      b"{\"x\":1,\"x\":2}", b"[1,]", b"01", b"1.", b"true false", b"\xff", b"\"\x01\"",
      b"{\"x\":",
    ] {
    try @moonmcap.json_filter("true").matches(bytes) catch {
      @moonmcap.Invalid(_) => ()
    } noraise {
      _ => fail("invalid JSON accepted")
    }
    assert_false(
      @moonmcap.json_filter("true", invalid=@moonmcap.JsonSkip).matches(bytes),
    )
  }
  for
    expression in [
      "{\"path\":\"/x~2\",\"op\":\"exists\"}", "{\"all\":[]}", "{\"path\":\"/x\",\"op\":\"exists\",\"value\":true}",
    ] {
    try @moonmcap.json_filter(expression) catch {
      @moonmcap.Invalid(_) => ()
    } noraise {
      _ => fail("invalid condition accepted")
    }
  }
  assert_false(
    @moonmcap.json_filter("true", invalid=@moonmcap.JsonSkip, max_bytes=2).matches(
      b"null",
    ),
  )
  assert_false(
    @moonmcap.json_filter("true", invalid=@moonmcap.JsonSkip, max_depth=1).matches(
      b"[[[0]]]",
    ),
  )
}

///|
test "JSON filtered providers paginate only matching messages in global order" {
  let builder = @moonmcap.new_builder()
  let channel = builder.register_channel("/json", "json")
  let raw = builder.register_channel("/raw", "raw")
  for i = 0; i < 20; i = i + 1 {
    builder.append(
      channel,
      (19 - i).to_uint64(),
      @utf8.encode("{\"n\":\{i}}"[:]),
    )
    builder.append(raw, i.to_uint64(), b"invalid but raw")
  }
  let bytes = builder.finish()
  let log = @moonmcap.read_log(bytes)
  let filter = @moonmcap.json_filter(
    "{\"any\":[{\"path\":\"/n\",\"op\":\">=\",\"value\":15},{\"path\":\"/n\",\"op\":\"=\",\"value\":1}]}",
  )
  let providers = [
    @moonmcap.build_query_index(log).query_source().filter_json(filter),
    @moonmcap.open_reader(@moonmcap.memory_source(bytes))
    .query_source()
    .filter_json(filter),
  ]
  for descending in [false, true] {
    let cursor = @moonmcap.open_dataset(providers).cursor({
      ..@moonmcap.all_query(),
      offset: 3,
      limit: 4,
      descending,
    })
    let results = cursor.next_page(2) + cursor.next_page(2)
    assert_eq(results.length(), 4)
    assert_eq(
      results.map(fn(r) { r.message.log_time }),
      if descending {
        [4UL, 3UL, 3UL, 2UL]
      } else {
        [1UL, 2UL, 2UL, 3UL]
      },
    )
    assert_eq(cursor.next(), None)
    cursor.seek(2UL)
    assert_true(cursor.next() is Some(_))
    cursor.cancel()
    assert_eq(cursor.next(), None)
  }
}
