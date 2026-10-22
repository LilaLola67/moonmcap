///|
test "stream aggregation keeps exact sums and rational averages" {
  let builder = @moonmcap.new_builder()
  let channel = builder.register_channel("/numbers", "json")
  for
    payload in [
      b"{\"n\":9007199254740993}", b"{\"n\":-9007199254740992}", b"{\"n\":0.25}",
      b"{}", b"{\"n\":null}",
    ] {
    builder.append(channel, 10UL, payload)
  }
  let dataset = @moonmcap.open_dataset([
    @moonmcap.build_query_index(builder.snapshot()).query_source(),
  ])
  let result = @moonmcap.aggregate_json(
    dataset.cursor(@moonmcap.all_query()),
    dataset.channels(),
    options={ ..@moonmcap.aggregate_options(), value_path: "/n", },
  )
  guard result is Object(fields) &&
    fields["groups"] is Array(groups) &&
    groups[0] is Object(row) else {
    fail("aggregate report")
  }
  assert_eq(row["count"], Json::number(5))
  assert_eq(row["numeric_count"], Json::number(3))
  assert_eq(row["missing_count"], Json::number(1))
  assert_eq(row["non_numeric_count"], Json::number(1))
  assert_eq(row["sum"], Json::string("125e-2"))
  assert_eq(row["min"], Json::string("-9007199254740992"))
  guard row["avg"] is Object(avg) else { fail("average") }
  assert_eq(avg["denominator"], Json::number(3))
  assert_eq(avg["decimal"], Json::string("416666666666666667e-18"))
}

///|
test "aggregation canonical groups windows and budgets" {
  let builder = @moonmcap.new_builder()
  let channel = builder.register_channel("/json", "json")
  builder.append(channel, 1UL, b"{\"g\":{\"a\":1,\"b\":2},\"n\":1e3}")
  builder.append(channel, 2UL, b"{\"g\":{\"b\":2.0,\"a\":1.0},\"n\":-1}")
  builder.append(channel, 11UL, b"{\"g\":null,\"n\":1}")
  builder.append(channel, 12UL, b"{\"n\":1}")
  let dataset = @moonmcap.open_dataset([
    @moonmcap.build_query_index(builder.snapshot()).query_source(),
  ])
  let options = {
    ..@moonmcap.aggregate_options(),
    value_path: "/n",
    group_paths: ["/g"],
    window: 10UL,
  }
  let result = @moonmcap.aggregate_json(
    dataset.cursor(@moonmcap.all_query()),
    dataset.channels(),
    options~,
  )
  guard result is Object(fields) && fields["groups"] is Array(groups) else {
    fail("groups")
  }
  assert_eq(groups.length(), 3)
  assert_true(
    groups.any(fn(g) {
      match g {
        Object(row) =>
          row["sum"] == Json::string("999e0") && row["count"] == Json::number(2)
        _ => false
      }
    }),
  )
  for
    settings in [{ ..options, max_groups: 1, }, { ..options, group_bytes: 1, }] {
    let cursor = dataset.cursor(@moonmcap.all_query())
    try
      @moonmcap.aggregate_json(cursor, dataset.channels(), options=settings)
    catch {
      @moonmcap.Invalid(_) => ()
    } noraise {
      _ => fail("aggregate budget")
    }
    assert_eq(cursor.next(), None)
  }
}
