///|
fn spill_fixture() -> @moonmcap.Log raise @moonmcap.McapError {
  let builder = @moonmcap.new_builder()
  let id = builder.register_channel("/stats", "json")
  for i = 0; i < 256; i = i + 1 {
    let value = [
        "1e3", "-9007199254740992", "9007199254740993", "0.00025", "null",
      ][i % 5]
    let payload = if i % 7 == 0 {
      "{\"g\":\{i % 17}}"
    } else {
      "{\"g\":\{i % 17},\"n\":\{value}}"
    }
    builder.append(id, (i % 43).to_uint64(), @utf8.encode(payload[:]))
  }
  builder.snapshot()
}

///|
fn spill_dataset(
  log : @moonmcap.Log,
) -> @moonmcap.Dataset raise @moonmcap.McapError {
  @moonmcap.open_dataset([@moonmcap.build_query_index(log).query_source()])
}

///|
test "external aggregates exactly match memory groups across repeated spills and merges" {
  let dataset = spill_dataset(spill_fixture())
  let options = {
    ..@moonmcap.aggregate_options(),
    value_path: "/n",
    group_paths: ["/g"],
    window: 10UL,
  }
  let expected = @moonmcap.aggregate_json(
    dataset.cursor(@moonmcap.all_query()),
    dataset.channels(),
    options~,
  )
  let files : Map[Int, @buffer.Buffer] = Map([])
  let closed = [false]
  let out = @buffer.Buffer::Buffer()
  let report = @moonmcap.aggregate_external(
    dataset.cursor(@moonmcap.all_query()),
    dataset.channels(),
    memory_sort_storage(files, closed),
    @moonmcap.new_write_sink(fn(bytes) { out.write_bytes(bytes[:]) }),
    aggregate={ ..options, max_groups: 2, group_bytes: 4096, },
    external={
      ..@moonmcap.external_options(),
      run_bytes: 4096,
      merge_bytes: 8192,
      fan_in: 2,
    },
  )
  let actual : Array[Json] = []
  for line in @utf8.decode(out.to_bytes()[:]).split("\n") {
    if line != "" {
      actual.push(@json.parse(line))
    }
  }
  guard expected is Object(fields) else { fail("aggregate result") }
  assert_eq(Json::array(actual), fields["groups"])
  assert_true(report is { "input_messages": 256, .. })
  assert_true(files.is_empty() && closed[0])
}

///|
test "external aggregate budgets cancellation and run corruption discard temporary state" {
  let log = spill_fixture()
  for mode in [0, 1, 2, 3] {
    let dataset = spill_dataset(log)
    let files : Map[Int, @buffer.Buffer] = Map([])
    let closed = [false]
    let base = memory_sort_storage(files, closed)
    let storage = @moonmcap.sort_storage(
      base.create,
      fn(id) {
        let bytes = files[id].to_bytes().to_array()
        if mode == 3 && bytes.length() > 24 {
          bytes[24] = bytes[24] ^ 0x01
        }
        @moonmcap.memory_source(Bytes::from_array(bytes))
      },
      base.remove,
      finish=base.finish,
    )
    let calls = [0]
    try
      @moonmcap.aggregate_external(
        dataset.cursor(@moonmcap.all_query()),
        dataset.channels(),
        storage,
        @moonmcap.new_write_sink(fn(_) { () }),
        aggregate={
          ..@moonmcap.aggregate_options(),
          value_path: "/n",
          group_paths: ["/g"],
          max_groups: 2,
        },
        external={
          ..@moonmcap.external_options(),
          run_bytes: 4096,
          merge_bytes: if mode == 0 {
            128
          } else {
            8192
          },
          spool_bytes: if mode == 1 {
            32UL
          } else {
            1048576UL
          },
          fan_in: 2,
        },
        cancelled=fn() {
          calls[0] += 1
          mode == 2 && calls[0] > 5
        },
      )
    catch {
      @moonmcap.Invalid(_) => ()
    } noraise {
      _ => fail("aggregate failure expected")
    }
    assert_true(files.is_empty() && closed[0])
  }
}
