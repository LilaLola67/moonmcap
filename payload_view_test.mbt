///|
test "binary views avoid array materialization and enforce window boundaries" {
  let decoder = @moonmcap.cdr_decoder(
    "demo/Bytes", b"uint32 stamp\nuint8[] data\nuint16 tail",
  )
  let bytes = b"\x00\x01\x00\x00\x09\x00\x00\x00\x04\x00\x00\x00\x01\x02\xfe\xff\x34\x12"
  let selection = { ..@moonmcap.decode_selection(), inline_array_items: 2, }
  let view = decoder.view(bytes, selection~)
  assert_eq(view.binary_fields().length(), 1)
  assert_eq(view.read_elements("/data", 2, 2), @json.parse("[254,255]"))
  assert_eq(view.read_binary("/data", 1, 2), b"\x02\xfe")
  let out = @buffer.Buffer::Buffer()
  assert_eq(
    view.write_binary(
      "/data",
      @moonmcap.new_write_sink(fn(b) { out.write_bytes(b[:]) }),
      block_bytes=1,
    ),
    4UL,
  )
  assert_eq(out.to_bytes(), b"\x01\x02\xfe\xff")
  for (start, count) in [(4, 1), (-1, 1), (0, 5)] {
    try view.read_elements("/data", start, count) catch {
      _ => ()
    } noraise {
      _ => fail("out of bounds window accepted")
    }
  }
  try view.read_binary("/data", 0, 4, max_bytes=3) catch {
    _ => ()
  } noraise {
    _ => fail("binary budget ignored")
  }
  let selected = decoder.view(bytes, selection={
    ..selection,
    paths: ["/tail"],
  })
  assert_eq(selected.json(), @json.parse("{\"tail\":4660}"))
  assert_eq(selected.binary_fields().length(), 0)
  try decoder.view(bytes[0:bytes.length() - 1].to_owned(), selection~) catch {
    _ => ()
  } noraise {
    _ => fail("truncated unselected tail accepted")
  }
  let boolean = @moonmcap.cdr_decoder("demo/Flags", b"bool[] flags")
  try
    boolean.view(b"\x00\x01\x00\x00\x02\x00\x00\x00\x00\x02", selection={
      ..selection,
      inline_array_items: 0,
    })
  catch {
    _ => ()
  } noraise {
    _ => fail("invalid skipped boolean accepted")
  }
  try
    decoder.view(bytes, selection={ ..selection, paths: ["/data/3"], })
  catch {
    _ => ()
  } noraise {
    _ => fail("opaque array element silently selected")
  }
}
