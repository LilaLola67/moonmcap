///|
test "paged field CRC layout identity and Bloom summaries are conservative" {
  let builder = @moonmcap.new_builder()
  let id = builder.register_channel("/n", "json")
  for i = 0; i < 80; i = i + 1 {
    builder.append(
      id,
      i.to_uint64(),
      @utf8.encode(
        "{\"n\":\{i},\"s\":\"\{if i % 2 == 0 { "aaa" } else { "zzz" }}\"}"[:],
      ),
    )
  }
  let source = @moonmcap.memory_source(builder.finish(messages_per_chunk=2))
  let out = @buffer.Buffer::Buffer()
  let digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  ignore(
    @moonmcap.build_paged_field_index(
      source,
      @moonmcap.new_write_sink(fn(b) { out.write_bytes(b[:]) }),
      ["/n", "/s"],
      fn(_) { digest },
      "v1",
      page_bytes=4096,
      page_zones=2,
    ),
  )
  let bytes = out.to_bytes()
  let prefix = bytes[8:12].to_owned()
  let header_size = 16 + prefix[0].to_int() + (prefix[1].to_int() << 8)
  let header = @moonmcap.open_paged_field_header(
    source.size(),
    bytes.length().to_uint64(),
    bytes[0:header_size].to_owned(),
    digest,
    "v1",
  )
  assert_eq(header.layout().pages, 20)
  let filter = @moonmcap.json_filter(
    "{\"path\":\"/s\",\"op\":\"=\",\"value\":\"middle\"}",
  )
  let present = @moonmcap.json_filter(
    "{\"path\":\"/s\",\"op\":\"=\",\"value\":\"aaa\"}",
  )
  let page = header.decode_page(
    bytes[header_size:header_size + 4096].to_owned(),
  )
  assert_eq(page.may_match(page.first_offset(), 1, filter), true) // mismatched spans stay candidates
  let locations : Array[@moonmcap.MessageLocation] = []
  ignore(
    @moonmcap.scan_source(
      source,
      @moonmcap.source_visitor(message=fn(_, location) {
        locations.push(location)
      }),
    ),
  )
  assert_eq(
    page.may_match(locations[0].offset, locations[0].span, filter),
    false,
  )
  assert_eq(
    page.may_match(locations[0].offset, locations[0].span, present),
    true,
  )
  let damaged = bytes[header_size:header_size + 4096].to_owned().to_array()
  damaged[100] = damaged[100] ^ 0x01
  try header.decode_page(Bytes::from_array(damaged)) catch {
    _ => ()
  } noraise {
    _ => fail("corrupt field page accepted")
  }
  try
    @moonmcap.open_paged_field_header(
      source.size(),
      bytes.length().to_uint64(),
      bytes[0:header_size].to_owned(),
      digest,
      "different",
    )
  catch {
    _ => ()
  } noraise {
    _ => fail("decoder mismatch accepted")
  }
}
