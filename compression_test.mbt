///|
test "all compression modes preserve indexed log and statistics" {
  let recorder = @moonmcap.new_builder()
  let channel = recorder.register_channel("/data", "raw")
  for i = 0; i < 30; i = i + 1 { recorder.append(channel, i.to_uint64(), b"abcabcabcabcabcabcabcabcabcabcabcabc") }
  let log = recorder.snapshot()
  for compression in [@moonmcap.Uncompressed, @moonmcap.Lz4, @moonmcap.Zstd] {
    let bytes = @moonmcap.write_indexed(log, compression~, messages_per_chunk=3)
    assert_eq(@moonmcap.read_log(bytes), log)
    assert_eq(@moonmcap.validate_file_indexes(bytes).length(), 10)
    let result = @moonmcap.query_file(bytes, { ..@moonmcap.all_query(), start_time: 3UL, end_time: 5UL })
    assert_eq(result.messages.length(), 3)
  }
}

///|
test "decompression enforces lengths and refuses trailing or corrupt frames" {
  let payload = b"hellohellohellohellohello"
  for compression in [@moonmcap.Lz4, @moonmcap.Zstd] {
    let encoded = @moonmcap.compress_payload(payload, compression)
    assert_eq(@moonmcap.decompress_payload(encoded, compression.name(), payload.length().to_uint64()), payload)
    for bytes in [encoded + b"x", encoded[0:encoded.length() - 1].to_owned()] {
      let rejected = try @moonmcap.decompress_payload(bytes, compression.name(), payload.length().to_uint64()) catch { _ => true } noraise { _ => false }
      assert_true(rejected)
    }
    let rejected = try @moonmcap.decompress_payload(encoded, compression.name(), 1UL) catch { _ => true } noraise { _ => false }
    assert_true(rejected)
    let limits = { ..@moonmcap.default_limits(), max_decompressed_bytes: 2 }
    let rejected = try @moonmcap.decompress_payload(encoded, compression.name(), payload.length().to_uint64(), limits~) catch { _ => true } noraise { _ => false }
    assert_true(rejected)
  }
}

///|
test "large RLE writer splits valid Zstd blocks" {
  let bytes = Bytes::make(300000, b'R')
  let encoded = @moonmcap.compress_payload(bytes, @moonmcap.Zstd)
  assert_eq(@moonmcap.decompress_payload(encoded, "zstd", 300000UL), bytes)
}
