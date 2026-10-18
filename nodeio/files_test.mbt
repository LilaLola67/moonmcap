///|
extern "js" fn temp_path() -> String =
  #| () => { const fs=process.getBuiltinModule('node:fs'), os=process.getBuiltinModule('node:os'), path=process.getBuiltinModule('node:path'); return path.join(fs.mkdtempSync(path.join(os.tmpdir(),'moonmcap-')),'source'); }

///|
extern "js" fn replace_file(path : String) -> Unit =
  #| path => process.getBuiltinModule('node:fs').writeFileSync(path,'changed length')

///|
extern "js" fn cleanup(path : String) -> Unit =
  #| path => { const fs=process.getBuiltinModule('node:fs'); fs.unlinkSync(path); fs.rmdirSync(process.getBuiltinModule('node:path').dirname(path)); }

///|
test "managed files bound read ahead preserve bytes and close scopes on errors" {
  let path = temp_path()
  let buffer = @buffer.Buffer::Buffer()
  for i = 0; i < 8192; i = i + 1 {
    buffer.write_byte((i % 256).to_byte())
  }
  let bytes = buffer.to_bytes()
  @nodeio.write_atomic_new(path, bytes)
  assert_eq(@nodeio.read_file(path), bytes)
  let file = @nodeio.open_managed_file(path, read_ahead_bytes=512)
  let source = file.source()
  for i = 0; i < 256; i = i + 1 {
    assert_eq(source.read_exact(i.to_uint64(), 1), bytes[i:i + 1].to_owned())
  }
  assert_eq(file.metrics().physical_reads, 1)
  assert_eq(file.metrics().physical_bytes, 512UL)
  assert_eq(file.metrics().cached_bytes, 512)
  assert_eq(source.read_exact(500UL, 700), bytes[500:1200].to_owned())
  assert_true(file.metrics().cached_bytes <= 512)
  assert_eq(source.read_exact(8191UL, 1), bytes[8191:].to_owned())
  file.close()
  file.close()
  assert_true(file.metrics().closed)
  assert_eq(file.metrics().cached_bytes, 0)
  try source.read_exact(0UL, 1) catch {
    _ => ()
  } noraise {
    _ => fail("closed handle read")
  }
  let scope = @nodeio.file_scope(max_open=1)
  assert_eq(scope.open(path).read_exact(0UL, 256), bytes[0:256].to_owned())
  assert_eq(scope.open(path).read_exact(0UL, 1), b"\x00")
  try scope.open(path + "-other") catch {
    _ => ()
  } noraise {
    _ => fail("file count budget")
  }
  scope.close()
  scope.close()
  try scope.open(path) catch {
    _ => ()
  } noraise {
    _ => fail("closed scope")
  }
  cleanup(path)
}

///|
test "managed cached reads reject changed source identity" {
  let path = temp_path()
  @nodeio.write_new_file(path, b"original")
  let file = @nodeio.open_managed_file(path)
  assert_eq(file.source().read_exact(0UL, 1), b"o")
  replace_file(path)
  try file.source().read_exact(0UL, 1) catch {
    _ => ()
  } noraise {
    _ => fail("stale cached source")
  }
  file.close()
  cleanup(path)
}

///|
test "Node sources detect changes and writers refuse existing paths" {
  let path = temp_path()
  @nodeio.write_new_file(path, b"original")
  let source = @nodeio.open_file(path)
  assert_eq(source.read_exact(0UL, 8), b"original")
  try @nodeio.write_new_file(path, b"replacement") catch {
    _ => ()
  } noraise {
    _ => fail("existing file overwritten")
  }
  assert_eq(@nodeio.read_file(path), b"original")
  replace_file(path)
  try source.read_exact(0UL, 8) catch {
    _ => ()
  } noraise {
    _ => fail("changed source was accepted")
  }
  cleanup(path)
}

///|
test "host SHA-256 adapter matches standard empty and abc vectors" {
  assert_eq(
    @nodeio.source_sha256(@mcap.memory_source(b"")),
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  )
  assert_eq(
    @nodeio.source_sha256(@mcap.memory_source(b"abc")),
    "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
  )
}
