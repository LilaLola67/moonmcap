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
