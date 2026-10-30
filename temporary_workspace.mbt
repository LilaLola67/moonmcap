///|
struct TemporaryWorkspace {
  storage : SortStorage
  maximum : UInt64
  live : Map[Int32, UInt64]
  mut bytes : UInt64
  mut peak : UInt64
}

///|
fn TemporaryWorkspace::create(
  self : TemporaryWorkspace,
  id : Int32,
) -> WriteSink raise McapError {
  if self.live.contains(id) {
    raise Invalid("temporary identifier already exists")
  }
  self.live[id] = 0UL
  let target = (self.storage.create)(id)
  new_write_sink(fn(bytes) {
    if bytes.length().to_uint64() > self.maximum - self.bytes {
      raise Invalid("combined temporary disk budget exceeded")
    }
    target.write(bytes)
    self.live[id] = self.live[id] + bytes.length().to_uint64()
    self.bytes += bytes.length().to_uint64()
    if self.bytes > self.peak {
      self.peak = self.bytes
    }
  })
}

///|
fn TemporaryWorkspace::remove(
  self : TemporaryWorkspace,
  id : Int32,
) -> Unit raise McapError {
  if self.live.get(id) is Some(size) {
    (self.storage.remove)(id)
    self.live.remove(id)
    self.bytes -= size
  }
}

///|
fn TemporaryWorkspace::finish(
  self : TemporaryWorkspace,
) -> Unit raise McapError {
  let errors = []
  for id in self.live.keys().to_array() {
    self.remove(id) catch {
      Invalid(reason) => errors.push(reason)
    }
  }
  (self.storage.finish)() catch {
    Invalid(reason) => errors.push(reason)
  }
  if !errors.is_empty() {
    raise Invalid("temporary cleanup failed: \{errors[0]}")
  }
}
