///|
test "every possible fragmentation preserves framed records" {
  let bytes = @moonmcap.frame(0x80, b"abc") + @moonmcap.frame(0x81, b"")
  let expected = @moonmcap.decode_records(bytes, base=8)
  for split = 0; split <= bytes.length(); split = split + 1 {
    let stream = @moonmcap.StreamDecoder::new(base=8)
    let actual = stream.feed(bytes[0:split].to_owned())
    for record in stream.feed(bytes[split:].to_owned()) {
      actual.push(record)
    }
    stream.finish()
    assert_eq(actual, expected)
  }
}

///|
test "one-byte streaming and poison behavior" {
  let bytes = @moonmcap.frame(0x80, b"payload")
  let stream = @moonmcap.StreamDecoder::new()
  let result = []
  for byte in bytes {
    for record in stream.feed(Bytes::from_array([byte][:])) {
      result.push(record)
    }
  }
  assert_eq(result.length(), 1)
  assert_eq(stream.buffered_bytes(), 0)
  stream.finish()
  assert_true(
    (Result::Ok(stream.feed(b"")) catch { error => Result::Err(error) })
    is Err(_),
  )
  let bad = @moonmcap.StreamDecoder::new()
  assert_true(
    (Result::Ok(bad.feed(b"\x80\xff\xff\xff\xff\xff\xff\xff\xff")) catch {
      error => Result::Err(error)
    })
    is Err(_),
  )
  assert_eq(bad.buffered_bytes(), 0)
  assert_true(
    (Result::Ok(bad.feed(bytes)) catch { error => Result::Err(error) })
    is Err(_),
  )
}

///|
test "stream finish rejects partial body" {
  let stream = @moonmcap.StreamDecoder::new()
  ignore(stream.feed(@moonmcap.frame(0x80, b"abc")[0:10].to_owned()))
  assert_true(
    (Result::Ok(stream.finish()) catch { error => Result::Err(error) })
    is Err(_),
  )
}
