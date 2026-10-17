///|
/// Strict unsigned decimal parsing, preserving all UInt64 timestamp bits.
pub fn parse_decimal(text : String) -> UInt64 raise McapError {
  if text.is_empty() {
    raise Invalid("empty unsigned decimal")
  }
  let mut value = 0UL
  for byte in @utf8.encode(text[:]) {
    if byte < b'0' || byte > b'9' {
      raise Invalid("invalid unsigned decimal: \{text}")
    }
    let digit = (byte.to_int() - 48).to_uint64()
    if value > (18446744073709551615UL - digit) / 10UL {
      raise Invalid("unsigned decimal overflow")
    }
    value = value * 10UL + digit
  }
  value
}
