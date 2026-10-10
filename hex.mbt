///|
/// Portable hexadecimal transport for binary fixtures and command output.
pub fn encode_hex(bytes : Bytes) -> String {
  let digits = "0123456789abcdef"
  let out = StringBuilder::new()
  for byte in bytes {
    let value = byte.to_int()
    out.write_char(digits[value >> 4].unsafe_to_char())
    out.write_char(digits[value & 15].unsafe_to_char())
  }
  out.to_string()
}

///|
fn hex_digit(value : Char) -> Int raise McapError {
  if value >= '0' && value <= '9' {
    value.to_int() - '0'.to_int()
  } else if value >= 'a' && value <= 'f' {
    value.to_int() - 'a'.to_int() + 10
  } else if value >= 'A' && value <= 'F' {
    value.to_int() - 'A'.to_int() + 10
  } else {
    raise Invalid("invalid hexadecimal digit")
  }
}

///|
pub fn decode_hex(
  value : String,
  max_bytes? : Int = 16 * 1024 * 1024,
) -> Bytes raise McapError {
  if value.length() % 2 != 0 {
    raise Invalid("hexadecimal string must have even length")
  }
  if max_bytes < 0 || value.length() / 2 > max_bytes {
    raise Invalid("hexadecimal input exceeds byte limit")
  }
  let out = @buffer.Buffer::Buffer()
  for i = 0; i < value.length(); i = i + 2 {
    let high = hex_digit(value[i].unsafe_to_char())
    let low = hex_digit(value[i + 1].unsafe_to_char())
    out.write_byte((high * 16 + low).to_byte())
  }
  out.to_bytes()
}
