///|
/// IEEE CRC32, reflected polynomial, initial and final XOR 0xffffffff.
pub fn crc32(bytes : Bytes) -> UInt {
  crc32_extend(0U, bytes)
}

///|
/// Accepts a previously finalized CRC, allowing independent streaming blocks.
pub fn crc32_extend(previous : UInt, bytes : Bytes) -> UInt {
  let mut value = previous ^ 0xffffffffU
  for byte in bytes {
    value = value ^ byte.to_uint()
    for i = 0; i < 8; i = i + 1 {
      value = if (value & 1U) != 0U {
        (value >> 1) ^ 0xedb88320U
      } else { value >> 1 }
    }
  }
  value ^ 0xffffffffU
}

///|
pub fn verify_crc(bytes : Bytes, expected : UInt, label? : String = "data") -> Unit raise McapError {
  if expected != 0U && crc32(bytes) != expected {
    raise Invalid("\{label} CRC mismatch")
  }
}
