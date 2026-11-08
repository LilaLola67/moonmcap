///|
/// IEEE CRC32, reflected polynomial, initial and final XOR 0xffffffff.
pub fn crc32(bytes : Bytes) -> UInt32 {
  crc32_extend(0U, bytes)
}

///|
let crc32_table : FixedArray[UInt32] = FixedArray::makei(256, index => {
  let mut value = index.reinterpret_as_uint()
  for _ in 0..<8 {
    value = if (value & 1U) != 0U {
      (value >> 1) ^ 0xedb88320U
    } else {
      value >> 1
    }
  }
  value
})

///|
/// Accepts a previously finalized CRC, allowing independent streaming blocks.
pub fn crc32_extend(previous : UInt32, bytes : Bytes) -> UInt32 {
  let mut value = previous ^ 0xffffffffU
  for byte in bytes {
    let index = ((value ^ byte.to_uint()) & 0xffU).reinterpret_as_int()
    value = (value >> 8) ^ crc32_table[index]
  }
  value ^ 0xffffffffU
}

///|
pub fn verify_crc(
  bytes : Bytes,
  expected : UInt32,
  label~ : String = "data",
) -> Unit raise McapError {
  if expected != 0U && crc32(bytes) != expected {
    raise Invalid("\{label} CRC mismatch")
  }
}
