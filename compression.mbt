///|
pub(all) enum Compression {
  Uncompressed
  Lz4
  Zstd
} derive(Eq, @debug.Debug)

///|
pub extend Compression with Eq::{equal, not_equal}

///|
pub extend Compression with @debug.Debug::{to_repr}

///|
pub fn Compression::name(self : Compression) -> String {
  match self { Uncompressed => ""; Lz4 => "lz4"; Zstd => "zstd" }
}

///|
pub fn compress_payload(bytes : Bytes, compression : Compression) -> Bytes {
  match compression {
    Uncompressed => bytes.copy()
    Lz4 => @lz4.compress(bytes)
    Zstd => @zstd.compress(bytes)
  }
}

///|
pub fn decompress_payload(bytes : Bytes, compression : String, expected_size : UInt64,
  limits? : Limits = default_limits()) -> Bytes raise McapError {
  let expected = bounded_length(expected_size, limits.max_decompressed_bytes, "decompressed chunk")
  if bytes.length() > limits.max_record_bytes { raise Invalid("compressed payload exceeds record limit") }
  match compression {
    "" => {
      if bytes.length() != expected { raise Invalid("uncompressed chunk size mismatch") }
      bytes.copy()
    }
    "lz4" => decode_lz4_frame(bytes, expected)
    "zstd" => {
      validate_zstd_frame(bytes, expected, limits.max_decompressed_bytes)
      let inflater = @zstd.Inflater::new(output_limit=expected)
      inflater.src(bytes[:])
      let out = @buffer.Buffer::Buffer()
      loop inflater.decode() {
        @zstd.Await => raise Invalid("truncated Zstd frame")
        @zstd.Error(error) => raise Invalid("invalid Zstd frame: \{error}")
        @zstd.Data(data) => {
          if data.length() > expected - out.length() { raise Invalid("Zstd output exceeds declared size") }
          out.write_bytes(data)
          continue inflater.decode()
        }
        @zstd.End => {
          if out.length() != expected { raise Invalid("Zstd output size mismatch") }
          out.to_bytes()
        }
      }
    }
    _ => raise Invalid("unsupported chunk compression: \{compression}")
  }
}

///|
/// Preflight the complete frame before a decoder allocates its history/window.
fn validate_zstd_frame(bytes : Bytes, expected : Int, maximum : Int) -> Unit raise McapError {
  let input = cursor(bytes, default_limits())
  if input.u32() != 0xfd2fb528U { raise Invalid("invalid Zstd standard frame magic") }
  let descriptor = input.take(1)[0].to_int()
  if (descriptor & 0x18) != 0 { raise Invalid("reserved Zstd frame bits") }
  let single = (descriptor & 0x20) != 0
  let checksum = (descriptor & 4) != 0
  let mut window_size = expected.to_uint64()
  if !single {
    let wd = input.take(1)[0].to_int()
    let exponent = (wd >> 3) + 10
    let window = (1UL << exponent) + ((wd & 7).to_uint64() * (1UL << (exponent - 3)))
    window_size = window
    if window > maximum.to_uint64() { raise Invalid("Zstd window exceeds decompression limit") }
  }
  let dictionary_size = match descriptor & 3 { 0 => 0; 1 => 1; 2 => 2; _ => 4 }
  let dict = input.take(dictionary_size)
  if dict.iter().any(fn(byte) { byte != 0 }) { raise Invalid("Zstd external dictionary is unsupported") }
  let size_flag = descriptor >> 6
  let content_size = match size_flag {
    0 => if single { Some(input.take(1)[0].to_uint64()) } else { None }
    1 => Some(input.u16().to_uint64() + 256UL)
    2 => Some(input.u32().to_uint64())
    _ => Some(input.u64())
  }
  match content_size {
    Some(size) => if size != expected.to_uint64() { raise Invalid("Zstd content size differs from Chunk") }
    None => ()
  }
  let mut last = false
  while !last {
    let header = input.take(3)
    let bits = header[0].to_int() | (header[1].to_int() << 8) | (header[2].to_int() << 16)
    last = (bits & 1) != 0
    let kind = (bits >> 1) & 3
    let size = bits >> 3
    let maximum_block = if window_size < 131072UL { window_size.to_int() } else { 131072 }
    if kind == 3 || size > maximum_block { raise Invalid("invalid Zstd block type or size") }
    ignore(input.take(if kind == 1 { 1 } else { size }))
  }
  if checksum { ignore(input.u32()) }
  input.finish()
}
