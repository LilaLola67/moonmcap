// Derived from bikallem/compress 0.3.4, Apache-2.0. Modified for MCAP bounds; see third_party/README.md.

///|
/// Zstandard compression error type.
pub(all) suberror ZstdError {
  InvalidHeader(String)
  CorruptData(String)
  ChecksumMismatch(expected~ : UInt32, got~ : UInt32)
  WindowTooLarge(String)
  UnexpectedEOF
} derive(Show)

///|
/// Encoder signals — returned by Deflater::encode().
pub(all) enum Encode {
  /// Input consumed. Call encode(Some(more)) or encode(None) to finish.
  Ok
  /// Produced output. Consume it, then call encode() again with same arg.
  Data(BytesView)
  /// Final output flushed. Encoding complete.
  End
  /// Unrecoverable error.
  Error(ZstdError)
} derive(Show)

///|
/// Decoder signals — returned by Inflater::decode().
pub(all) enum Decode {
  /// Need input. Call src(bytes) then decode() again.
  Await
  /// Produced output. Consume it, then decode() again.
  Data(BytesView)
  /// Stream complete.
  End
  /// Unrecoverable format error.
  Error(ZstdError)
} derive(Show)

///|
/// Compression level for Zstandard.
pub(all) enum CompressionLevel {
  Fast
  Default
  Best
  Level(Int32)
} derive(Eq, Show)

///|
/// Zstandard frame magic number.
const ZSTD_MAGIC : UInt32 = 0xFD2FB528_U

///|
/// Block types in Zstandard.
const BLOCK_TYPE_RAW : Int32 = 0

///|
const BLOCK_TYPE_RLE : Int32 = 1

///|
const BLOCK_TYPE_COMPRESSED : Int32 = 2

///|
/// MCAP adapter limit; checked before decoder allocation.
const MAX_WINDOW_SIZE : Int32 = 67108864

///|
/// Maximum block size (128 KB).
const MAX_BLOCK_SIZE : Int32 = 131072
