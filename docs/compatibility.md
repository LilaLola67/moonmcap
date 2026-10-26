# MCAP 0 compatibility

MoonMCAP **0.15.16** targets MCAP major version **0**, identified by the `MCAP0`
magic bytes. The official Python `mcap==1.3.0` package is the interoperability
reference; this is a package version, not an MCAP format version.

## Standard container records

All 15 standard record types have encoding/decoding paths:

| Opcode | Record | Validation |
| --- | --- | --- |
| 0x01 | Header | First record, profile/library UTF-8 |
| 0x02 | Footer | Last record, summary boundaries and CRC |
| 0x03 | Schema | Nonzero uint16 ID, consistent declarations |
| 0x04 | Channel | Schema reference and consistent declarations |
| 0x05 | Message | Prior channel reference, exact time/payload representation |
| 0x06 | Chunk | Size, compression frame, decoded records and CRC |
| 0x07 | MessageIndex | Channel, timestamps, chunk-relative record boundaries |
| 0x08 | ChunkIndex | Chunk position, lengths, compression properties and summary declarations |
| 0x09 | Attachment | Bounded data, UTF-8 and attachment CRC |
| 0x0a | AttachmentIndex | Actual attachment position, attributes and coverage |
| 0x0b | Statistics | Unique summary record, counts/time range match data |
| 0x0c | Metadata | Bounded unique-key map |
| 0x0d | MetadataIndex | Actual metadata position, length and name |
| 0x0e | SummaryOffset | Group boundaries/lengths and opcode grouping |
| 0x0f | DataEnd | Exactly one, data-section CRC |

The read path invokes summary, index and Statistics validation. Sequential
files, optional summary/summary-offset sections, absent message indexes and
mixed compression chunks are exercised by `tools/conformance.py`. Index
validation does not turn optional indexes into mandatory ones. Statistics with
a nonempty per-channel count map require preceding summary Channel copies.

CRC value zero means unavailable, as specified by MCAP. Unknown records can be
read through `scan_file`; semantic `Log` rewriting rebuilds indexes and does
not preserve opaque record bytes or unknown appended fields.

## Compression boundary

| Chunk value | Read | Write |
| --- | --- | --- |
| empty string | Uncompressed data | Uncompressed data |
| `lz4` | LZ4 standard frame, independent/dependent blocks, header/block/content checksums | Standard frame, independent blocks |
| `zstd` | One standard frame, raw/RLE/compressed blocks, known/unknown content size, optional checksum | One standard frame with a fixed encoding strategy |

External dictionaries, concatenated frames, skippable frames and unknown
algorithms are rejected. No compression-level compatibility is claimed: the
upstream Zstd encoder accepts a level argument but ignores its value. No Python
process, native codec binding or external conversion tool is used at runtime
on Wasm/JS. [Third-party attribution](../third_party/README.md) describes the
reused implementation and local bounds fixes.

Defaults: 16 MiB per encoded record, 64 MiB per decompressed chunk, 1 MiB per
string, and 100,000 total records including nested chunk records. Frame content
size/window is checked before decoder allocation; LZ4 writes into a fixed
declared-size output, and Zstd checks every block and cumulative output.
These are configurable resource bounds, not a promise of arbitrary-size file
processing or a fixed total-process memory footprint.

## Reproducible cases

[Runnable file examples](../examples/README.md) include six complete MCAP inputs,
one truncated input, three expected outputs and an independent verification
script: `python examples/run.py`. The files and expected results are available
directly in the repository, with SHA-256 values in `examples/manifest.json`.

```sh
moon test --target wasm
moon test --target js
python -m pip install mcap==1.3.0 lz4==4.4.5 zstandard==0.25.0
python tools/interop.py
python tools/conformance.py
```

`tools/conformance.py` generates synthetic two-topic inputs with 100 messages,
25 chunks, one schema, one attachment and one metadata record. It checks 30
cases: 18 input/layout/output combinations, four optional-layout/mixed-mode and three binary/multi-block
cases, and five workflow cases. The generated JSON report includes SHA-256 of
the input files. Reference compression frames are checked in as generated test
data for both targets; `tools/compression_fixtures.py` regenerates them.

Three complete workflows:

1. Read a Zstd indexed file and select `/imu` over the **inclusive** 20–30 second
   interval. Output has five messages at 21, 23, 25, 27 and 29 seconds, plus its
   original attachment/metadata. Re-encode as LZ4 and verify with the reference.
2. Merge an LZ4 file without summary and an uncompressed indexed file. Deduplicate
   declarations, remap IDs, retain all 200 messages, two attachments and two
   metadata records; write Zstd and check content plus Statistics.
3. Truncate the 13th chunk of each compression mode. Recover exactly the preceding
   12 complete chunks (48 messages), stop at the bad chunk's start, and produce a
   valid indexed file. Later attachments/metadata are not fabricated.

The official indexed Python reader can skip messages in files that have
ChunkIndex but no MessageIndex. The reference check uses its official
`NonSeekingReader` for semantic comparison of those valid layouts; indexed
output is also checked through summary and index APIs.

Unit tests additionally reject damaged frames, trailing bytes, unsupported
compression, false Statistics, bad physical indexes and output-length limits.
LZ4 reference frames cover both dependency modes and checksums; Zstd reference
frames cover levels 1/3/19 on the **input** side, missing content size, checksum,
and raw/RLE block examples. Output bytes and compression ratio need not equal
the reference encoder's output.

The library preserves message/schema/channel/attachment/metadata contents
within its declared semantic model. It does not decode CDR/Protobuf payloads,
validate every ROS profile rule or guarantee byte-identical rewrites.
[RangeReader](range-reader.md) performs selective file I/O using summary indexes;
fast mode validates the summary and loaded chunks, not unread data or full index
coverage. Full validation and unindexed fallback have an explicit encoded-file
budget. [StreamWriter](stream-writer.md) has chunk and retained-summary budgets,
synchronous backpressure and terminal failure states. [Batch transforms](batch-transform.md)
use in-memory Logs; [index rebuilding](diagnostics.md) requires a complete valid data section.
The [disk cursor](file-cursor.md) adds lazy ordered pages. The JS [async package](async-io.md)
provides real host IO, rotating recordings and [HTTP ranges](http-source.md).
[External merge](external-merge.md) spills bounded stable runs to disk; it preserves
attachments and metadata, with separate limits for declarations, input chunks and output summary.
[Independent recovery](block-recovery.md) requires CRC-checked summary indexes and individual
chunks and explicitly reports partial extraction; prefix recovery remains unchanged.
These additions retain the container/compression boundary above. The module is distributed as
`LilaLola67/moonmcap@0.15.16` on Mooncakes; GitHub contains the complete source and reproduction tools.

Sources: [MCAP specification](https://mcap.dev/spec),
[compression registry](https://mcap.dev/spec/registry),
[LZ4 frame specification](https://github.com/lz4/lz4/blob/dev/doc/lz4_Frame_format.md),
[Zstandard RFC 8878](https://www.rfc-editor.org/rfc/rfc8878.html).
