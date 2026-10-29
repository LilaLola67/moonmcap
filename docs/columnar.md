# Arrow IPC 与 Parquet 导出

MoonBit 负责 MCAP 容器校验、解码和显式归一化；独立 Python/PyArrow 适配器
将生成的 NDJSON 流转为带类型的列式文件。它是可选外部工具，不是 MoonBit
原生 Arrow/Parquet 编码器，不改变核心包的依赖。

```sh
python -m pip install pyarrow==21.0.0
moon run cmd/payload --target js -- normalize input.mcap examples/payload-rewrite/normalize.json normalized.ndjson
python tools/columnar.py normalized.ndjson examples/columnar/rules.json result.parquet
```

规则见示例：format 为 parquet 或 arrow（IPC file），每列指定 name、JSON Pointer
path、type、nullable。支持 string/bool/binary（严格 Base64 输入）、float64、
int32/uint32/int64/uint64、timestamp_ns（UTC），以及递归 list 和非空 struct。
struct 使用 `{"struct":[{"name":"x","type":"float64"}]}`，
list 使用 `{"list":"uint64"}`；嵌套最多 16 层，每层最多 128 个字段。
缺失与 null 映射为列空值；nullable:false 拒绝两者，struct 内也检查。
拒绝未声明的 struct 成员、重复 JSON 键、隐式字符串化和丢精度浮点转换。

MCAP 时间应首先映射为 uint64 列，能完整保留 0 到 18446744073709551615。
额外的 timestamp_ns 列只接受有符号 64 位范围；超界报错，不截断或自动缩放。
Parquet 使用 2.6 格式保留纳秒。二进制列不转换成字符串数组。

默认每批最多 1000 行或 8 MiB 输入字节，每行最多 1 MiB；可配置 batch_rows、
batch_bytes、row_bytes。限制的是输入批大小，PyArrow 内存和压缩器缓冲另计，
不宣称 RSS 上限。空输入也输出带 schema 的有效空文件。

输出只允许新路径，写入同目录临时文件，成功关闭并同步后通过硬链接防覆盖发布。
失败清理临时文件。文件 schema metadata 保存输入 SHA256、规范化规则 SHA256
和适配器版本；处理过程中再次计算输入散列，输入变化拒绝发布。
报告给出输出散列、行数、批数和输入批峰值。来源 MCAP 的散列保留在上一步
normalize 报告中，不把中间 NDJSON 散列当成 MCAP 散列。

`python tools/columnar_workflows.py` 读回两种实际文件，覆盖嵌套空值、二进制、
UInt64 上限、精确整数、带符号时间两端、跨批边界、规则元数据和失败原子性。
格式写入依据 [Arrow IPC API](https://arrow.apache.org/docs/python/generated/pyarrow.ipc.new_file.html)
及 [ParquetWriter API](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetWriter.html)。
