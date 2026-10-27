# MCAP 可运行样例

本目录提供可下载的 `.mcap` 输入、预期输出及独立验证脚本。全部数据均为合成数据，不包含真实采集内容或个人信息。容器目标为 **MCAP 0**，这些输出由 **MoonMCAP 0.2.0** 生成。

## 一键运行

从仓库根目录执行，需要 MoonBit（已验证 moonc 0.10.14）和 Node.js 22.3 以上：

```sh
moon update
python -m pip install mcap==1.3.0 lz4==4.4.5 zstandard==0.25.0
python examples/run.py
```

预期结果：

```text
PASS slice: messages=5 attachments=1 metadata=1
PASS merge: messages=200 attachments=2 metadata=2
PASS recover: messages=48 attachments=0 metadata=0
All 3 runnable examples passed; report: _interop/examples/report.json
```

脚本先校验样例文件的 SHA-256，再编译并调用 MoonBit 文件示例，最后使用官方 Python MCAP 读者检查结果。它逐项比较 schema、主题、编码、序号、时间、payload、附件和元数据，同时验证可用的 CRC 和 Statistics；任一断言失败均以非零状态退出。Python 仅用于独立验证，实际文件转换由 MoonBit 完成。

新生成的输出位于 `_interop/examples/`，不会改写本目录的输入和预期输出。JSON 报告记录各项结果及实际输出摘要；GitHub Actions 也执行这三个样例并保存报告。

已执行的结果见 [verified-results.json](verified-results.json)，其中记录实现提交、结果和输出 SHA-256；重新运行脚本可独立核对。

## 输入文件

| 文件 | Chunk 压缩 | 摘要 | 内容 |
| --- | --- | --- | --- |
| [none-summary.mcap](files/none-summary.mcap) | 无压缩 | 有 | 完整合成日志 |
| [none-no-summary.mcap](files/none-no-summary.mcap) | 无压缩 | 无 | 相同逻辑内容 |
| [lz4-summary.mcap](files/lz4-summary.mcap) | LZ4 | 有 | 相同逻辑内容 |
| [lz4-no-summary.mcap](files/lz4-no-summary.mcap) | LZ4 | 无 | 相同逻辑内容 |
| [zstd-summary.mcap](files/zstd-summary.mcap) | Zstd | 有 | 相同逻辑内容 |
| [zstd-no-summary.mcap](files/zstd-no-summary.mcap) | Zstd | 无 | 相同逻辑内容 |
| [truncated-zstd.mcap](files/truncated-zstd.mcap) | Zstd | 不完整 | 在第 13 个 Chunk 内截断 |

每份完整日志有 100 条消息、`/imu` 和 `/camera` 两个主题（各 50 条）、一个 schema、25 个 Chunk（各 4 条消息）、一个标定附件及一份元数据。消息时间为 1–100 秒，两主题交替出现。输入由官方 `mcap==1.3.0` 生成，生成过程见 [tools/conformance.py](../tools/conformance.py)。

[manifest.json](manifest.json) 列出文件大小、SHA-256、来源版本和每个案例的期望值。输入库版本与文件格式主版本分别标明，不把 Python 包的 1.3.0 称为 MCAP 格式版本。

## 样例一：查询时段并导出

输入：[Zstd 索引日志](files/zstd-summary.mcap)。操作：筛选 `/imu` 在 **20–30 秒闭区间**的消息，保留相关声明、附件和元数据，再输出为 LZ4 索引文件。

在一键脚本创建输出目录后，可单独执行：

```sh
moon run cmd/interop --target js -- examples/files/zstd-summary.mcap _interop/examples/slice-lz4.mcap lz4 slice
```

应得到 **5 条消息**，时间分别为 **21、23、25、27、29 秒**，并保留 **1 个附件、1 份元数据**。消息 payload 与输入对应消息完全相同。

预期输出：[slice-lz4.mcap](expected/slice-lz4.mcap)。该命令是固定主题/时间参数的演示；通用参数化命令已提供，见 [CLI 使用说明](../docs/cli.md)。

## 样例二：合并不同布局和压缩的日志

输入：[无摘要 LZ4 日志](files/lz4-no-summary.mcap)与[无压缩索引日志](files/none-summary.mcap)。操作：按内容去重 schema/channel，安全重映射 ID，保留重复消息，再写入 Zstd 索引文件。

```sh
moon run cmd/interop --target js -- examples/files/lz4-no-summary.mcap _interop/examples/merged-zstd.mcap zstd merge examples/files/none-summary.mcap
```

应得到 **200 条消息**，每主题 **100 条**，以及 **2 个附件、2 份元数据**；相同时间消息保持稳定次序。摘要中的 Statistics、索引及官方读取结果应与内容一致。

预期输出：[merged-zstd.mcap](expected/merged-zstd.mcap)。

## 样例三：恢复中断的压缩日志

输入：[截断 Zstd 日志](files/truncated-zstd.mcap)。它在第 13 个 Chunk 的内部结束，作为完整文件读取应失败。操作：保留连续的完整前缀，遇到不完整块停止，重新生成有效 Zstd 索引文件。

```sh
moon run cmd/interop --target js -- examples/files/truncated-zstd.mcap _interop/examples/recovered-zstd.mcap zstd recover
```

应恢复前 **12 个完整 Chunk 的 48 条消息**，报告停止偏移 **3404 字节**。附件与元数据原本位于截断点之后，恢复结果应为 **0 个附件、0 份元数据**，不会凭空补齐。

预期输出：[recovered-zstd.mcap](expected/recovered-zstd.mcap)。脚本同时检查停止位置、全部恢复消息的原始内容，以及重写文件能否被官方读者读取。

## 更多兼容检查

大文件诊断的 [百万条消息及超过 2 GiB 实测报告](validation/large-source-0.15.12.json) 和 [复现命令](../docs/source-diagnostics.md) 也随仓库提供；测试文件由脚本实际生成，不将大体积输入提交到仓库。

这三个样例展示完整操作流程。完整压缩矩阵、无消息索引、无摘要偏移、混合压缩块及大消息等 **30 项**验证可运行：

```sh
python tools/conformance.py
```

格式与压缩边界见 [兼容说明](../docs/compatibility.md)。不支持的压缩字典、拼接帧或消息体语义解码不会由这些样例隐含承诺。验证关注内容和容器语义一致，不要求未来编码器产生字节完全相同的输出。
