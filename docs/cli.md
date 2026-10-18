# 通用命令行

内容条件与字段投影见 [JSON 查询](json-query.md)；持久任务、恢复、查看和清理见 [可恢复合并](resumable-merge.md)。输入句柄的关闭与有界预读见 [文件 I/O](file-io.md)。

大文件有序合并使用 `external-merge`，按输入读取、排序段和归并预算处理，不受整文件载入限制。参数和失败清理约定见 [外部排序与合并](external-merge.md)。

需要 Node.js 22.3+。以下命令从项目根目录运行；`--` 后面是应用参数。

```sh
moon run cmd/mcap --target js -- help
moon run cmd/mcap --target js -- inspect examples/files/zstd-summary.mcap
moon run cmd/mcap --target js -- query examples/files/zstd-summary.mcap --topic /imu --start 20000000000 --end 30000000000 --format csv
moon run cmd/mcap --target js -- slice input.mcap --topic /imu --start 20 --end 30 --compression lz4 --output sliced.mcap
moon run cmd/mcap --target js -- merge a.mcap b.mcap --compression zstd --output merged.mcap
moon run cmd/mcap --target js -- recover broken.mcap --output recovered.mcap
moon run cmd/mcap --target js -- rewrite input.mcap --compression zstd --output indexed.mcap
```

也可 `moon build --target js` 后运行 `node _build/js/debug/build/cmd/mcap/mcap.js ...`。

| 参数 | 含义 |
| --- | --- |
| --topic TOPIC | query/slice 的主题，可重复指定 |
| --start NS / --end NS | UInt64 十进制纳秒，闭区间；默认完整范围 |
| --descending / --offset N / --limit N | query/slice 的顺序及分页，默认最多 100000 条 |
| --output PATH | MCAP 输出必填；inspect/query 可指定报告文件，默认标准输出 |
| --compression none/lz4/zstd | MCAP 输出压缩方式，默认 none |
| --chunk-messages N | MCAP 输出每块消息数，默认 256 |
| --format json/csv | query 导出格式，默认 json；inspect 为 json |
| --cache-bytes N | query 解压缓存预算，默认 8 MiB |
| --max-file-bytes N | 完整读取的编码文件预算，默认 64 MiB；merge 为全部输入合计 |
| --full | query 先执行完整容器校验；受完整读取预算约束 |

query 使用 RangeReader 的磁盘消息游标；快速模式只校验摘要和已读取块，在 JSON diagnostics 中明确标记 full_validation=false。无索引时使用有预算的完整扫描回退。NDJSON 逐条输出；普通 JSON/CSV 按查询 limit 收集返回消息。

inspect、slice、merge、recover、rewrite 在内存中读取、校验和处理数据。slice 默认保留原附件和元数据；recover 只恢复连续有效前缀，将停止位置/原因写到标准错误。JSON 消息时间使用字符串，负载使用 data_hex；显式 --where/--select 增加 JSON 内容解析与 decoded 字段。CSV 使用 CRLF 并将 topic 包在引号中。

输出路径必须不存在，包括作为输出的输入文件自身。先在同目录完整写入并同步临时文件，再通过硬链接独占发布，避免读到半成品或覆盖已有文件；文件系统必须支持同目录硬链接。不支持时报告失败，不退回覆盖写入。参数错误不产生输出，失败清理本次临时文件；不保证跨断电的目录持久性。

成功退出码为 0，参数/操作错误为 2，错误以 JSON 输出到标准错误。validate 检查到格式错误时将诊断写到报告输出并退出 1。`--` 作为应用参数可结束选项解析，随后将以 `--` 开头的名称当作输入路径。未知选项、重复单值选项、缺失值和数字溢出均报错。

执行 `python tools/cli_workflows.py` 可验证命令、CSV、精确时间、压缩输出、防覆盖及失败原子性。

`transform` 命令支持主题映射、时间窗采样、时间对齐、附件及元数据筛选，参数与处理顺序见 [批量变换](batch-transform.md)。

`validate INPUT [--output report.json]` 提供结构化完整诊断；`reindex INPUT --output NEW.mcap` 在数据区完整有效时重建索引和摘要，支持压缩及分块参数。详见 [诊断与修复](diagnostics.md)。两者均受 --max-file-bytes 完整读取预算限制。
# 独立块部分恢复

`recover-blocks INPUT --output NEW.mcap` 输出恢复清单到标准输出。完整边界及重定向示例见 [独立块恢复](block-recovery.md)。该模式不改变 `recover` 的连续前缀语义。

