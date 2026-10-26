# 分页磁盘侧车索引（MIDX2）

```sh
moon build --target js
node _build/js/debug/build/cmd/paged/paged.js build input.mcap input.midx2 temporary --page-entries 128 --run-bytes 2097152 --spool-bytes 536870912
node _build/js/debug/build/cmd/paged/paged.js query input.mcap input.midx2 --topic /imu --offset 10 --limit 20 --cache-pages 2
node _build/js/debug/build/cmd/paged/paged.js check input.mcap input.midx2
python tools/disk_index_workflows.py
python tools/disk_index_scale.py --million
```

`build_disk_sidecar(source, storage, output, fingerprint, ...)` 严格扫描完整源，保存声明和消息位置，通过临时磁盘外部排序，再按 `(log_time,原始物理序号)` 写入固定宽度页面。原始消息负载不写入索引，不保存全量定位对象数组。两个中间 MCAP 和全部排序段合并计入 spool_bytes；另有一个共享 64 KiB 临时写缓冲，避免逐条打开文件。

`open_disk_sidecar(source, index_source, verification, ...)` 使用随机访问索引，检查元信息和逐页 CRC、记录边界、通道、全局排序和末页零填充。打开时完整审核索引，默认另完整计算源 SHA-256，均使用有界缓冲，不能宣称打开只读取命中页。之后采用二分定位和 LRU 页缓存；取消息时再次验证位置、通道、时间、Chunk 解压和已有 CRC。

验证方式沿用 VerifyContent 与显式 TrustIdentity。CRC 和指纹不是签名，不能证明不可信方重新生成的索引没有遗漏；validate_entries() 可逐条验证定位。源和索引须保持稳定快照。

DiskSidecarCursor 提供 next/next_page、主题和闭区间时间查询、双向分页、seek、取消。同时间按物理次序，倒序反转整个次序。seek 清除 offset、恢复 limit；取消/失败后不能重开。query_source() 接入 Dataset 和 JSON 条件。核心支持 Wasm/JS，文件命令使用 Node。旧 MIDX1 保持兼容，新格式使用独立入口，不能交给旧读取器。

默认 128 条/页，允许 1–8192；每条 36 字节，页末 4 字节 CRC。缓存默认 4 页，允许 0–64；解压块缓存另计默认 8 MiB。声明默认 8 MiB，索引文件预算默认 1 TiB，条数仍受 Limits.max_records 的 Int 计数和扫描上限约束。百万条显式使用 `--record-limit 3000000`，适用于 build/query/check。负载预算不是进程 RSS 上限。

命令只写新路径，暂存后原子发布，失败清理暂存输出及本次临时目录。库调用者负责丢弃失败输出，SortStorage 须专用于任务。取消在读写及扫描阶段检查，不抢占解压计算。

2026-10-26 Windows x64 / Node v24.15.0 实际构建 1,000,000 条乱序消息索引：源 35,006,869 字节，索引 36,020,342 字节，页面 1024 条，排序段 2 MiB。构建约 270.33 秒，峰值 RSS 367,456 KiB；新进程打开并查询峰值 RSS 187,400 KiB，页缓存最多 2 页。打开包含全索引审核及源哈希，不是纯二分查找测量。[完整报告](../examples/validation/disk-index-million-0.15.14.json)。

常规回归覆盖三种压缩、无内置索引、随机/重复时间、主题和正反序分页、过期源、CRC 有效但跨页乱序、头/页损坏、截断、空间预算和清理。百万条脚本按生成规则独立核对序号和负载。CI 默认缩小规模，手动 large_source 可执行百万条索引构建。
