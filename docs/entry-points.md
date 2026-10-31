# 按输入和完整性要求选择入口

默认目标为 JS，方便文件、网络和异步接口及完整 API 文档。纯核心库仍支持 Wasm 与 JS；运行纯内存示例时可明确使用 `moon run cmd/main --target wasm`。`nodeio`、`asyncio` 和文件命令只声明 JS 支持，不能把其编译成功误认为能在浏览器或 Wasm 中操作 Node 文件系统。

| 目标 / 输入 | 建议起点 | 必须理解的边界 |
| --- | --- | --- |
| 第一次使用、小型本地文件 | [快速开始](quickstart.md)，再运行 [SO-101](../examples/so101/README.md) | 先完整校验，再查询和裁剪；所有命令检查退出状态 |
| 内存中已有完整 Bytes | 根包 `read_log` / `new_builder`，[API](api-contracts.md) | 内存预算须覆盖输入与操作；不会自动变成磁盘流式实现 |
| 大型本地文件，只读少量主题/时间 | [范围读取](range-reader.md)、[侧车索引](sidecar-index.md) | 索引必须匹配当前源；选择性读取只校验访问到的数据，不代表全文件完整 |
| 大量查询、索引本身也很大 | [分页索引](disk-sidecar.md) | 显式设置页缓存与读取预算；同一源的身份变化必须重建索引 |
| 远端不可变文件 | [HTTP 读取](http-source.md)与[持久缓存](http-cache.md) | 服务端须遵守范围、ETag/身份和 identity 编码约定；不是任意 URL 下载兼容层 |
| 持续录制或进程中断后继续 | [录制会话](recording-session.md)、[检查点](resumable-merge.md) | 区分已提交段和未完成尾部；进程终止测试不等于物理断电安全保证 |
| 输入在中间或尾部损坏 | [诊断](source-diagnostics.md)、[恢复](block-recovery.md) | 前缀恢复与跨块挽救不同；报告停止原因/偏移，不能将部分恢复标为完整 |
| 很多文件的有序处理 | [外部排序](external-merge.md)、[数据集](dataset.md) | 磁盘空间、文件句柄和队列均需预算；相关预算不等于总进程 RSS 上限 |
| 并行解压与异步处理 | [worker](parallel-codecs.md)、[异步 IO](async-io.md) | 只有满足分块条件的任务会进入 worker；小任务可能没有收益，取消不撤回已接受的写入 |
| 解码或语义处理 | [消息解码](payload-decoding.md)及对应格式文档 | 以各格式支持表为准；容器可读不等于任意 schema 都能解码 |

## 完整性和资源所有权

完整校验、选择性查询、恢复输出校验是不同结论。`complete=false` 不能靠重写成有效 MCAP 变成 `true`。命令的 stdout 可能在后续失败前已输出部分结果，调用方必须检查退出码；原子文件输出的提交状态见 [失败合同](api-contracts.md)。

创建的 managed source、writer、worker pool 等由创建方按接口约定关闭。游标取消只停止该游标，不关闭由调用者提供且可能共享的源；writer 接管 sink 后的构造/写入失败会通知 sink 取消，清理应幂等。详细保证及测试入口见[风险覆盖](risk-coverage.md)。

Python 用于示例编排、参考读取器和开发验证，MCAP 库不依赖 Python 执行业务逻辑。先使用核心路径；只有数据规模或部署条件确实需要时再启用索引、缓存、并行和可恢复任务。
