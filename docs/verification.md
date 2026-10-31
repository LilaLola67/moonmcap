# 可复现验收入口

质量证据按运行环境、确切源码和已发布包分别记录。没有运行的项目不能因为另一项
通过就算通过。测试不以源码行数或功能数量作为目标，也没有覆盖率百分比准入线。

```sh
moon update
python -m pip install mcap==1.3.0 lz4==4.4.5 zstandard==0.25.0 protobuf==6.33.0 rosbags==0.11.0
python tools/verify.py --profile source
```

`source` 执行 Wasm/JS 检查、构建、单元测试、运行时及文档核验、承诺追溯、干净导出树
快速开始、CLI 失败合同、文件系统故障、旧产物读取、实际进程中断和批次恢复。
需要 Python 3.12、Git、Node.js 22.3+ 和 moonc 0.10.17 基准或已验证的新版本。
Windows 与 Linux 的最小 Node 22.3.0、Windows 维护版 Node 22 都在 CI 中执行此配置；
主 Linux CI 另执行较完整的互操作工作流，当前编译器有独立任务。

| 配置 | 命令 | 额外依赖和含义 |
| --- | --- | --- |
| 全套源码 | `python tools/verify.py --profile full` | 另安装 PyArrow 21.0.0、foxglove-sdk 0.30.3、websockets 15.0.1；执行工作流及覆盖率盘点，不含需明确选择的公开下载和 2 GiB 场景 |
| 公开录制 | `python tools/verify.py --profile public` | 网络、rosbags；核对固定指纹后测试；下载或指纹失败均导致失败 |
| 已发布版本 | `python tools/verify.py --profile release --version VERSION --commit FULL_SHA` | 工作树须干净且 HEAD 匹配；需要 Mooncakes 与 GitHub API 网络，GitHub API 可使用只读 GITHUB_TOKEN |
| 真实超大文件 | `python tools/large_source_workflows.py --large` | 单独运行，至少 3 GiB 可用空间；常规 CI 不隐式执行 |

确切命令列表在 [verification-plan.json](../tools/verification-plan.json)，运行器不接受任意
shell 命令，也不把缺依赖、下载失败或未执行检查自动降级为通过。某步失败后停止并
记录后续未执行命令。CI 的主任务保留逐项命名步骤，平台任务使用统一 source 配置。

## 证据与判读

`_interop/verification/<随机编号>/manifest.json` 保存 UTC 时间、源码版本、HEAD、
Git 索引树、工作树是否干净、moon/Node/Python/系统版本、每条命令的退出码、耗时及
日志 SHA-256。本次更新的工具报告复制进同一证据目录并记录哈希；不会收集旧报告
冒充本次成功。日志替换本机用户和工作区路径；整个 `_interop` 不进入仓库或发布包。
本地最新入口为 `_interop/verification/latest.json`，CI 将同类证据作为 Actions artifact 保存。

工作树有未提交修改时，报告会明确标记，并只能作为候选验证；验收发布证据要求干净
的确切提交。源码配置通过不能证明 registry 内容；release 配置还会：

1. 读取固定 `@版本` 元数据，检查未撤回、服务端构建成功和 SHA-256。
2. 下载实际归档，核对 SHA-256，逐项按 Git blob 身份匹配目标提交，拒绝额外文件或内容漂移。
3. 核对目标提交的 push CI 已成功完成；可选的大规模/公开数据任务 skipped 不冒充已运行。
4. 在新使用者目录通过 Mooncakes 依赖安装，禁止本地源码路径覆盖，编译自己的 worker 并运行公共 API。
5. 检查在线最新 API 文档已包含目标版本；旧版本不应在新版本文档上线后重用此最新文档断言。

[发布验证工作流](../.github/workflows/release-verify.yml) 只读，不上传包、不持有发布令牌。
发布成功之后使用固定版本和完整提交 SHA 手动触发，安装失败或文档未更新时保留失败并重试核验。

## 风险测试与覆盖率

[契约清单](contracts.json) 关联实现和测试；[旧版本样本](../tests/fixtures/v0.17.1/manifest.json)
冻结生产者与逐文件哈希。[文件系统故障注入](../tools/fs_fault_workflows.py) 检查实际注入
发生、文件句柄关闭、输入不变、提交前目标不存在、提交后目标完整且不被覆盖。

`python tools/coverage_audit.py` 生成六个风险文件的已覆盖/可执行行与未覆盖行号。
仅统计 MoonBit JS 单元测试的行插桩；不是分支覆盖率，不统计内嵌 JS 宿主，也没有合并
外部 CLI 工作流。未覆盖位置用于人工审阅、选择边界回归；不通过重复实现断言来追求数值。
新增风险测试覆盖非法负预算在 IO 前拒绝和校验值合法但状态非法的批次恢复。

## 公开录制的完整操作链

```sh
python tools/real_journey.py
```

该命令使用 [固定公开清单](../examples/validation/public-corpus.json) 中原生 SO-101 MCAP：
完整校验、主题/时间查询、裁剪并由独立 Python MCAP 读取器逐条比较、制作截断副本、
提取连续前缀并独立比较每条消息及可用 CRC，最后确认原件 SHA-256 不变。
截断是测试主动制作的故障，报告记录字节偏移、哈希和方法，不声称原录制存在该故障。
原始公开数据不随仓库或发布包分发；MIT 不改变其来源许可。
