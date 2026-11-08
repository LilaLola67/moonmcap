# 静态检查与兼容策略

`python tools/static_audit.py` 对 Wasm/JS 执行 `moon check --deny-warn --output-json --no-mbti`，要求没有编译诊断，也检查 CLI 原始警告。CI 的检查、构建和测试同样启用 `--deny-warn`。这会阻止新警告悄悄混入正常构建，但不声称编译器能证明行为正确。

兼容例外集中在 [static-exceptions.json](static-exceptions.json)，每项记录路径、理由和作用域 SHA-256。审计拒绝未登记的抑制、作用域内容漂移及模块或包级警告关闭。修改相关声明须重新审查例外。

保留例外包括 42 个已发布的不透明类型名、7 处上游 Show 派生、3 处以 Result 表达增量状态的上游实现，以及 2 个用于区分旧版取消异常和普通错误的 async catch。后两处的资源清理已移入 `errdefer`，并由实际任务取消回归检查。没有把整个包的警告隐藏。

0.17.5 登记了 48 处旧式派生的声明级例外。固定基准 moonc 0.10.17 无法用 `#derive` 生成 trait 实现，因此这些声明保留 `derive(...)`。最新工具链仍使用 `--deny-warn`。HTTP 缓存和 worker 测试在取消清理期间保护各自的异步夹具关闭操作。

本轮将数值转换改为明确的位重解释 API，保留 Bytes 的真实复制语义；清理使用 `errdefer` 覆盖错误与取消。保留旧 `Replay.resume` 名称和三个 `resume=` 参数标签的兼容别名。`python tools/api_audit.py` 对比冻结的 0.17.3 公开接口，共 781 项声明，检查未意外删除旧接口；它不是行为或二进制 ABI 兼容证明。

默认目标改为 JS，以便默认文档包含 JS 宿主包。根核心库仍支持 Wasm；在依赖方选用 `--target wasm` 不受本模块默认目标影响。Node 专用包通过 `supported_targets="js"` 声明边界，移除曾用来掩盖不支持目标的空桩。旧磁盘产物继续由独立冻结的 0.17.1 样本验证，接口快照不替代产物读取测试。
