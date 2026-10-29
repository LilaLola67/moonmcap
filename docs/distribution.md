# 发行包与复现

从 0.16.8 起，每个 minor 的 patch 编号使用 1–10：0.16.80 后为 0.17.1，以此类推；已经发布的历史版本保持原编号。

0.16.8 的发行模块名称为 `LilaLola67/moonmcap`，版本与 Header library 标记一致，MIT 主许可证及第三方许可说明随包提供。

```sh
moon update
moon check
moon build
moon test
moon test --target js
python tools/package_audit.py
python tools/runtime_audit.py
```

`package_audit.py` 执行 `moon package --list`，同时进行编译检查并生成实际 ZIP，再逐项核对归档内容必须是 Git 已跟踪文件、无路径穿越、无本地临时目录或个人材料，检查必需清单/许可证及明显个人联系信息。报告记录版本、文件数、字节数及 SHA-256。该命令不上传归档、不发布版本，也不读取账号凭据。

本地排除规则不会替代发行过滤规则：`.moonignore` 独立排除构建/参考依赖/运行报告与私人文件。归档审计在 CI 中再次运行，避免仅凭本机 Git 忽略设置判断发行内容安全。发布须由拥有模块命名空间的 Mooncakes 账号执行；`moon whoami` 可核对身份。服务端试验 `moon publish --dry-run` 也会传输归档，需先通过本地审计。

安装方式为 `moon add LilaLola67/moonmcap@0.16.8`；也可克隆仓库复现 README 示例。当前工具链支持的发行过滤规则见 [MoonBit 模块说明](https://docs.moonbitlang.com/en/latest/toolchain/moon/module.html)。

运行组件的 LZ4 编码、XXH32/XXH64 和字节缓冲已按原许可适配为内部包，避免构建器读取上游旧格式模块清单。`runtime_audit.py` 核对源码来源、许可标记和实际包导入，防止依赖旧清单的路径被重新引入。内部引用代码不会计入原创核心源码行数。

GitHub Actions 分别验证已声明的 0.10.14 工具链和当前官方工具链。当前工具链任务执行 JS 检查、构建、测试和文档生成；文件互操作及恢复验证由固定工具链任务完整执行。Mooncakes 的包上传、独立安装和在线文档生成分别检查，上传成功本身不能替代后两项证据。

发布后执行 `python tools/install_smoke.py --version 0.16.8`。该脚本不使用当前仓库的源路径覆盖，在新目录依赖指定的 Mooncakes 版本，并以显式成功标记确认实际运行完成。`MOON` 环境变量可指定另一套工具链以复核安装兼容性。
