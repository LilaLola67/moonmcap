# 发行包与复现

后续每个 minor 的 patch 编号使用 1–10：0.16.10 后为 0.17.1，0.17.10 后为 0.18.1，以此类推；已经发布的历史版本保持原编号。

0.17.4 的发行模块名称为 `LilaLola67/moonmcap`，版本与 Header library 标记一致，MIT 主许可证及第三方许可说明随包提供。

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

安装方式为 `moon add LilaLola67/moonmcap@0.17.4`；也可克隆仓库复现 README 示例。当前工具链支持的发行过滤规则见 [MoonBit 模块说明](https://docs.moonbitlang.com/en/latest/toolchain/moon/module.html)。

运行组件的 LZ4 编码、XXH32/XXH64 和字节缓冲已按原许可适配为内部包，避免构建器读取上游旧格式模块清单。`runtime_audit.py` 核对源码来源、许可标记和实际包导入，防止依赖旧清单的路径被重新引入。内部引用代码不会计入原创核心源码行数。

GitHub Actions 的主要任务固定使用 `moonc 0.10.17+d971670cd`，完整执行 Wasm/JS 检查、构建、测试、文件互操作和恢复工作流；手动规模测试与公开语料任务也使用同一基准。该版本高于 `moonc >= 0.10.16` 的要求。另以当前官方工具链执行 JS 检查、构建、测试和文档生成。Mooncakes 的包上传、独立安装和在线文档生成分别检查，上传成功本身不能替代后两项证据。

发布后执行 `python tools/install_smoke.py --version 0.17.4`。该脚本不使用当前仓库的源路径覆盖，在新目录依赖指定的 Mooncakes 版本，并以显式成功标记确认实际运行完成。`MOON` 环境变量可指定另一套工具链以复核安装兼容性。

## GitHub Release

发行按确切提交执行：先通过源码 CI（包含 macOS ARM64 / Intel），发布 Mooncakes 并通过只读发行包核验，再创建指向该提交的 annotated tag `vVERSION`。GitHub Release 说明记录版本、完整提交、兼容变化、安装方式、边界及 CI 链接。先上传草稿资产并校验，再公开发布。

资产为与 Mooncakes SHA-256 一致的实际 ZIP、`SHA256SUMS`、去除本地路径的 `quality-evidence.json` 和仅含元数据的 `so101-preview.html`。不附带公开录制原件、私人文档、凭据或聊天内容。GitHub 自动生成的 Source code 压缩包与 Mooncakes 发行 ZIP 不同，二者不要求字节相同。

发布后任何人可只读复核固定标签、提交、四项资产及 registry 归档的一致性：

```sh
python tools/github_release_verify.py --version VERSION --commit FULL_SHA
```

验证检查当前公开状态，不宣称未来资产不可修改；测试详情以固定提交的 CI 证据为准。[GitHub Release 列表](https://github.com/LilaLola67/moonmcap/releases)提供实际发布入口。

只读 `verify-published-release` 工作流支持 `github_release=true`；在 GitHub Release 公开后启用，除发行包核验外还执行上述标签和资产检查，并保存报告。发布前保持该选项关闭。
