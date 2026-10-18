# 发行包与复现

0.15.5 的发行模块名称为 `LilaLola67/moonmcap`，版本与 Header library 标记一致，MIT 主许可证及第三方许可说明随包提供。

```sh
moon update
moon check
moon build
moon test
moon test --target js
python tools/package_audit.py
```

`package_audit.py` 执行 `moon package --list`，同时进行编译检查并生成实际 ZIP，再逐项核对归档内容必须是 Git 已跟踪文件、无路径穿越、无本地临时目录或个人材料，检查必需清单/许可证及明显个人联系信息。报告记录版本、文件数、字节数及 SHA-256。该命令不上传归档、不发布版本，也不读取账号凭据。

本地排除规则不会替代发行过滤规则：`.moonignore` 独立排除构建/参考依赖/运行报告与私人文件。归档审计在 CI 中再次运行，避免仅凭本机 Git 忽略设置判断发行内容安全。发布须由拥有模块命名空间的 Mooncakes 账号执行；`moon whoami` 可核对身份。服务端试验 `moon publish --dry-run` 也会传输归档，需先通过本地审计。

安装方式为 `moon add LilaLola67/moonmcap@0.15.5`；也可克隆仓库复现 README 示例。当前工具链支持的发行过滤规则见 [MoonBit 模块说明](https://docs.moonbitlang.com/en/latest/toolchain/moon/module.html)。
