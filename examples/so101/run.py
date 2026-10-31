"""Fetch a pinned SO-101 recording, export a joint-state interval and display recovery evidence."""
import argparse
import hashlib
import html
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent


def sha(data):
    return hashlib.sha256(data).hexdigest()


def render(summary, messages):
    escape = html.escape
    expected = summary["selection"]
    rows = "".join(
        "<tr>" + "".join("<td>" + escape(str(value)) + "</td>" for value in
                         (message["sequence"], message["log_time"], message["publish_time"])) + "</tr>"
        for message in messages
    )
    return f'''<!doctype html>
<html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>SO-101 日志处理结果 · MoonMCAP</title>
<style>
body{{margin:0;background:#f3f6fa;color:#162536;font:16px/1.65 system-ui,"Segoe UI","Microsoft YaHei",sans-serif}}
main{{max-width:1040px;margin:auto;padding:40px 24px}}h1{{font-size:32px;margin:8px 0}}h2{{font-size:21px}}
.eyebrow{{color:#306cab;font-weight:700}}.muted{{color:#506278}}.cards{{display:grid;grid-template-columns:repeat(3,1fr);gap:16px;margin:28px 0}}
.card,section{{background:white;border:1px solid #dce4ee;border-radius:12px;padding:22px}}.number{{font-size:36px;font-weight:750;color:#185d91}}
section{{margin:18px 0}}code{{word-break:break-all;background:#eff4fa;padding:2px 5px;border-radius:4px}}a{{color:#185d91}}
table{{width:100%;border-collapse:collapse;font-size:14px}}th,td{{text-align:left;border-bottom:1px solid #e3e9f0;padding:10px 8px;font-variant-numeric:tabular-nums}}
.scroll{{overflow:auto}}.pass{{color:#17654b;font-weight:650}}.notice{{border-left:4px solid #bd8127;padding-left:16px}}ul{{padding-left:22px}}
@media(max-width:650px){{.cards{{grid-template-columns:1fr}}main{{padding:24px 16px}}h1{{font-size:26px}}}}
</style><main>
<div class="eyebrow">MoonMCAP · 公开录制操作案例</div>
<h1>从 SO-101 日志导出关节状态片段</h1>
<p class="muted">完整校验 → 按主题和时间查询 → 对照侧车索引 → 裁剪并读回 → 验证截断副本的恢复前缀</p>
<p class="pass">本次操作全部通过，原始录制 SHA-256 保持不变。</p>
<div class="cards"><div class="card">原始录制<div class="number">{summary['source_messages']:,}</div>条消息，已完成结构及可用 CRC 校验</div>
<div class="card">目标时间窗<div class="number">{summary['clip_messages']}</div>条 {escape(expected['topic'])} 消息，索引与完整查询一致</div>
<div class="card">可恢复前缀<div class="number">{summary['recovered_prefix_messages']:,}</div>条消息，已写出有效 MCAP 并重新校验</div></div>
<section><h2>选择与结果</h2><p>主题 <code>{escape(expected['topic'])}</code>；时间为 UInt64 纳秒闭区间：</p>
<p><code>{expected['start']}</code> 至 <code>{expected['end']}</code></p>
<p>片段使用 LZ4 编码，保留对应消息的原始 payload、序号与时间。完整查询和索引查询的消息逐项一致。</p>
<div class="scroll"><table><thead><tr><th>序号</th><th>log_time / ns</th><th>publish_time / ns</th></tr></thead><tbody>{rows}</tbody></table></div></section>
<section><h2>输出文件</h2><ul><li><a href="clip.mcap">clip.mcap</a>：目标时间窗的完整 MCAP</li>
<li><a href="source.midx">source.midx</a>：绑定原始录制指纹的侧车索引</li>
<li><a href="recovered.mcap">recovered.mcap</a>：截断副本中的连续有效前缀</li>
<li><a href="summary.json">summary.json</a>：来源、参数、校验值与执行结果</li>
<li><a href="recovery.json">recovery.json</a>：部分恢复的停止偏移和原因</li></ul></section>
<section><h2>恢复结果如何判读</h2><div class="notice">截断由本示例主动制作，原录制没有被改写。本次恢复只证明输出是有效前缀，不能推断输入完整，也不估算丢失消息总量。</div>
<p>截断字节偏移：<code>{summary['derived_fault']['cutoff_bytes']}</code>；恢复报告 complete：<code>false</code>。</p></section>
<section><h2>来源与复现</h2><p><a href="{escape(summary['source']['source'], quote=True)}">发布方 SO-101 案例</a>；原文件大小 {summary['source']['bytes']:,} 字节。</p>
<p>固定 SHA-256：<code>{summary['source']['sha256']}</code></p>
<p>在仓库根目录执行 <code>python examples/so101/run.py</code>。本报告和数据均保存在本地；仓库不重新分发原始录制。</p>
<p class="muted">这是公开录制文件的离线处理案例，不表示已连接实体机械臂，也不包含相机画面。没有提供值的 CRC 不计作已验证校验和。</p></section>
</main></html>'''


def main():
    sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="new result directory; existing directories are refused")
    args = parser.parse_args()
    output = args.output.resolve() if args.output else ROOT / "_interop/so101-demo" / uuid.uuid4().hex
    if not args.output:
        output.parent.mkdir(parents=True, exist_ok=True)
    output.mkdir()  # Exclusive ownership; never merge into an existing user directory.
    expected = json.loads((HERE / "expected-summary.json").read_text(encoding="utf-8"))
    item = json.loads((ROOT / "examples/validation/public-corpus.json").read_text(encoding="utf-8"))["files"][0]
    assert item["sha256"] == expected["source_sha256"]
    source = ROOT / "_interop/public-corpus" / item["name"]
    source.parent.mkdir(parents=True, exist_ok=True)
    if not source.exists():
        with urllib.request.urlopen(item["url"], timeout=120) as response:
            data = response.read(item["bytes"] + 1)
        assert len(data) == item["bytes"] and sha(data) == item["sha256"], "source fingerprint mismatch"
        try:
            with source.open("xb") as file:
                file.write(data)
        except FileExistsError:
            pass
    original = source.read_bytes()
    assert len(original) == item["bytes"] and sha(original) == item["sha256"], "cached recording fingerprint mismatch"
    with (output / "build.log").open("wb") as log:
        subprocess.run([os.environ.get("MOON", "moon"), "build", "--target", "js", "--deny-warn"],
                       cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=600)
    commands = []
    def run(entry, *values, status=0):
        result = subprocess.run([os.environ.get("NODE", "node"), str(ROOT / f"_build/js/debug/build/cmd/{entry}/{entry}.js"), *map(str, values)],
                                cwd=ROOT, capture_output=True, text=True, encoding="utf-8", timeout=180)
        commands.append({"entry": entry, "operation": str(values[0]), "exit_code": result.returncode})
        assert result.returncode == status, result.stderr or result.stdout
        return json.loads(result.stdout), result.stderr
    valid, _ = run("diagnose", "validate", source)
    assert valid["valid"] and int(valid["observed_messages"]) == expected["source_messages"]
    print(f"PASS 原始录制：{expected['source_messages']} 条消息；结构及可用 CRC 校验通过", flush=True)
    options = ["--topic", expected["topic"], "--start", expected["start"], "--end", expected["end"], "--limit", "100000"]
    query, _ = run("mcap", "query", source, "--full", *options)
    index = output / "source.midx"
    run("mcap", "index-build", source, "--output", index)
    run("mcap", "index-check", source, "--index", index)
    indexed, _ = run("mcap", "index-query", source, "--index", index, *options)
    assert query["messages"] == indexed["messages"] and len(query["messages"]) == expected["clip_messages"]
    (output / "query.json").write_text(json.dumps(query, indent=2) + "\n", encoding="utf-8")
    run("mcap", "slice", source, *options, "--compression", "lz4", "--output", output / "clip.mcap")
    clip, _ = run("diagnose", "validate", output / "clip.mcap")
    assert clip["valid"] and int(clip["observed_messages"]) == expected["clip_messages"]
    print(f"PASS 查询与裁剪：{expected['clip_messages']} 条关节状态；索引与完整查询一致", flush=True)
    chunks, position = [], 8
    while position < len(original) - 8:
        opcode, length = struct.unpack_from("<BQ", original, position)
        if opcode == 6:
            chunks.append((position, length))
        position += 9 + length
    chunk, length = chunks[len(chunks) // 2]
    cutoff = chunk + 9 + length // 2
    damaged = output / "derived-truncated.mcap"
    damaged.write_bytes(original[:cutoff])
    invalid, _ = run("diagnose", "validate", damaged, status=1)
    assert not invalid["valid"]
    _, stderr = run("mcap", "recover", damaged, "--compression", "zstd", "--output", output / "recovered.mcap")
    recovery = json.loads(stderr)
    assert not recovery["complete"] and recovery["recovered_messages"] == expected["recovered_prefix_messages"]
    (output / "recovery.json").write_text(json.dumps(recovery, indent=2) + "\n", encoding="utf-8")
    recovered, _ = run("diagnose", "validate", output / "recovered.mcap")
    assert recovered["valid"] and int(recovered["observed_messages"]) == expected["recovered_prefix_messages"]
    assert sha(source.read_bytes()) == item["sha256"]
    summary = {"result": "passed", "source": item, "source_messages": expected["source_messages"],
               "selection": {key: expected[key] for key in ("topic", "start", "end")}, "clip_messages": expected["clip_messages"],
               "recovered_prefix_messages": expected["recovered_prefix_messages"], "original_unchanged": True,
               "derived_fault": {"kind": "artificial truncation of a copy", "cutoff_bytes": cutoff, "chunk_offset": chunk,
                                 "sha256": sha(damaged.read_bytes())}, "commands": commands,
               "artifacts": {name: sha((output / name).read_bytes()) for name in ("clip.mcap", "source.midx", "recovered.mcap")}}
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    (output / "report.html").write_text(render(summary, query["messages"]), encoding="utf-8")
    print(f"PASS 截断副本恢复：{expected['recovered_prefix_messages']} 条有效前缀，原件未改动\n报告：{output / 'report.html'}", flush=True)


if __name__ == "__main__":
    main()
