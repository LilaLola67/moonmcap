"""Real filesystem operations with deterministic host-call failures around publication."""

import hashlib, json, os, subprocess, uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

SHIM = r"""const fs=require('node:fs'),p=require('node:path');
const original={};for(const n of ['openSync','closeSync','writeSync','writeFileSync','fsyncSync','linkSync','unlinkSync'])original[n]=fs[n];
const handles=new Map(),synced=new Set();let injected=0;const mode=process.env.MCAP_FAULT;
const relevant=x=>typeof x==='string'&&p.basename(x).startsWith('.out.');
const selected=fd=>relevant(handles.get(fd));
const failure=code=>{injected++;const e=new Error('injected '+mode);e.code=code;return e;};
fs.openSync=function(file,...args){const fd=original.openSync.call(this,file,...args);handles.set(fd,typeof file==='string'?file:'');return fd;};
fs.closeSync=function(fd){const hit=selected(fd),path=handles.get(fd);const r=original.closeSync.call(this,fd);handles.delete(fd);if(hit&&!injected&&(mode==='create-close'||mode==='close'&&synced.has(path)))throw failure('EIO');return r;};
fs.writeFileSync=function(fd,data,...rest){if(selected(fd)&&mode==='space'){original.writeSync(fd,Buffer.from(data).subarray(0,3));throw failure('ENOSPC');}return original.writeFileSync.call(this,fd,data,...rest);};
fs.writeSync=function(fd,buffer,offset,length,position){if(selected(fd)){
 if(mode==='space'){if(!injected){injected++;return original.writeSync.call(this,fd,buffer,offset,Math.min(length,3),position);}throw failure('ENOSPC');}
 if(mode==='zero'){injected++;return 0;}
 if(mode==='short'){injected++;return original.writeSync.call(this,fd,buffer,offset,Math.min(length,3),position);}
}return original.writeSync.apply(this,arguments);};
fs.fsyncSync=function(fd){if(selected(fd)&&mode==='sync')throw failure('EIO');const r=original.fsyncSync.call(this,fd);synced.add(handles.get(fd));return r;};
fs.linkSync=function(from,to){if(relevant(from)&&mode==='link')throw failure('EPERM');return original.linkSync.call(this,from,to);};
fs.unlinkSync=function(file){if(relevant(file)&&mode==='cleanup')throw failure('EACCES');return original.unlinkSync.call(this,file);};
process.on('exit',()=>original.writeFileSync(process.env.MCAP_FAULT_REPORT,JSON.stringify({injected,open_owned_handles:[...handles.values()].filter(relevant).length})));
"""


def main():
    work = ROOT / "_interop/fs-faults" / uuid.uuid4().hex
    work.mkdir(parents=True)
    subprocess.run(
        [
            os.environ.get("MOON", "moon"),
            "build",
            "--target",
            "js",
            "--warn-list",
            "-a",
        ],
        cwd=ROOT,
        capture_output=True,
        check=True,
    )
    cli = ROOT / "_build/js/debug/build/cmd/mcap/mcap.js"
    source = ROOT / "examples/files/zstd-summary.mcap"
    source_sha = hashlib.sha256(source.read_bytes()).hexdigest()
    shim = work / "inject.cjs"
    shim.write_text(SHIM)
    rows = []
    for kind in ("atomic", "stream"):

        def command(target):
            return (
                ["slice", str(source), "--output", str(target)]
                if kind == "atomic"
                else [
                    "query",
                    str(source),
                    "--format",
                    "ndjson",
                    "--output",
                    str(target),
                ]
            )

        control = work / (kind + "-control")
        control.mkdir()
        baseline = control / "out"
        p = subprocess.run(["node", str(cli), *command(baseline)], capture_output=True)
        assert p.returncode == 0, p.stderr
        expected = baseline.read_bytes()
        for mode in (
            ["space", "sync", "link", "close", "cleanup"]
            if kind == "atomic"
            else [
                "space",
                "zero",
                "short",
                "sync",
                "link",
                "close",
                "create-close",
                "cleanup",
            ]
        ):
            directory = work / (kind + "-" + mode)
            directory.mkdir()
            out = directory / "out"
            report = directory / "fault.json"
            env = {**os.environ, "MCAP_FAULT": mode, "MCAP_FAULT_REPORT": str(report)}
            p = subprocess.run(
                ["node", "--require", str(shim), str(cli), *command(out)],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
                encoding="utf-8",
                timeout=90,
            )
            observed = json.loads(report.read_text())
            assert observed["injected"] > 0, (kind, mode, p.stderr)
            assert observed["open_owned_handles"] == 0, (kind, mode, observed)
            if mode == "short":
                assert p.returncode == 0 and out.read_bytes() == expected, (
                    kind,
                    mode,
                    p.stderr,
                )
            else:
                assert p.returncode == 2, (kind, mode, p.stderr)
                error = json.loads(p.stderr)["error"]
                assert ("no progress" if mode == "zero" else "injected") in error, (
                    kind,
                    mode,
                    error,
                )
                if mode == "cleanup":
                    assert "output published" in error and out.read_bytes() == expected
                    again = subprocess.run(
                        ["node", str(cli), *command(out)], capture_output=True
                    )
                    assert again.returncode == 2 and out.read_bytes() == expected
                else:
                    assert not out.exists() and not list(directory.glob(".out.*")), (
                        kind,
                        mode,
                    )
            assert hashlib.sha256(source.read_bytes()).hexdigest() == source_sha
            rows.append(
                {
                    "kind": kind,
                    "fault": mode,
                    "result": "passed",
                    "publication": "complete target retained"
                    if mode == "cleanup"
                    else "complete target"
                    if mode == "short"
                    else "no target",
                    "injections": observed["injected"],
                }
            )
    result = {
        "result": "passed",
        "cases": rows,
        "power_loss_tested": False,
        "source_unchanged": True,
        "notes": "Injected syscall failures exercise real staging writes. Post-link cleanup failure explicitly reports a complete published target; never retry blindly.",
    }
    (ROOT / "_interop/fs-faults/report.json").write_text(
        json.dumps(result, indent=2) + "\n"
    )
    print("Filesystem fault contracts passed:", len(rows), "cases")


if __name__ == "__main__":
    main()
