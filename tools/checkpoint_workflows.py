"""Kill a real merge process, resume committed work, and verify ownership checks."""
import io
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "_interop_deps"))
from mcap.writer import Writer, CompressionType
from mcap.reader import NonSeekingReader


def run():
    work = ROOT / "_interop/checkpoint"
    work.mkdir(parents=True, exist_ok=True)
    subprocess.run([os.environ.get("MOON", "moon"), "build", "--target", "js"], cwd=ROOT, check=True, capture_output=True)
    cli = ROOT / "_build/js/debug/build/cmd/mcap/mcap.js"
    base = [os.environ.get("NODE", "node"), str(cli)]

    def command(*args):
        return subprocess.run([*base, *map(str, args)], cwd=ROOT, capture_output=True, text=True, encoding="utf-8", timeout=180)

    rows, inputs, expected = [], [], []
    for source in range(2):
        buf = io.BytesIO()
        writer = Writer(buf, compression=CompressionType.ZSTD, chunk_size=4096, enable_data_crcs=True)
        writer.start()
        channel = writer.register_channel("/shared", "raw", 0)
        for i in range(600):
            data = bytes([source, i % 256])*256
            writer.add_message(channel, (599-i) % 97, data, i, i)
            expected.append((source, i, (599-i) % 97, data))
        writer.add_metadata("source", {"number": str(source)})
        writer.add_attachment(1, 2, f"source-{source}", "application/octet-stream", b"attachment")
        writer.finish()
        path = work / f"input-{source}.mcap"
        path.write_bytes(buf.getvalue())
        inputs.append(path)
    # A fresh owned child is used for each invocation; never remove arbitrary paths.
    suffix = str(time.time_ns())
    job, output = work / ("job-"+suffix), work / ("result-"+suffix+".mcap")
    args = ["external-merge", *inputs, "--job", job, "--output", output, "--run-bytes", 4096, "--fan-in", 2, "--compression", "lz4"]
    process = subprocess.Popen([*base, *map(str, args)], cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    journal = job / "checkpoint.bin"
    initial, completed = None, None
    deadline = time.monotonic()+90
    while time.monotonic() < deadline and process.poll() is None:
        if journal.exists():
            try:
                checkpoint = journal.read_bytes()
                pos = 8+65
                count = struct.unpack_from("<I", checkpoint, pos)[0]
                pos += 4
                for _ in range(count+1):
                    n = struct.unpack_from("<I", checkpoint, pos)[0]
                    pos += 4+n
                next_id, passes = struct.unpack_from("<II", checkpoint, pos+8)
                if initial is None:
                    initial = next_id
                if passes >= 2:
                    completed = (next_id, passes)
                    # Inspect/resume must reject the lock held by this running process.
                    locked = command("job-inspect", job)
                    assert locked.returncode != 0 and "locked" in locked.stderr
                    process.kill()
                    break
            except (FileNotFoundError, struct.error):
                pass
        time.sleep(0.005)
    stdout, stderr = process.communicate(timeout=20)
    assert completed is not None and process.returncode != 0, (stdout, stderr)
    assert not output.exists()
    rows.append({"case": "process-killed-after-committed-groups", "result": "passed", "completed_groups": completed[1]})
    # Clone only this test-owned directory to test failed resume without altering the reusable job.
    changed = work / ("changed-"+suffix)
    shutil.copytree(job, changed)
    altered = list(args)
    altered[altered.index(job)] = changed
    altered += ["--resume"]
    altered[altered.index("--fan-in")+1] = 3
    result = command(*altered)
    assert result.returncode != 0 and "options changed" in result.stderr, result.stderr
    assert command("job-clean", changed).returncode == 0
    rows.append({"case": "settings-mismatch", "result": "passed"})
    damaged = work / ("damaged-"+suffix)
    shutil.copytree(job, damaged)
    runfile = next(p for p in damaged.glob("*.run") if p.name != "-1.run")
    data = bytearray(runfile.read_bytes())
    data[-1] ^= 1
    runfile.write_bytes(data)
    altered = list(args)
    altered[altered.index(job)] = damaged
    result = command(*altered, "--resume")
    assert result.returncode != 0 and "fingerprint changed" in result.stderr, result.stderr
    assert command("job-clean", damaged).returncode == 0
    rows.append({"case": "run-corruption", "result": "passed"})
    # Uncommitted run and partial-output files are discarded under the job lock.
    (job / "999999.run").write_bytes(b"incomplete")
    result = command(*args, "--resume")
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["runs_created"] >= completed[0]
    actual = [(m.sequence, m.log_time, m.data) for _, _, m in NonSeekingReader(io.BytesIO(output.read_bytes())).iter_messages(log_time_order=False)]
    wanted = sorted(expected, key=lambda row: (row[2], row[0], row[1]))
    assert actual == [(i, t, data) for _, i, t, data in wanted]
    assert len(list(NonSeekingReader(io.BytesIO(output.read_bytes())).iter_attachments())) == 2
    assert len(list(NonSeekingReader(io.BytesIO(output.read_bytes())).iter_metadata())) == 2
    inspected = command("job-inspect", job)
    assert inspected.returncode == 0 and json.loads(inspected.stdout)["published"]
    assert command(*args, "--resume").returncode != 0
    saved_output = output.read_bytes()
    (job / "unrelated.txt").write_text("keep")
    assert command("job-clean", job).returncode != 0 and (job / "unrelated.txt").read_text() == "keep"
    (job / "unrelated.txt").unlink()
    assert command("job-clean", job).returncode == 0 and not job.exists()
    assert output.read_bytes() == saved_output
    rows += [{"case": "resume-matches-independent-reference", "result": "passed"}, {"case": "publication-inspection-and-owned-cleanup", "result": "passed"}]
    (work / "report.json").write_text(json.dumps(rows, indent=2)+"\n")
    print(f"Resumable merge workflows: {len(rows)} passed")


if __name__ == "__main__":
    run()
