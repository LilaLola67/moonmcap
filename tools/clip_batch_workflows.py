"""Independent filesystem checks for resumable context-clip batches."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / '_interop_deps'))
from mcap.reader import make_reader
from mcap.writer import Writer, CompressionType


def main():
    work = ROOT / '_interop/clip-batches' / uuid.uuid4().hex
    work.mkdir(parents=True)
    subprocess.run([os.environ.get('MOON', 'moon'), 'build', '--target', 'js', '--warn-list', '-a'],
                   cwd=ROOT, capture_output=True, check=True)
    cli = ROOT / '_build/js/debug/build/cmd/incident/incident.js'
    source = work / 'source.mcap'
    with source.open('wb') as f:
        writer = Writer(f, compression=CompressionType.NONE, chunk_size=65536, enable_data_crcs=True)
        writer.start()
        channel = writer.register_channel('/sensor', 'json', 0)
        for i in range(3000):
            payload = json.dumps({'n': i, 'data': hashlib.sha256(str(i).encode()).hexdigest() * 16}).encode()
            writer.add_message(channel, i, payload, i, i)
        writer.finish()
    config = {'windows': [{'start': str(i * 1000), 'end': str(i * 1000 + 999)} for i in range(3)],
              'topics': ['/sensor']}
    commands = 0

    def command(mode, directory, recipe=None):
        nonlocal commands
        commands += 1
        path = work / f'config-{commands}.json'
        path.write_text(json.dumps(config if recipe is None else recipe), encoding='utf-8')
        return ['node', str(cli), mode, str(source), str(path), str(directory)]

    def run(mode, directory, recipe=None, status=0):
        p = subprocess.run(command(mode, directory, recipe), cwd=ROOT, capture_output=True,
                           text=True, encoding='utf-8', timeout=180)
        assert p.returncode == status, (p.stderr, p.stdout)
        return json.loads(p.stdout) if status == 0 else None

    job = work / 'batch'
    assert run('batch', job, {**config, 'stop_after': 1})['completed'] == 1
    first = job / 'clip-0.mcap'
    initial = (hashlib.sha256(first.read_bytes()).hexdigest(), first.stat().st_mtime_ns)
    # Simulate an interrupted next output: no durable receipt, never published.
    (job / 'clip-1.part').write_bytes(b'incomplete MCAP prefix')
    done = run('resume-batch', job)
    assert done['complete'] and done['reused'] == 1 and done['executed'] == 2
    assert (hashlib.sha256(first.read_bytes()).hexdigest(), first.stat().st_mtime_ns) == initial
    for ordinal in range(3):
        with (job / f'clip-{ordinal}.mcap').open('rb') as f:
            messages = list(make_reader(f, validate_crcs=True).iter_messages())
        assert [m.sequence for _, _, m in messages] == list(range(ordinal * 1000, (ordinal + 1) * 1000))
    assert run('resume-batch', job)['executed'] == 0
    run('resume-batch', job, {**config, 'topics': ['/other']}, status=2)
    run('batch', job, status=2)
    # A live owner cannot be displaced, even by a second resume process.
    lock = job / 'lock'
    live_lock = json.dumps({'pid': os.getpid(), 'key': 'test-live-owner'})
    lock.write_text(live_lock)
    run('resume-batch', job, status=2)
    assert lock.read_text() == live_lock
    lock.unlink()
    # A real killed process leaves a stale lock, reclaimed on the next resume.
    interrupted = work / 'interrupted'
    args = command('batch', interrupted)
    process = subprocess.Popen(args, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    deadline = time.monotonic() + 60
    killed = False
    while time.monotonic() < deadline:
        if (interrupted / 'clip-0.part').exists():
            process.kill()
            killed = True
            break
        if process.poll() is not None:
            break
        time.sleep(0.002)
    if not killed:
        process.kill()
    process.communicate(timeout=15)
    assert killed, 'failed to interrupt actual staged output'
    assert (interrupted / 'lock').exists()
    assert run('resume-batch', interrupted)['complete']
    assert not (interrupted / 'lock').exists()
    assert not list(interrupted.glob('*.part'))
    # Completed files and checkpoint receipts cannot be silently replaced.
    original = first.read_bytes()
    first.write_bytes(original + b'modified')
    run('resume-batch', job, status=2)
    assert first.read_bytes() == original + b'modified'
    first.write_bytes(original)
    manifest = job / 'manifest.bin'
    saved = manifest.read_bytes()
    manifest.write_bytes(saved[:-1] + bytes([saved[-1] ^ 1]))
    run('resume-batch', job, status=2)
    assert manifest.read_bytes() != saved
    report = {'result': 'passed', 'commands': commands, 'origin': 'synthetic independent writer',
              'checks': ['per-output resume', 'completed output reused without rewriting',
                         'incomplete output restarted', 'actual killed-process recovery',
                         'live lock refusal', 'configuration change refusal',
                         'completed output SHA-256 validation', 'checkpoint CRC validation',
                         'independent MCAP payload/order/CRC checks'],
              'checkpoint_scope': 'whole completed outputs, not mid-chunk or mid-output'}
    (ROOT / '_interop/clip-batches/report.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print(f'Clip batch workflows passed: {commands} commands, actual process interruption and independent MCAP checks')


if __name__ == '__main__':
    main()
