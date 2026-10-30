"""User-visible failure contracts: ignored options, byte budgets, late errors and CRC scope."""
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / '_interop_deps'))
from mcap.writer import Writer, CompressionType


def main():
    work = ROOT / '_interop/reliability' / uuid.uuid4().hex
    work.mkdir(parents=True)
    subprocess.run([os.environ.get('MOON', 'moon'), 'build', '--target', 'js', '--warn-list', '-a'], cwd=ROOT, capture_output=True, check=True)
    cli = ROOT / '_build/js/debug/build/cmd/mcap/mcap.js'
    count = 0
    def run(*args, code=0):
        nonlocal count
        count += 1
        p = subprocess.run(['node', str(cli), *map(str, args)], cwd=work, capture_output=True, text=True, encoding='utf-8', timeout=90)
        assert p.returncode == code, (args, p.returncode, p.stdout, p.stderr)
        return p
    # Invalid arguments must fail before opening inputs or creating outputs/jobs.
    invalid = [
        ['slice', 'missing', '--output', 'unused.mcap', '--format', 'json'],
        ['index-build', 'missing', '--output', 'unused.idx', '--format', 'json'],
        ['diff', 'missing-a', 'missing-b', '--compression', 'zstd'],
        ['job-clean', 'missing-job', '--output', 'unused.json'],
        ['index-query', 'missing', '--index', 'missing-index', '--max-file-bytes', '1'],
        ['recover-blocks', 'missing', '--output', 'unused.mcap', '--max-file-bytes', '1'],
        ['job-inspect', 'missing', '--max-file-bytes', '1'],
        ['dataset-query', 'missing', '--index-bytes', '1'],
    ]
    for args, keyword in zip(invalid, ['format', 'format', 'compression', 'job-clean', 'max-file-bytes', 'max-file-bytes', 'max-file-bytes', 'index-bytes']):
        p = run(*args, code=2)
        assert keyword in json.loads(p.stderr)['error'] and not p.stdout, (args, p.stderr)
    assert not list(work.iterdir())
    source = work / '--日志 with space.mcap'
    shutil.copyfile(ROOT / 'examples/files/zstd-summary.mcap', source)
    size = source.stat().st_size
    assert json.loads(run('inspect', '--max-file-bytes', size, '--', source.name).stdout)['messages'] == 100
    p = run('inspect', source, '--max-file-bytes', size - 1, '--output', 'budget.json', code=2)
    assert json.loads(p.stderr)['error'] and not (work / 'budget.json').exists()
    cases = ['inapplicable options fail before IO', 'exact byte budget boundary', 'Unicode, spaces and option-like input path']
    for codec in CompressionType:
        path = work / (codec.name + '-late-json.mcap')
        with path.open('wb') as f:
            writer = Writer(f, compression=codec, chunk_size=1, enable_data_crcs=True)
            writer.start()
            channel = writer.register_channel('/n', 'json', 0)
            writer.add_message(channel, 0, b'{"n":1}', 0, 0)
            writer.add_message(channel, 1, b'{broken', 1, 1)
            writer.finish()
        out = work / (codec.name + '-rows.ndjson')
        p = run('query', path, '--where', 'true', '--format', 'ndjson', '--output', out, code=2)
        assert json.loads(p.stderr)['error'] and not out.exists()
        # stdout cannot roll back; one earlier row may already have been emitted.
        p = run('query', path, '--where', 'true', '--format', 'ndjson', code=2)
        assert len(p.stdout.splitlines()) == 1 and json.loads(p.stdout)['decoded']['data']['n'] == 1
        assert json.loads(p.stderr)['error']
    cases.append('late malformed JSON: atomic file failure versus partial stdout, three codecs')
    valid = work / 'valid.mcap'
    with valid.open('wb') as f:
        writer = Writer(f, compression=CompressionType.ZSTD, enable_data_crcs=True)
        writer.start()
        channel = writer.register_channel('/n', 'json', 0)
        writer.add_message(channel, 0, b'{"n":0}', 0, 0)
        writer.add_message(channel, (1 << 64) - 1, b'{"n":1}', (1 << 64) - 1, (1 << 32) - 1)
        writer.finish()
    p = run('query', valid, '--start', (1 << 64) - 1, '--end', (1 << 64) - 1)
    row = json.loads(p.stdout)['messages'][0]
    assert row['log_time'] == str((1 << 64) - 1) and row['sequence'] == (1 << 32) - 1
    data = bytearray(valid.read_bytes())
    offset = 8
    while offset < len(data) - 8:
        opcode, length = data[offset], struct.unpack_from('<Q', data, offset + 1)[0]
        if opcode == 0x0f:
            assert length == 4
            data[offset + 9] ^= 1
            break
        offset += 9 + length
    else:
        raise AssertionError('DataEnd not found')
    damaged = work / 'bad-data-crc.mcap'
    damaged.write_bytes(data)
    fast = json.loads(run('query', damaged).stdout)
    assert not fast['diagnostics']['full_validation'] and len(fast['messages']) == 2
    p = run('query', damaged, '--full', '--output', 'bad-full.json', code=2)
    assert json.loads(p.stderr)['error'] and not (work / 'bad-full.json').exists()
    p = run('validate', damaged, code=1)
    assert p.stdout and not p.stderr
    assert valid.read_bytes() != damaged.read_bytes()
    cases += ['independent UInt64/UInt32 boundary recording', 'fast query is not full CRC validation']
    sentinel = work / 'existing.mcap'
    sentinel.write_bytes(b'preserve existing destination')
    before = sentinel.read_bytes()
    run('slice', valid, '--output', sentinel, code=2)
    assert sentinel.read_bytes() == before
    assert not [p for p in work.iterdir() if p.name.startswith('.') or p.suffix in ('.part', '.tmp')]
    cases.append('existing destination preserved and temporary files cleaned')
    report = {'result': 'passed', 'commands': count, 'cases': cases, 'data': 'synthetic with independent Python MCAP writer'}
    (ROOT / '_interop/reliability/report.json').write_text(json.dumps(report, indent=2) + '\n')
    print(f'Reliability contracts passed: {count} commands, {len(cases)} groups')


if __name__ == '__main__':
    main()
