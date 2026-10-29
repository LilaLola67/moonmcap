"""Streaming workflows must not inherit the interactive query's 100k row cap."""
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / '_interop_deps'))
from mcap.writer import Writer, CompressionType
from mcap.reader import make_reader


def main():
    work = ROOT / '_interop/pull-limits' / uuid.uuid4().hex
    work.mkdir(parents=True)
    source = work / 'many.mcap'
    count = 100005
    with source.open('wb') as f:
        writer = Writer(f, compression=CompressionType.ZSTD, chunk_size=262144, enable_data_crcs=True)
        writer.start()
        ids = [writer.register_channel(t, 'json', 0) for t in ('/a', '/b')]
        for n in range(count):
            for channel in ids:
                writer.add_message(channel, n, b'{"x":1}', n, n)
        writer.finish()
    subprocess.run([os.environ.get('MOON', 'moon'), 'build', '--target', 'js', '--warn-list', '-a'], cwd=ROOT, capture_output=True, check=True)
    def run(package, command, config, output):
        path = work / 'rules.json'
        path.write_text(json.dumps(config))
        p = subprocess.run(['node', str(ROOT / f'_build/js/debug/build/cmd/{package}/{package}.js'), command, str(source), str(path), str(output)], capture_output=True, text=True, encoding='utf-8', timeout=300)
        assert p.returncode == 0, (p.stdout, p.stderr)
        return json.loads(p.stdout)
    output = work / 'clip.mcap'
    run('incident', 'clip', {'start': '0', 'end': str(count), 'stream': True}, output)
    with output.open('rb') as f:
        assert sum(1 for _ in make_reader(f, validate_crcs=True).iter_messages(log_time_order=False)) == 2 * count
    output = work / 'sync.ndjson'
    run('payload', 'sync', {'topics': [{'topic': '/a'}, {'topic': '/b'}], 'grid': {'start': str(count - 1), 'end': str(count - 1), 'period': '1'}}, output)
    rows = [json.loads(line) for line in output.read_text(encoding='utf-8').splitlines()]
    assert len(rows) == 1 and all(cell['source_time'] == str(count - 1) for cell in rows[0]['cells'][1:])
    (ROOT / '_interop/pull-limits/report.json').write_text(json.dumps({'result': 'passed', 'input_messages': 2 * count, 'clip_messages': 2 * count, 'last_sample_per_topic': count - 1, 'origin': 'synthetic independent MCAP writer'}, indent=2) + '\n')
    print(f'Pull-limit workflows passed: {2 * count} messages beyond interactive pagination')


if __name__ == '__main__':
    main()
