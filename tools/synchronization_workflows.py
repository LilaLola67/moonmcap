"""Independent MCAP fixtures for N-topic synchronization and resampling."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / '_interop_deps'))
from mcap.writer import Writer, CompressionType


def main():
    work = ROOT / '_interop/synchronization' / uuid.uuid4().hex
    work.mkdir(parents=True)
    subprocess.run([os.environ.get('MOON', 'moon'), 'build', '--target', 'js', '--warn-list', '-a'], cwd=ROOT, capture_output=True, check=True)
    cli = ROOT / '_build/js/debug/build/cmd/payload/payload.js'
    commands, rows_checked = 0, 0

    def run(source, config, status=0):
        nonlocal commands, rows_checked
        commands += 1
        rules = work / 'rules.json'
        rules.write_text(json.dumps(config))
        out = work / f'rows-{commands}.ndjson'
        p = subprocess.run(['node', str(cli), 'sync', str(source), str(rules), str(out)], cwd=ROOT, capture_output=True, text=True, encoding='utf-8', timeout=120)
        assert p.returncode == status, (config, p.stdout, p.stderr)
        if status:
            assert not out.exists()
            return []
        report = json.loads(p.stdout)
        assert report['source_sha256'] == hashlib.sha256(source.read_bytes()).hexdigest()
        rows = [json.loads(line) for line in out.read_text(encoding='utf-8').splitlines()]
        rows_checked += len(rows)
        return rows

    for codec in CompressionType:
        source = work / f'{codec.name}.mcap'
        with source.open('wb') as f:
            writer = Writer(f, compression=codec, chunk_size=128, enable_data_crcs=True)
            writer.start()
            channels = [writer.register_channel(topic, 'json', 0) for topic in ('/a', '/b', '/c')]
            records = [(t, lane, n) for lane, ts in enumerate(([10, 20, 30], [9, 21, 31], [10, 30])) for n, t in enumerate(ts)]
            for t, lane, n in sorted(records):
                writer.add_message(channels[lane], t, json.dumps({'x': t, 'stamp': {'sec': 0, 'ns': t + 100}}).encode(), t + 100, n)
            writer.finish()
        before = hashlib.sha256(source.read_bytes()).hexdigest()
        base = {'topics': [{'topic': t} for t in ('/a', '/b', '/c')], 'tolerance': '2'}
        rows = run(source, base)
        assert [r['time'] for r in rows] == ['10', '20', '30']
        assert [[c['source_time'] for c in r['cells']] for r in rows] == [['10', '9', '10'], ['20', '21', None], ['30', '31', '30']]
        assert len(run(source, {**base, 'keep_missing': False})) == 2
        for mode, expected in [('previous', ['9', None, None]), ('next', [None, '21', '31'])]:
            selected = run(source, {**base, 'mode': mode})
            assert [r['cells'][1]['source_time'] for r in selected] == expected
        for clock in ('publish', {'seconds': '/stamp/sec', 'nanos': '/stamp/ns'}):
            selected = run(source, {**base, 'topics': [{'topic': t, 'clock': clock} for t in ('/a', '/b', '/c')]})
            assert [r['time'] for r in selected] == ['110', '120', '130']
        grid = {'topics': [{'topic': '/a', 'paths': ['/x']}, {'topic': '/c', 'paths': ['/x']}], 'mode': 'linear', 'tolerance': '20', 'grid': {'start': '5', 'end': '35', 'period': '5'}}
        selected = run(source, grid)
        assert [r['cells'][0]['kind'] for r in selected] == ['grid'] * 7
        assert selected[0]['cells'][1]['kind'] == selected[-1]['cells'][1]['kind'] == 'missing'
        assert selected[2]['cells'][2]['interpolation']['value'] == {'x': 15}
        assert selected[2]['cells'][2]['before_time'] == '10'
        assert selected[2]['cells'][2]['after_time'] == '30'
        run(source, {**grid, 'reuse': False}, status=2)
        run(source, {**base, 'buffer_bytes': 31}, status=2)
        run(source, {**base, 'input_messages': '1'}, status=2)
        run(source, {**base, 'topics': [{'topic': '/absent'}, {'topic': '/a'}]}, status=2)
        assert hashlib.sha256(source.read_bytes()).hexdigest() == before
    report = {'result': 'passed', 'commands': commands, 'rows_checked': rows_checked,
              'origin': 'synthetic independently written MCAP, three compression modes',
              'checks': ['three topics', 'nearest/previous/next', 'missing rows', 'publish/header clocks',
                         'uniform interpolation and bracket provenance', 'no extrapolation', 'bounded failure', 'atomic output']}
    (ROOT / '_interop/synchronization/report.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print(f'Synchronization workflows passed: {commands} commands, {rows_checked} rows')


if __name__ == '__main__':
    main()
