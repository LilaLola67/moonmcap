"""Append physical MCAP records and resume atomic tail/query/quality outputs."""
import hashlib
import io
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import uuid
import zlib

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / '_interop_deps'))
from mcap.writer import Writer, CompressionType


def main():
    work = ROOT / '_interop/live' / uuid.uuid4().hex
    work.mkdir(parents=True)
    subprocess.run([os.environ.get('MOON', 'moon'), 'build', '--target', 'js', '--warn-list', '-a'], cwd=ROOT, capture_output=True, check=True)
    cli = ROOT / '_build/js/debug/build/cmd/payload/payload.js'
    commands = 0
    def run(source, config, status=0):
        nonlocal commands
        commands += 1
        path = work / 'rules.json'
        path.write_text(json.dumps(config))
        output = work / f'poll-{commands}.json'
        p = subprocess.run(['node', str(cli), 'tail', str(source), str(path), str(output)], cwd=ROOT, capture_output=True, text=True, encoding='utf-8', timeout=120)
        assert p.returncode == status, (p.stdout, p.stderr)
        if status:
            assert not output.exists()
            return None, output
        return json.loads(output.read_text(encoding='utf-8')), output

    for codec in CompressionType:
        data = io.BytesIO()
        writer = Writer(data, compression=codec, chunk_size=180, enable_data_crcs=True)
        writer.start()
        channel = writer.register_channel('/n', 'json', 0)
        binary = writer.register_channel('/camera', 'raw', 0)
        for n in range(16):
            writer.add_message(channel, n, json.dumps({'n': n}).encode(), n, n)
        writer.add_message(binary, 20, b'\xff\0\x80', 20, 0)
        writer.finish()
        full = data.getvalue()
        records = []
        position = 8
        while position < len(full) - 8:
            opcode, size = struct.unpack_from('<BQ', full, position)
            records.append((position, opcode, position + 9 + size))
            position += 9 + size
        chunk = next(record for record in records if record[1] == 6)
        source = work / f'growing-{codec.name}.mcap'
        source.write_bytes(full[:chunk[2] - 1])
        base = {'identity': 'synthetic-live-' + codec.name, 'topics': ['/n'], 'decode': True,
                'where': {'path': '/n', 'op': '>=', 'value': 4},
                'quality': {'rules': [{'topic': '/n', 'max_gap': '1', 'check_sequence': True, 'condition': {'path': '/n', 'op': '>=', 'value': 0}}], 'start': '0', 'lateness': '2'}}
        a, saved = run(source, base)
        assert a['rows'] == [] and a['evidence']['verification'] == 'partial-prefix'
        source.write_bytes(full[:chunk[2]])
        b, saved = run(source, {**base, 'snapshot': str(saved)})
        seen = [int(row['log_time']) for row in b['rows']]
        assert b['evidence']['verification'] == 'partial-prefix'
        before = source.read_bytes()
        source.write_bytes(before[:9] + bytes([before[9] ^ 1]) + before[10:])
        run(source, {**base, 'snapshot': str(saved)}, status=2)
        source.write_bytes(full)
        c, saved = run(source, {**base, 'snapshot': str(saved), 'quality': {**base['quality'], 'finish': '15'}})
        seen.extend(int(row['log_time']) for row in c['rows'])
        assert seen == list(range(4, 16)), seen
        assert c['evidence']['verification'] == 'complete-container'
        assert c['quality']['rules'][0]['status'] == 'passed' and c['quality']['rules'][0]['messages'] == '16'
        d, saved = run(source, {**base, 'snapshot': str(saved)})
        assert not d['rows'] and d['quality']['finished']
        run(source, {**base, 'snapshot': str(saved), 'where': True}, status=2)
        run(source, {**base, 'snapshot': str(saved), 'identity': 'other'}, status=2)
        source.write_bytes(full[:-1])
        run(source, {**base, 'snapshot': str(saved)}, status=2)

    # Produce a real MRSC1 manifest using independently generated complete segments.
    directory = work / 'recording'
    directory.mkdir()
    segments = []
    for ordinal in range(2):
        segment = directory / f'segment-{ordinal}.mcap'
        with segment.open('wb') as f:
            writer = Writer(f, compression=CompressionType.ZSTD, enable_data_crcs=True)
            writer.start()
            channel = writer.register_channel('/n', 'json', 0)
            writer.add_message(channel, ordinal, b'{}', ordinal, ordinal)
            writer.finish()
        segments.append({'ordinal': ordinal, 'bytes': str(segment.stat().st_size), 'sha256': hashlib.sha256(segment.read_bytes()).hexdigest(), 'messages': '1'})
    def manifest(rows):
        body = json.dumps({'version': 1, 'config': {'fixture': 'independent'}, 'segments': rows}).encode()
        (directory / 'manifest.bin').write_bytes(b'MRSC1\r\n\0' + struct.pack('<I', len(body)) + body + struct.pack('<I', zlib.crc32(body)))
    manifest(segments[:1])
    config = {'identity': 'recording', 'mode': 'recording'}
    a, saved = run(directory, config)
    assert len(a['rows']) == 1
    b, saved = run(directory, {**config, 'snapshot': str(saved)})
    assert b['rows'] == []
    manifest(segments)
    c, saved = run(directory, {**config, 'snapshot': str(saved)})
    assert [row['log_time'] for row in c['rows']] == ['1']
    manifest(segments[:1])
    run(directory, {**config, 'snapshot': str(saved)}, status=2)
    report = {'result': 'passed', 'commands': commands, 'origin': 'synthetic independent MCAP writer and manifest envelope',
              'checks': ['incomplete chunks wait', 'CRC distinctions', 'SHA256 prefix mutation', 'atomic query/quality/snapshot bundle', 'unrelated raw topic', 'filter before output', 'no duplicate resume', 'rule binding', 'committed segments only', 'manifest rewind refused']}
    (ROOT / '_interop/live/report.json').write_text(json.dumps(report, indent=2) + '\n')
    print(f'Live workflows passed: {commands} commands')


if __name__ == '__main__':
    main()
