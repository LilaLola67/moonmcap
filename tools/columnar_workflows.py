"""Read exported Arrow/Parquet files back through PyArrow with exact type checks."""
import base64
import json
from pathlib import Path
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / '_interop_deps'))
from columnar import export, pa, pq


def main():
    work = ROOT / '_interop/columnar' / uuid.uuid4().hex
    work.mkdir(parents=True)
    source = work / 'rows.ndjson'
    original = [{'log_time': str((1 << 64) - 1 - i), 'data': {'count': str((1 << 53) + 1 + i),
                 'nested': {'ok': True, 'samples': [1, None, i]}, 'bytes': base64.b64encode(bytes([0, 255, i])).decode(),
                 'optional': None if i % 2 else 'present'}} for i in range(25)]
    source.write_text(''.join(json.dumps(row) + '\n' for row in original), encoding='utf-8')
    fields = [{'name': 'log_time_ns', 'path': '/log_time', 'type': 'uint64', 'nullable': False},
              {'name': 'count', 'path': '/data/count', 'type': 'uint64'},
              {'name': 'nested', 'path': '/data/nested', 'type': {'struct': [{'name': 'ok', 'type': 'bool', 'nullable': False}, {'name': 'samples', 'type': {'list': 'int32'}}]}},
              {'name': 'bytes', 'path': '/data/bytes', 'type': 'binary'},
              {'name': 'optional', 'path': '/data/optional', 'type': 'string'},
              {'name': 'absent', 'path': '/absent', 'type': 'float64'}]
    checks = 0
    for fmt in ('arrow', 'parquet'):
        output = work / f'rows.{fmt}'
        config = {'format': fmt, 'fields': fields, 'batch_rows': 4}
        report = export(source, config, output)
        assert report['rows'] == 25 and report['batches'] == 7
        table = pa.ipc.open_file(output).read_all() if fmt == 'arrow' else pq.read_table(output)
        assert table.schema.field('log_time_ns').type == pa.uint64()
        assert table.schema.metadata[b'moonmcap.input_sha256'].decode() == report['input_sha256']
        rows = table.to_pylist()
        assert rows[0]['log_time_ns'] == (1 << 64) - 1
        assert rows[-1]['count'] == (1 << 53) + 25
        assert rows[0]['bytes'] == b'\0\xff\0'
        assert rows[1]['nested'] == {'ok': True, 'samples': [1, None, 1]}
        assert rows[1]['optional'] is None and rows[0]['absent'] is None
        checks += 25
        for bad in ([{'name': 'time', 'path': '/log_time', 'type': 'timestamp_ns'}],
                    [{'name': 'x', 'path': '/data/count', 'type': 'float64'}],
                    [{'name': 'x', 'path': '/absent', 'type': 'string', 'nullable': False}],
                    [{'name': 'x', 'path': '/data/count', 'type': 'int32'}],
                    [{'name': 'x', 'path': '/data/nested/ok', 'type': 'string'}]):
            failed = work / f'failed-{fmt}'
            try:
                export(source, {**config, 'fields': bad}, failed)
            except ValueError:
                pass
            else:
                raise AssertionError('invalid type accepted')
            assert not failed.exists()
        try:
            export(source, config, output)
        except ValueError:
            pass
        else:
            raise AssertionError('existing output overwritten')
    signed = work / 'signed.ndjson'
    signed.write_text('{"t":"9223372036854775807"}\n{"t":"-9223372036854775808"}\n')
    out = work / 'signed.arrow'
    export(signed, {'format': 'arrow', 'fields': [{'name': 'stamp', 'path': '/t', 'type': 'timestamp_ns'}]}, out)
    assert pa.ipc.open_file(out).read_all()['stamp'].cast(pa.int64()).to_pylist() == [(1 << 63) - 1, -(1 << 63)]
    for malformed in ('{"x":1,"x":2}\n', '{"x":NaN}\n', '{"x":"%%%"}\n'):
        signed.write_text(malformed)
        try:
            export(signed, {'fields': [{'name': 'x', 'type': 'binary'}]}, work / 'invalid.parquet')
        except ValueError:
            pass
        else:
            raise AssertionError('malformed value accepted')
    assert not list(work.glob('*.part'))
    report = {'result': 'passed', 'roundtrip_rows': checks, 'formats': ['Arrow IPC file', 'Parquet 2.6'], 'pyarrow': pa.__version__,
              'checks': ['explicit types', 'nested structs and nullable lists', 'binary', 'uint64 extremes', 'signed timestamp limits', 'batch boundaries', 'atomic failures', 'input/rules metadata']}
    (ROOT / '_interop/columnar/report.json').write_text(json.dumps(report, indent=2) + '\n')
    print(f'Columnar workflows passed: {checks} roundtrip rows and signed timestamp limits')


if __name__ == '__main__':
    main()
