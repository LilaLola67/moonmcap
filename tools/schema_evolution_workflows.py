"""Directional schema cases and equivalent query views from different encodings."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / '_interop_deps'))
from google.protobuf import descriptor_pb2 as pb, descriptor_pool, message_factory
from mcap.writer import Writer, CompressionType
from rosbags.typesys import get_typestore, Stores, get_types_from_msg


def descriptor(fields, syntax='proto3'):
    file = pb.FileDescriptorProto(name='schema.proto', package='demo', syntax=syntax)
    message = file.message_type.add(name='Value')
    for name, number, kind, label in fields:
        message.field.add(name=name, number=number, type=kind, label=label)
    pool = descriptor_pool.DescriptorPool()
    pool.Add(file)
    return pb.FileDescriptorSet(file=[file]).SerializeToString(), message_factory.GetMessageClass(pool.FindMessageTypeByName('demo.Value'))


def main():
    work = ROOT / '_interop/schema-evolution' / uuid.uuid4().hex
    work.mkdir(parents=True)
    subprocess.run([os.environ.get('MOON', 'moon'), 'build', '--target', 'js', '--warn-list', '-a'], cwd=ROOT, capture_output=True, check=True)
    cli = ROOT / '_build/js/debug/build/cmd/payload/payload.js'
    commands = 0

    def run(*args, status=0):
        nonlocal commands
        commands += 1
        p = subprocess.run(['node', str(cli), *map(str, args)], cwd=ROOT, capture_output=True, text=True, encoding='utf-8', timeout=120)
        assert p.returncode == status, (args, p.stdout, p.stderr)
        return json.loads(p.stdout) if status == 0 else None

    def compare(fmt, old, new, expected, old_name='demo.Value', new_name='demo.Value'):
        a, b = work / 'old.schema', work / 'new.schema'
        a.write_bytes(old)
        b.write_bytes(new)
        report = run('compare', fmt, a, old_name, b, new_name)
        assert tuple(report[key] for key in ('old_reader_new_data', 'new_reader_old_data', 'old_query_new_data')) == expected, report

    old, old_cls = descriptor([('x', 1, 5, 1)])
    new, new_cls = descriptor([('x', 1, 5, 1), ('label', 2, 9, 1)])
    compare('protobuf', old, new, (True, True, True))
    assert old_cls.FromString(new_cls(x=7, label='new').SerializeToString()).x == 7
    assert new_cls.FromString(old_cls(x=7).SerializeToString()).x == 7
    renamed, renamed_cls = descriptor([('value', 1, 5, 1)])
    compare('protobuf', old, renamed, (True, True, False))
    assert old_cls.FromString(renamed_cls(value=7).SerializeToString()).x == 7
    old2, old2_cls = descriptor([('x', 1, 5, 1)], syntax='proto2')
    required, required_cls = descriptor([('x', 1, 5, 1), ('label', 2, 9, 2)], syntax='proto2')
    compare('protobuf', old2, required, (True, False, True))
    assert not required_cls.FromString(old2_cls(x=7).SerializeToString()).IsInitialized()
    assert old2_cls.FromString(required_cls(x=7, label='required').SerializeToString()).IsInitialized()
    compare('protobuf', required, old2, (False, True, False))
    changed, _ = descriptor([('x', 1, 13, 1)])
    compare('protobuf', old, changed, (False, False, False))
    compare('cdr', b'uint32 x\nstring<=4 text', b'uint32 value\nstring<=4 text', (True, True, False), 'demo/Value', 'demo/Value')
    compare('cdr', b'uint32 x\nstring<=4 text', b'uint32 x\nstring<=8 text', (False, True, True), 'demo/Value', 'demo/Value')
    compare('cdr', b'uint32 x', b'uint32 x\nuint32 y', (False, False, False), 'demo/Value', 'demo/Value')
    compare('ros1', b'byte x', b'int8 x', (True, True, True), 'demo/Value', 'demo/Value')
    store = get_typestore(Stores.ROS2_HUMBLE)
    typename = 'demo/msg/Value'
    store.register(get_types_from_msg('float64 speed\nuint64 counter\nstring frame_id', typename))
    cls = store.types[typename]
    records = 0
    for fmt in ('json', 'cdr'):
        for codec in CompressionType:
            source = work / f'{fmt}-{codec.name}.mcap'
            with source.open('wb') as f:
                writer = Writer(f, compression=codec, chunk_size=256, enable_data_crcs=True)
                writer.start()
                sid = writer.register_schema(typename, 'ros2msg', store.generate_msgdef(typename, ros_version=2)[0].encode()) if fmt == 'cdr' else 0
                cid = writer.register_channel('/motion', fmt, sid)
                for i in range(12):
                    payload = bytes(store.serialize_cdr(cls(float(i), 9007199254740993 + i, 'base_link'), typename)) if fmt == 'cdr' else json.dumps({'speed_mm_s': i * 1000, 'counter': str(9007199254740993 + i)}).encode()
                    writer.add_message(cid, i, payload, i + 1, i)
                writer.finish()
            before = hashlib.sha256(source.read_bytes()).hexdigest()
            fields = [
                {'source': '/speed' if fmt == 'cdr' else '/speed_mm_s', 'target': '/motion/speed_m_s', 'conversion': 'scale', 'scale': 1 if fmt == 'cdr' else 0.001},
                {'source': '/counter', 'target': '/counter', 'conversion': 'unsigned', 'bits': 64},
                {'source': '/frame_id', 'target': '/frame_id', 'conversion': 'text', 'default': 'base_link'},
                {'source': '/temperature', 'target': '/temperature', 'required': False},
            ]
            config = {'fields': fields, 'topics': ['/motion'], 'where': {'path': '/motion/speed_m_s', 'op': '>=', 'value': 2}, 'limit': 3}
            path = work / 'config.json'
            path.write_text(json.dumps(config))
            output = work / f'{fmt}-{codec.name}.ndjson'
            assert run('normalize', source, path, output)['rows'] == '3'
            rows = [json.loads(line) for line in output.read_text(encoding='utf-8').splitlines()]
            assert [int(row['sequence']) for row in rows] == [2, 3, 4]
            for i, row in zip(range(2, 5), rows):
                assert row['data'] == {'motion': {'speed_m_s': float(i)}, 'counter': 9007199254740993 + i, 'frame_id': 'base_link'}
                assert row['missing'] == ['/temperature']
                assert row['defaulted'] == ([] if fmt == 'cdr' else ['/frame_id'])
                records += 1
            failed = work / f'{fmt}-{codec.name}-failed.ndjson'
            for bad_fields in ([{'source': '/absent', 'target': '/required'}],
                               [{'source': '/counter', 'target': '/counter', 'conversion': 'scale'}],
                               [{'source': '/counter', 'target': '/same'}, {'source': '/counter', 'target': '/same/child'}]):
                path.write_text(json.dumps({'fields': bad_fields}))
                run('normalize', source, path, failed, status=2)
                assert not failed.exists()
            assert hashlib.sha256(source.read_bytes()).hexdigest() == before
    assert not list(work.glob('*.part'))
    report = {'result': 'passed', 'commands': commands, 'normalized_rows': records,
              'checks': ['directional Protobuf required fields checked with official runtime',
                         'wire-compatible rename separated from query names', 'ordered ROS layout and bound changes',
                         'equivalent JSON/CDR views across three codecs', 'exact uint64 values',
                         'unit conversions', 'default/missing provenance', 'filter before limit', 'atomic failure'],
              'origin': 'synthetic independent writers and explicit normalization rules',
              'scope': 'conservative supported-schema comparison, not protoc or application semantic inference'}
    (ROOT / '_interop/schema-evolution/report.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print(f'Schema evolution workflows passed: {commands} commands and {records} equivalent normalized rows')


if __name__ == '__main__':
    main()
