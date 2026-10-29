"""Reverse serialization and MCAP edits checked by independent payload runtimes."""
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / '_interop_deps'))
from google.protobuf import json_format
from mcap.reader import make_reader
from mcap.writer import Writer, CompressionType
from rosbags.typesys import get_typestore, Stores, get_types_from_msg
import numpy as np
from cdr_workflows import fixtures, plain
from protobuf_workflows import schema as proto_schema


def main():
    work = ROOT / '_interop/payload-rewrite' / uuid.uuid4().hex
    work.mkdir(parents=True)
    subprocess.run([os.environ.get('MOON', 'moon'), 'build', '--target', 'js', '--warn-list', '-a'],
                   cwd=ROOT, capture_output=True, check=True)
    cli = ROOT / '_build/js/debug/build/cmd/payload/payload.js'
    commands = 0

    def json_path(value):
        path = work / f'{uuid.uuid4().hex}.json'
        path.write_text(json.dumps(value, ensure_ascii=True, allow_nan=False), encoding='utf-8')
        return path

    def run(*args, status=0):
        nonlocal commands
        commands += 1
        p = subprocess.run(['node', str(cli), *map(str, args)], cwd=ROOT, capture_output=True,
                           text=True, encoding='utf-8', timeout=120)
        assert p.returncode == status, (args, p.stdout, p.stderr)
        return json.loads(p.stdout) if status == 0 else None

    # All ordinary scalar kinds, nested/repeated/map/oneof and full-range uint64.
    descriptor, cls, sample = proto_schema()
    schema = work / 'protobuf.schema'
    schema.write_bytes(descriptor)
    value = json_format.MessageToDict(sample)
    encoded = work / 'proto.bin'
    run('encode', 'protobuf', schema, 'telemetry.Sample', json_path(value), encoded)
    assert cls.FromString(encoded.read_bytes()) == sample
    unknown = b'\xa0\x06\x2a\xab\x06\x08\x01\xac\x06'
    nested_unknown = b'\x98\x06\x2b'
    payload = sample.SerializeToString() + b'\x8a\x01\x03' + nested_unknown + unknown
    raw = work / 'proto-input.bin'
    raw.write_bytes(payload)
    rules = {'edits': [{'path': '/child/b', 'value': 'redacted'},
                       {'path': '/text', 'value': 'new-choice'},
                       {'path': '/f4', 'value': '9007199254740993'}]}
    changed = work / 'proto-rewritten.bin'
    run('raw-rewrite', 'protobuf', schema, 'telemetry.Sample', raw, json_path(rules), changed)
    result = cls.FromString(changed.read_bytes())
    assert result.child.b == 'redacted' and result.child.a == 7
    assert result.f4 == 9007199254740993 and result.WhichOneof('choice') == 'text'
    assert unknown in changed.read_bytes() and nested_unknown in changed.read_bytes()
    # CDR both endiannesses, full scalar/array/nested matrix, and the empty sentinel.
    store, message, cases = fixtures()
    for ordinal, (name, definition, wire, wanted) in enumerate(cases):
        schema_path = work / f'cdr-{ordinal}.msg'
        schema_path.write_bytes(definition)
        output = work / f'cdr-{ordinal}.bin'
        endian = ['--big-endian'] if wire[1] == 0 else []
        run('encode', 'cdr', schema_path, name, json_path(wanted), output, *endian)
        assert plain(store.deserialize_cdr(output.read_bytes(), name)) == wanted
        assert output.read_bytes()[1] == wire[1]
    # ROS1 packing, aliases, Header, timestamp, signed extrema and arrays.
    ros = get_typestore(Stores.ROS1_NOETIC)
    name = 'demo/msg/Sample'
    definition = 'byte small\nchar octet\nuint64 counter\nint64 negative\nHeader header\nuint8[] data\nfloat64[] values\n'
    ros.register(get_types_from_msg(definition, name))
    t = ros.types
    ros_message = t[name](-128, 255, 18446744073709551615, -9223372036854775808,
                          t['std_msgs/msg/Header'](7, t['builtin_interfaces/msg/Time'](3, 5), 'frame'),
                          np.array([0, 255, 8], dtype=np.uint8), np.array([1.5, -2.5], dtype=np.float64))
    ros_schema = ros.generate_msgdef(name, ros_version=1)[0].encode()
    ros_path = work / 'ros1.msg'
    ros_path.write_bytes(ros_schema)
    ros_output = work / 'ros1.bin'
    run('encode', 'ros1', ros_path, name, json_path(plain(ros_message)), ros_output)
    assert ros_output.read_bytes() == bytes(ros.serialize_ros1(ros_message, name))
    # End-to-end file edits for all four encodings and all three input codecs.
    cdr_name, cdr_schema, cdr_wire, _ = cases[1]
    cases_file = [
        ('protobuf', 'protobuf', 'telemetry.Sample', descriptor, payload,
         [{'path': '/f9', 'value': 'redacted'}],
         lambda data: cls.FromString(data).f9 == 'redacted' and unknown in data and nested_unknown in data),
        ('cdr', 'ros2msg', cdr_name, cdr_schema, cdr_wire,
         [{'path': '/header/frame_id', 'value': 'world'}],
         lambda data: store.deserialize_cdr(data, cdr_name).header.frame_id == 'world' and data[1] == cdr_wire[1]),
        ('ros1', 'ros1msg', name, ros_schema, ros_output.read_bytes(),
         [{'path': '/header/frame_id', 'value': 'world'}],
         lambda data: ros.deserialize_ros1(data, name).header.frame_id == 'world'),
        ('json', None, None, None, b'{"private":"secret","n":9007199254740993}',
         [{'path': '/private', 'remove': True}],
         lambda data: json.loads(data) == {'n': 9007199254740993}),
    ]
    checked = 0
    for fmt, schema_encoding, typename, definition, wire, edits, verify in cases_file:
        for codec in CompressionType:
            source = work / f'{fmt}-{codec.name}.mcap'
            stream = io.BytesIO()
            writer = Writer(stream, compression=codec, chunk_size=512, enable_data_crcs=True)
            writer.start(profile='rewrite-test')
            sid = writer.register_schema(typename, schema_encoding, definition) if schema_encoding else 0
            selected = writer.register_channel('/selected', fmt, sid)
            untouched = writer.register_channel('/untouched', 'raw', 0)
            for i in range(12):
                writer.add_message(selected, i, wire, i + 2, i)
                writer.add_message(untouched, i, b'unchanged', i + 1, i)
            writer.add_attachment(0, 0, 'calibration', 'application/octet-stream', b'keep-attachment')
            writer.add_metadata('source', {'kind': 'synthetic'})
            writer.finish()
            source.write_bytes(stream.getvalue())
            before = hashlib.sha256(source.read_bytes()).hexdigest()
            output = work / f'{fmt}-{codec.name}-out.mcap'
            config = {'edits': edits, 'topics': ['/selected']}
            report = run('rewrite', source, json_path(config), output)
            assert report['report']['rewritten'] == '12' and report['report']['messages'] == '24'
            with output.open('rb') as f:
                reader = make_reader(f, validate_crcs=True)
                assert reader.get_header().profile == 'rewrite-test'
                rows = list(reader.iter_messages(log_time_order=False))
            assert len(rows) == 24
            for i in range(12):
                s, c, m = rows[i * 2]
                assert c.topic == '/selected' and c.message_encoding == fmt
                assert m.sequence == i and m.log_time == i and m.publish_time == i + 2
                assert verify(m.data), fmt
                assert rows[i * 2 + 1][2].data == b'unchanged'
                if definition:
                    assert s.data == definition and s.encoding == schema_encoding
                checked += 1
            with output.open('rb') as f:
                assert [a.data for a in make_reader(f).iter_attachments()] == [b'keep-attachment']
            failed = work / f'{fmt}-{codec.name}-failed.mcap'
            run('rewrite', source, json_path({'edits': [{'path': '/missing/child', 'value': 1}], 'topics': ['/selected']}), failed, status=2)
            assert not failed.exists()
            run('rewrite', source, json_path(config), output, status=2)
            assert hashlib.sha256(source.read_bytes()).hexdigest() == before
    # A late container CRC error must not publish rewritten output.
    data = bytearray(source.read_bytes())
    pos = 8
    while pos + 9 <= len(data) - 8:
        length = struct.unpack_from('<Q', data, pos + 1)[0]
        if data[pos] == 0x0f:
            data[pos + 9] ^= 1
            break
        pos += 9 + length
    else:
        raise AssertionError('missing DataEnd')
    bad = work / 'bad-crc.mcap'
    bad.write_bytes(data)
    failed = work / 'bad-crc-out.mcap'
    run('rewrite', bad, json_path(config), failed, status=2)
    assert not failed.exists()
    assert not list(work.glob('*.part'))
    report = {'result': 'passed', 'commands': commands, 'message_oracles': checked,
              'formats': ['JSON', 'ordinary Protobuf objects', 'ROS1', 'plain XCDR1 big/little endian'],
              'reference': ['Google protobuf', 'rosbags', 'Python MCAP CRC reader'],
              'checks': ['typed reverse serialization', 'uint64 full range', 'nested unknown Protobuf retention',
                         'oneof replacement', 'message envelope and declarations retained',
                         'auxiliary retention', 'source unchanged', 'atomic errors including late CRC'],
              'data_origin': 'synthetic independent reference runtimes'}
    (ROOT / '_interop/payload-rewrite/report.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print(f'Payload rewrite workflows passed: {commands} commands, {checked} independent payload checks and atomic failures')


if __name__ == '__main__':
    main()
