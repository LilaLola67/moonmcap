"""Independent JSON/CDR TF recordings, historical lookup and self-contained clips."""
import io
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / '_interop_deps'))
from mcap.writer import Writer, CompressionType
from mcap.reader import make_reader
from rosbags.typesys import get_typestore, Stores


def main():
    work = ROOT / '_interop/tf' / uuid.uuid4().hex
    work.mkdir(parents=True)
    subprocess.run([os.environ.get('MOON', 'moon'), 'build', '--target', 'js', '--warn-list', '-a'], cwd=ROOT, capture_output=True, check=True)
    cli = ROOT / '_build/js/debug/build/cmd/payload/payload.js'
    commands = 0
    store = get_typestore(Stores.ROS2_HUMBLE)
    types = store.types
    typename = 'tf2_msgs/msg/TFMessage'

    def payload(parent, child, stamp, x, static=False):
        msg = {'header': {'frame_id': parent, 'stamp': {'sec': 0, 'nanosec': stamp}}, 'child_frame_id': child,
               'transform': {'translation': {'x': x, 'y': 0., 'z': 0.}, 'rotation': {'x': 0., 'y': 0., 'z': 0., 'w': 1.}}}
        return {'transforms': [msg]}

    def cdr(value):
        transforms = []
        for item in value['transforms']:
            stamp = item['header']['stamp']
            transforms.append(types['geometry_msgs/msg/TransformStamped'](
                types['std_msgs/msg/Header'](types['builtin_interfaces/msg/Time'](stamp['sec'], stamp['nanosec']), item['header']['frame_id']), item['child_frame_id'],
                types['geometry_msgs/msg/Transform'](types['geometry_msgs/msg/Vector3'](**item['transform']['translation']), types['geometry_msgs/msg/Quaternion'](**item['transform']['rotation']))))
        return bytes(store.serialize_cdr(types[typename](transforms), typename))

    def run(command, source, config, suffix, status=0):
        nonlocal commands
        commands += 1
        path = work / 'rules.json'
        path.write_text(json.dumps(config))
        output = work / f'output-{commands}.{suffix}'
        p = subprocess.run(['node', str(cli), command, str(source), str(path), str(output)], cwd=ROOT, capture_output=True, text=True, encoding='utf-8', timeout=120)
        assert p.returncode == status, (p.stdout, p.stderr)
        if status:
            assert not output.exists()
        return output

    for fmt in ('json', 'cdr'):
        for codec in CompressionType:
            source = work / f'{fmt}-{codec.name}.mcap'
            with source.open('wb') as f:
                writer = Writer(f, compression=codec, chunk_size=256, enable_data_crcs=True)
                writer.start()
                sid = writer.register_schema(typename, 'ros2msg', store.generate_msgdef(typename, ros_version=2)[0].encode()) if fmt == 'cdr' else 0
                ids = [writer.register_channel(t, fmt, sid) for t in ('/tf_static', '/tf')]
                sensor = writer.register_channel('/sensor', 'json', 0)
                for seq, (time, channel, value) in enumerate([(1, ids[0], payload('base', 'sensor', 0, 2.)), (2, ids[0], payload('base', 'sensor', 0, 2.)),
                        (1010, ids[1], payload('world', 'base', 10, 0.)), (1020, ids[1], payload('world', 'base', 20, 10.)), (1030, ids[1], payload('world', 'base', 30, 20.))]):
                    writer.add_message(channel, time, cdr(value) if fmt == 'cdr' else json.dumps(value).encode(), time, seq)
                writer.add_message(sensor, 1015, b'{"x":1}', 1015, 100)
                writer.add_metadata('calibration', {'value': 'yes'})
                writer.finish()
            queries = {'queries': [{'target': 'world', 'source': 'sensor', 'time': str(t)} for t in (15, 25)] + [{'target': 'base', 'source': 'sensor', 'time': '0'}]}
            original = json.loads(run('tf-query', source, queries, 'json').read_text(encoding='utf-8'))
            assert [q['transform']['translation']['x'] for q in original['queries']] == [7, 17, 2]
            rules = {'frames': ['sensor'], 'start': '1015', 'end': '1025', 'tf_start': '15', 'tf_end': '25', 'topics': ['/sensor'], 'copy_auxiliary': True}
            clip = run('tf-clip', source, rules, 'mcap')
            with clip.open('rb') as f:
                reader = make_reader(f, validate_crcs=True)
                records = list(reader.iter_messages())
            assert len(records) == 5
            assert [m.log_time for _, c, m in records if c.topic == '/tf'] == [1010, 1020, 1030]
            assert sum(c.topic == '/tf_static' for _, c, _ in records) == 1
            result = json.loads(run('tf-query', clip, queries, 'json').read_text(encoding='utf-8'))
            assert result['queries'] == original['queries']
            run('tf-query', source, {'queries': [{'target': 'world', 'source': 'sensor', 'time': '0'}]}, 'json', status=2)
            run('tf-query', source, {'queries': [{'target': 'world', 'source': 'sensor', 'time': '15', 'max_gap': '4'}]}, 'json', status=2)
            run('tf-clip', source, {**rules, 'frames': ['missing']}, 'mcap', status=2)
    assert not list(work.glob('*.part'))
    report = {'result': 'passed', 'commands': commands, 'origin': 'synthetic independent JSON/CDR MCAP writers',
              'checks': ['three compression modes', 'nonidentical TF and log clocks', 'historical chain/inverse', 'static replay deduplication', 'semantic context brackets', 'self-contained clip lookup equivalence', 'CRC readback', 'atomic invalid requests']}
    (ROOT / '_interop/tf/report.json').write_text(json.dumps(report, indent=2) + '\n')
    print(f'TF workflows passed: {commands} commands')


if __name__ == '__main__':
    main()
