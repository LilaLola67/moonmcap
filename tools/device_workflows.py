"""Real current SDK WebSocket subscription and independent MCAP recording readback."""
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / '_interop_deps'))
from foxglove_bridge import Bridge
from websockets.sync.client import connect
from mcap.reader import make_reader
from google.protobuf import descriptor_pb2


def main():
    work = ROOT / '_interop/device' / uuid.uuid4().hex
    work.mkdir(parents=True)
    subprocess.run([os.environ.get('MOON', 'moon'), 'build', '--target', 'js', '--warn-list', '-a'], cwd=ROOT, capture_output=True, check=True)
    rules = work / 'rules.json'
    file = descriptor_pb2.FileDescriptorProto(name='value.proto', package='test', syntax='proto3')
    message = file.message_type.add(name='Value')
    message.field.add(name='n', number=1, type=13, label=1)
    descriptor = descriptor_pb2.FileDescriptorSet(file=[file]).SerializeToString()
    config = {'identity': 'synthetic-device', 'segment_messages': 2, 'channels': [{'topic': '/value', 'message_encoding': 'json'}, {'topic': '/raw', 'message_encoding': 'protobuf', 'schema': {'name': 'test.Value', 'encoding': 'protobuf', 'data_hex': descriptor.hex()}}]}
    rules.write_text(json.dumps(config))
    directory = work / 'recording'
    bridge = Bridge(rules, directory, port=0)
    try:
        with connect(f'ws://127.0.0.1:{bridge.server.port}', subprotocols=['foxglove.sdk.v1'], proxy=None, open_timeout=10) as client:
            advertised = {}
            while len(advertised) < 2:
                frame = client.recv(timeout=10)
                assert isinstance(frame, str)
                message = json.loads(frame)
                if message['op'] == 'advertise':
                    advertised.update({channel['topic']: channel for channel in message['channels']})
            client.send(json.dumps({'op': 'subscribe', 'subscriptions': [{'id': i + 1, 'channelId': advertised[topic]['id']} for i, topic in enumerate(('/value', '/raw'))]}))
            deadline = time.monotonic() + 10
            while not all(channel.has_sinks() for channel in bridge.channels.values()):
                assert time.monotonic() < deadline, 'SDK subscription not established'
                time.sleep(.005)
            cases = [('/value', 5, {'value': {'n': 9007199254740993}}, b'{"n":9007199254740993}'),
                     ('/raw', (1 << 64) - 1, {'data_hex': '08ff01'}, b'\x08\xff\x01'),
                     ('/value', 6, {'data_hex': '7b226e223a327d'}, b'{"n":2}')]
            for n, (topic, stamp, payload, expected) in enumerate(cases):
                bridge.send({'topic': topic, 'log_time': str(stamp), 'publish_time': str(stamp - 1), 'sequence': str(n), **payload})
                while True:
                    frame = client.recv(timeout=10)
                    if isinstance(frame, bytes):
                        break
                assert frame[0] == 1
                subscription, timestamp = struct.unpack_from('<IQ', frame, 1)
                assert subscription == (1 if topic == '/value' else 2)
                assert timestamp == stamp and frame[13:] == expected
            for invalid in ({'topic': '/missing', 'log_time': '0', 'publish_time': '0', 'sequence': '0', 'value': {}},
                            {'topic': '/raw', 'log_time': '-1', 'publish_time': '0', 'sequence': '0', 'data_hex': ''},
                            {'topic': '/raw', 'log_time': '0', 'publish_time': '0', 'sequence': '4294967296', 'data_hex': ''},
                            {'topic': '/raw', 'log_time': '0', 'publish_time': '0', 'sequence': '0', 'value': {}}):
                try:
                    bridge.send(invalid)
                except ValueError:
                    pass
                else:
                    raise AssertionError('invalid device envelope accepted')
            report = bridge.finish()
            assert report['accepted'] == 3 and report['network_delivery_acknowledged'] is False
    finally:
        if not bridge.closed:
            bridge.abort()
    records = []
    for path in sorted(directory.glob('*.mcap')):
        with path.open('rb') as f:
            records.extend((channel.topic, message) for _, channel, message in make_reader(f, validate_crcs=True).iter_messages(log_time_order=False))
    assert len(records) == 3
    assert [message.data for _, message in records] == [case[3] for case in cases]
    assert [message.publish_time for _, message in records] == [case[1] - 1 for case in cases]
    resumed = Bridge(rules, directory, port=0, resume=True)
    resumed.send({'topic': '/value', 'log_time': '7', 'publish_time': '7', 'sequence': '3', 'value': {'n': 3}})
    resumed.finish()
    # Invalid producer input aborts active tail while preserving prior commits.
    cli = ROOT / '_build/js/debug/build/cmd/device/device.js'
    bad_dir = work / 'invalid'
    p = subprocess.run(['node', str(cli), str(rules), str(bad_dir)], input=b'{"topic":"/raw","log_time":"-1","publish_time":"0","sequence":"0","data_hex":"00"}\n', capture_output=True, timeout=30)
    assert p.returncode == 2 and not list(bad_dir.glob('*.mcap'))
    import importlib.metadata
    result = {'result': 'passed', 'sdk': importlib.metadata.version('foxglove-sdk'), 'network_messages_checked': 3,
              'checks': ['real SDK advertise/subscribe', 'independent binary WebSocket frame decoding', 'exact UInt64 time', 'payload byte equality', 'MoonBit durable segments', 'CRC reader', 'resume', 'invalid envelopes rejected'],
              'scope': 'local synthetic device envelopes; physical device and desktop visualization not tested'}
    (ROOT / '_interop/device/report.json').write_text(json.dumps(result, indent=2) + '\n')
    print('Device workflows passed: current SDK WebSocket frames and independent recorded MCAP readback')


if __name__ == '__main__':
    main()
