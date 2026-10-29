"""Current Foxglove SDK broadcaster plus MoonBit durable device recorder."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
if (ROOT / '_interop_deps').exists():
    sys.path.insert(0, str(ROOT / '_interop_deps'))
import foxglove


def strict_json(data):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('duplicate JSON key: ' + key)
            result[key] = value
        return result
    return json.loads(data, object_pairs_hook=pairs, parse_constant=lambda value: (_ for _ in ()).throw(ValueError('nonfinite JSON')))


def integer(value, bits):
    if not isinstance(value, str) or not re.fullmatch(r'0|[1-9][0-9]*', value):
        raise ValueError('unsigned decimal string required')
    number = int(value)
    if number >= 1 << bits:
        raise ValueError('integer overflow')
    return number


def hex_bytes(value, maximum):
    if not isinstance(value, str) or len(value) % 2 or len(value) > 2 * maximum or not re.fullmatch('[0-9a-fA-F]*', value):
        raise ValueError('bounded hexadecimal string required')
    return bytes.fromhex(value)


class Bridge:
    def __init__(self, rules, directory, *, host='127.0.0.1', port=8765, resume=False):
        self.context = foxglove.Context()
        self.channels, self.encodings = {}, {}
        self.server = None
        self.child = None
        self.count = 0
        self.closed = False
        self.stderr = tempfile.TemporaryFile()
        config = strict_json(Path(rules).read_bytes())
        if not isinstance(config, dict) or not isinstance(config.get('channels'), list) or not 1 <= len(config['channels']) <= 128:
            raise ValueError('1..128 configured channels required')
        try:
            for channel in config['channels']:
                topic = channel['topic']
                if not isinstance(topic, str) or not topic or topic in self.channels:
                    raise ValueError('unique nonempty topics required')
                encoding = channel['message_encoding']
                schema = channel.get('schema')
                if schema is None and encoding != 'json':
                    raise ValueError('Foxglove SDK requires an explicit schema for non-JSON channels')
                native = foxglove.Schema(name=schema['name'], encoding=schema['encoding'], data=hex_bytes(schema['data_hex'], 1024 * 1024)) if schema else None
                self.channels[topic] = foxglove.Channel(topic, schema=native, message_encoding=encoding, context=self.context, metadata=channel.get('metadata'))
                self.encodings[topic] = encoding
            self.server = foxglove.start_server(host=host, port=port, context=self.context, message_backlog_size=32, session_id=config['identity'])
            cli = ROOT / '_build/js/debug/build/cmd/device/device.js'
            self.child = subprocess.Popen([os.environ.get('NODE', 'node'), str(cli), str(Path(rules).resolve()), str(Path(directory).resolve()), *(['--resume'] if resume else [])], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.stderr, cwd=ROOT)
            if self._response().get('kind') != 'ready':
                raise ValueError('recorder did not become ready')
        except BaseException:
            self.abort()
            raise

    def _response(self):
        line = self.child.stdout.readline(1024 * 1024)
        if not line.endswith(b'\n'):
            raise RuntimeError('recorder stopped or response exceeded budget')
        return strict_json(line)

    def send(self, envelope):
        if self.closed:
            raise ValueError('bridge is closed')
        if not isinstance(envelope, dict) or set(envelope) - {'topic', 'log_time', 'publish_time', 'sequence', 'data_hex', 'value'}:
            raise ValueError('invalid device envelope')
        topic = envelope['topic']
        if topic not in self.channels or ('value' in envelope) == ('data_hex' in envelope):
            raise ValueError('registered topic and exactly one payload required')
        log_time = integer(envelope['log_time'], 64)
        integer(envelope['publish_time'], 64)
        integer(envelope['sequence'], 32)
        if 'value' in envelope:
            if self.encodings[topic] != 'json':
                raise ValueError('value requires a JSON channel')
            payload = json.dumps(envelope['value'], ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode()
        else:
            payload = hex_bytes(envelope['data_hex'], 1024 * 1024)
        if len(payload) > 1024 * 1024:
            raise ValueError('device payload exceeds 1 MiB')
        # Send precisely the same encoded bytes to the recorder and SDK.
        record = {key: value for key, value in envelope.items() if key != 'value'}
        record['data_hex'] = payload.hex()
        line = json.dumps(record, separators=(',', ':'), allow_nan=False).encode() + b'\n'
        if len(line) > 2101248:
            raise ValueError('device line byte budget exceeded')
        self.child.stdin.write(line)
        self.child.stdin.flush()
        accepted = self._response()
        if accepted.get('kind') != 'accepted':
            raise RuntimeError('recorder did not accept envelope')
        self.channels[topic].log(payload, log_time=log_time)
        self.count += 1
        return accepted

    def finish(self):
        if self.closed:
            raise ValueError('bridge is closed')
        self.child.stdin.close()
        report = self._response()
        code = self.child.wait(timeout=30)
        if code or report.get('kind') != 'finished':
            raise RuntimeError('recorder finalization failed')
        self.closed = True
        self.child.stdout.close()
        self.server.stop()
        for channel in self.channels.values():
            channel.close()
        self.stderr.close()
        return {'accepted': self.count, 'recording': report['recording'], 'sdk': 'foxglove-sdk', 'network_delivery_acknowledged': False}

    def abort(self):
        self.closed = True
        if self.child is not None and self.child.poll() is None:
            self.child.terminate()
            try:
                self.child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.child.kill()
                self.child.wait(timeout=10)
        if self.child is not None:
            for pipe in (self.child.stdin, self.child.stdout):
                if pipe is not None and not pipe.closed:
                    pipe.close()
        if self.server is not None:
            self.server.stop()
        for channel in self.channels.values():
            channel.close()
        self.stderr.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('rules')
    parser.add_argument('recording_directory')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=8765)
    parser.add_argument('--resume', action='store_true')
    args = parser.parse_args()
    bridge = None
    try:
        bridge = Bridge(args.rules, args.recording_directory, host=args.host, port=args.port, resume=args.resume)
        print(json.dumps({'kind': 'ready', 'host': args.host, 'port': bridge.server.port}), flush=True)
        while line := sys.stdin.buffer.readline(2101249):
            if len(line) > 2101248 or not line.endswith(b'\n'):
                raise ValueError('bounded newline-terminated device envelope required')
            bridge.send(strict_json(line))
        print(json.dumps(bridge.finish()), flush=True)
    except (Exception, KeyboardInterrupt) as error:
        if bridge is not None:
            bridge.abort()
        parser.exit(2, str(error) + '\n')


if __name__ == '__main__':
    main()
