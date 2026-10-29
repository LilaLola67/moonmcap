"""Typed bounded NDJSON -> Arrow IPC / Parquet adapter for MoonMCAP views."""
import argparse
import base64
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
if (ROOT / '_interop_deps').exists():
    sys.path.insert(0, str(ROOT / '_interop_deps'))
import pyarrow as pa
import pyarrow.parquet as pq


def digest(path):
    h = hashlib.sha256()
    with open(path, 'rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def strict_json(data):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('duplicate JSON member: ' + key)
            result[key] = value
        return result
    return json.loads(data, object_pairs_hook=pairs, parse_constant=lambda value: (_ for _ in ()).throw(ValueError('nonfinite JSON: ' + value)))


def pointer(value, path):
    if not isinstance(path, str) or (path and not path.startswith('/')):
        raise ValueError('JSON Pointer required')
    for token in path.split('/')[1:] if path else []:
        if re.search(r'~(?![01])', token):
            raise ValueError('invalid pointer escape')
        token = token.replace('~1', '/').replace('~0', '~')
        if isinstance(value, dict):
            value = value.get(token)
        elif isinstance(value, list) and re.fullmatch(r'0|[1-9][0-9]*', token):
            value = value[int(token)] if int(token) < len(value) else None
        else:
            return None
    return value


def fields(specs, depth=0):
    if not isinstance(specs, list) or not 1 <= len(specs) <= 128:
        raise ValueError('1..128 fields required')
    seen = set()
    result = []
    for spec in specs:
        if not isinstance(spec, dict) or set(spec) - {'name', 'path', 'type', 'nullable'}:
            raise ValueError('invalid field specification')
        name = spec['name']
        if not isinstance(name, str) or not name or name in seen:
            raise ValueError('unique nonempty field names required')
        seen.add(name)
        nullable = spec.get('nullable', True)
        if type(nullable) is not bool:
            raise ValueError('nullable must be boolean')
        result.append(pa.field(name, data_type(spec['type'], depth), nullable=nullable))
    return result


def data_type(spec, depth=0):
    if depth > 16:
        raise ValueError('type nesting exceeds 16')
    if isinstance(spec, str):
        simple = {'bool': pa.bool_(), 'string': pa.string(), 'binary': pa.binary(), 'float64': pa.float64(),
                  'int32': pa.int32(), 'uint32': pa.uint32(), 'int64': pa.int64(), 'uint64': pa.uint64(),
                  'timestamp_ns': pa.timestamp('ns', tz='UTC')}
        if spec not in simple:
            raise ValueError('unsupported type: ' + spec)
        return simple[spec]
    if isinstance(spec, dict) and set(spec) == {'list'}:
        return pa.list_(data_type(spec['list'], depth + 1))
    if isinstance(spec, dict) and set(spec) == {'struct'}:
        return pa.struct(fields(spec['struct'], depth + 1))
    raise ValueError('invalid type specification')


def convert(value, field):
    if value is None:
        if not field.nullable:
            raise ValueError('required column is missing or null: ' + field.name)
        return None
    kind = field.type
    if pa.types.is_struct(kind):
        if not isinstance(value, dict) or set(value) - set(kind.names):
            raise ValueError('struct requires only declared members: ' + field.name)
        return {child.name: convert(value.get(child.name), child) for child in kind}
    if pa.types.is_list(kind):
        if not isinstance(value, list):
            raise ValueError('list required: ' + field.name)
        return [convert(item, kind.value_field) for item in value]
    if pa.types.is_integer(kind) or pa.types.is_timestamp(kind):
        if type(value) is int:
            n = value
        elif isinstance(value, str) and re.fullmatch(r'-?(0|[1-9][0-9]*)', value):
            n = int(value)
        else:
            raise ValueError('exact integer required: ' + field.name)
        signed = not pa.types.is_unsigned_integer(kind)
        bits = 64 if pa.types.is_timestamp(kind) else kind.bit_width
        low, high = (-(1 << (bits - 1)), (1 << (bits - 1)) - 1) if signed else (0, (1 << bits) - 1)
        if not low <= n <= high:
            raise ValueError('integer/timestamp overflow: ' + field.name)
        return n
    if pa.types.is_floating(kind):
        if type(value) not in (int, float) or not math.isfinite(value):
            raise ValueError('finite number required: ' + field.name)
        if type(value) is int and int(float(value)) != value:
            raise ValueError('lossy integer to float conversion: ' + field.name)
        return float(value)
    if pa.types.is_boolean(kind):
        if type(value) is not bool:
            raise ValueError('boolean required: ' + field.name)
    elif pa.types.is_string(kind):
        if not isinstance(value, str):
            raise ValueError('string required: ' + field.name)
    elif pa.types.is_binary(kind):
        if not isinstance(value, str):
            raise ValueError('base64 string required: ' + field.name)
        return base64.b64decode(value, validate=True)
    return value


def export(source, config, output):
    source, output = Path(source), Path(output)
    if output.exists() or source.resolve() == output.resolve():
        raise ValueError('output must be a new file')
    if set(config) - {'format', 'fields', 'batch_rows', 'batch_bytes', 'row_bytes'}:
        raise ValueError('unknown columnar option')
    format_ = config.get('format', 'parquet')
    if format_ not in ('arrow', 'parquet'):
        raise ValueError('format must be arrow or parquet')
    budgets = [config.get(k, default) for k, default in [('batch_rows', 1000), ('batch_bytes', 8 * 1024 * 1024), ('row_bytes', 1024 * 1024)]]
    if any(type(n) is not int or n <= 0 for n in budgets) or budgets[0] > 100000 or budgets[1] > 256 * 1024 * 1024 or budgets[2] > budgets[1]:
        raise ValueError('invalid batch budgets')
    batch_rows, batch_bytes, row_bytes = budgets
    specs = config['fields']
    schema_fields = fields(specs)
    paths = [s.get('path', '/' + s['name'].replace('~', '~0').replace('/', '~1')) for s in specs]
    for path in paths:
        pointer({}, path)
    fingerprint = digest(source)
    rules_sha = hashlib.sha256(json.dumps(config, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    schema = pa.schema(schema_fields, metadata={b'moonmcap.input_sha256': fingerprint.encode(), b'moonmcap.rules_sha256': rules_sha.encode(), b'moonmcap.adapter': b'columnar-v1'})
    fd, temporary = tempfile.mkstemp(prefix='.' + output.name + '.', suffix='.part', dir=output.parent)
    os.close(fd)
    count, batches, peak = 0, 0, 0
    try:
        with pa.OSFile(temporary, 'wb') as sink:
            writer = pa.ipc.new_file(sink, schema) if format_ == 'arrow' else pq.ParquetWriter(sink, schema, version='2.6', compression='zstd')
            with writer:
                batch, size = [], 0
                observed = hashlib.sha256()
                def flush():
                    nonlocal batches
                    if batch:
                        record = pa.RecordBatch.from_pylist(batch, schema=schema)
                        if format_ == 'arrow':
                            writer.write_batch(record)
                        else:
                            writer.write_table(pa.Table.from_batches([record]))
                        batches += 1
                with source.open('rb') as stream:
                    while line := stream.readline(row_bytes + 1):
                        if len(line) > row_bytes:
                            raise ValueError('NDJSON row byte budget exceeded')
                        observed.update(line)
                        row = strict_json(line)
                        if batch and (len(batch) == batch_rows or size + len(line) > batch_bytes):
                            flush()
                            batch, size = [], 0
                        batch.append({field.name: convert(pointer(row, path), field) for field, path in zip(schema_fields, paths)})
                        size += len(line)
                        peak = max(peak, size)
                        count += 1
                flush()
                if observed.hexdigest() != fingerprint:
                    raise ValueError('input changed during export')
        with open(temporary, 'rb+') as stream:
            os.fsync(stream.fileno())
        os.link(temporary, output)
        return {'rows': count, 'batches': batches, 'peak_batch_input_bytes': peak, 'format': format_, 'input_sha256': fingerprint, 'rules_sha256': rules_sha, 'output_sha256': digest(output)}
    finally:
        os.unlink(temporary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source')
    parser.add_argument('rules')
    parser.add_argument('output')
    args = parser.parse_args()
    try:
        print(json.dumps(export(args.source, strict_json(Path(args.rules).read_bytes()), args.output)))
    except (ValueError, KeyError, OSError, pa.ArrowException) as error:
        parser.exit(2, str(error) + '\n')


if __name__ == '__main__':
    main()
