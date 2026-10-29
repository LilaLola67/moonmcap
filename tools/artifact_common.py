"""Small standard-library helpers shared by optional offline workflow adapters."""
from decimal import Decimal
import hashlib
import json
import math
import os
from pathlib import Path
import re
import tempfile

MISSING = object()


def strict_json(data, *, decimals=False):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('duplicate JSON member: ' + key)
            result[key] = value
        return result
    def finite_float(text):
        value = float(text)
        if not math.isfinite(value):
            raise ValueError('nonfinite JSON number')
        return value
    return json.loads(data, object_pairs_hook=pairs, parse_float=Decimal if decimals else finite_float,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError('nonfinite JSON')))


def sha256(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def atomic_json(path, value):
    path = Path(path)
    fd, temporary = tempfile.mkstemp(prefix='.' + path.name, suffix='.part', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as output:
            output.write((json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n').encode())
            output.flush()
            os.fsync(output.fileno())
        os.link(temporary, path)
    finally:
        os.unlink(temporary)


def pointer(value, path):
    if not isinstance(path, str) or (path and not path.startswith('/')):
        raise ValueError('JSON Pointer required')
    for token in path.split('/')[1:] if path else []:
        if re.search(r'~(?![01])', token):
            raise ValueError('invalid pointer escape')
        token = token.replace('~1', '/').replace('~0', '~')
        if isinstance(value, dict):
            value = value.get(token, MISSING)
        elif isinstance(value, list) and re.fullmatch(r'0|[1-9][0-9]*', token):
            value = value[int(token)] if int(token) < len(value) else MISSING
        else:
            return MISSING
    return value


def uint(value, bits=64):
    if not isinstance(value, str) or not re.fullmatch(r'0|[1-9][0-9]*', value) or len(value) > 30:
        raise ValueError('bounded unsigned decimal string required')
    number = int(value)
    if number >= 1 << bits:
        raise ValueError('unsigned integer overflow')
    return number


def rows(path, maximum=1024 * 1024, *, raw=False):
    with open(path, 'rb') as source:
        while line := source.readline(maximum + 1):
            if len(line) > maximum:
                raise ValueError('NDJSON row byte budget exceeded')
            value = strict_json(line, decimals=True)
            if not isinstance(value, dict):
                raise ValueError('NDJSON object required')
            yield (value, line) if raw else value
