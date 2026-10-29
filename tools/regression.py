"""Disk-indexed tolerant regression over MoonMCAP normalized NDJSON views."""
import argparse
from contextlib import closing
from decimal import Decimal
from fractions import Fraction
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import tempfile

from artifact_common import MISSING, atomic_json, pointer, rows, sha256, strict_json, uint

ROOT = Path(__file__).resolve().parents[1]


def exact_equal(a, b):
    if type(a) in (int, Decimal) and type(b) in (int, Decimal):
        return number(a) == number(b)
    if type(a) is not type(b):
        return False
    if isinstance(a, dict):
        return set(a) == set(b) and all(exact_equal(a[k], b[k]) for k in a)
    if isinstance(a, list):
        return len(a) == len(b) and all(exact_equal(x, y) for x, y in zip(a, b))
    return a == b


def number(value):
    if type(value) not in (int, Decimal, str):
        raise ValueError('exact numeric value required')
    text = str(value)
    if len(text) > 256 or not re.fullmatch(r'-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?', text):
        raise ValueError('bounded decimal value required')
    decimal = Decimal(text)
    if not decimal.is_finite() or len(decimal.as_tuple().digits) > 128 or abs(decimal.as_tuple().exponent) > 128:
        raise ValueError('numeric precision/exponent exceeds 128')
    return Fraction(decimal)


def compare(baseline, candidate, config, directory):
    if set(config) - {'fields', 'time_tolerance', 'baseline_offset', 'candidate_offset', 'consume', 'topics', 'thresholds', 'detail_limit', 'disk_bytes', 'events', 'evidence'}:
        raise ValueError('unknown regression option')
    specs = config['fields']
    if not isinstance(specs, list) or not 1 <= len(specs) <= 128:
        raise ValueError('1..128 comparison fields required')
    compiled = []
    paths = set()
    for spec in specs:
        if set(spec) - {'path', 'kind', 'abs', 'rel', 'missing'} or spec['path'] in paths:
            raise ValueError('invalid or duplicate comparison field')
        path, kind, missing = spec['path'], spec.get('kind', 'number'), spec.get('missing', 'fail')
        pointer({}, path)
        if kind not in ('number', 'exact') or missing not in ('fail', 'ignore', 'equal'):
            raise ValueError('invalid field comparison mode')
        absolute, relative = number(spec.get('abs', '0')), number(spec.get('rel', '0'))
        if min(absolute, relative) < 0:
            raise ValueError('negative tolerance')
        compiled.append((path, kind, missing, absolute, relative))
        paths.add(path)
    tolerance = uint(config.get('time_tolerance', '0'))
    offsets = [config.get(key, '0') for key in ('baseline_offset', 'candidate_offset')]
    if any(not isinstance(v, str) or not re.fullmatch(r'-?(0|[1-9][0-9]*)', v) or len(v) > 21 for v in offsets):
        raise ValueError('signed integer offset strings required')
    offsets = list(map(int, offsets))
    if any(not -(1 << 63) <= v < (1 << 63) for v in offsets):
        raise ValueError('offset outside Int64')
    consume = config.get('consume', True)
    detail_limit, disk_bytes = config.get('detail_limit', 128), config.get('disk_bytes', 1024 * 1024 * 1024)
    if type(consume) is not bool or type(detail_limit) is not int or not 0 <= detail_limit <= 10000 or type(disk_bytes) is not int or disk_bytes < 65536:
        raise ValueError('invalid comparison budgets')
    topics = config.get('topics', [])
    if not isinstance(topics, list) or any(not isinstance(t, str) or not t for t in topics):
        raise ValueError('topic strings required')
    thresholds = {'field_failures': 0, 'missing': 0, 'extra': 0, 'event_failures': 0, 'minimum_pairs': 1, **config.get('thresholds', {})}
    if set(thresholds) != {'field_failures', 'missing', 'extra', 'event_failures', 'minimum_pairs'} or any(type(v) is not int or v < 0 for v in thresholds.values()):
        raise ValueError('invalid regression thresholds')
    directory = Path(directory)
    directory.mkdir()  # An existing output directory is never reused.
    input_hashes = [sha256(path) for path in (baseline, candidate)]
    details, cases = [], []
    counts = {'pairs': 0, 'field_failures': 0, 'missing': 0, 'extra': 0, 'event_failures': 0}
    def issue(kind, topic, left=None, right=None, path=None, delta=None):
        entry = {'kind': kind, 'topic': topic, 'baseline_time': left, 'candidate_time': right}
        if path is not None:
            entry['path'] = path
        if delta is not None:
            entry['delta_exact'] = str(delta)
        if len(details) < detail_limit:
            details.append(entry)
        if len(cases) < 32 and not any((old['topic'], old['baseline_time'], old['candidate_time']) == (topic, left, right) for old in cases):
            cases.append(entry)
    def stamp(row, side):
        topic = row['topic']
        if not isinstance(topic, str) or not topic:
            raise ValueError('nonempty row topic required')
        original = uint(row['log_time'])
        aligned = original + offsets[side]
        if not 0 <= aligned < 1 << 64:
            raise ValueError('aligned timestamp overflow')
        return topic, original, aligned
    with tempfile.TemporaryDirectory(prefix='.index-', dir=directory) as temporary:
        with closing(sqlite3.connect(str(Path(temporary) / 'right.sqlite'))) as db:
            db.execute('PRAGMA page_size=4096')
            db.execute(f'PRAGMA max_page_count={disk_bytes // 4096}')
            db.execute('PRAGMA journal_mode=OFF')
            db.execute('PRAGMA cache_size=-4096')
            db.execute('CREATE TABLE samples(id INTEGER PRIMARY KEY,topic TEXT,time TEXT,row TEXT,used INTEGER DEFAULT 0)')
            db.execute('CREATE INDEX by_topic ON samples(topic,used,time,id)')
            db.execute('CREATE INDEX reusable ON samples(topic,time,id)')
            for row, encoded in rows(candidate, raw=True):
                topic, original, aligned = stamp(row, 1)
                if topics and topic not in topics:
                    continue
                # Store source bytes, without reserved marker keys or float coercion.
                db.execute('INSERT INTO samples(topic,time,row) VALUES(?,?,?)', (topic, f'{aligned:020}', encoded))
            previous = None
            for left in rows(baseline):
                topic, original, aligned = stamp(left, 0)
                if topics and topic not in topics:
                    continue
                if previous is not None and aligned < previous:
                    raise ValueError('baseline rows must be in nondecreasing aligned time')
                previous = aligned
                available = ' AND used=0' if consume else ''
                choices = []
                for operator, order in [('<=', 'DESC'), ('>=', 'ASC')]:
                    found = db.execute(f'SELECT id,time,row FROM samples WHERE topic=?{available} AND time{operator}? ORDER BY time {order},id ASC LIMIT 1', (topic, f'{aligned:020}')).fetchone()
                    if found:
                        choices.append(found)
                choice = min(choices, key=lambda item: (abs(int(item[1]) - aligned), int(item[1]), item[0])) if choices else None
                if choice is None or abs(int(choice[1]) - aligned) > tolerance:
                    counts['missing'] += 1
                    issue('missing', topic, str(original))
                    continue
                right = strict_json(choice[2], decimals=True)
                db.execute('UPDATE samples SET used=1 WHERE id=?', (choice[0],))
                counts['pairs'] += 1
                for path, kind, missing, absolute, relative in compiled:
                    a, b = pointer(left['data'], path), pointer(right['data'], path)
                    delta = None
                    if a is MISSING or b is MISSING:
                        passed = missing == 'ignore' or (missing == 'equal' and a is b)
                    elif kind == 'exact':
                        passed = exact_equal(a, b)
                    else:
                        try:
                            x, y = number(a), number(b)
                            delta = abs(x - y)
                            passed = delta <= max(absolute, relative * max(abs(x), abs(y)))
                        except ValueError:
                            passed = False
                    if not passed:
                        counts['field_failures'] += 1
                        issue('field', topic, str(original), right['log_time'], path, delta)
            counts['extra'] = db.execute('SELECT count(*) FROM samples WHERE used=0').fetchone()[0]
            for _, topic, encoded in db.execute('SELECT id,topic,row FROM samples WHERE used=0 ORDER BY id LIMIT 32'):
                right = strict_json(encoded)
                issue('extra', topic, right=right['log_time'])
    event_hashes = []
    if 'events' in config:
        event_config = config['events']
        if set(event_config) - {'baseline', 'candidate', 'tolerance'}:
            raise ValueError('unknown event comparison option')
        event_tolerance = uint(event_config.get('tolerance', '0'))
        sets = []
        for side, key in enumerate(('baseline', 'candidate')):
            path = Path(event_config[key])
            if path.stat().st_size > 4 * 1024 * 1024:
                raise ValueError('event input exceeds 4 MiB')
            event_hashes.append(sha256(path))
            value = strict_json(path.read_bytes())
            events = value['events'] if isinstance(value, dict) else value
            if not isinstance(events, list) or len(events) > 10000:
                raise ValueError('event list exceeds 10000')
            grouped = {}
            for event in events:
                label = event.get('label', '')
                if not isinstance(label, str):
                    raise ValueError('event label string required')
                start, end = uint(event['start']) + offsets[side], uint(event['end']) + offsets[side]
                if not 0 <= start <= end < 1 << 64:
                    raise ValueError('invalid aligned event range')
                grouped.setdefault(label, []).append((start, end))
            sets.append({key: sorted(value) for key, value in grouped.items()})
        for label in sorted(set(sets[0]) | set(sets[1])):
            a, b = sets[0].get(label, []), sets[1].get(label, [])
            for index in range(max(len(a), len(b))):
                if index >= len(a) or index >= len(b) or max(abs(a[index][j] - b[index][j]) for j in (0, 1)) > event_tolerance:
                    counts['event_failures'] += 1
                    issue('event:' + label, '', str(a[index][0] - offsets[0]) if index < len(a) else None, str(b[index][0] - offsets[1]) if index < len(b) else None)
    clips = []
    if 'evidence' in config:
        evidence = config['evidence']
        if set(evidence) - {'baseline_mcap', 'candidate_mcap', 'baseline_sha256', 'candidate_sha256', 'pre', 'post', 'max_cases', 'context'}:
            raise ValueError('unknown evidence option')
        maximum = evidence.get('max_cases', 8)
        if type(maximum) is not int or not 0 <= maximum <= 32:
            raise ValueError('evidence max_cases must be 0..32')
        pre, post = uint(evidence.get('pre', '0')), uint(evidence.get('post', '0'))
        cli = ROOT / '_build/js/debug/build/cmd/incident/incident.js'
        for side, key in enumerate(('baseline', 'candidate')):
            source = Path(evidence[key + '_mcap'])
            if sha256(source) != evidence[key + '_sha256']:
                raise ValueError('evidence MCAP fingerprint mismatch')
            for index, case in enumerate(cases[:maximum]):
                stamp = case[key + '_time']
                if stamp is None:
                    continue
                stamp = uint(stamp)
                rules = {'start': str(max(0, stamp - pre)), 'end': str(min((1 << 64) - 1, stamp + post)), 'stream': True,
                         'topics': [case['topic']] if case['topic'] else [], 'context': evidence.get('context', [])}
                rules_path = directory / '.clip-rules.json'
                rules_path.write_text(json.dumps(rules))
                output = directory / f'{key}-{index}.mcap'
                command = subprocess.run(['node', str(cli), 'clip', str(source), str(rules_path), str(output)], capture_output=True, text=True, encoding='utf-8', timeout=300)
                if command.returncode:
                    raise RuntimeError('evidence clip failed: ' + command.stderr[-1000:])
                clips.append({'side': key, 'case': index, 'file': output.name, 'sha256': sha256(output), 'source_sha256': evidence[key + '_sha256']})
            if sha256(source) != evidence[key + '_sha256']:
                raise ValueError('evidence source changed during clipping')
        (directory / '.clip-rules.json').unlink(missing_ok=True)
    if [sha256(path) for path in (baseline, candidate)] != input_hashes:
        raise ValueError('regression inputs changed during comparison')
    if 'events' in config and [sha256(config['events'][key]) for key in ('baseline','candidate')] != event_hashes:
        raise ValueError('event inputs changed during comparison')
    passed = counts['pairs'] >= thresholds['minimum_pairs'] and all(counts[key] <= thresholds[key] for key in ('field_failures', 'missing', 'extra', 'event_failures'))
    report = {'status': 'passed' if passed else 'failed', 'counts': counts, 'thresholds': thresholds, 'details': details, 'details_truncated': sum(counts[key] for key in ('field_failures', 'missing', 'extra', 'event_failures')) > len(details),
              'baseline_sha256': input_hashes[0], 'candidate_sha256': input_hashes[1], 'event_sha256': event_hashes,
              'rules_sha256': hashlib.sha256(json.dumps(config, sort_keys=True, separators=(',', ':')).encode()).hexdigest(), 'evidence': clips,
              'alignment': {'baseline_offset': str(offsets[0]), 'candidate_offset': str(offsets[1]), 'time_tolerance': str(tolerance), 'consume': consume},
              'matching': 'greedy baseline physical order; nearest same-topic candidate, earlier timestamp then ordinal tie break',
              'evidence_association': 'caller links normalized views to fingerprint-checked source MCAP files'}
    atomic_json(directory / 'report.json', report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ('baseline', 'candidate', 'rules', 'output_directory'):
        parser.add_argument(key)
    args = parser.parse_args()
    try:
        report = compare(args.baseline, args.candidate, strict_json(Path(args.rules).read_bytes()), args.output_directory)
        print(json.dumps({'status': report['status'], 'counts': report['counts']}))
        return 0 if report['status'] == 'passed' else 1
    except (ValueError, KeyError, OSError, sqlite3.Error, RuntimeError) as error:
        parser.exit(2, str(error) + '\n')


if __name__ == '__main__':
    raise SystemExit(main())
