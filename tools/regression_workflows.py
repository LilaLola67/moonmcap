"""Exact tolerance, alignment, missing/events, greedy oracle and MCAP evidence cases."""
from fractions import Fraction
import json
import os
from pathlib import Path
import random
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / '_interop_deps'))
from regression import compare, exact_equal
from artifact_common import sha256
from mcap.writer import Writer, CompressionType
from mcap.reader import make_reader


def main():
    work = ROOT / '_interop/regression' / uuid.uuid4().hex
    work.mkdir(parents=True)
    subprocess.run([os.environ.get('MOON', 'moon'), 'build', '--target', 'js', '--warn-list', '-a'], cwd=ROOT, capture_output=True, check=True)
    runs = 0
    def execute(a, b, rules):
        nonlocal runs
        runs += 1
        left, right = work / 'left.ndjson', work / 'right.ndjson'
        for path, rows in ((left, a), (right, b)):
            path.write_text(''.join(json.dumps(row) + '\n' for row in rows))
        return compare(left, right, rules, work / f'case-{runs}')
    def row(time, value):
        return {'topic': '/n', 'log_time': str(time), 'data': value}
    a = [row(n, {'v': '10', 'counter': '9007199254740993', 'reserved': {'$moonmcap_decimal': 'user-data'}}) for n in (10, 20, 30)]
    b = [row(n, {'v': '10.1', 'counter': '9007199254740993', 'reserved': {'$moonmcap_decimal': 'user-data'}}) for n in (511, 519, 531)]
    rules = {'fields': [{'path': '/v', 'abs': '0.1'}, {'path': '/counter'}, {'path': '/reserved', 'kind': 'exact'}], 'time_tolerance': '1', 'candidate_offset': '-500'}
    report = execute(a, b, rules)
    assert report['status'] == 'passed' and report['counts']['pairs'] == 3
    b[1]['data']['counter'] = '9007199254740994'
    report = execute(a, b, rules)
    assert report['counts']['field_failures'] == 1 and report['details'][0]['delta_exact'] == '1'
    assert execute(a, b, {**rules, 'thresholds': {'field_failures': 1}})['status'] == 'passed'
    assert execute(a, b[:2], rules)['counts']['missing'] == 1
    assert execute(a[:2], b, rules)['counts']['extra'] == 1
    for missing, expected in [('fail', 'failed'), ('equal', 'passed'), ('ignore', 'passed')]:
        assert execute([row(1, {})], [row(1, {})], {'fields': [{'path': '/absent', 'missing': missing}]})['status'] == expected
    assert execute([row(1, {'v': '100'})], [row(1, {'v': '101'})], {'fields': [{'path': '/v', 'rel': '0.01'}]})['status'] == 'passed'
    assert not exact_equal({'nested': True}, {'nested': 1})
    events_a, events_b = work / 'events-a.json', work / 'events-b.json'
    events_a.write_text(json.dumps({'events': [{'label': 'brake', 'start': '10', 'end': '20'}]}))
    events_b.write_text(json.dumps({'events': [{'label': 'brake', 'start': '511', 'end': '519'}]}))
    events = {'baseline': str(events_a), 'candidate': str(events_b), 'tolerance': '1'}
    assert execute(a, b, {**rules, 'events': events, 'thresholds': {'field_failures': 1}})['counts']['event_failures'] == 0
    events_b.write_text(json.dumps({'events': [{'label': 'brake', 'start': '511', 'end': '522'}]}))
    assert execute(a, b, {**rules, 'events': events})['counts']['event_failures'] == 1
    rng = random.Random(1908)
    oracle_pairs = 0
    for _ in range(12):
        left = sorted(rng.randrange(100) for _ in range(30))
        right = [rng.randrange(100) for _ in range(24)]
        available = list(enumerate(right))
        expected = 0
        for time in left:
            if not available:
                break
            pick = min(available, key=lambda item: (abs(item[1] - time), item[1], item[0]))
            if abs(pick[1] - time) <= 3:
                available.remove(pick)
                expected += 1
        result = execute([row(n, {'v': 1}) for n in left], [row(n, {'v': 1}) for n in right], {'fields': [{'path': '/v'}], 'time_tolerance': '3'})
        assert result['counts']['pairs'] == expected
        assert result['counts']['extra'] == len(right) - expected
        oracle_pairs += expected
    sources = []
    for side, values in enumerate((a, b)):
        source = work / f'source-{side}.mcap'
        with source.open('wb') as f:
            writer = Writer(f, compression=CompressionType.ZSTD, enable_data_crcs=True)
            writer.start()
            channel = writer.register_channel('/n', 'json', 0)
            state = writer.register_channel('/state', 'json', 0)
            writer.add_message(state, 0, b'{"state":"ready"}', 0, 0)
            for n, value in enumerate(values):
                writer.add_message(channel, int(value['log_time']), json.dumps(value['data']).encode(), int(value['log_time']), n)
            writer.finish()
        sources.append(source)
    evidence = {'baseline_mcap': str(sources[0]), 'candidate_mcap': str(sources[1]), 'baseline_sha256': sha256(sources[0]), 'candidate_sha256': sha256(sources[1]),
                'max_cases': 1, 'context': [{'topic': '/state'}]}
    report = execute(a, b, {**rules, 'evidence': evidence})
    assert len(report['evidence']) == 2
    for clip in report['evidence']:
        path = work / f'case-{runs}' / clip['file']
        with path.open('rb') as f:
            records = list(make_reader(f, validate_crcs=True).iter_messages())
        assert len(records) == 2 and records[0][1].topic == '/state'
    try:
        execute(a, b, {**rules, 'evidence': {**evidence, 'candidate_sha256': '0' * 64}})
    except ValueError:
        assert not (work / f'case-{runs}' / 'report.json').exists()
    else:
        raise AssertionError('evidence source mismatch accepted')
    for bad in ({**rules, 'candidate_offset': '-1000'}, {**rules, 'fields': [{'path': '/v', 'abs': '-1'}]}):
        try:
            execute(a, b, bad)
        except ValueError:
            pass
        else:
            raise AssertionError('invalid regression rules accepted')
    result = {'result': 'passed', 'runs': runs, 'brute_force_pairs': oracle_pairs, 'checks': ['exact decimal tolerance boundaries', 'UInt64-size counters', 'time alignment', 'missing/extra thresholds', 'event boundaries', 'reserved user keys', 'greedy brute-force oracle', 'fingerprint-checked MCAP evidence with context']}
    (ROOT / '_interop/regression/report.json').write_text(json.dumps(result, indent=2) + '\n')
    print(f'Regression workflows passed: {runs} runs and {oracle_pairs} brute-force pairs')


if __name__ == '__main__':
    main()
