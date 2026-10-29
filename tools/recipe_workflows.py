"""Real multi-step recipes, immutable fingerprints and both publication crash boundaries."""
import json
import os
from pathlib import Path
import subprocess
import sys
from unittest.mock import patch
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / '_interop_deps'))
import pyarrow.parquet as pq
from mcap.writer import Writer, CompressionType
from mcap.reader import make_reader
import recipes as r


def main():
    work = ROOT / '_interop/recipes' / uuid.uuid4().hex
    work.mkdir(parents=True)
    source = work / 'input.mcap'
    with source.open('wb') as stream:
        writer = Writer(stream, compression=CompressionType.ZSTD, enable_data_crcs=True)
        writer.start()
        schema = writer.register_schema('demo.Value', 'jsonschema', b'{"type":"object"}')
        channel = writer.register_channel('/value', 'json', schema)
        for n in range(12):
            writer.add_message(channel, n * 10, json.dumps({'v': n, 'counter': str(9007199254740993 + n)}).encode(), n * 10 + 2, n)
        writer.finish()
    configs = {
        'normalize': {'fields': [{'source': '/v', 'target': '/v'}, {'source': '/counter', 'target': '/counter', 'conversion': 'unsigned', 'bits': 64}]},
        'columnar': {'fields': [{'name': 'time', 'path': '/log_time', 'type': 'uint64'}, {'name': 'counter', 'path': '/data/counter', 'type': 'uint64'}], 'batch_rows': 3},
        'regression': {'fields': [{'path': '/counter'}, {'path': '/v'}]},
        'quality': {'start': '0', 'end': '110', 'rules': [{'topic': '/value', 'minimum_messages': 12}]},
        'clip': {'start': '30', 'end': '70', 'topics': ['/value'], 'stream': True},
    }
    for name, value in configs.items():
        (work / (name + '.json')).write_text(json.dumps(value))
    spec = {'version': 1, 'inputs': {'source': {'path': source.name, 'kind': 'mcap'}},
            'rules': {name: name + '.json' for name in configs}, 'steps': [
                {'id': 'inventory', 'op': 'inventory', 'input': 'source'},
                {'id': 'view', 'op': 'normalize', 'input': 'source', 'rules': 'normalize'},
                {'id': 'table', 'op': 'columnar', 'input': 'view', 'rules': 'columnar'},
                {'id': 'comparison', 'op': 'regression', 'baseline': 'view', 'candidate': 'view', 'rules': 'regression'},
                {'id': 'quality', 'op': 'quality', 'input': 'source', 'rules': 'quality'},
                {'id': 'clip', 'op': 'clip', 'input': 'source', 'rules': 'clip'},
            ]}
    recipe = work / 'recipe.json'
    recipe.write_text(json.dumps(spec))
    checks = []
    def execute(job, **kwargs):
        return r.execute_recipe(recipe, work / job, **kwargs)
    def reject(job, **kwargs):
        try:
            execute(job, **kwargs)
        except (ValueError, RuntimeError, FileExistsError):
            return
        raise AssertionError('invalid or modified recipe was accepted: ' + job)
    first = execute('resume', stop_after=2)
    assert not first['complete'] and first['completed'] == 2
    original = work / 'resume/artifacts/view/output.ndjson'
    stat, digest = original.stat().st_mtime_ns, r.sha256(original)
    report = execute('resume', resume=True)
    assert report['complete'] and report['analysis_status'] == 'passed'
    assert (original.stat().st_mtime_ns, r.sha256(original)) == (stat, digest)
    table = pq.read_table(work / 'resume/artifacts/table/output.parquet').to_pylist()
    assert [row['counter'] for row in table] == list(range(9007199254740993, 9007199254741005))
    with (work / 'resume/artifacts/clip/output.mcap').open('rb') as stream:
        messages = list(make_reader(stream, validate_crcs=True).iter_messages())
    assert [m.log_time for _, _, m in messages] == [30, 40, 50, 60, 70]
    plan = json.loads((work / 'resume/plan.json').read_bytes())
    assert plan['inputs']['source']['schemas'][0]['name'] == 'demo.Value'
    assert len(plan['inputs']['source']['schema_sha256']) == 64
    assert str(work) not in json.dumps(plan)
    assert execute('resume', resume=True) == report
    checks.append('six-step pipeline, exact Parquet readback, MCAP CRC readback, unchanged completed outputs')
    original.write_bytes(original.read_bytes() + b'\n')
    reject('resume', resume=True)
    checks.append('completed output modification rejected')
    for name in ('before-rename', 'after-rename'):
        original_save = r.save_state
        def crashing_save(job, state):
            if name == 'before-rename':
                original_save(job, state)
                if state['pending'] is not None:
                    raise SystemExit('simulated crash after durable pending receipt')
            elif state['completed']:
                raise SystemExit('simulated crash after artifact rename')
            else:
                original_save(job, state)
        with patch.object(r, 'save_state', crashing_save):
            try:
                execute(name)
            except SystemExit:
                pass
            else:
                raise AssertionError('crash boundary was not reached')
        state = json.loads((work / name / 'state.json').read_bytes())['state']
        assert state['pending']['id'] == 'inventory'
        result = execute(name, resume=True)
        assert result['complete'] and len(result['artifacts']) == 6
    checks.append('durable pending recovery before and after artifact rename')
    execute('changed', stop_after=1)
    path = work / 'quality.json'
    saved = path.read_bytes()
    path.write_bytes(saved + b' ')
    reject('changed', resume=True)
    path.write_bytes(saved)
    with patch.object(r, 'code_fingerprint', return_value='f' * 64):
        reject('changed', resume=True)
    stored_plan = work / 'changed/plan.json'
    saved_plan = stored_plan.read_bytes()
    modified_plan = json.loads(saved_plan)
    modified_plan['runtime']['node'] = 'changed'
    stored_plan.write_text(json.dumps(modified_plan))
    reject('changed', resume=True)
    stored_plan.write_bytes(saved_plan)
    saved_source = source.read_bytes()
    source.write_bytes(saved_source[:-1] + b'x')
    reject('changed', resume=True)
    source.write_bytes(saved_source)
    assert execute('changed', resume=True)['complete']
    checks.append('rule, code, stored plan and source fingerprint changes rejected')
    execute('locks', stop_after=0)
    child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])
    try:
        lock = work / 'locks/lock'
        lock.write_text(json.dumps({'pid': child.pid, 'host': r.HOST, 'token': 'test'}))
        reject('locks', resume=True)
        child.terminate()
        child.wait(timeout=10)
        assert execute('locks', resume=True)['complete']
    finally:
        if child.poll() is None:
            child.terminate()
            child.wait(timeout=10)
    checks.append('live owner refused; dead owner recovered')
    execute('subprocess', stop_after=0)
    child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])
    try:
        active = work / 'subprocess/logs/active.json'
        active.write_text(json.dumps({'pid': child.pid, 'host': r.HOST}))
        reject('subprocess', resume=True)
        child.terminate()
        child.wait(timeout=10)
        assert execute('subprocess', resume=True)['complete']
    finally:
        if child.poll() is None:
            child.terminate()
            child.wait(timeout=10)
    checks.append('surviving subprocess blocks retry until exited')
    execute('initialization', stop_after=0)
    (work / 'initialization/state.json').unlink()
    assert execute('initialization', resume=True)['complete']
    checks.append('empty initialization recovers only matching plan')
    # A process killed mid-step can leave an unfinished staging directory.
    execute('unfinished', stop_after=0)
    partial = work / 'unfinished/staging/inventory'
    partial.mkdir()
    (partial / 'incomplete').write_bytes(b'partial')
    assert execute('unfinished', resume=True)['complete']
    assert len(list((work / 'unfinished/abandoned').iterdir())) == 1
    checks.append('unreceipted staging is quarantined and retried')
    failed = {**spec, 'steps': [{'id': 'quality', 'op': 'quality', 'input': 'source', 'rules': 'quality'}]}
    recipe.write_text(json.dumps(failed))
    path.write_text(json.dumps({'start': '0', 'end': '110', 'rules': [{'topic': '/missing', 'minimum_messages': 1}]}))
    assert execute('quality-failed')['analysis_status'] == 'failed'
    checks.append('analysis failures remain completed reproducible artifacts')
    recipe.write_text(json.dumps({**spec, 'artifact_bytes': 1}))
    reject('budget')
    assert not list((work / 'budget/artifacts').iterdir())
    recipe.write_text(json.dumps({**spec, 'steps': [{'id': 'future', 'op': 'inventory', 'input': 'later'}]}))
    reject('cycle')
    assert not (work / 'cycle').exists()
    recipe.write_text(json.dumps({**spec, 'steps': [{'id': 'external', 'op': 'regression', 'baseline': 'source', 'candidate': 'source', 'rules': 'regression'}]}))
    (work / 'regression.json').write_text(json.dumps({'fields': [{'path': '/v'}], 'evidence': {'baseline_mcap': 'hidden', 'candidate_mcap': 'hidden'}}))
    reject('hidden')
    checks.append('publication budget, forward dependencies and hidden evidence files rejected')
    assert r.execute_recipe(ROOT / 'examples/recipes/clip.json', work / 'checked-in-example')['complete']
    checks.append('checked-in three-step recipe runs unchanged')
    result = {'result': 'passed', 'checks': checks, 'pipeline_steps': 6, 'table_rows': len(table), 'clip_messages': len(messages)}
    (ROOT / '_interop/recipes/report.json').write_text(json.dumps(result, indent=2) + '\n')
    print(f'Recipe workflows passed: {len(checks)} groups, six real processing steps')


if __name__ == '__main__':
    main()
