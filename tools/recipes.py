"""Reproducible local MoonMCAP recipes with checked, resumable artifact publication."""
import argparse
from contextlib import contextmanager
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import re
import socket
import subprocess
import sys
import tempfile
import uuid

from artifact_common import atomic_json, sha256, strict_json

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / '_interop_deps'))
ID = re.compile(r'[a-z][a-z0-9_-]{0,47}')
OPS = {'normalize': 'ndjson', 'rewrite': 'mcap', 'sync': 'ndjson', 'tf-query': 'json', 'tf-clip': 'mcap', 'inventory': 'json',
       'clip': 'mcap', 'quality': 'json', 'events': 'json', 'columnar': 'parquet', 'regression': 'json'}
OWNER = 'MoonMCAP recipe job 1\n'
HOST = hashlib.sha256(socket.gethostname().encode()).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def hash_json(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def regular(path):
    if path.is_symlink() or not path.is_file():
        raise ValueError('regular artifact file required')
    return path


def owned(root, path):
    # Every recursive move is confined to this explicitly created job directory.
    root, path = Path(root).resolve(), Path(path)
    if not path.resolve().is_relative_to(root) or path.is_symlink():
        raise ValueError('recipe path escaped its job directory')
    return path


def replace_json(path, value):
    path = Path(path)
    if path.exists():
        regular(path)
    fd, temporary = tempfile.mkstemp(prefix='.' + path.name, suffix='.part', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(canonical(value) + b'\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        sync_directory(path.parent)
    finally:
        Path(temporary).unlink(missing_ok=True)


def sync_directory(path):
    if os.name != 'nt':
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def alive(pid):
    if type(pid) is not int or pid <= 0:
        raise ValueError('invalid recipe lock PID')
    if os.name == 'nt':
        import ctypes
        from ctypes import wintypes
        kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        kernel.OpenProcess.restype = wintypes.HANDLE
        handle = kernel.OpenProcess(0x1000, False, pid)
        if not handle:
            return ctypes.get_last_error() != 87
        try:
            code = wintypes.DWORD()
            if not kernel.GetExitCodeProcess(wintypes.HANDLE(handle), ctypes.byref(code)):
                return True
            return code.value == 259
        finally:
            kernel.CloseHandle(wintypes.HANDLE(handle))
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


@contextmanager
def locked(directory, resume):
    directory = Path(directory).absolute()
    if not resume:
        directory.mkdir()
        (directory / 'owner').write_text(OWNER)
    if directory.is_symlink() or not directory.is_dir() or regular(directory / 'owner').read_text() != OWNER:
        raise ValueError('recipe job ownership mismatch')
    lock = directory / 'lock'
    token = uuid.uuid4().hex
    def acquire():
        descriptor = os.open(lock, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, 'wb') as stream:
            stream.write(canonical({'pid': os.getpid(), 'host': HOST, 'token': token}))
            stream.flush()
            os.fsync(stream.fileno())
    try:
        acquire()
    except FileExistsError:
        guard = directory / 'recovery'
        guard.mkdir()
        try:
            previous = strict_json(regular(lock).read_bytes())
            if previous['host'] != HOST or alive(previous['pid']):
                raise ValueError('recipe job is owned by a running or remote process')
            lock.unlink()
            acquire()
        finally:
            guard.rmdir()
    try:
        for name in ('logs', 'staging', 'artifacts', 'abandoned'):
            path = owned(directory, directory / name)
            path.mkdir(exist_ok=True)
            if path.is_symlink() or not path.is_dir():
                raise ValueError('recipe storage directory changed')
        active = directory / 'logs/active.json'
        if active.exists():
            previous = strict_json(regular(active).read_bytes())
            if previous['host'] != HOST or alive(previous['pid']):
                raise ValueError('a previous recipe subprocess is still running; retry after it exits')
            active.unlink()
        yield directory
    finally:
        if lock.exists() and strict_json(regular(lock).read_bytes()).get('token') == token:
            lock.unlink()
            sync_directory(directory)


def run_command(job, arguments, accepted=(0,), *, cwd=ROOT):
    number = uuid.uuid4().hex
    stdout, stderr = job / f'logs/{number}.out', job / f'logs/{number}.err'
    active = job / 'logs/active.json'
    with stdout.open('wb') as out, stderr.open('wb') as err:
        process = subprocess.Popen(list(map(str, arguments)), cwd=cwd, stdout=out, stderr=err)
        try:
            replace_json(active, {'pid': process.pid, 'host': HOST})
            code = process.wait(timeout=3600)
        except BaseException:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
            raise
        finally:
            active.unlink(missing_ok=True)
    if code not in accepted:
        raise RuntimeError('recipe command failed: ' + stderr.read_text(encoding='utf-8', errors='replace')[-2000:])
    if stdout.stat().st_size > 4 * 1024 * 1024:
        raise ValueError('recipe command report exceeds 4 MiB')
    return stdout.read_text(encoding='utf-8')


def tree(path, maximum):
    entries, total = [], 0
    for item in sorted(path.rglob('*')):
        if item.is_symlink():
            raise ValueError('symlink in recipe output')
        if item.is_dir():
            continue
        regular(item)
        relative = item.relative_to(path).as_posix()
        if len(relative) > 256 or len(entries) >= 256:
            raise ValueError('recipe artifact file/name budget exceeded')
        total += item.stat().st_size
        if total > maximum:
            raise ValueError('recipe artifact publication byte budget exceeded')
        entries.append({'file': relative, 'bytes': item.stat().st_size, 'sha256': sha256(item)})
    if not entries:
        raise ValueError('empty recipe artifact')
    return {'tree_sha256': hash_json(entries), 'files': entries, 'bytes': total}


def save_state(job, state):
    envelope = {'state': state, 'sha256': hash_json(state)}
    if len(canonical(envelope)) > 8 * 1024 * 1024:
        raise ValueError('recipe checkpoint exceeds 8 MiB')
    replace_json(job / 'state.json', envelope)


def code_fingerprint():
    files = []
    for directory, names, filenames in os.walk(ROOT):
        names[:] = [name for name in names if not name.startswith(('.', '_'))]
        for name in filenames:
            path = Path(directory) / name
            if not name.startswith(('.', '_')) and (path.suffix in ('.mbt', '.py', '.js') or name in ('moon.mod', 'moon.pkg')):
                regular(path)
                files.append({'file': path.relative_to(ROOT).as_posix(), 'sha256': sha256(path)})
    return hash_json(sorted(files, key=lambda entry: entry['file']))


def execute_recipe(recipe_path, directory, *, resume=False, stop_after=None):
    recipe_path = Path(recipe_path).resolve()
    if recipe_path.stat().st_size > 1024 * 1024:
        raise ValueError('recipe exceeds 1 MiB')
    recipe = strict_json(recipe_path.read_bytes())
    if not isinstance(recipe, dict) or set(recipe) - {'version', 'inputs', 'rules', 'steps', 'artifact_bytes'} or type(recipe.get('version')) is not int or recipe['version'] != 1:
        raise ValueError('unsupported recipe format')
    maximum = recipe.get('artifact_bytes', 10 * 1024 * 1024 * 1024)
    if type(maximum) is not int or maximum < 1:
        raise ValueError('positive artifact byte budget required')
    if stop_after is not None and (type(stop_after) is not int or stop_after < 0):
        raise ValueError('nonnegative stop_after required')
    inputs, rules, steps = recipe['inputs'], recipe.get('rules', {}), recipe['steps']
    if not isinstance(inputs, dict) or not 1 <= len(inputs) <= 128 or not isinstance(rules, dict) or len(rules) > 128 or not isinstance(steps, list) or not 1 <= len(steps) <= 64:
        raise ValueError('recipe input/rule/step count exceeds limits')
    resources, rule_values, input_info, rule_info = {}, {}, {}, {}
    for key, spec in inputs.items():
        if not ID.fullmatch(key) or not isinstance(spec, dict) or set(spec) != {'path', 'kind'} or spec['kind'] not in ('mcap', 'file'):
            raise ValueError('invalid recipe input')
        path = regular((recipe_path.parent / spec['path']).resolve())
        resources[key] = path
        input_info[key] = {'kind': spec['kind'], 'bytes': path.stat().st_size, 'sha256': sha256(path)}
    for key, name in rules.items():
        if not ID.fullmatch(key) or not isinstance(name, str):
            raise ValueError('invalid rule resource')
        path = regular((recipe_path.parent / name).resolve())
        if path.stat().st_size > 1024 * 1024:
            raise ValueError('recipe rules exceed 1 MiB')
        value = strict_json(path.read_bytes())
        rule_values[key] = value
        rule_info[key] = {'sha256': sha256(path), 'canonical_sha256': hash_json(value)}
    seen = set(inputs)
    for step in steps:
        if not isinstance(step, dict) or set(step) - {'id', 'op', 'input', 'baseline', 'candidate', 'rules', 'bindings'} or not isinstance(step.get('id'), str) or not ID.fullmatch(step['id']) or step['id'] in seen or not isinstance(step.get('op'), str) or step['op'] not in OPS:
            raise ValueError('invalid/duplicate recipe step')
        required = ['baseline', 'candidate'] if step['op'] == 'regression' else ['input']
        if any(step.get(key) not in seen for key in required) or any(key in step for key in ({'input', 'baseline', 'candidate'} - set(required))):
            raise ValueError('recipe dependencies must refer to inputs or earlier steps')
        if step['op'] != 'inventory' and step.get('rules') not in rules:
            raise ValueError('step rules resource missing')
        bindings = step.get('bindings', {})
        allowed = {'/events/baseline', '/events/candidate', '/evidence/baseline_mcap', '/evidence/candidate_mcap'} if step['op'] == 'regression' else set()
        if not isinstance(bindings, dict) or set(bindings) - allowed or any(resource not in seen for resource in bindings.values()):
            raise ValueError('invalid recipe file binding')
        if step['op'] == 'regression':
            config = rule_values[step['rules']]
            expected = ({'/events/baseline', '/events/candidate'} if 'events' in config else set()) | ({'/evidence/baseline_mcap', '/evidence/candidate_mcap'} if 'evidence' in config else set())
            if set(bindings) != expected:
                raise ValueError('every regression external file must be bound to a declared resource')
        seen.add(step['id'])
    with locked(directory, resume) as job:
        moon = os.environ.get('MOON', 'moon')
        run_command(job, [moon, 'build', '--target', 'js', '--warn-list', '-a'])
        identity = run_command(job, [moon, 'version', '--all'])
        compiler = re.search(r'moonc v([^\s]+)', identity)
        if compiler is None:
            raise ValueError('compiler identity unavailable')
        inventory_cli = ROOT / '_build/js/debug/build/cmd/payload/payload.js'
        for key, info in input_info.items():
            if info['kind'] == 'mcap':
                inventory = job / f'logs/inventory-{uuid.uuid4().hex}.json'
                run_command(job, ['node', inventory_cli, 'inventory', resources[key], inventory])
                value = strict_json(inventory.read_bytes())
                if value['source_sha256'] != info['sha256']:
                    raise ValueError('recipe input changed during inventory')
                info['schema_sha256'] = value['schema_sha256']
                info['schemas'] = value['types']['schemas']
        versions = {name: importlib.metadata.version(name) for name in ('pyarrow',) if any(step['op'] == 'columnar' for step in steps)}
        runtime = {'moonc': compiler[1], 'python': platform.python_version(), 'node': run_command(job, ['node', '--version']).strip(), 'adapters': versions}
        plan = {'format': 'moonmcap.recipe.1', 'recipe_sha256': sha256(recipe_path), 'inputs': input_info, 'rules': rule_info, 'steps': steps,
                'code_sha256': code_fingerprint(), 'executables': {name: sha256(ROOT / f'_build/js/debug/build/cmd/{name}/{name}.js') for name in ('payload', 'incident')}, 'runtime': runtime, 'artifact_bytes': maximum}
        plan_sha = hash_json(plan)
        if (job / 'state.json').exists():
            if not resume:
                raise ValueError('recipe state already exists')
            if strict_json(regular(job / 'plan.json').read_bytes()) != plan:
                raise ValueError('stored recipe plan fingerprint mismatch')
            data = regular(job / 'state.json')
            if data.stat().st_size > 8 * 1024 * 1024:
                raise ValueError('recipe checkpoint exceeds 8 MiB')
            envelope = strict_json(data.read_bytes())
            state = envelope['state']
            if hash_json(state) != envelope['sha256'] or state['plan_sha256'] != plan_sha:
                raise ValueError('recipe checkpoint/config/input/code fingerprint mismatch')
        else:
            if resume and (job / 'plan.json').exists():
                if strict_json(regular(job / 'plan.json').read_bytes()) != plan:
                    raise ValueError('recipe initialization fingerprint mismatch')
                if any(any((job / name).iterdir()) for name in ('staging', 'artifacts', 'abandoned')):
                    raise ValueError('recipe state missing beside existing artifacts')
            else:
                atomic_json(job / 'plan.json', plan)
            state = {'plan_sha256': plan_sha, 'completed': [], 'pending': None}
            save_state(job, state)
        if len(state['completed']) > len(steps):
            raise ValueError('recipe completed prefix exceeds plan')
        def verify_inputs():
            if any(sha256(resources[key]) != info['sha256'] for key, info in input_info.items()):
                raise ValueError('recipe input changed during execution')
            if any(sha256((recipe_path.parent / rules[key]).resolve()) != info['sha256'] for key, info in rule_info.items()) or sha256(recipe_path) != plan['recipe_sha256']:
                raise ValueError('recipe/rules changed during execution')
            if code_fingerprint() != plan['code_sha256']:
                raise ValueError('recipe code changed during execution')
            if any(sha256(ROOT / f'_build/js/debug/build/cmd/{name}/{name}.js') != digest for name, digest in plan['executables'].items()):
                raise ValueError('recipe executable changed during execution')
        def main_path(step):
            if step['op'] == 'regression':
                return 'comparison/report.json'
            suffix = OPS[step['op']]
            if step['op'] == 'columnar' and rule_values[step['rules']].get('format') == 'arrow':
                suffix = 'arrow'
            return 'output.' + suffix
        def register(receipt):
            index = len(state['completed'])
            if index >= len(steps) or receipt['id'] != steps[index]['id'] or receipt['main'] != main_path(steps[index]):
                raise ValueError('recipe artifact order/main mismatch')
            resources[receipt['id']] = job / 'artifacts' / receipt['id'] / receipt['main']
        for index, receipt in enumerate(state['completed']):
            if receipt['id'] != steps[index]['id'] or receipt['main'] != main_path(steps[index]) or tree(owned(job, job / 'artifacts' / receipt['id']), maximum) != receipt['tree']:
                raise ValueError('completed recipe artifact was modified')
            resources[receipt['id']] = regular(owned(job, job / 'artifacts' / receipt['id'] / receipt['main']))
        pending = state['pending']
        if pending is not None:
            register(pending)
            staged, final = owned(job, job / 'staging' / pending['id']), owned(job, job / 'artifacts' / pending['id'])
            existing = staged if staged.exists() else final
            if (staged.exists() and final.exists()) or not existing.exists() or tree(existing, maximum) != pending['tree']:
                raise ValueError('pending recipe receipt/artifact mismatch')
            regular(owned(job, existing / pending['main']))
            if staged.exists():
                os.rename(staged, final)
                sync_directory(final.parent)
            state['completed'].append(pending)
            state['pending'] = None
            save_state(job, state)
        added = 0
        for step in steps[len(state['completed']):]:
            if stop_after is not None and added >= stop_after:
                break
            verify_inputs()
            staged, final = owned(job, job / 'staging' / step['id']), owned(job, job / 'artifacts' / step['id'])
            if final.exists():
                raise ValueError('unreceipted published recipe artifact')
            if staged.exists():
                destination = owned(job, job / 'abandoned' / (step['id'] + '-' + uuid.uuid4().hex))
                os.rename(staged, destination)
            staged.mkdir()
            target = staged / main_path(step)
            config = None
            if step['op'] != 'inventory':
                config = strict_json(canonical(rule_values[step['rules']]))
                for pointer, resource in step.get('bindings', {}).items():
                    parent, key = pointer.strip('/').split('/')
                    config[parent][key] = str(resources[resource])
                rule_file = job / f'logs/rules-{step["id"]}-{uuid.uuid4().hex}.json'
                atomic_json(rule_file, config)
            op = step['op']
            if op in ('normalize', 'rewrite', 'sync', 'tf-query', 'tf-clip', 'inventory'):
                arguments = ['node', inventory_cli, op, resources[step['input']], *([] if op == 'inventory' else [rule_file]), target]
            elif op in ('clip', 'quality', 'events'):
                arguments = ['node', ROOT / '_build/js/debug/build/cmd/incident/incident.js', op, resources[step['input']], rule_file, *([target] if op == 'clip' else [])]
            elif op == 'columnar':
                arguments = [sys.executable, ROOT / 'tools/columnar.py', resources[step['input']], rule_file, target]
            else:
                arguments = [sys.executable, ROOT / 'tools/regression.py', resources[step['baseline']], resources[step['candidate']], rule_file, staged / 'comparison']
            response = run_command(job, arguments, accepted=(0, 1) if op in ('regression', 'quality') else (0,))
            if op in ('quality', 'events'):
                atomic_json(target, strict_json(response))
            verify_inputs()
            result = strict_json(target.read_bytes()) if op in ('quality', 'regression') else None
            analysis_failed = (op == 'regression' and result['status'] == 'failed') or (op == 'quality' and (result.get('quality', {}).get('status') == 'failed' or any(pair.get('status') == 'failed' for pair in result.get('pairs', []))))
            receipt = {'id': step['id'], 'main': main_path(step), 'tree': tree(staged, maximum), 'analysis_failed': analysis_failed}
            register(receipt)
            state['pending'] = receipt
            save_state(job, state)  # Durable receipt precedes publication.
            os.rename(staged, final)
            sync_directory(final.parent)
            state['completed'].append(receipt)
            state['pending'] = None
            save_state(job, state)
            added += 1
        complete = len(state['completed']) == len(steps)
        failed = any(row['analysis_failed'] for row in state['completed'])
        return {'complete': complete, 'completed': len(state['completed']), 'steps': len(steps), 'plan_sha256': plan_sha,
                'analysis_status': 'failed' if failed else ('passed' if complete else 'pending'), 'artifacts': state['completed']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('recipe')
    parser.add_argument('job_directory')
    parser.add_argument('--resume', action='store_true')
    parser.add_argument('--stop-after', type=int)
    args = parser.parse_args()
    try:
        report = execute_recipe(args.recipe, args.job_directory, resume=args.resume, stop_after=args.stop_after)
        print(json.dumps(report))
        return 1 if report['complete'] and report['analysis_status'] == 'failed' else 0
    except (ValueError, KeyError, TypeError, OSError, RuntimeError, subprocess.SubprocessError) as error:
        parser.exit(2, str(error) + '\n')


if __name__ == '__main__':
    raise SystemExit(main())
