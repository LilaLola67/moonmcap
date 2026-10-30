"""Replay the exact documented commands and README API example in a clean indexed source export."""
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import tarfile
import uuid

ROOT = Path(__file__).resolve().parents[1]


def main():
    work = ROOT / '_interop/quickstart' / uuid.uuid4().hex
    work.mkdir(parents=True)
    exported = work / 'clean source 目录'
    exported.mkdir()
    # Git index is the candidate under review, including newly staged documentation.
    tree = subprocess.check_output(['git', 'write-tree'], cwd=ROOT).decode().strip()
    archive = subprocess.check_output(['git', 'archive', tree], cwd=ROOT)
    with tarfile.open(fileobj=io.BytesIO(archive)) as source:
        for member in source.getmembers():
            path = (exported / member.name).resolve()
            assert path.is_relative_to(exported.resolve()) and not member.issym() and not member.islnk()
            assert not any(part in ('.git', '.mooncakes', '_build', '_interop', '_interop_deps') for part in Path(member.name).parts)
        source.extractall(exported, filter='data')
    assert not (exported / '_build').exists() and not (exported / '.mooncakes').exists()
    doc = (exported / 'docs/quickstart.md').read_text(encoding='utf-8')
    commands = [shlex.split(line) for block in re.findall(r'```sh verify:quickstart\n(.*?)\n```', doc, re.S) for line in block.splitlines() if line.strip()]
    assert len(commands) == 9 and all(args[0] in ('moon', 'node') for args in commands)
    for record in json.loads((exported / 'examples/manifest.json').read_text())['files']:
        data = (exported / 'examples' / record['path']).read_bytes()
        assert len(data) == record['bytes'] and hashlib.sha256(data).hexdigest() == record['sha256']
    moon = os.environ.get('MOON', 'moon')
    def run(args, cwd=exported, code=0):
        actual = [moon, *args[1:]] if args[0] == 'moon' else args
        p = subprocess.run(actual, cwd=cwd, capture_output=True, text=True, encoding='utf-8', errors='replace', timeout=300)
        assert p.returncode == code, (args, p.returncode, p.stdout[-1000:], p.stderr[-2000:])
        return p
    results = [run(args) for args in commands]
    query = json.loads(results[3].stdout)
    assert [m['log_time'] for m in query['messages']] == [str(t * 10**9) for t in (21, 23, 25, 27, 29)]
    assert query['diagnostics']['full_validation']
    sliced = json.loads(results[4].stdout)
    assert (sliced['messages'], sliced['attachments'], sliced['metadata']) == (5, 1, 1)
    assert json.loads(results[5].stdout)['valid']
    recovery = json.loads(results[6].stderr)
    assert recovery['recovered_messages'] == 48 and recovery['stop_offset'] == 3404 and not recovery['complete']
    recovered = json.loads(results[7].stdout)
    assert (recovered['messages'], recovered['attachments'], recovered['metadata']) == (48, 0, 0)
    # The last command verifies the recovered container too.
    assert json.loads(results[8].stdout)['valid']
    preserved = hashlib.sha256((exported / 'quickstart-slice.mcap').read_bytes()).hexdigest()
    run(commands[4], code=2)
    assert hashlib.sha256((exported / 'quickstart-slice.mcap').read_bytes()).hexdigest() == preserved
    readme = (exported / 'README.md').read_text(encoding='utf-8')
    example = re.findall(r'```moonbit\n(.*?)\n```', readme, re.S)[0]
    consumer = work / 'consumer'
    consumer.mkdir()
    version = re.search(r'^version\s*=\s*"([^"]+)"', (exported / 'moon.mod').read_text(encoding='utf-8'), re.M)[1]
    (consumer / 'moon.mod').write_text(f'name = "local/readme-example"\nversion = "0.0.1"\nimport {{ "LilaLola67/moonmcap@{version}", }}\n')
    (work / 'moon.work').write_text('members = ["clean source 目录", "consumer"]\n', encoding='utf-8')
    (consumer / 'moon.pkg').write_text('import { "LilaLola67/moonmcap" @mcap, }\npkgtype(kind:"executable")\n')
    (consumer / 'main.mbt').write_text(example, encoding='utf-8')
    for command in (['moon', 'run', '.'], ['moon', 'run', '.', '--target', 'js']):
        p = run(command, cwd=consumer)
        assert 'MCAP_SAMPLE_OK' in p.stdout
    report = {'result': 'passed', 'documented_commands': len(commands), 'fixture_sha256_checked': True,
              'workspace_build_artifacts_used': False, 'readme_api_example': 'Wasm and JS compiled and executed against exported source dependency',
              'source_tree': tree, 'fresh_published_install': False, 'checks': ['five exact timestamps', 'slice auxiliary records', 'recovery boundary', 'existing output preserved']}
    (ROOT / '_interop/quickstart/report.json').write_text(json.dumps(report, indent=2) + '\n')
    print('Clean quickstart passed: exact documented commands, README API and immutable fixtures')


if __name__ == '__main__':
    main()
