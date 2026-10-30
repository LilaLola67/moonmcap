"""Offline checks for public documentation links, Markdown anchors and release identity."""
import json
from pathlib import Path
import re
import subprocess
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]


def prose(text):
    return re.sub(r'^```[^\n]*\n.*?^```\s*$', '', text, flags=re.M | re.S)


def anchors(path):
    values, counts = set(), {}
    for heading in re.findall(r'^#{1,6}\s+(.+?)\s*#*$', prose(path.read_text(encoding='utf-8')), re.M):
        slug = re.sub(r'[^\w\s-]', '', heading.lower()).replace(' ', '-')
        n = counts.get(slug, 0)
        counts[slug] = n + 1
        values.add(slug if n == 0 else slug + '-' + str(n))
    return values


def main():
    files = subprocess.check_output(['git', 'ls-files', '-z'], cwd=ROOT).decode().split('\0')
    docs = [ROOT / name for name in files if name.endswith('.md') and not any(x in name for x in ('申报', '报名'))
            and (name == 'README.md' or name.startswith(('docs/', 'examples/', 'third_party/')))]
    checked = 0
    for path in docs:
        for raw in re.findall(r'(?<!!)\[[^\]\n]*\]\(([^)\n]+)\)', prose(path.read_text(encoding='utf-8'))):
            target = raw.strip().split(' ', 1)[0].strip('<>')
            parts = urlsplit(target)
            if parts.scheme or parts.netloc:
                continue
            assert not any(x in unquote(parts.path) for x in ('申报', '报名')), 'private document link: ' + str(path.relative_to(ROOT))
            resolved = (path.parent / unquote(parts.path)).resolve() if parts.path else path.resolve()
            assert resolved.is_relative_to(ROOT) and resolved.exists(), f'broken local link: {path.relative_to(ROOT)} -> {target}'
            if parts.fragment and resolved.suffix == '.md':
                assert unquote(parts.fragment) in anchors(resolved), f'broken anchor: {path.relative_to(ROOT)} -> {target}'
            checked += 1
    version = re.search(r'^version\s*=\s*"([^"]+)"', (ROOT / 'moon.mod').read_text(encoding='utf-8'), re.M)[1]
    assert f'library: "moonmcap/{version}"' in (ROOT / 'model.mbt').read_text(encoding='utf-8')
    assert f'moon add LilaLola67/moonmcap@{version}' in (ROOT / 'README.md').read_text(encoding='utf-8')
    output = ROOT / '_interop/docs'
    output.mkdir(parents=True, exist_ok=True)
    (output / 'report.json').write_text(json.dumps({'result': 'passed', 'documents': len(docs), 'local_links': checked, 'version': version, 'network_links_checked': False}, indent=2) + '\n')
    print(f'Documentation checks passed: {len(docs)} documents, {checked} local links/anchors, release {version}')


if __name__ == '__main__':
    main()
