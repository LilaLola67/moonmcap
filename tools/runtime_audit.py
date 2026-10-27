"""Audit portable runtime provenance and package imports without consulting caches."""
import hashlib,json,re
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]


def main():
    manifest=(ROOT/'moon.mod').read_text(encoding='utf-8')
    assert 'bikallem/compress@' not in manifest and 'bikallem/blit@' not in manifest
    imports=[]
    for path in ROOT.rglob('moon.pkg'):
        if any(part.startswith('.') or part.startswith('_') for part in path.relative_to(ROOT).parts):continue
        text=path.read_text(encoding='utf-8')
        assert 'bikallem/compress/' not in text and '"bikallem/blit"' not in text,path
        imports.append(str(path.relative_to(ROOT)).replace('\\','/'))
    rows=json.loads((ROOT/'third_party/runtime-sources.json').read_text(encoding='utf-8'))
    assert len(rows)==10 and len({row['file'] for row in rows})==10
    for row in rows:
        name=row['file'];assert name.startswith(('internal/lz4/','internal/checksum/','internal/blit/')) and '..' not in Path(name).parts
        text=(ROOT/name).read_text(encoding='utf-8')
        assert text.startswith('// Derived from ') and 'Apache-2.0' in text.splitlines()[0]
        assert 'third_party/README.md' in text.splitlines()[1] and row['license']=='Apache-2.0'
        assert re.fullmatch('[0-9a-f]{64}',row['upstream_sha256'])
    assert (ROOT/'third_party/Apache-2.0.txt').is_file()
    work=ROOT/'_interop/runtime';work.mkdir(parents=True,exist_ok=True)
    report={'result':'passed','adapted_files':len(rows),'package_manifests_audited':len(imports),'legacy_compression_dependencies':False,'license':'Apache-2.0','sources_manifest_sha256':hashlib.sha256((ROOT/'third_party/runtime-sources.json').read_bytes()).hexdigest(),'original_core_line_count_includes_internal_runtime':False}
    (work/'report.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    print('Runtime audit passed: 10 attributed source files, modern package imports, no legacy compression dependency')


if __name__=='__main__':main()
