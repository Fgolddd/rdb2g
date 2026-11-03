"""Run fixed machine-reference diagnostics on all successful attempts."""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from pathlib import Path
import subprocess
import sys

from experiments.public_kg.artifacts import file_hash, write_json


def execute(root, workers):
    root = Path(root)
    plan = json.loads((root / 'suite_manifest.json').read_text())
    if plan['status'] not in ('completed', 'completed_with_failures'):
        raise ValueError('suite must finish before diagnostics')
    refs = Path('data/public/benchmark_v2')
    scoring_hash = file_hash(Path('experiments/public_kg/machine_diagnostics.py'))
    frozen = {f'{dataset}/{name}': file_hash(refs / dataset / 'machine_review_v2' / name)
              for dataset in ('foshan_osm', 'geonames_cn')
              for name in ('mapping_reference.csv', 'relation_reference.json', 'manifest.json')}
    write_json(root / 'diagnostics_manifest.json', {
        'status': 'running', 'human_gold': False, 'reference_version': 'second_machine_review',
        'reference_sha256': frozen, 'scoring_source_sha256': scoring_hash,
        'note': 'References are machine-reviewed, not independently blinded or human gold.'})

    def score(run):
        path = Path(run['path'])
        command = [sys.executable, '-m', 'experiments.public_kg.machine_diagnostics', '--run', str(path),
                   '--reference-version', 'second']
        proc = subprocess.run(command, capture_output=True, text=True)
        return {'path': str(path), 'dataset': run['dataset'], 'method': run['method'],
                'status': 'success' if proc.returncode == 0 else 'error',
                'error': proc.stderr[-2000:] if proc.returncode else None}

    runs = [run for run in plan['runs'] if run['status'] == 'success']
    output = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for future in as_completed([executor.submit(score, run) for run in runs]):
            result = future.result()
            output.append(result)
            print(json.dumps(result), flush=True)
    unchanged = all(file_hash(refs / key.split('/')[0] / 'machine_review_v2' / key.split('/')[1]) == value
                    for key, value in frozen.items())
    write_json(root / 'diagnostics_manifest.json', {
        'status': 'completed' if all(r['status'] == 'success' for r in output) else 'completed_with_failures',
        'human_gold': False, 'reference_version': 'second_machine_review', 'reference_sha256': frozen,
        'scoring_source_sha256': scoring_hash,
        'scoring_source_unchanged': file_hash(Path('experiments/public_kg/machine_diagnostics.py')) == scoring_hash,
        'reference_hashes_unchanged': unchanged, 'results': output,
        'note': 'References are machine-reviewed, not independently blinded or human gold.'})


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('--workers', type=int, default=2, choices=range(1, 5))
    args = parser.parse_args()
    execute(args.root, args.workers)
