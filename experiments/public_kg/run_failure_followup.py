"""One uniformly specified follow-up for each failed original suite attempt.

This is a supplemental reliability experiment, not a replacement of original runs.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
from pathlib import Path
import time

from dotenv import load_dotenv
from experiments.public_kg.artifacts import file_hash, write_json
from experiments.public_kg.run_pilot import run
from experiments.public_kg.run_short_suite import BASE, ONTOLOGY, RETRIEVAL, ROOT


def execute(original, destination):
    original, destination = Path(original), Path(destination)
    plan_path = original / 'suite_manifest.json'
    plan = json.loads(plan_path.read_text())
    if plan['status'] not in ('completed', 'completed_with_failures'):
        raise ValueError('original suite is unfinished')
    for name, digest in plan['source_sha256'].items():
        if file_hash(ROOT / 'experiments/public_kg' / name) != digest:
            raise ValueError(f'original inference source changed: {name}')
    for dataset, digest in plan['dataset_sha256'].items():
        if file_hash(BASE / dataset / 'sample.sqlite') != digest:
            raise ValueError(f'dataset hash changed: {dataset}')
        if file_hash(RETRIEVAL / dataset / 'candidates.json') != plan['retrieval_sha256'][dataset]:
            raise ValueError(f'retrieval hash changed: {dataset}')
    if file_hash(ONTOLOGY) != plan['ontology_sha256']:
        raise ValueError('ontology hash changed')
    failed = sorted((r for r in plan['runs'] if r['status'] != 'success'),
                    key=lambda r: (r['dataset'], r['method'], r['repeat']))
    destination.mkdir(parents=True, exist_ok=True)
    if any(destination.iterdir()):
        raise FileExistsError('follow-up output must be empty')
    refs = {f'{d}/{name}': file_hash(BASE / d / 'machine_review_v2' / name)
            for d in ('foshan_osm', 'geonames_cn')
            for name in ('mapping_reference.csv', 'relation_reference.json', 'manifest.json')}
    diagnostic = json.loads((original / 'diagnostics_manifest.json').read_text())
    if refs != diagnostic['reference_sha256']:
        raise ValueError('second-review reference files changed since original scoring')
    load_dotenv(ROOT / '.env')
    model = os.getenv('QWEN_CHAT_MODEL', 'qwen-plus')
    for item in failed:
        original_run = json.loads((Path(item['path']) / 'manifest.json').read_text())
        if original_run['model'] != model:
            raise ValueError('configured model differs from original run')
    manifest = {'status': 'running', 'original_manifest_sha256': file_hash(plan_path),
                'original_suite': str(original), 'limits': plan['limits'],
                'reference_sha256': refs, 'model': model, 'human_gold': False,
                'policy': 'Exactly one independent follow-up for every failed original run, '
                          'same settings and frozen inputs; retain all originals and all follow-ups; '
                          'no rerun of successful attempts and no best-of selection. '
                          'Original outcomes remain primary; follow-up reported separately.',
                'targets': failed, 'results': []}
    write_json(destination / 'manifest.json', manifest)
    start = time.monotonic()

    def task(item):
        dataset, method, repeat = item['dataset'], item['method'], item['repeat']
        out = destination / f'{dataset}_{method.replace(chr(47), chr(95))}_r{repeat}_retry1'
        limits = plan['limits']
        try:
            result = run(dataset, method, out, RETRIEVAL / dataset / 'candidates.json',
                         limits['token_budget'], limits['max_calls'], limits['timeout'],
                         limits['max_output_tokens'], BASE, ONTOLOGY)
            return {'dataset': dataset, 'method': method, 'repeat': repeat, 'path': str(out),
                    'status': result['status'], 'usage': result.get('usage')}
        except Exception as exc:
            return {'dataset': dataset, 'method': method, 'repeat': repeat, 'path': str(out),
                    'status': 'runner_error', 'exception_type': type(exc).__name__}

    with ThreadPoolExecutor(max_workers=3) as pool:
        for future in as_completed([pool.submit(task, item) for item in failed]):
            manifest['results'].append(future.result())
            write_json(destination / 'manifest.json', manifest)
            print(json.dumps(manifest['results'][-1]), flush=True)
    manifest.update(status='completed', elapsed_seconds=time.monotonic() - start,
                    source_hashes_unchanged=all(file_hash(ROOT / 'experiments/public_kg' / name) == digest
                                                for name, digest in plan['source_sha256'].items()),
                    reference_hashes_unchanged=all(file_hash(BASE / key.split('/')[0] /
                        'machine_review_v2' / key.split('/')[1]) == digest for key, digest in refs.items()))
    write_json(destination / 'manifest.json', manifest)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--original', default='data/eval/public_kg/short_plan_20261029')
    parser.add_argument('--out', required=True)
    args = parser.parse_args()
    execute(args.original, args.out)
