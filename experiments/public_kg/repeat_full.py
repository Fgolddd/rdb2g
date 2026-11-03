"""Execute three fresh Full replicates on each frozen public database."""

import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from dotenv import load_dotenv

from experiments.public_kg.artifacts import file_hash, write_json
from experiments.public_kg.run_pilot import run

ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / 'data/public/benchmark_v2'
OUT = ROOT / 'data/eval/public_kg/semantic_repeats_20261030/full'
RETRIEVAL = ROOT / 'data/eval/public_kg/retrieval_v2_reviewed_retry'


def main():
    load_dotenv(ROOT / '.env')
    OUT.mkdir(parents=True, exist_ok=True)
    if any(OUT.iterdir()):
        raise FileExistsError('Refusing to overwrite an existing experiment')
    paths = [BASE / 'ontology/target_terms.json']
    for dataset in ('foshan_osm', 'geonames_cn'):
        paths.extend([BASE / dataset / 'sample.sqlite',
                      BASE / dataset / 'machine_review_v2/mapping_reference.csv',
                      RETRIEVAL / dataset / 'candidates.json'])
    frozen = {str(path.relative_to(ROOT)): file_hash(path) for path in paths}
    plan = {'model': os.getenv('QWEN_CHAT_MODEL', 'qwen-plus'),
            'repeats_per_dataset': 3, 'workers': 2, 'frozen_inputs': frozen,
            'selection_policy': 'all independent attempts retained; no best-of selection',
            'reference_status': 'second machine review; not human gold',
            'temperature': 0, 'provider_seed_applied': False,
            'limits': {'max_calls': 9, 'token_budget': 1000000,
                       'timeout': 120, 'max_output_tokens': 24000}}
    write_json(OUT / 'suite_manifest.json', {**plan, 'status': 'running'})

    def task(dataset, repeat):
        output = OUT / dataset / f'r{repeat}'
        try:
            for relative, digest in frozen.items():
                if file_hash(ROOT / relative) != digest:
                    raise ValueError(f'Frozen input changed: {relative}')
            result = run(dataset, 'full', output, RETRIEVAL / dataset / 'candidates.json',
                         1000000, 9, 120, 24000, BASE, BASE / 'ontology/target_terms.json')
            return {'dataset': dataset, 'repeat': repeat, 'path': str(output),
                    'status': result['status'], 'usage': result.get('usage')}
        except Exception as exc:
            return {'dataset': dataset, 'repeat': repeat, 'path': str(output),
                    'status': 'task_error', 'error_type': type(exc).__name__,
                    'error': str(exc)[:400]}

    results = []
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(task, dataset, repeat)
                   for repeat in range(1, 4) for dataset in ('foshan_osm', 'geonames_cn')]
        for future in as_completed(futures):
            item = future.result()
            results.append(item)
            write_json(OUT / 'progress.json', {'runs': results, 'completed': len(results), 'total': 6})
            print(json.dumps(item), flush=True)
    write_json(OUT / 'suite_manifest.json', {**plan, 'runs': results,
        'status': 'completed' if all(r['status'] == 'success' for r in results)
                  else 'completed_with_failures'})


if __name__ == '__main__':
    main()
