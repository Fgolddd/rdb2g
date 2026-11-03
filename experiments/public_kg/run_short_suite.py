"""Execute the fixed short public experiment; retain every attempt."""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from pathlib import Path
import time

from experiments.public_kg.artifacts import file_hash, write_json
from experiments.public_kg.run_pilot import run

ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / 'data/public/benchmark_v2'
ONTOLOGY = BASE / 'ontology/target_terms.json'
RETRIEVAL = ROOT / 'data/eval/public_kg/retrieval_v2_reviewed_retry'


def execute(destination, repeats=3, workers=4):
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    if any(destination.iterdir()):
        raise FileExistsError('Suite output must be empty; all earlier results are preserved')
    methods = ['full', 'single_agent', 'w/oValidator', 'without_probe']
    datasets = ['foshan_osm', 'geonames_cn']
    specs = [(dataset, method, repeat) for repeat in range(1, repeats + 1)
             for dataset in datasets for method in methods]
    code_hashes = {name: file_hash(ROOT / 'experiments/public_kg' / name)
                   for name in ['methods.py', 'run_pilot.py', 'artifacts.py', 'run_short_suite.py']}
    references = {
        'foshan_mapping': BASE / 'foshan_osm/machine_review/mapping_reference.csv',
        'foshan_relations': BASE / 'foshan_osm/machine_review/relation_reference.json',
        'geonames_mapping': BASE / 'geonames_cn/annotation/machine_mapping_reference.csv',
        'geonames_relations': BASE / 'geonames_cn/annotation/machine_relation_reference.json',
    }
    plan = {'status': 'running', 'evaluation_status': 'machine_reference_only_no_human_gold',
            'ontology_sha256': file_hash(ONTOLOGY), 'source_sha256': code_hashes,
            'dataset_sha256': {dataset: file_hash(BASE / dataset / 'sample.sqlite') for dataset in datasets},
            'machine_reference_sha256': {name: file_hash(path) for name, path in references.items()},
            'retrieval_sha256': {dataset: file_hash(RETRIEVAL / dataset / 'candidates.json') for dataset in datasets},
            'methods': methods, 'repeats': repeats, 'workers': workers,
            'limits': {'max_calls': 9, 'token_budget': 1000000, 'timeout': 120, 'max_output_tokens': 24000},
            'selection_policy': 'retain all attempts; no best-of selection; failed replicates not silently rerun',
            'replicate_policy': 'independent requests; temperature=0; provider seed not applied',
            'scope': 'public-adapted extension; two datasets reused for development, not untouched holdout'}
    write_json(destination / 'suite_manifest.json', plan)
    started = time.monotonic()

    def task(spec):
        dataset, method, repeat = spec
        output = destination / f'{dataset}_{method.replace(chr(47), chr(95))}_r{repeat}'
        try:
            current = {name: file_hash(ROOT / 'experiments/public_kg' / name) for name in code_hashes}
            if current != code_hashes:
                raise RuntimeError('Inference source changed after suite freeze')
            result = run(dataset, method, output, RETRIEVAL / dataset / 'candidates.json',
                         1000000, 9, 120, 24000, BASE, ONTOLOGY)
            return {'dataset': dataset, 'method': method, 'repeat': repeat,
                    'path': str(output), 'status': result['status'], 'usage': result.get('usage')}
        except Exception as exc:
            return {'dataset': dataset, 'method': method, 'repeat': repeat, 'path': str(output),
                    'status': 'suite_task_error', 'exception_type': type(exc).__name__}

    results = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(task, spec): spec for spec in specs}
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            write_json(destination / 'progress.json', {'completed': len(results), 'total': len(specs), 'runs': results})
            print(json.dumps(result), flush=True)
    plan.update(status='completed' if all(item['status'] == 'success' for item in results) else 'completed_with_failures',
                runs=results, elapsed_seconds=time.monotonic() - started)
    plan['source_hashes_unchanged'] = all(file_hash(ROOT / 'experiments/public_kg' / name) == digest
                                         for name, digest in code_hashes.items())
    plan['machine_reference_hashes_unchanged'] = all(
        file_hash(path) == plan['machine_reference_sha256'][name] for name, path in references.items())
    write_json(destination / 'suite_manifest.json', plan)
    return plan


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', required=True, type=Path)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--workers', type=int, default=4)
    args = parser.parse_args()
    if args.repeats < 1 or not 1 <= args.workers <= 4:
        parser.error('repeats must be positive and workers must be 1..4')
    execute(args.out, args.repeats, args.workers)
