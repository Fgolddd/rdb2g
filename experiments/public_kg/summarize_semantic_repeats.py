"""Aggregate every fresh attempt without replacing missing scores by zero."""

import csv
import json
from pathlib import Path
from statistics import mean, stdev

from experiments.public_kg.artifacts import file_hash, write_json

ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / 'data/eval/public_kg/semantic_repeats_20261030'
DATASETS = ('foshan_osm', 'geonames_cn')
METHODS = ('Full', 'LLMatch', 'Magneto Base', 'LLM4VKG')


def collect():
    rows = []
    for dataset in DATASETS:
        for method in METHODS:
            for repeat in range(1, 4):
                if method == 'Full':
                    folder = BASE / 'full' / dataset / f'r{repeat}'
                    manifest_path = folder / 'manifest.json'
                    score_path = folder / 'machine_diagnostics.json'
                elif method == 'LLM4VKG':
                    folder = BASE / 'llm4vkg' / dataset / f'r{repeat}'
                    manifest_path = folder / 'attempt.json'
                    score_path = folder / 'field_mapping_v2/machine_diagnostics.json'
                else:
                    suite = 'external_timeout180_output32k' if method == 'LLMatch' else 'external_magneto_final'
                    folder = BASE / suite / ('llmatch' if method == 'LLMatch' else 'magneto-base') / dataset / f'r{repeat}'
                    manifest_path = folder / 'run_manifest.json'
                    score_path = folder / 'normalized/machine_diagnostics.json'
                if not manifest_path.is_file():
                    raise FileNotFoundError(f'Missing attempt: {manifest_path}')
                manifest = json.loads(manifest_path.read_text())
                score = json.loads(score_path.read_text()).get('mapping', {}) if score_path.exists() else {}
                row = {'dataset': dataset, 'method': method, 'repeat': repeat,
                       'status': manifest.get('status'), 'error': manifest.get('error'),
                       'attempt_manifest': str(manifest_path.relative_to(ROOT)),
                       'attempt_manifest_sha256': file_hash(manifest_path),
                       'diagnostic_path': str(score_path.relative_to(ROOT)) if score_path.exists() else None,
                       'diagnostic_sha256': file_hash(score_path) if score_path.exists() else None}
                for metric in ('precision', 'recall', 'f1', 'accuracy'):
                    row[metric] = score.get(metric) if score.get('status') == 'machine_reference_diagnostic_only' else None
                rows.append(row)
    groups = []
    for dataset in DATASETS:
        for method in METHODS:
            selected = [row for row in rows if row['dataset'] == dataset and row['method'] == method]
            group = {'dataset': dataset, 'method': method, 'attempts': len(selected),
                     'scored': sum(row['f1'] is not None for row in selected)}
            for metric in ('precision', 'recall', 'f1', 'accuracy'):
                values = [row[metric] for row in selected if row[metric] is not None]
                group[metric] = {'values': values, 'n': len(values),
                                 'mean': mean(values) if values else None,
                                 'sample_std': stdev(values) if len(values) > 1 else None}
            groups.append(group)
    payload = {'reference': 'second machine-reviewed reference; not human gold',
               'aggregation': 'arithmetic mean and sample standard deviation ddof=1',
               'failed_attempts_counted_as_zero': False,
               'selection': 'all three fresh scheduled attempts; no best-of selection',
               'runs': rows, 'groups': groups}
    write_json(BASE / 'summary.json', payload)
    with (BASE / 'per_run.csv').open('w', encoding='utf-8-sig', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    with (BASE / 'means.csv').open('w', encoding='utf-8-sig', newline='') as stream:
        writer = csv.writer(stream)
        writer.writerow(['dataset', 'method', 'scored', 'attempts', 'precision_mean', 'recall_mean', 'f1_mean', 'f1_sample_std'])
        for group in groups:
            writer.writerow([group['dataset'], group['method'], group['scored'], group['attempts'],
                             group['precision']['mean'], group['recall']['mean'],
                             group['f1']['mean'], group['f1']['sample_std']])
    print(json.dumps(groups, ensure_ascii=False, indent=2))
    return payload


if __name__ == '__main__':
    collect()
