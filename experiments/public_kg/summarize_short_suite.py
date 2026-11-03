"""Summarize every attempt in the fixed suite without selecting the best run."""

import argparse
from collections import defaultdict
import json
from pathlib import Path
from statistics import mean, stdev

from experiments.public_kg.artifacts import write_json


def summarize(root):
    root = Path(root)
    plan = json.loads((root / 'suite_manifest.json').read_text())
    groups = defaultdict(list)
    for run in plan.get('runs', []):
        groups[(run['dataset'], run['method'])].append(run)
    summary = []
    for (dataset, method), runs in sorted(groups.items()):
        metrics = defaultdict(list)
        statuses = defaultdict(int)
        for run in runs:
            statuses[run['status']] += 1
            directory = Path(run['path'])
            diagnostic = directory / 'machine_diagnostics.json'
            if diagnostic.exists() and run['status'] == 'success':
                data = json.loads(diagnostic.read_text())
                for name, value in [('mapping_f1', data.get('mapping', {}).get('f1')),
                                    ('relation_f1', data.get('relations', {}).get('f1'))]:
                    if value is not None:
                        metrics[name].append(value)
        values = {name: {'n': len(numbers), 'mean': mean(numbers),
                         'sample_std': stdev(numbers) if len(numbers) > 1 else None,
                         'values': numbers} for name, numbers in metrics.items()}
        summary.append({'dataset': dataset, 'method': method, 'attempts': len(runs),
                        'statuses': dict(statuses), 'metrics': values})
    result = {'status': 'machine_reference_diagnostic_only', 'human_gold': False,
              'note': 'Only successful runs with available diagnostics are summarized; failed attempts are counted.',
              'groups': summary}
    write_json(root / 'summary.json', result)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    summarize(parser.parse_args().root)
