"""Reorganize retained experiments around mapping, relations and Foshan queries."""

import argparse
import csv
import json
from pathlib import Path
from statistics import mean, stdev

from experiments.public_kg.artifacts import file_hash, write_json

ROOT = Path(__file__).resolve().parents[2]


def stats(values):
    return {'n': len(values), 'mean': mean(values) if values else None,
            'sample_std': stdev(values) if len(values) > 1 else None, 'values': values}


def build(destination):
    destination = Path(destination)
    base = ROOT / 'data/eval/public_kg'
    original = base / 'short_plan_20261029'
    plan = json.loads((original / 'suite_manifest.json').read_text())
    records, sources = [], {}

    def read(path):
        path = Path(path)
        sources[str(path.relative_to(ROOT))] = file_hash(path)
        return json.loads(path.read_text())

    def record(dataset, method, component, paths, attempts, scope):
        rows = [read(path / 'machine_diagnostics.json') for path in paths]
        key = 'mapping' if component == 'semantic_mapping' else 'relations'
        result = {'dataset': dataset, 'method': method, 'component': component,
                  'attempts': attempts, 'successful_runs': len(paths), 'scope': scope}
        for metric in ('precision', 'recall', 'f1'):
            result[metric] = stats([row[key][metric] for row in rows if metric in row.get(key, {})])
        if component == 'semantic_mapping':
            result['assessed_fields'] = sorted({row['mapping']['items'] for row in rows})
        else:
            result['unassessed_predictions'] = [row['relations']['unassessed_predictions'] for row in rows]
        records.append(result)

    for dataset in plan['dataset_sha256']:
        for method in plan['methods']:
            all_runs = [r for r in plan['runs'] if r['dataset'] == dataset and r['method'] == method]
            paths = [ROOT / r['path'] for r in all_runs if r['status'] == 'success']
            for component in ('semantic_mapping', 'implicit_relations'):
                record(dataset, method, component, paths, len(all_runs), 'public-adapted joint inference output')
        for method in ('llmatch', 'magneto'):
            directory = base / 'external_setup_v2/normalized' / f'{method}_{dataset}'
            record(dataset, method, 'semantic_mapping', [directory], 1, 'upstream field component')
    llm4vkg = base / 'short_plan_external/llm4vkg_geonames/field_mapping_v2'
    record('geonames_cn', 'llm4vkg', 'semantic_mapping', [llm4vkg], 1, 'exact URI literal-field projection')
    relations = base / 'relation_components_20261029'
    if relations.exists():
        for path in sorted(relations.glob('*/*/manifest.json')):
            manifest = read(path)
            if (path.parent / 'machine_diagnostics.json').is_file():
                record(manifest['dataset'], manifest['method'], 'implicit_relations', [path.parent], 1,
                       'simple baseline; shared table classes from Full r1, not paper reproduction')
    end_to_end = []
    for run in sorted(plan['runs'], key=lambda r: (r['method'], r['repeat'])):
        if run['dataset'] != 'foshan_osm':
            continue
        path = ROOT / run['path']
        manifest = read(path / 'manifest.json')
        entry = {'method': run['method'], 'repeat': run['repeat'], 'status': run['status'],
                 'elapsed_seconds': manifest.get('elapsed_seconds'), 'usage': manifest.get('usage'),
                 'graph': manifest.get('graph'), 'query_exact_matches': None, 'query_count': None}
        query = base / 'short_plan_queries/foshan_osm' / path.name / 'diagnostic_results.json'
        if query.exists():
            answer = read(query)
            entry.update(query_exact_matches=answer['exact_matches'], query_count=answer['query_count'])
        end_to_end.append(entry)
    result = {'scope': {'semantic_mapping': ['foshan_osm', 'geonames_cn'],
                        'implicit_relations': ['foshan_osm', 'geonames_cn'],
                        'end_to_end': ['foshan_osm']},
              'reference': 'frozen second machine review accepted by user; not independent human gold',
              'original_suite_sha256': file_hash(original / 'suite_manifest.json'),
              'selection': 'all original attempts; follow-ups remain separate',
              'component_results': records, 'foshan_end_to_end': end_to_end, 'source_sha256': sources}
    write_json(destination / 'results.json', result)
    with (destination / 'component_results.csv').open('w', newline='', encoding='utf-8-sig') as stream:
        writer = csv.writer(stream)
        writer.writerow(['dataset', 'component', 'method', 'attempts', 'successful_runs',
                         'precision_mean', 'recall_mean', 'f1_mean', 'f1_sample_std', 'scope'])
        for row in records:
            writer.writerow([row['dataset'], row['component'], row['method'], row['attempts'],
                             row['successful_runs'], row['precision']['mean'], row['recall']['mean'],
                             row['f1']['mean'], row['f1']['sample_std'], row['scope']])
    print(json.dumps({'component_rows': len(records), 'foshan_attempts': len(end_to_end),
                      'output': str(destination)}, ensure_ascii=False))
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path, default=ROOT / 'data/eval/public_kg/component_report_20261029')
    build(parser.parse_args().out)
