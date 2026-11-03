"""Deterministic JOINT-adapted relation baseline; not an official reproduction."""
from __future__ import annotations
import argparse, heapq, json, math, re, sqlite3, time
from collections import defaultdict
from difflib import SequenceMatcher
from pathlib import Path
from urllib.parse import quote
from rdflib import Graph, URIRef
from experiments.public_kg.artifacts import file_hash, materialize, write_json
from experiments.public_kg.methods import schema_profile
from experiments.public_kg.relation_component_baselines import _class_hierarchy, _predicate_for_classes, _terms

PAPER = "https://arxiv.org/abs/2509.07230"
DATASETS = ("foshan_osm", "geonames_cn")
BENCHMARK = Path("data/public/benchmark_v2")
CONTEXT = Path("data/eval/public_kg/short_plan_20261029")
OUTPUT = Path("data/eval/public_kg/frontier_20261029/joint")
VALUE_CAP, PAIR_CAP, RULE_CAP = 32, 3, 100
RULE_EDGE_CAP, TOTAL_EDGE_CAP = 2000, 20000
NAME_MIN, VALUE_MIN, JACCARD_MIN, EPS = .55, .80, .05, 1e-9
VALUE_SCORING_PAIRS_PER_TABLE_PAIR = 3

def _json(path): return json.loads(Path(path).read_text(encoding="utf-8"))
def _q(name): return '"' + name.replace('"', '""') + '"'

def _tokens(value):
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", str(value)).casefold()
    return re.findall(r"[a-z0-9]+|[\u3400-\u9fff]", value)

def name_similarity(left, right):
    a, b = _tokens(left), _tokens(right)
    sa, sb = set(a), set(b)
    token = len(sa & sb) / len(sa | sb) if sa | sb else 0.0
    return (SequenceMatcher(None, " ".join(a), " ".join(b)).ratio() + token) / 2

def value_similarity(left, right):
    a, b = " ".join(sorted(_tokens(left))), " ".join(sorted(_tokens(right)))
    if not a or not b: return float(a == b)
    return SequenceMatcher(None, a, b).ratio()

def _values(db, table, column):
    with sqlite3.connect(Path(db).resolve().as_uri() + "?mode=ro", uri=True) as c:
        rows = c.execute(f"SELECT DISTINCT {_q(column)} FROM {_q(table)} WHERE {_q(column)} IS NOT NULL ORDER BY {_q(column)} LIMIT ?", (VALUE_CAP,))
        return [str(r[0]) for r in rows if str(r[0]) != ""]

def _jaccard(a, b):
    # Bound expensive fuzzy comparisons: shortlist each source value against at
    # most 16 targets using character-trigram Jaccard, then apply SequenceMatcher.
    # This preserves deterministic fuzzy matching while avoiding a full Cartesian
    # matrix for every candidate field pair.
    def grams(value):
        compact = " ".join(_tokens(value))
        if not compact: return set()
        if len(compact) < 3: return set(compact)
        return {compact[i:i + 3] for i in range(len(compact) - 2)}
    b_grams = [grams(value) for value in b]
    scores = []
    for i, x in enumerate(a):
        gx = grams(x)
        ranked_targets = []
        for j, y in enumerate(b):
            gy = b_grams[j]
            union = len(gx | gy)
            cheap = len(gx & gy) / union if union else 0.0
            ranked_targets.append((cheap, y, j))
        shortlisted = sorted(ranked_targets, key=lambda z: (-z[0], z[1], z[2]))[:16]
        for _, y, j in shortlisted:
            score = value_similarity(x, y)
            if score >= VALUE_MIN: scores.append((score, x, y, i, j))
    used_a, used_b, matches = set(), set(), []
    for score, x, y, i, j in sorted(scores, key=lambda z: (-z[0], z[1], z[2])):
        if i in used_a or j in used_b: continue
        used_a.add(i); used_b.add(j)
        matches.append({"source_value": x, "target_value": y, "similarity": score})
    union = len(a) + len(b) - len(matches)
    return (len(matches) / union if union else 0.0), matches

def _affinity(declared):
    t = (declared or "").upper()
    if "INT" in t: return "INTEGER"
    if any(x in t for x in ("CHAR", "CLOB", "TEXT")): return "TEXT"
    if any(x in t for x in ("REAL", "FLOA", "DOUB")): return "REAL"
    if not t or "BLOB" in t: return "BLOB"
    return "NUMERIC"

def discover(db, profile, classes, terms, is_a):
    fields = {}
    for table, meta in profile.items():
        for col in meta["columns"]:
            if col["distinct_count"] >= 2:
                fields[table, col["name"]] = col
    pairs = []
    for s in sorted(profile):
        for t in sorted(profile):
            if s == t: continue
            pred = _predicate_for_classes(classes.get(s), classes.get(t), terms, is_a)
            if not pred: continue
            if classes.get(s) == classes.get(t) and s > t: continue
            pairs.append((s, t, pred))
    value_cache = {}
    def values(table, column):
        key = (table, column)
        if key not in value_cache:
            value_cache[key] = _values(db, table, column)
        return value_cache[key]
    rules, mismatched = [], 0
    for st, tt, pred in pairs:
        named = []
        for (s, sc), sm in fields.items():
            if s != st: continue
            for (t, tc), tm in fields.items():
                if t != tt: continue
                if _affinity(sm["type"]) != _affinity(tm["type"]):
                    mismatched += 1; continue
                ns = name_similarity(sc, tc)
                if ns < NAME_MIN: continue
                named.append((ns, sc, tc))
        named.sort(key=lambda z: (-z[0], z[1], z[2]))
        ranked = []
        for ns, sc, tc in named[:VALUE_SCORING_PAIRS_PER_TABLE_PAIR]:
                sv, tv = values(st, sc), values(tt, tc)
                jac, matches = _jaccard(sv, tv)
                if jac < JACCARD_MIN: continue
                ranked.append((.5 * ns + .5 * jac, ns, jac, sc, tc, matches, len(sv), len(tv)))
        ranked.sort(key=lambda z: (-z[0], -z[2], -z[1], z[3], z[4]))
        for score, ns, jac, sc, tc, matches, nsv, ntv in ranked[:PAIR_CAP]:
            rules.append({"source_table": st, "target_table": tt,
                "source_expr": {"op": "column", "name": sc}, "target_expr": {"op": "column", "name": tc},
                "predicate": pred, "direction": "forward",
                "joint_adapted": {"candidate_score": score, "name_similarity": ns,
                    "fuzzy_value_jaccard": jac, "value_matches": matches,
                    "matched_distinct_value_pairs": len(matches), "source_sample_distinct_values": nsv,
                    "target_sample_distinct_values": ntv}})
    rules.sort(key=lambda r: (-r["joint_adapted"]["candidate_score"], r["source_table"], r["target_table"], r["source_expr"]["name"], r["target_expr"]["name"]))
    return rules[:RULE_CAP], {"ontology_supported_table_pairs": len(pairs), "profiled_fields": len(fields), "value_profiled_fields": len(value_cache), "type_mismatched_pairs": mismatched, "value_scored_pairs_per_table_pair_cap": VALUE_SCORING_PAIRS_PER_TABLE_PAIR}

def dijkstra(rules, tables):
    adj = defaultdict(list)
    for r in rules:
        e = {"source_table": r["source_table"], "target_table": r["target_table"],
             "source_column": r["source_expr"]["name"], "target_column": r["target_expr"]["name"],
             "predicate": r["predicate"], "jaccard": r["joint_adapted"]["fuzzy_value_jaccard"]}
        e["weight"] = -math.log(min(1.0, max(EPS, e["jaccard"])))
        adj[e["source_table"]].append(e)
        adj[e["target_table"]].append({**e, "source_table": e["target_table"], "target_table": e["source_table"], "source_column": e["target_column"], "target_column": e["source_column"]})
    result = []
    for start in sorted(tables):
        dist, prev, queue = {start: 0.0}, {}, [(0.0, start)]
        while queue:
            cost, node = heapq.heappop(queue)
            if cost != dist.get(node): continue
            for e in sorted(adj[node], key=lambda x: (x["target_table"], x["source_column"], x["target_column"])):
                nxt, new = e["target_table"], cost + e["weight"]
                if new < dist.get(nxt, math.inf):
                    dist[nxt], prev[nxt] = new, (node, e)
                    heapq.heappush(queue, (new, nxt))
        for target in sorted(dist):
            if target == start: continue
            steps, cur = [], target
            while cur != start:
                parent, edge = prev[cur]; steps.append(edge); cur = parent
            steps.reverse()
            result.append({"source_table": start, "target_table": target, "total_weight": dist[target], "retained_overlap_product": math.exp(-dist[target]), "edges": steps})
    return result

def materialize_fuzzy(db, run_dir, rules, dataset, profile):
    gd = run_dir / "graph"; ttl = gd / "graph.ttl"; evidence = gd / "edge_evidence.jsonl"
    graph = Graph(); graph.parse(ttl, format="turtle")
    with evidence.open(encoding="utf-8") as f:
        existing = {(e["head"], e["predicate"], e["tail"]) for line in f if line.strip() for e in [json.loads(line)]}
    conn = sqlite3.connect(Path(db).resolve().as_uri() + "?mode=ro", uri=True); conn.row_factory = sqlite3.Row
    added, skipped, exhausted = [], 0, False
    try:
        for rid, r in enumerate(rules):
            st, tt = r["source_table"], r["target_table"]; sc, tc = r["source_expr"]["name"], r["target_expr"]["name"]
            spk = [c["name"] for c in profile[st]["columns"] if c.get("primary_key")]; tpk = [c["name"] for c in profile[tt]["columns"] if c.get("primary_key")]
            spk = spk[0] if len(spk) == 1 else None; tpk = tpk[0] if len(tpk) == 1 else None
            for match in r["joint_adapted"]["value_matches"]:
                sr = conn.execute(f"SELECT rowid _rid, {_q(sc)} _val{', '+_q(spk)+' _pk' if spk else ''} FROM {_q(st)} WHERE CAST({_q(sc)} AS TEXT)=? ORDER BY rowid", (match["source_value"],)).fetchall()
                tr = conn.execute(f"SELECT rowid _rid, {_q(tc)} _val{', '+_q(tpk)+' _pk' if tpk else ''} FROM {_q(tt)} WHERE CAST({_q(tc)} AS TEXT)=? ORDER BY rowid", (match["target_value"],)).fetchall()
                local = 0
                for a in sr:
                    for b in tr:
                        if local >= RULE_EDGE_CAP or len(added) >= TOTAL_EDGE_CAP:
                            exhausted = len(added) >= TOTAL_EDGE_CAP; break
                        sk = str(a["_pk"] if spk else a["_rid"]); tk = str(b["_pk"] if tpk else b["_rid"])
                        h = "https://w3id.org/rdb2g/record/" + "/".join(quote(x, safe="") for x in (dataset, st, "pk" if spk else "row", sk))
                        t = "https://w3id.org/rdb2g/record/" + "/".join(quote(x, safe="") for x in (dataset, tt, "pk" if tpk else "row", tk))
                        triple = (h, r["predicate"], t)
                        if triple in existing: skipped += 1; continue
                        graph.add((URIRef(h), URIRef(r["predicate"]), URIRef(t)))
                        added.append({"rule_id": rid, "head": h, "predicate": r["predicate"], "tail": t,
                            "source_table": st, "source_key": sk, "target_table": tt, "target_key": tk,
                            "value_similarity": match["similarity"], "fuzzy_value_jaccard": r["joint_adapted"]["fuzzy_value_jaccard"]})
                        existing.add(triple); local += 1
                    if local >= RULE_EDGE_CAP or len(added) >= TOTAL_EDGE_CAP: break
                if len(added) >= TOTAL_EDGE_CAP: exhausted = True; break
            if exhausted: break
        if added:
            graph.serialize(ttl, format="turtle")
            with evidence.open("a", encoding="utf-8") as f:
                for e in added: f.write(json.dumps(e, ensure_ascii=False) + "\n")
            mp = gd / "structural_metrics.json"; metrics = _json(mp)
            metrics["edge_evidence_rows"] += len(added); metrics["unique_relation_edges"] += len(added)
            metrics["rdf_triples"] = len(graph); metrics["joint_adapted_fuzzy_edges"] = len(added); write_json(mp, metrics)
        return {"fuzzy_edges_added": len(added), "duplicate_edges_skipped": skipped, "edge_budget_exhausted": exhausted}
    finally: conn.close()

def run_dataset(dataset, benchmark=BENCHMARK, context_dir=CONTEXT, out_dir=OUTPUT):
    started = time.perf_counter(); benchmark, context_dir, out_dir = map(Path, (benchmark, context_dir, out_dir))
    db = benchmark / dataset / "sample.sqlite"; ontology = benchmark / "ontology/target_terms.json"
    context_path = context_dir / f"{dataset}_full_r1/predictions.json"; context = _json(context_path)
    if context.get("run_status") != "success": raise ValueError(f"missing successful Full r1 context: {context_path}")
    profile, terms, is_a = schema_profile(db), _terms(ontology), _class_hierarchy(ontology)
    classes = {x["table"]: x.get("target_uri") for x in context["table_mappings"]}
    rules, counts = discover(db, profile, classes, terms, is_a)
    predictions = {"method": "joint_adapted_frontier", "method_scope": "JOINT-adapted relation-only; Full r1 classes; bounded fuzzy matching and traversal", "dataset": dataset, "run_status": "success", "table_mappings": context["table_mappings"], "mappings": [{"table": t, "column": c["name"], "target_uri": None} for t, d in profile.items() for c in d["columns"]], "relation_rules": rules, "component_context": {"table_class_method": "public_adapted_full", "table_class_run": f"{dataset}_full_r1", "table_class_prediction_sha256": file_hash(context_path), "field_mapping_component": "not_run; all field targets explicitly null", "reference_data_read_before_prediction_freeze": False}, "dataset_sha256": file_hash(db), "ontology_sha256": file_hash(ontology)}
    run = out_dir / dataset
    if run.exists():
        import shutil
        shutil.rmtree(run)
    run.mkdir(parents=True, exist_ok=True); pp = run / "predictions.json"; write_json(pp, predictions)
    manifest = {"status": "success", "method": predictions["method"], "paper_label": "JOINT-adapted; not an official reproduction", "paper_url": PAPER, "arxiv_html_source": "https://arxiv.org/html/2509.07230", "dataset_sha256": file_hash(db), "ontology_sha256": file_hash(ontology), "full_r1_context_sha256": file_hash(context_path), "prediction_sha256": file_hash(pp), "reference_data_read_before_prediction_freeze": False, "paid_model_calls": 0, "source_code_search": "arXiv entry/paper text inspected; no official code repository linked", "deviations": ["single-database ontology adaptation", "transformer embedding omitted", "SequenceMatcher on sorted tokens approximates RapidFuzz token_sort_ratio", "deterministic cap of 256 sorted distinct values per field", "greedy one-to-one fuzzy matches with similarity >= 0.80", "name score averages SequenceMatcher and token Jaccard; threshold 0.35", "Jaccard floor 0.05; at most 5 ranked candidates per table pair and 100 rules", "Dijkstra reports paths using -log(Jaccard + 1e-9); direct candidates are materialized", "fuzzy row materialization uses sampled matched values, cap 2000 edges per rule and 20000 total"], "parameters": {"max_values_per_column": VALUE_CAP, "max_candidates_per_table_pair": PAIR_CAP, "max_rules": RULE_CAP, "max_edges_per_rule": RULE_EDGE_CAP, "max_edges_total": TOTAL_EDGE_CAP, "name_threshold": NAME_MIN, "value_similarity_threshold": VALUE_MIN, "jaccard_threshold": JACCARD_MIN, "epsilon": EPS}, "candidate_summary": counts}
    write_json(run / "manifest.json", manifest)
    manifest["dataset"] = dataset
    manifest["source_sha256"] = file_hash(Path(__file__))
    manifest["path_weight_correction"] = "-log(clamp(Jaccard, epsilon, 1)); nonnegative for Dijkstra"
    write_json(run / "manifest.json", manifest)
    # Freeze prediction + manifest before materialization and any diagnostic reference access.
    structural = materialize(db, pp, ontology, run / "graph")
    if structural.get("status") != "success": raise RuntimeError(structural)
    fuzzy = materialize_fuzzy(db, run, rules, dataset, profile)
    paths = dijkstra(rules, profile)
    write_json(run / "traversal_paths.json", {"algorithm": "Dijkstra", "edge_weight": "-log(fuzzy_value_jaccard + epsilon)", "paths": paths})
    elapsed = time.perf_counter() - started
    result = {"dataset": dataset, "status": "success", "method": predictions["method"], "rules": len(rules), "edges": structural["unique_relation_edges"] + fuzzy["fuzzy_edges_added"], "fuzzy_edges": fuzzy, "paths": len(paths), "elapsed_seconds_including_materialization": elapsed, "machine_reference_diagnostics": "pending second-pass diagnostics", "human_gold_score": "not computed"}
    write_json(run / "run_summary.json", result)
    return result

def main():
    p = argparse.ArgumentParser(description=__doc__); p.add_argument("--dataset", choices=DATASETS, action="append"); p.add_argument("--benchmark-dir", type=Path, default=BENCHMARK); p.add_argument("--context-dir", type=Path, default=CONTEXT); p.add_argument("--out-dir", type=Path, default=OUTPUT); a = p.parse_args()
    results = [run_dataset(d, a.benchmark_dir, a.context_dir, a.out_dir) for d in (a.dataset or DATASETS)]
    write_json(a.out_dir / "run_summary.json", {"status": "success", "paper_label": "JOINT-adapted; not an official reproduction", "prediction_freeze_precedes_diagnostics": True, "datasets": results})
    print(json.dumps(results, indent=2))

if __name__ == "__main__": main()
