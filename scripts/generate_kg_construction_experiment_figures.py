from __future__ import annotations

import json
from pathlib import Path

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


DATA_DIR = Path("docs/2026-05-22/kg_construction_experiments")
OUT_DIR = Path("paper/assets/figures")

BLUE = "#4E6E8E"
ORANGE = "#C9825B"
GREEN = "#7C9A92"
SAND = "#D7B28A"
LILAC = "#B9A7C9"
NEUTRAL = "#9AA6B2"
LIGHT = "#E8EDF2"
DARK = "#263238"
PALE_BLUE = "#D7E3EC"
PALE_ORANGE = "#EAD7C8"


def setup_style() -> None:
    mpl.rcParams.update(
        {
            "font.family": "sans-serif",
            "font.sans-serif": ["Arial", "Helvetica", "DejaVu Sans", "sans-serif"],
            "svg.fonttype": "none",
            "pdf.fonttype": 42,
            "font.size": 7,
            "axes.spines.right": False,
            "axes.spines.top": False,
            "axes.linewidth": 0.55,
            "axes.edgecolor": DARK,
            "axes.labelcolor": DARK,
            "xtick.color": DARK,
            "ytick.color": DARK,
            "xtick.major.width": 0.5,
            "ytick.major.width": 0.5,
            "legend.frameon": False,
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "savefig.facecolor": "white",
        }
    )


def save_pub(fig: plt.Figure, stem: str, dpi: int = 600) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT_DIR / f"{stem}.svg", bbox_inches="tight")
    fig.savefig(OUT_DIR / f"{stem}.pdf", bbox_inches="tight")
    fig.savefig(OUT_DIR / f"{stem}.tiff", dpi=dpi, bbox_inches="tight")
    fig.savefig(OUT_DIR / f"{stem}.png", dpi=300, bbox_inches="tight")
    plt.close(fig)


def panel_label(ax: plt.Axes, label: str) -> None:
    ax.text(-0.14, 1.06, label, transform=ax.transAxes, fontsize=8, fontweight="bold", va="bottom")


def soften_axis(ax: plt.Axes, grid: str | None = None) -> None:
    ax.tick_params(length=2.5, pad=2)
    if grid:
        ax.grid(axis=grid, color=LIGHT, linewidth=0.5)
        ax.set_axisbelow(True)


def read_json(name: str) -> dict:
    return json.loads((DATA_DIR / name).read_text(encoding="utf-8"))


def abbreviate_table(name: str) -> str:
    return name.replace("zs_", "").replace("_bz", "").replace("_dm", "").replace("_xq", "")


def category_label(name: str) -> str:
    labels = {
        "编码字段": "Code",
        "名称字段": "Name",
        "空间位置字段": "Spatial",
        "地址字段": "Address",
        "管理或层级字段": "Admin/level",
        "其他字段": "Other",
    }
    return labels.get(str(name), str(name))


def fig_7_6_graph_scale_and_entity_coverage() -> None:
    summary = read_json("e1_graph_scale_summary.json")
    coverage = pd.read_csv(DATA_DIR / "e1_entity_coverage_by_table.csv")
    coverage = coverage.sort_values("sqlite_rows", ascending=True)

    metrics = [
        ("Subjects", summary["ttl_unique_subjects"]),
        ("Triples", summary["ttl_triples"]),
        ("Literal\ntriples", summary["literal_triples"]),
        ("Resource\ntriples", summary["resource_triples"]),
        ("Relation\ntriples", summary["resource_object_non_type_triples"]),
    ]
    labels = [m[0] for m in metrics]
    values = np.array([m[1] for m in metrics], dtype=float)

    fig = plt.figure(figsize=(7.0, 3.2))
    gs = fig.add_gridspec(1, 2, width_ratios=[1.15, 1.0], wspace=0.42)
    ax1 = fig.add_subplot(gs[0, 0])
    ax2 = fig.add_subplot(gs[0, 1])

    x = np.arange(len(labels))
    ax1.bar(x, values / 1000, color=[BLUE, ORANGE, GREEN, SAND, LILAC], width=0.62)
    ax1.set_xticks(x, labels)
    ax1.set_ylabel("Count (thousand)")
    ax1.set_title("Graph scale", fontsize=7, fontweight="bold", pad=4)
    for xi, value in zip(x, values):
        ax1.text(xi, value / 1000 + max(values / 1000) * 0.025, f"{value/1000:.0f}k", ha="center", va="bottom", fontsize=5.8)
    soften_axis(ax1, grid="y")
    panel_label(ax1, "a")

    y = np.arange(len(coverage))
    ax2.barh(y, coverage["sqlite_rows"], color=PALE_BLUE, edgecolor="none", label="SQLite rows")
    ax2.scatter(coverage["kg_subjects"], y, s=14, color=BLUE, label="KG subjects", zorder=3)
    ax2.set_yticks(y, [abbreviate_table(v) for v in coverage["table"]])
    ax2.set_xlabel("Entities per table")
    ax2.set_title("Entity coverage by table", fontsize=7, fontweight="bold", pad=4)
    ax2.legend(loc="lower right", fontsize=5.8, handletextpad=0.3)
    soften_axis(ax2, grid="x")
    panel_label(ax2, "b")

    fig.text(0.02, 0.01, "Claim: all 90,372 sample records are represented as KG subjects.", fontsize=6, color="#52616B")
    fig.subplots_adjust(left=0.08, right=0.98, bottom=0.20, top=0.88, wspace=0.42)
    save_pub(fig, "fig-7-6-kg-scale-entity-coverage")


def fig_7_7_property_fidelity() -> None:
    summary = read_json("e2_property_fidelity_summary.json")
    df = pd.read_csv(DATA_DIR / "e2_property_fidelity_testset_100.csv")
    by_cat = df.groupby(["column_category", "status"]).size().unstack(fill_value=0)
    by_cat = by_cat.reindex(sorted(by_cat.index), fill_value=0)

    fig = plt.figure(figsize=(6.4, 2.8))
    gs = fig.add_gridspec(1, 2, width_ratios=[0.88, 1.35], wspace=0.45)
    ax1 = fig.add_subplot(gs[0, 0])
    ax2 = fig.add_subplot(gs[0, 1])

    vals = [summary["matched"], summary["missing"]]
    colors = [GREEN, NEUTRAL]
    ax1.pie(vals, startangle=90, counterclock=False, colors=colors, wedgeprops={"width": 0.36, "edgecolor": "white", "linewidth": 0.8})
    ax1.text(0, 0.05, f"{summary['coverage_rate']:.0%}", ha="center", va="center", fontsize=16, fontweight="bold", color=DARK)
    ax1.text(0, -0.22, "direct\nmatch", ha="center", va="center", fontsize=6.4, color=DARK)
    ax1.set_title("Attribute value traceability", fontsize=7, fontweight="bold", pad=4)
    panel_label(ax1, "a")

    y = np.arange(len(by_cat))
    matched = by_cat.get("matched", pd.Series(0, index=by_cat.index)).values
    missing = by_cat.get("missing", pd.Series(0, index=by_cat.index)).values
    ax2.barh(y, matched, color=GREEN, label="matched")
    ax2.barh(y, missing, left=matched, color=NEUTRAL, label="missing")
    ax2.set_yticks(y, [category_label(v) for v in by_cat.index])
    ax2.set_xlabel("Samples (n = 100)")
    ax2.set_title("Coverage by field category", fontsize=7, fontweight="bold", pad=4)
    for yi, m, miss in zip(y, matched, missing):
        ax2.text(m + miss + 0.8, yi, f"{int(m)}/{int(m+miss)}", va="center", fontsize=5.8)
    ax2.set_xlim(0, max((matched + missing).max() * 1.18, 10))
    ax2.legend(loc="lower right", fontsize=5.8, handletextpad=0.3)
    soften_axis(ax2, grid="x")
    panel_label(ax2, "b")
    fig.subplots_adjust(left=0.08, right=0.98, bottom=0.16, top=0.84, wspace=0.45)
    save_pub(fig, "fig-7-7-property-fidelity")


def fig_7_8_semantic_mapping_prereview() -> None:
    df = pd.read_csv(DATA_DIR / "e3_semantic_mapping_review_testset_100.csv")
    table_counts = df.groupby(["table", "auto_review_status"]).size().unstack(fill_value=0)
    table_counts["total"] = table_counts.sum(axis=1)
    table_counts = table_counts.sort_values("total", ascending=True)
    cat_counts = df["column_category"].value_counts().sort_values(ascending=True)

    fig = plt.figure(figsize=(7.5, 4.25))
    gs = fig.add_gridspec(1, 2, width_ratios=[1.55, 1.0], wspace=0.46)
    ax1 = fig.add_subplot(gs[0, 0])
    ax2 = fig.add_subplot(gs[0, 1])

    y = np.arange(len(table_counts))
    ok = table_counts.get("auto_consistent_pending_human_review", pd.Series(0, index=table_counts.index)).values
    review = table_counts.get("needs_human_review", pd.Series(0, index=table_counts.index)).values
    ax1.barh(y, ok, color=GREEN, label="auto-consistent", height=0.62)
    ax1.barh(y, review, left=ok, color=ORANGE, label="needs review", height=0.62)
    ax1.set_yticks(y, [abbreviate_table(v) for v in table_counts.index], fontsize=5.8)
    ax1.set_xlabel("Field mappings")
    ax1.set_title("Balanced mapping pre-review across 17 tables", fontsize=7, fontweight="bold", pad=4)
    ax1.legend(loc="upper center", bbox_to_anchor=(0.55, 1.02), ncols=2, fontsize=5.6, handletextpad=0.25, columnspacing=0.7)
    soften_axis(ax1, grid="x")
    panel_label(ax1, "a")

    ax2.barh(np.arange(len(cat_counts)), cat_counts.values, color=BLUE)
    ax2.set_yticks(np.arange(len(cat_counts)), [category_label(v) for v in cat_counts.index])
    ax2.set_xlabel("Samples")
    ax2.set_title("Field categories", fontsize=7, fontweight="bold", pad=4)
    for yi, val in enumerate(cat_counts.values):
        ax2.text(val + 0.5, yi, str(int(val)), va="center", fontsize=5.8)
    ax2.set_xlim(0, cat_counts.max() * 1.22)
    soften_axis(ax2, grid="x")
    panel_label(ax2, "b")
    fig.text(0.03, 0.015, "Pre-review only: not a final semantic accuracy score.", fontsize=6, color="#52616B")
    fig.subplots_adjust(left=0.14, right=0.98, bottom=0.14, top=0.90, wspace=0.46)
    save_pub(fig, "fig-7-8-semantic-mapping-prereview")


def fig_7_9_relation_rule_effectiveness() -> None:
    summary = read_json("e4_relation_rule_effectiveness_summary.json")
    df = pd.read_csv(DATA_DIR / "e4_relation_rule_effectiveness.csv")
    rules = df.copy()
    rules["rule_index"] = pd.to_numeric(rules["rule_index"], errors="coerce").astype(int)
    rules = rules.sort_values("rule_index", ascending=True).reset_index(drop=True)
    probed = df[df["validation_status"] == "probed"].copy()
    probed["rule_index"] = pd.to_numeric(probed["rule_index"], errors="coerce").astype(int)
    probed["hit_rate"] = pd.to_numeric(probed["hit_rate"], errors="coerce")
    probed = probed.dropna(subset=["hit_rate"])
    probed["enabled_label"] = np.where(probed["enabled"].astype(str) == "True", "enabled", "disabled")
    probed = probed.sort_values("rule_index", ascending=True).reset_index(drop=True)
    probed["plot_y"] = probed["rule_index"].map({idx: pos for pos, idx in enumerate(rules["rule_index"])})

    fig = plt.figure(figsize=(7.2, 4.4))
    gs = fig.add_gridspec(1, 2, width_ratios=[0.82, 1.55], wspace=0.34)
    ax1 = fig.add_subplot(gs[0, 0])
    ax2 = fig.add_subplot(gs[0, 1])

    labels = ["Accepted\nstrong", "Rejected/\ndisabled", "Weak\ncandidates"]
    vals = [summary["accepted_strong_rules"], summary["rejected_or_disabled_rules"], summary["weak_candidate_rules"]]
    ax1.bar(np.arange(3), vals, color=[GREEN, NEUTRAL, SAND], width=0.62)
    ax1.set_xticks(np.arange(3), labels)
    ax1.set_ylabel("Rules")
    ax1.set_title("Rule status", fontsize=7, fontweight="bold", pad=4)
    for xi, val in enumerate(vals):
        ax1.text(xi, val + 0.4, str(val), ha="center", va="bottom", fontsize=6)
    ax1.set_ylim(0, max(vals) * 1.25)
    soften_axis(ax1, grid="y")
    panel_label(ax1, "a")

    colors = np.where(probed["enabled_label"] == "enabled", GREEN, NEUTRAL)
    y = np.arange(len(rules))
    labels2 = [f"R{int(i):02d}" for i in rules["rule_index"]]
    ax2.scatter(probed["hit_rate"], probed["plot_y"], s=20, color=colors, zorder=3)
    ax2.axvline(0.8, color=ORANGE, linewidth=0.8, linestyle="--", label="0.8 threshold")
    ax2.set_yticks(y, labels2, fontsize=5.8)
    ax2.set_ylim(len(rules) - 0.5, -0.5)
    ax2.set_xlabel("Probe hit rate")
    ax2.set_xlim(-0.03, 1.05)
    ax2.set_title("Data-probe evidence", fontsize=7, fontweight="bold", pad=4)
    ax2.legend(loc="lower left", fontsize=5.8, handletextpad=0.3)
    soften_axis(ax2, grid="x")
    panel_label(ax2, "b")
    fig.text(0.02, 0.01, "Right panel: Rxx = rule_index in e4_relation_rule_effectiveness.csv.", fontsize=6, color="#52616B")
    fig.subplots_adjust(left=0.08, right=0.98, bottom=0.16, top=0.90, wspace=0.34)
    save_pub(fig, "fig-7-9-relation-rule-effectiveness")


def fig_7_10_relation_edge_validation() -> None:
    df = pd.read_csv(DATA_DIR / "e5_relation_edge_validation_testset_100.csv")
    rel_counts = df["relation"].value_counts().sort_values(ascending=True)
    target_counts = df["target_table"].value_counts().sort_values(ascending=True)

    fig = plt.figure(figsize=(7.0, 3.0))
    gs = fig.add_gridspec(1, 2, width_ratios=[1.05, 1.0], wspace=0.48)
    ax1 = fig.add_subplot(gs[0, 0])
    ax2 = fig.add_subplot(gs[0, 1])

    y = np.arange(len(rel_counts))
    ax1.barh(y, rel_counts.values, color=GREEN)
    ax1.set_yticks(y, [v.replace("zs:", "") for v in rel_counts.index])
    ax1.set_xlabel("Validated edges")
    ax1.set_title("Relation coverage in edge validation", fontsize=7, fontweight="bold", pad=4)
    for yi, val in enumerate(rel_counts.values):
        ax1.text(val + 0.35, yi, str(int(val)), va="center", fontsize=5.8)
    ax1.set_xlim(0, rel_counts.max() * 1.2)
    soften_axis(ax1, grid="x")
    panel_label(ax1, "a")

    y2 = np.arange(len(target_counts))
    ax2.barh(y2, target_counts.values, color=BLUE)
    ax2.set_yticks(y2, [abbreviate_table(v) for v in target_counts.index])
    ax2.set_xlabel("Target samples")
    ax2.set_title("Target entity coverage", fontsize=7, fontweight="bold", pad=4)
    for yi, val in enumerate(target_counts.values):
        ax2.text(val + 0.35, yi, str(int(val)), va="center", fontsize=5.8)
    ax2.set_xlim(0, target_counts.max() * 1.2)
    soften_axis(ax2, grid="x")
    panel_label(ax2, "b")
    fig.text(0.02, 0.01, "All 100 sampled edges were supported by source and target key values.", fontsize=6, color="#52616B")
    fig.subplots_adjust(left=0.15, right=0.98, bottom=0.20, top=0.86, wspace=0.45)
    save_pub(fig, "fig-7-10-relation-edge-validation")


def fig_7_11_graph_structure_connectivity() -> None:
    summary = read_json("e6_graph_structure_summary.json")
    rel = pd.read_csv(DATA_DIR / "e6_relation_type_distribution.csv")
    rel["relation"] = rel["relation"].str.replace("zs:", "", regex=False)
    rel = rel.sort_values("edge_count", ascending=True)

    fig = plt.figure(figsize=(7.2, 3.0))
    gs = fig.add_gridspec(1, 2, width_ratios=[0.9, 1.25], wspace=0.45)
    ax1 = fig.add_subplot(gs[0, 0])
    ax2 = fig.add_subplot(gs[0, 1])

    relation_connected_coverage = summary["connected_entity_nodes"] / summary["entity_nodes"] if summary["entity_nodes"] else 0
    vals = [summary["connected_entity_nodes"], summary["entity_nodes"] - summary["connected_entity_nodes"]]
    ax1.pie(vals, startangle=90, counterclock=False, colors=[GREEN, NEUTRAL], wedgeprops={"width": 0.36, "edgecolor": "white", "linewidth": 0.8})
    ax1.text(0, 0.05, f"{relation_connected_coverage:.1%}", ha="center", va="center", fontsize=11, fontweight="bold", color=DARK)
    ax1.text(0, -0.20, "relation-connected\ncoverage", ha="center", va="center", fontsize=6.2, color=DARK)
    ax1.set_title("Entity connectivity", fontsize=7, fontweight="bold", pad=4)
    panel_label(ax1, "a")

    y = np.arange(len(rel))
    colors = [ORANGE if r == "managedByPoliceOrg" else BLUE for r in rel["relation"]]
    ax2.barh(y, rel["edge_count"], color=colors)
    ax2.set_yticks(y, rel["relation"])
    ax2.set_xlabel("Business relation edges")
    ax2.set_title("Relation type distribution", fontsize=7, fontweight="bold", pad=4)
    for yi, val in enumerate(rel["edge_count"]):
        ax2.text(val + rel["edge_count"].max() * 0.015, yi, f"{int(val):,}", va="center", fontsize=5.8)
    ax2.set_xlim(0, rel["edge_count"].max() * 1.18)
    soften_axis(ax2, grid="x")
    panel_label(ax2, "b")
    fig.text(0.02, 0.01, f"Largest connected component: {summary['largest_connected_component_nodes']:,} entities; relation-connected coverage: {relation_connected_coverage:.1%}.", fontsize=6, color="#52616B")
    fig.subplots_adjust(left=0.08, right=0.98, bottom=0.20, top=0.86, wspace=0.45)
    save_pub(fig, "fig-7-11-graph-structure-connectivity")


def fig_7_12_path_explainability() -> None:
    df = pd.read_csv(DATA_DIR / "e7_kg_explainability_testset_100.csv")
    rel_counts = df["query_type"].value_counts().sort_values(ascending=True)
    start_counts = df["start_table"].value_counts().sort_values(ascending=True)

    fig = plt.figure(figsize=(7.0, 3.2))
    gs = fig.add_gridspec(1, 2, width_ratios=[1.0, 1.15], wspace=0.50)
    ax1 = fig.add_subplot(gs[0, 0])
    ax2 = fig.add_subplot(gs[0, 1])

    y = np.arange(len(rel_counts))
    ax1.barh(y, rel_counts.values, color=GREEN)
    ax1.set_yticks(y, rel_counts.index)
    ax1.set_xlabel("Path samples")
    ax1.set_title("Path evidence by relation", fontsize=7, fontweight="bold", pad=4)
    for yi, val in enumerate(rel_counts.values):
        ax1.text(val + 0.3, yi, str(int(val)), va="center", fontsize=5.8)
    ax1.set_xlim(0, rel_counts.max() * 1.2)
    soften_axis(ax1, grid="x")
    panel_label(ax1, "a")

    top = start_counts.tail(10)
    y2 = np.arange(len(top))
    ax2.barh(y2, top.values, color=BLUE)
    ax2.set_yticks(y2, [abbreviate_table(v) for v in top.index])
    ax2.set_xlabel("Start entities")
    ax2.set_title("Entry entities for evidence chains", fontsize=7, fontweight="bold", pad=4)
    for yi, val in enumerate(top.values):
        ax2.text(val + 0.3, yi, str(int(val)), va="center", fontsize=5.8)
    ax2.set_xlim(0, top.max() * 1.2)
    soften_axis(ax2, grid="x")
    panel_label(ax2, "b")
    fig.text(0.02, 0.01, "Path availability supports future Graph RAG retrieval context, not answer-generation accuracy.", fontsize=6, color="#52616B")
    fig.subplots_adjust(left=0.13, right=0.98, bottom=0.20, top=0.86, wspace=0.50)
    save_pub(fig, "fig-7-12-path-explainability-graph-rag-basis")


def main() -> None:
    setup_style()
    fig_7_6_graph_scale_and_entity_coverage()
    fig_7_7_property_fidelity()
    fig_7_8_semantic_mapping_prereview()
    fig_7_9_relation_rule_effectiveness()
    fig_7_10_relation_edge_validation()
    fig_7_11_graph_structure_connectivity()
    fig_7_12_path_explainability()


if __name__ == "__main__":
    main()
