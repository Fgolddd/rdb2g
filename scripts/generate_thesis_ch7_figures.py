from __future__ import annotations

from pathlib import Path

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np


OUT_DIR = Path("paper/assets/figures")

SQL = "#4E6E8E"
KG = "#C9825B"
NEUTRAL = "#9AA6B2"
LIGHT = "#E8EDF2"
DARK = "#263238"
ACCENT = "#7C9A92"


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


def save_pub_py(fig: plt.Figure, stem: str, dpi: int = 600) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT_DIR / f"{stem}.svg", bbox_inches="tight")
    fig.savefig(OUT_DIR / f"{stem}.pdf", bbox_inches="tight")
    fig.savefig(OUT_DIR / f"{stem}.tiff", dpi=dpi, bbox_inches="tight")
    fig.savefig(OUT_DIR / f"{stem}.png", dpi=300, bbox_inches="tight")
    plt.close(fig)


def panel_label(ax: plt.Axes, label: str) -> None:
    ax.text(-0.12, 1.05, label, transform=ax.transAxes, fontsize=8, fontweight="bold", va="bottom")


def soften_axis(ax: plt.Axes, grid: bool = False) -> None:
    ax.tick_params(length=2.5, pad=2)
    if grid:
        ax.grid(axis="x", color=LIGHT, linewidth=0.5)
        ax.set_axisbelow(True)


def fig_7_1_overall_performance() -> None:
    metrics = ["Mean", "P95"]
    sql = [68.266, 380.970]
    kg = [97.240, 305.700]

    fig, axes = plt.subplots(1, 2, figsize=(5.0, 2.25), sharey=False)
    for ax, metric, sql_val, kg_val, label in zip(axes, metrics, sql, kg, ["a", "b"]):
        x = np.array([0, 1])
        vals = [sql_val, kg_val]
        ax.bar(x, vals, width=0.52, color=[SQL, KG], edgecolor="none")
        ax.set_xticks(x, ["SQL", "KG"])
        ax.set_ylabel("Latency (ms)")
        ax.set_title(metric, fontsize=7, pad=4, fontweight="bold")
        ax.set_ylim(0, max(vals) * 1.25)
        for xi, val in zip(x, vals):
            ax.text(xi, val + max(vals) * 0.035, f"{val:.1f}", ha="center", va="bottom", fontsize=6.4)
        soften_axis(ax)
        panel_label(ax, label)
    fig.text(0.02, 0.01, "Server-side Neo4j timing; SQL measured locally.", fontsize=6, color="#52616B")
    fig.tight_layout(w_pad=1.2)
    save_pub_py(fig, "fig-7-1-overall-performance")


def fig_7_2_task_type_latency() -> None:
    groups = [
        ("Low-latency tasks", ["semantic", "basic"], np.array([0.138, 0.965]), np.array([4.300, 19.154]), 24),
        ("Join/path tasks", ["join", "multihop"], np.array([71.704, 193.650]), np.array([136.867, 209.750]), 230),
    ]

    fig, axes = plt.subplots(1, 2, figsize=(6.1, 2.35), gridspec_kw={"wspace": 0.52})
    for ax, (title, task_types, sql, kg, xmax), label in zip(axes, groups, ["a", "b"]):
        y = np.arange(len(task_types))
        for yi, s, k in zip(y, sql, kg):
            ax.plot([s, k], [yi, yi], color=NEUTRAL, linewidth=0.8, zorder=1)
        ax.scatter(sql, y, s=24, color=SQL, label="SQL", zorder=3)
        ax.scatter(kg, y, s=24, color=KG, label="KG", zorder=3)
        for yi, s, k in zip(y, sql, kg):
            ax.text(s, yi - 0.17, f"{s:.1f}", ha="center", va="top", fontsize=5.7, color=SQL)
            ax.text(k, yi + 0.17, f"{k:.1f}", ha="center", va="bottom", fontsize=5.7, color=KG)
        ax.set_yticks(y, task_types)
        ax.set_xlabel("Mean latency (ms)")
        ax.set_xlim(-xmax * 0.05, xmax)
        ax.set_ylim(-0.48, len(task_types) - 0.52)
        ax.invert_yaxis()
        ax.set_title(title, fontsize=7, pad=4, fontweight="bold")
        soften_axis(ax, grid=True)
        panel_label(ax, label)
    axes[1].legend(loc="lower right", ncols=2, handletextpad=0.2, columnspacing=0.8)
    fig.subplots_adjust(left=0.12, right=0.98, bottom=0.22, top=0.82, wspace=0.50)
    save_pub_py(fig, "fig-7-2-task-type-latency")


def fig_7_3_kg_faster_cases() -> None:
    rows = [
        ("C088", "multihop", 6.657),
        ("C092", "multihop", 5.859),
        ("C095", "multihop", 5.323),
        ("C091", "multihop", 5.057),
        ("C089", "multihop", 5.005),
        ("C094", "multihop", 4.980),
        ("C100", "multihop", 4.979),
        ("C099", "multihop", 4.938),
        ("C098", "multihop", 4.600),
        ("C096", "multihop", 3.721),
        ("C037", "join", 2.869),
        ("C044", "join", 2.721),
        ("C041", "join", 1.704),
    ]
    labels = [r[0] for r in rows]
    types = [r[1] for r in rows]
    speedup = np.array([r[2] for r in rows])
    y = np.arange(len(rows))
    colors = [KG if t == "multihop" else SQL for t in types]

    fig, ax = plt.subplots(figsize=(5.8, 4.2))
    ax.hlines(y, 1, speedup, color=LIGHT, linewidth=2.2, zorder=1)
    ax.scatter(speedup, y, s=25, color=colors, zorder=3)
    ax.axvline(1, color=NEUTRAL, linewidth=0.7, linestyle="--")
    for yi, val in zip(y, speedup):
        ax.text(val + 0.35, yi, f"{val:.1f}x", va="center", fontsize=5.8)
    ax.text(6.657, -0.58, "largest gain", ha="center", va="bottom", fontsize=5.8, color=KG)
    ax.set_yticks(y, labels)
    ax.set_xlabel("Speedup (SQL / KG)")
    ax.set_xlim(0, 8)
    ax.invert_yaxis()
    ax.scatter([], [], color=KG, label="multihop", s=24)
    ax.scatter([], [], color=SQL, label="join", s=24)
    ax.legend(loc="lower right", handletextpad=0.3)
    soften_axis(ax, grid=True)
    panel_label(ax, "a")
    fig.tight_layout()
    save_pub_py(fig, "fig-7-3-kg-faster-cases")


def fig_7_4_rule_status() -> None:
    labels = ["Auto-accepted\nstrong", "Rejected or\ndisabled", "Weak\ncandidates"]
    values = np.array([18, 12, 2])
    total = values.sum()
    colors = [ACCENT, NEUTRAL, "#D7B28A"]

    fig, ax = plt.subplots(figsize=(5.0, 1.8))
    left = 0
    for label, value, color in zip(labels, values, colors):
        ax.barh([0], [value], left=left, height=0.36, color=color, edgecolor="white", linewidth=0.6)
        ax.text(left + value / 2, 0, f"{value}\n({value / total:.0%})", ha="center", va="center", fontsize=6.2, color="white" if value >= 8 else DARK)
        ax.text(left + value / 2, -0.36, label, ha="center", va="top", fontsize=5.8, color=DARK)
        left += value
    ax.set_xlim(0, total)
    ax.set_ylim(-0.72, 0.38)
    ax.set_yticks([])
    ax.set_xlabel("Candidate relation rules (n = 32)")
    ax.spines["left"].set_visible(False)
    ax.spines["bottom"].set_visible(False)
    ax.tick_params(axis="x", length=0, labelbottom=False)
    panel_label(ax, "a")
    fig.tight_layout()
    save_pub_py(fig, "fig-7-4-rule-status")


def fig_7_5_edge_consistency() -> None:
    labels = ["IN_QX", "IN_CITY", "IN_PROVINCE", "ON_STREET", "REF_MP"]
    sql = np.array([2474, 2474, 2474, 110, 1], dtype=float)
    neo4j = np.array([1645, 1657, 1618, 79, 1], dtype=float)
    y = np.arange(len(labels))

    fig, ax = plt.subplots(figsize=(5.7, 3.0))
    for yi, s, n in zip(y, sql, neo4j):
        ax.plot([s, n], [yi, yi], color=NEUTRAL, linewidth=0.9, zorder=1)
    ax.scatter(sql, y, s=28, color=SQL, label="SQL relation instances", zorder=3)
    ax.scatter(neo4j, y, s=28, color=KG, label="Neo4j edges", zorder=3)
    for yi, s, n in zip(y, sql, neo4j):
        ax.text(s + 65, yi - 0.12, f"{int(s):,}", fontsize=5.8, color=SQL, va="top")
        ax.text(n - 65, yi + 0.12, f"{int(n):,}", fontsize=5.8, color=KG, va="bottom", ha="right")
    ax.set_yticks(y, labels)
    ax.set_xlabel("Count")
    ax.set_xlim(-100, 2750)
    ax.invert_yaxis()
    ax.legend(loc="lower right", fontsize=6, handletextpad=0.3)
    soften_axis(ax, grid=True)
    panel_label(ax, "a")
    fig.tight_layout()
    save_pub_py(fig, "fig-7-5-edge-consistency")


def main() -> None:
    setup_style()
    fig_7_1_overall_performance()
    fig_7_2_task_type_latency()
    fig_7_3_kg_faster_cases()
    fig_7_4_rule_status()
    fig_7_5_edge_consistency()


if __name__ == "__main__":
    main()
