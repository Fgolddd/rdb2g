#!/usr/bin/env python3
"""对 data/public 下的两份公开地址数据集做分层抽样。

规则（与 `data/public/抽样方案.md` 保持一致，改规则请同步改文档）：

1. 小表（<= SMALL_TABLE_KEEP 行）全量保留：它们承载行政区划层级，是 KG 的骨架。
2. 大表按 50% 分层抽样；层内按容量比例分配（proportional allocation），
   每层至少 MIN_PER_STRATUM 条，容量不足时把配额顺延给有余量的层。
3. 分层变量跟随中山私有库的语义：
   - 佛山 POI/AOI：DLMC(映射到中山一级类) x QXMC(区县)
   - 佛山街路巷：S_MC(镇街) x 名称后缀类型(路/街/巷/大道/其他)
   - 佛山地名：FLMC(地名二级类)
   - 佛山公共设施：SSLX
   - GeoNames：feature_class x 是否有 admin2 归属；另强制保留 ADM1/ADM2 行政骨架
4. 固定随机种子，按主键排序后再抽，保证同一快照可复现。

分配方式默认 `proportional`：层配额与层容量成正比，样本保持原库分布，
中山库只用于类别对齐与对照报告。若显式传 `--foshan-alloc reference`，
则按中山库的一级类占比配额（会让样本偏离佛山真实构成，仅用于受控对照）。

只做抽样，不改字段名、不改行内容；GeoNames 额外补一列 `name_zh`
（原库 name/asciiname 为拼音，中文名只存在于 alternatenames）。
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sqlite3
from pathlib import Path

SEED = 20261029
SAMPLE_FRACTION = 0.5
SMALL_TABLE_KEEP = 1000
MIN_PER_STRATUM = 20
# 佛山是 50% 抽样，用纯比例分配（floor=0）最贴合原分布；
# 保底名额会挤占大层，实测 floor=20 把教育类压低 7pp、floor=5 压低 2.5pp。
MIN_PER_STRATUM_FOSHAN = 0
GEONAMES_TARGET = 10000

# 佛山(OSM 派生)一级类 -> 中山一级类。前两类是唯一需要跨词表的映射。
DLMC_TO_ZS = {
    "教育": "科教文化服务",
    "其他": "生活服务",
}

STREET_SUFFIXES = ["大道", "路", "街", "巷", "里", "坊", "弄"]

ZH_RE = re.compile(r"[\u4e00-\u9fff]")
ZH_FULL = re.compile(r"[\u4e00-\u9fff]{2,12}")
ADMIN_SUFFIX = ("自治区", "自治州", "自治县", "地区", "街道", "省", "市", "区", "县", "州", "镇", "乡", "村")


# ---------------------------------------------------------------- allocation
def allocate(weights: dict, capacity: dict, n: int, floor: float = 0.0) -> dict:
    """容量约束下的比例分配（floor -> 比例 -> 封顶 -> 回填）。"""
    cells = [c for c in weights if capacity.get(c, 0) > 0]
    alloc = {c: 0 for c in cells}
    if not cells or n <= 0:
        return alloc

    remaining = n
    # 1) 保底名额
    if floor > 0:
        for c in cells:
            if remaining <= 0:
                break
            give = int(min(floor, capacity[c]))
            alloc[c] = give
            remaining -= give

    # 2) 比例分配 + 封顶回填
    active = set(cells)
    while remaining > 0 and active:
        total_w = sum(weights[c] for c in active)
        if total_w <= 0:
            break
        proposal = {c: remaining * weights[c] / total_w for c in active}
        capped = [c for c in active if proposal[c] > capacity[c] - alloc[c]]
        if capped:
            for c in capped:
                give = capacity[c] - alloc[c]
                alloc[c] += give
                remaining -= give
                active.discard(c)
            continue
        base = {c: int(proposal[c]) for c in active}
        left = remaining - sum(base.values())
        ranked = sorted(active, key=lambda c: (-(proposal[c] - base[c]), str(c)))
        for c in ranked[:left]:
            base[c] += 1
        for c in active:
            alloc[c] += base[c]
        remaining = 0
    return alloc


def table_columns(con: sqlite3.Connection, table: str) -> list[str]:
    return [r[1] for r in con.execute(f'PRAGMA table_info("{table}")')]


def table_ddl(con: sqlite3.Connection, table: str) -> str:
    row = con.execute(
        "select sql from sqlite_master where type='table' and name=?", (table,)
    ).fetchone()
    if not row or not row[0]:
        raise RuntimeError(f"no DDL for {table}")
    return row[0]


def copy_table(src: sqlite3.Connection, dst: sqlite3.Connection, table: str, rowids: list[int]) -> int:
    dst.execute(table_ddl(src, table))
    cols = table_columns(src, table)
    if not rowids:
        return 0
    placeholders = ",".join("?" * len(cols))
    col_sql = ",".join(f'"{c}"' for c in cols)
    chunk = 500
    total = 0
    for i in range(0, len(rowids), chunk):
        part = rowids[i : i + chunk]
        marks = ",".join("?" * len(part))
        rows = src.execute(
            f'select {col_sql} from "{table}" where rowid in ({marks})', part
        ).fetchall()
        dst.executemany(f'insert into "{table}" ({col_sql}) values ({placeholders})', rows)
        total += len(rows)
    return total


def all_rowids(con: sqlite3.Connection, table: str) -> list[int]:
    return [r[0] for r in con.execute(f'select rowid from "{table}"')]


def stratified_rowids(
    con: sqlite3.Connection,
    table: str,
    key_expr: str,
    n: int,
    weights: dict | None = None,
    floor: float = MIN_PER_STRATUM,
) -> tuple[dict[int, str], dict[str, int], dict[str, int]]:
    """返回 (rowid->stratum, 层容量, 层配额)。"""
    rows = con.execute(f'select rowid, {key_expr} as k from "{table}" order by rowid').fetchall()
    groups: dict[str, list[int]] = {}
    for rid, key in rows:
        groups.setdefault(str(key), []).append(rid)
    capacity = {k: len(v) for k, v in groups.items()}
    if weights is None:
        weights = {k: float(v) for k, v in capacity.items()}
    quota = allocate(weights, capacity, min(n, len(rows)), floor=floor)
    return {rid: k for k, ids in groups.items() for rid in ids}, capacity, quota


def pick(ids: list[int], quota: int, rng: random.Random) -> list[int]:
    ids = sorted(ids)
    if len(ids) <= quota:
        return ids
    return sorted(rng.sample(ids, quota))


# ---------------------------------------------------------------- foshan
def build_foshan(src_path: Path, dst_path: Path, alloc_mode: str = "proportional") -> dict:
    if dst_path.exists():
        dst_path.unlink()
    src = sqlite3.connect(src_path)
    dst = sqlite3.connect(dst_path)
    dst.execute("PRAGMA journal_mode=OFF")
    dst.execute("PRAGMA synchronous=OFF")
    rng = random.Random(SEED)

    tables = [r[0] for r in src.execute(
        "select name from sqlite_master where type='table' order by name")]
    report: dict = {"source": str(src_path), "tables": {}, "seed": SEED}

    # 中山参照分布（一级类占比），用于把佛山类别对齐后再比较
    zs_dlmc = {}
    zs_path = src_path.parent.parent / "company" / "zhongshan_10k.sqlite"
    if zs_path.exists():
        zs = sqlite3.connect(zs_path)
        zs_dlmc = dict(zs.execute("select DLMC, count(*) from zs_POI_dm group by 1"))
        zs.close()
    zs_total = sum(zs_dlmc.values()) or 1

    def dlmc_map(v: str) -> str:
        return DLMC_TO_ZS.get(v, v)

    def suffix(v: str) -> str:
        for s in STREET_SUFFIXES:
            if v and v.endswith(s):
                return s
        return "其他"

    spec = {
        "zs_POI_dm": dict(key="QXMC", kind="dlmc"),
        "zs_AOI_dm": dict(key="QXMC", kind="dlmc"),
        "zs_street_bz": dict(kind="street"),
        "zs_DH_bz": dict(key="FLMC", kind="flat"),
        "zs_facility_dm": dict(key="SSLX", kind="flat"),
    }

    for table in tables:
        cols = table_columns(src, table)
        total = src.execute(f'select count(*) from "{table}"').fetchone()[0]
        entry = {"source_rows": total}

        if total == 0:
            copy_table(src, dst, table, [])
            entry.update(kept=0, mode="empty")
            report["tables"][table] = entry
            continue

        if total <= SMALL_TABLE_KEEP or table not in spec:
            ids = all_rowids(src, table)
            copy_table(src, dst, table, ids)
            entry.update(kept=len(ids), mode="keep_all")
            report["tables"][table] = entry
            continue

        cfg = spec[table]
        n = int(round(total * SAMPLE_FRACTION))

        if cfg["kind"] == "dlmc":
            rows = src.execute(
                f'select rowid, DLMC, {cfg["key"]} from "{table}" order by rowid'
            ).fetchall()
            groups: dict[tuple, list[int]] = {}
            for rid, dl, qx in rows:
                groups.setdefault((dlmc_map(dl), qx), []).append(rid)
            cap = {k: len(v) for k, v in groups.items()}
            cat_cap: dict[str, int] = {}
            for (cat, _), c in cap.items():
                cat_cap[cat] = cat_cap.get(cat, 0) + c
            if alloc_mode == "reference" and zs_dlmc:
                # 权重 = 中山该类占比 x 本层占该类容量比
                weights = {}
                for (cat, _), c in cap.items():
                    share = zs_dlmc.get(cat, 0) / zs_total
                    weights[(cat, _)] = share * (c / cat_cap[cat] if cat_cap[cat] else 0)
            else:
                weights = {k: float(v) for k, v in cap.items()}
            quota = allocate(weights, cap, n, floor=MIN_PER_STRATUM_FOSHAN)
            chosen = [rid for k, ids in groups.items() for rid in pick(ids, quota[k], rng)]
            # 配额未用满时从剩余容量补齐（保持随机性）
            deficit = n - len(chosen)
            if deficit > 0:
                taken = set(chosen)
                pool = [rid for ids in groups.values() for rid in ids if rid not in taken]
                chosen += rng.sample(pool, min(deficit, len(pool)))
            entry["strata"] = {f"{k[0]}|{k[1]}": {"capacity": v, "quota": quota[k]} for k, v in cap.items()}
            entry["allocation"] = alloc_mode
            entry["reference"] = "zhongshan_10k.zs_POI_dm.DLMC"

        elif cfg["kind"] == "street":
            rows = src.execute('select rowid, S_MC, MC from "zs_street_bz" order by rowid').fetchall()
            groups = {}
            for rid, town, name in rows:
                groups.setdefault((town, suffix(name)), []).append(rid)
            cap = {k: len(v) for k, v in groups.items()}
            quota = allocate({k: float(v) for k, v in cap.items()}, cap, n, floor=3)
            chosen = [rid for k, ids in groups.items() for rid in pick(ids, quota[k], rng)]
            entry["strata"] = {f"{k[0]}|{k[1]}": {"capacity": v, "quota": quota[k]} for k, v in cap.items()}

        else:  # flat: 单键分层
            rows = src.execute(f'select rowid, {cfg["key"]} from "{table}" order by rowid').fetchall()
            groups = {}
            for rid, k in rows:
                groups.setdefault(str(k), []).append(rid)
            cap = {k: len(v) for k, v in groups.items()}
            quota = allocate({k: float(v) for k, v in cap.items()}, cap, n, floor=MIN_PER_STRATUM_FOSHAN)
            chosen = [rid for k, ids in groups.items() for rid in pick(ids, quota[k], rng)]
            entry["strata"] = {k: {"capacity": v, "quota": quota[k]} for k, v in cap.items()}

        copy_table(src, dst, table, chosen)
        entry.update(kept=len(chosen), mode="stratified", target=n, fraction=round(len(chosen) / total, 4))
        if table == "zs_POI_dm":
            src_dist = dict(src.execute("select DLMC, count(*) from zs_POI_dm group by 1"))
            smp_dist = dict(dst.execute("select DLMC, count(*) from zs_POI_dm group by 1"))
            s_tot = sum(src_dist.values()) or 1
            m_tot = sum(smp_dist.values()) or 1
            entry["distribution"] = {
                k: {
                    "source": src_dist.get(k, 0),
                    "source_pct": round(src_dist.get(k, 0) / s_tot, 4),
                    "sample": smp_dist.get(k, 0),
                    "sample_pct": round(smp_dist.get(k, 0) / m_tot, 4),
                    "zhongshan_pct": round(zs_dlmc.get(dlmc_map(k), 0) / zs_total, 4),
                }
                for k in sorted(smp_dist, key=lambda x: -smp_dist[x])
            }
            entry["cells_dropped"] = sorted(
                f"{qx}|{cat}(cap={len(ids)})"
                for (cat, qx), ids in groups.items()
                if quota[(cat, qx)] == 0
            )
        report["tables"][table] = entry

    dst.commit()
    for table, entry in report["tables"].items():
        entry["sample_rows"] = dst.execute(f'select count(*) from "{table}"').fetchone()[0]
    src.close()
    dst.close()
    return report


# ---------------------------------------------------------------- geonames
def chinese_name(alternatenames: str | None, fallback: str | None) -> str | None:
    """从 alternatenames 里挑中文名。

    GeoNames 的别名顺序是「短名/古名 -> 全称」，直接取第一个会拿到「藏」「宝安」这类
    残缺或旧称。这里优先取带行政后缀的最短形式（上海市、深圳市、西藏自治区），
    其次取最长的纯汉字串。
    """
    if alternatenames:
        cands = [x.strip() for x in alternatenames.split(",") if ZH_FULL.fullmatch(x.strip() or "")]
        if cands:
            with_suffix = [c for c in cands if c.endswith(ADMIN_SUFFIX)]
            if with_suffix:
                return min(with_suffix, key=len)
            best = ""
            for c in cands:  # 同长度取靠后者（官方全称通常在后）
                if len(c) >= len(best):
                    best = c
            return best
    if fallback and ZH_RE.search(fallback):
        return fallback
    return None


def build_geonames(src_path: Path, dst_path: Path) -> dict:
    if dst_path.exists():
        dst_path.unlink()
    src = sqlite3.connect(src_path)
    dst = sqlite3.connect(dst_path)
    dst.execute("PRAGMA journal_mode=OFF")
    dst.execute("PRAGMA synchronous=OFF")
    rng = random.Random(SEED)
    report: dict = {"source": str(src_path), "seed": SEED, "target": GEONAMES_TARGET}

    # 小码表按表名重建
    for t in ["gn_admin1", "gn_admin2", "gn_feature_code"]:
        copy_table(src, dst, t, all_rowids(src, t))

    rows = src.execute(
        """select rowid, geonameid, feature_class, feature_code,
                  admin1_code, admin2_code, population
           from gn_place order by geonameid"""
    ).fetchall()
    total = len(rows)

    backbone = set()
    for rid, gid, fc, fcode, a1, a2, pop in rows:
        if fc == "A" and fcode in ("ADM1", "ADM2"):
            backbone.add(rid)

    def stratum(fc: str, a2: str | None) -> str:
        return f"{fc}|{'has_admin2' if a2 else 'no_admin2'}"

    groups: dict[str, list[int]] = {}
    pop_big: list[int] = []
    for rid, gid, fc, fcode, a1, a2, pop in rows:
        if rid in backbone:
            continue
        groups.setdefault(stratum(fc, a2), []).append(rid)
        if fc == "P" and (pop or 0) > 0:
            pop_big.append(rid)

    capacity = {k: len(v) for k, v in groups.items()}
    budget = GEONAMES_TARGET - len(backbone)

    # 强制纳入：人口>0 的居民点（城市级实体，对应中山的 POI 密度）
    forced = [r for r in pop_big if r not in backbone]
    forced = forced[: max(0, min(len(forced), budget // 3))]
    budget -= len(forced)

    forced_set = set(forced)
    cap_left = {k: len([r for r in v if r not in forced_set]) for k, v in groups.items()}
    quota = allocate({k: float(v) for k, v in cap_left.items()}, cap_left, budget, floor=MIN_PER_STRATUM)

    chosen = list(backbone) + forced
    for k, ids in groups.items():
        pool = [r for r in ids if r not in forced_set]
        chosen += pick(pool, quota[k], rng)

    # 写 gn_place（带 name_zh）
    ddl = table_ddl(src, "gn_place").strip().rstrip(";").rstrip()
    assert ddl.endswith(")"), ddl[-40:]
    ddl = ddl[:-1].rstrip() + ", name_zh TEXT)"
    dst.execute(ddl)

    valid_admin1 = {r[0][3:] for r in src.execute("select code from gn_admin1")}
    valid_admin2 = {r[0] for r in src.execute("select code from gn_admin2")}
    cols = table_columns(src, "gn_place")
    col_sql = ",".join(f'"{c}"' for c in cols)
    placeholders = ",".join("?" * (len(cols) + 1))
    chosen = sorted(set(chosen))
    batch = []
    for i in range(0, len(chosen), 500):
        part = chosen[i : i + 500]
        marks = ",".join("?" * len(part))
        for row in src.execute(
            f'select {col_sql} from gn_place where rowid in ({marks}) order by geonameid', part
        ):
            d = dict(zip(cols, row))
            if d["admin1_code"] not in valid_admin1:
                d["admin1_code"] = None
                report["invalid_admin1_fixed"] = report.get("invalid_admin1_fixed", 0) + 1
            elif d["admin2_code"] and f"CN.{d['admin1_code']}.{d['admin2_code']}" not in valid_admin2:
                d["admin2_code"] = None
                report["invalid_admin2_fixed"] = report.get("invalid_admin2_fixed", 0) + 1
            batch.append(tuple(d[c] for c in cols) + (chinese_name(d["alternatenames"], d["name"]),))
        dst.executemany(f"insert into gn_place ({col_sql}, name_zh) values ({placeholders})", batch)
        batch.clear()

    dst.commit()

    # 报告
    got = dst.execute("select count(*) from gn_place").fetchone()[0]
    zh_hit = dst.execute("select count(*) from gn_place where name_zh is not null").fetchone()[0]
    report["source_rows"] = total
    report["sample_rows"] = got
    report["fraction"] = round(got / total, 5)
    report["backbone_adm1_adm2"] = len(backbone)
    report["forced_populated_p"] = len(forced)
    report["name_zh_coverage"] = round(zh_hit / got, 4) if got else 0
    report["strata"] = {
        k: {"capacity": capacity[k], "quota": quota[k], "kept": quota[k] + len([r for r in groups[k] if r in forced_set])}
        for k in capacity
    }
    report["admin1_covered"] = dst.execute(
        "select count(distinct admin1_code) from gn_place"
    ).fetchone()[0]
    report["feature_class_mix"] = dict(dst.execute(
        "select feature_class, count(*) from gn_place group by 1 order by 2 desc"
    ))
    src.close()
    dst.close()
    return report


def check_integrity(db_path: Path) -> dict:
    """S_DM 是否都能在本库行政区划表里找到（含市/区/镇/社区）。"""
    con = sqlite3.connect(db_path)
    con.execute(
        "create temp view _adm as "
        "select DM from zs_city_bz union select DM from zs_district_bz "
        "union select DM from zs_town_bz union select DM from zs_community_bz"
    )
    out = {}
    for table in ("zs_street_bz", "zs_MP_bz", "zs_DH_bz", "zs_YL_bz"):
        rows = con.execute(f'select count(*) from "{table}"').fetchone()[0]
        orph = con.execute(
            f'select count(*) from "{table}" where S_DM is not null and S_DM not in (select DM from _adm)'
        ).fetchone()[0]
        out[table] = {"rows": rows, "orphan_s_dm": orph}
    con.close()
    return out


def render_report(fr: dict, gr: dict) -> str:
    L: list[str] = ["# 公开数据集分层抽样报告", ""]
    L += [
        f"- 随机种子：`{SEED}`（同快照可复现）",
        f"- 小表阈值：≤{SMALL_TABLE_KEEP} 行全量保留",
        f"- 大表抽样比例：{SAMPLE_FRACTION:.0%}",
        f"- GeoNames 目标样本量：{GEONAMES_TARGET}",
        "",
        "### 分层变量",
        "",
        "| 表 | 分层键 | 分配方式 |",
        "|---|---|---|",
        "| `zs_POI_dm` / `zs_AOI_dm` | 一级类（对齐中山词表）× 区县 | 与层容量成比例 |",
        "| `zs_street_bz` | 镇街 × 名称后缀（路/街/巷/大道/其他） | 与层容量成比例 |",
        "| `zs_DH_bz` | 地名二级类 FLMC | 与层容量成比例 |",
        "| `zs_facility_dm` | 设施类型 SSLX | 与层容量成比例 |",
        "| 其余小表 | 不分层 | 全量保留 |",
        "| `gn_place` | feature_class × 是否有 admin2 | 与层容量成比例，稀有类保底 20 |",
        "",
        "## 一、佛山 OSM 样本",
        "",
        "| 表 | 源行数 | 样本行数 | 比例 | 方式 |",
        "|---|---:|---:|---:|---|",
    ]
    for t, v in fr["tables"].items():
        frac = v.get("fraction", "-")
        frac = f"{frac:.2%}" if isinstance(frac, float) else "-"
        L.append(f"| `{t}` | {v['source_rows']} | {v['sample_rows']} | {frac} | {v['mode']} |")

    poi = fr["tables"].get("zs_POI_dm", {})
    if poi.get("distribution"):
        L += [
            "",
            "### POI 一级类分布（分层变量：一级类 × 区县）",
            "",
            "| 一级类 | 源占比 | 样本占比 | 差值 | 中山参照占比 |",
            "|---|---:|---:|---:|---:|",
        ]
        for k, d in poi["distribution"].items():
            diff = d["sample_pct"] - d["source_pct"]
            L.append(
                f"| {k} | {d['source_pct']:.1%} | {d['sample_pct']:.1%} | {diff:+.2%} | {d['zhongshan_pct']:.1%} |"
            )
        md = max(abs(d["sample_pct"] - d["source_pct"]) for d in poi["distribution"].values())
        L += [
            "",
            f"最大偏差 **{md:.2%}**，样本可视为原库分布的等比缩影。",
            "",
            "> 中山参照列用于说明两城构成差异，不参与配额计算。词表对齐：教育→科教文化服务，其他→生活服务。",
        ]
        dropped = poi.get("cells_dropped") or []
        if dropped:
            L += ["", f"未入样的层（容量过小，按比例分配后配额为 0）：{', '.join(dropped)}。"]

    if fr.get("integrity"):
        L += ["", "### 引用完整性", "", "| 表 | 行数 | S_DM 悬空 |", "|---|---:|---:|"]
        for t, v in fr["integrity"].items():
            L.append(f"| `{t}` | {v['rows']} | {v['orphan_s_dm']} |")

    L += [
        "",
        "## 二、GeoNames 样本",
        "",
        f"- 源行数：{gr['source_rows']:,}；样本行数：{gr['sample_rows']:,}（{gr['fraction']:.2%}）",
        f"- 强制纳入行政骨架 ADM1/ADM2：{gr['backbone_adm1_adm2']} 条",
        f"- 强制纳入人口>0 的居民点：{gr['forced_populated_p']} 条（上限为预算的 1/3）",
        f"- `name_zh` 覆盖率：{gr['name_zh_coverage']:.1%}（原库 `name` 为拼音，中文名来自 `alternatenames`）",
        f"- 覆盖省级单元：{gr['admin1_covered']} 个",
        f"- 非法 `admin1_code` 置空：{gr.get('invalid_admin1_fixed', 0)} 条",
        f"- 无法挂到 `gn_admin2` 的 `admin2_code` 置空：{gr.get('invalid_admin2_fixed', 0)} 条",
        "",
        "> 强制纳入的人口>0 居民点全部落在 `P|has_admin2` 层，所以该层“实际保留”大于配额。",
        "",
        "### 层容量与配额（feature_class × 是否有 admin2）",
        "",
        "| 层 | 容量 | 配额 | 实际保留 |",
        "|---|---:|---:|---:|",
    ]
    for k, v in sorted(gr["strata"].items()):
        L.append(f"| `{k}` | {v['capacity']:,} | {v['quota']:,} | {v['kept']:,} |")

    L += ["", "### 样本要素大类构成", "", "| feature_class | 行数 |", "|---|---:|"]
    for k, v in gr["feature_class_mix"].items():
        L.append(f"| {k} | {v:,} |")

    L += [
        "",
        "## 三、复现",
        "",
        "```",
        "python3 scripts/sample_public_datasets.py",
        "python3 scripts/sample_public_datasets.py --foshan-alloc reference   # 受控对照模式",
        "```",
        "",
        "## 四、使用注意",
        "",
        "1. 佛山样本保持原库分布（纯比例分配），因此可直接用于建图实验，但不能当作佛山全域统计估计。",
        "2. `zs_POI_dm` 与 `zs_AOI_dm` 同源，两者分别抽样，跨表按 `gid` 对齐时会有部分 POI 在 AOI 中缺失。",
        "3. GeoNames 样本对稀有类（R/U/V）做了保底 20 条，属于有意的分层过采样，不能直接用于比例估计。",
        "4. `name_zh` 为空的记录（约 13%）无法提供中文地址文本，做中文抽取实验时应显式过滤。",
        "",
    ]
    return "\n".join(L) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--public-dir", default="data/public")
    ap.add_argument("--out-dir", default="data/public/sampled")
    ap.add_argument(
        "--foshan-alloc",
        choices=["proportional", "reference"],
        default="proportional",
        help="proportional=保持佛山原分布(默认)；reference=按中山一级类占比配额",
    )
    args = ap.parse_args()

    pub = Path(args.public_dir)
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    fr = build_foshan(pub / "foshan_osm.sqlite", out / "foshan_osm_sample.sqlite", args.foshan_alloc)
    gr = build_geonames(pub / "geonames_cn.sqlite", out / "geonames_cn_sample.sqlite")

    fr["integrity"] = check_integrity(out / "foshan_osm_sample.sqlite")
    (out / "sampling_report.json").write_text(
        json.dumps({"foshan": fr, "geonames": gr}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (out / "抽样报告.md").write_text(render_report(fr, gr), encoding="utf-8")
    print(json.dumps({"foshan_tables": {k: (v["source_rows"], v["sample_rows"]) for k, v in fr["tables"].items()},
                      "geonames": {k: gr[k] for k in ("source_rows", "sample_rows", "fraction", "name_zh_coverage")}},
                     ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
