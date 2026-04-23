# data/company SQL 表字段一致性检查报告

- 检查时间: 2026-03-27 12:29:47
- 检查目录: `/home/lyx/llm_graph/rdb2g/data/company`
- 检查方式: 解析 `INSERT INTO ... (列...) VALUES` 的列清单，并按表名聚合对比

## 结论

- 共识别到 **5** 个表。
- 各表字段是否完全相同（按字段集合）: **否**。
- 结论说明: 不同表的字段设计不同，未发现“所有表字段完全一致”的情况。

## 表内一致性（同一表在不同文件/多次 INSERT 的列是否一致）

- 所有表在其全部 INSERT 语句中，列定义均一致。

## 各表字段概览


| 表名                    | 列数  | 出现文件                                       | INSERT 数量 |
| --------------------- | --- | ------------------------------------------ | --------- |
| `public."zs_AOI_dm"`  | 27  | `_zs_AOI_dm__202603161342.sql`, `poi.sql`  | 196       |
| `public."zs_FJ_bz"`   | 32  | `poi.sql`, `zs_FJ_bz.sql`                  | 200       |
| `public."zs_MP_bz"`   | 32  | `poi.sql`, `zs_MP_bz.sql`                  | 200       |
| `public."zs_POI_dm"`  | 25  | `poi.sql`                                  | 150       |
| `public.zs_street_bz` | 28  | `poi.sql`, `zs_street_bz_202603161341.sql` | 200       |


## 关键差异

- `public."zs_AOI_dm"` vs `public."zs_POI_dm"`: 字段集合不同（差异字段数 2）。
  - 仅 `public."zs_AOI_dm"` 有: `"Shape_Area"`, `"Shape_Length"`
- `public."zs_FJ_bz"` vs `public."zs_MP_bz"`: 字段集合不同（差异字段数 6）。
  - 仅 `public."zs_FJ_bz"` 有: `"FJH"`, `"FJHBM"`, `"FJHQC"`
  - 仅 `public."zs_MP_bz"` 有: `"DHBM"`, `"MPH"`, `"MPQC"`
- `public."zs_FJ_bz"` vs `public.zs_street_bz`: 字段集合不同（差异字段数 14）。
  - 仅 `public."zs_FJ_bz"` 有: `"DLMC"`, `"DZLX"`, `"FJH"`, `"FJHBM"`, `"FJHQC"`, `"SYPL"` ...
  - 仅 `public.zs_street_bz` 有: `"BM"`, `"MC"`, `"S_GADM"`, `"S_GAMC"`, `"Shape_Length"`

## 全量字段列表（按表）

### `public."zs_AOI_dm"` (27 列)

- `gid`
- `geom`
- `"GUID"`
- `"MC"`
- `"DZ"`
- `"DLMC"`
- `"ZLMC"`
- `"XLMC"`
- `"SJMC"`
- `"SSJMC"`
- `"QXMC"`
- `"LXFS"`
- `"BZ_GUID"`
- `"BZDZMC"`
- `"ZDGACM"`
- `"ZDGADM"`
- `"SYZT"`
- `"SLSJ"`
- `"QYSJ"`
- `"TYSJ"`
- `"GXSJ"`
- `"DZLX"`
- `ing_2000`
- `lat_2000`
- `"BZ"`
- `"Shape_Length"`
- `"Shape_Area"`

### `public."zs_FJ_bz"` (32 列)

- `gid`
- `geom`
- `"GUID"`
- `"DM"`
- `"FJH"`
- `"FJHQC"`
- `"FJHBM"`
- `"FLMC"`
- `"FLDM"`
- `"DLMC"`
- `"ZLMC"`
- `"XLMC"`
- `"CJMC"`
- `"CJDM"`
- `"GAMC"`
- `"GADM"`
- `"XNBS"`
- `"SYPL"`
- `"S_GUID"`
- `"S_DM"`
- `"S_MC"`
- `"S_FLMC"`
- `"S_FLDM"`
- `"S_CJMC"`
- `"S_CJDM"`
- `"SYZT"`
- `"SLSJ"`
- `"QYSJ"`
- `"TYSJ"`
- `"GXSJ"`
- `"DZLX"`
- `"BZ"`

### `public."zs_MP_bz"` (32 列)

- `gid`
- `geom`
- `"GUID"`
- `"DM"`
- `"MPH"`
- `"MPQC"`
- `"DHBM"`
- `"FLMC"`
- `"FLDM"`
- `"DLMC"`
- `"ZLMC"`
- `"XLMC"`
- `"CJMC"`
- `"CJDM"`
- `"GAMC"`
- `"GADM"`
- `"XNBS"`
- `"SYPL"`
- `"S_GUID"`
- `"S_DM"`
- `"S_MC"`
- `"S_FLMC"`
- `"S_FLDM"`
- `"S_CJMC"`
- `"S_CJDM"`
- `"SYZT"`
- `"SLSJ"`
- `"QYSJ"`
- `"TYSJ"`
- `"GXSJ"`
- `"DZLX"`
- `"BZ"`

### `public."zs_POI_dm"` (25 列)

- `gid`
- `geom`
- `"GUID"`
- `"MC"`
- `"DZ"`
- `"DLMC"`
- `"ZLMC"`
- `"XLMC"`
- `"SJMC"`
- `"SSJMC"`
- `"QXMC"`
- `"LXFS"`
- `"BZ_GUID"`
- `"BZDZMC"`
- `"ZDGACM"`
- `"ZDGADM"`
- `"SYZT"`
- `"SLSJ"`
- `"QYSJ"`
- `"TYSJ"`
- `"GXSJ"`
- `"DZLX"`
- `ing_2000`
- `lat_2000`
- `"BZ"`

### `public.zs_street_bz` (28 列)

- `gid`
- `geom`
- `"Shape_Length"`
- `"GUID"`
- `"DM"`
- `"MC"`
- `"BM"`
- `"FLMC"`
- `"FLDM"`
- `"CJMC"`
- `"CJDM"`
- `"GAMC"`
- `"GADM"`
- `"S_GUID"`
- `"S_DM"`
- `"S_MC"`
- `"S_FLMC"`
- `"S_FLDM"`
- `"S_CJMC"`
- `"S_CJDM"`
- `"S_GAMC"`
- `"S_GADM"`
- `"SYZT"`
- `"SLSJ"`
- `"QYSJ"`
- `"TYSJ"`
- `"GXSJ"`
- `"BZ"`

## 全集字段补充（所有表）

### 1) 所有表列名全集（去重，不带表名前缀）

- 全集列数: **52**
- 列表:
  - `BM`
  - `BZ`
  - `BZDZMC`
  - `BZ_GUID`
  - `CJDM`
  - `CJMC`
  - `DHBM`
  - `DLMC`
  - `DM`
  - `DZ`
  - `DZLX`
  - `FJH`
  - `FJHBM`
  - `FJHQC`
  - `FLDM`
  - `FLMC`
  - `GADM`
  - `GAMC`
  - `GUID`
  - `GXSJ`
  - `LXFS`
  - `MC`
  - `MPH`
  - `MPQC`
  - `QXMC`
  - `QYSJ`
  - `SJMC`
  - `SLSJ`
  - `SSJMC`
  - `SYPL`
  - `SYZT`
  - `S_CJDM`
  - `S_CJMC`
  - `S_DM`
  - `S_FLDM`
  - `S_FLMC`
  - `S_GADM`
  - `S_GAMC`
  - `S_GUID`
  - `S_MC`
  - `Shape_Area`
  - `Shape_Length`
  - `TYSJ`
  - `XLMC`
  - `XNBS`
  - `ZDGACM`
  - `ZDGADM`
  - `ZLMC`
  - `geom`
  - `gid`
  - `ing_2000`
  - `lat_2000`

### 2) `data/poi.json` 缺失列（基于“去重列名全集”）

- `poi.json` 已覆盖列数: **52 / 52**
- 缺失列数: **0**
- 缺失列列表: **无**

### 3) 按“表.列”口径的全集与缺失

- 全集（`表.列`）数量: **144**
- `poi.json` 缺失（`表.列`）数量: **0**

| 表 | 总列数(表.列) | poi.json 缺失 | poi.json 已覆盖 |
|---|---:|---:|---:|
| `zs_AOI_dm` | 27 | 0 | 27 |
| `zs_FJ_bz` | 32 | 0 | 32 |
| `zs_MP_bz` | 32 | 0 | 32 |
| `zs_POI_dm` | 25 | 0 | 25 |
| `zs_street_bz` | 28 | 0 | 28 |

> 说明: 当前 `poi.json` 已覆盖 5 张表的全部字段。

