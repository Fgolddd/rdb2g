# 去除 `schema.org` 依赖改造方案

## 1. 目标

生成 `TTL` 时不再出现 `schema.org` 相关前缀或 URI（包括 `schema:Thing`、`schema:name`）。

## 2. 改造范围

主要修改文件：`graph_builder.py`

当前依赖点：

1. `self.SCHEMA = Namespace("http://schema.org/")`
2. `self.g.bind("schema", self.SCHEMA)`
3. `self.g.add((subject_uri, RDF.type, self.SCHEMA.Thing))`
4. 复合主键场景：`self.SCHEMA.name`

## 3. 改造方案

### 3.1 引入本地本体命名空间

- 新增本地命名空间（示例）：`http://example.org/ontology/`
- 绑定前缀（示例）：`onto`

建议：

- 类 URI：`onto:{table_name}`（每张表对应一个本地类）
- 通用属性 URI：`onto:name`

### 3.2 替换实体类型声明

将：

- `rdf:type schema:Thing`

替换为：

- `rdf:type onto:{table_name}`

效果：类型语义保留，同时完全去除 `schema.org` 依赖。

### 3.3 替换复合主键名称属性

将：

- `schema:name`

替换为：

- `onto:name`

### 3.4 保持列映射逻辑不变

- 列级属性仍按 `mapping` 输出，不在本次改造中改变。
- 若后续需要彻底规范 URI，可另做“映射统一成 URI（非中文标签）”改造。

## 4. 兼容性与风险

1. **历史兼容性**：旧 TTL 中若依赖 `schema:Thing` 的下游查询需同步改写。
2. **语义一致性**：实体类型从通用类切换为本地类，不影响三元组结构。
3. **风险等级**：低（仅本地命名空间替换，影响集中在导出语义层）。

## 5. 验证步骤

1. 重新生成：`poi.ttl`
2. 检查不应出现：`schema.org`
3. 检查应出现：`@prefix onto:` 与 `rdf:type onto:...`
4. 抽样确认：主语 URI、属性三元组数量与改造前同量级

可用命令（示例）：

```bash
rg "schema.org|schema1:" data/ttl/poi.ttl
rg "@prefix onto:|rdf:type onto:" data/ttl/poi.ttl
```

## 6. 可选增强（非必须）

- 增加 CLI 参数：`--type-namespace`，支持运行时切换类型命名空间。
- 增加 `--entity-class-mode`：`table`（按表建类）/ `generic`（统一 `onto:Entity`）。
