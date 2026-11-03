import csv
import re
from collections import defaultdict
from pathlib import Path

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, RDFS


SCHEMA_NAME = URIRef("http://schema.org/name")
ZS_NS = "http://example.org/zhongshan/"
ZS_SOURCE_TABLE = URIRef(ZS_NS + "sourceTable")
ZS_ENTITY_TYPE = URIRef(ZS_NS + "entityType")
ZS_ENTITY_ID = URIRef(ZS_NS + "entityId")

RELATION_PREDICATES = {
    ZS_NS + "managedByPoliceOrg": "MANAGED_BY_POLICE_ORG",
    ZS_NS + "parentPoliceOrg": "PARENT_POLICE_ORG",
    ZS_NS + "parentAdminArea": "PARENT_ADMIN_AREA",
    ZS_NS + "locatedInAdminArea": "LOCATED_IN_ADMIN_AREA",
    ZS_NS + "partOfAddressEntity": "PART_OF_ADDRESS_ENTITY",
    ZS_NS + "locatedOnStreet": "LOCATED_ON_STREET",
}

SKIP_PROPERTY_PREDICATES = {
    str(RDF.type),
    str(RDFS.label),
    str(SCHEMA_NAME),
    str(ZS_SOURCE_TABLE),
    str(ZS_ENTITY_TYPE),
    str(ZS_ENTITY_ID),
}


def _safe_key(text):
    tail = str(text or "").strip().rsplit("#", 1)[-1].rsplit("/", 1)[-1]
    tail = tail.replace(".", "__")
    tail = re.sub(r"[^0-9A-Za-z_\u4e00-\u9fff]+", "_", tail)
    tail = tail.strip("_")
    if not tail:
        tail = "value"
    if tail[0].isdigit():
        tail = "p_" + tail
    return tail


def _neo4j_label(text):
    label = _safe_key(text)
    return label[:1].upper() + label[1:]


def _literal_text(value):
    if isinstance(value, Literal):
        return str(value)
    return str(value)


def _first_literal(graph, subject, predicate):
    for value in graph.objects(subject, predicate):
        if isinstance(value, Literal):
            return str(value)
    return ""


def _entity_label(graph, subject):
    entity_type = _first_literal(graph, subject, ZS_ENTITY_TYPE)
    if entity_type:
        return _neo4j_label(entity_type)
    for obj in graph.objects(subject, RDF.type):
        obj_text = str(obj)
        if obj_text.startswith(ZS_NS):
            return _neo4j_label(obj_text.rsplit("/", 1)[-1])
    return "Entity"


def _collect_nodes_and_relationships(graph):
    subjects = set(graph.subjects(ZS_SOURCE_TABLE, None))
    nodes = {}
    property_keys = set()
    relationships = []

    for subject in subjects:
        uri = str(subject)
        props = {
            "uri:ID": uri,
            ":LABEL": _entity_label(graph, subject),
            "uri": uri,
            "name": _first_literal(graph, subject, SCHEMA_NAME) or _first_literal(graph, subject, RDFS.label),
            "sourceTable": _first_literal(graph, subject, ZS_SOURCE_TABLE),
            "entityType": _first_literal(graph, subject, ZS_ENTITY_TYPE),
            "entityId": _first_literal(graph, subject, ZS_ENTITY_ID),
        }

        for pred, obj in graph.predicate_objects(subject):
            pred_text = str(pred)
            if pred_text in SKIP_PROPERTY_PREDICATES or pred_text in RELATION_PREDICATES:
                continue
            if not isinstance(obj, Literal):
                continue
            key = _safe_key(pred_text)
            if key in props and props[key]:
                continue
            props[key] = _literal_text(obj)
            property_keys.add(key)
        nodes[uri] = props

    rel_id = 0
    for subject, pred, obj in graph:
        rel_type = RELATION_PREDICATES.get(str(pred))
        if not rel_type:
            continue
        if str(subject) not in nodes or str(obj) not in nodes:
            continue
        rel_id += 1
        relationships.append({
            ":START_ID": str(subject),
            ":END_ID": str(obj),
            ":TYPE": rel_type,
            "uri": f"rel:{rel_id}",
        })
    return nodes, property_keys, relationships


def _write_nodes_csv(path, nodes, property_keys):
    base_fields = ["uri:ID", ":LABEL", "uri", "name", "sourceTable", "entityType", "entityId"]
    fields = base_fields + sorted(key for key in property_keys if key not in base_fields)
    with Path(path).open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for uri in sorted(nodes):
            writer.writerow(nodes[uri])


def _write_relationships_csv(path, relationships):
    fields = [":START_ID", ":END_ID", ":TYPE", "uri"]
    with Path(path).open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for rel in relationships:
            writer.writerow(rel)


def _write_schema_cypher(path):
    content = """CREATE CONSTRAINT rdb2g_entity_uri IF NOT EXISTS
FOR (n:Entity) REQUIRE n.uri IS UNIQUE;

CREATE INDEX rdb2g_entity_type IF NOT EXISTS
FOR (n:Entity) ON (n.entityType);

CREATE INDEX rdb2g_source_table IF NOT EXISTS
FOR (n:Entity) ON (n.sourceTable);
"""
    Path(path).write_text(content, encoding="utf-8")


def _write_import_cypher(path, nodes_file="nodes.csv", relationships_file="relationships.csv"):
    content = f"""// Run from Neo4j Browser/cypher-shell after placing CSV files in Neo4j import directory.
// This imports a Cypher-friendly property graph exported from rdb2g TTL.

LOAD CSV WITH HEADERS FROM 'file:///{nodes_file}' AS row
MERGE (n:Entity {{uri: row.uri}})
SET n += row
WITH n, row
CALL apoc.create.addLabels(n, [row.`:LABEL`]) YIELD node
RETURN count(node);

LOAD CSV WITH HEADERS FROM 'file:///{relationships_file}' AS row
MATCH (s:Entity {{uri: row.`:START_ID`}})
MATCH (t:Entity {{uri: row.`:END_ID`}})
CALL apoc.create.relationship(s, row.`:TYPE`, {{uri: row.uri}}, t) YIELD rel
RETURN count(rel);
"""
    Path(path).write_text(content, encoding="utf-8")


def export_neo4j_csv(ttl_file, out_dir):
    ttl_file = Path(ttl_file)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    graph = Graph()
    graph.parse(str(ttl_file), format="turtle")
    nodes, property_keys, relationships = _collect_nodes_and_relationships(graph)

    nodes_path = out_dir / "nodes.csv"
    relationships_path = out_dir / "relationships.csv"
    schema_path = out_dir / "schema.cypher"
    import_path = out_dir / "import.cypher"

    _write_nodes_csv(nodes_path, nodes, property_keys)
    _write_relationships_csv(relationships_path, relationships)
    _write_schema_cypher(schema_path)
    _write_import_cypher(import_path)

    summary = {
        "ttl_file": str(ttl_file),
        "out_dir": str(out_dir),
        "node_count": len(nodes),
        "relationship_count": len(relationships),
        "node_property_count": len(property_keys),
        "nodes_csv": str(nodes_path),
        "relationships_csv": str(relationships_path),
        "schema_cypher": str(schema_path),
        "import_cypher": str(import_path),
    }
    print(f"Exported Neo4j CSV nodes={len(nodes)} relationships={len(relationships)} out={out_dir}")
    return summary
