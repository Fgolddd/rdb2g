#!/usr/bin/env python3
"""Adapt LLM4VKG's simple PostgreSQL-flavoured R2RML views for SQLite.

Only quoted SELECT * FROM "schema"."table" views are accepted. This writes a
separate mapping and validates references and execution in read-only SQLite;
it does not materialize RDF or claim an end-to-end reproduction.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import sqlite3

from rdflib import Graph, Literal, RDF
from rdflib.namespace import Namespace

RR = Namespace("http://www.w3.org/ns/r2rml#")
VIEW_SQL = re.compile(
    r'\s*SELECT\s+\*\s+FROM\s+"((?:[^"]|"")+)"\s*\.\s*"((?:[^"]|"")+)"\s*;?\s*',
    re.IGNORECASE,
)


def unquote_identifier(value: str) -> str:
    return value.replace('""', '"')


def parse_view(sql: str, expected_schema: str) -> tuple[str, str]:
    match = VIEW_SQL.fullmatch(sql)
    if not match:
        raise ValueError(f"Unsupported or unsafe R2RML SQL view: {sql!r}")
    schema, table = (unquote_identifier(part) for part in match.groups())
    if schema != expected_schema:
        raise ValueError(f"SQL schema {schema!r} does not match database stem {expected_schema!r}")
    if not table or "\x00" in table:
        raise ValueError("Invalid table identifier")
    quoted = '"' + table.replace('"', '""') + '"'
    return table, f"SELECT * FROM {quoted}"


def adapt_and_validate(database: Path, mapping: Path, adapted_mapping: Path, report_path: Path) -> dict:
    source = Graph()
    source.parse(str(mapping), format="turtle")
    adapted = Graph()
    for prefix, namespace in source.namespaces():
        adapted.bind(prefix, namespace)
    for triple in source:
        adapted.add(triple)

    db_uri = f"file:{database.resolve()}?mode=ro"
    queries = []
    with sqlite3.connect(db_uri, uri=True) as conn:
        conn.execute("PRAGMA query_only = ON")
        available = {row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )}
        triples_maps = sorted(set(source.subjects(RDF.type, RR.TriplesMap)), key=str)
        if not triples_maps:
            raise ValueError("No rr:TriplesMap resources found")

        for triples_map in triples_maps:
            logical_tables = list(source.objects(triples_map, RR.logicalTable))
            if len(logical_tables) != 1:
                raise ValueError(f"{triples_map}: expected exactly one rr:logicalTable")
            logical_table = logical_tables[0]
            sql_values = list(source.objects(logical_table, RR.sqlQuery))
            if len(sql_values) != 1:
                raise ValueError(f"{triples_map}: expected exactly one rr:sqlQuery")
            table, sqlite_sql = parse_view(str(sql_values[0]), database.stem)
            if table not in available:
                raise ValueError(f"{triples_map}: source table {table!r} does not exist")

            cursor = conn.execute(sqlite_sql)
            result_columns = [item[0] for item in cursor.description or []]
            column_names = set(result_columns)
            row_count = 0
            while True:
                batch = cursor.fetchmany(2048)
                if not batch:
                    break
                row_count += len(batch)

            subject_maps = list(source.objects(triples_map, RR.subjectMap))
            if len(subject_maps) != 1:
                raise ValueError(f"{triples_map}: expected exactly one rr:subjectMap")
            templates = list(source.objects(subject_maps[0], RR.template))
            if len(templates) != 1:
                raise ValueError(f"{triples_map}: expected exactly one subject rr:template")
            template_columns = re.findall(r"\{([^{}]+)\}", str(templates[0]))
            missing = sorted(set(template_columns) - column_names)
            if missing:
                raise ValueError(f"{triples_map}: subject template columns absent from {table}: {missing}")

            poms = list(source.objects(triples_map, RR.predicateObjectMap))
            for pom in poms:
                object_maps = list(source.objects(pom, RR.objectMap))
                if len(object_maps) != 1:
                    raise ValueError(f"{triples_map}: expected one rr:objectMap per predicate map")
                columns = list(source.objects(object_maps[0], RR.column))
                object_templates = list(source.objects(object_maps[0], RR.template))
                if len(columns) == 1 and not object_templates:
                    references = [str(columns[0])]
                elif len(object_templates) == 1 and not columns:
                    references = re.findall(r"\{([^{}]+)\}", str(object_templates[0]))
                    if not references:
                        raise ValueError(f"{triples_map}: empty rr:template reference")
                else:
                    raise ValueError(f"{triples_map}: expected one rr:column or rr:template object map")
                missing_columns = sorted(set(references) - column_names)
                if missing_columns:
                    raise ValueError(f"{triples_map}: object references absent columns in {table}: {missing_columns}")

            adapted.remove((logical_table, RR.sqlQuery, None))
            adapted.add((logical_table, RR.sqlQuery, Literal(sqlite_sql)))
            queries.append({
                "triples_map": str(triples_map),
                "source_schema_qualifier": database.stem,
                "source_table": table,
                "sqlite_sql": sqlite_sql,
                "row_count": row_count,
                "column_count": len(result_columns),
                "subject_template_columns": template_columns,
                "predicate_object_map_count": len(poms),
            })

    adapted_mapping.parent.mkdir(parents=True, exist_ok=True)
    adapted.serialize(destination=str(adapted_mapping), format="turtle")
    report = {
        "method": "LLM4VKG",
        "status": "sqlite_query_and_reference_validation_completed",
        "source_mapping": str(mapping),
        "source_database": str(database),
        "adapted_mapping": str(adapted_mapping),
        "adaptation": "Removed the PostgreSQL-style database qualifier from strictly parsed SELECT * views; each rewritten SELECT was executed through read-only SQLite.",
        "validation_scope": [
            "R2RML parses as Turtle and contains triples maps",
            "Only quoted SELECT * FROM schema.table views are accepted",
            "Source tables exist and rewritten queries execute in read-only SQLite",
            "Subject template and rr:column references exist in query result columns",
        ],
        "not_run": ["R2RML processor or RDF materialization", "Neo4j import", "semantic mapping quality evaluation"],
        "full_pipeline_reproduction": False,
        "quality_scored": False,
        "queries": queries,
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--mapping", type=Path, required=True)
    parser.add_argument("--adapted-mapping", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    report = adapt_and_validate(args.db, args.mapping, args.adapted_mapping, args.report)
    print(json.dumps({"status": report["status"], "triples_maps": len(report["queries"]),
                      "rows_by_table": {q["source_table"]: q["row_count"] for q in report["queries"]}},
                     ensure_ascii=False))


if __name__ == "__main__":
    main()
