IGNORED_RAG_COLUMNS = {
    "geom",
    "gid",
    "s_guid",
    "shape_length",
    "shape_area",
}


def is_ignored_rag_column(column_name):
    return str(column_name or "").strip().lower() in IGNORED_RAG_COLUMNS


def is_ignored_rdf_property(column_name):
    return is_ignored_rag_column(column_name)
