def get_create_tables_queries(graph_name, backend):
    """Format a SQlite CREATE TABLE statement with the name of the RDF graph to insert."""
    if backend == "sqlite":
        return [(
            f"CREATE TABLE {graph_name} ("
            f"subject TEXT, "
            f"predicate TEXT, "
            f"object TEXT);"
        )]
    elif backend == "sqlite-catalog":
        return [
            (
                f"CREATE TABLE IF NOT EXISTS catalog ("
                f"id INTEGER PRIMARY KEY, "
                f"value TEXT);"
            ),
            (
                f"CREATE UNIQUE INDEX IF NOT EXISTS catalog_locate_index ON catalog (value);"
            ),
            # (
            #     f"CREATE INDEX IF NOT EXISTS catalog_extract_index ON catalog (id);"
            # ),
            (
                f"CREATE TABLE {graph_name} ("
                f"subject INTEGER, "
                f"predicate INTEGER, "
                f"object INTEGER);"
            )
        ]
    else:
        raise Exception(f"Unknown backend for SQlite: {backend}")


def get_create_indexes_queries(graph_name, backend):
    """Format all SQlite CREATE INDEXES statements with the name of the RDF graph to insert."""
    if backend == "sqlite" or backend == "sqlite-catalog":
        return [
            f"CREATE UNIQUE INDEX IF NOT EXISTS {graph_name}_spo_index ON {graph_name} (subject,predicate,object);",
            f"CREATE UNIQUE INDEX IF NOT EXISTS {graph_name}_osp_index ON {graph_name} (object,subject,predicate);",
            f"CREATE UNIQUE INDEX IF NOT EXISTS {graph_name}_pos_index ON {graph_name} (predicate,object,subject);"
        ]
    else:
        raise Exception(f"Unknown backend for SQlite: {backend}")


def get_insert_into_query(graph_name):
    """Get an INSERT INTO statement compatible with the "executemany" function of SQlite to support the bulk loading."""
    return f"INSERT INTO {graph_name} (subject,predicate,object) VALUES (?, ?, ?) ON CONFLICT DO NOTHING"


def get_insert_into_catalog_query():
    """Get an INSERT INTO statement compatible with the "executemany" function of SQlite to support the bulk loading."""
    return f"INSERT INTO catalog (value) VALUES (?) ON CONFLICT DO NOTHING"


def get_select_identifier_query():
    """Get a SELECT statement to retrieve the identifier of a RDF term."""
    return f"SELECT id FROM catalog WHERE value = ?"


def get_analyze_query(graph_name):
    """Format an ANALYZE query with the name of the inserted RDF graph."""
    return f"ANALYZE {graph_name}"
