# postgres_utils.py
# Author: Thomas MINIER - MIT License 2017-2019

def get_create_tables_queries(graph_name, backend):
    """Format a PostgreSQL CREATE TABLE query with the name of the RDF graph to insert."""
    if backend == "postgres":
        return [(
            f"CREATE TABLE {graph_name} ("
            f"subject TEXT, "
            f"predicate TEXT, "
            f"object TEXT);"
        )]
    elif backend == "postgres-mvcc":
        return [(
            f"CREATE TABLE {graph_name} ("
            f"subject TEXT, "
            f"predicate TEXT, "
            f"object TEXT, "
            f"insert_t abstime DEFAULT transaction_timestamp(), "
            f"delete_t abstime DEFAULT \"infinity\");"
        )]
    elif backend == "postgres-catalog":
        return [
            (
                f"CREATE TABLE IF NOT EXISTS catalog ("
                f"id BIGSERIAL, "
                f"value TEXT);"
            ),
            (
                f"CREATE UNIQUE INDEX IF NOT EXISTS catalog_locate_index ON catalog (md5(value));"
            ),
            (
                f"CREATE INDEX IF NOT EXISTS catalog_extract_index ON catalog using HASH (id);"
            ),
            (
                f"CREATE TABLE {graph_name} ("
                f"subject BIGINT, "
                f"predicate BIGINT, "
                f"object BIGINT);"
            )
        ]
    else:
        raise Exception(f"Unknown backend for PostgreSQL: {backend}")


def get_create_indexes_queries(graph_name, backend):
    """Format all PostgreSQL CREATE INDEX queries with the name of the RDF graph to insert."""
    if backend == "postgres":
        return [
            f"CREATE UNIQUE INDEX IF NOT EXISTS {graph_name}_spo_index ON {graph_name} (subject,predicate,md5(object));",
            f"CREATE UNIQUE INDEX IF NOT EXISTS {graph_name}_osp_index ON {graph_name} (md5(object),subject,predicate);",
            f"CREATE UNIQUE INDEX IF NOT EXISTS {graph_name}_pos_index ON {graph_name} (predicate,md5(object),subject);"
        ]
    elif backend == "postgres-mvcc":
        return [
            f"CREATE UNIQUE INDEX IF NOT EXISTS {graph_name}_spo_index ON {graph_name} (subject,predicate,md5(object),insert_t abstime_ops,delete_t abstime_ops);",
            f"CREATE UNIQUE INDEX IF NOT EXISTS {graph_name}_osp_index ON {graph_name} (md5(object),subject,predicate,insert_t abstime_ops,delete_t abstime_ops);",
            f"CREATE UNIQUE INDEX IF NOT EXISTS {graph_name}_pos_index ON {graph_name} (predicate,md5(object),subject,insert_t abstime_ops,delete_t abstime_ops);"
        ]
    elif backend == "postgres-catalog":
        return [
            f"CREATE UNIQUE INDEX IF NOT EXISTS {graph_name}_spo_index ON {graph_name} (subject,predicate,md5(object));",
            f"CREATE UNIQUE INDEX IF NOT EXISTS {graph_name}_osp_index ON {graph_name} (md5(object),subject,predicate);",
            f"CREATE UNIQUE INDEX IF NOT EXISTS {graph_name}_pos_index ON {graph_name} (predicate,md5(object),subject);"
        ]
    else:
        raise Exception(f"Unknown backend for PostgreSQL: {backend}")


def get_insert_into_query(graph_name):
    """Get an INSERT INTO query compatible with "psycopg2.extras.execute_values" to support the bulk loading."""
    return f"INSERT INTO {graph_name} (subject,predicate,object) VALUES %s ON CONFLICT DO NOTHING"


def get_insert_into_catalog_query():
    """Get an INSERT INTO query compatible with "psycopg2.extras.execute_values" to support the bulk loading."""
    return f"INSERT INTO catalog (value) VALUES %s ON CONFLICT (md5(value)) DO UPDATE SET value=EXCLUDED.value RETURNING ID"


def get_analyze_query(graph_name):
    """Format an ANALYZE query with the name of the inserted RDF graph."""
    return f"ANALYZE {graph_name}"
