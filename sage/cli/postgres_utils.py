# postgres_utils.py
# Author: Thomas MINIER - MIT License 2017-2019

POSTGRES_CREATE_TABLE = """
    CREATE TABLE {} (
        subject text,
        predicate text,
        object text
    );
    """

POSTGRES_CREATE_MVCC_TABLE = """
    CREATE TABLE {} (
        subject text,
        predicate text,
        object text,
        insert_t timestamp DEFAULT transaction_timestamp(),
        delete_t timestamp DEFAULT 'infinity'
    );
    """

POSTGRES_CREATE_INDEXES = [
    # Create index of SPO
    """
    CREATE INDEX {}_spo_index ON {}(subject text_ops,predicate text_ops, object text_ops);
    """,
    # Create index of OSP
    """
    CREATE INDEX {}_osp_index ON {}(object text_ops,subject text_ops,predicate text_ops);
    """,
    # Create index on POS
    """
    CREATE INDEX {}_pos_index ON {}(predicate text_ops,object text_ops,subject text_ops);
    """
]

POSTGRES_CREATE_MVCC_INDEXES = [
    # Create index of SPO
    """
    CREATE INDEX {}_spo_index ON {}(subject text_ops,predicate text_ops, object text_ops, insert_t timestamp_ops,delete_t timestamp_ops);
    """,
    # Create index of OSP
    """
    CREATE INDEX {}_osp_index ON {}(object text_ops,subject text_ops,predicate text_ops, insert_t timestamp_ops,delete_t timestamp_ops);
    """,
    # Create index on POS
    """
    CREATE INDEX {}_pos_index ON {}(predicate text_ops,object text_ops,subject text_ops, insert_t timestamp_ops,delete_t timestamp_ops);
    """
]


def get_postgres_create_table(table_name, enable_mvcc=False):
    """Format a postgre CREATE TABLE with the name of a SQL table"""
    if enable_mvcc:
        return POSTGRES_CREATE_MVCC_TABLE.format(table_name)
    return POSTGRES_CREATE_TABLE.format(table_name)


def get_postgres_create_indexes(table_name, enable_mvcc=False):
    """Format all postgre CREATE INDEXE with the name of a SQL table"""
    def __mapper(query):
        return query.format(table_name, table_name)

    if enable_mvcc:
        return map(__mapper, POSTGRES_CREATE_MVCC_INDEXES)
    return map(__mapper, POSTGRES_CREATE_INDEXES)


def get_postgres_insert_into(table_name, enable_mvcc=False):
    """
        Get an INSERT INTO query compatible with `psycopg2.extras.execute_values` (to support bulk loading).
    """
    if enable_mvcc:
        return "INSERT INTO {} (subject,predicate,object) VALUES %s".format(table_name)
    return "INSERT INTO {} (subject,predicate,object) VALUES %s".format(table_name)
