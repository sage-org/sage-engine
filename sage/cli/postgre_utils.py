# postgre_utils.py
# Author: Thomas MINIER - MIT License 2017-2019

POSTGRE_CREATE_TABLE = """
    CREATE TABLE {} (
        subject text,
        predicate text,
        object text,
        CONSTRAINT {}_spo_index_pkey PRIMARY KEY (subject, predicate, object)
    );
    """
# CREATE UNIQUE INDEX {}_spo_index_pkey ON {}(subject text_ops,predicate text_ops,object text_ops);

POSTGRE_CREATE_INDEXES = [
    # Create index of OSP
    """
    CREATE INDEX {}_osp_index ON {}(object text_ops,subject text_ops,predicate text_ops);
    """,
    # Create index on POS
    """
    CREATE INDEX {}_pos_index ON {}(predicate text_ops,object text_ops,subject text_ops);
    """
]

POSTGRE_FUNCTIONS = [
    # Start an Index scan on pattern (spo)
    """
    CREATE OR REPLACE FUNCTION {}_scan_spo(refcursor, subj text, pred text, obj text) RETURNS refcursor AS $$
    BEGIN
        OPEN $1 FOR SELECT * FROM {} WHERE subject = $2 AND predicate = $3 AND object = $4;
        RETURN $1;
    END;
    $$ LANGUAGE plpgsql;
    """,
    # Start an Index scan on pattern (???)
    """
    CREATE OR REPLACE FUNCTION {}_scan_vvv(refcursor) RETURNS refcursor AS $$
    BEGIN
        OPEN $1 FOR SELECT * FROM {};
        RETURN $1;
    END;
    $$ LANGUAGE plpgsql;
    """,
    # Resume an Index scan on pattern (???)
    """
    CREATE OR REPLACE FUNCTION {}_resume_vvv(refcursor, subj text, pred text, obj text) RETURNS refcursor AS $$
    BEGIN
        OPEN $1 FOR SELECT * FROM {} WHERE (subject, predicate, object) >= ($2, $3, $4);
        RETURN $1;
    END;
    $$ LANGUAGE plpgsql;
    """,
    # Start an Index scan on pattern (s??)
    """
    CREATE OR REPLACE FUNCTION {}_scan_svv(refcursor, subj text) RETURNS refcursor AS $$
    BEGIN
        OPEN $1 FOR SELECT * FROM {} WHERE subject = $2;
        RETURN $1;
    END;
    $$ LANGUAGE plpgsql;
    """,
    # Resume an Index scan on pattern (s??)
    """
    CREATE OR REPLACE FUNCTION {}_resume_svv(refcursor, subj text, pred text, obj text) RETURNS refcursor AS $$
    BEGIN
        OPEN $1 FOR SELECT * FROM {} WHERE subject = $2 AND (predicate, object) >= ($3, $4);
        RETURN $1;
    END;
    $$ LANGUAGE plpgsql;
    """,
    # Start an Index scan on pattern (?p?)
    """
    CREATE OR REPLACE FUNCTION {}_scan_vpv(refcursor, pred text) RETURNS refcursor AS $$
    BEGIN
        OPEN $1 FOR SELECT * FROM {} WHERE predicate = $2;
        RETURN $1;
    END;
    $$ LANGUAGE plpgsql;
    """,
    # Resume an Index scan on pattern (?p?)
    """
    CREATE OR REPLACE FUNCTION {}_resume_vpv(refcursor, subj text, pred text, obj text) RETURNS refcursor AS $$
    BEGIN
        OPEN $1 FOR SELECT * FROM {} WHERE predicate = $3 AND (subject, object) >= ($2, $4);
        RETURN $1;
    END;
    $$ LANGUAGE plpgsql;
    """,
    # Start an Index scan on pattern (??o)
    """
    CREATE OR REPLACE FUNCTION {}_scan_vvo(refcursor, obj text) RETURNS refcursor AS $$
    BEGIN
        OPEN $1 FOR SELECT * FROM {} WHERE object = $2;
        RETURN $1;
    END;
    $$ LANGUAGE plpgsql;
    """,
    # Resume an Index scan on pattern (??o)
    """
    CREATE OR REPLACE FUNCTION {}_resume_vvo(refcursor, subj text, pred text, obj text) RETURNS refcursor AS $$
    BEGIN
        OPEN $1 FOR SELECT * FROM {} WHERE object = $4 AND (subject, predicate) >= ($2, $3);
        RETURN $1;
    END;
    $$ LANGUAGE plpgsql;
    """,
    # Start an Index scan on pattern (sp?)
    """
    CREATE OR REPLACE FUNCTION {}_scan_spv(refcursor, subj text, pred text) RETURNS refcursor AS $$
    BEGIN
        OPEN $1 FOR SELECT * FROM {} WHERE subject = $2 AND predicate = $3;
        RETURN $1;
    END;
    $$ LANGUAGE plpgsql;
    """,
    # Resume an Index scan on pattern (sp?)
    """
    CREATE OR REPLACE FUNCTION {}_resume_spv(refcursor, subj text, pred text, obj text) RETURNS refcursor AS $$
    BEGIN
        OPEN $1 FOR SELECT * FROM {} WHERE subject = $2 AND predicate = $3 AND (object) >= ($4);
        RETURN $1;
    END;
    $$ LANGUAGE plpgsql;
    """,
    # Start an Index scan on pattern (?po)
    """
    CREATE OR REPLACE FUNCTION {}_scan_vpo(refcursor, pred text, obj text) RETURNS refcursor AS $$
    BEGIN
        OPEN $1 FOR SELECT * FROM {} WHERE predicate = $2 AND object = $3;
        RETURN $1;
    END;
    $$ LANGUAGE plpgsql;
    """,
    # Resume an Index scan on pattern (?po)
    """
    CREATE OR REPLACE FUNCTION {}_resume_vpo(refcursor, subj text, pred text, obj text) RETURNS refcursor AS $$
    BEGIN
        OPEN $1 FOR SELECT * FROM {} WHERE predicate = $3 AND object = $4 AND (subject) >= ($2);
        RETURN $1;
    END;
    $$ LANGUAGE plpgsql;
    """,
    # Start an Index scan on pattern (s?o)
    """
    CREATE OR REPLACE FUNCTION {}_scan_svo(refcursor, subj text, obj text) RETURNS refcursor AS $$
    BEGIN
        OPEN $1 FOR SELECT * FROM {} WHERE subject = $2 AND object = $3;
        RETURN $1;
    END;
    $$ LANGUAGE plpgsql;
    """,
    # Resume an Index scan on pattern (s?o)
    """
    CREATE OR REPLACE FUNCTION {}_resume_svo(refcursor, subj text, pred text, obj text) RETURNS refcursor AS $$
    BEGIN
        OPEN $1 FOR SELECT * FROM {} WHERE subject = $2 AND object = $4 AND (predicate) >= ($3);
        RETURN $1;
    END;
    $$ LANGUAGE plpgsql;
    """
]


def get_postgre_create_table(table_name):
    """Format a postgre CREATE TABLE with the name of a SQL table"""
    return POSTGRE_CREATE_TABLE.format(table_name, table_name, table_name, table_name)


def get_postgre_create_indexes(table_name):
    """Format all postgre CREATE INDEXE with the name of a SQL table"""
    def __mapper(fn):
        return fn.format(table_name, table_name)

    return map(__mapper, POSTGRE_CREATE_INDEXES)


def get_postgre_functions(table_name):
    """Format all postgre functions with the name of a SQL table"""
    def __mapper(fn):
        return fn.format(table_name, table_name)

    return map(__mapper, POSTGRE_FUNCTIONS)


def get_postgre_insert_into(table_name):
    """
        Get an INSERT INTO query, compatible with `psycopg2.extras.execute_values` to support bulk loading
    """
    return "INSERT INTO {} (subject,predicate,object) VALUES %s ON CONFLICT (subject,predicate,object) DO NOTHING".format(table_name)
