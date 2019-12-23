# mvcc_queries.py
# Author: Thomas MINIER - MIT License 2017-2020
from sage.database.utils import get_kind


def get_start_query(subj, pred, obj, table_name):
    """
        Get a prepared SQL query which starts scanning for a triple pattern
        and the parameters used to execute it.
    """
    kind = get_kind(subj, pred, obj)
    query = f"SELECT * FROM {table_name} "
    params = None
    if kind == 'spo':
        query += "WHERE subject = %s AND predicate = %s AND object = %s ORDER BY subject, predicate, object, insert_t, delete_t"
        params = (subj, pred, obj)
    elif kind == '???':
        query += ' ORDER BY subject, predicate, object, insert_t, delete_t'
    elif kind == 's??':
        query += "WHERE subject = %s ORDER BY subject, predicate, object, insert_t, delete_t"
        params = [subj]
    elif kind == 'sp?':
        query += "WHERE subject = %s AND predicate = %s ORDER BY subject, predicate, object, insert_t, delete_t"
        params = (subj, pred)
    elif kind == '?p?':
        query += "WHERE predicate = %s ORDER BY predicate, object, subject, insert_t, delete_t"
        params = [pred]
    elif kind == '?po':
        query += "WHERE predicate = %s AND object = %s ORDER BY predicate, object, subject, insert_t, delete_t"
        params = (pred, obj)
    elif kind == 's?o':
        query += "WHERE subject = %s AND object = %s ORDER BY object, subject, predicate, insert_t, delete_t"
        params = (subj, obj)
    elif kind == '??o':
        query += "WHERE object = %s ORDER BY object, subject, predicate, insert_t, delete_t"
        params = [obj]
    else:
        raise Exception(f"Unkown pattern type: {kind}")
    return query, params


def get_resume_query(subj, pred, obj, last_read, table_name, symbol=">="):
    """
        Get a prepared SQL query which resumes scanning for a triple pattern
        and the parameters used to execute it.
    """
    last_s, last_p, last_o, last_insert_t, last_delete_t = last_read
    kind = get_kind(subj, pred, obj)
    query = f"SELECT * FROM {table_name} "
    params = None
    if kind == 'spo':
        return None, None
    elif kind == '???':
        query += f"WHERE (subject, predicate, object, insert_t, delete_t) {symbol} (%s, %s, %s, %s, %s) ORDER BY subject, predicate, object, insert_t, delete_t"
        params = (last_s, last_p, last_o, last_insert_t, last_delete_t)
    elif kind == 's??':
        query += f"WHERE subject = %s AND (predicate, object, insert_t, delete_t) {symbol} (%s, %s, %s, %s) ORDER BY subject, predicate, object, insert_t, delete_t"
        params = (last_s, last_p, last_o, last_insert_t, last_delete_t)
    elif kind == 'sp?':
        query += f"WHERE subject = %s AND predicate = %s AND (object, insert_t, delete_t) {symbol} (%s, %s, %s) ORDER BY subject, predicate, object, insert_t, delete_t"
        params = (last_s, last_p, last_o, last_insert_t, last_delete_t)
    elif kind == '?p?':
        query += f"WHERE predicate = %s AND (object, subject, insert_t, delete_t) {symbol} (%s, %s, %s, %s) ORDER BY predicate, object, subject, insert_t, delete_t"
        params = (last_p, last_o, last_s, last_insert_t, last_delete_t)
    elif kind == '?po':
        query += f"WHERE predicate = %s AND object = %s AND (subject, insert_t, delete_t) {symbol} (%s, %s, %s) ORDER BY predicate, object, subject, insert_t, delete_t"
        params = (last_p, last_o, last_s, last_insert_t, last_delete_t)
    elif kind == 's?o':
        query += f"WHERE subject = %s AND object = %s AND (predicate, insert_t, delete_t) {symbol} (%s, %s, %s) ORDER BY object, subject, predicate, insert_t, delete_t"
        params = (last_s, last_o, last_p, last_insert_t, last_delete_t)
    elif kind == '??o':
        query += f"WHERE object = %s AND (subject, predicate, insert_t, delete_t) {symbol} (%s, %s, %s, %s) ORDER BY object, subject, predicate, insert_t, delete_t"
        params = (last_o, last_s, last_p, last_insert_t, last_delete_t)
    else:
        raise Exception(f"Unkown pattern type: {kind}")
    return query, params


def get_insert_query(table_name):
    """
        Build a SQL query to insert a RDF triple into a PostgreSQL dataset.
        Returns a prepared SQL statement that expect the following arguments:
            - subject: RDF triple subject
            - predicate: RDF triple predicate
            - object: RDF triple object
    """
    return f"INSERT INTO {table_name} (subject, predicate, object, insert_t, delete_t) VALUES (%s, %s, %s, transaction_timestamp(), 'infinity'::timestamp)"


def get_insert_many_query(table_name):
    """Build a SQL query to insert a RDF triple into a PostgreSQL dataset"""
    return f"INSERT INTO {table_name} (subject, predicate, object, insert_t, delete_t) VALUES %s"


def get_delete_query(table_name):
    """
        Build a SQL query to delete a RDF triple form a PostgreSQL dataset.
        Returns a prepared SQL statement that expect the following arguments:
            - subject: RDF triple subject
            - predicate: RDF triple predicate
            - object: RDF triple object
    """
    return f"UPDATE {table_name} SET delete_t = transaction_timestamp() WHERE subject = %s AND predicate = %s AND object = %s AND delete_t = 'infinity'::timestamp"
