# queries.py
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
        query += "WHERE subject = %s AND predicate = %s AND object = %s ORDER BY subject, predicate, object"
        params = (subj, pred, obj)
    elif kind == '???':
        query += "ORDER BY subject, predicate, object"
    elif kind == 's??':
        query += "WHERE subject = %s ORDER BY subject, predicate, object"
        params = [subj]
    elif kind == 'sp?':
        query += "WHERE subject = %s AND predicate = %s ORDER BY subject, predicate, object"
        params = (subj, pred)
    elif kind == '?p?':
        query += "WHERE predicate = %s ORDER BY predicate, object, subject"
        params = [pred]
    elif kind == '?po':
        query += "WHERE predicate = %s AND object = %s ORDER BY predicate, object, subject"
        params = (pred, obj)
    elif kind == 's?o':
        query += "WHERE subject = %s AND object = %s ORDER BY object, subject, predicate"
        params = (subj, obj)
    elif kind == '??o':
        query += "WHERE object = %s ORDER BY object, subject, predicate"
        params = [obj]
    else:
        raise Exception(f"Unkown pattern type: {kind}")
    return query, params


def get_resume_query(subj, pred, obj, last_read, table_name, symbol=">="):
    """
        Get a prepared SQL query which resumes scanning for a triple pattern
        and the parameters used to execute it.
    """
    last_s, last_p, last_o = last_read
    kind = get_kind(subj, pred, obj)
    query = f"SELECT * FROM {table_name} "
    params = None
    if kind == 'spo':
        return None, None
    elif kind == '???':
        query += f"WHERE (subject, predicate, object) {symbol} (%s, %s, %s) ORDER BY subject, predicate, object"
        params = (last_s, last_p, last_o)
    elif kind == 's??':
        query += f"WHERE subject = %s AND (predicate, object) {symbol} (%s, %s) ORDER BY subject, predicate, object"
        params = (last_s, last_p, last_o)
    elif kind == 'sp?':
        query += f"WHERE subject = %s AND predicate = %s AND (object) {symbol} (%s) ORDER BY subject, predicate, object"
        params = (last_s, last_p, last_o)
    elif kind == '?p?':
        query += f"WHERE predicate = %s AND (object, subject) {symbol} (%s, %s) ORDER BY predicate, object, subject"
        params = (last_p, last_o, last_s)
    elif kind == '?po':
        query += f"WHERE predicate = %s AND object = %s AND (subject) {symbol} (%s) ORDER BY predicate, object, subject"
        params = (last_p, last_o, last_s)
    elif kind == 's?o':
        query += f"WHERE subject = %s AND object = %s AND (predicate) {symbol} (%s) ORDER BY object, subject, predicate"
        params = (last_s, last_o, last_p)
    elif kind == '??o':
        query += f"WHERE object = %s AND (subject, predicate) {symbol} (%s, %s) ORDER BY object, subject, predicate"
        params = (last_o, last_s, last_p)
    else:
        raise Exception(f"Unkown pattern type: {kind}")
    return query, params


def get_insert_query(table_name):
    """Build a SQL query to insert a RDF triple into a PostgreSQL dataset"""
    return f"INSERT INTO {table_name} (subject,predicate,object) VALUES (%s,%s,%s)"


def get_insert_many_query(table_name):
    """Build a SQL query to insert a RDF triple into a PostgreSQL dataset"""
    return f"INSERT INTO {table_name} (subject,predicate,object) VALUES %s"


def get_delete_query(table_name):
    """Build a SQL query to delete a RDF triple form a PostgreSQL dataset"""
    return f"DELETE FROM {table_name} WHERE subject = %s AND predicate = %s AND object = %s"
