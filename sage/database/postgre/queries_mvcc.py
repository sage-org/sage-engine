# queries_mvcc.py
# Author: Thomas MINIER - MIT License 2017-2019
from sage.database.utils import get_kind


def get_start_query_mvcc(subj, pred, obj, version, table_name, fetch_size=100):
    """
        Get a prepared SQL query which starts scanning for a triple pattern (at a specific version)
        and the parameters used to execute it.
    """
    kind = get_kind(subj, pred, obj)
    query = "SELECT subject, predicate, object FROM {} ".format(table_name)
    params = None
    if kind == 'spo':
        query += "WHERE created_at <= %s AND deleted_at >= %s AND subject = %s AND predicate = %s AND object = %s"
        params = (version, version, subj, pred, obj)
    elif kind == '???':
        query += "WHERE created_at <= %s AND deleted_at >= %s"
        params = (version, version)
    elif kind == 's??':
        query += "WHERE created_at <= %s AND deleted_at >= %s AND subject = %s ORDER BY created_at, deleted_at, subject, predicate, object"
        params = (version, version, subj)
    elif kind == 'sp?':
        query += "WHERE created_at <= %s AND deleted_at >= %s AND subject = %s AND predicate = %s ORDER BY created_at, deleted_at, subject, predicate, object"
        params = (version, version, subj, pred)
    elif kind == '?p?':
        query += "WHERE created_at <= %s AND deleted_at >= %s AND predicate = %s ORDER BY created_at, deleted_at, predicate, object, subject"
        params = (version, version, pred)
    elif kind == '?po':
        query += "WHERE created_at <= %s AND deleted_at >= %s AND predicate = %s AND object = %s ORDER BY created_at, deleted_at, predicate, object, subject"
        params = (version, version, pred, obj)
    elif kind == 's?o':
        query += "WHERE created_at <= %s AND deleted_at >= %s AND subject = %s AND object = %s ORDER BY created_at, deleted_at, object, subject, predicate"
        params = (version, version, subj, obj)
    elif kind == '??o':
        query += "WHERE created_at <= %s AND deleted_at >= %s AND object = %s ORDER BY created_at, deleted_at, object, subject, predicate"
        params = (version, version, obj)
    else:
        raise Exception("Unkown pattern type: {}".format(kind))
    query += " LIMIT {}".format(fetch_size)
    return query, params


def get_resume_query_mvcc(subj, pred, obj, last_read, version, table_name, fetch_size=100, symbol=">="):
    """
        Get a prepared SQL query which resumes scanning for a triple pattern (at a specific version)
        and the parameters used to execute it.
    """
    last_s, last_p, last_o = last_read
    kind = get_kind(subj, pred, obj)
    query = "SELECT subject, predicate, object FROM {} ".format(table_name)
    params = None
    if kind == 'spo':
        return None, None
    elif kind == '???':
        query += "WHERE created_at <= %s AND deleted_at >= %s AND (subject, predicate, object) {} (%s, %s, %s)".format(symbol)
        params = (version, version, last_s, last_p, last_o)
    elif kind == 's??':
        query += "WHERE created_at <= %s AND deleted_at >= %s AND subject = %s AND (predicate, object) {} (%s, %s) ORDER BY created_at, deleted_at, subject, predicate, object".format(symbol)
        params = (version, version, last_s, last_p, last_o)
    elif kind == 'sp?':
        query += "WHERE created_at <= %s AND deleted_at >= %s AND subject = %s AND predicate = %s AND (object) {} (%s) ORDER BY created_at, deleted_at, subject, predicate, object".format(symbol)
        params = (version, version, last_s, last_p, last_o)
    elif kind == '?p?':
        query += "WHERE created_at <= %s AND deleted_at >= %s AND predicate = %s AND (object, subject) {} (%s, %s) ORDER BY created_at, deleted_at, predicate, object, subject".format(symbol)
        params = (version, version, last_p, last_o, last_s)
    elif kind == '?po':
        query += "WHERE created_at <= %s AND deleted_at >= %s AND predicate = %s AND object = %s AND (subject) {} (%s) ORDER BY created_at, deleted_at, predicate, object, subject".format(symbol)
        params = (version, version, last_p, last_o, last_s)
    elif kind == 's?o':
        query += "WHERE created_at <= %s AND deleted_at >= %s AND subject = %s AND object = %s AND (predicate) {} (%s) ORDER BY created_at, deleted_at, object, subject, predicate".format(symbol)
        params = (version, version, last_s, last_o, last_p)
    elif kind == '??o':
        query += "WHERE created_at <= %s AND deleted_at >= %s AND object = %s AND (subject, predicate) {} (%s, %s) ORDER BY created_at, deleted_at, object, subject, predicate".format(symbol)
        params = (version, version, last_o, last_s, last_p)
    else:
        raise Exception("Unkown pattern type: {}".format(kind))
    query += " LIMIT {}".format(fetch_size)
    return query, params


def get_insert_query(table_name):
    """Build a SQL query to insert a RDF triple into a PostgreSQL dataset (at a specific version)"""
    return "INSERT INTO {} (subject,predicate,object,created_at) VALUES (%s,%s,%s,%s) ON CONFLICT (subject,predicate,object,created_at,deleted_at) DO NOTHING".format(table_name)


def get_insert_many_query(table_name):
    """Build a SQL query to insert a RDF triple into a PostgreSQL dataset"""
    return "INSERT INTO {} (subject,predicate,object,created_at) VALUES %s ON CONFLICT (subject,predicate,object,created_at,deleted_at) DO NOTHING".format(table_name)


def get_mark_deleted_query(table_name):
    """Build a SQL query to mark a RDF triple as deleted"""
    return "UPDATE {} SET deleted_at = %s WHERE subject = %s AND predicate = %s AND object = %s".format(table_name)


def get_delete_version(table_name, version):
    """Build a SQL query to delete a version from a PostgreSQL dataset"""
    return "DELETE FROM {} WHERE created_at <= %s AND deleted_at >= %s".format(table_name)
