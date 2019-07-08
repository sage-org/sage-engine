# queries.py
# Author: Thomas MINIER - MIT License 2017-2019
from sage.database.utils import get_kind


def get_start_query(subj, pred, obj, table_name, fetch_size=100):
    """
        Get a prepared SQL query which starts scanning for a triple pattern
        and the parameters used to execute it.
    """
    kind = get_kind(subj, pred, obj)
    query = "SELECT * FROM {} ".format(table_name)
    params = None
    if kind == 'spo':
        query += "WHERE subject = %s AND predicate = %s AND object = %s ORDER BY subject, predicate, object"
        params = (subj, pred, obj)
    elif kind == '???':
        query += ' ORDER BY subject, predicate, object'
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
        raise Exception("Unkown pattern type: {}".format(kind))
    query += " LIMIT {}".format(fetch_size)
    return query, params


def get_resume_query(subj, pred, obj, last_read, table_name, fetch_size=100, symbol=">="):
    """
        Get a prepared SQL query which resumes scanning for a triple pattern
        and the parameters used to execute it.
    """
    last_s, last_p, last_o = last_read
    kind = get_kind(subj, pred, obj)
    query = "SELECT * FROM {} ".format(table_name)
    params = None
    if kind == 'spo':
        return None, None
    elif kind == '???':
        query += "WHERE (subject, predicate, object) {} (%s, %s, %s) ORDER BY subject, predicate, object".format(symbol)
        params = (last_s, last_p, last_o)
    elif kind == 's??':
        query += "WHERE subject = %s AND (predicate, object) {} (%s, %s) ORDER BY subject, predicate, object".format(symbol)
        params = (last_s, last_p, last_o)
    elif kind == 'sp?':
        query += "WHERE subject = %s AND predicate = %s AND (object) {} (%s) ORDER BY subject, predicate, object".format(symbol)
        params = (last_s, last_p, last_o)
    elif kind == '?p?':
        query += "WHERE predicate = %s AND (object, subject) {} (%s, %s) ORDER BY predicate, object, subject".format(symbol)
        params = (last_p, last_o, last_s)
    elif kind == '?po':
        query += "WHERE predicate = %s AND object = %s AND (subject) {} (%s) ORDER BY predicate, object, subject".format(symbol)
        params = (last_p, last_o, last_s)
    elif kind == 's?o':
        query += "WHERE subject = %s AND object = %s AND (predicate) {} (%s) ORDER BY object, subject, predicate".format(symbol)
        params = (last_s, last_o, last_p)
    elif kind == '??o':
        query += "WHERE object = %s AND (subject, predicate) {} (%s, %s) ORDER BY object, subject, predicate".format(symbol)
        params = (last_o, last_s, last_p)
    else:
        raise Exception("Unkown pattern type: {}".format(kind))
    query += " LIMIT {}".format(fetch_size)
    return query, params


def get_insert_query(table_name):
    """Build a SQL query to insert a RDF triple into a PostgreSQL dataset"""
    return "INSERT INTO {} (subject,predicate,object) VALUES (%s,%s,%s) ON CONFLICT (subject,predicate,object) DO NOTHING".format(table_name)


def get_insert_many_query(table_name):
    """Build a SQL query to insert a RDF triple into a PostgreSQL dataset"""
    return "INSERT INTO {} (subject,predicate,object) VALUES %s ON CONFLICT (subject,predicate,object) DO NOTHING".format(table_name)


def get_delete_query(table_name):
    """Build a SQL query to delete a RDF triple form a PostgreSQL dataset"""
    return "DELETE FROM {} WHERE subject = %s AND predicate = %s AND object = %s".format(table_name)
