# mvcc_queries.py
# Author: Thomas MINIER - MIT License 2017-2020
from datetime import datetime
from typing import List, Tuple

from sage.database.utils import get_kind


def get_start_query(subj: str, pred: str, obj: str, table_name: str) -> Tuple[str, List[str]]:
    """Get a prepared SQL query which starts scanning for a triple pattern.

    Args:
      * subj: Subject of the triple pattern.
      * pred: Predicate of the triple pattern.
      * obj: Object of the triple pattern.
      * table_name: Name of the SQL table to scan for RDF triples.
    
    Returns:
      A tuple with the prepared SQL query and its parameters.
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


def get_resume_query(subj: str, pred: str, obj: str, last_read: Tuple[str, str, str, datetime, datetime], table_name: str, symbol: str = ">=") -> Tuple[str, List[str]]:
    """Get a prepared SQL query which resumes scanning for a triple pattern.

    The SQL query rely on keyset pagination to resume query processing using an optimized Index Scan.

    Args:
      * subj: Subject of the triple pattern.
      * pred: Predicate of the triple pattern.
      * obj: Object of the triple pattern.
      * last_read: The SQL row from whoch to resume scanning.
      * table_name: Name of the SQL table to scan for RDF triples.
      * symbol: Symbol used to perform the keyset pagination. Defaults to ">=".
    
    Returns:
      A tuple with the prepared SQL query and its parameters.
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


def get_insert_query(table_name: str) -> str:
    """Build a SQL query to insert a RDF triple into a MVCC-PostgreSQL table.

    Argument: Name of the SQL table in which the triple will be inserted.

    Returns: A prepared SQL query that can be executed with a tuple (subject, predicate, object).
    """
    return f"INSERT INTO {table_name} (subject, predicate, object, insert_t, delete_t) VALUES (%s, %s, %s, transaction_timestamp(), 'infinity'::timestamp)"


def get_insert_many_query(table_name: str) -> str:
    """Build a SQL query to insert several RDF triples into a MVCC-PostgreSQL table.

    Argument: Name of the SQL table in which the triples will be inserted.

    Returns: A prepared SQL query that can be executed with a list of tuples (subject, predicate, object).
    """
    return f"INSERT INTO {table_name} (subject, predicate, object, insert_t, delete_t) VALUES %s"


def get_delete_query(table_name: str) -> str:
    """Build a SQL query to delete a RDF triple from a MVCC-PostgreSQL table.

    Argument: Name of the SQL table from which the triple will be deleted.

    Returns: A prepared SQL query that can be executed with a tuple (subject, predicate, object).
    """
    return f"UPDATE {table_name} SET delete_t = transaction_timestamp() WHERE subject = %s AND predicate = %s AND object = %s AND delete_t = 'infinity'::timestamp"
