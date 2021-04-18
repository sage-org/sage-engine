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
        query += """WHERE subject = %s
                    AND predicate = %s
                    AND md5(object) = md5(%s)
                    ORDER BY subject, predicate, md5(object)"""
        return query, (subj, pred, obj)
    elif kind == '???':
        query += "ORDER BY subject, predicate, md5(object)"
        # query += "ORDER BY predicate, md5(object), subject"
        # query += "ORDER BY md5(object), subject, predicate"
        return query, None
    elif kind == 's??':
        query += """WHERE subject = %s
                    ORDER BY subject, predicate, md5(object)"""
        return query, [subj]
    elif kind == 'sp?':
        query += """WHERE subject = %s
                    AND predicate = %s
                    ORDER BY subject, predicate, md5(object)"""
        return query, (subj, pred)
    elif kind == '?p?':
        query += """WHERE predicate = %s
                    ORDER BY predicate, md5(object), subject"""
        return query, [pred]
    elif kind == '?po':
        query += """WHERE predicate = %s
                    AND md5(object) = md5(%s)
                    ORDER BY predicate, md5(object), subject"""
        return query, (pred, obj)
    elif kind == 's?o':
        query += """WHERE md5(object) = md5(%s)
                    AND subject = %s
                    ORDER BY md5(object), subject, predicate"""
        return query, (obj, subj)
    elif kind == '??o':
        query += """WHERE md5(object) = md5(%s)
                    ORDER BY md5(object), subject, predicate"""
        return query, [obj]
    else:
        raise Exception(f"Unkown pattern type: {kind}")


def get_resume_query(subj: str, pred: str, obj: str, last_read: Tuple[str, str, str], table_name: str, symbol: str = ">=") -> Tuple[str, str]:
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
    last_s, last_p, last_o = last_read
    kind = get_kind(subj, pred, obj)
    query = f"SELECT * FROM {table_name} "
    if kind == 'spo':
        return None, None
    elif kind == '???':
        query += f"""WHERE (subject, predicate, md5(object)) {symbol} (%s, %s, md5(%s))
                     ORDER BY subject, predicate, md5(object)"""
        # query += f"""WHERE (predicate, md5(object), subject) {symbol} (%s, md5(%s), %s)
        #              ORDER BY predicate, md5(object), subject"""
        # query += f"""WHERE (md5(object), subject, predicate) {symbol} (md5(%s), %s, %s)
        #              ORDER BY md5(object), subject, predicate"""
        return query, (last_s, last_p, last_o)
        # return query, (last_p, last_o, last_s)
        # return query, (last_o, last_s, last_p)
    elif kind == 's??':
        query += f"""WHERE subject = %s
                     AND (predicate, md5(object)) {symbol} (%s, md5(%s))
                     ORDER BY subject, predicate, md5(object)"""
        return query, (last_s, last_p, last_o)
    elif kind == 'sp?':
        query += f"""WHERE subject = %s
                     AND predicate = %s
                     AND (md5(object)) {symbol} (md5(%s))
                     ORDER BY subject, predicate, md5(object)"""
        return query, (last_s, last_p, last_o)
    elif kind == '?p?':
        query += f"""WHERE predicate = %s
                     AND (md5(object), subject) {symbol} (md5(%s), %s)
                     ORDER BY predicate, md5(object), subject"""
        return query, (last_p, last_o, last_s)
    elif kind == '?po':
        query += f"""WHERE predicate = %s
                     AND md5(object) = md5(%s)
                     AND (subject) {symbol} (%s)
                     ORDER BY predicate, md5(object), subject"""
        return query, (last_p, last_o, last_s)
    elif kind == 's?o':
        query += f"""WHERE md5(object) = md5(%s)
                     AND subject = %s
                     AND (predicate) {symbol} (%s)
                     ORDER BY md5(object), subject, predicate"""
        return query, (last_o, last_s, last_p)
    elif kind == '??o':
        query += f"""WHERE md5(object) = md5(%s)
                     AND (subject, predicate) {symbol} (%s, %s)
                     ORDER BY md5(object), subject, predicate"""
        return query, (last_o, last_s, last_p)
    else:
        raise Exception(f"Unkown pattern type: {kind}")


def get_insert_query(table_name: str) -> str:
    """Build a SQL query to insert a RDF triple into a PostgreSQL table.

    Argument: Name of the SQL table in which the triple will be inserted.

    Returns: A prepared SQL query that can be executed with a tuple (subject, predicate, object).
    """
    return f"INSERT INTO {table_name} (subject,predicate,object) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING"


def get_insert_many_query(table_name: str) -> str:
    """Build a SQL query to insert several RDF triples into a PostgreSQL table.

    Argument: Name of the SQL table in which the triples will be inserted.

    Returns: A prepared SQL query that can be executed with a list of tuples (subject, predicate, object).
    """
    return f"INSERT INTO {table_name} (subject,predicate,object) VALUES %s ON CONFLICT DO NOTHING"


def get_delete_query(table_name: str) -> str:
    """Build a SQL query to delete a RDF triple from a PostgreSQL table.

    Argument: Name of the SQL table from which the triple will be deleted.

    Returns: A prepared SQL query that can be executed with a tuple (subject, predicate, object).
    """
    return f"""DELETE FROM {table_name}
               WHERE subject = %s
               AND predicate = %s
               AND md5(object) = md5(%s)"""
