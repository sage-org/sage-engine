from sage.database.utils import get_kind


def get_start_query(subj, pred, obj, table_name):
    """
        Get a prepared SQL query which starts scanning for a triple pattern
        and the parameters used to execute it.
    """
    kind = get_kind(subj, pred, obj)
    query = f"SELECT * FROM {table_name} "
    if kind == 'spo':
        query += "WHERE subject = ? AND predicate = ? AND object = ? ORDER BY subject, predicate, object"
        return query, (subj, pred, obj)
    elif kind == '???':
        query += "ORDER BY subject, predicate, object"
        # query += "ORDER BY predicate, object, subject"
        # query += "ORDER BY object, subject, predicate"
        return query, []
    elif kind == 's??':
        query += "WHERE subject = ? ORDER BY subject, predicate, object"
        return query, [subj]
    elif kind == 'sp?':
        query += "WHERE subject = ? AND predicate = ? ORDER BY subject, predicate, object"
        return query, (subj, pred)
    elif kind == '?p?':
        query += "WHERE predicate = ? ORDER BY predicate, object, subject"
        return query, [pred]
    elif kind == '?po':
        query += "WHERE predicate = ? AND object = ? ORDER BY predicate, object, subject"
        return query, (pred, obj)
    elif kind == 's?o':
        query += "WHERE object = ? AND subject = ? AND ORDER BY object, subject, predicate"
        return query, (obj, subj)
    elif kind == '??o':
        query += "WHERE object = ? ORDER BY object, subject, predicate"
        return query, [obj]
    else:
        raise Exception(f"Unkown pattern type: {kind}")


def get_resume_query(subj, pred, obj, last_read, table_name, symbol=">="):
    """
        Get a prepared SQL query which resumes scanning for a triple pattern
        and the parameters used to execute it.
    """
    last_s, last_p, last_o = last_read
    kind = get_kind(subj, pred, obj)
    query = f"SELECT * FROM {table_name} "
    if kind == 'spo':
        return None, []
    elif kind == '???':
        query += f"WHERE (subject, predicate, object) {symbol} (?, ?, ?) ORDER BY subject, predicate, object"
        # query += f"WHERE (predicate, object, subject) {symbol} (?, ?, ?) ORDER BY predicate, object, subject"
        # query += f"WHERE (object, subject, predicate) {symbol} (?, ?, ?) ORDER BY object, subject, predicate"
        return query, (last_s, last_p, last_o)
        # return query, (last_p, last_o, last_s)
        # return query, (last_o, last_s, last_p)
    elif kind == 's??':
        query += f"WHERE subject = ? AND (predicate, object) {symbol} (?, ?) ORDER BY subject, predicate, object"
        return query, (last_s, last_p, last_o)
    elif kind == 'sp?':
        query += f"WHERE subject = ? AND predicate = ? AND (object) {symbol} (?) ORDER BY subject, predicate, object"
        return query, (last_s, last_p, last_o)
    elif kind == '?p?':
        query += f"WHERE predicate = ? AND (object, subject) {symbol} (?, ?) ORDER BY predicate, object, subject"
        return query, (last_p, last_o, last_s)
    elif kind == '?po':
        query += f"WHERE predicate = ? AND object = ? AND (subject) {symbol} (?) ORDER BY predicate, object, subject"
        return query, (last_p, last_o, last_s)
    elif kind == 's?o':
        query += f"WHERE object = ? AND subject = ? AND (predicate) {symbol} (?) ORDER BY object, subject, predicate"
        return query, (last_o, last_s, last_p)
    elif kind == '??o':
        query += f"WHERE object = ? AND (subject, predicate) {symbol} (?, ?) ORDER BY object, subject, predicate"
        return query, (last_o, last_s, last_p)
    else:
        raise Exception(f"Unkown pattern type: {kind}")


def get_insert_query(table_name):
    """Build a SQL query to insert a RDF triple into a SQlite dataset"""
    return f"INSERT INTO {table_name} (subject,predicate,object) VALUES (?,?,?) ON CONFLICT (subject,predicate,object) DO NOTHING"


def get_insert_many_query(table_name):
    """Build a SQL query to insert a RDF triple into a SQlite dataset"""
    return f"INSERT INTO {table_name} (subject,predicate,object) VALUES ? ON CONFLICT (subject,predicate,object) DO NOTHING"


def get_delete_query(table_name):
    """Build a SQL query to delete a RDF triple form a SQlite dataset"""
    return f"DELETE FROM {table_name} WHERE subject = ? AND predicate = ? AND object = ?"
