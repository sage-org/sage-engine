from sage.database.utils import get_kind


def get_start_query(subj, pred, obj, table_name):
    """
        Get a prepared SQL query which starts scanning for a triple pattern
        and the parameters used to execute it.
    """
    kind = get_kind(subj, pred, obj)
    query = f"""SELECT cs.value, cp.value, co.value
                FROM {table_name}
                INNER JOIN catalog AS cs ON subject = cs.id
                INNER JOIN catalog AS cp ON predicate = cp.id
                INNER JOIN catalog AS co ON object = co.id """
    if kind == 'spo':
        query += f"""WHERE subject = ({get_locate_query()})
                     AND predicate = ({get_locate_query()})
                     AND object = ({get_locate_query()})
                     ORDER BY subject, predicate, object"""
        return query, (subj, pred, obj)
    elif kind == '???':
        query += "ORDER BY subject, predicate, object"
        # query += "ORDER BY predicate, object, subject"
        # query += "ORDER BY object, subject, predicate"
        return query, None
    elif kind == 's??':
        query += f"""WHERE subject = ({get_locate_query()})
                     ORDER BY subject, predicate, object"""
        return query, [subj]
    elif kind == 'sp?':
        query += f"""WHERE subject = ({get_locate_query()})
                     AND predicate = ({get_locate_query()})
                     ORDER BY subject, predicate, object"""
        return query, (subj, pred)
    elif kind == '?p?':
        query += f"""WHERE predicate = ({get_locate_query()})
                     ORDER BY predicate, object, subject"""
        return query, [pred]
    elif kind == '?po':
        query += f"""WHERE predicate = ({get_locate_query()})
                     AND object = ({get_locate_query()})
                     ORDER BY predicate, object, subject"""
        return query, (pred, obj)
    elif kind == 's?o':
        query += f"""WHERE object = ({get_locate_query()})
                     AND subject = ({get_locate_query()})
                     ORDER BY object, subject, predicate"""
        return query, (obj, subj)
    elif kind == '??o':
        query += f"""WHERE object = ({get_locate_query()})
                     ORDER BY object, subject, predicate"""
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
    query = f"""SELECT cs.value, cp.value, co.value
                FROM {table_name}
                INNER JOIN catalog AS cs ON subject = cs.id
                INNER JOIN catalog AS cp ON predicate = cp.id
                INNER JOIN catalog AS co ON object = co.id """
    if kind == 'spo':
        return None, None
    elif kind == '???':
        query += f"""WHERE (subject, predicate, object) {symbol} (({get_locate_query()}), ({get_locate_query()}), ({get_locate_query()}))
                     ORDER BY subject, predicate, object"""
        # query += f"""WHERE (predicate, object, subject) {symbol} (({get_locate_query()}), ({get_locate_query()}), ({get_locate_query()}))
        #              ORDER BY predicate, object, subject"""
        # query += f"""WHERE (object, subject, predicate) {symbol} (({get_locate_query()}), ({get_locate_query()}), ({get_locate_query()}))
        #              ORDER BY object, subject, predicate"""
        return query, (last_s, last_p, last_o)
        # return query, (last_p, last_o, last_s)
        # return query, (last_o, last_s, last_p)
    elif kind == 's??':
        query += f"""WHERE subject = ({get_locate_query()})
                     AND (predicate, object) {symbol} (({get_locate_query()}), ({get_locate_query()}))
                     ORDER BY subject, predicate, object"""
        return query, (last_s, last_p, last_o)
    elif kind == 'sp?':
        query += f"""WHERE subject = ({get_locate_query()})
                     AND predicate = ({get_locate_query()})
                     AND (object) {symbol} ({get_locate_query()})
                     ORDER BY subject, predicate, object"""
        return query, (last_s, last_p, last_o)
    elif kind == '?p?':
        query += f"""WHERE predicate = ({get_locate_query()})
                     AND (object, subject) {symbol} (({get_locate_query()}), ({get_locate_query()}))
                     ORDER BY predicate, object, subject"""
        return query, (last_p, last_o, last_s)
    elif kind == '?po':
        query += f"""WHERE predicate = ({get_locate_query()})
                     AND object = ({get_locate_query()})
                     AND (subject) {symbol} ({get_locate_query()})
                     ORDER BY predicate, object, subject"""
        return query, (last_p, last_o, last_s)
    elif kind == 's?o':
        query += f"""WHERE object = ({get_locate_query()})
                     AND subject = ({get_locate_query()})
                     AND (predicate) {symbol} ({get_locate_query()})
                     ORDER BY object, subject, predicate"""
        return query, (last_o, last_s, last_p)
    elif kind == '??o':
        query += f"""WHERE object = ({get_locate_query()})
                     AND (subject, predicate) {symbol} (({get_locate_query()}), ({get_locate_query()}))
                     ORDER BY object, subject, predicate"""
        return query, (last_o, last_s, last_p)
    else:
        raise Exception(f"Unkown pattern type: {kind}")


def get_locate_query():
    return "SELECT id FROM catalog WHERE md5(value) = md5(%s)"


def get_extract_query():
    return "SELECT value FROM catalog WHERE id = %s"


def get_insert_query(table_name):
    """Build a SQL query to insert a RDF triple into a PostgreSQL dataset"""
    return f"INSERT INTO {table_name} (subject,predicate,object) VALUES (%s,%s,%s) ON CONFLICT (subject,predicate,object) DO NOTHING"


def get_insert_many_query(table_name):
    """Build a SQL query to insert a RDF triple into a PostgreSQL dataset"""
    return f"INSERT INTO {table_name} (subject,predicate,object) VALUES %s ON CONFLICT (subject,predicate,object) DO NOTHING"


def get_catalog_insert_query():
    """Build a SQL query to insert a RDF term into a PostgreSQL dataset"""
    return f"INSERT INTO catalog (value) VALUES (%s) ON CONFLICT (md5(value)) DO UPDATE SET value=EXCLUDED.value RETURNING ID"


def get_catalog_insert_many_query():
    """Build a SQL query to insert a RDF term into a PostgreSQL dataset"""
    return f"INSERT INTO catalog (value) VALUES %s ON CONFLICT (md5(value)) DO UPDATE SET value=EXCLUDED.value RETURNING ID"


def get_delete_query(table_name):
    """Build a SQL query to delete a RDF triple form a PostgreSQL dataset"""
    return f"""DELETE FROM {table_name}
               WHERE subject = ({get_locate_query()})
               AND predicate = ({get_locate_query()})
               AND object = ({get_locate_query()})"""
