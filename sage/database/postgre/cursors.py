# cursors.py
# Author: Thomas MINIER - MIT License 2017-2019
from uuid import uuid4
from sage.database.utils import get_kind

# mapping: triple pattern -> PL/SQL cursor factory function
CURSORS_FUNCTIONS = {
    '???': {
        'start': '{}_scan_vvv',
        'resume': '{}_resume_vvv'
    },
    'spo': {
        'start': '{}_scan_spo',
        'resume': None
    },
    's??': {
        'start': '{}_scan_svv',
        'resume': '{}_resume_svv'
    },
    'sp?': {
        'start': '{}_scan_spv',
        'resume': '{}_resume_spv'
    },
    '?p?': {
        'start': '{}_scan_vpv',
        'resume': '{}_resume_vpv'
    },
    '?po': {
        'start': '{}_scan_vpo',
        'resume': '{}_resume_vpo'
    },
    's?o': {
        'start': '{}_scan_svo',
        'resume': '{}_resume_svo'
    },
    '??o': {
        'start': '{}_scan_vvo',
        'resume': '{}_resume_vvo'
    }
}


def create_start_cursor(cursor, table_name, subject, predicate, obj):
    """
        Creates a server-side cursor to evaluation a triple pattern using an index scan.

        Args:
            - cursor: `psycopg2` Cursor used to execute SQL queries
            - table_name `str`: Name of the SQL table containing RDF data
            - subject `str`: Subject of the triple pattern to execute
            - predicate `str`: Predicate of the triple pattern to execute
            - obj `str`: Object of the triple pattern to execute
        Returns:
            A `psycopg2` server-side cursor, iterating over all RDF triples matching the triple pattern.

    """
    kind = get_kind(subject, predicate, obj)
    cursor_name = "{}_start_{}_cursor_{}".format(table_name, kind, str(uuid4()))
    params = list()
    if kind == 'spo':
        params = [cursor_name, subject, predicate, obj]
    elif kind == '???':
        params = [cursor_name]
    elif kind == 's??':
        params = [cursor_name, subject]
    elif kind == 'sp?':
        params = [cursor_name, subject, predicate]
    elif kind == '?p?':
        params = [cursor_name, predicate]
    elif kind == '?po':
        params = [cursor_name, predicate, obj]
    elif kind == 's?o':
        params = [cursor_name, subject, obj]
    elif kind == '??o':
        params = [cursor_name, obj]
    else:
        raise Exception("Unkown pattern type: {}".format(kind))
    sql_fn = CURSORS_FUNCTIONS[kind]['start'].format(table_name)
    cursor.callproc(sql_fn, params)
    return cursor_name


def create_resume_cursor(cursor, table_name, pattern, subject, predicate, obj):
    """
        Creates a server cursor to resume evaluation of a triple pattern using an index scan.

        Args:
            - cursor: `psycopg2` Cursor used to execute SQL queries
            - table_name `str`: Name of the SQL table containing RDF data
            - pattern `(str, str, str)`: Triple pattern to resume
            - subject `str`: Subject of the RDF triple used to resume the cursor
            - predicate `str`: Predicate of the RDF triple used to resume the cursor
            - obj `str`: Object of the RDF triple used to resume the cursor
        Returns:
            A `psycopg2` server-side cursor, resuming iterating over all RDF triples matching the triple pattern.
    """
    kind = get_kind(pattern[0], pattern[1], pattern[2])
    cursor_name = "{}_resume_{}_cursor_{}".format(table_name, kind, str(uuid4()))
    sq_fn = CURSORS_FUNCTIONS[kind]['resume'].format(table_name)
    cursor.callproc(sq_fn, [cursor_name, subject, predicate, obj])
    return cursor_name
