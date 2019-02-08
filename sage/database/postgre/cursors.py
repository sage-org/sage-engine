# cursors.py
# Author: Thomas MINIER - MIT License 2017-2019
from uuid import uuid4
from sage.database.utils import get_kind

# mapping: triple pattern -> PL/SQL cursor factory function
CURSORS_FUNCTIONS = {
    '???': {
        'start': 'sage_scan_vvv',
        'resume': 'sage_resume_vvv'
    },
    'spo': {
        'start': 'sage_scan_spo',
        'resume': None
    },
    's??': {
        'start': 'sage_scan_svv',
        'resume': 'sage_resume_svv'
    },
    'sp?': {
        'start': 'sage_scan_spv',
        'resume': 'sage_resume_spv'
    },
    '?p?': {
        'start': 'sage_scan_vpv',
        'resume': 'sage_resume_vpv'
    },
    '?po': {
        'start': 'sage_scan_vpo',
        'resume': 'sage_resume_vpo'
    },
    's?o': {
        'start': 'sage_scan_svo',
        'resume': 'sage_resume_svo'
    },
    '??o': {
        'start': 'sage_scan_vvo',
        'resume': 'sage_resume_vvo'
    }
}


def create_start_cursor(cursor, subject, predicate, obj):
    """Creates a server-side cursor to evaluation a triple pattern using an index scan"""
    kind = get_kind(subject, predicate, obj)
    cursor_name = "sage_start_cursor_{}_{}".format(kind, str(uuid4()))
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
    cursor.callproc(CURSORS_FUNCTIONS[kind]['start'], params)
    return cursor_name


def create_resume_cursor(cursor, pattern, subject, predicate, obj):
    """
        Creates a server cursor to resume evaluation of a triple pattern using an index scan.
        'kind' can be one of: '???', 'spo', 's??', 'sp?', '?p?', '?po', 's?o' and '??o'
    """
    kind = get_kind(pattern[0], pattern[1], pattern[2])
    cursor_name = "sage_resume_cursor_{}_{}".format(kind, str(uuid4()))
    cursor.callproc(CURSORS_FUNCTIONS[kind]['resume'], [cursor_name, subject, predicate, obj])
    return cursor_name
