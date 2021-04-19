# postgre_connector.py
# Author: Thomas MINIER - MIT License 2017-2019
import json
import uuid
import logging

from math import ceil
from time import time
from functools import reduce
from typing import Dict, List, Optional, Tuple

from sage.database.db_connector import DatabaseConnector
from sage.database.db_iterator import DBIterator, EmptyIterator
from sage.database.sqlite_backends.transaction_manager import TransactionManager
from sage.database.utils import get_kind


class SQliteConnector(DatabaseConnector):
    """
        A SQliteConnector search for RDF triples in a SQlite database.
        Constructor arguments:
            - table_name `str`: Name of the SQL table containing RDF data.
            - database `str`: the name of the sqlite database file.
            - fetch_size `int`: how many RDF triples are fetched per SQL query (default to 500)
    """

    def __init__(self, table_name: str, database: str, fetch_size: int = 500):
        super(SQliteConnector, self).__init__()
        self._table_name = table_name
        self._manager = TransactionManager(database)
        self._fetch_size = fetch_size
        self._warmup = True

        # Data used for cardinality estimation.
        # They are initialized using SQlite statistics, after the 1st connection to the DB.
        self._spo_index_stats = {
            'row_count': 0,
            'same_s_row_count': 0,
            'same_sp_row_count': 0,
            'same_spo_row_count': 0
        }
        self._pos_index_stats = {
            'row_count': 0,
            'same_p_row_count': 0,
            'same_po_row_count': 0,
            'same_pos_row_count': 0
        }
        self._osp_index_stats = {
            'row_count': 0,
            'same_o_row_count': 0,
            'same_os_row_count': 0,
            'same_osp_row_count': 0
        }

    def open(self):
        """Open the database connection"""
        if self._manager.is_open():
            self._manager.open_connection()

        # Do warmup phase if required, i.e., fetch stats for query execution
        if self._warmup:
            cursor = self._manager.start_transaction()
            # improve SQlite performance using PRAGMA
            # cursor.execute("PRAGMA mmap_size=10485760")
            # cursor.execute("PRAGMA cache_size=-10000")
            # fetch SPO index statistics
            cursor.execute(f'SELECT stat FROM sqlite_stat1 WHERE idx = \'{self._table_name}_spo_index\'')
            (row_count, same_s_row_count, same_sp_row_count, same_spo_row_count) = cursor.fetchone()[0].split(' ')
            self._spo_index_stats = {
                'row_count': int(row_count),
                'same_s_row_count': int(same_s_row_count),
                'same_sp_row_count': int(same_sp_row_count),
                'same_spo_row_count': int(same_spo_row_count)
            }
            # fetch POS index statistics
            cursor.execute(f'SELECT stat FROM sqlite_stat1 WHERE idx = \'{self._table_name}_pos_index\'')
            (row_count, same_p_row_count, same_po_row_count, same_pos_row_count) = cursor.fetchone()[0].split(' ')
            self._pos_index_stats = {
                'row_count': int(row_count),
                'same_p_row_count': int(same_p_row_count),
                'same_po_row_count': int(same_po_row_count),
                'same_pos_row_count': int(same_pos_row_count)
            }
            # fetch OSP index statistics
            cursor.execute(f'SELECT stat FROM sqlite_stat1 WHERE idx = \'{self._table_name}_osp_index\'')
            (row_count, same_o_row_count, same_os_row_count, same_osp_row_count) = cursor.fetchone()[0].split(' ')
            self._osp_index_stats = {
                'row_count': int(row_count),
                'same_o_row_count': int(same_o_row_count),
                'same_os_row_count': int(same_os_row_count),
                'same_osp_row_count': int(same_osp_row_count)
            }
            # commit & close cursor
            self._manager.commit()
            self._warmup = False

    def close(self):
        """Close the database connection"""
        # commit, then close the cursor and the connection
        self._manager.close_connection()

    def start_transaction(self):
        """Start a PostgreSQL transaction"""
        # print("Process {} called start_transaction".format(os.getpid()))
        self._manager.start_transaction()

    def commit_transaction(self):
        """Commit any ongoing transaction"""
        self._manager.commit()

    def abort_transaction(self):
        """Abort any ongoing transaction"""
        self._manager.abort()

    def _estimate_cardinality(self, subject, predicate, obj) -> int:
        """
            Estimate the cardinality of a triple pattern using SQlite statistics.
            Args:
                - subject ``string`` - Subject of the triple pattern
                - predicate ``string`` - Predicate of the triple pattern
                - obj ``string`` - Object of the triple pattern
            Returns:
                The estimated cardinality of the triple pattern
        """
        # estimate triple cardinality using sqlite statistics (more or less a variable counting join ordering)
        kind = get_kind(subject, predicate, obj)
        if kind == 'spo':
            return self._spo_index_stats['same_spo_row_count']
        elif kind == '???':
            return self._spo_index_stats['row_count']
        elif kind == 's??':
            return self._spo_index_stats['same_s_row_count']
        elif kind == 'sp?':
            return self._spo_index_stats['same_sp_row_count']
        elif kind == '?p?':
            return self._pos_index_stats['same_p_row_count']
        elif kind == '?po':
            return self._pos_index_stats['same_po_row_count']
        elif kind == 's?o':
            return self._osp_index_stats['same_os_row_count']
        elif kind == '??o':
            return self._osp_index_stats['same_o_row_count']
        else:
            raise Exception(f"Unkown pattern type: {kind}")
