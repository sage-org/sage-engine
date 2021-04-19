import happybase

from os import getpid
from datetime import datetime
from typing import Optional, Tuple

from sage.database.db_connector import DatabaseConnector
from sage.database.hbase.iterator import HBaseIterator
from sage.database.hbase.utils import build_row_key
from sage.database.estimators import pattern_shape_estimate
from sage.database.utils import get_kind


def find_triples(connection, s, p, o):
    """Evaluate a triple pattern using the table SPO, POS or OSP"""
    table = None
    start_key = ''
    kind = get_kind(s, p, o)
    if kind == 'spo' or kind == 's??' or kind == 'sp?':
        table = connection.table('spo')
        start_key = build_row_key(s, p, o)
    elif kind == '???':
        table = connection.table('spo')
        # table = connection.table('pos')
        # table = connection.table('osp')
    elif kind == '?p?' or kind == '?po':
        table = connection.table('pos')
        start_key = build_row_key(p, o, s)
    elif kind == 's?o' or kind == '??o':
        table = connection.table('osp')
        start_key = build_row_key(o, s, p)
    else:
        raise Exception(f"Unkown pattern type: {kind}")
    return table, start_key


def resume_triples(connection, last_read, s, p, o):
    """Resume the evaluation of a triple pattern from a RDF triple"""
    table = None
    kind = get_kind(s, p, o)
    if kind == '???':
        table = connection.table('spo')
        # table = connection.table('pos')
        # table = connection.table('osp')
    elif kind == 'spo' or kind == 's??' or kind == 'sp?':
        table = connection.table('spo')
    elif kind == '?p?' or kind == '?po':
        table = connection.table('pos')
    elif kind == 's?o' or kind == '??o':
        table = connection.table('osp')
    else:
        raise Exception(f"Unkown pattern type: {kind}")
    return table, last_read


class HBaseConnector(DatabaseConnector):
    """A HBaseConnector allows SaGe to query RDF data stored in Apache HBase"""

    def __init__(self, graph_name: int, thrift_host: str, thrift_port: int = 9090):
        super(HBaseConnector, self).__init__()
        self._graph_name = graph_name
        self._thrift_host = thrift_host
        self._thrift_port = thrift_port
        self._connection = happybase.Connection(self._thrift_host, protocol="compact", transport="framed", port=self._thrift_port, table_prefix=self._graph_name)
        # batches used to perform updates
        self._spo_batch = None
        self._pos_batch = None
        self._osp_batch = None

    def __del__(self) -> None:
        self.close()

    def open(self):
        pass

    def close(self):
        self._connection.close()

    def commit(self):
        """Commit update batches"""
        if self._spo_batch is not None:
            self._spo_batch.send()
        if self._pos_batch is not None:
            self._pos_batch.send()
        if self._osp_batch is not None:
            self._osp_batch.send()
        # reset batches
        self._spo_batch = None
        self._pos_batch = None
        self._osp_batch = None

    def __init__batches(self):
        if self._spo_batch is None:
            self._spo_batch = self._connection.table('spo').batch()
        if self._pos_batch is None:
            self._pos_batch = self._connection.table('pos').batch()
        if self._osp_batch is None:
            self._osp_batch = self._connection.table('osp').batch()

    def __refresh_connection(self):
        try:
            list(self._connection.table('spo').scan(limit=1))
        except:
            self._connection = happybase.Connection(self._thrift_host, protocol="compact", transport="framed", port=self._thrift_port, table_prefix=self._graph_name)

    def search(self, subject: str, predicate: str, obj: str, last_read: Optional[str] = None, as_of: Optional[datetime] = None) -> Tuple[HBaseIterator, int]:
        """Get an iterator over all RDF triples matching a triple pattern.

            Args:
                * subject ``string`` - Subject of the triple pattern
                * predicate ``string`` - Predicate of the triple pattern
                * object ``string`` - Object of the triple pattern
                * last_read ``string=None`` ``optional`` -  OFFSET ID used to resume scan
                * as_of: A version timestamp. When set, perform all reads against a consistent snapshot represented by this timestamp.

            Returns:
                A tuple (`iterator`, `cardinality`), where `iterator` is a Python iterator over RDF triples matching the given triples pattern, and `cardinality` is the estimated cardinality of the triple pattern
        """
        subject = subject if (subject is not None) and (not subject.startswith('?')) else None
        predicate = predicate if (predicate is not None) and (not predicate.startswith('?')) else None
        obj = obj if (obj is not None) and (not obj.startswith('?')) else None
        pattern = {'subject': subject, 'predicate': predicate, 'object': obj}

        self.__refresh_connection()

        if last_read is None:
            (table, row_key) = find_triples(self._connection, subject, predicate, obj)
        else:
            (table, row_key) = resume_triples(self._connection, last_read, subject, predicate, obj)

        iterator = HBaseIterator(self._connection, table, row_key, pattern)
        card = pattern_shape_estimate(subject, predicate, obj) if iterator.has_next() else 0
        return iterator, card

    def insert(self, s: str, p: str, o: str) -> None:
        """Insert a RDF triple into the database"""
        self.__init__batches()
        columns = {
            b'rdf:subject': s.encode('utf-8'),
            b'rdf:predicate': p.encode('utf-8'),
            b'rdf:object': o.encode('utf-8')
        }
        spo_key = build_row_key(s, p, o)
        pos_key = build_row_key(p, o, s)
        osp_key = build_row_key(o, s, p)
        self._spo_batch.put(spo_key, columns)
        self._pos_batch.put(pos_key, columns)
        self._spo_batch.put(osp_key, columns)

    def delete(self, s: str, p: str, o: str) -> None:
        """Delete a RDF triple from the database"""
        self.__init__batches()
        spo_key = build_row_key(s, p, o)
        pos_key = build_row_key(p, o, s)
        osp_key = build_row_key(o, s, p)
        self._spo_batch.delete(spo_key)
        self._pos_batch.delete(pos_key)
        self._spo_batch.delete(osp_key)

    def from_config(config: dict) -> DatabaseConnector:
        """Build a HBaseConnector from a configuration object"""
        if 'thrift_host' not in config:
            raise SyntaxError('A valid configuration for a Apache HBase connector must contains the thrift_host field')
        graph_name = config['name']
        port = config['thrift_port'] if 'thrift_port' in config else 9090
        return HBaseConnector(graph_name, config['thrift_host'], thrift_port=port)

    @property
    def nb_triples(self):
        """Get the number of RDF triples in the database"""
        return 0

    @property
    def nb_subjects(self):
        """Get the number of subjects in the database"""
        return 0

    @property
    def nb_predicates(self):
        """Get the number of predicates in the database"""
        return 0

    @property
    def nb_objects(self):
        """Get the number of objects in the database"""
        return 0
