# update_sequence.py
# Author: Thomas MINIER - MIT License 2017-2019
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator
from sage.query_engine.exceptions import DeleteInsertConflict


class UpdateSequenceOperator(PreemptableIterator):
    """
        An UpdateSequenceOperator evaluates a "IF_EXISTS {} DELETE {} INSERT {}" query.
        It is used to provide serializability per solution group.
        To do so, it sequentually evaluates a IfExistsOperator, then a DeleteOperator and finally an InsertOperator.
    """

    def __init__(self, if_exists_op, delete_op, insert_op):
        super(UpdateSequenceOperator, self).__init__()
        self._if_exists_op = if_exists_op
        self._delete_op = delete_op
        self._insert_op = insert_op

    def serialized_name(self):
        return "update_sequence"

    def has_next(self):
        # abort if a conflict was detected
        if self._if_exists_op.missing_nquads:
            raise DeleteInsertConflict('A read-write conflict has been detected. It seems that a concurrent SPARQL query has already deleted some RDF triples that you previously read.')
        return self._if_exists_op.has_next() or self._delete_op.has_next() or self._insert_op.has_next()

    async def next(self):
        """Advance in the sequence of operations"""
        # abort if a conflict was detected
        if self._if_exists_op.missing_nquads:
            raise DeleteInsertConflict('A read-write conflict has been detected. It seems that a concurrent SPARQL query has already deleted some RDF triples that you previously read.')
        if not self.has_next():
            raise StopIteration()

        # advance the sequence
        if self._if_exists_op.has_next():
            await self._if_exists_op.next()
        elif self._delete_op.has_next():
            await self._delete_op.next()
        elif self._insert_op.has_next():
            await self._insert_op.next()
        return None

    def save(self):
        """Useless for this operator, as it MUST run completely inside a quantum"""
        return ''
