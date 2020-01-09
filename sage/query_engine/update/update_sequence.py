# update_sequence.py
# Author: Thomas MINIER - MIT License 2017-2020
from typing import Dict, Optional

from sage.query_engine.exceptions import DeleteInsertConflict
from sage.query_engine.iterators.preemptable_iterator import PreemptableIterator


class UpdateSequenceOperator(PreemptableIterator):
    """An UpdateSequenceOperator evaluates a "IF_EXISTS DELETE INSERT" query.
    
    It is used to provide serializability per solution group.
    To do so, it sequentually evaluates a IfExistsOperator, then a DeleteOperator and finally an InsertOperator.

    Args:
      * if_exists_op: Operator used to evaluated the IF_EXISTS clause.
      * delete_op: Operator used to evaluated the DELETE clause.
      * insert_op: Operator used to evaluated the INSERT clause.
    """

    def __init__(self, if_exists_op: PreemptableIterator, delete_op: PreemptableIterator, insert_op: PreemptableIterator):
        super(UpdateSequenceOperator, self).__init__()
        self._if_exists_op = if_exists_op
        self._delete_op = delete_op
        self._insert_op = insert_op

    def serialized_name(self) -> str:
        """Get the name of the iterator, as used in the plan serialization protocol"""
        return "update_sequence"

    def has_next(self) -> bool:
        """Return True if the iterator has more quads to process."""
        # abort if a conflict was detected
        if self._if_exists_op.missing_nquads:
            raise DeleteInsertConflict('A read-write conflict has been detected. It seems that a concurrent SPARQL query has already deleted some RDF triples that you previously read.')
        return self._if_exists_op.has_next() or self._delete_op.has_next() or self._insert_op.has_next()

    async def next(self) -> Optional[Dict[str, str]]:
        """Advance in the sequence of operations.

        This function works in an iterator fashion, so it can be used in a pipeline of iterators.
        It may also contains `non interruptible` clauses which must 
        be atomically evaluated before preemption occurs.

        Returns: Always `None` 

        Throws:
          * `StopAsyncIteration` if the iterator has fnished query processing.
          * `DeleteInsertConflict` if a read-write conflict is detected.
        """
        # abort if a conflict was detected
        if self._if_exists_op.missing_nquads:
            raise DeleteInsertConflict('A read-write conflict has been detected. It seems that a concurrent SPARQL query has already deleted some RDF triples that you previously read.')
        if not self.has_next():
            raise StopAsyncIteration()

        # advance the sequence
        if self._if_exists_op.has_next():
            await self._if_exists_op.next()
        elif self._delete_op.has_next():
            await self._delete_op.next()
        elif self._insert_op.has_next():
            await self._insert_op.next()
        return None

    def save(self) -> str:
        """Useless for this operator, as it MUST run completely inside a quantum"""
        return ''
