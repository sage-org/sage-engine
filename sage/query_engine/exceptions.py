class DeleteInsertConflict(Exception):
    """
    Raised when a conflict happended during the serialization of a transaction.
    """
    pass


class QuantumExhausted(Exception):
    """
    Raised when the time quantum for a query execution has been exceeded.
    """
    pass


class TooManyResults(Exception):
    """
    Raised when the maximum number of results for a query execution has been
    exceeded.
    """
    pass


class TOPKLimitReached(Exception):
    """
    Raised when the buffer of the TOPKIterator is full.
    """
    pass


class UnsupportedSPARQL(Exception):
    """
    Raised when a SPARQL feature is not supported by the Sage query engine.
    """
    pass
