# exceptions.py
# Author: Thomas MINIER - MIT License 2017-2020


class DeleteInsertConflict(Exception):
    """Raised when a conflict happended during the serialization of a transaction"""
    pass

class TooManyResults(Exception):
    """Raised when the maximum number of results for a query execution has been exceeded"""
    pass

class UnsupportedSPARQL(Exception):
    """Raised when a SPARQL feature is not supported by the Sage query engine"""
    pass
