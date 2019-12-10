# exceptions.py
# Author: Thomas MINIER - MIT License 2017-2020


class DeleteInsertConflict(Exception):
    """
        Exception raised when...
    """
    pass

class TooManyResults(Exception):
    """
        Exception raised when the max. number of results for a query execution
        has been exceeded
    """
    pass

class UnsupportedSPARQL(Exception):
    """Thrown when a SPARQL feature is not supported by the Sage query engine"""
    pass
