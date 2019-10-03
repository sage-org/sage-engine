# exceptions.py
# Author: Thomas MINIER - MIT License 2017-2018


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
