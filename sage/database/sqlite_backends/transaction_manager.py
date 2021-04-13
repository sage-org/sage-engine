# transaction_manager.py
# Author: Thomas MINIER - MIT License 2017-2019
from os import getpid
import sqlite3


class TransactionManager:
    """A TransactionManager handles transactions for a (MVCC-)PostgreSQL connector"""

    def __init__(self, database):
        super(TransactionManager, self).__init__()
        self._database = database
        self._connections = dict()
        self._transactions = dict()

    def __is_open(self, connection):
        try:
            connection.cursor()
            return True
        except:
            return False

    def is_open(self):
        """Returns True if the current process has an open connection to the DB"""
        pid = getpid()
        return pid in self._connections

    def get_connection(self):
        """Get the connection of a given processs"""
        pid = getpid()
        if not self.__is_open(self._connections[pid]):
            del self._connections[pid]
            self.open_connection()
        return self._connections[pid]

    def open_connection(self):
        """Open a new connection for a given process"""
        pid = getpid()
        if pid not in self._connections:
            self._connections[pid] = sqlite3.connect(self._database)

    def close_connection(self):
        """Close the connection for a given processs"""
        pid = getpid()
        if pid in self._connections:
            self._connections[pid].rollback()
            if pid in self._transactions:
                self._transactions[pid].close()
                del self._transactions[pid]
            self._connections[pid].close()
            del self._connections[pid]

    def close_all(self):
        """Close all connections & rollback any ongoing transactions"""
        for pid, connection in self._connections.items():
            connection.rollback()
            if pid in self._transactions:
                self._transactions[pid].close()
                del self._transactions[pid]
            connection.close()
        self._connections = dict()

    def start_transaction(self):
        """Starts a new transaction"""
        pid = getpid()
        if pid not in self._connections:
            self.open_connection()
        if pid not in self._transactions:
            self._transactions[pid] = self._connections[pid].cursor()
        return self._transactions[pid]

    def commit(self):
        """Commit an ongoing transaction"""
        pid = getpid()
        if pid in self._transactions:
            self._connections[pid].commit()
            self._transactions[pid].close()
            del self._transactions[pid]
            return True
        return False

    def abort(self):
        """Abort an ongoing transaction"""
        pid = getpid()
        if pid in self._transactions:
            self._connections[pid].rollback()
            self._transactions[pid].close()
            del self._transactions[pid]
            return True
        return False
