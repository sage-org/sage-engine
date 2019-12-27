# transaction_manager.py
# Author: Thomas MINIER - MIT License 2017-2020
from os import getpid

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_SERIALIZABLE


class TransactionManager:
    """A TransactionManager handles transactions for a (MVCC-)PostgreSQL connector.
    
    Args:
      * dbname: the database name.
      * user: user name used to authenticate.
      * password: password used to authenticate.
      * host: database host address (defaults to UNIX socket if not provided).
      * port: connection port number (defaults to 5432 if not provided).
    """

    def __init__(self, dbname: str, user: str, password: str, host: str = '', port: int = 5432):
        super(TransactionManager, self).__init__()
        self._dbname = dbname
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._connections = dict()
        self._transactions = dict()

    def is_open(self) -> bool:
        """Returns True if the current process has an open connection to the DB"""
        pid = getpid()
        return pid in self._connections

    def get_connection(self):
        """Get the connection of the current processs"""
        pid = getpid()
        return self._connections[pid]

    def open_connection(self) -> None:
        """Open a new connection for the given process"""
        pid = getpid()
        if pid not in self._connections:
            self._connections[pid] = psycopg2.connect(dbname=self._dbname, user=self._user, password=self._password, host=self._host, port=self._port)
            # disable autocommit & set isolation level
            self._connections[pid].autocommit = False
            self._connections[pid].isolation_level = ISOLATION_LEVEL_SERIALIZABLE

    def close_connection(self) -> None:
        """Close the connection for a given processs"""
        pid = getpid()
        if pid in self._connections:
            self._connections[pid].rollback()
            if pid in self._transactions:
                self._transactions[pid].close()
                del self._transactions[pid]
            self._connections[pid].close()
            del self._connections[pid]

    def close_all(self) -> None:
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

    def commit(self) -> bool:
        """Commit an ongoing transaction"""
        pid = getpid()
        if pid in self._transactions:
            self._connections[pid].commit()
            self._transactions[pid].close()
            del self._transactions[pid]
            return True
        return False

    def abort(self) -> bool:
        """Abort an ongoing transaction"""
        pid = getpid()
        if pid in self._transactions:
            self._connections[pid].rollback()
            self._transactions[pid].close()
            del self._transactions[pid]
            return True
        return False
