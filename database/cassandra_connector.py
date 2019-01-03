from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from database.db_connector import DatabaseConnector
from database.db_iterator import DBIterator

class CassandraIterator(DBIterator):
    """An HDTIterator implements a DBIterator for a triple pattern evaluated using an HDT file"""

    # def __init__(self, source, pattern):    #, start_offset=0
    #     super(CassandraIterator, self).__init__(pattern)
    #     self._rowset = iter(source)
    #     print('chocolat')
    #     print(self._rowset)
    #     print(type(self._rowset))
    #     # self._set = source
    #     # self._start_offset = start_offset
    #
    # def last_read(self):
    #     """Return the ID of the last element read"""
    #     return str(self._rowset[-1])
    #
    # def next(self):
    #     """Return the next solution mapping or raise `StopIteration` if there are no more solutions"""
    #     print("next")
    #     # print(str(self._set))
    #     print(str(self._rowset))
    #     print(str(next(self._rowset)))
    #     return next(self._rowset)
    #
    # def has_next(self):
    #     """Return True if there is still results to read, and False otherwise"""
    #     # if self._rowset._next:
    #     #     return True
    #     # try:
    #     #     self._rowset._next = next(self._rowset._it)
    #     #     return True
    #     # except StopIteration:
    #     #     return False
    #     return self._rowset.has_next()
    #     # print(self._rowset)
    #     # print(str(self._rowset.has_next()))
    #     # return self._rowset.has_next()
    def __init__(self, source, pattern, start_offset=0):
        super(CassandraIterator, self).__init__(pattern)
        self._source = source
        print(type(source))
        print(type(self._source))
        # self._start_offset = start_offset

    def last_read(self):
        """Return the ID of the last element read"""
        #on en a pas besoin il faudrait plutot le paging state
        return str(self._source[0])

    def next(self):
        """Return the next solution mapping or raise `StopIteration` if there are no more solutions"""
        print('next')
        return next(self._source)


    def has_next(self):
        """Return True if there is still results to read, and False otherwise"""
        print('has next')
        #return self._source.has_more_pages()
        print(type(self._source))
        print(type(self._source.has_more_pages))
        return self._source.has_more_pages

class CassandraConnector(DatabaseConnector):

    def __init__(self, file):
        super(CassandraConnector, self).__init__()

    def search(self, subject, predicate, obj, offset=None):
        """
            Get an iterator over all RDF triples matching a triple pattern.

            Args:
                - subject ``string`` - Subject of the triple pattern
                - predicate ``string`` - Predicate of the triple pattern
                - object ``string`` - Object of the triple pattern
                - offset ``string=None`` ``optional`` -  OFFSET ID used to resume scan

            Returns:
                A Python iterator over RDF triples matching the given triples pattern
        """

        query = "SELECT sujet, predicat, objet FROM records "
        subject = subject if (subject is not None) and (not subject.startswith('?')) else ""
        if subject:
            query += " WHERE sujet = " + subject
        predicate = predicate if (predicate is not None) and (not predicate.startswith('?')) else ""
        if predicate:
            query += "and predicat = " + predicate
        obj = obj if (obj is not None) and (not obj.startswith('?')) else ""
        if obj:
            query += " and obj = " + obj
        # convert None & empty string to offset = 0
        # offset = 0 if offset is None or offset == '' else int(float(offset))

        #query += " limit 10"
        print(query)

        cluster = Cluster()
        session = cluster.connect()

        session.set_keyspace('pkspo')

        tailleFetch = 1
        # statement = SimpleStatement(query, fetch_size=2000)
        statement = SimpleStatement(query, fetch_size=tailleFetch)
        res=session.execute_async(statement)
        resultat = res.result()
        print(resultat[0])
        print(type(resultat))
        pattern = {'subject': subject, 'predicate': predicate, 'object': obj}
        print('before return search')
        #le 0 c'est le card qui est renvoye avec searhc triple normalement (pour plan builder, etc)
        return CassandraIterator(resultat, pattern), 0

        # iterator, card = self._hdt.search_triples(subject, predicate, obj, offset=offset)
        # return HDTIterator(iterator, pattern, start_offset=offset), card

    @property
    def nb_triples(self):
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

    def from_config(config):
        return CassandraConnector(config["keyspace"])
