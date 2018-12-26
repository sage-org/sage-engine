from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

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
        subject = subject if (subject is not None) and (not subject.startswith('?')) else ""
        predicate = predicate if (predicate is not None) and (not predicate.startswith('?')) else ""
        obj = obj if (obj is not None) and (not obj.startswith('?')) else ""
        # convert None & empty string to offset = 0
        offset = 0 if offset is None or offset == '' else int(float(offset))
        pattern = {'subject': subject, 'predicate': predicate, 'object': obj}

        #cluster = Cluster()
        session = cluster.connect()

        session.set_keyspace('pkpos')

        query = "SELECT sujet, predicat, objet FROM records WHERE predicat='b'"
        tailleFetch = 1000
        # statement = SimpleStatement(query, fetch_size=2000)
        statement = SimpleStatement(query, fetch_size=tailleFetch)
        res=session.execute_async(statement)
        return res.result()
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
        return CassandraConnector(config["file"])
