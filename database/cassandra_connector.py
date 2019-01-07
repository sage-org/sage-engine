from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from database.db_connector import DatabaseConnector
from database.db_iterator import DBIterator
from base64 import b64encode, b64decode
import os

class CassandraIterator(DBIterator):
    """A CassandraIterator implements a DBIterator for a triple pattern evaluated using cassandra """

    def __init__(self, source, pattern, start_offset=0):
        super(CassandraIterator, self).__init__(pattern)
        self._source = source
        self._paging_state = source.paging_state
        print(type(source))
        print(type(self._source))
        print(type(iter(source)))

    def last_read(self):
        """Return the ID of the last element read"""
        #print(b64encode(self._paging_state).decode('utf-8'))
        if self._paging_state is None:
            return 'None'
        return b64encode(self._paging_state).decode('utf-8')
        # return ''

    def next(self):
        """Return the next solution mapping or raise `StopIteration` if there are no more solutions"""
        print('next')
        #Attention la sauvegarde du paging state ne doit pas etre faite nimporte comment
        #i.e il faut la sauvegarder avant de fetch
        res = self._source.current_rows
        self._paging_state = self._source.paging_state
        self._source.fetch_next_page()
        #il faut renvoyer un tuple (res c'est un cassandra.Row et pas tuple)
        return (res[0][0], res[0][1], res[0][2])

    def has_next(self):
        """Return True if there is still results to read, and False otherwise"""
        print('has next')
        return self._source.has_more_pages


class CassandraConnector(DatabaseConnector):

    def __init__(self, file):
        self.keyspace = file
        # print(os.path(config["keyspace"])
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

        #choix de la table (hexastore/c)
        # if subject is not None and predicate is not None:
        #     record = "spo"
        # elif subject is not None and obj is not None:
        #     record = "spo"
        # elif subject is not None:
        #     record = "spo"
        # elif obj is not None and subject is not None:
        #     record = "osp"
        # elif obj is not None and predicate is not None:
        #     record = "osp"
        # elif obj is not None:
        #     record = "osp"
        # elif predicate is not None and subject is not None:
        #     record = "pos"
        # elif predicate is not None and obj is not None:
        #     record = "pos"
        # elif predicate is not None:
        #     record = "pos"
        # else:
        #     record = "spo"

        if subject is None and predicate and obj is None:
            record = "pos"
        elif subject is None and predicate and obj:
            record = "pos"
        elif subject and predicate is None and obj:
            record = "osp"
        elif subject is None and predicate is None and obj:
            record = "osp"
        else:
            record = "spo"


        print(offset)
        offset=b64decode(offset) if offset is not None else None
        query = "SELECT sujet, predicat, objet FROM " + record + " "
        subject = subject if (subject is not None) and (not subject.startswith('?')) else ""
        if subject:
            subject = " $$" + subject.replace("$", "\$") + "$$ "
            query += " WHERE sujet = " + subject
        predicate = predicate if (predicate is not None) and (not predicate.startswith('?')) else ""
        if predicate:
            predicate = " $$" + predicate.replace("$", "\$") + "$$ "
            if subject:
                query += " and predicat = " + predicate
            else:
                query += " WHERE predicat = " + predicate
        obj = obj if (obj is not None) and (not obj.startswith('?')) else ""
        if obj:
            obj = " $$" + obj.replace("$", "\$") + "$$ "
            if predicate or subject:
                query += " and objet = " + obj
            else:
                query += " WHERE objet = " + obj
        query += " ALLOW FILTERING;"
        # convert None & empty string to offset = 0
        # offset = 0 if offset is None or offset == '' else int(float(offset))

        #query += " limit 10"
        # print(query)
        # query2 = "SELECT sujet, predicat, objet FROM records WHERE sujet = 'a4'"

        #IL faut recuperer celui de la configuration

        tailleFetch = 1
        # statement = SimpleStatement(query, fetch_size=2000)
        statement = SimpleStatement(query, fetch_size=tailleFetch)
        print(offset)
        print(type(offset))
        print(query)

        cluster = Cluster()
        session = cluster.connect()
        print("FILE")
        print(self.keyspace)
        # Le changement de keyspace ne doit pas etre fait
        # ici si on fait ca bien mais on arrive pas a le declarer
        # dans linit
        session.set_keyspace(self.keyspace)
        if offset is not None:
            res=session.execute_async(statement,paging_state=offset)
        else:
            res=session.execute_async(statement)
        resultat = res.result()
        # print(resultat[0])
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
