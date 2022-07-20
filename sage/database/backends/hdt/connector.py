import os.path

from typing import Optional, Tuple
from datetime import datetime
from hdt import HDTDocument

from sage.database.backends.db_connector import DatabaseConnector
from sage.database.backends.hdt.iterator import HDTIterator


class HDTFileConnector(DatabaseConnector):
    """
    A HDTFileConnector search for RDF triples in a HDT file.

    Parameters
    ----------
    file: str
        Path to the HDT file.
    mapped: bool
        True to map the HDT file on disk (faster), False to load everything in memory.
    indexed: bool
        True if the HDT must be loaded with indexes, False otherwise.
    """

    def __init__(self, file: str, mapped=True, indexed=True):
        super(HDTFileConnector, self).__init__()
        self._hdt = HDTDocument(file, map=mapped, indexed=indexed)

    def search(
        self, subject: str, predicate: str, obj: str,
        last_read: Optional[str] = None, as_of: Optional[datetime] = None
    ) -> Tuple[HDTIterator, int]:
        """
        Get an iterator over all RDF triples matching a triple pattern.

        Parameters
        ----------
        subject: str
            Subject of the triple pattern.
        predicate: str
            Predicate of the triple pattern.
        object: str
            Object of the triple pattern.
        last_read: None | str
            A RDF triple ID. When set, the search is resumed for this RDF triple.
        as_of: None | datetime
            A version timestamp. When set, perform all reads against a
            consistent snapshot represented by this timestamp.

        Returns
        -------
        Tuple[DBIterator, int]
            A tuple (iterator, cardinality) where:
                - iterator: Python iterator over RDF triples matching the given
                  triple pattern.
                - cardinality: Estimated cardinality of the triple pattern.
        """
        subject = subject if (subject is not None) and (not subject.startswith("?")) else ""
        predicate = predicate if (predicate is not None) and (not predicate.startswith("?")) else ""
        obj = obj if (obj is not None) and (not obj.startswith("?")) else ""

        offset = 0 if last_read is None or last_read == "" else int(float(last_read))

        pattern = {"subject": subject, "predicate": predicate, "object": obj}
        iterator, card = self._hdt.search_triples(subject, predicate, obj, offset=offset)
        return HDTIterator(iterator, pattern, start_offset=offset), card

    @property
    def nb_triples(self) -> int:
        return self._hdt.total_triples

    @property
    def nb_subjects(self) -> int:
        """
        Get the number of subjects in the database.
        """
        return self._hdt.nb_subjects

    @property
    def nb_predicates(self) -> int:
        """
        Get the number of predicates in the database.
        """
        return self._hdt.nb_predicates

    @property
    def nb_objects(self) -> int:
        """
        Get the number of objects in the database.
        """
        return self._hdt.nb_objects

    def from_config(config: dict):
        """
        Builds a HDTFileFactory from a configuration object.

        Parameters
        ----------
        config: Dict[str, Any]
            Configuration object. Must contains the "file" field.

        Example
        -------
        >>> config = { "file": "./dbpedia.hdt" }
        >>> connector = HDTFileConnector.from_config(config)
        >>> print(f"The HDT file contains {connector.nb_triples} RDF triples")
        """
        if not os.path.isfile(config["file"]):
            raise Exception("HDT file not found: " + config["file"])
        mapped = config.setdefault("mapped", True)
        indexed = config.setdefault("indexed", True)
        return HDTFileConnector(config["file"], mapped=mapped, indexed=indexed)
