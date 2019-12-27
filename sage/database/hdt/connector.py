# hdt_file_connector.py
# Author: Thomas MINIER - MIT License 2017-2020
import os.path
from typing import Optional, Tuple

from hdt import HDTDocument

from sage.database.db_connector import DatabaseConnector
from sage.database.hdt.iterator import HDTIterator

from datetime import datetime

class HDTFileConnector(DatabaseConnector):
    """A HDTFileConnector search for RDF triples in a HDT file.
    
    Args:
      * file: Path to the HDT file.
      * mapped: True maps the HDT file on disk (faster), False loads everything in memory.
      * indexed: True if the HDT must be loaded with indexes, False otherwise.
    """

    def __init__(self, file: str, mapped=True, indexed=True):
        super(HDTFileConnector, self).__init__()
        self._hdt = HDTDocument(file, map=mapped, indexed=indexed)

    def search(self, subject: str, predicate: str, obj: str, last_read: Optional[str] = None, as_of: Optional[datetime] = None) -> Tuple[HDTIterator, int]:
        """Get an iterator over all RDF triples matching a triple pattern.

        Args:
          * subject: Subject of the triple pattern.
          * predicate: Predicate of the triple pattern.
          * object: Object of the triple pattern.
          * last_read: A RDF triple ID. When set, the search is resumed for this RDF triple.
          * as_of: A version timestamp. When set, perform all reads against a consistent snapshot represented by this timestamp.
          
        Returns:
          A tuple (`iterator`, `cardinality`), where `iterator` is a Python iterator over RDF triples matching the given triples pattern, and `cardinality` is the estimated cardinality of the triple pattern.
        """
        subject = subject if (subject is not None) and (not subject.startswith('?')) else ""
        predicate = predicate if (predicate is not None) and (not predicate.startswith('?')) else ""
        obj = obj if (obj is not None) and (not obj.startswith('?')) else ""
        # convert None & empty string to offset = 0
        offset = 0 if last_read is None or last_read == '' else int(float(last_read))
        pattern = {'subject': subject, 'predicate': predicate, 'object': obj}
        iterator, card = self._hdt.search_triples(subject, predicate, obj, offset=offset)
        return HDTIterator(iterator, pattern, start_offset=offset), card

    @property
    def nb_triples(self) -> int:
        return self._hdt.total_triples

    @property
    def nb_subjects(self) -> int:
        """Get the number of subjects in the database"""
        return self._hdt.nb_subjects

    @property
    def nb_predicates(self) -> int:
        """Get the number of predicates in the database"""
        return self._hdt.nb_predicates

    @property
    def nb_objects(self) -> int:
        """Get the number of objects in the database"""
        return self._hdt.nb_objects

    def from_config(config: dict):
        """Build a HDTFileFactory from a configuration object.
        
        Args:
          * config: configuration object. Must contains the 'file' field.
        
        Example:
          >>> config = { "file": "./dbpedia.hdt" }
          >>> connector = HDTFileConnector.from_config(config)
          >>> print(f"The HDT file contains {connector.nb_triples} RDF triples")
        """
        if not os.path.isfile(config["file"]):
            raise Exception(f"HDT file not found: {config['file']}")
        mapped = config['mapped'] if 'mapped' in config else True
        indexed = config['indexed'] if 'indexed' in config else True
        return HDTFileConnector(config["file"], mapped=mapped, indexed=indexed)
