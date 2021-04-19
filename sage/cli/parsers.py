import sys
import re

from abc import ABC, abstractmethod
from rdflib.namespace import XSD
from rdflib.term import Literal, BNode, URIRef
from rdflib.plugins.parsers.ntriples import NTriplesParser, unquote, uriquote

uriref = r'<([^:]+:[^\s"<>]*)>'
literal = r'"([^"\\]*(?:\\.[^"\\]*)*)"'
litinfo = r'(?:@([a-zA-Z]+(?:-[a-zA-Z0-9]+)*)|\^\^' + uriref + r')?'
exponent = r'[eE][+-]?[0-9]+'

r_wspace = re.compile(r'[ \t]*')
r_wspaces = re.compile(r'[ \t]+')
r_tail = re.compile(r'[ \t]*\.[ \t]*(#.*)?')
r_literal = re.compile(literal + litinfo)
r_integer = re.compile(r'[0-9]+')
r_decimal = re.compile(r'([0-9]+\.[0-9]*|\.[0-9]+)')
r_double = re.compile(rf'([0-9]+\.[0-9]*{exponent}|\.[0-9]+{exponent}|[0-9]+{exponent})')
r_boolean = re.compile(r'(true|false)')


class ParseError(Exception):
    """Raised Raised when an error occurs while parsing an RDF file."""
    pass

class Parser(ABC):

    def __init__(self, bucket_size=100):
        self.bucket_size = bucket_size
        self.bucket = list()

    def on_bucket(self, bucket):
        """Called when a new bucket of triples is ready to be inserted into the database."""
        pass

    def on_error(self, error):
        """Called when an error is raised by the parser."""
        pass

    def on_complete(self):
        """Called when the file has been fully parsed."""
        pass

    @abstractmethod
    def parsefile(self, file_path):
        """Parse a RDF file into bucket of triples."""
        pass


class CustomNTriplesParser(Parser, NTriplesParser):

    def __init__(self, bucket_size=100):
        super(CustomNTriplesParser, self).__init__(bucket_size)

    def parse(self):
        while True:
            line = self.readline()
            self.line = line
            if self.line is None:
                if len(self.bucket) > 0:
                    self.on_bucket(self.bucket)
                self.on_complete()
                break
            self.parseline()

    def parseline(self):
        line = self.line
        try:
            self.eat(r_wspace)
            if (not self.line) or self.line.startswith('#'):
                return  # The line is empty or a comment

            subject = self.subject()
            subject.n3()
            self.eat(r_wspaces)

            predicate = self.predicate()
            predicate.n3()
            self.eat(r_wspaces)

            object = self.object()
            object.n3()
            self.eat(r_tail)

            subject = str(subject)
            predicate = str(predicate)
            if isinstance(object, Literal) or isinstance(object, BNode):
                object = object.n3()
            else:
                object = str(object)

            self.bucket.append((subject, predicate, object))
        except ParseError as error:
            self.on_error(error)
        except:
            self.on_error(ParseError(f"Invalid triple: {line}"))
        finally:
            if len(self.bucket) >= self.bucket_size:
                self.on_bucket(self.bucket)
                self.bucket = list()

    def literal(self):
        if self.peek('"'):
            lit, lang, dtype = self.eat(r_literal).groups()
            if lang:
                lang = lang
            else:
                lang = None
            if dtype:
                dtype = unquote(dtype)
                dtype = uriquote(dtype)
                dtype = URIRef(dtype)
            elif re.fullmatch(r_integer, lit):
                dtype = XSD.integer
            elif re.fullmatch(r_decimal, lit):
                dtype = XSD.decimal
            elif re.fullmatch(r_double, lit):
                dtype = XSD.double
            elif re.fullmatch(r_boolean, lit):
                dtype = XSD.boolean
            else:
                dtype = None
            if lang and dtype:
                raise ParseError("Can't have both a language and a datatype")
            lit = unquote(lit)
            return Literal(lit, lang, dtype)
        return False


class NTParser(CustomNTriplesParser):

    def parsefile(self, file_path):
        """Parse an N-Triples file."""
        self.file = open(file_path, 'r')
        self.buffer = ''
        self.parse()


class HDTParser(CustomNTriplesParser):

    def __init__(self, bucket_size):
        super(HDTParser, self).__init__(bucket_size)
        self.iterator = None

    def parsefile(self, file_path):
        """Parse an HDT file as an N-Triples file."""

        from hdt import HDTDocument

        doc = HDTDocument(file_path, indexed=False)
        iterator, _ = doc.search_triples("", "", "")
        self.iterator = iterator
        self.parse()

    def readline(self):
        """Convert triples read from an HDT file into N-Triples."""
        try:
            (subject, predicate, object) = next(self.iterator)
            if subject.startswith('http'):
                subject = f'<{subject}>'
            if predicate.startswith('http'):
                predicate = f'<{predicate}>'
            if object.startswith('http'):
                object = f'<{object}>'
            return f'{subject} {predicate} {object}.'
        except StopIteration:
            return None


class ParserFactory():

    def create_parser(format: str, bucket_size: int = 100) -> Parser:
        if format == 'hdt':
            return HDTParser(bucket_size)
        elif format == 'nt':
            return NTParser(bucket_size)
        else:
            raise Exception(f'Unsupported RDF format: "{format}"')
