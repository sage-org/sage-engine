# schema.py
# Author: Thomas MINIER - MIT License 2017-2018
from marshmallow import Schema, fields


class ManyNested(fields.Nested):
    """An overloaded Nested field that can handle more than a single schema.
    By giving it a list of schemas, it will iterate through them to find one
    that matches with the input data. It raises an error if the data doesn't
    correspond to any schema.
    """

    def _deserialize(self, value, attr, data):
        try:
            return super(fields.Nested, self)._deserialize(value, attr, data)
        except ValueError:
            if isinstance(self.nested, list):
                for schema in self.nested:
                    if isinstance(schema, type):
                        schema = schema()
                    data, errors = schema.load(value)
                    if not errors:
                        return data
                self.fail("validator_failed")
            raise


class TriplePatternSchema(Schema):
    """Marshmallow schema for a triple pattern"""
    subject = fields.Str(required=True)
    predicate = fields.Str(required=True)
    object = fields.Str(require=True)


class FilterSchema(Schema):
    """Marshmallow schema for a SPARQL filter parameter"""
    type = fields.Str(required=True)
    operator = fields.Str(required=True)
    args = fields.List(ManyNested(fields.Str(), fields.Dict()), required=True)


class SparqlQuerySchema(Schema):
    """Marshmallow schema for a SPARQL query"""
    type = fields.Str(required=True)
    bgp = fields.Nested(TriplePatternSchema, required=True, many=True)
    optional = fields.Nested(TriplePatternSchema, required=False, many=True)
    filters = fields.Nested(FilterSchema, required=False, many=True)


class QueryRequest(Schema):
    """Marshmallow schema for a query to SaGe SPARQL API"""
    query = fields.Nested(SparqlQuerySchema, required=True)
    next = fields.Str(required=False, allow_none=True)
