# schemas.py
# Author: Thomas MINIER - MIT License 2017-2018
from marshmallow import Schema, fields, post_load, validates, validates_schema, ValidationError


class BGPQuery(Schema):
    """Validation schema for POST query with the BGP interface"""
    class Meta:
        fields = ("bgp", "next")
    bgp = fields.List(fields.Dict(key=fields.Str(), values=fields.Str()), required=True)
    next = fields.String()

    @post_load
    def set_default_next(self, item):
        if 'next' not in item:
            item['next'] = None
        return item

    @validates('bgp')
    def validate_bgp(self, bgp):
        for triple in bgp:
            if 'subject' not in triple or 'predicate' not in triple or 'object' not in triple:
                raise ValidationError('In a BGP, each triple pattern must have a subject, a predicate and an object.')

    # @validates('offsets')
    # def validate_offsets(self, offsets):
    #     for k, offset in offsets.items():
    #         if offset < 0:
    #             raise ValidationError('Only positive offsets ([0, n]) are accepted.')
