# schemas.py
# Author: Thomas MINIER - MIT License 2017-2018
from marshmallow import Schema, fields, post_load, validates, validates_schema, ValidationError


class BGPQuery(Schema):
    """Validation schema for POST query with the BGP interface"""
    class Meta:
        fields = ("bgp", "controls")
    bgp = fields.Dict(keys=fields.Str(), values=fields.Dict(key=fields.Str(), values=fields.Str()), required=True)
    controls = fields.Dict()

    @post_load
    def set_default_controls(self, item):
        if 'controls' not in item:
            item['controls'] = {}
        return item

    @validates('bgp')
    def validate_bgp(self, bgp):
        for name, triple in bgp.items():
            if 'subject' not in triple or 'predicate' not in triple or 'object' not in triple:
                raise ValidationError('In a BGP, each triple pattern must have a subject, a predicate and an object.')

    # @validates('offsets')
    # def validate_offsets(self, offsets):
    #     for k, offset in offsets.items():
    #         if offset < 0:
    #             raise ValidationError('Only positive offsets ([0, n]) are accepted.')
