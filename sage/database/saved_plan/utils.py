# utils.py
# Author: Thomas MINIER - MIT License 2017-2020
from base64 import b64decode, b64encode

from sage.query_engine.protobuf.iterators_pb2 import RootTree


def encode_saved_plan(savedPlan: RootTree) -> str:
    """Encode a Protobuf-based saved plan into string format.

    Argument: A saved plan, encoded as a Protobuf message.

    Returns: The saved plan, encoded as a string of bytes.
    """
    if savedPlan is None:
        return None
    bytes = savedPlan.SerializeToString()
    return b64encode(bytes).decode('utf-8')


def decode_saved_plan(input: str) -> RootTree:
    """Decode a Protobuf-based saved plan from a string format.

    Argument: A saved plan, encoded as a string of bytes.

    Returns: The saved plan, encoded as a Protobuf message.
    """
    return b64decode(input) if input is not None else None
