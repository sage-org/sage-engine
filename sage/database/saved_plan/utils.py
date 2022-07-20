from base64 import b64decode, b64encode

from sage.query_engine.types import SavedPlan


def encode_saved_plan(saved_plan: SavedPlan) -> str:
    """
    Encodes a Protobuf-based saved plan into a string format.

    Parameters
    ----------
    saved_plan: SavedPlan
        A saved plan, encoded as a Protobuf message.

    Returns
    -------
    str
        The saved plan, encoded as a string of bytes.
    """
    return b64encode(saved_plan.SerializeToString()).decode('utf-8')


def decode_saved_plan(saved_plan: str) -> SavedPlan:
    """
    Decodes a Protobuf-based saved plan from a string format.

    Parameters
    ----------
    saved_plan: str
        A saved plan, encoded as a string of bytes.

    Returns
    -------
    SavedPlan
        The saved plan, encoded as a Protobuf message.
    """
    return b64decode(saved_plan)
