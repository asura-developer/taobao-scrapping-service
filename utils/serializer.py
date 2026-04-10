"""
Utility to make MongoDB documents fully JSON-serializable.
Handles: ObjectId, datetime, date, Decimal128, bytes, and any other BSON types.
"""
from datetime import datetime, date
from decimal import Decimal


def clean(obj):
    """
    Recursively convert any MongoDB / BSON value into a plain Python type
    that Python's standard json module can serialize.
    """
    # ── BSON / PyMongo types ──────────────────────────────────────────────
    try:
        from bson import ObjectId, Decimal128, Binary, Regex
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, Decimal128):
            return float(str(obj))
        if isinstance(obj, Binary):
            return obj.hex()
        if isinstance(obj, Regex):
            return obj.pattern
    except ImportError:
        pass

    # ── Standard Python types that json can't handle by default ──────────
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, bytes):
        return obj.hex()

    # ── Containers — recurse ──────────────────────────────────────────────
    if isinstance(obj, dict):
        return {k: clean(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [clean(i) for i in obj]

    # ── Primitive types json handles natively ─────────────────────────────
    return obj