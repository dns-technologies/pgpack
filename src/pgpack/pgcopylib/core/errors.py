class PGCopyError(Exception):
    """Base PGCopy error."""


class PGCopyValueError(PGCopyError, ValueError):
    """PGCopy value error."""


class PGCopyTypeError(PGCopyError, TypeError):
    """PGCopy type error."""


class PGCopySignatureError(PGCopyValueError):
    """Signature not match."""


class PGCopyRecordError(PGCopyValueError):
    """Record length error."""
