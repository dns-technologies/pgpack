from .errors import PGCopyValueError
from .enums import PGOid


def __pgtypes_to_metadata(
    pgtypes: list[PGOid],
) -> list[dict[str, dict[str, int]]]:
    """Generate metadata from list[PGOid]."""

    return [
        {f"col_{num}": {"oid": pgoid.value}}
        for num, pgoid in enumerate(pgtypes)
    ]


def __metadata_to_pgtypes(
    metadata: list[dict[str, dict[str, int]]],
) -> list[PGOid]:
    """Generate list[PGOid] from metadata."""

    return [
        PGOid(pgtype["oid"])
        for column in metadata
        for _, pgtype in column.items()
    ]


def init_metadata(
    init_parameter: list[dict[str, dict[str, int]]] | list[PGOid],
) -> tuple[list[dict[str, dict[str, int]]], list[PGOid]]:
    """Make metadata and pgtypes from only one parameter."""

    if not init_parameter:
        return [], []

    if isinstance(init_parameter[0], PGOid):
        return __pgtypes_to_metadata(init_parameter), init_parameter
    elif isinstance(init_parameter[0], dict):
        return init_parameter, __metadata_to_pgtypes(init_parameter)

    raise PGCopyValueError(
        f"Unsupported metadata type {init_parameter.__class__}",
    )
