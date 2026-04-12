from json import (
    dumps,
    loads,
)
from typing import NamedTuple

from pandas import DataFrame as PdFrame
from polars import DataFrame as PlFrame

from ..pgcopylib.core.enums import (
    PGOid,
    PGOidToDType,
)
from .casts import pandas_astype
from .detector import detect_oid
from .param import PGParam


class PGPackMeta(NamedTuple):
    """Metadata for PGPack."""

    columns: list[str]
    pgtypes: list[PGOid]
    pgparams: list[PGParam]
    pgcopy_metadata: list[dict[str, dict[str, int]]]

    @property
    def pandas_astype(self) -> dict[str, str]:
        """Make pandas dtypes from columns."""

        return pandas_astype(
            self.columns,
            [PGOidToDType[oid] for oid in self.pgtypes],
        )

    @classmethod
    def from_params(
        cls,
        columns: list[str],
        pgtypes: list[PGOid],
        pgparams: list[PGParam],
    ) -> "PGPackMeta":
        """Make object from params."""

        pgcopy_metadata = [
            {column: {
                "oid": pgoid.value,
                "length": param.length,
                "scale": param.scale,
                "nested": param.nested,
            }}
            for column, pgoid, param in zip(columns, pgtypes, pgparams)
        ]
        return cls(columns, pgtypes, pgparams, pgcopy_metadata)

    @classmethod
    def from_bytes(cls, metadata: bytes) -> "PGPackMeta":
        """Make object from bytes."""

        pgcopy_metadata: list[dict[str, dict[str, int]]] = loads(metadata)
        columns = [
            column_name
            for column in pgcopy_metadata
            for column_name, _ in column.items()
        ]
        pgtypes = [
            PGOid(items["oid"])
            for column in pgcopy_metadata
            for _, items in column.items()
        ]
        pgparams = [
            PGParam(items["length"], items["scale"], items["nested"])
            for column in pgcopy_metadata
            for _, items in column.items()
        ]
        return cls(columns, pgtypes, pgparams, pgcopy_metadata)

    def to_bytes(self) -> bytes:
        """Convert object to bytes."""

        return dumps(self.pgcopy_metadata, ensure_ascii=False).encode("utf-8")

    def __bytes__(self) -> bytes:
        """Bytes representation of CSVPackMeta."""

        return self.to_bytes()

    def __repr__(self) -> str:
        """String representation of CSVPackMeta."""

        return f"""\
Column Names: [{", ".join(self.columns)}]
Column Types: [{", ".join(pgtype.name for pgtype in self.pgtypes)}]
Total Columns: {len(self.columns)}\
"""


def metadata_from_frame(frame: PdFrame | PlFrame) -> bytes:
    """Generate metadata from pandas.DataFrame | polars.DataFrame."""

    pgcopy_metadata = list(map(
        lambda column: {str(column): detect_oid(frame[column])},
        frame.columns,
    ))
    return dumps(pgcopy_metadata, ensure_ascii=False).encode("utf-8")
