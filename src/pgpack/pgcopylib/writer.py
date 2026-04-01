from collections.abc import (
    Generator,
    Iterable,
)
from io import (
    BytesIO,
    BufferedWriter,
)
from types import FunctionType
from typing import Any

from .core.enums import (
    ArrayOidToOid,
    PGOid,
    PGOidToDType,
)
from .core.errors import PGCopyRecordError
from .core.dtype import PostgreSQLDtype
from .core.functions import (
    make_rows,
    nullable_writer,
    writer,
)
from .core.repr import pgcopylib_repr


class PGCopyWriter:
    """PGCopy dump writer."""

    fileobj: BufferedWriter
    pgtypes: list[PGOid]
    pgoid: list[int]
    pgoid_functions: list[FunctionType]
    postgres_dtype: list[PostgreSQLDtype]
    num_columns: int
    num_rows: int
    buffer_object: BytesIO
    pos: int

    def __init__(
        self,
        fileobj: BufferedWriter | None,
        pgtypes: list[PGOid],
    ) -> None:
        """Class initialization."""

        if not pgtypes:
            raise PGCopyRecordError("PGOids not defined!")

        self.fileobj = fileobj
        self.pgtypes = pgtypes
        self.num_columns = len(pgtypes)
        self.num_rows = 0
        self.pos = 0
        self.postgres_dtype: list[PostgreSQLDtype] = [
            PGOidToDType[pgtype]
            for pgtype in pgtypes
        ]
        self.pgoid_functions: list[FunctionType] = [
            PGOidToDType[ArrayOidToOid[self.pgtypes[column]]].write
            if self.pgtypes and ArrayOidToOid.get(
                self.pgtypes[column]
            ) else None
            for column in range(self.num_columns)
        ]
        self.pgoid: list[int] = [
            ArrayOidToOid[self.pgtypes[column]].value
            if self.pgtypes and ArrayOidToOid.get(
                self.pgtypes[column]
            ) else 0
            for column in range(self.num_columns)
        ]
        self.buffer_object = BytesIO()

    def write_row(
        self,
        dtype_values: list[Any] | tuple[Any],
    ) -> Generator[bytes, None, None]:
        """Write single row."""

        for postgres_dtype, dtype_value, pgoid_function, pgoid in zip(
            self.postgres_dtype,
            dtype_values,
            self.pgoid_functions,
            self.pgoid,
        ):
            yield nullable_writer(
                postgres_dtype.write,
                dtype_value,
                pgoid_function,
                self.buffer_object,
                pgoid,
            )
        self.num_rows += 1

    def from_rows(
        self,
        dtype_values: Iterable[list[Any] | tuple[Any]],
    ) -> Generator[bytes, None, None]:
        """Write all rows."""

        return make_rows(self.write_row, dtype_values, self.num_columns)

    def write(self, dtype_values: list[Any]) -> None:
        """Write all rows into file."""

        if self.fileobj is None:
            raise PGCopyRecordError("File not defined!")

        self.pos = writer(
            self.fileobj,
            self.write_row,
            dtype_values,
            self.num_columns,
        )

    def tell(self) -> int:
        """Return current position."""

        return self.pos

    def close(self) -> None:
        """Close file object."""

        if self.fileobj:
            if hasattr(self.fileobj, "close"):
                self.fileobj.close()

    def __repr__(self) -> str:
        """String representation of PGCopyWriter."""

        return pgcopylib_repr(
            self.pgtypes,
            self.num_columns,
            self.num_rows,
            "writer",
        )
