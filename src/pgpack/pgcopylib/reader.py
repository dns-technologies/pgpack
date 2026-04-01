from collections.abc import Generator
from io import (
    BufferedReader,
    BytesIO,
)
from types import FunctionType
from typing import Any
from struct import (
    error as UnpackError,
    unpack,
)

from .core.dtype import PostgreSQLDtype
from .core.enums import (
    ArrayOidToOid,
    PGOid,
    PGOidToDType,
)
from .core.errors import PGCopySignatureError
from .core.functions import (
    read_num_columns,
    read_record,
    skip_all,
)
from .core.header import HEADER
from .core.repr import pgcopylib_repr


class PGCopyReader:
    """PGCopy dump reader."""

    fileobj: BufferedReader
    pgtypes: list[PGOid]
    pgoid: list[int]
    pgoid_functions: list[FunctionType]
    postgres_dtype: list[PostgreSQLDtype]
    header: bytes
    flags_area: list[int]
    is_oid_enable: bool
    is_empty: bool
    column_length: int
    header_ext_length: int
    num_columns: int
    num_rows: int
    buffer_object: BytesIO

    def __init__(
        self,
        fileobj: BufferedReader,
        pgtypes: list[PGOid] | None = None,
    ) -> None:
        """Class initialization."""

        if not pgtypes:
            pgtypes = []

        self.fileobj = fileobj
        self.pgtypes = pgtypes
        self.header = self.fileobj.read(11)

        if self.header != HEADER:
            raise PGCopySignatureError("PGCopy signature not match!")

        self.flags_area = [
            (byte >> i) & 1
            for byte in self.fileobj.read(4)
            for i in range(7, -1, -1)
        ]
        self.is_oid_enable = bool(self.flags_area[16])

        if self.is_oid_enable:
            self.column_length = 6
        else:
            self.column_length = 2

        try:
            self.header_ext_length = unpack(
                "!i",
                self.fileobj.read(4),
            )[0]
            self.num_columns = read_num_columns(
                self.fileobj,
                self.column_length,
            )
            self.is_empty = False
        except UnpackError:
            self.header_ext_length = 0
            self.num_columns = len(self.pgtypes)
            self.is_empty = True

        self.num_rows = 0
        self.postgres_dtype = [
            PGOidToDType[self.pgtypes[column]]
            if self.pgtypes else PostgreSQLDtype.Bytes
            for column in range(self.num_columns)
        ]
        self.pgoid_functions = [
            PGOidToDType[ArrayOidToOid[self.pgtypes[column]]].read
            if self.pgtypes and ArrayOidToOid.get(
                self.pgtypes[column]
            ) else None
            for column in range(self.num_columns)
        ]
        self.pgoid = [
            ArrayOidToOid[self.pgtypes[column]].value
            if self.pgtypes and ArrayOidToOid.get(
                self.pgtypes[column]
            ) else 0
            for column in range(self.num_columns)
        ]
        self.buffer_object = BytesIO()

    def read_info(self) -> None:
        """Read info without reading data."""

        if not self.num_rows:
            self.num_rows = skip_all(
                self.fileobj,
                self.column_length,
                self.num_columns,
                self.num_rows,
            )

    def read_row(self) -> Generator[Any, None, None]:
        """Read single row."""

        if self.is_empty:
            return

        for postgres_dtype, pgoid_function, pgoid in zip(
            self.postgres_dtype,
            self.pgoid_functions,
            self.pgoid,
        ):
            yield read_record(
                self.fileobj,
                postgres_dtype.read,
                pgoid_function,
                self.buffer_object,
                pgoid,
            )

    def to_rows(self) -> Generator[list[Any], None, None]:
        """Read all rows."""

        columns = self.num_columns

        while columns != 0xffff and not self.is_empty:
            yield [*self.read_row()]
            self.num_rows += 1
            columns = read_num_columns(
                self.fileobj,
                self.column_length,
            )

    def tell(self) -> int:
        """Return current position."""

        return self.fileobj.tell()

    def close(self) -> None:
        """Close file object."""

        if hasattr(self.fileobj, "close"):
            self.fileobj.close()

    def __repr__(self) -> str:
        """String representation of PGCopyReader."""

        return pgcopylib_repr(
            self.pgtypes or [PGOid.bytea for _ in range(self.num_columns)],
            self.num_columns,
            self.num_rows,
            "reader",
        )
