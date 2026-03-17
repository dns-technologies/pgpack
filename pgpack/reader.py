from collections.abc import Generator
from io import BufferedReader
from struct import unpack
from typing import (
    Any,
    Optional,
)
from zlib import (
    crc32,
    decompress,
)

from light_compressor import (
    CompressionMethod,
    define_reader,
)
from pandas import DataFrame as PdFrame
from pgcopylib import (
    PGCopyReader,
    PGOid,
)
from polars import (
    DataFrame as PlFrame,
    LazyFrame as LfFrame,
    Object,
)

from .common import (
    HEADER,
    metadata_reader,
    pandas_astype,
    PGPackHeaderError,
    PGPackMetadataCrcError,
    PGParam,
)


S3_SIGNATURE = 0x70677061636b5f73, 0x335f6f626a656374
ISLAZY = {
    False: PlFrame,
    True: LfFrame,
}


class PGPackReader:
    """Class for read PGPack format."""

    fileobj: BufferedReader
    metadata: bytes
    columns: list[str]
    pgtypes: list[PGOid]
    pgparam: list[PGParam]
    pgcopy_compressed_length: int
    pgcopy_data_length: int
    compression_method: CompressionMethod
    compression_stream: BufferedReader
    s3_file: bool
    pgcopy_start: int
    pgcopy: PGCopyReader
    schema_overrides: dict[str, Object]
    _str: Optional[str]

    def __init__(
        self,
        fileobj: BufferedReader,
    ) -> None:
        """Class initialization."""

        self.fileobj = fileobj

        header = self.fileobj.read(8)

        if header != HEADER:
            raise PGPackHeaderError()

        metadata_crc, metadata_length = unpack(
            "!2L",
            self.fileobj.read(8),
        )
        metadata_zlib = self.fileobj.read(metadata_length)

        if crc32(metadata_zlib) != metadata_crc:
            raise PGPackMetadataCrcError()

        self.metadata = decompress(metadata_zlib)
        (
            self.columns,
            self.pgtypes,
            self.pgparam,
        ) = metadata_reader(self.metadata)
        (
            compression_method,
            self.pgcopy_compressed_length,
            self.pgcopy_data_length,
        ) = unpack(
            "!B2Q",
            self.fileobj.read(17),
        )

        self.compression_method = CompressionMethod(compression_method)
        self.compression_stream = define_reader(
            self.fileobj,
            self.compression_method,
        )
        self.s3_file = (
            self.pgcopy_compressed_length,
            self.pgcopy_data_length,
        ) == S3_SIGNATURE
        self.pgcopy_start = self.fileobj.tell()

        if self.s3_file:
            if self.fileobj.seekable():
                self.fileobj.seek(-16, 2)
                (
                    self.pgcopy_compressed_length,
                    self.pgcopy_data_length,
                ) = unpack("!2Q", self.fileobj.read(16))
                self.fileobj.seek(self.pgcopy_start)
            else:
                self.pgcopy_compressed_length = 0
                self.pgcopy_data_length = -1

        self.pgcopy = PGCopyReader(
            self.compression_stream,
            self.pgtypes,
        )
        self.schema_overrides = {
            column: Object
            for column, pgtype in zip(self.columns, self.pgtypes)
            if pgtype in (
                PGOid._uuid,
                PGOid._json,
                PGOid._jsonb,
                PGOid._inet,
                PGOid._cidr,
                PGOid._tsquery,
                PGOid._tsvector,
            )
        }
        self._str = None

    def __repr__(self) -> str:
        """String representation in interpreter."""

        return self.__str__()

    def __str__(self) -> str:
        """String representation of PGPackReader."""

        def to_col(text: str) -> str:
            """Format string element."""

            text = text[:14] + "…" if len(text) > 15 else text
            return f" {text: <15} "

        if not self._str:
            dump_type = "s3file" if self.s3_file else "dump"
            empty_line = (
                "├─────────────────┼─────────────────┤"
            )
            end_line = (
                "└─────────────────┴─────────────────┘"
            )
            _str = [
                f"<PostgreSQL/GreenPlum compressed {dump_type}>",
                "┌─────────────────┬─────────────────┐",
                "│ Column Name     │ PostgreSQL Type │",
                "╞═════════════════╪═════════════════╡",
            ]

            for column, pgtype in zip(self.columns, self.pgtypes):
                _str.append(
                    f"│{to_col(column)}│{to_col(pgtype.name)}│",
                )
                _str.append(empty_line)

            _str[-1] = end_line
            self._str = "\n".join(_str) + f"""
Total columns: {len(self.columns)}
Compression method: {self.compression_method.name}
Unpacked size: {self.pgcopy_data_length} bytes
Compressed size: {self.pgcopy_compressed_length} bytes
Compression rate: {round(
    (self.pgcopy_compressed_length / self.pgcopy_data_length) * 100, 2
)} %
"""
        return self._str

    def to_rows(self) -> Generator[list[Any], None, None]:
        """Convert to python objects."""

        return self.pgcopy.to_rows()

    def to_pandas(self) -> PdFrame:
        """Convert to pandas.DataFrame."""

        return PdFrame(
            data=self.pgcopy.to_rows(),
            columns=self.columns,
        ).astype(pandas_astype(
            self.columns,
            self.pgcopy.postgres_dtype,
        ))

    def to_polars(self, is_lazy: bool = False) -> PlFrame | LfFrame:
        """Convert to polars.DataFrame."""

        return ISLAZY[is_lazy](
            data=self.pgcopy.to_rows(),
            schema=self.columns,
            schema_overrides=self.schema_overrides,
            infer_schema_length=None,
        )

    def to_bytes(self) -> Generator[bytes, None, None]:
        """Get raw unpacked pgcopy data."""

        if self.compression_method is CompressionMethod.NONE:
            self.fileobj.seek(self.pgcopy_start)
        else:
            self.compression_stream.seek(0)

        chunk_size = 65536
        read_size = 0

        while 1:
            chunk = self.compression_stream.read(chunk_size)
            read_size += len(chunk)

            if not chunk:
                break

            yield chunk

    def tell(self) -> int:
        """Return current position."""

        return self.pgcopy.tell()

    def close(self) -> None:
        """Close file object."""

        self.pgcopy.close()
        self.fileobj.close()
