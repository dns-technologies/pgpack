from collections.abc import Generator
from io import BufferedReader
from struct import unpack
from typing import Any
from zlib import (
    crc32,
    decompress,
)

from light_compressor import (
    CompressionMethod,
    LimitedReader,
    define_reader,
)
from pandas import DataFrame as PdFrame
from polars import (
    DataFrame as PlFrame,
    LazyFrame as LfFrame,
    Object,
)

from .common import (
    Error,
    Fmt,
    Signature,
    Size,
    PGParam,
    metadata_reader,
    pandas_astype,
    pgpack_repr,
)
from .pgcopylib import (
    PGCopyReader,
    PGOid,
)


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
    compressed_length: int
    data_length: int
    compression_method: CompressionMethod
    compression_stream: BufferedReader
    s3_file: bool
    pgcopy_start: int
    pgcopy: PGCopyReader | None
    schema_overrides: dict[str, Object]

    def __init__(
        self,
        fileobj: BufferedReader,
    ) -> None:
        """Class initialization."""

        self.fileobj = fileobj
        header = self.fileobj.read(Size.HEADER_LENS)

        if header != Signature.HEADER:
            raise Error.PGPackHeaderError()

        metadata_crc, metadata_length = unpack(
            Fmt.METADATA_CRC_LENGTH,
            self.fileobj.read(Size.METADATA_PROMPT),
        )
        metadata_zlib = self.fileobj.read(metadata_length)

        if crc32(metadata_zlib) != metadata_crc:
            raise Error.PGPackMetadataCrcError()

        self.metadata = decompress(metadata_zlib)
        (
            self.columns,
            self.pgtypes,
            self.pgparam,
        ) = metadata_reader(self.metadata)
        (
            compression_method,
            self.compressed_length,
            self.data_length,
        ) = unpack(
            Fmt.COMPRESS_METHOD_LENGTH,
            self.fileobj.read(Size.PGDATA_PROMPT),
        )
        self.compression_method = CompressionMethod(compression_method)
        self.compression_stream = define_reader(
            self.fileobj,
            self.compression_method,
        )
        self.s3_file = (
            self.compressed_length,
            self.data_length,
        ) == Signature.S3_FILE_INTEGERS
        self.pgcopy_start = self.fileobj.tell()

        if self.s3_file:
            self.fileobj.seek(-Size.S3_TAIL, Size.SEEK_END)
            limit = self.fileobj.tell()
            (
                self.compressed_length,
                self.data_length,
            ) = unpack(
                Fmt.COMPRESS_LENGTH,
                self.fileobj.read(Size.S3_TAIL)
            )
            self.fileobj.seek(self.pgcopy_start)
            self.fileobj = LimitedReader(self.fileobj, limit)

        try:
            self.pgcopy = PGCopyReader(
                self.compression_stream,
                self.pgtypes,
            )
        except IndexError:
            self.pgcopy = None

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

    @property
    def dtypes(self) -> list[str]:
        """Get column data types."""

        return [pgtype.name for pgtype in self.pgtypes]

    def read_info(self) -> None:
        """Read info without reading data."""

        if self.pgcopy:
            self.pgcopy.read_info()

    def to_rows(self) -> Generator[list[Any], None, None]:
        """Convert to python objects."""

        if not self.pgcopy:
            return []

        return self.pgcopy.to_rows()

    def to_pandas(self) -> PdFrame:
        """Convert to pandas.DataFrame."""

        return PdFrame(
            data=self.to_rows(),
            columns=self.columns,
        ).astype(pandas_astype(
            self.columns,
            self.pgcopy.postgres_dtype if self.pgcopy else [],
        ))

    def to_polars(self, is_lazy: bool = False) -> PlFrame | LfFrame:
        """Convert to polars.DataFrame."""

        return ISLAZY[is_lazy](
            data=self.to_rows(),
            schema=self.columns,
            schema_overrides=self.schema_overrides,
            infer_schema_length=None,
        )

    def to_bytes(self) -> Generator[bytes, None, None]:
        """Get raw unpacked pgcopy data."""

        if self.compression_method is CompressionMethod.NONE:
            self.compression_stream.seek(self.pgcopy_start)
        else:
            self.compression_stream.seek(Size.SEEK_SET)

        while chunk := self.compression_stream.read(Size.CHUNK_SIZE):
            yield chunk

    def tell(self) -> int:
        """Return current position."""

        if not self.pgcopy:
            return self.compression_stream.tell()

        return self.pgcopy.tell()

    def close(self) -> None:
        """Close file object."""

        if hasattr(self.fileobj, "close"):
            self.fileobj.close()

    def __repr__(self) -> str:
        """String representation of PGPackReader."""

        if not self._str:
            self._str = pgpack_repr(
                self.columns,
                self.pgtypes,
                self.pgparam,
                self.s3_file,
                self.compressed_length,
                self.data_length,
                self.compression_method,
            )

        return self._str
