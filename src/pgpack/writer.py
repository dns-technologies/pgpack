from collections.abc import Iterable
from io import BufferedWriter
from struct import pack
from typing import Any
from zlib import (
    crc32,
    compress,
)

from light_compressor import (
    CompressionLevel,
    CompressionMethod,
    CompressorType,
)
from pandas import DataFrame as PdFrame
from polars import (
    DataFrame as PlFrame,
    LazyFrame as LfFrame,
)

from .common import (
    Error,
    Fmt,
    Signature,
    Size,
    PGParam,
    metadata_from_frame,
    metadata_reader,
    pgpack_repr,
)
from .pgcopylib import (
    PGCopyWriter,
    PGOid,
)


class PGPackWriter:
    """Class for write PGPack format."""

    fileobj: BufferedWriter | None
    metadata: bytes | None
    columns: list[str]
    pgtypes: list[PGOid]
    pgparam: list[PGParam]
    compressed_length: int
    data_length: int
    compression_method: CompressionMethod
    compression_level: int
    s3_file: bool
    pgcopy_start: int | None = None
    pgcopy: PGCopyWriter | None = None

    def __init__(
        self,
        fileobj: BufferedWriter | None = None,
        metadata: bytes | None = None,
        compression_method: CompressionMethod = CompressionMethod.ZSTD,
        compression_level: int = CompressionLevel.ZSTD_DEFAULT,
        s3_file: bool = False,
    ) -> None:
        """Class initialization."""

        self.fileobj = fileobj
        self.metadata = metadata
        self.compression_method = compression_method
        self.compression_level = compression_level
        self.s3_file = s3_file
        self.columns = []
        self.pgtypes = []
        self.pgparam = []
        self.compressed_length = Size.SEEK_SET
        self.data_length = -Size.SEEK_CUR

        if self.fileobj:
            self.pgcopy_start = self.fileobj.tell()

        if self.metadata:
            self.init_pgcopy(self.metadata)

    @property
    def dtypes(self) -> list[str]:
        """Get column data types."""

        return [pgtype.name for pgtype in self.pgtypes]

    def __validate_write_state(self) -> None:
        """Validate expected parameters."""

        if not self.fileobj:
            raise Error.PGPackNotDefineError("Fileobject not define.")
        if not self.fileobj.writable():
            raise Error.PGPackModeError("Fileobject don't support write.")
        if not self.metadata:
            raise Error.PGPackMetadataCrcError("Metadata error.")

    def __get_compressor(self) -> CompressorType | None:
        """Get current compressor."""

        if self.compression_method is CompressionMethod.NONE:
            return None
        elif isinstance(self.compression_method, CompressionMethod):
            return self.compression_method.compressor(self.compression_level)
        else:
            raise Error.PGPackTypeError(
                f"Unsupported compression method {self.compression_method}"
            )

    def __write_header(self) -> None:
        """Write PGPack header."""

        if not self.pgcopy:
            self.init_pgcopy(self.metadata)

        self.pgcopy_start = self.fileobj.tell()

        metadata_zlib = compress(self.metadata)
        metadata_crc = pack(Fmt.U_LONG, crc32(metadata_zlib))
        metadata_length = pack(Fmt.U_LONG, len(metadata_zlib))
        compression_method = pack(Fmt.U_CHAR, self.compression_method.value)

        for data in (
            Signature.HEADER,
            metadata_crc,
            metadata_length,
            metadata_zlib,
            compression_method,
            Signature.S3_FILE if self.s3_file else bytes(Size.S3_TAIL),
        ):
            self.pgcopy_start += self.fileobj.write(data)

    def __write_data(self, bytes_data: Iterable[bytes]) -> None:
        """Write PGCopy data."""

        compressor = self.__get_compressor()
        start_pos = self.fileobj.tell()

        if compressor:
            for chunk in compressor.send_chunks(bytes_data):
                self.fileobj.write(chunk)
            self.data_length = compressor.decompressed_size
        else:
            for chunk in bytes_data:
                self.fileobj.write(chunk)
            self.data_length = self.fileobj.tell() - start_pos

        self.compressed_length = self.fileobj.tell() - self.pgcopy_start

    def __write_trailer(self) -> None:
        """Write compress length and data length."""

        if not self.s3_file:
            self.fileobj.seek(self.pgcopy_start - Size.S3_TAIL)

        self.fileobj.write(
            pack(
                Fmt.COMPRESS_LENGTH,
                self.compressed_length,
                self.data_length,
            )
        )

    def init_pgcopy(
        self,
        metadata: bytes,
    ) -> None:
        """Initialize pgcopy from metadata."""

        self.metadata = metadata
        (
            self.columns,
            self.pgtypes,
            self.pgparam,
        ) = metadata_reader(self.metadata)
        self.pgcopy = PGCopyWriter(None, self.pgtypes)

    def from_rows(
        self,
        dtype_values: Iterable[Any],
    ) -> int:
        """Convert python rows to pgpack format."""

        if not self.metadata:
            raise Error.PGPackMetadataCrcError("Metadata error.")

        if not self.pgcopy:
            self.init_pgcopy(self.metadata)

        return self.from_bytes(self.pgcopy.from_rows(dtype_values))

    def from_pandas(
        self,
        data_frame: PdFrame,
    ) -> int:
        """Convert pandas.DataFrame to pgpack format."""

        if not self.metadata:
            self.metadata = metadata_from_frame(data_frame)

        return self.from_rows(data_frame.itertuples(index=False))

    def from_polars(
        self,
        data_frame: PlFrame | LfFrame,
    ) -> int:
        """Convert polars.DataFrame to pgpack format."""

        if data_frame.__class__ is LfFrame:
            data_frame = data_frame.collect(engine="streaming")

        if not self.metadata:
            self.metadata = metadata_from_frame(data_frame)

        return self.from_rows(data_frame.iter_rows())

    def from_bytes(
        self,
        bytes_data: Iterable[bytes],
    ) -> int:
        """Convert pgcopy bytes to pgpack format."""

        self.__validate_write_state()
        self.__write_header()
        self.__write_data(bytes_data)
        self.__write_trailer()
        self.fileobj.flush()
        return self.tell()

    def tell(self) -> int:
        """Return current position."""

        if self.pgcopy:
            return self.pgcopy.tell()

        return self.fileobj.tell()

    def close(self) -> None:
        """Close file object."""

        if self.fileobj:
            if hasattr(self.fileobj, "close"):
                self.fileobj.close()

    def __repr__(self) -> str:
        """String representation of PGPackWriter."""

        return pgpack_repr(
            self.columns,
            self.pgtypes,
            self.pgparam,
            self.s3_file,
            self.compressed_length,
            self.data_length,
            self.compression_method,
        )
