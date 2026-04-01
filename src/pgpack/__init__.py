"""Library for read and write storage format for PGCopy dump
packed into LZ4, ZSTD or uncompressed
with meta data information packed into zlib."""

from light_compressor import (
    CompressionLevel,
    CompressionMethod,
)

from .common import (
    PGParam,
    metadata_from_frame,
    metadata_reader,
)
from .common.errors import (
    PGPackError,
    PGPackHeaderError,
    PGPackMetadataCrcError,
    PGPackModeError,
    PGPackNotDefineError,
    PGPackTypeError,
)
from .pgcopylib import (
    PGCopyReader,
    PGCopyWriter,
    PGCopyError,
    PGCopyRecordError,
    PGCopySignatureError,
    PGCopyTypeError,
    PGCopyValueError,
    PGOid,
)
from .reader import PGPackReader
from .writer import PGPackWriter


__all__ = (
    "CompressionLevel",
    "CompressionMethod",
    "PGCopyReader",
    "PGCopyWriter",
    "PGCopyError",
    "PGCopyRecordError",
    "PGCopySignatureError",
    "PGCopyTypeError",
    "PGCopyValueError",
    "PGOid",
    "PGPackError",
    "PGPackHeaderError",
    "PGPackMetadataCrcError",
    "PGPackModeError",
    "PGPackNotDefineError",
    "PGPackReader",
    "PGPackTypeError",
    "PGPackWriter",
    "PGParam",
    "metadata_from_frame",
    "metadata_reader",
)
__author__ = "0xMihalich"
__version__ = "0.3.3.dev4"
