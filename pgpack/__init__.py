"""Library for read and write storage format for PGCopy dump
packed into LZ4, ZSTD or uncompressed
with meta data information packed into zlib."""

from light_compressor import (
    CompressionLevel,
    CompressionMethod,
)

from .common import (
    metadata_from_frame,
    metadata_reader,
    PGPackError,
    PGPackHeaderError,
    PGPackMetadataCrcError,
    PGPackModeError,
)
from .reader import PGPackReader
from .writer import PGPackWriter


__all__ = (
    "CompressionLevel",
    "CompressionMethod",
    "PGPackError",
    "PGPackHeaderError",
    "PGPackMetadataCrcError",
    "PGPackModeError",
    "PGPackReader",
    "PGPackWriter",
    "metadata_from_frame",
    "metadata_reader",
)
__author__ = "0xMihalich"
__version__ = "0.3.3.dev0"
