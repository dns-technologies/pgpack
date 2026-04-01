from light_compressor import CompressionMethod

from ..pgcopylib.core.enums import PGOid
from ..pgcopylib.core.repr import table_repr
from .param import PGParam


def compile_pgtype(
    pgtype: PGOid,
    param: PGParam,
) -> str:
    """Generate Postgres data type representation."""

    pgtype_name = pgtype.name
    extended = ""

    if pgtype is PGOid.numeric:
        extended = f"({param.length}, {param.scale})"
    elif (pgtype is PGOid.bpchar) or (
        pgtype is PGOid.varchar and param.length > 0
    ):
        extended = f"({param.length})"
    elif "_" in pgtype_name:
        extended = f"[{param.nested}]"

    return f"{pgtype_name}{extended}"


def pgpack_repr(
    columns: list[str],
    pgtypes: list[PGOid],
    pgparam: list[PGParam],
    s3_file: bool,
    compressed_length: int,
    data_length: int,
    compression_method: CompressionMethod,
) -> str:
    """Generate string representation for PGPack."""

    dump_type = "s3file" if s3_file else "dump"
    dump_rate = (compressed_length / data_length) * 100
    return table_repr(
        columns,
        [
            compile_pgtype(pgtype, param)
            for pgtype, param in zip(pgtypes, pgparam)
        ],
        f"<PostgreSQL/GreenPlum compressed {dump_type}>",
        [
            f"Total Columns: {len(columns)}",
            f"Compression Method: {compression_method.name}",
            f"Unpacked Size: {data_length} bytes",
            f"Compressed Size: {compressed_length} bytes",
            f"Compression Rate: {round(dump_rate, 2)} %"
        ],
    )
