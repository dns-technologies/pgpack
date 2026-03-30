from light_compressor import CompressionMethod
from pgcopylib import PGOid

from .param import PGParam


EMPTY_LINE = "├─────────────────┼─────────────────┤"
END_LINE = "└─────────────────┴─────────────────┘"
HEADER_LINES = [
    "┌─────────────────┬─────────────────┐",
    "│   Column Name   │    Data Type    │",
    "╞═════════════════╪═════════════════╡",
]


def to_col(text: str) -> str:
    """Format string element."""

    return text[:14] + "…" if len(text) > 15 else text


def table_repr(
    columns: list[str],
    dtypes: list[str],
    header: str | None = None,
    tail: list[str] | None = None,
) -> str:
    """Generate table for string representation."""

    table = [
        header,
        *HEADER_LINES,
    ] if header else HEADER_LINES

    for column, dtype in zip(columns, dtypes):
        table.extend([
            f"│ {to_col(column): <15} │ {to_col(dtype): >15} │",
            EMPTY_LINE,
        ])

    table[-1] = END_LINE

    if tail:
        table.extend(tail)

    return "\n".join(table)


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
