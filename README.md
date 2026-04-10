# PGPack

Storage format for PostgreSQL COPY binary dumps with compression support (GZIP, LZ4, SNAPPY, ZSTD) and metadata packed into zlib.

This package combines `pgcopylib` (PostgreSQL binary format reader/writer) and `pgpack` (compressed storage format) into a single unified library.

## Features

- **Read/Write PostgreSQL COPY binary format** with full type support
- **Compressed storage** with multiple compression algorithms
- **Metadata preservation** including column names and OID types
- **S3-compatible mode** with tail storage for streaming uploads
- **Pandas and Polars integration**
- **No external dependencies** for core functionality

## Installation

### From pip

```bash
pip install pgpack -U --index-url https://dns-technologies.github.io/dbhose-dev-pip/simple/
```

### From local directory

```bash
pip install . --extra-index-url https://dns-technologies.github.io/dbhose-dev-pip/simple/
```

### From git

```bash
pip install git+https://github.com/dns-technologies/pgpack --extra-index-url https://dns-technologies.github.io/dbhose-dev-pip/simple/
```

## Package Structure

The package provides two main components:

1. **`pgcopylib`** – Core PostgreSQL COPY binary format reader/writer
2. **`pgpack`** – Compressed storage format with metadata

## PGPack Format Structure

```text
- header "PGPACK\n\x00" (8 bytes)
- unsigned long zlib.crc32 for packed metadata (4 bytes)
- unsigned long zlib packed metadata length (4 bytes)
- zlib packed metadata
- unsigned char compression method (1 byte)
- unsigned long long packed pgcopy data length (8 bytes)
- unsigned long long unpacked pgcopy data length (8 bytes)
- packed pgcopy data
```

### S3 File Mode

When `s3_file=True`, the packed and unpacked lengths are written at the end of the file instead of the beginning, allowing streaming upload to S3 without knowing the final size upfront.

## Metadata Format

Metadata contains column information in the following structure:

```json
[
    {"column_name": {"oid": oid, "length": length, "scale": scale, "nested": nested}},
    {"another_column": {"oid": oid, "length": length, "scale": scale, "nested": nested}},
    ...
]
```

Where:
- `column_name` – Column name as string
- `oid` – PostgreSQL OID value (e.g., 23 for int4, 25 for text)
- `length` – Fixed length for fixed-size types, -1 for variable length
- `scale` – Scale for numeric/decimal types
- `nested` – Nesting level for arrays (0 for non-array, 1 for arrays)

## Compression Methods

| Method | Value | Description |
|--------|-------|-------------|
| NONE   | 0x02  | No compression |
| GZIP   | 0x99  | GZIP compression |
| LZ4    | 0x82  | LZ4 compression |
| SNAPPY | 0x9f  | Snappy compression |
| ZSTD   | 0x90  | Zstandard compression (default) |

```python
from pgpack import CompressionMethod

# Available compression methods
CompressionMethod.NONE    # no compression
CompressionMethod.GZIP    # gzip compression
CompressionMethod.LZ4     # lz4 compression
CompressionMethod.SNAPPY  # snappy compression
CompressionMethod.ZSTD    # zstd compression (default)
```

## PGPackReader Class

### Initialization

```python
from pgpack import PGPackReader

reader = PGPackReader(fileobj)
```

### Properties

| Property | Description |
|----------|-------------|
| `metadata` | Raw metadata bytes |
| `columns` | List of column names |
| `pgtypes` | List of PGOid values for each column |
| `pgparam` | List of PGParam (length, scale, nested) |
| `compressed_length` | Size of compressed data |
| `data_length` | Size of uncompressed data |
| `compression_method` | CompressionMethod used |
| `compression_stream` | BufferedReader for decompressed data |
| `s3_file` | Boolean indicating S3 mode |
| `_reader` | PGCopyReader instance |
| `_reader_pos` | Offset of compressed data start |

### Methods

```python
# Read as Python objects
rows = list(reader.to_rows())

# Convert to pandas DataFrame
df = reader.to_pandas()

# Convert to polars DataFrame/LazyFrame
df = reader.to_polars()
lf = reader.to_polars(is_lazy=True)

# Read as raw bytes chunks
for chunk in reader.to_bytes():
    process(chunk)

# Get current position
pos = reader.tell()

# Close the reader
reader.close()
```

## PGPackWriter Class

### Initialization

```python
from pgpack import PGPackWriter, CompressionMethod

writer = PGPackWriter(
    fileobj=buffer,
    metadata=metadata_bytes,           # optional, auto-detected if not provided
    compression_method=CompressionMethod.ZSTD,
    compression_level=3,
    s3_file=False,
)
```

### Methods

```python
# Write from Python rows
writer.from_rows(rows)

# Write from pandas DataFrame
writer.from_pandas(dataframe)

# Write from polars DataFrame/LazyFrame
writer.from_polars(dataframe)

# Write from bytes chunks
writer.from_bytes(bytes_generator)

# Get current position
pos = writer.tell()

# Close the writer
writer.close()
```

## PGCopyLib (Core PostgreSQL Binary Format)

The `pgcopylib` module provides low-level access to PostgreSQL COPY binary format.

### PGCopyReader

```python
from pgpack.pgcopylib import PGCopyReader, PGOid

# Read with known types
reader = PGCopyReader(fileobj, pgtypes=[PGOid.int4, PGOid.text])

# Read without types (returns bytes)
reader = PGCopyReader(fileobj)

# Read metadata without data
reader.read_info()

# Read rows
rows = list(reader.to_rows())
```

### PGCopyWriter

```python
from pgpack.pgcopylib import PGCopyWriter, PGOid

# Create writer
writer = PGCopyWriter(fileobj, pgtypes=[PGOid.int4, PGOid.text])

# Write rows
writer.write([(1, "Alice"), (2, "Bob")])

# Write as generator
for chunk in writer.from_rows(rows):
    fileobj.write(chunk)
```

## Supported PostgreSQL Data Types

### Base Types

| PostgreSQL Type | Python Type |
|-----------------|-------------|
| int2, int4, int8, serial2, serial4, serial8 | int |
| float4, float8, money | float |
| bool | bool |
| text, varchar, bpchar, char, xml | str |
| bytea | bytes |
| date | datetime.date |
| time, timetz | datetime.time |
| timestamp, timestamptz | datetime.datetime |
| interval | dateutil.relativedelta.relativedelta |
| uuid | uuid.UUID |
| numeric | decimal.Decimal |
| oid | int |
| inet | ipaddress.IPv4Address / IPv6Address |
| cidr | ipaddress.IPv4Network / IPv6Network |
| macaddr, macaddr8 | str |
| bit, varbit | str |
| json, jsonb | dict / list / str / int / float / bool / None |

### Geometric Types

| PostgreSQL Type | Python Type |
|-----------------|-------------|
| point | tuple[float, float] |
| line | tuple[float, float, float] |
| lseg | list[tuple[float, float]] |
| box | tuple[tuple[float, float], tuple[float, float]] |
| path | list[tuple[float, float]] (open) / tuple[tuple[float, float]] (closed) |
| polygon | tuple[tuple[float, float]] |
| circle | tuple[float, float, float] |

### Array Types

Array types (prefixed with `_`) are supported for all base types and return Python lists.

## Examples

### Write and Read PGPack File

```python
import io
from pgpack import PGPackWriter, PGPackReader, CompressionMethod

# Write data
buffer = io.BytesIO()
writer = PGPackWriter(fileobj=buffer, compression_method=CompressionMethod.ZSTD)

# Write from pandas
import pandas as pd
df = pd.DataFrame({'id': [1, 2], 'name': ['Alice', 'Bob']})
writer.from_pandas(df)

# Read data
buffer.seek(0)
reader = PGPackReader(buffer)
df_result = reader.to_pandas()
print(df_result)
```

### Transfer Between PostgreSQL Databases

```python
from pgpack.pgcopylib import PGCopyWriter, PGCopyReader

# Read from source
with open('dump.pgcopy', 'rb') as f:
    reader = PGCopyReader(f)
    
    # Write to destination
    with open('output.pgcopy', 'wb') as out:
        writer = PGCopyWriter(reader.pgtypes, out)
        writer.write(reader.to_rows())
```

### S3 Mode for Streaming Upload

```python
# Write in S3 mode (lengths at end)
writer = PGPackWriter(
    fileobj=buffer,
    compression_method=CompressionMethod.ZSTD,
    s3_file=True,
)
writer.from_pandas(df)

# After upload, the lengths can be read from the end
buffer.seek(-16, io.SEEK_END)
compressed_len, data_len = struct.unpack('!2Q', buffer.read(16))
```

## Errors

| Exception | Description |
|-----------|-------------|
| `PGPackError` | Base PGPack error |
| `PGPackHeaderError` | Invalid header signature |
| `PGPackMetadataCrcError` | Metadata CRC32 mismatch |
| `PGPackModeError` | File object mode error |
| `PGPackTypeError` | Type mismatch error |
| `PGPackValueError` | Value error |
| `PGCopyError` | Base PGCopy error |
| `PGCopyRecordError` | Record length error |
| `PGCopySignatureError` | Signature not match |
| `PGCopyTypeError` | PGCopy type error |
| `PGCopyValueError` | PGCopy value error |

## License

MIT
