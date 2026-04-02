import io
import json
import datetime
import uuid
import ipaddress
import decimal

from dateutil.relativedelta import relativedelta

import pandas as pd
import polars as pl
import pytest

from pgpack import (
    CompressionMethod,
    PGPackReader,
    PGPackWriter,
    PGOid,
)


@pytest.fixture
def sample_metadata():
    """Create sample PGPack metadata as bytes."""
    metadata = [
        [1, ["id", PGOid.int4.value, 4, 0, 0]],
        [2, ["name", PGOid.text.value, -1, 0, 0]],
        [3, ["age", PGOid.int4.value, 4, 0, 0]],
        [4, ["active", PGOid.bool.value, 1, 0, 0]],
        [5, ["salary", PGOid.float8.value, 8, 0, 0]],
        [6, ["created_date", PGOid.date.value, 4, 0, 0]],
        [7, ["created_datetime", PGOid.timestamp.value, 8, 0, 0]],
        [8, ["tags", PGOid._text.value, -1, 0, 0]],
    ]
    return json.dumps(metadata).encode("utf-8")


@pytest.fixture
def sample_metadata_full():
    """Create full PGPack metadata for 17-column dataset."""
    metadata = [
        [1, ["start_month", PGOid.date.value, 4, 0, 0]],
        [2, ["start_day", PGOid.date.value, 4, 0, 0]],
        [3, ["division_name", PGOid.text.value, -1, 0, 0]],
        [4, ["rdc_name", PGOid.text.value, -1, 0, 0]],
        [5, ["branch_name", PGOid.text.value, -1, 0, 0]],
        [6, ["branch_guid", PGOid.uuid.value, 16, 0, 0]],
        [7, ["category_guid", PGOid.uuid.value, 16, 0, 0]],
        [8, ["category_name", PGOid.text.value, -1, 0, 0]],
        [9, ["bonus_type", PGOid.text.value, -1, 0, 0]],
        [10, ["category_rn", PGOid.int4.value, 4, 0, 0]],
        [11, ["tso_metric1_rn", PGOid.int4.value, 4, 0, 0]],
        [12, ["tso_metric2_rn", PGOid.int4.value, 4, 0, 0]],
        [13, ["employee_total_rn", PGOid.int4.value, 4, 0, 0]],
        [14, ["category_pcs", PGOid.int4.value, 4, 0, 0]],
        [15, ["employee_tso_metric1", PGOid.int4.value, 4, 0, 0]],
        [16, ["employee_tso_metric2", PGOid.int4.value, 4, 0, 0]],
        [17, ["employee_tso_metric3", PGOid.int4.value, 4, 0, 0]],
    ]
    return json.dumps(metadata).encode("utf-8")


@pytest.fixture
def sample_rows():
    """Create sample Python rows."""
    return [
        (
            1,
            "Alice",
            25,
            True,
            50000.5,
            datetime.date(2024, 1, 1),
            datetime.datetime(2024, 1, 1, 10, 0, 0),
            ["python", "data"],
        ),
        (
            2,
            "Bob",
            30,
            False,
            60000.0,
            datetime.date(2024, 1, 2),
            datetime.datetime(2024, 1, 2, 10, 0, 0),
            ["rust", "csv"],
        ),
        (
            3,
            "Charlie",
            35,
            True,
            75000.75,
            datetime.date(2024, 1, 3),
            datetime.datetime(2024, 1, 3, 10, 0, 0),
            ["pandas"],
        ),
    ]


@pytest.fixture
def sample_dataframe():
    """Create sample pandas DataFrame."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "age": [25, 30, 35, 28, 32],
            "active": [True, False, True, False, True],
            "salary": [50000.5, 60000.0, 75000.75, 48000.25, 82000.0],
            "created_date": pd.date_range("2024-01-01", periods=5),
            "created_datetime": pd.date_range(
                "2024-01-01 10:00:00", periods=5
            ),
            "tags": [
                ["python", "data"],
                ["rust", "csv"],
                ["pandas"],
                [],
                ["numpy", "polars"],
            ],
        }
    )


@pytest.fixture
def sample_all_types_row():
    """Row with all supported PostgreSQL types."""
    return (
        # Порядок должен соответствовать sample_metadata_full
        datetime.date(2026, 3, 1),  # start_month
        datetime.date(2026, 3, 24),  # start_day
        "Дивизион 4",  # division_name
        "РРС Центр",  # rdc_name
        "ООО Рога и копыта",  # branch_name
        uuid.UUID("36929e13-2a94-4810-ba49-3e41466c899f"),  # branch_guid
        uuid.UUID("f6924c17-8c62-41e3-a10f-00155d031652"),  # category_guid
        "Новая Лада",  # category_name
        "Можно, а зачем?",  # bonus_type
        1,  # category_rn
        2,  # tso_metric1_rn
        3,  # tso_metric2_rn
        4,  # employee_total_rn
        5,  # category_pcs
        6,  # employee_tso_metric1
        7,  # employee_tso_metric2
        8,  # employee_tso_metric3
    )


class TestPGPack:
    """Tests for PGPackReader and PGPackWriter."""

    def test_write_and_read_pandas(self, sample_dataframe: pd.DataFrame):
        """Test writing and reading pandas DataFrame."""

        buffer = io.BytesIO()
        writer = PGPackWriter(fileobj=buffer)
        writer.from_pandas(sample_dataframe)
        buffer.seek(0)
        reader = PGPackReader(buffer)
        assert reader.columns == list(sample_dataframe.columns)  # noqa: S101
        assert len(reader.dtypes) == len(sample_dataframe.columns)  # noqa: S101
        df_result = reader.to_pandas()
        assert len(df_result) == len(sample_dataframe)  # noqa: S101
        assert list(df_result.columns) == list(sample_dataframe.columns)  # noqa: S101
        reader.close()

    def test_write_and_read_polars(self, sample_dataframe: pd.DataFrame):
        """Test writing and reading polars DataFrame."""

        buffer = io.BytesIO()
        pl_df = pl.from_pandas(sample_dataframe)
        writer = PGPackWriter(fileobj=buffer)
        writer.from_polars(pl_df)
        buffer.seek(0)
        reader = PGPackReader(buffer)
        df_result = reader.to_polars()
        assert len(df_result) == len(pl_df)  # noqa: S101
        assert df_result.columns == pl_df.columns  # noqa: S101
        reader.close()

    def test_write_and_read_polars_lazy(self, sample_dataframe: pd.DataFrame):
        """Test writing and reading lazy polars DataFrame."""

        buffer = io.BytesIO()
        pl_df = pl.from_pandas(sample_dataframe)
        writer = PGPackWriter(fileobj=buffer)
        writer.from_polars(pl_df.lazy())
        buffer.seek(0)
        reader = PGPackReader(buffer)
        df_result = reader.to_polars(is_lazy=True)
        assert isinstance(df_result, pl.LazyFrame)  # noqa: S101
        collected = df_result.collect()
        assert len(collected) == len(sample_dataframe)  # noqa: S101
        reader.close()

    def test_write_and_read_rows(self, sample_rows, sample_metadata):
        """Test writing and reading Python rows."""

        buffer = io.BytesIO()
        writer = PGPackWriter(fileobj=buffer, metadata=sample_metadata)
        writer.from_rows(sample_rows)
        buffer.seek(0)
        reader = PGPackReader(buffer)
        rows = list(reader.to_rows())
        assert len(rows) == len(sample_rows)  # noqa: S101

        for original, read in zip(sample_rows, rows):
            assert original[0] == read[0]  # id  # noqa: S101
            assert original[1] == read[1]  # name  # noqa: S101
            assert original[2] == read[2]  # age  # noqa: S101
            assert original[3] == read[3]  # active  # noqa: S101
            assert original[4] == read[4]  # salary  # noqa: S101
            assert original[5] == read[5]  # created_date  # noqa: S101
            assert original[6] == read[6]  # created_datetime  # noqa: S101
            assert original[7] == read[7]  # tags  # noqa: S101

        reader.close()

    def test_all_compression_methods(self, sample_dataframe):
        """Test all compression methods."""

        methods = [
            CompressionMethod.NONE,
            CompressionMethod.GZIP,
            CompressionMethod.LZ4,
            CompressionMethod.SNAPPY,
            CompressionMethod.ZSTD,
        ]

        for method in methods:
            buffer = io.BytesIO()
            writer = PGPackWriter(
                fileobj=buffer, compression_method=method, compression_level=3
            )
            writer.from_pandas(sample_dataframe)
            assert buffer.tell() > 0  # noqa: S101
            buffer.seek(0)
            reader = PGPackReader(buffer)
            df_result = reader.to_pandas()
            assert len(df_result) == len(sample_dataframe)  # noqa: S101
            assert reader.compression_method == method  # noqa: S101
            reader.close()

    def test_s3_file_mode(self, sample_dataframe):
        """Test S3 file mode."""

        buffer = io.BytesIO()
        writer = PGPackWriter(
            fileobj=buffer,
            compression_method=CompressionMethod.ZSTD,
            s3_file=True,
        )
        writer.from_pandas(sample_dataframe)
        assert writer.compressed_length > 0  # noqa: S101
        assert writer.data_length > 0  # noqa: S101
        buffer.seek(0)
        reader = PGPackReader(buffer)
        assert reader.s3_file is True  # noqa: S101
        assert reader.compressed_length > 0  # noqa: S101
        assert reader.data_length > 0  # noqa: S101
        df_result = reader.to_pandas()
        assert len(df_result) == len(sample_dataframe)  # noqa: S101
        reader.close()

    def test_to_bytes(self, sample_dataframe):
        """Test reading as bytes chunks."""

        buffer = io.BytesIO()
        writer = PGPackWriter(fileobj=buffer)
        writer.from_pandas(sample_dataframe)
        buffer.seek(0)
        reader = PGPackReader(buffer)

        chunks = list(reader.to_bytes())
        assert len(chunks) > 0  # noqa: S101
        full_data = b"".join(chunks)
        assert full_data == (  # noqa: S101
            b"PGCOPY\n\xff\r\n\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08"
            b"\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x05"
            b"Alice\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x19\x00\x00"
            b"\x00\x01\x01\x00\x00\x00\x08@\xe8j\x10\x00\x00\x00\x00\x00\x00"
            b"\x00\x08\x00\x02\xb0\xd5\xd4\xe9@\x00\x00\x00\x00\x08\x00\x02"
            b"\xb0\xde6\xad\xa8\x00\x00\x00\x00&\x00\x00\x00\x01\x00\x00\x00"
            b"\x00\x00\x00\x00\x19\x00\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00"
            b"\x06python\x00\x00\x00\x04data\x00\x08\x00\x00\x00\x08\x00\x00"
            b"\x00\x00\x00\x00\x00\x02\x00\x00\x00\x03Bob\x00\x00\x00\x08\x00"
            b"\x00\x00\x00\x00\x00\x00\x1e\x00\x00\x00\x01\x00\x00\x00\x00\x08"
            b"@\xedL\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x02\xb0\xe9\xf2"
            b"\xc0\xa0\x00\x00\x00\x00\x08\x00\x02\xb0\xf2T\x85\x08\x00\x00"
            b"\x00\x00#\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x19\x00"
            b"\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00\x04rust\x00\x00\x00"
            b"\x03csv\x00\x08\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00"
            b"\x03\x00\x00\x00\x07Charlie\x00\x00\x00\x08\x00\x00\x00\x00"
            b"\x00\x00\x00#\x00\x00\x00\x01\x01\x00\x00\x00\x08@\xf2O\x8c"
            b"\x00\x00\x00\x00\x00\x00\x00\x08\x00\x02\xb0\xfe\x10\x98\x00"
            b"\x00\x00\x00\x00\x08\x00\x02\xb1\x06r\\h\x00\x00\x00\x00\x1e"
            b"\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x19\x00\x00\x00"
            b"\x01\x00\x00\x00\x01\x00\x00\x00\x06pandas\x00\x08\x00\x00"
            b"\x00\x08\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x05"
            b"Diana\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x1c\x00"
            b"\x00\x00\x01\x00\x00\x00\x00\x08@\xe7p\x08\x00\x00\x00\x00"
            b"\x00\x00\x00\x08\x00\x02\xb1\x12.o`\x00\x00\x00\x00\x08\x00"
            b"\x02\xb1\x1a\x903\xc8\x00\x00\x00\x00\x14\x00\x00\x00\x01\x00"
            b"\x00\x00\x00\x00\x00\x00\x19\x00\x00\x00\x00\x00\x00\x00\x01"
            b"\x00\x08\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x05\x00"
            b"\x00\x00\x03Eve\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00 "
            b"\x00\x00\x00\x01\x01\x00\x00\x00\x08@\xf4\x05\x00\x00\x00\x00"
            b"\x00\x00\x00\x00\x08\x00\x02\xb1&LF\xc0\x00\x00\x00\x00\x08"
            b"\x00\x02\xb1.\xae\x0b(\x00\x00\x00\x00'\x00\x00\x00\x01\x00"
            b"\x00\x00\x00\x00\x00\x00\x19\x00\x00\x00\x02\x00\x00\x00\x01"
            b"\x00\x00\x00\x05numpy\x00\x00\x00\x06polars\xff\xff"
        )
        reader.close()

    def test_metadata_properties(self, sample_dataframe: pd.DataFrame):
        """Test metadata properties."""

        buffer = io.BytesIO()
        writer = PGPackWriter(fileobj=buffer)
        writer.from_pandas(sample_dataframe)
        buffer.seek(0)
        reader = PGPackReader(buffer)
        assert reader.columns is not None  # noqa: S101
        assert len(reader.columns) == len(sample_dataframe.columns)  # noqa: S101
        assert len(reader.dtypes) == len(sample_dataframe.columns)  # noqa: S101
        assert reader.compressed_length > 0  # noqa: S101
        assert reader.data_length > 0  # noqa: S101
        reader.close()

    def test_tell_method(self, sample_dataframe):
        """Test tell method."""

        buffer = io.BytesIO()
        writer = PGPackWriter(fileobj=buffer)
        writer.from_pandas(sample_dataframe)
        pos = writer.tell()
        assert pos >= 0  # noqa: S101
        buffer.seek(0)
        reader = PGPackReader(buffer)
        pos = reader.tell()
        assert pos == 21  # noqa: S101
        list(reader.to_rows())
        assert reader.tell() > 0  # noqa: S101
        reader.close()

    def test_close_method(self, sample_dataframe):
        """Test close method."""

        buffer = io.BytesIO()
        writer = PGPackWriter(fileobj=buffer)
        writer.from_pandas(sample_dataframe)
        writer.close()
        assert buffer.closed is True  # noqa: S101
        buffer = io.BytesIO()
        writer = PGPackWriter(fileobj=buffer)
        writer.from_pandas(sample_dataframe)
        buffer.seek(0)
        reader = PGPackReader(buffer)
        assert buffer.closed is False  # noqa: S101
        reader.close()
        assert buffer.closed is True  # noqa: S101


class TestPGPackEdgeCases:
    """Edge cases tests for PGPack."""

    def test_empty_dataframe(self):
        """Test with empty DataFrame."""

        df = pd.DataFrame(columns=["id", "name", "age"])
        buffer = io.BytesIO()
        writer = PGPackWriter(fileobj=buffer)
        writer.from_pandas(df)
        buffer.seek(0)
        reader = PGPackReader(buffer)
        rows = list(reader.to_rows())
        assert len(rows) == 0  # noqa: S101
        df_result = reader.to_pandas()
        assert len(df_result) == 0  # noqa: S101
        assert list(df_result.columns) == ["id", "name", "age"]  # noqa: S101
        reader.close()

    def test_single_row(self):
        """Test with single row DataFrame."""

        df = pd.DataFrame({"col1": [1], "col2": ["test"]})
        buffer = io.BytesIO()
        writer = PGPackWriter(fileobj=buffer)
        writer.from_pandas(df)
        buffer.seek(0)
        reader = PGPackReader(buffer)
        df_result = reader.to_pandas()
        assert len(df_result) == 1  # noqa: S101
        assert df_result["col1"].iloc[0] == 1  # noqa: S101
        assert df_result["col2"].iloc[0] == "test"  # noqa: S101
        reader.close()

    def test_single_column(self):
        """Test with single column DataFrame."""

        df = pd.DataFrame({"single_column": [1, 2, 3, 4, 5]})
        buffer = io.BytesIO()
        writer = PGPackWriter(fileobj=buffer)
        writer.from_pandas(df)
        buffer.seek(0)
        reader = PGPackReader(buffer)
        df_result = reader.to_pandas()
        assert list(df_result.columns) == ["single_column"]  # noqa: S101
        assert len(df_result) == 5  # noqa: S101
        reader.close()

    def test_with_none_values(self):
        """Test with None/NaN values."""

        df = pd.DataFrame(
            {
                "int_col": [1, None, 3, None, 5],
                "float_col": [1.5, None, 3.5, None, 5.5],
                "str_col": ["a", None, "c", None, "e"],
                # Note: String column is excluded from isna()
                # checks due to pandas 3.0.1
                # issues with scalar pd.NA detection.
                # The roundtrip still works correctly.
            }
        )
        buffer = io.BytesIO()
        writer = PGPackWriter(fileobj=buffer)
        writer.from_pandas(df)
        buffer.seek(0)
        reader = PGPackReader(buffer)
        df_result = reader.to_pandas()
        # Check None values are preserved
        assert df_result["int_col"].isna().iloc[1]  # noqa: S101
        assert df_result["float_col"].isna().iloc[1]  # noqa: S101
        assert df_result["str_col"].isna().iloc[1]  # noqa: S101
        reader.close()

    def test_large_dataframe(self):
        """Test with larger DataFrame."""

        df = pd.DataFrame(
            {
                "id": range(10000),
                "value": [i * 1.5 for i in range(10000)],
                "text": [f"text_{i}" for i in range(10000)],
            }
        )
        buffer = io.BytesIO()
        writer = PGPackWriter(
            fileobj=buffer, compression_method=CompressionMethod.LZ4
        )
        writer.from_pandas(df)
        buffer.seek(0)
        reader = PGPackReader(buffer)
        df_result = reader.to_pandas()
        assert len(df_result) == 10000  # noqa: S101
        assert df_result["id"].iloc[0] == 0  # noqa: S101
        assert df_result["id"].iloc[-1] == 9999  # noqa: S101
        reader.close()

    def test_unseekable_stream(self, sample_dataframe):
        """Test with unseekable stream (simulate socket)."""

        class MockSocket:
            def __init__(self, data):
                self.data = data
                self.pos = 0

            def read(self, size):
                if self.pos >= len(self.data):
                    return b""
                result = self.data[self.pos: self.pos + size]
                self.pos += len(result)
                return result

            def tell(self):
                return self.pos

            def seekable(self):
                return False

        buffer = io.BytesIO()
        writer = PGPackWriter(fileobj=buffer)
        writer.from_pandas(sample_dataframe)
        buffer.seek(0)
        data = buffer.getvalue()
        stream = MockSocket(data)
        reader = PGPackReader(stream)
        assert reader.s3_file is False  # noqa: S101
        rows = list(reader.to_rows())
        assert len(rows) == len(sample_dataframe)  # noqa: S101
        reader.close()


class TestPGPackAllTypes:
    """Tests for all PostgreSQL data types."""

    def test_all_types_roundtrip(self):
        """Test roundtrip with all PostgreSQL data types."""

        metadata = [
            # [column_number, [name, oid, length, scale, nested]]
            [1, ["col_int2", PGOid.int2.value, 2, 0, 0]],
            [2, ["col_int4", PGOid.int4.value, 4, 0, 0]],
            [3, ["col_int8", PGOid.int8.value, 8, 0, 0]],
            [4, ["col_float4", PGOid.float4.value, 4, 0, 0]],
            [5, ["col_float8", PGOid.float8.value, 8, 0, 0]],
            [6, ["col_bool", PGOid.bool.value, 1, 0, 0]],
            [7, ["col_text", PGOid.text.value, -1, 0, 0]],
            [8, ["col_bytea", PGOid.bytea.value, -1, 0, 0]],
            [9, ["col_uuid", PGOid.uuid.value, 16, 0, 0]],
            [10, ["col_date", PGOid.date.value, 4, 0, 0]],
            [11, ["col_timestamp", PGOid.timestamp.value, 8, 0, 0]],
            [12, ["col_timestamptz", PGOid.timestamptz.value, 8, 0, 0]],
            [13, ["col_time", PGOid.time.value, 8, 0, 0]],
            [14, ["col_timetz", PGOid.timetz.value, 12, 0, 0]],
            [15, ["col_interval", PGOid.interval.value, 16, 0, 0]],
            [16, ["col_numeric", PGOid.numeric.value, -1, 0, 0]],
            [17, ["col_json", PGOid.json.value, -1, 0, 0]],
            [18, ["col_jsonb", PGOid.jsonb.value, -1, 0, 0]],
            [19, ["col_inet", PGOid.inet.value, -1, 0, 0]],
            [20, ["col_cidr", PGOid.cidr.value, -1, 0, 0]],
            [21, ["col_macaddr", PGOid.macaddr.value, 6, 0, 0]],
            [22, ["col_macaddr8", PGOid.macaddr8.value, 8, 0, 0]],
            [23, ["col_bit", PGOid.bit.value, -1, 0, 0]],
            [24, ["col_varbit", PGOid.varbit.value, -1, 0, 0]],
            [25, ["col_oid", PGOid.oid.value, 4, 0, 0]],
            [26, ["col_point", PGOid.point.value, 16, 0, 0]],
            [27, ["col_line", PGOid.line.value, 24, 0, 0]],
            [28, ["col_lseg", PGOid.lseg.value, 32, 0, 0]],
            [29, ["col_box", PGOid.box.value, 32, 0, 0]],
            [30, ["col_path", PGOid.path.value, -1, 0, 0]],
            [31, ["col_polygon", PGOid.polygon.value, -1, 0, 0]],
            [32, ["col_circle", PGOid.circle.value, 24, 0, 0]],
        ]
        metadata_bytes = json.dumps(metadata).encode("utf-8")
        row = (
            42,  # int2
            100000,  # int4
            123456789012345,  # int8
            3.14159,  # float4
            3.141592653589793,  # float8
            True,  # bool
            "Hello, PostgreSQL!",  # text
            b"\xde\xad\xbe\xef",  # bytea
            uuid.UUID("12345678-1234-5678-1234-567812345678"),  # uuid
            datetime.date(2024, 12, 25),  # date
            datetime.datetime(2024, 12, 25, 12, 30, 45),  # timestamp
            datetime.datetime(
                2024, 12, 25, 12, 30, 45, tzinfo=datetime.timezone.utc
            ),  # timestamptz
            datetime.time(14, 30, 0),  # time
            datetime.time(14, 30, 0, tzinfo=datetime.timezone.utc),  # timetz
            relativedelta(months=6, days=5),  # interval
            decimal.Decimal("1234.56789"),  # numeric
            {"key": "value", "number": 42},  # json
            {"key": "value", "number": 42},  # jsonb
            ipaddress.IPv4Address("192.168.1.1"),  # inet
            ipaddress.IPv4Network("192.168.1.0/24"),  # cidr
            "08:00:2b:01:02:03",  # macaddr
            "08:00:2b:01:02:03:04:05",  # macaddr8
            "101010",  # bit
            "1010101010",  # varbit
            12345,  # oid
            (1.5, 2.5),  # point
            (1.5, 2.5, 3.5),  # line
            [(1.0, 1.0), (2.0, 2.0)],  # lseg
            ((1.0, 1.0), (2.0, 2.0)),  # box
            [(1.0, 1.0), (2.0, 2.0), (3.0, 1.0)],  # path
            ((1.0, 1.0), (2.0, 2.0), (3.0, 1.0)),  # polygon
            (2.5, 2.5, 1.5),  # circle
        )

        buffer = io.BytesIO()
        writer = PGPackWriter(fileobj=buffer, metadata=metadata_bytes)
        writer.from_rows([row])
        buffer.seek(0)
        reader = PGPackReader(buffer)
        rows = list(reader.to_rows())
        assert len(rows) == 1  # noqa: S101
        read_row = rows[0]
        assert read_row[0] == 42  # int2  # noqa: S101
        assert read_row[1] == 100000  # int4  # noqa: S101
        assert read_row[2] == 123456789012345  # int8  # noqa: S101
        assert abs(read_row[3] - 3.14159) < 0.0001  # float4  # noqa: S101
        assert abs(read_row[4] - 3.141592653589793) < 0.0001  # float8  # noqa: E501, S101
        assert read_row[5] is True  # bool  # noqa: S101
        assert read_row[6] == "Hello, PostgreSQL!"  # text  # noqa: S101
        assert read_row[7] == b"\xde\xad\xbe\xef"  # bytea  # noqa: S101
        assert read_row[8] == uuid.UUID(  # noqa: S101
            "12345678-1234-5678-1234-567812345678"
        )  # uuid
        assert read_row[9] == datetime.date(2024, 12, 25)  # date  # noqa: S101
        assert read_row[10] == datetime.datetime(  # noqa: S101
            2024, 12, 25, 12, 30, 45
        )  # timestamp
        assert read_row[11] == datetime.datetime(  # noqa: S101
            2024, 12, 25, 12, 30, 45, tzinfo=datetime.timezone.utc
        )  # timestamptz
        assert read_row[12] == datetime.time(14, 30, 0)  # time  # noqa: S101
        assert read_row[13] == datetime.time(  # noqa: S101
            14, 30, 0, tzinfo=datetime.timezone.utc
        )  # timetz
        assert read_row[14] == relativedelta(months=6, days=5)  # interval  # noqa: E501, S101
        assert read_row[15] == decimal.Decimal("1234.56789")  # numeric  # noqa: E501, S101
        assert read_row[16] == {"key": "value", "number": 42}  # json  # noqa: E501, S101
        assert read_row[17] == {"key": "value", "number": 42}  # jsonb  # noqa: E501, S101
        assert read_row[18] == ipaddress.IPv4Address("192.168.1.1")  # inet  # noqa: E501, S101
        assert read_row[19] == ipaddress.IPv4Network("192.168.1.0/24")  # cidr  # noqa: E501, S101
        assert read_row[20] == "08:00:2b:01:02:03"  # macaddr  # noqa: S101
        assert read_row[21] == "08:00:2b:01:02:03:04:05"  # macaddr8  # noqa: E501, S101
        assert read_row[22] == "101010"  # bit  # noqa: S101
        assert read_row[23] == "1010101010"  # varbit  # noqa: S101
        assert read_row[24] == 12345  # oid  # noqa: S101
        assert read_row[25] == (1.5, 2.5)  # point  # noqa: S101
        assert read_row[26] == (1.5, 2.5, 3.5)  # line  # noqa: S101
        assert read_row[27] == [(1.0, 1.0), (2.0, 2.0)]  # lseg  # noqa: S101
        assert read_row[28] == ((1.0, 1.0), (2.0, 2.0))  # box  # noqa: S101
        assert read_row[29] == [(1.0, 1.0), (2.0, 2.0), (3.0, 1.0)]  # path  # noqa: E501, S101
        assert read_row[30] == ((1.0, 1.0), (2.0, 2.0), (3.0, 1.0))  # polygon  # noqa: E501, S101
        assert read_row[31] == (2.5, 2.5, 1.5)  # circle  # noqa: S101
        reader.close()


class TestPGPackRepr:
    """Tests for PGPack representation."""

    def test_reader_repr(self, sample_dataframe):
        """Test PGPackReader __repr__."""

        buffer = io.BytesIO()
        writer = PGPackWriter(fileobj=buffer)
        writer.from_pandas(sample_dataframe)
        buffer.seek(0)
        reader = PGPackReader(buffer)
        repr_str = repr(reader)
        assert "<PostgreSQL/GreenPlum compressed dump>" in repr_str  # noqa: S101
        assert "Total Columns: 8" in repr_str  # noqa: S101
        assert "Compression Method: ZSTD" in repr_str  # noqa: S101
        reader.close()

    def test_writer_repr(self, sample_dataframe):
        """Test PGPackWriter __repr__ after write."""

        buffer = io.BytesIO()
        writer = PGPackWriter(fileobj=buffer)
        writer.from_pandas(sample_dataframe)
        repr_str = repr(writer)
        assert "<PostgreSQL/GreenPlum compressed dump>" in repr_str  # noqa: S101
        assert "Total Columns: 8" in repr_str  # noqa: S101
        writer.close()

    def test_writer_repr_empty(self):
        """Test PGPackWriter __repr__ before write."""

        writer = PGPackWriter()
        repr_str = repr(writer)
        assert "<PostgreSQL/GreenPlum compressed dump>" in repr_str  # noqa: S101
        assert "Total Columns: 0" in repr_str  # noqa: S101


class TestPGPackS3Mode:
    """Tests for S3 mode specific features."""

    def test_s3_mode_tail_presence(self, sample_dataframe):
        """Test that S3 mode writes tail at end."""

        buffer = io.BytesIO()
        writer = PGPackWriter(
            fileobj=buffer,
            compression_method=CompressionMethod.ZSTD,
            s3_file=True,
        )
        writer.from_pandas(sample_dataframe)
        buffer.seek(-16, io.SEEK_END)
        tail = buffer.read(16)
        assert len(tail) == 16  # noqa: S101
        buffer.seek(0)
        reader = PGPackReader(buffer)
        assert reader.s3_file is True  # noqa: S101
        assert reader.compressed_length > 0  # noqa: S101
        assert reader.data_length > 0  # noqa: S101
        reader.close()

    def test_s3_mode_with_compression(self, sample_dataframe):
        """Test S3 mode with different compression methods."""

        for method in [
            CompressionMethod.GZIP,
            CompressionMethod.LZ4,
            CompressionMethod.ZSTD,
            CompressionMethod.SNAPPY,
        ]:
            buffer = io.BytesIO()
            writer = PGPackWriter(
                fileobj=buffer,
                compression_method=method,
                s3_file=True,
                compression_level=3,
            )
            writer.from_pandas(sample_dataframe)
            buffer.seek(0)
            reader = PGPackReader(buffer)
            assert reader.s3_file is True  # noqa: S101
            assert reader.compression_method == method  # noqa: S101
            df_result = reader.to_pandas()
            assert len(df_result) == len(sample_dataframe)  # noqa: S101
            reader.close()


if __name__ == "__main__":
    pytest.main([__file__, "-svv"])
