import io
import uuid
import ipaddress
import decimal
from datetime import date, datetime, time, timezone
from dateutil.relativedelta import relativedelta

import pytest

from pgpack import (
    PGCopyReader,
    PGCopyWriter,
    PGOid,
    PGCopySignatureError,
    PGCopyRecordError,
)


expected_columns = ["column_0", "column_1", "column_2"]


@pytest.fixture
def sample_pgtypes():
    """Sample PostgreSQL OID types for tests."""
    return [PGOid.int4, PGOid.text, PGOid.uuid]


@pytest.fixture
def all_pgtypes():
    """All supported PostgreSQL OID types for tests."""
    return [
        PGOid.int2,
        PGOid.int4,
        PGOid.int8,
        PGOid.float4,
        PGOid.float8,
        PGOid.bool,
        PGOid.text,
        PGOid.bytea,
        PGOid.uuid,
        PGOid.date,
        PGOid.timestamp,
        PGOid.timestamptz,
        PGOid.time,
        PGOid.timetz,
        PGOid.interval,
        PGOid.numeric,
        PGOid.json,
        PGOid.jsonb,
        PGOid.inet,
        PGOid.cidr,
        PGOid.macaddr,
        PGOid.macaddr8,
        PGOid.bit,
        PGOid.varbit,
        PGOid.oid,
        PGOid.point,
        PGOid.line,
        PGOid.lseg,
        PGOid.box,
        PGOid.path,
        PGOid.polygon,
        PGOid.circle,
    ]


@pytest.fixture
def all_types_row():
    """Row with all supported data types."""
    return (
        42,  # int2
        100000,  # int4
        123456789012345,  # int8
        3.14159,  # float4
        3.141592653589793,  # float8
        True,  # bool
        "Hello, PostgreSQL!",  # text
        b"\xde\xad\xbe\xef",  # bytea
        uuid.UUID("12345678-1234-5678-1234-567812345678"),  # uuid
        date(2024, 12, 25),  # date
        datetime(2024, 12, 25, 12, 30, 45),  # timestamp
        datetime(
            2024, 12, 25, 12, 30, 45, tzinfo=timezone.utc
        ),  # timestamptz
        time(14, 30, 0),  # time
        time(14, 30, 0, tzinfo=timezone.utc),  # timetz
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


@pytest.fixture
def array_pgtypes():
    """Array types for tests."""
    return [
        PGOid._int4,
        PGOid._text,
        PGOid._uuid,
        PGOid._float8,
        PGOid._date,
        PGOid._timestamp,
        PGOid._bool,
    ]


@pytest.fixture
def array_row():
    """Row with array data."""
    return (
        [1, 2, 3, 4, 5],  # int4 array
        ["John", "Jane", "Bob"],  # text array
        [
            uuid.UUID("12345678-1234-5678-1234-567812345678"),
            uuid.UUID("87654321-4321-8765-4321-876543210987"),
        ],  # uuid array
        [1.1, 2.2, 3.3],  # float8 array
        [date(2024, 1, 1), date(2024, 12, 31)],  # date array
        [
            datetime(2024, 10, 1, 10, 0, 0),
            datetime(2024, 10, 2, 14, 30, 0),
        ],  # timestamp array
        [True, False, True],  # bool array
    )


@pytest.fixture
def sample_row():
    """Sample row for tests."""
    return (
        42,
        "Hello, World!",
        uuid.UUID("12345678-1234-5678-1234-567812345678"),
    )


@pytest.fixture
def sample_data(sample_pgtypes, sample_row):
    """Create buffer with sample data."""
    buffer = io.BytesIO()
    writer = PGCopyWriter(sample_pgtypes, buffer)
    writer.write([sample_row])
    buffer.seek(0)
    return buffer


@pytest.fixture
def reader(sample_data, sample_pgtypes):
    """Create reader with sample data."""
    return PGCopyReader(sample_data, sample_pgtypes)


class TestPGCopyWriter:
    """Тесты для PGCopyWriter."""

    def test_writer_basic(self, sample_pgtypes, sample_row):
        """Test basic write operation."""

        output = io.BytesIO()
        writer = PGCopyWriter(sample_pgtypes, output)
        writer.write([sample_row])
        result = output.getvalue()
        assert len(result) > 0  # noqa: S101
        assert result[:10] == b"PGCOPY\n\xff\r\n"  # noqa: S101
        output.seek(0)
        reader = PGCopyReader(output, sample_pgtypes)
        rows = list(reader.to_rows())
        assert len(rows) == 1  # noqa: S101
        assert rows[0][0] == sample_row[0]  # noqa: S101
        assert rows[0][1] == sample_row[1]  # noqa: S101
        assert rows[0][2] == sample_row[2]  # noqa: S101

    def test_writer_all_types(self, all_pgtypes, all_types_row):
        """Test writing all supported data types."""

        output = io.BytesIO()
        writer = PGCopyWriter(all_pgtypes, output)
        writer.write([all_types_row])
        output.seek(0)
        reader = PGCopyReader(output, all_pgtypes)
        rows = list(reader.to_rows())
        assert len(rows) == 1  # noqa: S101
        read_row = rows[0]
        assert read_row[0] == 42  # noqa: S101
        assert read_row[1] == 100000  # noqa: S101
        assert read_row[2] == 123456789012345  # noqa: S101
        assert abs(read_row[3] - 3.14159) < 0.0001  # noqa: S101
        assert abs(read_row[4] - 3.141592653589793) < 0.0001  # noqa: S101
        assert read_row[5] is True  # noqa: S101
        assert read_row[6] == "Hello, PostgreSQL!"  # noqa: S101
        assert read_row[7] == b"\xde\xad\xbe\xef"  # noqa: S101
        assert read_row[8] == uuid.UUID("12345678-1234-5678-1234-567812345678")  # noqa: S101
        assert read_row[9] == date(2024, 12, 25)  # noqa: S101
        assert read_row[10] == datetime(2024, 12, 25, 12, 30, 45)  # noqa: S101
        assert read_row[11] == datetime(  # noqa: S101
            2024, 12, 25, 12, 30, 45, tzinfo=timezone.utc
        )
        assert read_row[12] == time(14, 30, 0)  # noqa: S101
        assert read_row[13] == time(14, 30, 0, tzinfo=timezone.utc)  # noqa: S101
        assert read_row[14] == relativedelta(months=6, days=5)  # noqa: S101
        assert read_row[15] == decimal.Decimal("1234.56789")  # noqa: S101
        assert read_row[16] == {"key": "value", "number": 42}  # noqa: S101
        assert read_row[17] == {"key": "value", "number": 42}  # noqa: S101
        assert read_row[18] == ipaddress.IPv4Address("192.168.1.1")  # noqa: S101
        assert read_row[19] == ipaddress.IPv4Network("192.168.1.0/24")  # noqa: S101
        assert read_row[20] == "08:00:2b:01:02:03"  # noqa: S101
        assert read_row[21] == "08:00:2b:01:02:03:04:05"  # noqa: S101
        assert read_row[22] == "101010"  # noqa: S101
        assert read_row[23] == "1010101010"  # noqa: S101
        assert read_row[24] == 12345  # noqa: S101
        assert read_row[25] == (1.5, 2.5)  # noqa: S101
        assert read_row[26] == (1.5, 2.5, 3.5)  # noqa: S101
        assert read_row[27] == [(1.0, 1.0), (2.0, 2.0)]  # noqa: S101
        assert read_row[28] == ((1.0, 1.0), (2.0, 2.0))  # noqa: S101
        assert read_row[29] == [(1.0, 1.0), (2.0, 2.0), (3.0, 1.0)]  # noqa: S101
        assert read_row[30] == ((1.0, 1.0), (2.0, 2.0), (3.0, 1.0))  # noqa: S101
        assert read_row[31] == (2.5, 2.5, 1.5)  # noqa: S101

    def test_writer_multiple_rows(self, sample_pgtypes, sample_row):
        """Test writing multiple rows."""

        output = io.BytesIO()
        writer = PGCopyWriter(sample_pgtypes, output)
        writer.write([sample_row, sample_row])
        output.seek(0)
        reader = PGCopyReader(output, sample_pgtypes)
        rows = list(reader.to_rows())
        assert len(rows) == 2  # noqa: S101

    def test_writer_empty_rows(self, sample_pgtypes):
        """Test writing empty rows."""

        output = io.BytesIO()
        writer = PGCopyWriter(sample_pgtypes, output)
        writer.write([])
        result = output.getvalue()
        assert len(result) > 0  # noqa: S101

    def test_writer_no_pgtypes(self):
        """Test writer with no pgtypes."""

        with pytest.raises(PGCopyRecordError, match="PGOids not defined!"):
            PGCopyWriter([], io.BytesIO())

    def test_writer_tell(self, sample_pgtypes, sample_row):
        """Test tell method."""

        output = io.BytesIO()
        writer = PGCopyWriter(sample_pgtypes, output)
        assert writer.tell() == 0  # noqa: S101
        writer.write([sample_row])
        assert writer.tell() > 0  # noqa: S101

    def test_writer_repr(self, sample_pgtypes):
        """Test string representation."""

        writer = PGCopyWriter(sample_pgtypes, io.BytesIO())
        repr_str = repr(writer)
        assert "PGCopy dump writer" in repr_str  # noqa: S101
        assert "Total columns: 3" in repr_str  # noqa: S101
        assert "Total rows: 0" in repr_str  # noqa: S101


class TestPGCopyReader:
    """Тесты для PGCopyReader."""

    def test_reader_columns(self, reader: PGCopyReader, sample_pgtypes):
        """Test columns property."""

        assert [col for col in reader.pgtypes] == sample_pgtypes  # noqa: S101

    def test_reader_to_rows(self, reader: PGCopyReader, sample_row):
        """Test to_rows method."""

        rows = list(reader.to_rows())
        assert len(rows) == 1  # noqa: S101
        assert rows[0][0] == sample_row[0]  # noqa: S101
        assert rows[0][1] == sample_row[1]  # noqa: S101
        assert rows[0][2] == sample_row[2]  # noqa: S101

    def test_reader_tell(self, reader: PGCopyReader):
        """Test tell method."""

        pos = reader.tell()
        assert pos > 0  # noqa: S101

    def test_reader_repr(self, reader: PGCopyReader):
        """Test string representation."""

        reader.read_info()
        repr_str = repr(reader)
        assert "PGCopy dump reader" in repr_str  # noqa: S101
        assert "Total columns: 3" in repr_str  # noqa: S101
        assert "Total rows: 1" in repr_str  # noqa: S101

    def test_reader_read_info(self, sample_pgtypes, sample_row):
        """Test read_info method."""

        buffer = io.BytesIO()
        writer = PGCopyWriter(sample_pgtypes, buffer)
        writer.write([sample_row, sample_row])
        buffer.seek(0)
        reader = PGCopyReader(buffer)
        reader.read_info()
        assert reader.num_rows == 2  # noqa: S101

    def test_reader_read_info_with_pgtypes(self, sample_pgtypes, sample_row):
        """Test read_info with pgtypes."""

        buffer = io.BytesIO()
        writer = PGCopyWriter(sample_pgtypes, buffer)
        writer.write([sample_row, sample_row])
        buffer.seek(0)
        reader = PGCopyReader(buffer, sample_pgtypes)
        reader.read_info()
        assert reader.num_rows == 2  # noqa: S101
        assert [dtype for dtype in reader.pgtypes] == sample_pgtypes  # noqa: S101

    def test_reader_invalid_signature(self):
        """Test reader with invalid signature."""

        buffer = io.BytesIO(b"invalid signature")
        with pytest.raises(
            PGCopySignatureError, match="PGCopy signature not match!"
        ):
            PGCopyReader(buffer)

    def test_reader_empty_file(self):
        """Test reader with empty file."""

        buffer = io.BytesIO()
        buffer.write(b"PGCOPY\n\xff\r\n\x00\x00\x00\x00\x00\x00\x00\x00")
        buffer.seek(0)
        reader = PGCopyReader(buffer)
        rows = list(reader.to_rows())
        assert len(rows) == 0  # noqa: S101


class TestPGCopyReaderWithArrays:
    """Тесты для работы с массивами."""

    def test_write_arrays(self, array_pgtypes, array_row):
        """Test writing rows with arrays."""

        output = io.BytesIO()
        writer = PGCopyWriter(array_pgtypes, output)
        writer.write([array_row])
        output.seek(0)
        reader = PGCopyReader(output, array_pgtypes)
        rows = list(reader.to_rows())
        assert len(rows) == 1  # noqa: S101
        assert rows[0][0] == [1, 2, 3, 4, 5]  # noqa: S101
        assert rows[0][1] == ["John", "Jane", "Bob"]  # noqa: S101
        assert rows[0][2][0] == uuid.UUID(  # noqa: S101
            "12345678-1234-5678-1234-567812345678"
        )
        assert rows[0][2][1] == uuid.UUID(  # noqa: S101
            "87654321-4321-8765-4321-876543210987"
        )
        assert rows[0][3] == [1.1, 2.2, 3.3]  # noqa: S101
        assert rows[0][4] == [date(2024, 1, 1), date(2024, 12, 31)]  # noqa: S101
        assert rows[0][5][0] == datetime(2024, 10, 1, 10, 0, 0)  # noqa: S101
        assert rows[0][5][1] == datetime(2024, 10, 2, 14, 30, 0)  # noqa: S101
        assert rows[0][6] == [True, False, True]  # noqa: S101

    def test_read_write_arrays_roundtrip(self, array_pgtypes, array_row):
        """Test roundtrip with arrays."""

        buffer = io.BytesIO()
        writer = PGCopyWriter(array_pgtypes, buffer)
        writer.write([array_row])
        buffer.seek(0)
        reader = PGCopyReader(buffer, array_pgtypes)
        rows = list(reader.to_rows())
        assert len(rows) == 1  # noqa: S101
        assert rows[0][0] == array_row[0]  # noqa: S101
        assert rows[0][1] == array_row[1]  # noqa: S101
        assert rows[0][2] == array_row[2]  # noqa: S101
        assert rows[0][3] == array_row[3]  # noqa: S101
        assert rows[0][4] == array_row[4]  # noqa: S101
        assert rows[0][5] == array_row[5]  # noqa: S101
        assert rows[0][6] == array_row[6]  # noqa: S101

    def test_empty_arrays(self, array_pgtypes):
        """Test empty arrays."""

        row = ([], [], [], [], [], [], [])
        buffer = io.BytesIO()
        writer = PGCopyWriter(array_pgtypes, buffer)
        writer.write([row])
        buffer.seek(0)
        reader = PGCopyReader(buffer, array_pgtypes)
        rows = list(reader.to_rows())
        assert len(rows) == 1  # noqa: S101
        assert rows[0][0] == []  # noqa: S101
        assert rows[0][1] == []  # noqa: S101
        assert rows[0][2] == []  # noqa: S101
        assert rows[0][3] == []  # noqa: S101
        assert rows[0][4] == []  # noqa: S101
        assert rows[0][5] == []  # noqa: S101
        assert rows[0][6] == []  # noqa: S101


class TestPGCopyEdgeCases:
    """Тесты для граничных случаев."""

    def test_none_values(self, sample_pgtypes):
        """Test None values (NULL in PostgreSQL)."""

        row = (None, None, None)
        buffer = io.BytesIO()
        writer = PGCopyWriter(sample_pgtypes, buffer)
        writer.write([row])
        buffer.seek(0)
        reader = PGCopyReader(buffer, sample_pgtypes)
        rows = list(reader.to_rows())
        assert len(rows) == 1  # noqa: S101
        assert rows[0][0] is None  # noqa: S101
        assert rows[0][1] is None  # noqa: S101
        assert rows[0][2] is None  # noqa: S101

    def test_empty_strings(self, sample_pgtypes):
        """Test empty strings."""

        row = (1, "", uuid.uuid4())
        buffer = io.BytesIO()
        writer = PGCopyWriter(sample_pgtypes, buffer)
        writer.write([row])
        buffer.seek(0)
        reader = PGCopyReader(buffer, sample_pgtypes)
        rows = list(reader.to_rows())
        assert len(rows) == 1  # noqa: S101
        assert rows[0][1] == ""  # noqa: S101

    def test_large_dataset(self, sample_pgtypes, sample_row):
        """Test with large number of rows."""

        buffer = io.BytesIO()
        writer = PGCopyWriter(sample_pgtypes, buffer)
        rows = [sample_row for _ in range(1000)]
        writer.write(rows)
        buffer.seek(0)
        reader = PGCopyReader(buffer, sample_pgtypes)
        rows_read = list(reader.to_rows())
        assert len(rows_read) == 1000  # noqa: S101

    def test_bytes_data(self):
        """Test bytes data (bytea type)."""

        pgtypes = [PGOid.bytea]
        test_bytes = b"\xde\xad\xbe\xef"
        row = (test_bytes,)
        buffer = io.BytesIO()
        writer = PGCopyWriter(pgtypes, buffer)
        writer.write([row])
        buffer.seek(0)
        reader = PGCopyReader(buffer, pgtypes)
        rows = list(reader.to_rows())
        assert len(rows) == 1  # noqa: S101
        assert rows[0][0] == test_bytes  # noqa: S101

    def test_numeric_precision(self):
        """Test numeric with high precision."""

        pgtypes = [PGOid.numeric]
        test_value = decimal.Decimal("123456789.123456789")
        row = (test_value,)
        buffer = io.BytesIO()
        writer = PGCopyWriter(pgtypes, buffer)
        writer.write([row])
        buffer.seek(0)
        reader = PGCopyReader(buffer, pgtypes)
        rows = list(reader.to_rows())
        assert len(rows) == 1  # noqa: S101
        assert rows[0][0] == test_value  # noqa: S101

    def test_close(self, sample_data):
        """Test close method."""

        reader = PGCopyReader(sample_data)
        reader.close()
        reader.close()  # Should not raise error


class TestPGCopyWriterFromGenerator:
    """Тесты для записи из генератора."""

    def test_from_rows_generator(self, sample_pgtypes, sample_row):
        """Test from_rows with generator."""

        def row_generator():
            for _ in range(10):
                yield sample_row

        output = io.BytesIO()
        writer = PGCopyWriter(sample_pgtypes, output)
        writer.write(row_generator())
        output.seek(0)
        reader = PGCopyReader(output, sample_pgtypes)
        rows = list(reader.to_rows())
        assert len(rows) == 10  # noqa: S101

    def test_from_rows_iterator(self, sample_pgtypes, sample_row):
        """Test from_rows with iterator."""

        output = io.BytesIO()
        writer = PGCopyWriter(sample_pgtypes, output)
        writer.write(iter([sample_row, sample_row]))
        output.seek(0)
        reader = PGCopyReader(output, sample_pgtypes)
        rows = list(reader.to_rows())
        assert len(rows) == 2  # noqa: S101


class TestPGCopyReaderProperties:
    """Тесты для свойств reader."""

    def test_num_columns(self, reader: PGCopyReader):
        """Test num_columns property."""

        assert reader.num_columns == 3  # noqa: S101

    def test_num_rows_after_read(self, reader: PGCopyReader):
        """Test num_rows after reading."""

        assert reader.num_rows == 0  # noqa: S101
        list(reader.to_rows())
        assert reader.num_rows == 1  # noqa: S101

    def test_read_row_generator(self, reader: PGCopyReader, sample_row):
        """Test read_row generator."""

        row_gen = reader.read_row()
        row = list(row_gen)
        assert row is not None  # noqa: S101
        assert row[0] == sample_row[0]  # noqa: S101
        assert row[1] == sample_row[1]  # noqa: S101
        assert row[2] == sample_row[2]  # noqa: S101


class TestPGCopyWriterProperties:
    """Тесты для свойств writer."""

    def test_num_columns(self, sample_pgtypes):
        """Test num_columns property."""

        writer = PGCopyWriter(sample_pgtypes, io.BytesIO())
        assert writer.num_columns == 3  # noqa: S101

    def test_num_rows_after_write(self, sample_pgtypes, sample_row):
        """Test num_rows after writing."""

        writer = PGCopyWriter(sample_pgtypes, io.BytesIO())
        assert writer.num_rows == 0  # noqa: S101
        writer.write([sample_row])
        assert writer.num_rows == 1  # noqa: S101


if __name__ == "__main__":
    pytest.main([__file__, "-svv"])
