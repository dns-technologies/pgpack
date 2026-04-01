from datetime import (
    date,
    datetime,
    time,
    timedelta,
    timezone,
)
from dateutil.relativedelta import relativedelta
from struct import (
    pack,
    unpack,
    unpack_from,
)

from pgpack.pgcopylib.core.errors import PGCopyTypeError


cdef object DEFAULT_DATE = date(2000, 1, 1)
cdef object DEFAULT_DATETIME = datetime(2000, 1, 1)
cdef long long MICROSECONDS_PER_SECOND = 1_000_000
cdef int SECONDS_PER_MINUTE = 60
cdef int HOURS_PER_DAY = 24
cdef int SECONDS_PER_HOUR = 3600


cpdef object read_date(
    bytes binary_data,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Unpack date value."""

    return DEFAULT_DATE + timedelta(days=unpack("!i", binary_data)[0])


cpdef bytes write_date(
    object dtype_value,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Pack date value."""

    if dtype_value.__class__.__name__ in ("Timestamp", "datetime"):
        dtype_value = dtype_value.date()

    cdef int days = (dtype_value - DEFAULT_DATE).days
    return pack("!i", days)


cpdef object read_timestamp(
    bytes binary_data,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Unpack timestamp value."""

    return DEFAULT_DATETIME + timedelta(
        microseconds=unpack("!q", binary_data)[0]
    )


cpdef bytes write_timestamp(
    object dtype_value,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Pack timestamp value."""

    cdef long long microseconds

    if dtype_value.__class__.__name__ == "Timestamp":
        dtype_value = dtype_value.to_pydatetime()
    elif dtype_value.__class__ is date:
        dtype_value = datetime.combine(dtype_value, datetime.min.time())

    if dtype_value.__class__ is not datetime:
        raise PGCopyTypeError(f"Expected datetime, got {type(dtype_value)}")

    if dtype_value.tzinfo is not None:
        dtype_value = (
            dtype_value.astimezone(timezone.utc).replace(tzinfo=None)
        )
    
    microseconds = int(
        (dtype_value - DEFAULT_DATETIME).total_seconds() *
        MICROSECONDS_PER_SECOND
    )
    return pack("!q", microseconds)


cpdef object read_timestamptz(
    bytes binary_data,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Unpack timestamptz value."""

    return read_timestamp(binary_data).replace(tzinfo=timezone.utc)


cpdef bytes write_timestamptz(
    object dtype_value,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Pack timestamptz value."""

    if dtype_value.__class__.__name__ == "Timestamp":
        dtype_value = dtype_value.to_pydatetime()

    if not hasattr(dtype_value, 'astimezone'):
        raise PGCopyTypeError(
            f"Expected datetime with tzinfo, got {type(dtype_value)}",
        )

    if dtype_value.tzinfo is not None:
        dt_utc = dtype_value.astimezone(timezone.utc)
    else:
        dt_utc = dtype_value.replace(tzinfo=timezone.utc)

    dt_utc_naive = dt_utc.replace(tzinfo=None)
    return write_timestamp(dt_utc_naive)


cpdef object read_time(
    bytes binary_data,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Unpack time value."""

    cdef long long microseconds, total_seconds, total_minutes
    cdef long long hours, minutes, seconds, microsecond

    microseconds = unpack_from("!q", binary_data)[0]
    total_seconds = microseconds // MICROSECONDS_PER_SECOND
    microsecond = microseconds % MICROSECONDS_PER_SECOND
    seconds = total_seconds % SECONDS_PER_MINUTE
    total_minutes = total_seconds // SECONDS_PER_MINUTE
    minutes = total_minutes % SECONDS_PER_MINUTE
    hours = total_minutes // SECONDS_PER_MINUTE
    hours = hours % HOURS_PER_DAY

    return time(hours, minutes, seconds, microsecond)


cpdef bytes write_time(
    object dtype_value,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Pack time value."""

    cdef long long total_microseconds

    if dtype_value.__class__ is timedelta:
        total_microseconds = int(
            dtype_value.total_seconds() *
            MICROSECONDS_PER_SECOND
        )
        total_microseconds = total_microseconds % (
            HOURS_PER_DAY *
            SECONDS_PER_HOUR *
            MICROSECONDS_PER_SECOND
        )
    elif dtype_value.__class__ is time:
        if dtype_value.tzinfo is not None:
            dummy_date = date(2000, 1, 1)
            dt = datetime.combine(dummy_date, dtype_value)
            dt_utc = dt.astimezone(timezone.utc)
            dtype_value = dt_utc.timetz().replace(tzinfo=None)

        total_microseconds = (
            (dtype_value.hour * SECONDS_PER_HOUR * MICROSECONDS_PER_SECOND) +
            (dtype_value.minute * SECONDS_PER_MINUTE *
            MICROSECONDS_PER_SECOND) +
            (dtype_value.second * MICROSECONDS_PER_SECOND) +
            dtype_value.microsecond
        )
    else:
        raise PGCopyTypeError(
            "dtype_value must be datetime.time or datetime.timedelta",
        )

    return pack("!q", total_microseconds)


cpdef object read_timetz(
    bytes binary_data,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Unpack timetz value."""

    cdef object time_notz = read_time(binary_data)
    cdef long tz_offset_sec = unpack("!i", binary_data[-4:])[0]
    cdef object tz_offset = timedelta(seconds=tz_offset_sec)
    return time_notz.replace(tzinfo=timezone(tz_offset))


cpdef bytes write_timetz(
    object dtype_value,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Pack timetz value."""

    cdef long long total_microseconds
    cdef int offset
    cdef object tz_offset

    if dtype_value.__class__ is timedelta:
        total_microseconds = int(
            dtype_value.total_seconds() * MICROSECONDS_PER_SECOND
        )
        total_microseconds = total_microseconds % (
            HOURS_PER_DAY * SECONDS_PER_HOUR * MICROSECONDS_PER_SECOND
        )
        offset = 0

    elif dtype_value.__class__ is time:
        total_microseconds = (
            (dtype_value.hour * SECONDS_PER_HOUR * MICROSECONDS_PER_SECOND) +
            (dtype_value.minute * SECONDS_PER_MINUTE *
            MICROSECONDS_PER_SECOND) + (dtype_value.second *
            MICROSECONDS_PER_SECOND) + dtype_value.microsecond
        )

        if dtype_value.tzinfo is None:
            offset = 0
        else:
            tz_offset = dtype_value.tzinfo.utcoffset(None)
            offset = int(tz_offset.total_seconds())
    else:
        raise PGCopyTypeError(
            "dtype_value must be datetime.time or datetime.timedelta",
        )

    return pack("!qi", total_microseconds, offset)


cpdef object read_interval(
    bytes binary_data,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Unpack interval value."""

    cdef long long microseconds
    cdef int days, months

    if len(binary_data) < 16:
        return relativedelta()

    microseconds, days, months = unpack("!qii", binary_data)
    result = relativedelta(
        months=months,
        days=days,
        microseconds=microseconds,
    )
    return result


cpdef bytes write_interval(
    object dtype_value,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Pack interval value."""

    cdef:
        long long total_microseconds
        int days, months

    months = dtype_value.months or 0
    days = dtype_value.days or 0
    hours = dtype_value.hours or 0
    minutes = dtype_value.minutes or 0
    seconds = dtype_value.seconds or 0
    microseconds = dtype_value.microseconds or 0
    total_microseconds = (
        hours * 3600 * 1_000_000 +
        minutes * 60 * 1_000_000 +
        seconds * 1_000_000 +
        microseconds
    )
    return pack("!qii", total_microseconds, days, months)
