from struct import (
    pack,
    unpack,
)


cpdef (double, double) read_point(
    bytes binary_data,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Unpack point value."""

    return unpack("!2d", binary_data)


cpdef bytes write_point(
    (double, double) dtype_value,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Pack point value."""

    return pack("!2d", *dtype_value)


cpdef (double, double, double) read_line(
    bytes binary_data,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Unpack line value."""

    return unpack("!3d", binary_data)


cpdef bytes write_line(
    (double, double, double) dtype_value,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Pack line value."""

    return pack("!3d", *dtype_value)


cpdef list read_lseg(
    bytes binary_data,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Unpack lseg value."""

    cdef double x1, y1, x2, y2

    if len(binary_data) < 32:
        return [(0.0, 0.0), (0.0, 0.0)]

    x1, y1, x2, y2 = unpack("!4d", binary_data)
    return [(x1, y1), (x2, y2)]


cpdef bytes write_lseg(
    list dtype_value,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Pack lseg value."""
    
    return pack("!4d", *dtype_value[0], *dtype_value[1])


cpdef ((double, double), (double, double)) read_box(
    bytes binary_data,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Unpack box value."""

    cdef double x1, y1, x2, y2

    if len(binary_data) < 32:
        return ((0.0, 0.0), (0.0, 0.0))

    x1 = unpack("!d", binary_data[0:8])[0]
    y1 = unpack("!d", binary_data[8:16])[0]
    x2 = unpack("!d", binary_data[16:24])[0]
    y2 = unpack("!d", binary_data[24:32])[0]

    return ((x1, y1), (x2, y2))


cpdef bytes write_box(
    object dtype_value,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Pack box value."""

    return pack("!dddd",
        dtype_value[0][0], dtype_value[0][1],
        dtype_value[1][0], dtype_value[1][1]
    )


cpdef object read_path(
    bytes binary_data,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Unpack path value."""

    cdef bint is_closed
    cdef int num_points
    cdef list path_data = []
    cdef long i
    cdef double x, y

    if len(binary_data) < 8:
        return []

    is_closed = unpack("!i", binary_data[:4])[0]
    num_points = unpack("!i", binary_data[4:8])[0]

    for i in range(num_points):
        offset = 8 + i * 16

        if offset + 16 > len(binary_data):
            break

        x = unpack("!d", binary_data[offset:offset + 8])[0]
        y = unpack("!d", binary_data[offset + 8:offset + 16])[0]
        path_data.append((x, y))

    if is_closed:
        return tuple(path_data)

    return path_data


cpdef bytes write_path(
    object dtype_value,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Pack path value."""

    cdef int is_closed = 1 if isinstance(dtype_value, tuple) else 0
    cdef int num_points = len(dtype_value)
    cdef list coords = []
    cdef object point

    for point in dtype_value:
        coords.append(point[0])
        coords.append(point[1])

    return pack(f"!ii{num_points * 2}d", is_closed, num_points, *coords)


cpdef tuple read_polygon(
    bytes binary_data,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Unpack polygon value."""

    if len(binary_data) < 4:
        return ()

    cdef int num_points = unpack("!i", binary_data[:4])[0]
    cdef list coords = []
    cdef long i
    cdef double x, y

    for i in range(num_points):
        offset = 4 + i * 16
        if offset + 16 > len(binary_data):
            break
        x = unpack("!d", binary_data[offset:offset + 8])[0]
        y = unpack("!d", binary_data[offset + 8:offset + 16])[0]
        coords.append((x, y))

    return tuple(coords)


cpdef bytes write_polygon(
    tuple dtype_value,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Pack polygon value."""

    cdef int num_points = len(dtype_value)
    cdef list coords = []
    cdef object point

    for point in dtype_value:
        coords.append(point[0])
        coords.append(point[1])

    return pack(f"!i{num_points * 2}d", num_points, *coords)


cpdef tuple read_circle(
    bytes binary_data,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Unpack circle value."""

    if len(binary_data) < 24:
        return (0.0, 0.0, 0.0)

    return unpack("!ddd", binary_data)


cpdef bytes write_circle(
    tuple dtype_value,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Pack circle value."""

    return pack("!ddd", *dtype_value)
