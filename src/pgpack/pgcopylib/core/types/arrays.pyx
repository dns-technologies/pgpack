# arrays.pyx
from cpython cimport PyBytes_AsString
from struct import (
    pack,
    unpack,
)


cdef bytes NULLABLE = b"\xff\xff\xff\xff"


cdef list recursive_elements(list elements, list array_struct):
    """Recursive unpack array struct."""

    cdef long chunk, num_chunks
    cdef long _i, start, end
    cdef long elements_len = len(elements)
    cdef list result = []

    if not array_struct:
        return elements

    chunk = array_struct.pop()

    if elements_len == chunk:
        return recursive_elements(elements, array_struct)

    num_chunks = (elements_len + chunk - 1) // chunk

    for _i in range(num_chunks):
        start = _i * chunk
        end = start + chunk

        if end > elements_len:
            end = elements_len

        result.append(elements[start:end])

    return recursive_elements(result, array_struct)


cdef list get_num_dim(object type_values):
    """Get list of num dim."""

    cdef list num_dim = []
    cdef object current = type_values

    while current.__class__ is list and len(current) > 0:
        num_dim.append(len(current))
        current = current[0]

    return num_dim


cdef long prod(list iterable):
    """Cython math.prod."""

    cdef long item, result = 1

    for item in iterable:
        result *= item

    return result


cdef object _reader(object buffer_object, object pgoid_function):
    """Read array record."""

    cdef bytes _bytes = buffer_object.read(4)

    if len(_bytes) < 4:
        return None

    cdef const unsigned char *buf = <const unsigned char*>PyBytes_AsString(
        _bytes,
    )
    cdef int length = (buf[0] << 24) | (buf[1] << 16) | (buf[2] << 8) | buf[3]

    if length == -1:
        return None

    return pgoid_function(buffer_object.read(length))


cdef list _flatten_list(list lst, list result):
    """Flatten nested list into flat list."""

    cdef object item

    for item in lst:
        if isinstance(item, list):
            _flatten_list(item, result)
        else:
            result.append(item)

    return result


cpdef list read_array(
    bytes binary_data,
    object pgoid_function,
    object buffer_object,
    long pgoid,
):
    """Unpack array values."""

    cdef:
        unsigned int num_dim, has_null, oid
        list array_struct = []
        list array_elements = []
        int i
        int total_elements

    if len(binary_data) == 0:
        return []

    buffer_object.write(binary_data)
    buffer_object.seek(0)
    header = buffer_object.read(12)

    if len(header) < 12:
        buffer_object.seek(0)
        buffer_object.truncate()
        return []

    num_dim, has_null, oid = unpack("!3I", header)

    for i in range(num_dim):
        dim_data = buffer_object.read(8)

        if len(dim_data) < 8:
            break

        array_struct.append(unpack("!2I", dim_data)[0])

    total_elements = prod(array_struct) if array_struct else 0

    for i in range(total_elements):
        array_elements.append(_reader(buffer_object, pgoid_function))

    buffer_object.seek(0)
    buffer_object.truncate()

    if not array_struct:
        return array_elements

    return recursive_elements(array_elements, array_struct)


cpdef bytes write_array(
    list dtype_value,
    object pgoid_function,
    object buffer_object,
    long pgoid,
):
    """Pack array values."""

    cdef:
        list num_dim = get_num_dim(dtype_value)
        int dim_length = len(num_dim)
        int has_null = 0
        list flat_values = []
        list dimensions = []
        int dim
        object value
        bytes binary_data
        int i

    if not dtype_value:
        buffer_object.write(pack("!3I", 1, 0, pgoid))
        buffer_object.write(pack("!2I", 0, 1))
        binary_data = buffer_object.getvalue()
        buffer_object.seek(0)
        buffer_object.truncate()
        return binary_data

    flat_values = _flatten_list(dtype_value, [])

    for value in flat_values:
        if value is None:
            has_null = 1
            break

    for dim in num_dim:
        dimensions.append(dim)
        dimensions.append(1)

    buffer_object.write(pack("!3I", dim_length, has_null, pgoid))
    buffer_object.write(pack("!%dI" % (dim_length * 2), *dimensions))

    for value in flat_values:
        if value is None:
            buffer_object.write(NULLABLE)
        else:
            binary_data = pgoid_function(value)
            buffer_object.write(pack("!I", len(binary_data)))
            buffer_object.write(binary_data)

    binary_data = buffer_object.getvalue()
    buffer_object.seek(0)
    buffer_object.truncate()
    return binary_data
