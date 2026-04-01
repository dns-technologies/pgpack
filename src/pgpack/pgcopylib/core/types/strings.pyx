cpdef str read_text(
    bytes binary_data,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Unpack text value."""

    return binary_data.decode("utf-8", errors="replace")


cpdef bytes write_text(
    object dtype_value,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Pack text value."""

    if not dtype_value.__class__ is str:
        dtype_value = str(dtype_value)

    cdef str string_value = dtype_value.replace("\x00", "")
    return string_value.encode("utf-8", errors="replace")


cpdef str read_macaddr(
    bytes binary_data,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Unpack macaddr and macaddr8 value."""

    cdef long i
    cdef long data_len = len(binary_data)
    cdef const unsigned char[:] view = binary_data
    cdef list parts = [None] * data_len

    for i in range(data_len):
        parts[i] = f"{view[i]:02x}"

    return ":".join(parts).lower()


cpdef bytes write_macaddr(
    str dtype_value,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Pack macaddr and macaddr8 value."""

    return bytes.fromhex(dtype_value.replace(":", ""))


cpdef str read_bits(
    bytes binary_data,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Unpack bit and varbit value."""

    cdef unsigned int length
    cdef long data_len = len(binary_data)
    cdef long bit_data_len
    cdef list bits = []
    cdef short i, j
    cdef unsigned char byte_val

    if data_len < 4:
        return ""

    length = (
        (binary_data[0] << 24) |
        (binary_data[1] << 16) |
        (binary_data[2] << 8) |
        binary_data[3]
    )
    
    if data_len < 4 + (length + 7) // 8:
        return ""

    for i in range(4, data_len):
        byte_val = binary_data[i]
        bits_left = length - (i - 4) * 8

        if bits_left <= 0:
            break

        bits_to_read = min(8, bits_left)

        for j in range(7, 7 - bits_to_read, -1):
            bits.append(str((byte_val >> j) & 1))

    return "".join(bits)


cpdef bytes write_bits(
    str dtype_value,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Pack bit and varbit value."""
    
    cdef unsigned int length = len(dtype_value)
    cdef unsigned int byte_length = (length + 7) // 8
    cdef bytes result
    cdef int i, j
    cdef unsigned char byte_val
    cdef list bytes_list = []

    bytes_list.append((length >> 24) & 0xFF)
    bytes_list.append((length >> 16) & 0xFF)
    bytes_list.append((length >> 8) & 0xFF)
    bytes_list.append(length & 0xFF)

    for i in range(byte_length):
        byte_val = 0
        for j in range(8):
            bit_pos = i * 8 + j
            if bit_pos < length and dtype_value[bit_pos] == "1":
                byte_val |= (1 << (7 - j))
        bytes_list.append(byte_val)

    return bytes(bytes_list)


cpdef bytes read_bytea(
    bytes binary_data,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Unpack bytea value."""

    return binary_data


cpdef bytes write_bytea(
    bytes dtype_value,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Pack bytea value."""

    return dtype_value
