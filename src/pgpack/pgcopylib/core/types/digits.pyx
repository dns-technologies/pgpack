from decimal import Decimal
from libc.math cimport round
from struct import (
    pack,
    unpack,
    unpack_from,
)


cpdef object read_bool(
    bytes binary_data,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Unpack bool value."""

    return unpack("!?", binary_data)[0]


cpdef bytes write_bool(
    object dtype_value,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Pack bool value."""

    return pack("!?", bool(dtype_value))


cpdef unsigned long read_oid(
    bytes binary_data,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Unpack oid value."""

    return unpack("!I", binary_data)[0]


cpdef bytes write_oid(
    object dtype_value,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Pack oid value."""

    cdef unsigned long int_value = <unsigned long>round(dtype_value)

    return pack("!I", int_value)


cpdef unsigned short read_serial2(
    bytes binary_data,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Unpack serial2 value."""

    return unpack("!H", binary_data)[0]


cpdef bytes write_serial2(
    object dtype_value,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Pack serial2 value."""

    cdef unsigned short int_value = <unsigned short>round(dtype_value)

    return pack("!H", int_value)


cpdef unsigned long read_serial4(
    bytes binary_data,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Unpack serial4 value."""

    return unpack("!L", binary_data)[0]


cpdef bytes write_serial4(
    object dtype_value,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Pack serial4 value."""

    cdef unsigned long int_value = <unsigned long>round(dtype_value)

    return pack("!L", int_value)


cpdef unsigned long long read_serial8(
    bytes binary_data,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Unpack serial8 value."""

    return unpack("!Q", binary_data)[0]


cpdef bytes write_serial8(
    object dtype_value,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Pack serial8 value."""

    cdef unsigned long long int_value = <unsigned long long>round(dtype_value)

    return pack("!Q", int_value)


cpdef short read_int2(
    bytes binary_data,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Unpack int2 value."""

    return unpack("!h", binary_data)[0]


cpdef bytes write_int2(
    object dtype_value,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Pack int2 value."""

    cdef short int_value = <short>round(dtype_value)

    return pack("!h", int_value)


cpdef long read_int4(
    bytes binary_data,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Unpack int4 value."""

    return unpack("!l", binary_data)[0]


cpdef bytes write_int4(
    object dtype_value,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Pack int4 value."""

    cdef long int_value = <long>round(dtype_value)

    return pack("!l", int_value)


cpdef long long read_int8(
    bytes binary_data,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Unpack int8 value."""

    return unpack("!q", binary_data)[0]


cpdef bytes write_int8(
    object dtype_value,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Pack int8 value."""

    cdef long long int_value = <long long>round(dtype_value)

    return pack("!q", int_value)


cpdef double read_money(
    bytes binary_data,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Unpack money value."""

    return read_int8(binary_data) * 0.01


cpdef bytes write_money(
    object dtype_value,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Pack money value."""

    cdef long long int_value = <long long>round(dtype_value / 0.01)

    return write_int8(int_value)


cpdef float read_float4(
    bytes binary_data,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Unpack float4 value."""

    return unpack("!f", binary_data)[0]


cpdef bytes write_float4(
    object dtype_value,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Pack float4 value."""

    return pack("!f", <float>dtype_value)


cpdef double read_float8(
    bytes binary_data,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Unpack float8 value."""

    return unpack("!d", binary_data)[0]


cpdef bytes write_float8(
    object dtype_value,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Pack float8 value."""

    return pack("!d", <double>dtype_value)


cpdef object read_numeric(
    bytes binary_data,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Unpack numeric value."""
    
    cdef:
        int ndigits, weight, sign, dscale
        int i, pos
        bint is_negative
        list digits = []
        object result
        short digit

    if len(binary_data) < 8:
        return Decimal(0)

    ndigits, weight, sign, dscale = unpack_from("!hhhh", binary_data)

    if sign == 0xc000:
        return Decimal("NaN")

    is_negative = (sign == 0x4000)

    for i in range(8, 8 + ndigits * 2, 2):
        if i + 1 < len(binary_data):
            digit = unpack_from("!h", binary_data, i)[0]
            digits.append(digit)

    result_str = ""
    if is_negative:
        result_str += "-"

    if weight >= 0:
        for j in range(weight + 1):
            if j < len(digits):
                result_str += str(digits[j])
            else:
                result_str += "0"
    else:
        result_str += "0"

    if dscale > 0 or len(digits) > weight + 1:
        result_str += "."
        for j in range(weight + 1, len(digits)):
            result_str += (
                str(digits[j]).zfill(4)
                if j > weight + 1
                else str(digits[j])
            )

        current_dscale = len(
            result_str.split(".")[1]
        ) if "." in result_str else 0

        if current_dscale < dscale:
            result_str += "0" * (dscale - current_dscale)

    return Decimal(result_str)


cpdef bytes write_numeric(
    object dtype_value,
    object pgoid_function = None,
    object buffer_object = None,
    object pgoid = None,
):
    """Pack numeric value."""
    
    cdef:
        bint is_negative
        int sign, dscale, ndigits, weight
        int digit
        object abs_value
        list digits = []
        str value_str
        str int_part, frac_part

    dtype_value = Decimal(dtype_value)

    if dtype_value.is_nan():
        return pack("!hhhh", 0, 0, 0xc000, 0)

    is_negative = dtype_value < 0
    sign = 0x4000 if is_negative else 0x0000
    abs_value = abs(dtype_value)
    value_str = format(abs_value, "f")

    if "." in value_str:
        int_part, frac_part = value_str.split(".")
    else:
        int_part, frac_part = value_str, ""

    dscale = len(frac_part)
    int_part = int_part.lstrip("0") or "0"
    int_digits = []

    for i in range(0, len(int_part), 4):
        group = int_part[i:i+4]
        int_digits.append(int(group))

    frac_digits = []

    for i in range(0, len(frac_part), 4):
        group = frac_part[i:i+4].ljust(4, "0")
        frac_digits.append(int(group))

    all_digits = int_digits + frac_digits
    ndigits = len(all_digits)
    weight = len(int_digits) - 1 if int_digits else -1
    header = pack("!hhhh", ndigits, weight, sign, dscale)
    digit_bytes = b""

    for digit in all_digits:
        digit_bytes += pack("!h", digit)

    return header + digit_bytes
