'''
Created on 2013-3-1

@author: yelu01
'''

from mysql.connector.utils import *
import datetime
import decimal

def read_signed_int(buf, size, big_endian = True):
    """Read a big endian integer values based on byte number
    
    Returns a tuple (truncated buffer, int)
    '''''
    """
    if big_endian:
        endian = '>'
    else:
        endian = '<'
    if size == 1:
        res = struct.unpack(endian+'b', buf)[0]
        return (buf[1:], res)
    elif size == 2:
        res = struct.unpack(endian+'h', buf)[0]
        return (buf[2:], res)
    elif size == 3:
        a, b, c = struct.unpack("BBB", buf)
        #TODO:may be wrong.
        if a & 128:
            res =  a + (b << 8) + (c << 16)
        else:
            res =  (a + (b << 8) + (c << 16)) * -1
        return (buf[3:], res)
    elif size == 4:
        res = struct.unpack(endian+'i', buf)[0]
        return (buf[4:], res)
    elif size == 8:
        res = struct.unpack(endian+'q', buf)[0]
        return (buf[8:], res)
    else:
        raise


def read_unsigned_int(buf, size, big_endian = False):
    """Read a little endian integer values based on byte number
    
    Returns a tuple (truncated buffer, int)
    """
    if big_endian:
        endian = '>'
    else:
        endian = '<'
    if size == 1:
        res = struct.unpack(endian+'B', buf)[0]
        return (buf[1:], res)
    elif size == 2:
        res = struct.unpack(endian+'H', buf)[0]
        return (buf[2:], res)
    elif size == 3:
        a, b, c = struct.unpack("BBB", buf)
        res =  a + (b << 8) + (c << 16)
        return (buf[3:], res)
    elif size == 4:
        res = struct.unpack(endian+'I', buf)[0]
        return (buf[4:], res)
    elif size == 8:
        res = struct.unpack(endian+'Q', buf)[0]
        return (buf[8:], res)
    else:
        raise

def read_float(buf):
    res = struct.unpack("<f", buf)[0]
    return (buf[4:], res)

def read_double(buf):
    res = struct.unpack("<d", buf)[0]
    return (buf[8:], res)

def read_lc_pascal_string(buf, size):
    '''Read a string with length coded using pascal style. The string start by the size of the string'''
    head, length = read_unsigned_int(buf, size)
    head, res = read_bytes(head, length)
    return (head, res)

def read_lc_pascal_string_decoded(buf, size, charset):
    '''Read a string with length coded using pascal style. The string start by the size of the string'''
    head, res = read_lc_pascal_string(buf, size)
    res = res.decode(charset)
    return (head, res)

def read_bits(head, bytes, bits):
    """Read MySQL BIT type"""
    resp = ""
    for byte in range(0, bytes):
        current_byte = ""
        head, data = read_unsigned_int(head, 1)
        if byte == 0:
            if bytes == 1:
                end = bits
            else:
                end = bits % 8
                if end == 0:
                    end = 8
        else:
            end = 8
        for bit in range(0, end):
            if data & (1 << bit):
                current_byte += "1"
            else:
                current_byte += "0"
        resp += current_byte[::-1]
    return (head, resp)

def read_datetime(head):
    head, value = read_unsigned_int(head, 8)
    date = value / 1000000
    time = value % 1000000
    date = datetime.datetime(
        year = int(date / 10000),
        month = int((date % 10000) / 100),
        day = int(date % 100),
        hour = int(time / 10000),
        minute = int((time % 10000) / 100),
        second = int(time % 100))
    return (head, date)

def read_time(head):
    head, time = read_unsigned_int(head, 3)
    date = datetime.time(
        hour = int(time / 10000),
        minute = int((time % 10000) / 100),
        second = int(time % 100))
    return (head, date)

def read_date(head):
    head, time = read_unsigned_int(head, 3)
    date = datetime.date(
        year = (time & ((1 << 15) - 1) << 9) >> 9,
        month = (time & ((1 << 4) - 1) << 5) >> 5,
        day = (time & ((1 << 5) - 1))
    )
    return (head, date)

def read_new_decimal(head, precision, decimals):
    '''Read MySQL's new decimal format introduced in MySQL 5'''
    
    # https://github.com/jeremycole/mysql_binlog/blob/master/lib/mysql_binlog/binlog_field_parser.rb

    digits_per_integer = 9
    compressed_bytes = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4]
    integral = (precision - decimals)
    uncomp_integral = int(integral / digits_per_integer)
    uncomp_fractional = int(decimals / digits_per_integer)
    comp_integral = integral - (uncomp_integral * digits_per_integer)
    comp_fractional = decimals - (uncomp_fractional * digits_per_integer)

    # Support negative
    # The sign is encoded in the high bit of the the byte
    # But this bit can also be used in the value
    value = struct.pack('B', head)[0]
    res, mask = ["", 0] if (value & 0x80 != 0) else ["-", -1]
    head[0] = value ^ 0x80

    size = compressed_bytes[comp_integral]

    if size > 0:
        head, value = read_unsigned_int(head, size, True) ^ mask
        res += str(value)

    for i in range(0, uncomp_integral):
        head, value = read_signed_int(head, 4, True)
        value = value ^ mask
        res += str(value)

    res += "."

    for i in range(0, uncomp_fractional):
        head, value = read_signed_int(head, 4, True)
        value = value ^ mask
        res += str(value)

    size = compressed_bytes[comp_fractional]
    if size > 0:
        head, value = read_signed_int(head, size, True)
        value = value ^ mask
        res += str(value)

    return decimal.Decimal(res)