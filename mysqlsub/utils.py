'''
Created on 2013-3-1

@author: yelu01
'''

from mysql.connector.utils import *

def read_signed_int(buf, size):
    """Read an unsigned integer from buffer
    
    Returns a tuple (truncated buffer, int)
    """
    try:
        str = buf[0:size]
        if size == 1:
            res = struct.unpack('<b', str)[0]
        elif size <= 4:
            tmp = str + '\x00'*(4-size)
            res = struct.unpack('<i', tmp)[0]
        else:
            tmp = str + '\x00'*(8-size)
            res = struct.unpack('<q', tmp)[0]
    except:
        raise
    return (buf[size:], res)

def read_int24(buf):
    a, b, c = struct.unpack("BBB", buf)
    res = 0
    if a & 128:
        res =  a + (b << 8) + (c << 16)
    else:
        res =  (a + (b << 8) + (c << 16)) * -1
    return (buf[3:], res)

def read_uint24(buf):
    a, b, c = struct.unpack("BBB", buf)
    res =  a + (b << 8) + (c << 16)
    return (buf[3:], res)

def read_float(buf):
    res = struct.unpack("<f", buf)[0]
    return (buf[4:], res)

def read_double(buf):
    res = struct.unpack("<d", buf)[0]
    return (buf[8:], res)

def read_lc_pascal_string(buf, size):
    '''Read a string with length coded using pascal style. The string start by the size of the string'''
    head, length = read_int(buf, size)
    head, str = read_bytes(head, length)
    return (head, str)

def __read_new_decimal(self, column):
    '''Read MySQL's new decimal format introduced in MySQL 5'''
    
    # This project was a great source of inspiration for
    # understanding this storage format.
    # https://github.com/jeremycole/mysql_binlog

    digits_per_integer = 9
    compressed_bytes = [0, 1, 1, 2, 2, 3, 3, 4, 4, 4]
    integral = (column.precision - column.decimals)
    uncomp_integral = int(integral / digits_per_integer)
    uncomp_fractional = int(column.decimals / digits_per_integer)
    comp_integral = integral - (uncomp_integral * digits_per_integer)
    comp_fractional = column.decimals - (uncomp_fractional * digits_per_integer)

    # Support negative
    # The sign is encoded in the high bit of the the byte
    # But this bit can also be used in the value
    value = self.packet.read_uint8()
    if value & 0x80 != 0:
        res = ""
        mask = 0
    else:
        mask = -1
        res = "-"
    self.packet.unread(struct.pack('<B', value ^ 0x80))


    size = compressed_bytes[comp_integral]

    if size > 0:
        value = self.packet.read_int_be_by_size(size) ^ mask 
        res += str(value)

    for i in range(0, uncomp_integral):
        value = struct.unpack('>i', self.packet.read(4))[0] ^ mask
        res += str(value)

    res += "."

    for i in range(0, uncomp_fractional):
        value = struct.unpack('>i', self.packet.read(4))[0] ^ mask
        res += str(value)

    size = compressed_bytes[comp_fractional]
    if size > 0:
        value = self.packet.read_int_be_by_size(size) ^ mask
        res += str(value)

    return decimal.Decimal(res)