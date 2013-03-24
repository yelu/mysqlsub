#!/usr/bin/env python
#coding:utf-8

from mysql.connector import utils
from mysql.connector.constants import FieldType
import json

class Column(object):
    '''Definition of a column'''

    def __init__(self, column_type, column_schema, buf):
        self.type = column_type
        self.name = column_schema["COLUMN_NAME"]
        self.collation_name = column_schema["COLLATION_NAME"]
        self.character_set_name = column_schema["CHARACTER_SET_NAME"]
        self.comment = column_schema["COLUMN_COMMENT"]
        self.unsigned = False
        head = buf

        if column_schema["COLUMN_TYPE"].find("unsigned") != -1:
            self.unsigned = True
        if self.type == FieldType.VAR_STRING or self.type == FieldType.STRING:
            self.__read_string_metadata(packet, column_schema)
        elif self.type == FieldType.VARCHAR:
            head, self.max_length = utils.read_int(head, 2)
        elif self.type == FieldType.BLOB:
            head, self.length_size = utils.read_int(head, 1)
        elif self.type == FieldType.GEOMETRY:
            head, self.length_size = utils.read_int(head, 1)
        elif self.type == FieldType.NEWDECIMAL:
            head, self.precision = utils.read_int(head, 1)
            head, self.decimals = utils.read_int(head, 1)
        elif self.type == FieldType.DOUBLE:
            head, self.size = utils.read_int(head, 1)
        elif self.type == FieldType.FLOAT:
            head, self.size = utils.read_int(head, 1)
        elif self.type == FieldType.BIT:
            head, bits = utils.read_int(head, 1)
            head, bytes = utils.read_int(head, 1)
            self.bits = (bytes * 8) + bits
            self.bytes = int((self.bits + 7) / 8)

    def __read_string_metadata(self, head, column_schema):
        head, byte0 = utils.read_int(head, 1)
        head, byte1 = utils.read_int(head, 1)
        metadata  = (byte0 << 8) + byte1
        real_type = metadata >> 8
        if real_type == FieldType.SET or real_type == FieldType.ENUM:
            self.type = real_type
            self.size = metadata & 0x00ff
            self.__read_enum_metadata(column_schema)
        else:
            self.max_length = (((metadata >> 4) & 0x300) ^ 0x300) + (metadata & 0x00ff)

    def __read_enum_metadata(self, column_schema):
        enums = column_schema["COLUMN_TYPE"]
        if self.type == FieldType.ENUM:
            self.enum_values = enums.replace('enum(', '').replace(')', '').replace('\'', '').split(',')
        else:
            self.set_values = enums.replace('set(', '').replace(')', '').replace('\'', '').split(',')
