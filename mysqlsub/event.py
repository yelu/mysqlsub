#!/usr/bin/env python
#coding:utf-8

from tools import log
from tools import get_trace_info
from constants import EventType
from mysql.connector.constants import FieldType
from mysql.connector.conversion import MySQLConverter
import utils 
import json
import time
import struct
import datetime


"""
Binlog::EventHeader.
+============================================+
| event_header     |  timestamp         0 : 4 |
|                  +------------------------―+
|                  |  event_type        4 : 1 |
|                  +-------------------------+
|                  |  server_id         5 : 4 |
|                  |  event_size        9 : 4 |
|                  |  log_pos          13 : 4 |
|                  |  flags            17 : 2 |
+============================================+
more details refer to 
http://dev.mysql.com/doc/internals/en/replication-protocol.html#binlog-event-header
"""

class EventHeader(object):
    def __init__(self, buf):
        self.timestamp = utils.read_int(buf, 4)[1]
        self.event_type = utils.read_int(buf[4:], 1)[1]
        self.server_id = utils.read_int(buf[5:], 4)[1]
        self.event_size = utils.read_int(buf[9:], 4)[1]
        self.log_pos = utils.read_int(buf[13:], 4)[1]
        self.flags = utils.read_int(buf[17:], 2)[1]
        
    def __str__(self):
        res = {}
        res["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S", \
                                         time.localtime(self.timestamp))
        res["event_type"] = "0x%.2x"%self.event_type
        res["server_id"] = self.server_id
        res["event_size"] = self.event_size
        res["log_pos"] = self.log_pos
        res["flags"] = "0x%.4x"%self.flags
        return json.dumps(res)

"""
Binlog packet.
+=============================================+
| packet header    |  packet length     0 : 3 |   
|                  +-------------------------+   
|                  |  sequence number   3 : 1 |
+============================================+
| OK/ERR/EOF       |                    4 : 1 |
| event_header     |                    5 : X |
+============================================+
"""

class BinlogEvent(object):
    def __init__(self, packet):
        self._body = packet[4:]
        self.header = None
        try:
            self.header = EventHeader(self._body[1:20])
        except:
            msg = get_trace_info()
            log.warning(msg)
        
    
    def is_eof(self):
        header = utils.read_int(self._body, 1)[1]
        if 0xfe == header:
            log.debug("received eof packet.")
            return True
        else:
            return False
    
    def is_error(self):
        header = utils.read_int(self._body, 1)[1]
        if 0xff == header:
            log.debug("received err packet.")
            return True
        else:
            return False
     
    def type(self):
        return self.header.event_type
    
    def __str__(self):
        return self.header.__str__()


class TableMapEvent(BinlogEvent):
    
    def __init__(self, packet, table_map, table_subscribed, ctl_conn):
        super(TableMapEvent, self).__init__(packet)
        self._payload = packet[24:]
        self._ctl_conn = ctl_conn
        head = self._payload
        
        head, self.table_id = utils.read_int(head, 6)
        # add or modify table map.
        if self.table_id not in table_map:
            table_map[self.table_id] = {"schema":None, "table":None, "column_schemas":[]}           
        if self.schema in table_subscribed and self.table in table_subscribed[self.schema]:
            table_map[self.table_id]["column_schemas"] = \
               self.__get_table_informations(self.schema, self.table)
               
        head, self.flags = utils.read_int(head, 2)
        head, schema_name_len = utils.read_int(head, 1)
        head, self.schema = utils.read_bytes(head, schema_name_len)
        self.schema = str(self.schema)
        table_map[self.table_id]["schema"] = self.schema
        head, _ = utils.read_bytes(head, 1) #filler
        head, table_name_len = utils.read_int(head, 1)
        head, self.table = utils.read_bytes(head, table_name_len)
        self.table = str(self.table)
        table_map[self.table_id]["table"] = self.table
        head, _ = utils.read_bytes(head, 1) #filler
        head, self.columns_cnt = utils.read_lc_int(head)
        head, column_types = utils.read_bytes(head, self.columns_cnt)
        for i in range(0, self.columns_cnt):
            schema = table_map[self.table_id]["column_schemas"][i]
            t = ord(column_types[i])
            schema["TYPE_ID"] = t
            head, _ = self.__read_metadata(t, schema, head)
            
    def __get_table_informations(self, schema, table):
        sql = '''SELECT * FROM information_schema.columns WHERE table_schema="%s" AND table_name="%s";''', (schema, table)
        res, _ = self._ctl_conn.query(sql)
        return res
    
    def __read_metadata(self, column_type, column_schema, head):
        column_schema["REAL_TYPE"] = column_schema["TYPE_ID"]
        if column_schema["COLUMN_TYPE"].find("unsigned") != -1:
            column_schema["IS_UNSIGNED"] = True
        else:
            column_schema["IS_UNSIGNED"] = False
        if column_type == FieldType.VAR_STRING or column_type == FieldType.STRING:
            head, _ = self.__read_string_metadata(column_schema, head)
        elif column_type == FieldType.VARCHAR:
            head, column_schema["MAX_LENGTH"] = utils.read_unsigned_int(head, 2)
        elif column_type == FieldType.BLOB:
            head, column_schema["LENGTH_SIZE"] = utils.read_unsigned_int(head, 1)
        elif column_type == FieldType.GEOMETRY:
            head, column_schema["LENGTH_SIZE"] = utils.read_unsigned_int(head, 1)
        elif column_type == FieldType.NEWDECIMAL:
            head, column_schema["PRECISION"] = utils.read_unsigned_int(head, 1)
            head, column_schema["DECIMALS"] = utils.read_unsigned_int(head, 1)
        elif column_type == FieldType.DOUBLE:
            head, column_schema["SIZE"] = utils.read_unsigned_int(head, 1)
        elif column_type == FieldType.FLOAT:
            head, column_schema["SIZE"] = utils.read_unsigned_int(head, 1)
        elif column_type == FieldType.BIT:
            head, bit = utils.read_unsigned_int(head, 1)
            head, byte = utils.read_unsigned_int(head, 1)
            column_schema["BITS"] = (byte * 8) + bit
            column_schema["BYTES"] = int((bit + 7) / 8)
        return (head, None)
 
    def __read_string_metadata(self, column_schema, head):
        head, byte0 = utils.read_unsigned_int(head, 1)
        head, byte1 = utils.read_unsigned_int(head, 1)
        metadata  = (byte0 << 8) + byte1
        real_type = metadata >> 8
        if real_type == FieldType.SET or real_type == FieldType.ENUM:
            column_schema["TYPE_ID"] = real_type
            column_schema["SIZE"] = metadata & 0x00ff
            self.__read_enum_metadata(column_schema, real_type)
        else:
            column_schema["MAX_LENGTH"] = (((metadata >> 4) & 0x300) ^ 0x300) + (metadata & 0x00ff)
        return (head, None)
    
    def __parse_enum_metadata(self, column_schema, column_type):
        enums = column_schema["TYPE_ID"]
        if column_type == FieldType.ENUM:
            column_schema["ENUM_VALUES"] = enums.replace('enum(', '').replace(')', '').replace('\'', '').split(',')
        else:
            column_schema["SET_VALUES"] = enums.replace('set(', '').replace(')', '').replace('\'', '').split(',')   

    @property
    def table_map(self):
        table_map = {"table_id":self.table_id, \
                     "schema":self.schema, \
                     "table":self.table, \
                     "column_cnt":self.columns_cnt, \
                     "column_types":self.columns_type
                     }
        return table_map
    
    def __str__(self):
        return json.dumps(self.table_map)
        

class RowsEvent(BinlogEvent):
    
    def __init__(self, packet, table_map, table_subscribed):
        super(RowsEvent, self).__init__(packet)
        self._payload = packet[23:]
        self._table_map = table_map
        self._table_subscribed = table_subscribed
        
        head = self._payload
        # header
        head, self.table_id = utils.read_int(head, 6)
        head, self.flags = utils.read_int(head, 2)
        # with MySQL 5.6.x there will be other data following.
        
        # body
        head, self.number_of_columns = utils.read_lc_int(head)
        columns_present_bitmap_len = (self.number_of_columns + 7) / 8
        head, columns_present_bitmap1 = utils.read_int(head, 
                                        columns_present_bitmap_len)
        if self.header.event_type == EventType.UPDATE_ROWS_EVENT:
            head, columns_present_bitmap2 = utils.read_int(head, 
                                            columns_present_bitmap_len)
        # read rows.
        null_bitmap_len = (self.number_of_columns + 7) / 8;
        head, null_bitmap = utils.read_int(head, null_bitmap_len)
        row = {}
        for i in range(self.number_of_columns):
            is_null = True if ((null_bitmap[i/8] >> (i%8)) & 0x01) else False
            
        
        #self.columns = self.table_map[self.table_id].columns

        #Aditionnal informations
        #self.schema = self.table_map[self.table_id].schema
        #self.table = self.table_map[self.table_id].table
        
    def __read_columns(self, head, null_bitmap, column_schemas):
        columns = []
        for i in xrange(0, len(columns)):
            schema = column_schemas[i]
            type = schema["REAL_TYPE"]
            column = {"type":type, "value":None}
            null = True if (null_bitmap[i/8]>>(i%8))&0x01 else False
            unsigned = column_schemas[i]["IS_UNSIGNED"]
            if null:
                column["value"] = None
            elif type == FieldType.TINY:
                if unsigned:
                    head, columns["value"] = utils.read_unsigned_int(head, 1, False)
                else:
                    head, columns["value"] = utils.read_signed_int(head, 1, False)
            elif type == FieldType.SHORT:
                if unsigned:
                    head, columns["value"] = utils.read_unsigned_int(head, 2, False)
                else:
                    head, columns["value"] = utils.read_signed_int(head, 2, False)
            elif type == FieldType.LONG:
                if unsigned:
                    head, columns["value"] = utils.read_unsigned_int(head, 4, False)
                else:
                    head, columns["value"] = utils.read_signed_int(head, 4, False)
            elif type == FieldType.INT24:
                if unsigned:
                    head, columns["value"] = utils.read_unsigned_int(head, 3, False)
                else:
                    head, columns["value"] = utils.read_signed_int(head, 3, False)
            elif type == FieldType.FLOAT:
                head, columns["value"] = utils.read_float(head)
            elif type == FieldType.DOUBLE:
                head, columns["value"] = utils.read_double(head)
            elif type == FieldType.VARCHAR or column.type == FieldType.STRING:
                if schema["MAX_LENGTH"] > 255:
                    head, columns["value"] = utils.read_lc_pascal_string(head, 2)
                else:
                    head, columns["value"] = utils.read_lc_pascal_string(head, 1)
            elif type == FieldType.NEWDECIMAL:
                head, columns["value"] = self.__read_new_decimal(column)
            elif type == FieldType.BLOB:
                length_size = schema["LENGTH_SIZE"]
                charset = schema["CHARACTER_SET_NAME"]
                head, columns["value"] = utils.read_lc_pascal_string(head, length_size, charset)
            elif type == FieldType.DATETIME:
                head, columns["value"] = utils.read_datetime(head)
            elif type == FieldType.TIME:
                head, columns["value"] = utils.read_time(head)
            elif type == FieldType.DATE:
                head, columns["value"] = utils.read_date(head)
            elif type == FieldType.TIMESTAMP:
                head, timestamp = utils.read_unsigned_int(head, 4)
                columns["value"] = datetime.datetime.fromtimestamp(timestamp)
            elif type == FieldType.LONGLONG:
                if unsigned:
                    head, columns["value"] = utils.read_unsigned_int(head, 8, False)
                else:
                    head, columns["value"] = utils.read_signed_int(head, 8, False)
            elif type == FieldType.YEAR:
                head, year = utils.read_unsigned_int(head, 1, False)
                columns["value"] = year + 1900
            elif type == FieldType.ENUM:
                size = schema["SIZE"]
                head, index = utils.read_unsigned_int(head, size, False) - 1
                columns["value"] = schema["ENUM_VALUES"][index]
            elif type == FieldType.SET:
                size = schema["SIZE"]
                head, index = utils.read_unsigned_int(head, size, False) - 1
                columns["value"] = schema["SET_VALUES"][index]
            elif type == FieldType.BIT:
                bytes = schema["BYTES"]
                bits = schema["BITS"]
                head, columns["value"] = utils.read_bits(head, bytes, bits)
            elif type == FieldType.GEOMETRY:
                length_size = schema["LENGTH_SIZE"]
                head, columns["value"] = utils.read_lc_pascal_string(head, length_size)
            else:
                raise NotImplementedError("Unknown MySQL column type: %d" % (type))
            columns.append(column)
        return columns

"""  
class RotateEvent(BinLogEvent):
    pass


class FormatDescriptionEvent(BinLogEvent):
    pass


class XidEvent(BinLogEvent):

    def __init__(self, from_packet, event_size, table_map, ctl_connection):
        super(XidEvent, self).__init__(from_packet, event_size, table_map, ctl_connection)
        self.xid = struct.unpack('<Q', self.packet.read(8))[0]

    def _dump(self):
        super(XidEvent, self)._dump()
        print("Transaction ID: %d" % (self.xid))


class QueryEvent(BinLogEvent):
    def __init__(self, from_packet, event_size, table_map, ctl_connection):
        super(QueryEvent, self).__init__(from_packet, event_size, table_map, ctl_connection)

        # Post-header
        self.slave_proxy_id = self.packet.read_uint32()
        self.execution_time = self.packet.read_uint32()
        self.schema_length =  byte2int(self.packet.read(1))
        self.error_code = self.packet.read_uint16()
        self.status_vars_length = self.packet.read_uint16()

        # Payload
        self.status_vars = self.packet.read(self.status_vars_length)
        self.schema =  self.packet.read(self.schema_length)
        self.packet.advance(1)

        self.query = self.packet.read(event_size - 13 - self.status_vars_length - self.schema_length - 1).decode()
        #string[EOF]    query

    def _dump(self):
        super(QueryEvent, self)._dump()
        print("Schema: %s" % (self.schema))
        print("Execution time: %d" % (self.execution_time)) 
        print("Query: %s" % (self.query))
"""


class EventMap:
    map = {
    EventType.TABLE_MAP_EVENT : TableMapEvent
    }
    
    def get_event_type(self, t):
        if t in self.__class__.map:
            return self.__class__.map[t]
        else:
            return None
