#!/usr/bin/env python
#coding:utf-8

from mysql.connector import utils
from tools import log
from tools import get_trace_info
from constants import EventType
from mysql.connector.constants import FieldType
from mysql.connector.conversion import MySQLConverter
import json
import time
import struct


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
    
    def __init__(self, packet):
        super(TableMapEvent, self).__init__(packet)
        self._payload = packet[24:]
        
        # header
        head = self._payload
        head, self.table_id = utils.read_int(head, 6)
        head, self.flags = utils.read_int(head, 2)
        head, db_name_len = utils.read_int(head, 1)
        head, self.db = utils.read_bytes(head, db_name_len)
        self.db = str(self.db)
        head, _ = utils.read_bytes(head, 1) #filler
        head, table_name_len = utils.read_int(head, 1)
        head, self.table = utils.read_bytes(head, table_name_len)
        self.table = str(self.table)
        head, _ = utils.read_bytes(head, 1)
        head, self.columns_cnt = utils.read_lc_int(head)
        head, columns_type = utils.read_bytes(head, self.columns_cnt)
        self.columns_type = []
        for i in range(0, self.columns_cnt):
            self.columns_type.append(ord(columns_type[i]))
        head, self.column_schema = utils.read_lc_string(head)
    
    @property
    def table_map(self):
        table_map = {"table_id":self.table_id, \
                     "db":self.db, \
                     "table":self.table, \
                     "columns_cnt":self.columns_cnt, \
                     "columns_type":self.columns_type
                     }
        #"columns_schema":self.column_schema
        print self.column_schema
        return table_map
    
    def __str__(self):
        return json.dumps(self.table_map)
        

class RowsEvent(BinlogEvent):
    
    def __init__(self, packet, table_map, column_map):
        super(RowsEvent, self).__init__(packet)
        self._payload = packet[23:]
        
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
        
    def _read_columns(self, buf, null_bitmap, columns_cnt, columns_info):
        columns = []
        head = buf
        for i in range(0, columns_cnt):
            column = None
            type = columns_info["columns_info"][columns_info["pos_map"][i+1]][1]
            is_null = True if (null_bitmap>>i)&0x01 else False
            if is_null:
                column = None
            elif type == FieldType.TINY:
                head, column = utils.read_int(head, 1)
            elif type == FieldType.SHORT:
                head, column = utils.read_int(head, 2)
            elif type == FieldType.LONG:
                head, column = utils.read_int(head, 4)
            elif type == FieldType.INT24:
                head, column = utils.read_int(head, 3)
            elif type == FieldType.FLOAT:
                head, column = utils.read_bytes(head, 4)
                column = struct.unpack('f', column)
            elif type == FieldType.DOUBLE:
                head, column = utils.read_bytes(head, 8)
                column = struct.unpack('d', column)
            elif type == FieldType.VARCHAR or type == FieldType.STRING:
                head, column = utils.read_lc_string(head)
            elif column.type == FieldType.NEWDECIMAL:
                #head, cloumn = utils.
                values[name] = self.__read_new_decimal(column)
            elif column.type == FieldType.BLOB:
                values[name] = self.__read_string(column.length_size, column)
            elif column.type == FieldType.DATETIME:
                values[name] = self.__read_datetime()
            elif column.type == FIELD_TYPE.TIME:
                values[name] = self.__read_time()
            elif column.type == FIELD_TYPE.DATE:
                values[name] = self.__read_date()
            elif column.type == FIELD_TYPE.TIMESTAMP:
                values[name] = datetime.datetime.fromtimestamp(self.packet.read_uint32())
            elif column.type == FIELD_TYPE.LONGLONG:
                if unsigned:
                    values[name] = self.packet.read_uint64()
                else:
                    values[name] = self.packet.read_int64()
            elif column.type == FIELD_TYPE.YEAR:
                values[name] = self.packet.read_uint8() + 1900
            elif column.type == FIELD_TYPE.ENUM:
                values[name] = column.enum_values[self.packet.read_uint_by_size(column.size) - 1]
            elif column.type == FIELD_TYPE.SET:
                values[name] = column.set_values[self.packet.read_uint_by_size(column.size) - 1]
            elif column.type == FIELD_TYPE.BIT:
                values[name] = self.__read_bit(column)
            elif column.type == FIELD_TYPE.GEOMETRY:
                values[name] = self.packet.read_length_coded_pascal_string(column.length_size)
            else:
                raise NotImplementedError("Unknown MySQL column type: %d" % (column.type))
                MySQLConverter.to_python(self, vtype, value)
        return values
        pass


"""
type == FieldType.TINY:
                head, column = utils.read_int(head, 1)
            elif type == FieldType.SHORT:
                head, column = utils.read_int(head, 2)
            elif type == FieldType.LONG:
                head, column = utils.read_int(head, 4)
            elif type == FieldType.INT24:
                head, column = utils.read_int(head, 3)
            elif type == FieldType.FLOAT:
                head, column = utils.read_bytes(head, 4)
                column = struct.unpack('f', column)
            elif type == FieldType.DOUBLE:
                head, column = utils.read_bytes(head, 8)
                column = struct.unpack('d', column)
            elif type == FieldType.VARCHAR or type == FieldType.STRING:
                head, column = utils.read_lc_string(head)
            elif column.type == FieldType.NEWDECIMAL:
                #head, cloumn = utils.
                values[name] = self.__read_new_decimal(column)
            elif column.type == FieldType.BLOB:
                values[name] = self.__read_string(column.length_size, column)
            elif column.type == FieldType.DATETIME:
                values[name] = self.__read_datetime()
            elif column.type == FIELD_TYPE.TIME:
                values[name] = self.__read_time()
            elif column.type == FIELD_TYPE.DATE:
                values[name] = self.__read_date()
            elif column.type == FIELD_TYPE.TIMESTAMP:
                values[name] = datetime.datetime.fromtimestamp(self.packet.read_uint32())
            elif column.type == FIELD_TYPE.LONGLONG:
                if unsigned:
                    values[name] = self.packet.read_uint64()
                else:
                    values[name] = self.packet.read_int64()
            elif column.type == FIELD_TYPE.YEAR:
                values[name] = self.packet.read_uint8() + 1900
            elif column.type == FIELD_TYPE.ENUM:
                values[name] = column.enum_values[self.packet.read_uint_by_size(column.size) - 1]
            elif column.type == FIELD_TYPE.SET:
                values[name] = column.set_values[self.packet.read_uint_by_size(column.size) - 1]
            elif column.type == FIELD_TYPE.BIT:
                values[name] = self.__read_bit(column)
            elif column.type == FIELD_TYPE.GEOMETRY:
                values[name] = self.packet.read_length_coded_pascal_string(column.length_size)
            else:
                raise NotImplementedError("Unknown MySQL column type: %d" % (column.type))
"""


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