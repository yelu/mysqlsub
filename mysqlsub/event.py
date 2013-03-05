#!/usr/bin/env python
#coding:utf-8

from mysql.connector import utils
from tools import log
from tools import get_trace_info
from constants import EventType

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
        import time
        t = time.localtime(self.timestamp)
        t_str = time.strftime("%Y-%m-%d %H:%M:%S", t)
        res = "[timestamp:%s][event_type:0x%.2x][server_id:%u]" \
                  "[event_size:%u][log_pos:%u][flags:0x%.4x]" % \
                  (t_str, \
                  self.event_type, \
                  self.server_id, \
                  self.event_size, \
                  self.log_pos, \
                  self.flags)
        return res

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
            self.header = EventHeader(self._body[1:21])
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
        self._payload = packet[23:]
        
        # header
        self._table_map = {}
        head = self._payload
        head, self.table_id = utils.read_int(head, 6)
        head, self.flags = utils.read_int(head, 2)
        head, db_name_len = utils.read_int(head, 1)
        head, self._table_map["db"] = utils.read_bytes(head, db_name_len) + "0x00"
        head, _ = utils.read_bytes(head, 1) #filler
        head, table_name_len = utils.read_int(head, 1)
        head, self._table_map["table"] = utils.read_bytes(head, table_name_len) + "0x00"
        head, _ = utils.read_bytes(head, 1)
        head, cols_cnt = utils.read_lc_string(head)
    
    @property
    def table_map(self):
        pass
        


class RowsEvent(BinlogEvent):
    
    def __init__(self, packet):
        super(RowsEvent, self).__init__(packet)
        self._payload = packet[23:]
        
        head = self._payload
        # header
        head, self.table_id = utils.read_int(head, 6)
        head, self.flags = utils.read_int(head, 2)
        # with MySQL 5.6.x there will be other data.
        
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
            is_null = True if ((null_bitmap[i/8] >> (i%8)) & 0x01) else False;
            
        
        #self.columns = self.table_map[self.table_id].columns

        #Aditionnal informations
        #self.schema = self.table_map[self.table_id].schema
        #self.table = self.table_map[self.table_id].table

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