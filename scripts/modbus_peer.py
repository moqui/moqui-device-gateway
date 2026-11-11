#!/usr/bin/env python3
# This software is in the public domain under CC0 1.0 Universal plus a
# Grant of Patent License.
# 
# To the extent possible under law, the author(s) have dedicated all
# copyright and related and neighboring rights to this software to the
# public domain worldwide. This software is distributed without any
# warranty.
# 
# You should have received a copy of the CC0 Public Domain Dedication
# along with this software (see the LICENSE.md file). If not, see
# <http://creativecommons.org/publicdomain/zero/1.0/>.

"""Deterministic Modbus TCP peer for protocol and independent-host acceptance, not a PLC simulator."""
import socketserver
import struct
import threading

class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        while True:
            header = self.rfile.read(7)
            if not header: return
            if len(header) != 7: return
            transaction,protocol,length,unit = struct.unpack(">HHHB",header)
            pdu=self.rfile.read(length-1)
            if not pdu: return
            function=pdu[0]
            try:
                with self.server.lock:
                    if function == 3:
                        address,count=struct.unpack(">HH",pdu[1:5])
                        values=self.server.registers[address:address+count]
                        if len(values) != count: raise ValueError()
                        response=bytes([3,count*2])+struct.pack(">"+"H"*count,*values)
                    elif function == 6:
                        address,value=struct.unpack(">HH",pdu[1:5]); self.server.registers[address]=value; response=pdu
                    elif function == 16:
                        address,count,size=struct.unpack(">HHB",pdu[1:6])
                        values=struct.unpack(">"+"H"*count,pdu[6:])
                        self.server.registers[address:address+count]=values
                        response=bytes([16])+struct.pack(">HH",address,count)
                    else: response=bytes([function|128,1])
            except (ValueError,IndexError,struct.error): response=bytes([function|128,2])
            self.wfile.write(struct.pack(">HHHB",transaction,protocol,len(response)+1,unit)+response)
            self.wfile.flush()

class Server(socketserver.ThreadingTCPServer):
    daemon_threads=True
    allow_reuse_address=True
    def __init__(self):
        super().__init__(("127.0.0.1",0),Handler)
        self.registers=[42]*65536
        self.lock=threading.Lock()

if __name__ == "__main__":
    with Server() as server:
        print(server.server_address[1],flush=True)
        server.serve_forever()

