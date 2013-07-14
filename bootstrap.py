# -*- coding: utf-8 -*-

import re
import types
import time
import struct
import socket
import threading

import dht

RE_NETWORK_ID = re.compile('^[a-zA-Z]+[a-zA-Z0-9]+$')

BROADCAST_PORT = 9199
BROADCAST_SOCKET_TIMEOUT = 0.25
BROADCAST_TELL_TIMEOUT = 1
BROADCAST_MAGIC = 0x915C
BROADCAST_HDR = "!HH" # (magic, cmd)
BROADCAST_TELL_HDR = "!20sI" # (node-id, port)
BROADCAST_CMD_ASK = 1
BROADCAST_CMD_TELL = 2

def get_connect_ip(remote_host, remote_port):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect((remote_host, remote_port))
    try:
        return s.getsockname()[0]
    finally:
        s.close()

class Bootstrapper(object):
    def __init__(self, network_id, node_id, dht_port):
        assert isinstance(network_id, types.StringType)
        assert RE_NETWORK_ID.match(network_id)
        assert isinstance(node_id, (types.LongType, types.IntType))
        assert 0 <= node_id <= dht.MAX_ID
        assert isinstance(dht_port, types.IntType)
        
        self._network_id = network_id
        self._node_id = node_id
        self._dht_port = dht_port
        
        self._broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._broadcast_socket.settimeout(BROADCAST_SOCKET_TIMEOUT)
        self._broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self._broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._broadcast_socket.bind(('<broadcast>', BROADCAST_PORT))
        self._broadcast_response = []
        
        self._active = True
        self._broadcast_thread = threading.Thread(target=self._broadcast_responder)
        self._broadcast_thread.start()
    
    def start_network(self, network):
        assert isinstance(network, dht.DHT)
        
        nodes = []
        
        # Get nodes from broadcast
        nodes += self._broadcast_ask()
        
        # Start network
        return network.start(start_points=nodes)

    def stop_network(self, network):
        assert isinstance(network, dht.DHT)
        
        self._active = False
        
        # Stop bootstrapper
        
        self._broadcast_thread.join()
        return network.stop()

    def _broadcast_ask(self):
        self._broadcast_response = []
        self._broadcast_socket.sendto(struct.pack(BROADCAST_HDR, 
                                                  BROADCAST_MAGIC,
                                                  BROADCAST_CMD_ASK),
                                      ('<broadcast>', BROADCAST_PORT))
        time.sleep(BROADCAST_TELL_TIMEOUT)
        return self._broadcast_response

    def _broadcast_responder(self):
        while self._active:
            try:
                buf, addr = self._broadcast_socket.recvfrom(4096)
            except socket.timeout:
                continue
            
            magic, cmd = struct.unpack(BROADCAST_HDR, buf[:struct.calcsize(BROADCAST_HDR)])
            buf = buf[struct.calcsize(BROADCAST_HDR):]
            
            if magic != BROADCAST_MAGIC:
                print "Broadcast magic does not match"
                continue
            
            if cmd == BROADCAST_CMD_ASK:
                hdr = struct.pack(BROADCAST_HDR, 
                                  BROADCAST_MAGIC,
                                  BROADCAST_CMD_TELL)
                data = struct.pack(BROADCAST_TELL_HDR,
                                   dht.id_to_packed(self._node_id),
                                   self._dht_port)
                self._broadcast_socket.sendto("%s%s" % (hdr, data),
                                              ('<broadcast>', BROADCAST_PORT))
            elif cmd == BROADCAST_CMD_TELL:
                node_id, port = struct.unpack(BROADCAST_TELL_HDR,
                                              buf[:struct.calcsize(BROADCAST_TELL_HDR)])
                buf = buf[struct.calcsize(BROADCAST_TELL_HDR):]
                node_id = dht.packed_to_id(node_id)
                self._broadcast_response.append((node_id, addr[0], int(port)))
            else:
                print "Unknown broadcast command: %d" % cmd