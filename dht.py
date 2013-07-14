# -*- coding: utf-8 -*-

"""
TODO:
- Streaming support for huge files (save/load) without holding the whole file in RAM
- Block-wise receive for one key from multiple nodes at the same time for more speed
  (for example load half of the file from node A and other half from node B simultaneously)
- regular distribution
- Add handle_bootstrap for bootstrap-module in case the DHT runs out of peers
"""

import copy
import time
import types
import struct
import socket
import math
import threading

from Crypto.Random import random
from Crypto.Hash import SHA

# Configuration
M = 160
MAX_ID = 2**M - 1
BUCKET_SIZE = 20
REPLICAS = 5
BUFSIZE = 8192
NODE_TIMEOUT = 0.125 # in seconds
SIMULTANEOUS_REQUESTS = 3
SOCKET_TIMEOUT = NODE_TIMEOUT # in seconds
MAX_KEY_SIZE = 10 * 1024 * 1024 # 10 MB/key currently

# Discovery-Protocol
DISCOVERY_MAGIC = 0x081F
DISCOVERY_PROTOCOL_VERSION = 1
DISCOVERY_HDR = "!HHH20s" # (magic, proto-version, cmd, sender-id)
DISCOVERY_HDR_FIND_NODE = "!20s20s" # (node-id, search-id)
DISCOVERY_NODE = "!20s4sH" # (node-id, ip, port)
DISCOVERY_NODE_LIST = "!Q20s?" # (following total node count, search-id,
                               # sender has key locally [bool])

DISCOVERY_CMD_FIND_NODE = 1
DISCOVERY_CMD_FIND_NODE_RESULT = 2


DISCOVERY_CMD_BYE = 10 # TODO: Indicates node is leaving the network
                       # (make sure everything is replicated from this node)

# Transport-protocol
TRANSPORT_MAGIC = 0xB1B9
TRANSPORT_PROTOCOL_VERSION = 1
TRANSPORT_HDR = "!HHH" # (magic, proto-version, cmd)
TRANSPORT_HDR_RESPONSE_SUCCESS = "!?" # (success bool,)
TRANSPORT_HDR_RESPONSE_VALUE = "!?Q" # (success bool, datalen) + data
TRANSPORT_HDR_SAVE = "!20sQ" # (key-id, datalen) + data
TRANSPORT_HDR_KEY = "!20s" # (key-id, )
TRANSPORT_CMD_SAVE = 1
TRANSPORT_CMD_LOAD = 2
TRANSPORT_CMD_HAS_KEY = 3
TRANSPORT_CMD_DELETE = 4


# Helper functions
def id_to_packed(i):
    assert isinstance(i, (types.IntType, types.LongType))
    assert 0 <= i < 2**160 # Hard limit for this type of function

    x1 = i >> 96
    x2 = i >> 32 & 0xFFFFFFFFFFFFFFFF
    x3 = i & 0xFFFFFFFF
    return struct.pack("!QQI", x1, x2, x3)

def packed_to_id(x):
    x1, x2, x3 = struct.unpack("!QQI", x)
    return (x1 << 96) + (x2 << 32) + x3

def hash_data(x):
    assert isinstance(x, types.StringType)
    return packed_to_id(SHA.new(x).digest())

class NodeException(Exception): pass
class NodeTransportException(NodeException): pass
class NodeTimeout(NodeException): pass
class Node(object):
    """
    A node represents a client for discovery purposes.
    """

    @classmethod
    def acquire(cls, network, node_id, ip, port):
        assert isinstance(network, DHT)
        assert isinstance(node_id, (types.IntType, types.LongType))
        assert 1 <= node_id <= MAX_ID
        assert isinstance(ip, types.StringType)
        assert isinstance(port, types.IntType)
        
        ip = socket.gethostbyname(ip)

        return network._nodes.setdefault(node_id,
                                         Node(network=network, node_id=node_id,
                                              ip=ip, port=port))

    def __init__(self, network, node_id, ip, port):
        self._network = network
        self._node_id = node_id
        self._ip = ip
        self._port = port
        self._last_touched = 0
        self._marked_as_bad = False

        self._bucket = network._buckets.get_bucket_by_node(self)

        self._discovery_socket = self._network._discovery_socket
        self._transport_lock = threading.Lock()
        self._transport_socket = None
        self._transport_buffer = ""

    def __del__(self):
        if self._transport_socket:
            self._transport_socket.close()
            self._transport_socket = None

    def _discovery_send(self, cmd, data=None):
        assert isinstance(cmd, types.IntType)
        assert 0 <= cmd < 2**16
        assert isinstance(data, (types.NoneType, types.StringType))

        hdr = struct.pack(DISCOVERY_HDR, DISCOVERY_MAGIC, DISCOVERY_PROTOCOL_VERSION,
                          cmd, id_to_packed(self._network._node_id))

        if data:
            buf = "%s%s" % (hdr, data)
        else:
            buf = hdr

        assert len(buf) == self._discovery_socket.sendto(buf, (self._ip, self._port))
        return True

    def _transport_send(self, cmd, data=None):
        assert isinstance(cmd, types.IntType)
        assert 0 <= cmd < 2**16
        assert isinstance(data, (types.NoneType, types.StringType))

        hdr = struct.pack(TRANSPORT_HDR, TRANSPORT_MAGIC,
                          TRANSPORT_PROTOCOL_VERSION, cmd)

        if data:
            buf = "%s%s" % (hdr, data)
        else:
            buf = hdr

        try:
            self._transport_socket.sendall(buf)
        except socket.error, e:
            self._transport_socket = None
            raise NodeTimeout('transport_socket sendall error: %s' % e) # TODO (Catch this thing elsewhere!)
            #return False

        return True

    def _transport_receive_success(self):
        awaited_data = struct.calcsize(TRANSPORT_HDR_RESPONSE_SUCCESS)
        while len(self._transport_buffer) < awaited_data:
            buf = self._transport_socket.recv(BUFSIZE)
            if not buf:
                self._transport_socket = None
                raise NodeTimeout('transport_socket recv error') # TODO (Catch this thing elsewhere!)
                #return False

            self._transport_buffer += buf

        success, = struct.unpack(TRANSPORT_HDR_RESPONSE_SUCCESS,
                                 self._transport_buffer[:struct.calcsize(TRANSPORT_HDR_RESPONSE_SUCCESS)])
        self._transport_buffer = self._transport_buffer[struct.calcsize(TRANSPORT_HDR_RESPONSE_SUCCESS):]

        return success

    def _transport_receive_key_data(self):
        awaited_data = struct.calcsize(TRANSPORT_HDR_RESPONSE_VALUE)
        while len(self._transport_buffer) < awaited_data:
            buf = self._transport_socket.recv(BUFSIZE)
            if not buf:
                self._transport_socket = None
                raise NodeTimeout('transport_socket recv error') # TODO (Catch this thing elsewhere!)
                #return False, None

            self._transport_buffer += buf

        success, datalen = struct.unpack(TRANSPORT_HDR_RESPONSE_VALUE,
                                         self._transport_buffer[:struct.calcsize(TRANSPORT_HDR_RESPONSE_VALUE)])
        self._transport_buffer = self._transport_buffer[struct.calcsize(TRANSPORT_HDR_RESPONSE_VALUE):]

        data = None
        if success and datalen > 0:
            while len(self._transport_buffer) < datalen:
                buf = self._transport_socket.recv(BUFSIZE)
                if not buf:
                    self._transport_socket = None
                    raise NodeTimeout('transport_socket recv error') # TODO (Catch this thing elsewhere!)

                self._transport_buffer += buf

            data = self._transport_buffer[:datalen]
            self._transport_buffer = self._transport_buffer[datalen:]

        return success, data

    def touch(self):
        self._last_touched = time.time()
        self._bucket.touch(self)
    
    def mark_as_bad(self):
        self._marked_as_bad += 1
        
        # TODO: Add consequence if the counter excees a limit

    def find_node(self, node_id, search_id):
        assert isinstance(node_id, (types.IntType, types.LongType))
        assert 1 <= node_id <= MAX_ID
        assert isinstance(search_id, (types.IntType, types.LongType))
        assert 0 <= search_id <= MAX_ID

        data = struct.pack(DISCOVERY_HDR_FIND_NODE,
                           id_to_packed(node_id),
                           id_to_packed(search_id))

        self._discovery_send(DISCOVERY_CMD_FIND_NODE, data=data)

    def __repr__(self):
        return "<Node id=%d ip=%s port=%d>" % (self._node_id, self._ip, self._port)

    def _prepare_transport(self):
        if self._transport_socket:
            return True

        self._transport_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self._transport_socket.connect((self._ip, self._port))
        except socket.error, e:
            self._transport_socket = None
            self.mark_as_bad()
            raise NodeTimeout('transport_socket connect error: %s' % e) # TODO (Catch this thing elsewhere!)
            #return False
            #raise NodeTransportException('Could not connect to transport endpoint')
            #return False # Better raise exception?

        return True

    def save(self, key, value):
        assert isinstance(key, (types.IntType, types.LongType))
        assert 1 <= key <= MAX_ID
        assert isinstance(value, types.StringType) # If value is unicode, it must be first encoded into UTF-8, for example

        with self._transport_lock:
            if not self._prepare_transport():
                return False

            hdr = struct.pack(TRANSPORT_HDR_SAVE, id_to_packed(key), len(value))
            self._transport_send(TRANSPORT_CMD_SAVE, "%s%s" % (hdr, value))

            return self._transport_receive_success() # Returns a success boolean

    def load(self, key):
        assert isinstance(key, (types.IntType, types.LongType))
        assert 1 <= key <= MAX_ID

        with self._transport_lock:
            if not self._prepare_transport():
                return False, None

            hdr = struct.pack(TRANSPORT_HDR_KEY, id_to_packed(key))
            self._transport_send(TRANSPORT_CMD_LOAD, hdr)

            return self._transport_receive_key_data() # Returns (success bool, data)

    def has_key(self, key):
        assert isinstance(key, (types.IntType, types.LongType))
        assert 1 <= key <= MAX_ID

        with self._transport_lock:
            if not self._prepare_transport():
                return False # TODO: Is this the right value to return (might be confusing because False = has verifiable no key)

            hdr = struct.pack(TRANSPORT_HDR_KEY, id_to_packed(key))
            self._transport_send(TRANSPORT_CMD_HAS_KEY, hdr)

            return self._transport_receive_success() # Returns a success boolean

    def delete(self, key):
        assert isinstance(key, (types.IntType, types.LongType))
        assert 1 <= key <= MAX_ID

        with self._transport_lock:
            if not self._prepare_transport():
                return False # TODO: Is this the right value to return (might be confusing because False = not deleted)

            hdr = struct.pack(TRANSPORT_HDR_KEY, id_to_packed(key))
            self._transport_send(TRANSPORT_CMD_DELETE, hdr)

            return self._transport_receive_success() # Returns a success boolean

class Bucket(object):
    """
    Represents a list item of the discovery routing table and can hold at max
    BUCKET_SIZE items.
    """
    
    # TODO FIXME: Resort bucket related to LRU
    
    def __init__(self, no):
        assert isinstance(no, types.IntType)
        self._no = no
        self._last_changed = 0
        self._nodes = []
        self._cache = []

    def touch(self, node):
        assert isinstance(node, Node)
        self._last_changed = time.time()
        self.add(node)
        
        # Resort bucket related to LRU
        self._nodes.sort(key=lambda x: x._last_touched, reverse=True)

    def timeout(self, node):
        # TODO: Node timed out, check if we can replace him with a node from the cache
        pass

    def add(self, node):
        assert isinstance(node, Node)

        # Add, if we have less than BUCKET_SIZE items
        if len(self._nodes) < BUCKET_SIZE:
            if not node in self._nodes:
                self._nodes.append(node)
        else:
            # Add to cache because bucket is full
            # Might be used if another node times out
            self._cache.append(node)

    def __getitem__(self, key):
        return self._nodes[key]

    def __len__(self):
        return len(self._nodes)

class BucketMgr(object):

    def __init__(self, network):
        self._network = network
        self._buckets = []

        for i in xrange(M):
            self._buckets.append(Bucket(i))

    def add(self, node):
        assert isinstance(node, Node)
        bucket = self.get_bucket_by_node(node)
        return bucket.add(node)

    def get_bucket_by_node(self, node):
        assert isinstance(node, Node)
        idx = int(math.log(node._node_id, 2))
        return self._buckets[idx]

    def get_closest_nodes(self, node_id, count=BUCKET_SIZE):
        assert isinstance(node_id, (types.LongType, types.IntType))
        assert 1 <= node_id <= MAX_ID

        idx = int(math.log(node_id, 2))

        result = []

        start_bucket = self._buckets[idx]

        for node in start_bucket[:count]:
            result.append(node)

        if len(result) < count:
            # Too little results (`coun` wanted)
            diff = 1
            while len(result) < count:
                tried = False

                if idx + diff < M:
                    tried = True
                    for node in self._buckets[idx+diff]:
                        result.append(node)

                if idx - diff >= 0:
                    tried = True
                    for node in self._buckets[idx-diff]:
                        result.append(node)

                diff += 1

                if not tried:
                    break

        return result[:count]

class Search(object):

    def __init__(self, network, node_id):
        assert isinstance(network, DHT)
        assert isinstance(node_id, (types.IntType, types.LongType))
        assert 1 <= node_id <= MAX_ID

        # Create new search id and register search
        search_id = random.randint(0, MAX_ID)
        with network._search_db_lock:
            network._search_db[search_id] = self

        self._network = network
        self._search_id = search_id # Identification for this search
        self._node_id = node_id # Node id to be searched

        self._lock = threading.Lock()

        self._query_list = network._buckets.get_closest_nodes(self._node_id)
        self._waitlist = {}
        self._queried = []
        self._results = []
        self._stores = []
        self._changed = False

    def add_results(self, sender, nodes, sender_has_key):
        assert isinstance(sender, Node)
        assert isinstance(nodes, types.ListType)
        assert isinstance(sender_has_key, types.BooleanType)

        with self._lock:
            if sender in self._waitlist:
                del self._waitlist[sender]

            if sender_has_key and sender not in self._stores:
                self._stores.append(sender)

            for node in nodes:
                node.touch() # TODO: Is this right?

                if not node in self._queried and not node in self._waitlist and \
                    not node in self._query_list:
                    # Add new node to query todo list
                    self._query_list.append(node)

                if not node in self._results:
                    self._results.append(node)

            # Compute new results
            new_results = sorted(self._results,
                                 cmp=lambda x, y: cmp(x._node_id ^ self._node_id,
                                                      y._node_id ^ self._node_id))

            # Compare new with old results and mark as changed if needed
            if len(new_results) != len(self._results):
                self._changed = True
                self._results = new_results
            else:
                for idx, item in enumerate(new_results):
                    if item != self._results[idx]:
                        self._changed = True
                        self._results = new_results
                        break

    def run(self):
        if not self._query_list:
            # We have nobody to query
            return [], []

        # Search loop
        while True:
            with self._lock:
                do_query = len(self._waitlist) < SIMULTANEOUS_REQUESTS and \
                    self._query_list

            if not do_query:
                time.sleep(0.0001) # just wait a little
            else:
                # Do query
                with self._lock:
                    itemcount = min(SIMULTANEOUS_REQUESTS - len(self._waitlist),
                                    len(self._query_list))

                    # Query
                    for i in xrange(itemcount):
                        n = self._query_list[i]
                        n.find_node(node_id=self._node_id, search_id=self._search_id)
                        self._waitlist[n] = time.time()
                        self._queried.append(n)

                    # Remove from todo query list
                    self._query_list = self._query_list[itemcount:]

            with self._lock:
                # Do check response timeouts
                if len(self._waitlist) > 0:
                    # We have nodes we're waiting for
                    for node, time_asked in copy.copy(self._waitlist).iteritems():
                        if time.time() - time_asked > NODE_TIMEOUT:
                            #print "timeout!!!", node
                            # Response timed out, remove node from waiting list
                            del self._waitlist[node]
                            # TODO: Inform other components about node's timeout
                            # Maybe mark him as questionable
                else:
                    # No nodes to wait for
                    # If we don't have any nodes to query left and the result set doesn't
                    # changed, we can end our search iteration
                    if not self._waitlist and not self._changed and not self._query_list:
                        # TODO FIXME: Query at least all remaining not-queried nodes (max BUCKETSIZE)
                        # TODO: Have a look at the protocol specification again
                        break # Stop search iteration

                    self._changed = False

        with self._lock and self._network._search_db_lock:
            # Remove search id from db
            del self._network._search_db[self._search_id]

        return self._results, self._stores

class DHT(object):

    def __init__(self, node_id, port, ip_bind=''):
        """
        Initiates a DHT node.
        """
        assert isinstance(node_id, (types.IntType, types.LongType))
        assert 1 <= node_id <= MAX_ID
        assert isinstance(port, types.IntType)
        assert isinstance(ip_bind, types.StringType)

        self._node_id = node_id
        self._port = port
        self._ip_bind = ip_bind

        # Prepare buckets
        self._buckets = BucketMgr(self)

        # Node cache
        self._nodes = {} # id -> Node-instance

        # Search queues
        self._search_db_lock = threading.Lock()
        self._search_db = {} # id -> SearchQueue()

        # Prepare sockets + dispatcher threads
        self._active = False

        self._discovery_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._discovery_socket.settimeout(SOCKET_TIMEOUT)
        self._discovery_thread = threading.Thread(target=self._discovery_dispatcher)

        self._transport_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._transport_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._transport_socket.settimeout(SOCKET_TIMEOUT)
        self._transport_thread = threading.Thread(target=self._transport_dispatcher)

    def on_start(self):
        """
        Can be used by overwriting. Will be triggered AFTER the network has
        been initialized.
        """
        pass

    def start(self, start_points=None):
        if self._active:
            return False

        self._discovery_socket.bind((self._ip_bind, self._port))
        self._transport_socket.bind((self._ip_bind, self._port))
        self._transport_socket.listen(3)

        self._active = True
        self._discovery_thread.start()
        self._transport_thread.start()

        if start_points:
            self.join(start_points=start_points) # find_node is part of the join process
        else:
            self.find_node(self._node_id) # propagate yourself in the network
        
        self.on_start()

        return True
    
    def join(self, start_points):
        assert isinstance(start_points, types.ListType)
        for nid, ip, port in start_points:
            n = Node.acquire(self, node_id=nid, ip=ip, port=port)
            self._buckets.add(n)
        self.find_node(self._node_id) # propagate yourself in the network

    def on_stop(self):
        """
        Can be used by overwriting. Will be triggered BEFORE the network has
        been stopped.
        """
        pass

    def stop(self):
        if not self._active:
            return False
    
        self.on_stop()

        self._active = False
        self._discovery_socket.close()
        self._transport_socket.close()

        self._discovery_thread.join()
        self._transport_thread.join()

        return True

    def find_node(self, node_id):
        """
        Searches for node_id in the DHT.

        @return: List of nearest nodes found.
        """

        search = Search(self, node_id)
        nodes, _ = search.run()
        return nodes

    def find_stores(self, node_id):
        search = Search(self, node_id)
        _, stores = search.run()
        return stores

    def find(self, node_id):
        search = Search(self, node_id)
        return search.run()

    def handle_has_key(self, key):
        raise NotImplementedError("Please subclass and implement this method (handle_has_key).")

    def handle_save(self, key, value):
        """
        Saves key-value-pair persistently locally.

        Must return True or False.
        """
        # TODO: Return true = success (so the "pusher" can count all replicas)
        #              false = fail, not allowed, etc.
        raise NotImplementedError("Please subclass and implement this method (handle_save).")

    def handle_load(self, key):
        """
        Loads key from disk.

        @return: Datastring (found) or None (not found)
        """
        raise NotImplementedError("Please subclass and implement this method (handle_load).")

    def handle_delete(self, key):
        raise NotImplementedError("Please subclass and implement this method (handle_delete).")

    # !!!! TODO: Add methods for rebalancing and replicating the keys !!!!
    # Maybe add a method to ITERATE over ALL local keys (to check for replicas/rebalancing)

    def save(self, key, value, replicas=REPLICAS):
        """
        Save the given key-value-pair in the network.
        """
        assert isinstance(key, (types.IntType, types.LongType))
        assert 1 <= key <= MAX_ID
        assert isinstance(value, types.StringType)
        #assert isinstance(use_cache, types.BooleanType)

        nodes, stores = self.find(key)

        store_count = 0

        # Update all stores
        for node in stores:
            if node.save(key, value):
                store_count += 1

        # Check if first node is in stores, if not, store on this node
        # (ignore replica here because we always want our data on the best node in the network)
        if nodes and stores and not nodes[0] in stores:
            if nodes[0].save(key, value):
                store_count += 1
                nodes.remove(nodes[0])

        # Now replicate on other nodes!
        while store_count < replicas and nodes:
            node = nodes[0]
            nodes = nodes[1:]

            #print "Replicating during save() on %d..." % node._node_id
            
            if node in stores:
                # Ignore nodes which are already stores
                continue
            
            if node.save(key, value):
                store_count += 1
            else:
                print "Attention: Node couldn't save!"

        if store_count == 0:
            # We don't have ANY node to store on. Store locally and re-balance later.
            return self.handle_save(key=key, value=value)
            # TODO: Add to a rebalance db which will be checked when other nodes
            # came up
        
        # Only use cache if we could save it in the network
        # FIXME: use_cache deactivated!
        #if use_cache:
        #    self.handle_save(key=key, value=value, use_cache=True)

        return True # TODO MAYBE: Only succeeded if we have more saved than errored?

    def load(self, key):
        """
        Determines the data postion in the network and receives it.

        @field use_cache: If True, network will do a local cache lookup.
        @return: Data string (found) or None (not found)
        """
        assert isinstance(key, (types.IntType, types.LongType))
        assert 1 <= key <= MAX_ID
        #assert isinstance(use_cache, types.BooleanType)

        #if use_cache and self.handle_has_key(key, use_cache=True):
        #    return self.handle_load(key, use_cache=True)

        nodes, stores = self.find(key)

        # Receive key from one store
        value = None
        for store in stores:
            success, data = store.load(key)
            if success:
                value = data
                break

        if value is None:
            # Not found in DHT :-|
            return None

        # Ensure that we have enough replicas in the network
        # TODO: We could do this in it's own thread for performance reasons
        new_replicas = len(stores)
        while new_replicas < REPLICAS:
            #print "Replicating (found %d stores, required %d)..." % (len(stores), REPLICAS)
            for node in nodes:
                if node not in stores:
                    #print "Replicating on %s..." % node,
                    if node.save(key, value):
                        new_replicas += 1
                        stores.append(node)
                        #print "Done"
                        break
                    #else:
                    #    print "Failed"
            else:
                break

        # Cache key after loading locally
        # TODO: We could do this in it's own thread for performance reasons
        # self.handle_save(key, value, use_cache=True)

        return value

    def delete(self, key):
        """
        Delete any key from the network.

        @return: Returns True on success, False otherwise.
        """
        assert isinstance(key, (types.IntType, types.LongType))
        assert 1 <= key <= MAX_ID

        # Remove on all other stores in the network
        stores = self.find_stores(key)

        for store in stores:
            if not store.delete(key):
                print "Warning: Node %d couldn't delete our key %d" % \
                    (store._node_id, key)

        # Remove locally if existent
        if self.handle_has_key(key):
            self.handle_delete(key)

        # Remove locally if existent (even in cache)
        #if self.handle_has_key(key, use_cache=True):
        #    self.handle_delete(key, use_cache=True)

        return True

    def has_key(self, key):
        assert isinstance(key, (types.IntType, types.LongType))
        assert 1 <= key <= MAX_ID

        # Remove on all other stores in the network
        stores = self.find_stores(key)
        
        return len(stores)

    def _discovery_dispatcher(self):
        while self._active:
            try:
                buf, addr = self._discovery_socket.recvfrom(BUFSIZE)
            except socket.timeout:
                continue

            try:
                magic, version, cmd, sender_id = struct.unpack(DISCOVERY_HDR,
                                                               buf[:struct.calcsize(DISCOVERY_HDR)])
                buf = buf[struct.calcsize(DISCOVERY_HDR):]
            except struct.error:
                print "Malformed packet received."
                continue
            sender_id = packed_to_id(sender_id)

            if magic != DISCOVERY_MAGIC:
                print "Magic doesn't match"
                continue

            if version != DISCOVERY_PROTOCOL_VERSION:
                print "Version doesn't match"
                continue

            # Touch the sender
            ip, port = addr
            sender_node = Node.acquire(self, sender_id, ip, port)
            sender_node.touch()

            if cmd == DISCOVERY_CMD_FIND_NODE:
                self._handle_find_node(buf, addr, sender_node)
            elif cmd == DISCOVERY_CMD_FIND_NODE_RESULT:
                self._handle_find_node_result(buf, addr, sender_node)
            else:
                print "Unknown command received: %d" % cmd

    def _transport_dispatcher(self):
        while self._active:
            try:
                conn, addr = self._transport_socket.accept()
            except socket.timeout:
                continue

            t = threading.Thread(target=self._transport_client_dispatcher,
                                 args=(conn, addr))
            t.start()

    def _transport_client_dispatcher(self, conn, addr):
        conn.settimeout(SOCKET_TIMEOUT)
        buffer = ""
        while self._active:
            try:
                buf = conn.recv(BUFSIZE)
            except socket.timeout:
                continue

            if not buf:
                conn.close()
                return
            buffer += buf

            if len(buffer) < struct.calcsize(TRANSPORT_HDR):
                # We're waiting for more data
                continue

            magic, version, cmd = struct.unpack(TRANSPORT_HDR, buffer[:struct.calcsize(TRANSPORT_HDR)])
            buffer = buffer[struct.calcsize(TRANSPORT_HDR):]

            if magic != TRANSPORT_MAGIC:
                print "Received wrong transport magic, closing connection."
                conn.close()
                # TODO: Do anything else here?
                return

            if version != TRANSPORT_PROTOCOL_VERSION:
                print "Received wrong transport version, closing connection."
                conn.close()
                # TODO: Do anything else here?
                return

            if cmd == TRANSPORT_CMD_HAS_KEY:
                buffer = self._handle_has_key(conn, addr, buffer)
            elif cmd == TRANSPORT_CMD_SAVE:
                buffer = self._handle_save(conn, addr, buffer)
            elif cmd == TRANSPORT_CMD_LOAD:
                buffer = self._handle_load(conn, addr, buffer)
            elif cmd == TRANSPORT_CMD_DELETE:
                buffer = self._handle_delete(conn, addr, buffer)
            else:
                print "Unknown transport command: %d" % cmd
                continue

        # TODO: Send the clients a bye bye
        conn.close()

    def _handle_find_node(self, buf, addr, sender_node):
        node_id, search_id = struct.unpack(DISCOVERY_HDR_FIND_NODE,
                                           buf[:struct.calcsize(DISCOVERY_HDR_FIND_NODE)])
        buf = buf[struct.calcsize(DISCOVERY_HDR_FIND_NODE):]
        node_id = packed_to_id(node_id)

        nodes = self._buckets.get_closest_nodes(node_id)

        hdr = struct.pack(DISCOVERY_HDR, DISCOVERY_MAGIC, DISCOVERY_PROTOCOL_VERSION,
                  DISCOVERY_CMD_FIND_NODE_RESULT, id_to_packed(self._node_id))

        hdr_list = struct.pack(DISCOVERY_NODE_LIST,
                               len(nodes),
                               search_id,
                               self.handle_has_key(node_id))

        data = []
        for node in nodes:
            data.append(struct.pack(DISCOVERY_NODE,
                                    id_to_packed(node._node_id),
                                    socket.inet_aton(node._ip),
                                    node._port))

        self._discovery_socket.sendto("%s%s%s" % (hdr, hdr_list,
                                                  "".join(data)),
                                      addr)

    def _handle_find_node_result(self, buf, addr, sender_node):
        nodecount, search_id, has_key_locally = struct.unpack(DISCOVERY_NODE_LIST,
                                                              buf[:struct.calcsize(DISCOVERY_NODE_LIST)])
        buf = buf[struct.calcsize(DISCOVERY_NODE_LIST):]
        search_id = packed_to_id(search_id)

        with self._search_db_lock:
            search = self._search_db.get(search_id)

        if not search:
            print "Search id not found (anymore): '%d'" % search_id, self._search_db.keys()
            return

        nodes = []
        for _ in xrange(nodecount):
            node_id, ip, port = struct.unpack(DISCOVERY_NODE,
                                              buf[:struct.calcsize(DISCOVERY_NODE)])
            buf = buf[struct.calcsize(DISCOVERY_NODE):]
            node_id = packed_to_id(node_id)
            ip = socket.inet_ntoa(ip)

            nodes.append(Node.acquire(self, node_id, ip, port))

        search.add_results(sender=sender_node,
                           nodes=nodes,
                           sender_has_key=has_key_locally)

    def _handle_save(self, conn, addr, buffer):
        """
        Handles the incoming request to save data locally.
        """
        # Make sure we got the save header
        while len(buffer) < struct.calcsize(TRANSPORT_HDR_SAVE):
            if not self._active:
                return buffer

            try:
                buf = conn.recv(BUFSIZE)
            except socket.timeout:
                continue

            if not buf:
                return buffer

            buffer += buf

        key_id, datalen = struct.unpack(TRANSPORT_HDR_SAVE,
                                        buffer[:struct.calcsize(TRANSPORT_HDR_SAVE)])
        buffer = buffer[struct.calcsize(TRANSPORT_HDR_SAVE):]
        key_id = packed_to_id(key_id)
        
        if datalen > MAX_KEY_SIZE:
            # Key is too big for me, closing connection 
            # to prevent him from sending more and more and more and mo... 
            # TODO: Dunno whether we can make this better
            print "Closing connection to node due too big key size (%d bytes)" % datalen 
            conn.close()
            return buffer

        # Make sure we get all the data
        while len(buffer) < datalen:
            if not self._active:
                return buffer

            try:
                buf = conn.recv(BUFSIZE)
            except socket.timeout:
                continue

            if not buf:
                return buffer

            buffer += buf

        value = buffer[:datalen]
        buffer = buffer[datalen:]

        saved = self.handle_save(key=key_id, value=value)
        assert isinstance(saved, types.BooleanType)

        return_hdr = struct.pack(TRANSPORT_HDR_RESPONSE_SUCCESS, saved)
        conn.sendall(return_hdr)

        return buffer

    def _handle_load(self, conn, addr, buffer):
        """
        Handles incoming load request.
        """
        # Make sure we got the load header
        while len(buffer) < struct.calcsize(TRANSPORT_HDR_KEY):
            if not self._active:
                return buffer

            try:
                buf = conn.recv(BUFSIZE)
            except socket.timeout:
                continue

            if not buf:
                return buffer

            buffer += buf

        key_id,  = struct.unpack(TRANSPORT_HDR_KEY,
                                 buffer[:struct.calcsize(TRANSPORT_HDR_KEY)])
        buffer = buffer[struct.calcsize(TRANSPORT_HDR_KEY):]
        key_id = packed_to_id(key_id)

        data = self.handle_load(key_id)
        assert isinstance(data, (types.StringType, types.NoneType))

        return_hdr = struct.pack(TRANSPORT_HDR_RESPONSE_VALUE, data is not None,
                                 data and len(data) or 0)
        conn.sendall(return_hdr)
        conn.sendall(data)

        return buffer

    def _handle_delete(self, conn, addr, buffer):
        """
        Handles incoming deletion request.
        """
        # Make sure we got the delete header
        while len(buffer) < struct.calcsize(TRANSPORT_HDR_KEY):
            if not self._active:
                return buffer

            try:
                buf = conn.recv(BUFSIZE)
            except socket.timeout:
                continue

            if not buf:
                return buffer

            buffer += buf

        key_id,  = struct.unpack(TRANSPORT_HDR_KEY,
                                 buffer[:struct.calcsize(TRANSPORT_HDR_KEY)])
        buffer = buffer[struct.calcsize(TRANSPORT_HDR_KEY):]
        key_id = packed_to_id(key_id)

        deleted = self.handle_delete(key_id)
        assert isinstance(deleted, types.BooleanType)

        return_hdr = struct.pack(TRANSPORT_HDR_RESPONSE_SUCCESS, deleted)
        conn.sendall(return_hdr)

        return buffer

    def _handle_has_key(self, conn, addr, buffer):
        """
        Handles incoming key-check.
        """

        # Make sure we got the has_key header
        while len(buffer) < struct.calcsize(TRANSPORT_HDR_KEY):
            if not self._active:
                return buffer

            try:
                buf = conn.recv(BUFSIZE)
            except socket.timeout:
                continue

            if not buf:
                return buffer

            buffer += buf

        key_id,  = struct.unpack(TRANSPORT_HDR_KEY,
                                 buffer[:struct.calcsize(TRANSPORT_HDR_KEY)])
        buffer = buffer[struct.calcsize(TRANSPORT_HDR_KEY):]
        key_id = packed_to_id(key_id)

        has_key = self.handle_has_key(key_id)
        assert isinstance(has_key, types.BooleanType)

        return_hdr = struct.pack(TRANSPORT_HDR_RESPONSE_SUCCESS, has_key)
        conn.sendall(return_hdr)

        return buffer
