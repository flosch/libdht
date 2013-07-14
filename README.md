**Important note**: The library is still **work in progress**, there are a lot of TODOs in it. So please be careful when using libdht. I strongly advise against using it in a production environment yet.

libdht supports bootstrapping (currently only searching for nodes in the local network using broadcast).

Simple example of how to use the DHT library:


```bash
(There are already 2 other nodes started.)

$ python example.py
My ID = 315266357664053077240863549245018455720435477024

Saving: True
Loading: Hallo!
How many nodes have this key? 3
Removing: True
How many nodes have this key now? 0
Enter to exit.
```

```python
# -*- coding: utf-8 -*-

import random, time, sys, dht, bootstrap
random.seed(time.time())

class MyNetwork(dht.DHT):
    def __init__(self, *args, **kwargs):
        self._my_db = {}
        super(MyNetwork, self).__init__(*args, **kwargs)

    def handle_save(self, key, value):
        self._my_db[key] = value
        return True

    def handle_load(self, key):
        return self._my_db.get(key)

    def handle_delete(self, key):
        del self._my_db[key]
        return True

    def handle_has_key(self, key):
        return key in self._my_db

def main():
    # Uses port as first argument for communication (TCP+UDP)
    my_id = random.randint(0, dht.MAX_ID)
    port = random.randint(5000, 10000)
    n = MyNetwork(node_id=my_id, port=port)
    
    bootstrapper = bootstrap.Bootstrapper(network_id="test", node_id=my_id,
                                          dht_port=port)
    bootstrapper.start_network(n)
    try:
        print "My ID = %d" % my_id
        print
     
        # Hash your data (160-bit integer), for this example we'll get a random int
        data_id = random.randint(0, dht.MAX_ID)
    
        # Returns True on success, gets automatically replicated
        print "Saving:", n.save(data_id, "Hallo!", replicas=20)
    
        # Returns "Hallo!" (received from one of the nodes available in the network having this key)
        print "Loading:", n.load(data_id)
    
        # Is the key available in the network? Returns the number of replicas.
        print "How many nodes have this key?", n.has_key(data_id)
    
        # Removes the key+data from all nodes in the network
        print "Removing:", n.delete(data_id)
        
        print "How many nodes have this key now?", n.has_key(data_id)
    
        raw_input("Enter to exit.")
    finally:
        # Make sure network is always shutting down
        bootstrapper.stop_network(n)

if __name__ == "__main__":
    main()
```
