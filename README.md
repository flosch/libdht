Simple example of how to use the DHT library:
```python
import random, time, dht
random.seed(time.time())

class MyNetwork(dht.DHT):
    def __init__(self, *args, **kwargs):
        self._my_db = {}
        super(MyNetwork, self).__init__(*args, **kwargs)
    
    def handle_save(self, key, value, use_cache=False):
        # Note: the DHT caches data from time to time for faster access, you can 
        # handle these cache save/load requests by respecting the use_cache flag.
        
        self._my_db[key] = value
        return True
    
    def handle_load(self, key, use_cache=False):
        return self._my_db.get(key)
    
    def handle_delete(self, key, use_cache=False):
        del self._my_db[key]
        return True
    
    def handle_has_key(self, key, use_cache=False):
        return key in self._my_db

def main():
    # Uses port 5000 TCP+UDP, any other port would work too
    n = MyNetwork(node_id=random.randint(0, dht.MAX_ID), port=5000)
    n.start()
    
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
    
    n.stop()
```
