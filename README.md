# ShardedCacheMap
An asynchronously concurrent sharded cache map in Rust.

# The Intention Behind this Data Structure
Truth is, making this data structure is very experimental for me. 

At first, I've been thinking about how a lock-free *shared* LRU/FIFO cache data structure would work. If it were to be possible, that would make such a data structure be really efficient and effective in a multithreaded environment (think of databases that may use this) because instead of applying threads applying a lock to the whole Hashmap data structure and its accompanying queue/linked list to make it thread-safe, we can allow threads to work independently on hash slots of the Hashmap and add to the queue/linked-list with memory ordering.

However, this is a really difficult problem to address. Part of that difficulty arises from making a lock-free bounded doubly linked list for LRU (not so much with FIFO with a bounded lock-free queue) alongside a lock-free Hashmap and making these two data structures interact with each other in a lock-free manner as well. For example, these questions appeared in my head:
    - In an LRU cache, the get/put method on an existing key will move the doubly linked list node to the head while eviction occurs at the tail end of the list. What should occur if that existing key was at the tail end and both a get/put update to that key conflicts with a put key that wants to evict at this tail end? How do we use atomic operations to synchronize all that?

    - To determine capacity of the Hashmap, on whether it's full or has space to add keys to it, how do we use memory ordering to obtain a correct view of the Hashmap's capacity among multiple threads? It's possible a thread could be in the middle of a put operation while another thread also wants to put another key to the cache. It's necessary that this other thread that's initiating a put request needs a correct view of the Hashmap's capacity to determine whether it should evict a key or not.
    
    - If multiple threads were performing a put request to the same key with different values simultaneously, now that could incur a problem as well. The correct behavior for this is that only one of this key should exist in the cache structure with the value corresponding to the last put that occurred here. How do we ensure that our cache data structure won't have multiple of the same keys? 
    
    - What's the behavior of getters and putters working together in the same hash slot? We know that multiple getters won't step on each other; multiple putters could step on each other; but can a putter step on a getter (or vice versa)? Well, a problem that could arise here is that you don't want a getter to get an incorrect view of a key in the hash slot; this could occur during a put request from another thread evicting a key and inserting its own key that a getter saw and wanted.
        - By this point and the previous, this already signals that a RWLock is necessary for putters/getters. 

You get the picture. So I chose a different route in making this data structure, and I chose to make this a purely async data structure.

# Design Behind Sharded Cache Map