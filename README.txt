The following project implements Client & Server infrastructure using Java Netty** that allows multiple clients to retrieve/insert data 
from/to the server using a specific API. The server is a TCP server and supports caching with data persistance to disk.

**Java Netty is an asynchronous event-driven network application framework for rapid development of maintainable high performance protocol 
servers & clients.

How the internals work:

Caching mechanism on the server is thread-safe, it was impelmented using concurrent data structures as well as read/write locks for 
more complex procedures. Server cache implements LRU caching scheme, it uses a queue to infer the LRU entries to be evicted 
from cache(entries are evicted when cache max size manual threshold is crossed), it stores them in a recently removed collection until a 
data persisting thread wakes up, checks if there is enough evicted entries(using another manual threshold) and stores them to disk if 
necessary, then cleans the recently removed collection and continues.
Data persistance mechanism mainly relies on built in java serialization for various objects.

Client communicates with the server using STDIN with the following protocol:

* getallkeys_<pattern> - to receive all keys matching the specified pattern, example: getallkeys_abc
* rightadd_<K>_<V> - to add a value V to key K, from the right, example: rightadd_abc_123
* leftadd_<K>_<V> - to add a value V to key K, from the left, example: leftadd_abc_123
* set_<K>_<[V]> - to add a pair of key K with values list [V] separated by comma, example: set_abc_1,2,3
* get_<K> - to get a values list by key K, example: get_abc
* help - to show option menu
* exit - to quit

Server may also receive commands via STDIN, currently supports termination only.

Difficulties I've encountered during the process:

* Understanding Netty Asynchronous API.
* Synchronizing the cache with minimum impact on parallelization.
* Minimizing I/O vs avoiding heavy memory consumption.
* Getting optimal performance for various operations, such as getallkeys, choosing the right data structure.

Run configuration:
* To run server > java -jar Server.jar -h <host> -p <port> -f <filepath for data persistance>
* To run client > java -jar Client.jar -h <host> -p <port>
* All flags are optional, default values are: -h localhost -p 9999 for both and -f data.ser for server.
