:u5272:
## Why do you need a Partitioning Service?

There are many large-scale problems, where the full data or processing does not fit on one server.
In those cases, it is required to distribute the load across many servers. The partitioning service 
makes that easy.


The kind of problem solved by a partitioning service has these characteristics:
  1. The problem space can be sharded based on a hash (e.g., hash of primary key, or hash of user ID). Each hash is a "partition".
  2. The number of partitions is larger than a number of servers, so a given server serves multiple partitions
  3. Correctness: A given partition is served by one and only one server (the system need to make sure two servers never serve the same partition)
  4. Availability: If a server serving a partition goes down, a different server needs to pick up the load from the downed server
  5. Load distribution: It is desirable for the load to be roughly evenly distributed between partitions

  
There are many problems in real life that fit this profile:
  1. **Sharded Databases**: The data is sharded by primary key, and independent instances of database servers serve the various shards. The database 
itself could be a relational or NoSQL database.
  2. **Partitioned Load**: A large number of users us divided into a number of application servers. A given application server serves the users in it's 
shard. This is useful for applications that have to have a "master server" per user that has a full view of a given user(e.g., IM, chat, social networks, etc.).
  3. **Partitioned Processing**: A set of long-running processes are processing a set of resources. A given resource should be processed by only one server at a 
time, but if that server crashes, the processing can be restarted by another process (two servers should never be processing the same resource at one time).



For details on how to use, see the [Usage](PartitioningServiceUsing.md) doc. 
  


