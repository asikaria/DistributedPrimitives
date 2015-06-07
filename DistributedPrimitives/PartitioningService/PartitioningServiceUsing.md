:u5272:
## How do I use the partitioning service.

### `IParticipant` and `IClient`
The servers participating in the partitioning service use the IParticipant interface 
to talk to the partitioning service. They need to implement the `IParticipantCallback` 
interface to get callbacks/notifications from the partitioning service. 

### `IParticipant`
The client needs to instantiate an instance of a the service. This distribution includes a 
service implemented based on the Azure Table service. However, there could be other implementations
(based on Zookeeper, say) and that would be just fine - as long as your app just uses the `IParticipant`
interface, you can swap the implementation out later.

The methods in IParticipant are:

  - `void Init(int numPartitions, int heartbeatIntervalInSeconds, int leaseValidityInSeconds, int acquireLeaseOlderThanSeconds, int maxPartitionsPerNode, string nodeId, IParticipantCallbacks callbackObject)`
This Initilizes the service, and gets any background threads running if needed

  - `bool havePartition(string partitionID)`
This lets the app server query whether it still has ownership of the partition. This would typically be called by the app before it serves a request, to check if it still has the lease

  - `List<string> listOwnedPartitions()`
This lets the app query the list of partitions it owns

  - `void dropPartition(string PartitionID)`
This call lets the app drop a particular partition. The app should stop processing the partition _before_ making this call.

### `IClient`
The clients of your service need to interact with your service to find out which server to contact. Given a resource they need to contact, 
they first calculate the partition ID based on the hash function, and then contact the server that is serving that partition.

At start-up a client can call the `getPartitionMap` method to get and cache the full list of partitions and the nodes serving them. However, 
as servers are load-balanced or fail, the partition map held by the client will get obsolete. For a given request, the client uses the 
partition map it has in it's cache, and contacts the server pointed to by the partition map. If the server returns an error that says it is 
not the right server for that partitionID, then the client knows that it's entry for that partition is incorrect and it needs to update it. 
It calls `getOwnerNode` to find the right server and caches the new node ID. It then contacts this new server to fulfil that request. 
(There could be a race where the node has changed in the meantime. the client has to then repeat the discovery process again, until it 
finds the right server. The assumption is that the partition map does not change very frequently, so this should eventually lead to the 
right server).

  - `string getOwnerNode(string partitionID)`
Get the current node serving a partition

  - `Dictionary<string, string> getPartitionMap()`
Get the full partition map


### Parameterization

#### Lease renewal period
Lease renewal period is best decided based on app requirements.  Let's take an example with a 10-seconds lease renewal period.

Small intervals are better, because that results in better availability. Note that the period should be at least 2x larger 
than potential azure storage server+network delays, otherwise there is no benefit to a short interval, and it only results in 
extra lease renewal IOs. A larger lease renewal period results in fewer IOs to the table service.

#### Lease expiry time (time at which server should stop processing a partition)
Lease expiry time is the time it takes to safely assume that lease has not been renewed. This needs to be Lease renewal period + all the skews. So,
>  Lease Renewal Period 
>+ Clock drift (very small here)
>+ Round-trip time to renew lease (say, 10-sec server timeout + 2 sec network time + 1x more of both for retry)
>+ CPU time on local node (should be small)
This adds up to ~30 seconds for our example

The partitioning service will call dropPartition if this period elapses without the lease getting renewed.

#### Lease acquisition time (when it is safe to take over a lease)
This should be more than lease expiry time plus potential drift. Few seconds allowance should be ok. In this case we are using 45 secs. 

The main thing to guarantee for safety is to make sure that in no case does a new server take over processing a partition before the 
old server assumes itself dead. With the parameters above (10s renewal, 30s expiry), a 45-sec acquisition time is pretty safe.

#### Number of servers
Number of servers comes from load/scale characteristics of the app – i.e., how many total servers are needed to process the 
total load. This is the minimum number of servers needed. Add some resilience allowance to that, so the app can be tolerant 
to server failures.

Note that as the number of servers goes up, so does the number of IOs on the lease table, since each server does a full scan 
of all the rows in the lease table every renewal period. For current scale this will not be problem since we are well within 
the 20k tps limit of an azures torage account. As scale (number of partitions/number of servers) increases, the number of IOs
per second on the azure table should be evaluated to make sure it is within the 20K envelope.


#### Number of partitions
The number of partitions indicates the granularity at which the load can be distributed. If there is no load balancing needed, 
then we can go down to 1 or 2 partitions per server, so we can grow to thousands of servers. 

If load balancing is needed, then 1 or 2 per server does not provide enough granularity, and customer would want to keep at 
least 8-10 partitions per server for effective load balancing.

As the partition count goes up, the IOs on the lease table go up too. See discussion above in the section on “Number of Servers”.

#### Max Number of partitions per server
Max number of partitions is a simple calculation between server count and partition count, to make sure the number of servers 
we have is enough to serve the number of partitions we have. Max count = ceiling(# of partitions / # of servers). 
Once this is calculated, customer needs to throw in more servers, to ensure resilience when servers go down.


