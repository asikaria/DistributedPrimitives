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

