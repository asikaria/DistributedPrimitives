# Distributed Primitives
#### A Library of primitives needed to build resilient scalable distributed systems
*Because building a distributed system should not require a Ph.D.*

These are the libraries planned, currently only the first one is implemented:
  - [Partitioning service](DistributedPrimitives/PartitioningService/README.md)
    * Allows servers to divide the load among many servers, using a distributed lease mechanism
	* See: [Motivation](DistributedPrimitives/PartitioningService/PartitioningServiceMotivation.md); [Usage](DistributedPrimitives/PartitioningService/PartitioningServiceUsing.md); [Design](DistributedPrimitives/PartitioningService/PartitioningServiceDesign.md)
  - Command and Control
    * Allows you to set up a "command and control" server, so you can send admin commands to the servers in your cluster
  - Dynamic Configuration System
    * Allows configuration values to be updated dynamically at run-time, across many servers
  - Bit distribution
    * Allows you to distribute bits efficiently across machines, without making a single file share or blob as the bottleneck






