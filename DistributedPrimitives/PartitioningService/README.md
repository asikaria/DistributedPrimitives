:u5272:

### See:
  - *Motivation*:  [Why you need a partitioning service](PartitioningServiceMotivation.md)
  - *Usage*: [How to use it](PartitioningServiceUsing.md)
  - *Design*: [Design and Internals](PartitioningServiceDesign.md)
  
  
### Code:
  - `Interfaces.cs` defines the interfaces that a partitioning service needs to implement. It 
should be possible to build clients based on this interface alone. The implementation does 
not matter to the client.

  - `TablePartitioningService.cs` provides an implementation based on the table partitioning service. 
It implements all the methods needed by the participating servers and the clients of those servers that 
need tocontact the servers. It also implements a number of administrative mthods specific to the 
Azure table-based implementation.

