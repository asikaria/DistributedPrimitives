:u5272:

A Partitioning service can be implemented using any structured storage that implements an atomic update 
interface, and can store named objects (e.g., a key/vanue store). It is commonly implemented using a 
consensus protocol/system, like [Zookeeper](https://zookeeper.apache.org/), or 
[Paxos](http://static.googleusercontent.com/media/research.google.com/en/us/archive/chubby-osdi06.pdf).
However, for most systems, running multiple servers just to provide atomic locking is overkill. Most apps 
that are not *huge* can use other hosted solutions that provide atomicity. For example, the Azure Table 
storage service. (Note that Azure storage [internally](http://sigops.org/sosp/sosp11/current/2011-Cascais/printable/11-calder.pdf) 
uses a lock service based on paxos, but that is completely transparent to the users of the service.

A service based on Azure tables can scale to hundreds of servers, with thousands of partitions. For 
most applications, this is sufficient scale.


### Implementation

For our examples, we will assume there are 1024 partitions being served by 64 servers.

The “lease table” has the following row schema:
 - *Partition key*: The partition ID
 - *Row key*: Doesn’t matter, use a fixed string like “0”
 - *Row Content*: The row content doesn’t matter as well, but can be used to put in diagnostic 
information: The name of the server holding the lease and the local time of the server the last 
time it acquired the lease.

Servers renew their leases every x minutes (say, 2 minutes for our discussion) for the partitions
they serve. To renew the lease, server updates the lease row for that partition, putting in the
server name and local timestamp in the row data. (This operation inherently updates the LMT of
the row, which is an important side-effect). In order to make sure it is renewing it’s own lease,
the update should be conditional based on ETag from the last renewal.

If a server is not able to renew a lease within a certain max interval (say 3 mins) then it should
stop processing the partition(s) for which it was unable to renew the lease. Reasons for being unable
to renew the lease can be many – local CPU load on the server, delay in updating lease table in 
table storage, delay in ack back from table storage, etc. Regardless, all operations that change 
state on the server should check the last time the lease was renewed and if it is more than the max 
of 3 mins, then the operation should not proceed.

Every 2 minutes the servers also pull the full list of leases (1024 rows from the lease table) and 
compares their ETag to the ETag that it last saw. If the ETag has not changed, then the row has not 
been updated in the last 4 minutes, meaning the lease has not been renewed. At this point, this 
server can take over the lease and start processing the orphaned partition. To take the lease, 
it just updates the row with it’s information, using the “if-match” header and specifying the 
etag of the row it read. If the write succeeds then it got the lease and can start processing 
the partition. If the write failed because of an etag conflict, then some other server has likely 
acquired the lease – it needs to read the row again to ensure that someone has the lease.

### Other Considerations
#### Capacity and resilience
  - In order to avoid one server taking all the partitions, the servers need to be configured with 
a max number of partitions they will serve (say, MAXCOUNT. Set MAXCOUNT to 16 in the example above).
  - Customer can add servers as he goes to add more scale. To distribute load, customer will need 
to add servers to rotation, and decrease the max count each server can take (to increase server 
count from 64 to 128, the MAXCOUNT will decrease from 16 to 8). `Max count = ceiling(# of partitions / # of servers)`.
  - If a server goes down, then the partition it serves need to be served by other servers. But 
if all severs are at their configured max, then no other server will pick up these partitions. 
To build resilience, customer should deploy more servers than are minimally needed for running 
the service. So for example, for the above service, customer might go with 70 servers in rotation 
instead of the needed 64 minimum (the MAXCOUNT should based on 64, and the actual number of 
servers should be 64+some tolerance (70 in this case)).

#### Load Balancing
With the above mechanism there is no way to balance load between servers – servers will greedily take all 
the partitions upto the configured MAXCOUNT. Servers can do load balancing in a decentralized manner
(meaning each server makes its own load balancing decisions, without needing a central "master"). Each 
server can decide if it is getting overloaded. If so, it can just drop some partitions (indicate lease 
breakage in a field in the lease table row and stop renewing) and other servers will pick it up as long
as there is aggregate capacity available. It is also possible to make the load balancing decision based
on global information, if each server publishes it’s load stats in the lease table as well (in 
`TablePartitioningService`, the publishing is implemented, but load balancing based on that published 
information is not implemented).



#### Azure Table Scale
With the parameters above the main concern is the tps the lease table will sustain. 

All partitions are getting their lease renewed every 2 mins, so we are updating 1024 rows 
every 120 secs – this amounts to ~8.6 entities per second. This can be distributed well if 
the servers are renewing leases in a randomized staggered manner rather than aligned at 
the 2-min UTC boundary. So each server should renew lease every 2 mins, but the 
2-min period should not begin at even 2-min clock times.

The second source of tps is the checking of leases every 2 mins. All servers (64 of them) 
are reading all 1024 rows every 2 mins. This results in 64*1024/120 = ~546 entities per second. 


#### Clock drift/skew (client side)
Clock skew (difference in time between servers) on the client servers will not make any difference, 
since no decisions are being made based on the client’s clock time. 

#### Clock drift (difference in clock rate between servers) – the decisions for lease renewal are based 
on elapsed times as measured by local clock time. Per [research](http://research.google.com/archive/spanner-osdi2012.pdf), measured clock drifts for the 
2-min max period here should be less than 0.05 seconds, so it should not be a concern in general. 
The time to do the table operation will be a far greater source of variability than the clock drift 
itself.

#### Clock Drift/Skew (on storage servers)
Clock skew and drift on storage servers do not matter either, since none of the decisions are made 
on the actual time returned in the table row timestamp or ETag. 

#### Two servers serving the same partition
The main safety consideration is that two servers should never serve the same partition. By enforcing 
the lease mechanism as described above, that is guaranteed. The key thing is to make sure the 
server unilaterally stops processing if it does not have a current lease. 

#### Servers going down
Servers going down should not be concern, as long as there are other servers available to take up the 
load. If the number of servers in rotation is higher than the number of servers that can go down concurrently, 
there will be enough capacity available to serve the load.

Note that there could be small periods of unavailability when a server goes down, until it’s lease 
expires and another server picks it up.

#### Monitoring
To monitor this system, customer should monitor the lease table (every x mins) to ensure there are no 
leases older than 5 mins (since it takes 4 mins + some seconds for a lost lease to be picked up by 
another server). Also, customer should monitor the liveness of the servers and make sure no less 
than the minimum + some threshold of servers are up. For example, with the above parameters 
(64 minimum servers, deployment has 70 servers), customer may want to alert if the number of servers 
that are up goes down to 66 or below.

The admin service has a detectStale command-line mode, that implements this logic.



### Error Cases

#### Lost messages
For lease renewal, what matters is the end-to-end time to receive an ack back from table storage. Until 
the ack is received, the partition is not renewed, so far as the server is concerned. So if the lease 
renewal is written into the row and the ack lost, then the partition will not be eligible for taking 
by another server for another 4 mins, but then will be picked up by another server. Note that the 
old server will not go into a loop of perpetually renewing the lease (and keeping the partition 
offline because of lost messages) because it’s 3-min timer for giving up the partition will expire 
after the lease is lost. Also, if another server then acquires the lease, then this server cannot 
renew it again because it’s ETag wont match.

#### Delayed messages
The case here is if old renewal or lease acquire messages make it to table storage potentially ending up 
with conflicting leases. This cant happen because all lease renewals and acquires are done with ETag. 
So old update requests won’t succeed since the ETags won’t match on the server side.

#### Azure storage down
If Azure table storage is down, then all the leases will expire after 3 mins and the servers will stop 
processing the partitions. So there will be complete availability loss after 3 mins.

#### Azure storage Throttling/Flakiness
Note that the with the current parameterization, we are well within the tps limits, so throttling caused 
by this design should be minimal. But still, there may be innocent bystander situations where the target 
partition/server could start throttling (at least until XStore-side load balancing kicks in).

If Azure storage throttles the partition, then some of the lease renewal requests may fail. If it 
fails repeatedly, then the 3-min timer may expire and the partition will stop processing the 
partition. If this happens randomly to many requests, then partitions will sporadically get 
acquired and dropped on various servers, as the lease renewal/acquire requests go through.

To mitigate against this, a nice-to-have feature would be throttling detection. If throttling 
is detected, then the app should dial down the scans for expired leases, while keeping the lease 
renewal messages intact. This will prevent new partitions from going offline, while some of the 
already lost partitions stay offline – this is better than all traffic getting throttled and 
even more partitions going offline. This is not implemented yet.

### Manual Control

#### *Bumping* a partition
This might be needed if we need to “reload” the partition for some reason.
Bumping a partition involves simply changing the lease row. While any update will do the trick, 
it is sensible to have the change be to add a property indicating the action taken. With an update 
the ETag will change, thereby preventing the existing server from renewing the lease. Of course, 
this can also be achieved through out-of-band mechanisms like direct control message to the 
concerned server.

#### Preventing particular partition from loading on a particular server
Through a tool add, a column to the lease table (“Prohibited servers list”). This will cause the 
ETag to change causing the current server to drop the partition – and any further lease acquisitions 
can be ok since servers can look at this column as part of the scan to acquire leases.

This can also be achieved out-of-band by Dynamic Config on the server (“Prohibited Partition IDs list”), 
which it uses to never renew or acquire leases for. 

While support for this is implemented in the table partitioning service, a dynamic config system is better, 
because the prohibited servers list is lost at the next lease renewal (since the entire row is replaced by 
the acquiring server).

#### Taking a partition offline
This might be needed if a partition’s data is causing servers to crash, or if we know there is a data issue 
on the partition and we need to prevent it from creating more garbage. This can be achieved by deleting the 
row for the partition from the lease table. Renewal will then fail because update to the row will fail. And 
no new servers will acquire it because it will not show up in the scan.

#### Taking a server offline
There are a number of options for doing this:
  - Take a server offline is through the service management API operation, so the server is reliably shut down. This 
is a non-graceful shutdown, and the partition will stay offline for up to 4 minutes of lease table scan interval.
  - Do it through out-of-band mechanisms through direct message to server instructing it to shut down, thereby 
immediately giving up the lease by calling `DropPartition`.
  - Have  dynamic configuration on the server (like a “poison pill” setting) that, when set to that server, 
causes it to go down/restart.
  - Use the “prohibited servers list” column (described above) to every row in the lease table. But that will 
cause the servers to also lose the current leases (since ETag changes), thereby causing all partitions to 
have to reload. This may not be the desired effect. Not a good option.
  - Have a separate table of “prohibited servers” that the servers periodically look for their name in, and then go 
offline if they find a row for their name. But at this point, this is just another way of doing a limited 
dynamic config system. This is not implemented in the TablePartitioningService.

#### Upgrades
For upgrades walk, it may be desirable to take a server about to be upgraded offline gracefully as a prep step. See options 
for “Taking a Server Offline” above.

#### Initial build-out
The lease table has to be populated initially with the list of partitions. The admin app has a command-line 
option (create) to create the desired number of partitions.

###  Monitoring to mitigate against manual control mistakes
Each of these manual controls can be used in case of a Live Site Incident. However, it is possible to forget 
to undo the action after the incident. If partitions are taken offline by deleting lease table rows, then 
periodic monitoring should be added to ensure the table has all the rows (just scan and make sure you see 
partition keys 0 through 1023). If servers are taken offline (or prohibited by dynamic config – so they are 
up but not participating), then they should be included in the server count monitoring to make sure when you 
account for downed servers, these are included.

This is not implemented in TablePartitioningService, but needs to be done from outside. There is an admin command line
option that shows all partitions, that can be used to implement this monitoring.

