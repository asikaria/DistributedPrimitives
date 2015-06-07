using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DistributedPrimitives.PartitioningService
{
    public interface IParticipant
    {
        void Init(int numPartitions, int heartbeatIntervalInSeconds, int leaseValidityInSeconds, int acquireLeaseOlderThanSeconds, int maxPartitionsPerNode, string nodeId, IParticipantCallbacks callbackObject);
        bool havePartition(string partitionID);
        List<string> listOwnedPartitions();
        void dropPartition(string PartitionID);
    }


    public interface IParticipantCallbacks
    {
        void gotPartition(string partitionID);
        void lostPartition(string partitionID);
        string getCurrentLoad(string partitionID);
    }


    public interface IClient
    {
        string getOwnerNode(string partitionID);

        Dictionary<string, string> getPartitionMap();


    }

}
