using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DistributedPrimitives.PartitioningService;

namespace PartitioningApp
{
    class PartitioningAppProgram
    {

        private const string storageAccountName = "thezoo";
        private const string storageAccountKey = "PYFm1lBrUWvyqFTj5QP9KcoQJSwBe7YFd6ZabJ6gTJI/gaAZK4OVRIWgeZGWn9oiJXMz0FQBOgYFLmfO29RrVQ==";
        private const string storageTableName = "zoo";

        static void Main(string[] args)
        {
            PartitioningApp app = new PartitioningApp();
            TablePartitioningService ps = TablePartitioningService.GetInstance(storageAccountName, storageAccountKey, storageTableName);
            Random rng = new Random();
            string nodeID = "N" + rng.Next(1000).ToString("D3");
            ps.Init(64, 10, 30, 43, 64, nodeID, app);
            Console.WriteLine("Started Node ID {0}", nodeID);

        }
    }

    public class PartitioningApp : IParticipantCallbacks
    {
        Dictionary<string, string> myPartitions = new Dictionary<string, string>(256);

        public void gotPartition(string partitionID)
        {
            Console.WriteLine("gotPartition  Called: {0}", partitionID);
            if (myPartitions.ContainsKey(partitionID))
            {
                Console.WriteLine("    Error***: already have the Partition: {0}", partitionID);
            }
            else
            {
                myPartitions.Add(partitionID, partitionID);
                Console.WriteLine("    Acquired Partition: {0}", partitionID);
            }
            return; 
        }

        public void lostPartition(string partitionID)
        {
            Console.WriteLine("lostPartition Called: {0}", partitionID);
            if (myPartitions.ContainsKey(partitionID))
            {
                myPartitions.Remove(partitionID);
                Console.WriteLine("    Removed Partition: {0}", partitionID);
            }
            else
            {
                Console.WriteLine("    Error***: don't have the Partition: {0}", partitionID);
            }
            return; 
        }

        public string getCurrentLoad(string partitionID)
        {
            string load = @"CPU:23;tps:40";
            //Console.WriteLine("getCurrentLoad Called: {0}", partitionID);
            if (myPartitions.ContainsKey(partitionID))
            {
                //Console.WriteLine("    Load for {0} is: {1}", partitionID, load);
            }
            else
            {
                Console.WriteLine("    Error***: getCurrentLoad called, but don't have the Partition: {0}", partitionID);
                load = "";
            }
            return load;
        }



    }





}
