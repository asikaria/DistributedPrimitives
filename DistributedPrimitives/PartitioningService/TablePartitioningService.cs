using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using System.Net;
using System.Threading;
using System.Web;
using System.Diagnostics;


namespace DistributedPrimitives.PartitioningService
{
    public class TablePartitioningService : IParticipant, IClient
    {
        private int NumPartitions;
        private int heartbeatIntervalInSeconds;
        private IParticipantCallbacks callbackObject;
        private int maxPartitionsPerNode = 16;
        private string nodeId;
        private int leaseValidityInSeconds;
        private int acquireLeaseOlderThanSeconds;
        private int maxPartitionAcquiresPerCycle = 3;

        private string storageAccountName;
        private string storageAccountKey;
        private string tableName;

        private Thread t;
        bool initialized = false;

        private Dictionary<string, Lease> allLeases = new Dictionary<string, Lease>(8192);
        private int numLeasesHeld;

        private const string NodeIdPropertyName = "NodeID";
        private const string LoadDataPropertyName = "LoadData";
        private const string PartitionInvalidPropertyName = "PartitionInvalid";
        private const string DisallowedNodePropertyName = "DisallowedNode";
        private const string LeaseGivenUpPropertyName = "LeaseGivenUp";
        

        // Usually the accesses to the lease table is done by the one partitioning service thread. 
        // However, the IParticipant calls happen on the app's thread(s), so access to the lease table 
        // needs to be synchronized
        private object lockObj = new Object();
        
        public static TablePartitioningService GetInstance(string storageAccountName, string storageAccountKey, string tableName)
        {
            ServicePointManager.DefaultConnectionLimit = 100;
            ServicePointManager.Expect100Continue = false;
            ServicePointManager.UseNagleAlgorithm = false;
            ServicePointManager.CheckCertificateRevocationList = false;
            
            TablePartitioningService p = new TablePartitioningService();
            p.storageAccountName = storageAccountName;
            p.storageAccountKey = storageAccountKey;
            p.tableName = tableName;
            return p;
        }

        public void Init(int numPartitions, int heartbeatIntervalInSeconds, int leaseValidityInSeconds, int acquireLeaseOlderThanSeconds, int maxPartitionsPerNode, string nodeId, IParticipantCallbacks callbackObject)
        {
            lock (lockObj)
            {
                if (!initialized)
                {
                    this.NumPartitions = numPartitions;
                    this.heartbeatIntervalInSeconds = heartbeatIntervalInSeconds;
                    this.callbackObject = callbackObject;
                    this.maxPartitionsPerNode = maxPartitionsPerNode;
                    this.nodeId = nodeId;  // node ID for this node
                    this.leaseValidityInSeconds = leaseValidityInSeconds;
                    this.acquireLeaseOlderThanSeconds = acquireLeaseOlderThanSeconds;

                    CheckTable(); // create Table Rows
                    GetMaxPartitions(); 
                    t = new Thread(threadMethod);
                    t.Start();  
                    initialized = true;
                }
            }
            Console.WriteLine("Started Thread");
        }


        private void CheckTable()
        {
            StorageCredentials creds = new StorageCredentials(storageAccountName, storageAccountKey);
            CloudStorageAccount storageAccount = new CloudStorageAccount(creds, true);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference(tableName);

            // Running a query has the effect of validating that account exists and is reachable, key is correct, table exists
            TableQuery query = new TableQuery();
            query.FilterString = "PartitionKey eq 'Control' and RowKey eq 'Created'";
            IEnumerable<DynamicTableEntity> results = table.ExecuteQuery(query);        // this will throw StorageException for any issues with the account or table
            if (results.Count() <= 0) throw new ArgumentException(String.Format("Partition rows have not been created in Table {0}", tableName));

        }


        private void GetMaxPartitions()
        {
            StorageCredentials creds = new StorageCredentials(storageAccountName, storageAccountKey);
            CloudStorageAccount storageAccount = new CloudStorageAccount(creds, true);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference(tableName);

            TableQuery query;
            IEnumerable<DynamicTableEntity> results;

            query = new TableQuery();
            query.FilterString = "PartitionKey eq 'Control' and RowKey eq 'MaxPartitionsPerNode'";
            results = table.ExecuteQuery(query);
            foreach (DynamicTableEntity row in results)
            {
                this.maxPartitionsPerNode = (int)row["MaxPartitions"].Int32Value;
            }

            query = new TableQuery();
            query.FilterString = "PartitionKey eq 'Control' and RowKey eq 'MaxPartitionAcquiresPerCycle'";
            results = table.ExecuteQuery(query);
            foreach (DynamicTableEntity row in results)
            {
                this.maxPartitionAcquiresPerCycle = (int)row["MaxPerCycle"].Int32Value;
            }
        }


        private void threadMethod()
        {
            while (true)
            {
                lock (lockObj)
                {
                    RefreshLeaseStateFromServer();
                }
                Thread.Sleep(heartbeatIntervalInSeconds * 1000);
            }
        }



        /*
         * Server Table Schema:
         * Partition Key:    Partition ID
         * Row Key:          "0"  (not used)
         * NodeId:           ID of the node that currently owns the lease (may be expired)
         * LoadData:         opaque string containing load information for that partition. This can be used for load-balancing operations in future (not implemented currently)
         * LeaseGivenUp:     last holder has voluntarily relinquished lease. Ok to acquire even if ETag refresh is recent
         * PartitionInvalid: partition is not valid to load (admin can set this). Drop partition if owning. Do not acquire at acquire time. 
         * DisallowedNode:   NodeID of node not allowed to load this partition (admin can set this)
        */

        private void RefreshLeaseStateFromServer()
        {
            Console.Write("Renew Leases: Scanning: ");
            Stopwatch stopwatch = Stopwatch.StartNew();

            List<Lease> LeasesToRenew = new List<Lease>(maxPartitionsPerNode);    // leases we own, that can be renewed
            List<Lease> LeasesLost = new List<Lease>(maxPartitionsPerNode);       // leases we own that we need to give up
            List<Lease> LeasesToAcquire = new List<Lease>(maxPartitionsPerNode);  // leases that can be acquired
            List<Lease> LeasesToDelete = new List<Lease>(maxPartitionsPerNode);   // partition IDs that are not valid anymore

            lock (lockObj)
            {
                // Mark part of mark-and-sweep GC for invalid lease entries in local lease table
                foreach (Lease lease in allLeases.Values) { lease.MarkedforGC = true; }
            }

            //update local lease table from Server
            StorageCredentials creds = new StorageCredentials(storageAccountName, storageAccountKey);
            CloudStorageAccount storageAccount = new CloudStorageAccount(creds, true);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference(tableName);
            TableQuery query = new TableQuery();
            query.FilterString = "PartitionKey gt 'x' and PartitionKey lt 'y'";    //Partition IDs are always 'x' followed by a hex number. 
            IEnumerable<DynamicTableEntity> results = table.ExecuteQuery(query);
            DateTime now = DateTime.Now;
            DateTime AquirableLeaseTimestamp = now.AddSeconds(-this.acquireLeaseOlderThanSeconds);
            DateTime StaleLeaseTimestamp = DateTime.Now.AddSeconds(-this.leaseValidityInSeconds);
            foreach (DynamicTableEntity row in results)
            {
                // The lock should be inside the loop, since results is not a segmented query - the enumerator 
                // will make a server call to get next continuation. We cannot keep lock across a 
                // sever call. One bad side effect is that for most rows, this will do a lot of repeated 
                // acquire/release of the lock. Usually the lock is uncontended, so it should not be a problem
                lock (lockObj)
                {
                    Lease lease;
                    if (!allLeases.ContainsKey(row.PartitionKey))
                    {   // this is a new partition we dont know about. Add it to our list
                        lease = new Lease(row.PartitionKey);
                        lease.RowId = row.RowKey;
                        lease.ETag = row.ETag;
                        lease.LastRefreshed = now;
                        lease.LoadData = (row.Properties[LoadDataPropertyName] != null) ? row.Properties[LoadDataPropertyName].StringValue : "";
                        lease.NodeId = (row.Properties[NodeIdPropertyName] != null) ? row.Properties[NodeIdPropertyName].StringValue : "";
                        lease.PartitionInvalid = (row.Properties[PartitionInvalidPropertyName] != null) ? (row.Properties[PartitionInvalidPropertyName].BooleanValue ?? false) : false;
                        lease.DisallowedNodeId = (row.Properties[DisallowedNodePropertyName] != null) ? row.Properties[DisallowedNodePropertyName].StringValue.Trim().ToLower() : "";
                        lease.HaveLease = false;
                        lease.MarkedforGC = false;
                        allLeases.Add(lease.PartitionId, lease);
                        lease = null;
                    }


                    lease = allLeases[row.PartitionKey];
                    lease.MarkedforGC = false;                // valid row, do not GC
                    lease.RowId = row.RowKey;                 // RowId is not used, so this should generally be a no-op
                    bool ETagChanged = false;
                    if (lease.ETag != row.ETag)
                    {
                        ETagChanged = true;
                        lease.ETag = row.ETag;
                        lease.LastRefreshed = now;            // this is the time (client time) we first saw this etag value
                    }
                    lease.LoadData = (row.Properties[LoadDataPropertyName] != null) ? row.Properties[LoadDataPropertyName].StringValue : "";
                    lease.NodeId = (row.Properties[NodeIdPropertyName] != null) ? row.Properties[NodeIdPropertyName].StringValue : "";
                    lease.PartitionInvalid = (row.Properties[PartitionInvalidPropertyName] != null) ? (row.Properties[PartitionInvalidPropertyName].BooleanValue ?? false) : false;
                    lease.DisallowedNodeId = (row.Properties[DisallowedNodePropertyName] != null) ? row.Properties[DisallowedNodePropertyName].StringValue.Trim().ToLower() : "";
                    bool LeaseGivenUp = (row.Properties[LeaseGivenUpPropertyName] != null) ? (row.Properties[LeaseGivenUpPropertyName].BooleanValue ?? false) : false;
                    if (lease.HaveLease)
                    {
                        // if the Etag is not what we know, then someone else must have got it or invalidated it
                        // anyone else touching the row while we have the lease is equivalent to invalidatng the lease
                        // Also, of course, if we have not kept the lease current then someone else could have acquired it, so we need to give it up
                        bool stale = (lease.LastRefreshed < StaleLeaseTimestamp);
                        if (!ETagChanged && !stale)
                        {
                            LeasesToRenew.Add(lease);
                        }
                        else
                        {
                            LeasesLost.Add(lease);
                        }
                    }
                    else
                    {
                        if (lease.LastRefreshed < AquirableLeaseTimestamp || LeaseGivenUp)   // Lease's ETag has not been refreshed for long time, or last holder has given up the lease
                        {
                            if (!lease.PartitionInvalid && !(lease.DisallowedNodeId == this.nodeId))
                            {
                                LeasesToAcquire.Add(lease);
                            }
                        }
                    }
                }
            }

            stopwatch.Stop();
            Console.WriteLine(" {0} ms", stopwatch.ElapsedMilliseconds);

            lock (lockObj)
            {
                // Sweep part of mark-and-sweep GC for invalid lease entries in local lease table
                foreach (KeyValuePair<string, Lease> kvp in allLeases)
                {
                    Lease lease = kvp.Value;
                    if (lease.MarkedforGC)
                    {
                        LeasesToDelete.Add(lease);
                    }
                }
            }

            foreach (Lease lease in LeasesLost)
            {
                RemoveLease(lease);
            }

            Console.WriteLine("              Acquiring: ");
            stopwatch.Restart();
            int acquirecount = 0;
            foreach (Lease lease in LeasesToAcquire) {
                if (AcquireLease(lease)) acquirecount++;
                if (acquirecount >= maxPartitionAcquiresPerCycle) break;
            }
            stopwatch.Stop();
            Console.WriteLine("              Acquire time: {0} ms", stopwatch.ElapsedMilliseconds);

            Console.Write("              Renewing: ");
            stopwatch.Restart();
            foreach (Lease lease in LeasesToRenew)
            {
                RenewLease(lease);
            }
            stopwatch.Stop();
            Console.WriteLine("{0} ms ", stopwatch.ElapsedMilliseconds);

            foreach (Lease lease in LeasesToDelete)  { DeleteLocalLeaseEntry(lease);  }

            Console.WriteLine("              Node: {0}", this.nodeId);
        }


        private void DeleteLocalLeaseEntry(Lease lease)
        {
            if (lease.HaveLease)
            {
                RemoveLease(lease);
            }
            if (allLeases.ContainsKey(lease.PartitionId))
            {
                allLeases.Remove(lease.PartitionId);
            }
        }

        private void RenewLease(Lease lease)
        {
            {
                string LoadData = callbackObject.getCurrentLoad(lease.PartitionId); // TODO: Add robustness here: exception, long running time, etc.

                StorageCredentials creds = new StorageCredentials(storageAccountName, storageAccountKey);
                CloudStorageAccount storageAccount = new CloudStorageAccount(creds, true);
                CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
                CloudTable table = tableClient.GetTableReference(tableName);
                DynamicTableEntity leaseRowInTable = new DynamicTableEntity(lease.PartitionId, lease.RowId);
                leaseRowInTable.Properties[NodeIdPropertyName] = new EntityProperty(this.nodeId);
                leaseRowInTable.Properties[LoadDataPropertyName] = new EntityProperty(LoadData);  
                leaseRowInTable.Properties[LeaseGivenUpPropertyName] = new EntityProperty(false);
                leaseRowInTable.Properties[PartitionInvalidPropertyName] = new EntityProperty(false);
                leaseRowInTable.Properties[DisallowedNodePropertyName] = new EntityProperty("");
                leaseRowInTable.ETag = lease.ETag;
                TableOperation leaseUpdateOperation = TableOperation.Replace(leaseRowInTable);
                try
                {
                    TableResult tableResult = table.Execute(leaseUpdateOperation);
                    lock (lockObj)
                    {
                        lease.ETag = ((DynamicTableEntity)tableResult.Result).ETag;
                        lease.LastRefreshed = DateTime.Now;
                        lease.LoadData = LoadData;
                        lease.NodeId = this.nodeId;
                        lease.PartitionInvalid = false;
                        lease.DisallowedNodeId = "";
                        lease.HaveLease = true;
                        // do not touch: lease.RowId, lease.MarkedForGC
                    }
                }
                catch (StorageException ex)
                {
                    Console.WriteLine("Failed to Renew lease for partition {0}", lease.PartitionId);
                    Console.WriteLine("     Exception {0} ({1}): {2}", ex.RequestInformation.HttpStatusCode, ex.RequestInformation.HttpStatusMessage, ex.Message);
                    if (ex.RequestInformation.HttpStatusCode == 412)
                    {
                        RemoveLease(lease);    // if I fail to renew (someone else has it), then I've lost it
                    }
                    // swallow - failed to renew lease
                    // TODO: Log it
                }
            }
        }

        private void RemoveLease(Lease lease)
        {
            callbackObject.lostPartition(lease.PartitionId);
            lock (lockObj)
            {
                lease.HaveLease = false;
                numLeasesHeld--;
            }
        }


        private void RemoveLeaseOnServer(Lease lease)
        {
            RemoveLease(lease);
            
            // All of the remaining is a performance optimization to let the partition be reacquired by someone else sooner
            // Correctness is not impacted if this fails - in that case this partition will just wait for the full 
            // Lease validity period before being acquired by someone else
            StorageCredentials creds = new StorageCredentials(storageAccountName, storageAccountKey);
            CloudStorageAccount storageAccount = new CloudStorageAccount(creds, true);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference(tableName);

            DynamicTableEntity leaseRowInTable = new DynamicTableEntity(lease.PartitionId, lease.RowId);
            leaseRowInTable.Properties[NodeIdPropertyName] = new EntityProperty("");
            leaseRowInTable.Properties[LoadDataPropertyName] = new EntityProperty("");
            leaseRowInTable.Properties[LeaseGivenUpPropertyName] = new EntityProperty(true);
            leaseRowInTable.Properties[PartitionInvalidPropertyName] = new EntityProperty(false);
            leaseRowInTable.Properties[DisallowedNodePropertyName] = new EntityProperty("");
            leaseRowInTable.ETag = lease.ETag;
            TableOperation leaseUpdateOperation = TableOperation.Replace(leaseRowInTable);
            try
            {
                TableResult tableResult = table.Execute(leaseUpdateOperation);
                lock (lockObj)
                {
                    lease.ETag = ((DynamicTableEntity)tableResult.Result).ETag;
                    lease.LastRefreshed = DateTime.Now;
                    lease.LoadData = "";
                    lease.NodeId = "";
                    lease.PartitionInvalid = false;
                    lease.HaveLease = false;
                    lease.DisallowedNodeId = "";
                    // do not touch: lease.RowId, lease.MarkedForGC
                }
            }
            catch (StorageException ex)
            {
                Console.WriteLine("Failed to acquire lease for eligible partition {0}", lease.PartitionId);
                Console.WriteLine("     Exception {0}({1}): {2}", ex.RequestInformation.HttpStatusCode, ex.RequestInformation.HttpStatusMessage, ex.Message);
                // swallow - failed to acquire lease
                // TODO: Log it
            }
        }

        private bool AcquireLease(Lease lease)
        {
            bool acquired = false;

            if (numLeasesHeld < maxPartitionsPerNode)
            {
                StorageCredentials creds = new StorageCredentials(storageAccountName, storageAccountKey);
                CloudStorageAccount storageAccount = new CloudStorageAccount(creds, true);
                CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
                CloudTable table = tableClient.GetTableReference(tableName);

                DynamicTableEntity leaseRowInTable = new DynamicTableEntity(lease.PartitionId, lease.RowId);
                leaseRowInTable.Properties[NodeIdPropertyName] = new EntityProperty(this.nodeId);
                leaseRowInTable.Properties[LoadDataPropertyName] = new EntityProperty("");
                leaseRowInTable.Properties[LeaseGivenUpPropertyName] = new EntityProperty(false);
                leaseRowInTable.Properties[PartitionInvalidPropertyName] = new EntityProperty(false);
                leaseRowInTable.Properties[DisallowedNodePropertyName] = new EntityProperty("");
                leaseRowInTable.ETag = lease.ETag;
                TableOperation leaseUpdateOperation = TableOperation.Replace(leaseRowInTable);
                try
                {
                    TableResult tableResult = table.Execute(leaseUpdateOperation);
                    lock (lockObj)
                    {
                        lease.ETag = ((DynamicTableEntity)tableResult.Result).ETag;
                        lease.LastRefreshed = DateTime.Now;
                        lease.LoadData = "";
                        lease.NodeId = this.nodeId;
                        lease.PartitionInvalid = false;
                        lease.HaveLease = true;
                        lease.DisallowedNodeId = "";
                        // do not touch: lease.RowId, lease.MarkedForGC
                        numLeasesHeld++;
                        acquired = true;
                    }
                    this.callbackObject.gotPartition(lease.PartitionId);
                }
                catch (StorageException ex)
                {
                    Console.WriteLine("Failed to acquire lease for eligible partition {0}", lease.PartitionId);
                    Console.WriteLine("     Exception {0}({1}): {2}", ex.RequestInformation.HttpStatusCode, ex.RequestInformation.HttpStatusMessage, ex.Message);
                    // swallow - failed to acquire lease
                    // TODO: Log it
                }
            }
            return acquired;
        }

        private class Lease
        {
            public readonly string PartitionId;
            public string RowId;
            public string ETag;
            public DateTime LastRefreshed; // client time
            public string LoadData;
            public string NodeId;
            public bool PartitionInvalid;
            public string DisallowedNodeId;
            public bool HaveLease;
            public bool MarkedforGC;

            public Lease(string partitionId)
            {
                this.PartitionId = partitionId;
            }
        }

        /*
         * 
         * IParticipant Methods
         * 
        */

        public bool havePartition(string partitionID)
        {
            if (!initialized) throw new InvalidOperationException("Partitioning Service instance has not been inititalized yet");

            // TODO: This is a problem. Callers will call this many times, but may get stuck on this 
            //       lock while the partitioning service is updating the tables from Azure tables
            lock (lockObj)
            {
                if (allLeases.ContainsKey(partitionID)) return allLeases[partitionID].HaveLease;
                return false;
            }
        }

        public List<string> listOwnedPartitions()
        {
            if (!initialized) throw new InvalidOperationException("Partitioning Service instance has not been inititalized yet");
            List<string> list = new List<string>(maxPartitionsPerNode);
            lock (lockObj)
            {
                foreach (KeyValuePair<string, Lease> kvp in allLeases)
                {
                    list.Add(kvp.Value.PartitionId);
                }
                return list;
            }
        }

        public void dropPartition(string PartitionID)
        {
            if (!initialized) throw new InvalidOperationException("Partitioning Service instance has not been inititalized yet");
            Lease lease = null;
            lock (lockObj)
            {
                if (allLeases.ContainsKey(PartitionID))
                {
                    lease = allLeases[PartitionID];
                }
            }
            if (lease!=null) RemoveLeaseOnServer(lease);
        }


        /*
         * 
         * IClient Methods
         * 
        */

        public string getOwnerNode(string partitionID)
        {
            string owner = null;

            StorageCredentials creds = new StorageCredentials(storageAccountName, storageAccountKey);
            CloudStorageAccount storageAccount = new CloudStorageAccount(creds, true);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference(tableName);

            //update local lease table from Server
            TableQuery query = new TableQuery();
            query.FilterString = String.Format(@"PartitionKey eq '{0}' and PartitionKey eq '0'", partitionID);
            IEnumerable<DynamicTableEntity> results = table.ExecuteQuery(query);
            foreach (DynamicTableEntity row in results)
            {
                if (row.Properties.ContainsKey(NodeIdPropertyName))
                {
                    owner = row.Properties[NodeIdPropertyName].StringValue;
                }
            }
            return owner;
        }

        public Dictionary<string, string> getPartitionMap()
        {
            Dictionary<string, string> result = new Dictionary<string, string>(8192);

            StorageCredentials creds = new StorageCredentials(storageAccountName, storageAccountKey);
            CloudStorageAccount storageAccount = new CloudStorageAccount(creds, true);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference(tableName);

            //update local lease table from Server
            TableQuery query = new TableQuery();
            query.FilterString = "PartitionKey gt 'x' and PartitionKey lt 'y'";
            IEnumerable<DynamicTableEntity> results = table.ExecuteQuery(query);
            foreach (DynamicTableEntity row in results)
            {
                if (row.Properties.ContainsKey(NodeIdPropertyName))
                {
                    result[row.PartitionKey] = row.Properties[NodeIdPropertyName].StringValue;
                }
            }
            return result;
        }



        /*
         * 
         * Admin Methods
         * 
        */

        public void createTable(int numPartitions, int maxPartitionsPerNode, int maxPartitionAcquiresPerCycle)
        {
            StorageCredentials creds = new StorageCredentials(storageAccountName, storageAccountKey);
            CloudStorageAccount storageAccount = new CloudStorageAccount(creds, true);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference(tableName);

            if (numPartitions > 0xffff) { throw new ArgumentException(String.Format("Maximum number of partitions is {0}", 0xffff)); }
            if (numPartitions < maxPartitionsPerNode) { throw new ArgumentException("maxPartitionsPerNode is more than numPartitions"); }

            if (table.CreateIfNotExists())
            {   // if table was created in the CreateIfNotExists call
                for (int i = 0; i < numPartitions; i++)
                {   // create rows for all partitions. This should only be called once in the app's lifetime.
                    string partitionKey = String.Format("x{0:x4}", i);
                    string rowKey = "0";
                    DynamicTableEntity row = new DynamicTableEntity(partitionKey, rowKey);
                    row.Properties[NodeIdPropertyName] = new EntityProperty("none");
                    row.Properties[LoadDataPropertyName] = new EntityProperty("");  
                    row.Properties[LeaseGivenUpPropertyName] = new EntityProperty(true);  
                    row.Properties[PartitionInvalidPropertyName] = new EntityProperty(false);  
                    row.Properties[DisallowedNodePropertyName] = new EntityProperty("");  
                    TableOperation insertOperation = TableOperation.Insert(row);
                    table.Execute(insertOperation);
                }

                DynamicTableEntity controlRow;
                TableOperation controlInsertOperation;

                controlRow = new DynamicTableEntity("Control", "Created");
                controlInsertOperation = TableOperation.Insert(controlRow);
                table.Execute(controlInsertOperation);

                controlRow = new DynamicTableEntity("Control", "MaxPartitionsPerNode");
                controlRow.Properties.Add(new KeyValuePair<string, EntityProperty>("MaxPartitions", new EntityProperty(maxPartitionsPerNode)));
                controlInsertOperation = TableOperation.Insert(controlRow);
                table.Execute(controlInsertOperation);

                controlRow = new DynamicTableEntity("Control", "MaxPartitionAcquiresPerCycle");
                controlRow.Properties.Add(new KeyValuePair<string, EntityProperty>("MaxPerCycle", new EntityProperty(maxPartitionAcquiresPerCycle)));
                controlInsertOperation = TableOperation.Insert(controlRow);
                table.Execute(controlInsertOperation);
            }
            else
            {
                throw new ArgumentException("table {0} already exists", tableName);
            }
        }

        public void deleteTable()
        {
            StorageCredentials creds = new StorageCredentials(storageAccountName, storageAccountKey);
            CloudStorageAccount storageAccount = new CloudStorageAccount(creds, true);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference(tableName);

            table.DeleteIfExists();
        }

        public void kickPartition(string partitionID, string disallowedNode = "")
        {
            if (disallowedNode == null) { disallowedNode = "";  }
            disallowedNode = disallowedNode.Trim();

            StorageCredentials creds = new StorageCredentials(storageAccountName, storageAccountKey);
            CloudStorageAccount storageAccount = new CloudStorageAccount(creds, true);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference(tableName);

            DynamicTableEntity leaseRowInTable = new DynamicTableEntity(partitionID, "0");
            leaseRowInTable.Properties[NodeIdPropertyName] = new EntityProperty("kicked");
            leaseRowInTable.Properties[LoadDataPropertyName] = new EntityProperty("");
            leaseRowInTable.Properties[LeaseGivenUpPropertyName] = new EntityProperty(false);
            leaseRowInTable.Properties[PartitionInvalidPropertyName] = new EntityProperty(false);
            leaseRowInTable.Properties[DisallowedNodePropertyName] = new EntityProperty(disallowedNode);
            leaseRowInTable.ETag = "*";

            TableOperation leaseUpdateOperation = TableOperation.Replace(leaseRowInTable); 
            TableResult tableResult = table.Execute(leaseUpdateOperation);
        }

        public List<Dictionary<string, string>> getAllPartitionsInfo()
        {
            List<Dictionary<string, string>> result = new List<Dictionary<string, string>>(8192);
            
            StorageCredentials creds = new StorageCredentials(storageAccountName, storageAccountKey);
            CloudStorageAccount storageAccount = new CloudStorageAccount(creds, true);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference(tableName);

            //update local lease table from Server
            TableQuery query = new TableQuery();
            query.FilterString = "PartitionKey gt 'x' and PartitionKey lt 'y'";
            IEnumerable<DynamicTableEntity> results = table.ExecuteQuery(query);
            foreach (DynamicTableEntity row in results)
            {
                Dictionary<string, string> dict = new Dictionary<string, string>(16);
                dict["partitionID"] = row.PartitionKey;
                dict["ETag(URLDecoded)"] = HttpUtility.UrlDecode(row.ETag);
                dict["TimeStamp"] = row.Timestamp.ToString("O");
                foreach (KeyValuePair<string, EntityProperty> kvp in row.Properties)
                {
                    dict[kvp.Key] = EntityPropertyToString(kvp.Value);
                }
                result.Add(dict);
            }
            return result;
        }

        private static string EntityPropertyToString(EntityProperty e)
        {
            switch (e.PropertyType)
            {
                case EdmType.Binary: return e.BinaryValue.ToString();
                case EdmType.Boolean: return e.BooleanValue.ToString();
                case EdmType.DateTime: return e.DateTime.ToString();
                case EdmType.Double: return e.DoubleValue.ToString();
                case EdmType.Guid: return e.GuidValue.ToString();
                case EdmType.Int32: return e.Int32Value.ToString();
                case EdmType.Int64: return e.Int64Value.ToString();
                case EdmType.String: return e.StringValue;
                default: return null;

            }
        }


    }
}
