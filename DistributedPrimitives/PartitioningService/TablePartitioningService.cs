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


namespace DistributedPrimitives.PartitioningService
{
    public class TablePartitioningService : IParticipant
    {
        private int numPartitions;
        private int heartbeatIntervalInSeconds;
        private IParticipantCallbacks callbackObject;
        private int maxPartitionsPerNode = 0;
        private string nodeId;
        private int leaseValidityInSeconds;
        private int acquireLeaseOlderThanSeconds;

        private string storageAccountName;
        private string storageAccountKey;
        private string tableName;

        private Thread t;
        bool initialized = false;

        private const string nodeIdPropertyName = "NodeID";
        private const string loadDataPropertyName = "LoadData";

        private object lockObj = new Object();
        
        public static TablePartitioningService getInstance(string storageAccountName, string storageAccountKey, string tableName)
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

        public void init(int numPartitions, int heartbeatIntervalInSeconds, int leaseValidityInSeconds, int acquireLeaseOlderThanSeconds, int maxPartitionsPerNode, string nodeId, IParticipantCallbacks callbackObject)
        {
            lock (lockObj)
            {
                if (!initialized)
                {
                    this.numPartitions = numPartitions;
                    this.heartbeatIntervalInSeconds = heartbeatIntervalInSeconds;
                    this.callbackObject = callbackObject;
                    this.maxPartitionsPerNode = maxPartitionsPerNode;
                    this.nodeId = nodeId;  // node ID for this node
                    this.leaseValidityInSeconds = leaseValidityInSeconds;
                    this.acquireLeaseOlderThanSeconds = acquireLeaseOlderThanSeconds;

                    checkTable(); // create Table Rows
                    t = new Thread(threadMethod);
                    t.Start();  //start Thread
                    initialized = true;
                }
            }
            Console.WriteLine("Started Thread");
        }


        private void checkTable()
        {
            StorageCredentials creds = new StorageCredentials(storageAccountName, storageAccountKey);
            CloudStorageAccount storageAccount = new CloudStorageAccount(creds, true);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference(tableName);

            TableQuery query = new TableQuery();
            query.FilterString = "PartitionKey eq 'Control' and RowKey eq 'Created'";
            IEnumerable<DynamicTableEntity> results = table.ExecuteQuery(query);        // this will throw StorageException for any issues with the account
            if (results.Count() <= 0) throw new ArgumentException(String.Format("Partition rows have not been created in Table {0}", tableName));

            TableQuery query2 = new TableQuery();
            query2.FilterString = "PartitionKey eq 'Control' and RowKey eq 'MaxPartitionsPerNode'";
            IEnumerable<DynamicTableEntity> results2 = table.ExecuteQuery(query2);
            foreach (DynamicTableEntity row in results2)
            {
                this.maxPartitionsPerNode = (int)row["MaxPartitions"].Int32Value;
            }

        }

        private void threadMethod()
        {
            while (true)
            {
                lock (lockObj)
                {
                    renewLeases();
                    expireLeases();
                    acquireLeases();
                }
                Thread.Sleep(heartbeatIntervalInSeconds * 1000);
            }
        }

        //private List<CurrentLease> currentLeases = new List<CurrentLease>(64);
        private SortedDictionary<string,CurrentLease> currentLeases = new SortedDictionary<string,CurrentLease>();
        private int numLeasesHeld;

        private void renewLeases()
        {
            Console.WriteLine("Renew Leases");
            List<string> leasesToRemove = new List<string>(256);
            foreach (KeyValuePair<string, CurrentLease> kvp in currentLeases)
            {
                CurrentLease lease = kvp.Value;
                StorageCredentials creds = new StorageCredentials(storageAccountName, storageAccountKey);
                CloudStorageAccount storageAccount = new CloudStorageAccount(creds, true);
                CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
                CloudTable table = tableClient.GetTableReference(tableName);

                DynamicTableEntity leaseRowInTable = new DynamicTableEntity(lease.partitionId, lease.rowId);
                leaseRowInTable.Properties[nodeIdPropertyName] = new EntityProperty(nodeId);
                leaseRowInTable.Properties[loadDataPropertyName] = new EntityProperty(callbackObject.getCurrentLoad(lease.partitionId));  // TODO: Add robustness here
                leaseRowInTable.ETag = lease.ETag;
                TableOperation leaseUpdateOperation = TableOperation.Replace(leaseRowInTable);

                

                try
                {
                    TableResult tableResult = table.Execute(leaseUpdateOperation);
                    lease.ETag = ((DynamicTableEntity)tableResult.Result).ETag;
                    lease.lastRefreshed = DateTime.Now;
                }
                catch (StorageException ex)
                {
                    Console.WriteLine("Failed to Renew lease for partition {0}", lease.partitionId);
                    Console.WriteLine("     Exception {0} ({1}): {2}", ex.RequestInformation.HttpStatusCode, ex.RequestInformation.HttpStatusMessage, ex.Message);
                    if (ex.RequestInformation.HttpStatusCode == 412)
                    {
                        leasesToRemove.Add(lease.partitionId);
                    }
                    // swallow - failed to renew lease
                    // TODO: Log it
                }

            }
            removeLeases(leasesToRemove);
        }

        private void expireLeases()
        {
            Console.WriteLine("Expire Leases");

            List<string> leasesToRemove = new List<string>(256);
            DateTime staleLeaseTimestamp = DateTime.Now.AddSeconds(-this.leaseValidityInSeconds);
            foreach (KeyValuePair<string, CurrentLease> kvp in currentLeases)
            {
                CurrentLease lease = kvp.Value;
                if (lease.lastRefreshed < staleLeaseTimestamp)
                { // lease has expired
                    // TODO: Log the loss of lease
                    leasesToRemove.Add(lease.partitionId);
                }
            }
            removeLeases(leasesToRemove);
        }


        private void removeLeases(List<string> leasesToRemove)
        {
            if (leasesToRemove == null) return;
            foreach (string partitionID in leasesToRemove)
            {
                removeLease(partitionID);
            }
        }

        private void removeLease(string partitionID)
        {
            if (partitionID == null || partitionID == "") return;

            callbackObject.lostPartition(partitionID);
            currentLeases.Remove(partitionID);
            numLeasesHeld--;

        }

        private Dictionary<string, Lease> allLeases = new Dictionary<string, Lease>(8192);

        private void acquireLeases()
        {
            Console.WriteLine("Renew Leases: Scanning");
            if (numLeasesHeld >= maxPartitionsPerNode) return;

            StorageCredentials creds = new StorageCredentials(storageAccountName, storageAccountKey);
            CloudStorageAccount storageAccount = new CloudStorageAccount(creds, true);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference(tableName);

            //update local lease table from Server
            TableQuery query = new TableQuery();
            query.FilterString = "PartitionKey gt 'x' and PartitionKey lt 'y'";
            IEnumerable<DynamicTableEntity> results = table.ExecuteQuery(query);
            DateTime now = DateTime.Now;
            foreach (DynamicTableEntity row in results)
            {
                if (allLeases.ContainsKey(row.PartitionKey))
                {
                    Lease lease = allLeases[row.PartitionKey];
                    lease.rowId = row.RowKey;
                    if (lease.ETag != row.ETag) {
                        lease.ETag = row.ETag;
                        lease.lastRefreshed = now;
                    }
                    lease.LoadData = (row.Properties[loadDataPropertyName] != null) ? row.Properties[loadDataPropertyName].StringValue : "";
                    lease.nodeID = (row.Properties[nodeIdPropertyName] != null) ? row.Properties[nodeIdPropertyName].StringValue : "";
                }
                else
                {
                    Lease lease = new Lease();
                    lease.partitionId = row.PartitionKey;
                    lease.rowId = row.RowKey;
                    lease.ETag = row.ETag;
                    lease.lastRefreshed = now;
                    lease.LoadData = (row.Properties[loadDataPropertyName]!=null) ? row.Properties[loadDataPropertyName].StringValue : "";
                    lease.nodeID = (row.Properties[nodeIdPropertyName] != null) ? row.Properties[nodeIdPropertyName].StringValue : "";
                    allLeases.Add(lease.partitionId, lease);
                    // TODO: Log the acquisition of Lease
                }
            }

            Console.WriteLine("Renew Leases: Acquiring");
            DateTime staleLeaseTimestamp = DateTime.Now.AddSeconds(-this.acquireLeaseOlderThanSeconds);
            foreach (KeyValuePair<string, Lease> kvp in allLeases)
            {
                Lease lease = kvp.Value;
                if (lease.lastRefreshed < staleLeaseTimestamp)
                {
                    if (numLeasesHeld < maxPartitionsPerNode)
                    {
                        DynamicTableEntity leaseRowInTable = new DynamicTableEntity(lease.partitionId, lease.rowId);
                        leaseRowInTable.Properties[nodeIdPropertyName] = new EntityProperty(nodeId);
                        leaseRowInTable.Properties[loadDataPropertyName] = new EntityProperty(""); 
                        leaseRowInTable.ETag = lease.ETag;
                        TableOperation leaseUpdateOperation = TableOperation.Replace(leaseRowInTable);
                        try
                        {
                            TableResult tableResult = table.Execute(leaseUpdateOperation);
                            lease.ETag = ((DynamicTableEntity)tableResult.Result).ETag;
                            lease.lastRefreshed = now;

                            CurrentLease currentLease = new CurrentLease();
                            currentLease.partitionId = lease.partitionId;
                            currentLease.rowId = lease.rowId;
                            currentLease.lastRefreshed = now;
                            currentLease.ETag = lease.ETag;
                            currentLeases.Add(lease.partitionId, currentLease);

                            numLeasesHeld++;
                            callbackObject.gotPartition(lease.partitionId);
                            if (numLeasesHeld >= maxPartitionsPerNode) break;
                        }
                        catch (StorageException ex)
                        {
                            Console.WriteLine("Failed to acquire lease for eligible partition {0}", lease.partitionId);
                            Console.WriteLine("     Exception {0}({1}): {2}", ex.RequestInformation.HttpStatusCode, ex.RequestInformation.HttpStatusMessage, ex.Message);
                            // swallow - failed to acquire lease
                            // TODO: Log it
                        }
                    }
                    else
                    {
                        break;
                    }
                }
            }
        }

        private class CurrentLease
        {
            public string partitionId;
            public string rowId;
            public string ETag;
            public DateTime lastRefreshed; // client time
        }

        private class Lease
        {
            public string partitionId;
            public string rowId;
            public string ETag;
            public DateTime lastRefreshed; // client time
            public string LoadData;
            public string nodeID;
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
                if (currentLeases.ContainsKey(partitionID)) return true;
                return false;
            }
        }

        public List<string> listOwnedPartitions()
        {
            if (!initialized) throw new InvalidOperationException("Partitioning Service instance has not been inititalized yet");
            lock (lockObj)
            {
                List<string> list = new List<string>(currentLeases.Count);
                foreach (KeyValuePair<string, CurrentLease> kvp in currentLeases)
                {
                    CurrentLease lease = kvp.Value;
                    list.Add(lease.partitionId);
                }
                return list;
            }
        }

        public void dropPartition(string PartitionID)
        {
            if (!initialized) throw new InvalidOperationException("Partitioning Service instance has not been inititalized yet");
            lock (lockObj)
            {
                if (currentLeases.ContainsKey(PartitionID))
                {
                    CurrentLease lease = currentLeases[PartitionID];
                    callbackObject.lostPartition(lease.partitionId);
                    currentLeases.Remove(PartitionID);
                    numLeasesHeld--;
                }
            }
        }

        /*
         * 
         * Admin Methods
         * 
        */

        public void createTable(int numPartitions, int maxPartitionsPerNode)
        {
            StorageCredentials creds = new StorageCredentials(storageAccountName, storageAccountKey);
            CloudStorageAccount storageAccount = new CloudStorageAccount(creds, true);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference(tableName);

            if (table.CreateIfNotExists())
            {   // if table was created in the CreateIfNotExists call
                for (int i = 0; i < numPartitions; i++)
                {   // create rows for all partitions. This should only be called once in the app's lifetime.
                    string partitionKey = String.Format("x{0:x4}", i);
                    string rowKey = "0";
                    DynamicTableEntity row = new DynamicTableEntity(partitionKey, rowKey);
                    row.Properties[nodeIdPropertyName] = new EntityProperty("none");
                    row.Properties[loadDataPropertyName] = new EntityProperty("");  
                    TableOperation insertOperation = TableOperation.Insert(row);
                    table.Execute(insertOperation);
                }
                DynamicTableEntity controlRow = new DynamicTableEntity("Control", "Created");
                TableOperation controlInsertOperation = TableOperation.Insert(controlRow);
                table.Execute(controlInsertOperation);

                DynamicTableEntity controlRow2 = new DynamicTableEntity("Control", "MaxPartitionsPerNode");
                controlRow2.Properties.Add(new KeyValuePair<string, EntityProperty>("MaxPartitions", new EntityProperty(maxPartitionsPerNode)));
                TableOperation controlInsertOperation2 = TableOperation.Insert(controlRow2);
                table.Execute(controlInsertOperation2);
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

        public void kickPartition(string partitionID)
        {
            StorageCredentials creds = new StorageCredentials(storageAccountName, storageAccountKey);
            CloudStorageAccount storageAccount = new CloudStorageAccount(creds, true);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference(tableName);

            DynamicTableEntity leaseRowInTable = new DynamicTableEntity(partitionID, "0");
            leaseRowInTable.Properties[nodeIdPropertyName] = new EntityProperty("kicked");
            leaseRowInTable.Properties[loadDataPropertyName] = new EntityProperty("");
            leaseRowInTable.ETag = "*";
            TableOperation leaseUpdateOperation = TableOperation.Replace(leaseRowInTable); 

            OperationContext opContext = new OperationContext();
            opContext.UserHeaders = new Dictionary<string, string>(1);
            opContext.UserHeaders["If-Match"] = "*";
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
