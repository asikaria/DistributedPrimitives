using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DistributedPrimitives.PartitioningService;
using System.Threading;

namespace PartitioningAppAdmin
{
    class PartitioningAppAdminProgram
    {
        private const string storageAccountName = "thezoo";
        private const string storageAccountKey = "PYFm1lBrUWvyqFTj5QP9KcoQJSwBe7YFd6ZabJ6gTJI/gaAZK4OVRIWgeZGWn9oiJXMz0FQBOgYFLmfO29RrVQ==";
        private const string storageTableName = "zoo";

        static void Main(string[] args)
        {
            TablePartitioningService ps = TablePartitioningService.GetInstance(storageAccountName, storageAccountKey, storageTableName);

            if (args.Length < 1)
            {
                showUsage(); return;
            }

            if (args[0].Trim().ToLower() == "create")
            {
                ps.createTable(1024, 256, 8);
            }
            else if (args[0].Trim().ToLower() == "delete")
            {
                ps.deleteTable();
            }
            else if (args[0].Trim().ToLower() == "kick")
            {
                if (args.Length != 2) { showUsage(); return;  } else { ps.kickPartition(args[1]); }
            }
            else if (args[0].Trim().ToLower() == "showall")
            {
                showAll(ps.getAllPartitionsInfo());
            }
            else if (args[0].Trim().ToLower() == "shownode")
            {
                if (args.Length != 2) { showUsage(); return; } else { showNode(ps.getAllPartitionsInfo(), args[1]); }
            }
            else if (args[0].Trim().ToLower() == "showpartition")
            {
                if (args.Length != 2) { showUsage(); return; } else { showPartition(ps.getAllPartitionsInfo(), args[1]); }
            }
            else if (args[0].Trim().ToLower() == "showbynode")
            {
                showByNode(ps.getAllPartitionsInfo());
            }
            else if (args[0].Trim().ToLower() == "detectstale")
            {
                if (args.Length != 2) 
                    { showUsage(); return; } 
                else {
                    string secondsToWaitStr = args[1];
                    int s = 60;
                    int.TryParse(secondsToWaitStr, out s);
                    detectStale(ps, s); 
                }
            }
            else
            {
                showUsage(); return;
            }
        }


        static void showUsage()
        {
            Console.WriteLine();
            Console.WriteLine("Usage: PartioningAppAdmin <operation>\n\nWhere operation is one of:");
            Console.WriteLine("     create");
            Console.WriteLine("     kick <partitionID>");
            Console.WriteLine("     delete");
            Console.WriteLine("     detectStale <seconds-to-watch>");
            Console.WriteLine("     showAll");
            Console.WriteLine("     showByNode");
            Console.WriteLine("     showPartition <partitionID>");
            Console.WriteLine("     showNode <nodeID>");
        }

        private static void detectStale(TablePartitioningService ps, int secondsToWait)
        {
            Console.WriteLine("Getting Sample 1");
            List<Dictionary<string, string>> sample1 = ps.getAllPartitionsInfo();
            Console.WriteLine("Waiting {0} seconds...", secondsToWait);
            Thread.Sleep(1000 * secondsToWait);
            Console.WriteLine("Getting Sample 2");
            List<Dictionary<string, string>> sample2 = ps.getAllPartitionsInfo();
            Console.WriteLine("Comparing");

            

            // We have two sorted lists. We need to find corresponding elements (matching 
            // partition ID) and report rows that have the same ETag (implying they havent been refreshed)

            Dictionary<string, string> row2 = null;
            IEnumerator<Dictionary<string, string>> enum2 = sample2.GetEnumerator();
            bool neof = enum2.MoveNext();          // neof == Not-EOF
            if (neof) row2 = enum2.Current;
            foreach (Dictionary<string, string> row1 in sample1)
            {
                if (row2 == null)
                {
                    Console.WriteLine("Partition: {0}  *** (Deleted)", row1["partitionID"]);
                    Console.WriteLine("    LKG Node: {0}", row1["NodeID"]);
                    Console.WriteLine("    Etag: {0}", row1["ETag(URLDecoded)"]);
                    Console.WriteLine("--------------");
                    Console.WriteLine();
                    continue;
                }

                if (row1["partitionID"].CompareTo(row2["partitionID"]) > 0)
                {
                    while ((row2 != null) && (row1["partitionID"].CompareTo(row2["partitionID"]) > 0))
                    {
                        row2 = null;
                        neof = enum2.MoveNext();          // neof == Not-EOF
                        if (neof) row2 = enum2.Current;
                    }
                }

                if ((row2 == null) || (row2 != null && (row1["partitionID"] == row2["partitionID"])) && (row1["ETag(URLDecoded)"] == row2["ETag(URLDecoded)"]) )
                {
                    {
                        Console.WriteLine("Partition: {0}", row1["partitionID"]);
                        Console.WriteLine("    LKG Node: {0}", row1["NodeID"]);
                        Console.WriteLine("    Etag: {0}", row1["ETag(URLDecoded)"]);
                        Console.WriteLine("--------------");
                        Console.WriteLine();
                    }
                } 
            }
        }


        static void showAll(List<Dictionary<string, string>> result)
        {
            if (result == null) return;

            foreach (Dictionary<string, string> row in result)
            {
                foreach (KeyValuePair<string, string> kvp in row)
                {
                    Console.WriteLine("{0}:\t{1}", kvp.Key, kvp.Value);
                }
                Console.WriteLine("-----------------------------------------------");
                Console.WriteLine();
            }
        }


        static void showByNode(List<Dictionary<string, string>> result)
        {
            if (result == null) return;

            Dictionary<string, List<Dictionary<string, string>>> nodeMap = new Dictionary<string, List<Dictionary<string, string>>>(256);

            foreach (Dictionary<string, string> row in result)
            {
                string node = row["NodeID"];
                if (nodeMap.ContainsKey(node))
                {
                    nodeMap[node].Add(row);
                }
                else
                {
                    List<Dictionary<string, string>> partitionsInNode = new List<Dictionary<string, string>>(256);
                    partitionsInNode.Add(row);
                    nodeMap.Add(node, partitionsInNode);
                }
            }

            foreach (KeyValuePair<string, List<Dictionary<string, string>>> kvp in nodeMap)
            {
                Console.WriteLine("{0} : ", kvp.Key);
                int count = 0;
                foreach (Dictionary<string, string> row in kvp.Value) 
                {
                    Console.Write("      {0}", row["partitionID"]);
                    count++;
                    if (count >= 8) { Console.WriteLine(); count = 0;  }
                }
                Console.WriteLine();
                Console.WriteLine("-----------------------------------------------");
            }


        }

        static void showNode(List<Dictionary<string, string>> result, string nodeID)
        {
            bool found = false;
            if (result == null) return;

            foreach (Dictionary<string, string> row in result)
            {
                if (row["NodeID"] == nodeID)
                {
                    found = true;
                    foreach (KeyValuePair<string, string> kvp in row)
                    {
                        Console.WriteLine("{0}:\t{1}", kvp.Key, kvp.Value);
                    }
                    Console.WriteLine("-----------------------------------------------");
                    Console.WriteLine();
                }
            }
            if (!found) { Console.WriteLine("Not Found"); }
            Console.WriteLine();
        }

        static void showPartition(List<Dictionary<string, string>> result, string partitionID)
        {
            bool found = false;
            if (result == null) return;

            foreach (Dictionary<string, string> row in result)
            {
                if (row["partitionID"] == partitionID)
                {
                    found = true;
                    foreach (KeyValuePair<string, string> kvp in row)
                    {
                        Console.WriteLine("{0}:\t{1}", kvp.Key, kvp.Value);
                    }
                }
            }
            if (!found) { Console.WriteLine("Not Found"); }
            Console.WriteLine();
        }


    }
}
