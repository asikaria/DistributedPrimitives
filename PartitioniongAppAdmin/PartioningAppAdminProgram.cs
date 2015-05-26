using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DistributedPrimitives.PartitioningService;

namespace PartitioningAppAdmin
{
    class PartitioningAppAdminProgram
    {
        private const string storageAccountName = "thezoo";
        private const string storageAccountKey = "PYFm1lBrUWvyqFTj5QP9KcoQJSwBe7YFd6ZabJ6gTJI/gaAZK4OVRIWgeZGWn9oiJXMz0FQBOgYFLmfO29RrVQ==";
        private const string storageTableName = "zoo";

        static void Main(string[] args)
        {
            TablePartitioningService ps = TablePartitioningService.getInstance(storageAccountName, storageAccountKey, storageTableName);

            if (args.Length < 1)
            {
                showUsage();
            }

            if (args[0].Trim().ToLower() == "create")
            {
                ps.createTable(1024, 256);
            }
            else if (args[0].Trim().ToLower() == "delete")
            {
                ps.deleteTable();
            }
            else if (args[0].Trim().ToLower() == "kick")
            {
                if (args.Length != 2) { showUsage(); } else { ps.kickPartition(args[1]); }
            }
            else if (args[0].Trim().ToLower() == "show")
            {
                showAll(ps.getAllPartitionsInfo());
            }


        }

        static void showUsage()
        {
            Console.WriteLine("Usage: PartioningAppAdmin <operation>\n where operation: create|kick <partitionID>|delete|show");
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
    }
}
