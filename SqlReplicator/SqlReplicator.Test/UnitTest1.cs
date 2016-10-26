using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SqlReplicator.Core;
using System.Threading.Tasks;

namespace SqlReplicator.Test
{
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public async Task TestMethod1()
        {
            using (DatabaseReplicatorTask test = new DatabaseReplicatorTask()) {
                test.Job = new ConfigJob {
                    Source = new ConfigDatabase {
                        Server = "s800",
                        TrustedConnection = true,
                        Database = "SideBusiness"
                    }
                };

                await test.ReplicateAsync();
            }
        }
    }
}
