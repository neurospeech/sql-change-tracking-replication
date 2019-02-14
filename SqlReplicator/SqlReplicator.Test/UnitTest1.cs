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
                        Database = "Casting800",
                        TrustedConnection = true,
                    },
                    Destination = new ConfigDatabase {
                        Provider = "MySql.Data.MySqlClient",
                        Server = "192.168.0.6",
                        Username = "test",
                        Password = "$abcd123",
                        Database = "Casting"
                    }
                };

                await test.ReplicateAsync();
            }
        }
    }
}
