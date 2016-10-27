using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data.Common;
using System.Data.Entity;
using System.Data.Entity.Migrations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlReplicator.Core
{

    //public class DestinationContext : DbContext {

    //    static DestinationContext(){
    //        Database.SetInitializer(new MigrateDatabaseToLatestVersion<DestinationContext, DestinationContextConfiguration>());
    //    }

    //    public DestinationContext(DbConnection conn):base(conn,false)
    //    {

    //    }

    //    public DbSet<ReplicatedTable> ReplicatedTables { get; set; }

    //    public async Task<ReplicatedTable> GetTableAsync(string name)
    //    {
    //        DateTime now = DateTime.UtcNow;
    //        var rt = ReplicatedTables.FirstOrDefault(x => x.TableName == name);
    //        if (rt == null)
    //        {
    //            rt = new Core.ReplicatedTable
    //            {
    //                TableName = name,
    //                LastHashSync = now,
    //                LastSync = now,
    //                LastVersion = 0
    //            };
    //            ReplicatedTables.Add(rt);
    //            await SaveChangesAsync();
    //        }
    //        return rt;
    //    }
    //}

    //internal class DestinationContextConfiguration : DbMigrationsConfiguration<DestinationContext>
    //{
    //    public DestinationContextConfiguration()
    //    {
    //        AutomaticMigrationsEnabled = true;
    //    }
    //}


    //public class ReplicatedTable {

    //    [Key]
    //    public string TableName { get; set; }

    //    /// <summary>
    //    /// Last sync time... UTC
    //    /// </summary>
    //    public DateTime LastSync { get; set; }

    //    /// <summary>
    //    /// Last Change Tracking Version
    //    /// </summary>
    //    public long LastVersion { get; set; }

    //    /// <summary>
    //    /// Last Time when Entire Table was Hash Synced
    //    /// </summary>
    //    public DateTime LastHashSync { get; set; }

    //}
}
