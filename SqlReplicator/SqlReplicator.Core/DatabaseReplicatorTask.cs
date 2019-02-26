using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlReplicator.Core
{
    public class DatabaseReplicatorTask: IDisposable
    {

        public string Log { get; set; }

        public ConfigJob Job { get; set; }

        public DatabaseReplicatorTask()
        {
            SqlServerTypes.Utilities.LoadNativeAssemblies(AppDomain.CurrentDomain.BaseDirectory);
        }


        public async Task ReplicateAsync() {


            var isFirstTime = Job.Tables.Count == 0;
            Job.Tables.Clear();

            using (var sourceQuery = await Job.Source.OpenAsync())
            {

                var columns = (await sourceQuery.GetCommonSchemaAsync()).OrderBy( x=> x.TableName);

                foreach (var table in columns.GroupBy(x => x.TableName)) {
                    SqlTable st = new SqlTable();
                    st.Name = table.Key;
                    st.ID = table.First().TableID;
                    st.HasIdentity = table.Any(x => x.IsIdentity);
                    st.Columns.AddRange(table);

                    if (!st.PrimaryKey.Any()) {
                        // throw new InvalidOperationException("Each table for replication must have atleast one primary key");
                        Console.WriteLine($"Table {table.Key} does not have any primary key");
                        continue;
                    }

                    Job.Tables.Add(st);

                }
                if (isFirstTime)
                {
                    // await sourceQuery.SetupChangeTrackingAsync(Job.Tables);

                    await sourceQuery.GetIndexes(Job.Tables);
                }

            }

            if (isFirstTime)
            {
                using (var destQuery = await Job.Destination.OpenAsync())
                {

                    // create ReplicationState table...
                    // await destQuery.ExecuteAsync(Scripts.CreateReplicationStateTable);
                    await destQuery.CreateReplicationStateTable();

                }
            }

            // await Task.WhenAll( Job.Tables.Select( x=> SyncTable(x) ) );

            //foreach(var table in Job.Tables.Slice(4).ToList()) {
            //    await Task.WhenAll(table.ToList().Select( async x =>
            //    {
            //        int i = 3;
            //        while (true)
            //        {
            //            i--;
            //            try
            //            {
            //                await SyncTable(x);
            //                return;
            //            } catch (Exception ex)
            //            {
            //                if (i <= 0)
            //                {
            //                    Console.Write(ex);
            //                    break;
            //                }
            //                if (ex.ToString().ToLower().Contains("timeout"))
            //                {
            //                    await Task.Delay(TimeSpan.FromSeconds(30));
            //                    continue;
            //                }
            //            }
            //        }
            //    }));
            //}

            //foreach (var table in Job.Tables.Slice(4).ToList())
            //{
            //    var list = table.ToList();
            //    await Task.WhenAll(list.Select(SyncTable));
            //    // Console.WriteLine($"Syncing {table.Name}");
            //    // await SyncTable(table);
            //    var names = string.Join(", ", list.Select(x => x.Name));
            //    Console.WriteLine($"{names} Sync Complete");
            //}

            foreach (var table in Job.Tables)
            {
                Console.WriteLine($"Syncing {table.Name}");
                await SyncTable(table);
            }
        }

        private async Task SyncTable(SqlTable srcTable)
        {
            SyncState state = null;
            DateTime now = DateTime.UtcNow.AddDays(-10);
            using (var sourceQuery = await Job.Source.OpenAsync()) {
                using (var destQuery = await Job.Destination.OpenAsync()) {


                    try
                    {

                        // first always sync schema if there are any changes...
                        await destQuery.SyncSchema(srcTable);

                        state = await destQuery.GetLastSyncVersion(srcTable);
                        
                        if (state.LastVersion == 0)
                        {
                            // full sync pending ???


                            state.LastSyncResult = "Full sync started";

                            await destQuery.UpdateSyncState(state);

                            if(!await FullSyncAsync(sourceQuery, destQuery, srcTable))
                            {
                                return;
                            }

                            state.LastVersion = await sourceQuery.GetCurrentVersionAsync(srcTable);
                            state.LastFullSync = DateTime.UtcNow;
                            state.LastSyncResult = "Full sync finished";
                            await destQuery.UpdateSyncState(state);
                            return;

                        }

                        var changes = await sourceQuery.ReadChangedRows(srcTable, state.LastVersion);
                        if (changes.Any())
                        {
                            await destQuery.WriteToServerAsync(srcTable, changes, state);
                        }

                    }
                    catch (Exception ex) {
                        Console.WriteLine(ex);
                        state.LastSyncResult = ex.ToString();
                        await destQuery.UpdateSyncState(state);
                    }
                }
            }
        }


        private async Task<bool> FullSyncAsync(SqlQuery sourceQuery, SqlQuery destQuery, SqlTable srcTable)
        {

            while (true)
            {


                await destQuery.ReadMaxPrimaryKeys(srcTable);

                using (var r = await sourceQuery.ReadObjectsAbovePrimaryKeys(srcTable))
                {
                    var copied = await destQuery.WriteToServerAsync(srcTable, r);
                    if (copied == 0)
                    {
                        return true;
                    }
                }

            }
            // return false;

            

        }

        public void Dispose()
        {
            
        }
    }
}
