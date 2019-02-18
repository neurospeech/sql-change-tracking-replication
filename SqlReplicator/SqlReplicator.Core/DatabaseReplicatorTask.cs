﻿using System;
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

                var columns = await sourceQuery.GetCommonSchemaAsync();

                foreach (var table in columns.GroupBy(x => x.TableName)) {
                    SqlTable st = new SqlTable();
                    st.Name = table.Key;
                    st.ID = table.First().TableID;
                    st.HasIdentity = table.Any(x => x.IsIdentity);
                    st.Columns.AddRange(table);

                    if (!st.PrimaryKey.Any()) {
                        // throw new InvalidOperationException("Each table for replication must have atleast one primary key");
                        Trace.TraceWarning($"Table {table.Key} does not have any primary key");
                        continue;
                    }

                    Job.Tables.Add(st);
                }
                if (isFirstTime)
                {
                    await sourceQuery.SetupChangeTrackingAsync(Job.Tables);
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

            foreach(var table in Job.Tables.Slice(4).ToList()) {
                await Task.WhenAll(table.Select( async x =>
                {
                    int i = 3;
                    while (true)
                    {
                        i--;
                        try
                        {
                            await SyncTable(x);
                            break;
                        } catch (Exception ex)
                        {
                            if (i <= 0)
                            {
                                Trace.WriteLine(ex.ToString());
                                break;
                            }
                            if (ex.ToString().ToLower().Contains("timeout"))
                            {
                                await Task.Delay(TimeSpan.FromSeconds(30));
                                continue;
                            }
                        }
                    }
                }));
            }

            //foreach (var table in Job.Tables)
            //{
            //    await SyncTable(table);
            //}

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
                        await destQuery.SyncSchema(srcTable.Name, srcTable.Columns);

                        state = await destQuery.GetLastSyncVersion(srcTable);
                        
                        if (state.LastVersion == 0)
                        {
                            // full sync pending ???


                            state.LastSyncResult = "Full sync started";

                            await destQuery.UpdateSyncState(state);

                            await FullSyncAsync(sourceQuery, destQuery, srcTable);

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
                        Trace.TraceError(ex.ToString());
                        state.LastSyncResult = ex.ToString();
                        await destQuery.UpdateSyncState(state);
                    }
                }
            }
        }


        private async Task FullSyncAsync(SqlQuery sourceQuery, SqlQuery destQuery, SqlTable srcTable)
        {

            for(var i=0;i<100;i++)
            {


                await destQuery.ReadMaxPrimaryKeys(srcTable);

                using (var r = await sourceQuery.ReadObjectsAbovePrimaryKeys(srcTable))
                {
                    if (!await destQuery.WriteToServerAsync(srcTable,r))
                        break;
                }

            } 

            

        }

        public void Dispose()
        {
            
        }
    }
}
