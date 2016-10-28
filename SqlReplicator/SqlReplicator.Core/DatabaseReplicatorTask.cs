using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlReplicator.Core
{
    public class DatabaseReplicatorTask: IDisposable
    {

        public string Log { get; set; }

        public ConfigJob Job { get; set; }


        public async Task ReplicateAsync() {


            var isFirstTime = Job.Tables.Count == 0;
            Job.Tables.Clear();

            using (var sourceQuery = await Job.Source.OpenAsync())
            {

                var columns = await sourceQuery.GetCommonSchemaAsync();

                foreach (var table in columns.GroupBy(x => x.TableName)) {
                    SqlTable st = new SqlTable();
                    st.Name = table.Key;

                    st.Columns.AddRange(table);

                    if (!st.PrimaryKey.Any()) {
                        throw new InvalidOperationException("Each table for replication must have atleast one primary key");
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
                    await destQuery.ExecuteAsync(Scripts.CreateReplicationStateTable);

                    await Task.WhenAll(Job.Tables.Select(x => SyncTableSchema(x)));

                }
            }

            await Task.WhenAll( Job.Tables.Select( x=> SyncTable(x) ) );

        }

        private async Task SyncTableSchema(SqlTable table)
        {
            using (var destQuery = await Job.Destination.OpenAsync())
            {
                await destQuery.SyncSchema(table.Name, table.Columns);
            }
        }

        private async Task SyncTable(SqlTable srcTable)
        {
            SyncState state = null;
            DateTime now = DateTime.UtcNow;
            using (var sourceQuery = await Job.Source.OpenAsync()) {
                using (var destQuery = await Job.Destination.OpenAsync()) {


                    try
                    {

                        state = await destQuery.GetLastSyncVersion(srcTable);

                        if (state.LastFullSync == null)
                        {
                            // full sync pending ???


                            state.LastVersion = await sourceQuery.GetCurrentVersionAsync(srcTable);
                            state.LastSyncResult = "Full sync started";
                            await SyncTableSchema(srcTable);

                            await destQuery.UpdateSyncState(state);

                            await FullSyncAsync(sourceQuery, destQuery, srcTable);
                            return;

                        }

                        var changes = await sourceQuery.ReadChangedRows(srcTable, state.LastVersion);

                        await destQuery.WriteToServerAsync(srcTable, changes);

                    }
                    catch (Exception ex) {
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
