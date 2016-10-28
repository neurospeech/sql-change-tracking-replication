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

                        /*var changeDetection = string.Join(",", srcTable.Columns
                            .Select(x => x.IsPrimaryKey ?
                                $"T.{x.ColumnName} AS D_{x.ColumnName}" :
                                $"(CASE CHANGE_TRACKING_IS_COLUMN_IN_MASK({x.ID},CT.SYS_CHANGE_COLUMNS) WHEN 1 THEN T.{x.ColumnName}  ELSE NULL END) as D_{x.ColumnName}, CHANGE_TRACKING_IS_COLUMN_IN_MASK({x.ID},CT.SYS_CHANGE_COLUMNS) AS IC_{x.ColumnName}")
                        );

                        string sq = $"SELECT TOP 100 {changeDetection}, CT.SYS_CHANGE_OPERATION, CT.SYS_CHANGE_VERSION  FROM CHANGETABLE(CHANGES {tableName},@lastVersion) AS CT "
                            + $"JOIN {tableName} as T ON ({primaryKeyJoinOn})"
                            + " ORDER BY CT.SYS_CHANGE_VERSION ASC";

                        // compare schema...
                        using (var reader = await sourceQuery.ReadAsync(sq, new KeyValuePair<string, object>("@lastVersion", lastVersion)))
                        {

                            while (await reader.ReadAsync())
                            {


                                //....

                            }

                        }*/

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

            do
            {


                bool hasMax = false;

                var pkNames = string.Join(",", srcTable.PrimaryKey.Select(x => x.ColumnName));
                var pkOrderBy = string.Join(",", srcTable.PrimaryKey.Select(x => x.ColumnName + " DESC"));

                // get last primary keys....
                string spk = $"SELECT TOP 1 { pkNames } FROM {srcTable.Name} ORDER BY {pkOrderBy}";
                using (var r = await destQuery.ReadAsync(spk))
                {
                    if (await r.ReadAsync())
                    {
                        foreach (var pk in srcTable.PrimaryKey)
                        {
                            pk.LastValue = r.GetValue<object>(pk.ColumnName);
                            hasMax = true;
                        }
                    }
                }


                string s = $" * FROM {srcTable.Name}";
                KeyValuePair<string, object>[] parray = null;

                string filter = "";

                if (hasMax)
                {
                    filter = $" WHERE { string.Join(" AND ", srcTable.PrimaryKey.Select(x => $"{x.ColumnName} > @C{x.ID}"))} ";
                    parray = srcTable.PrimaryKey.Select(x => new KeyValuePair<string, object>($"@C{x.ID}", x.LastValue)).ToArray();
                }

                filter += $" ORDER BY { string.Join(",", srcTable.PrimaryKey.Select(x => x.ColumnName)) }";


                using (var r = await sourceQuery.ReadAsync("SELECT TOP 1 " + s + filter, parray)) {
                    if (!await r.ReadAsync()) {

                        // nothing to save...
                        await UpdateRST(destQuery, srcTable.Name, DateTime.UtcNow, "Full Sync Completed", 0);

                        break;
                    }
                }


                using (var r = await sourceQuery.ReadAsync("SELECT TOP 1000 " + s + filter, parray))
                {

                    using (SqlBulkCopy sbc = new SqlBulkCopy((SqlConnection)destQuery.Connection, 
                        SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.UseInternalTransaction
                        , null))
                    {

                        sbc.DestinationTableName = srcTable.Name;

                        

                        await sbc.WriteToServerAsync(r.Reader);
                    }

                }

            } while (true);

        }

        public void Dispose()
        {
            
        }
    }
}
