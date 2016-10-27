using System;
using System.Collections.Generic;
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

            if (Job.Tables.Count == 0)
            {

                using (var sourceQuery = await Job.Source.OpenAsync())
                {

                    var columns = await sourceQuery.GetCommonSchemaAsync();

                    foreach (var table in columns.GroupBy(x => x.TableName)) {
                        SqlTable st = new SqlTable();
                        st.Name = table.Key;

                        st.Columns.AddRange(table);

                        st.PrimaryKey = st.Columns.Where(x => x.IsPrimaryKey).Select(x => x.ColumnName).FirstOrDefault();
                        Job.Tables.Add(st);
                    }

                    await sourceQuery.SetupChangeTrackingAsync(Job.Tables);

                }

                using (var destQuery = await Job.Destination.OpenAsync()) {

                    // create ReplicationState table...
                    await destQuery.ExecuteAsync(Scripts.CreateReplicationStateTable);

                    await Task.WhenAll(Job.Tables.Select(x=>SyncTableSchema(x)));

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

            DateTime now = DateTime.UtcNow;
            using (var sourceQuery = await Job.Source.OpenAsync()) {
                using (var destQuery = await Job.Destination.OpenAsync()) {


                }
            }
        }

        public void Dispose()
        {
            
        }
    }
}
