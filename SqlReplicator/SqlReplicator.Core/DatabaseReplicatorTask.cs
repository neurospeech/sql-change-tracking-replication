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

        public List<SqlTable> Tables { get; }
            = new List<SqlTable>();

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
                        Tables.Add(st);
                    }

                    await sourceQuery.SetupChangeTrackingAsync(Tables);

                }
            }

            await Task.WhenAll( Job.Tables.Select( x=> SyncTable(x) ) );

        }

        private async Task SyncTable(SqlTable x)
        {
            using (var sourceQuery = await Job.Source.OpenAsync()) {
                using (var destQuery = await Job.Destination.OpenAsync()) {


                    // fetch few records....


                    // try to save them...


                }
            }
        }

        public void Dispose()
        {
            
        }
    }
}
