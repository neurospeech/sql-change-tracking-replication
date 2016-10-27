using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;

namespace SqlReplicator.Core
{
    public abstract class SqlQuery : IDisposable
    {


        protected ConfigDatabase config;


        public SqlQuery(ConfigDatabase config)
        {
            this.config = config;
        }

        public DbConnection Connection { get; protected set; }

        public abstract Task Open();

        public abstract DbCommand CreateCommand(String command, Dictionary<string, object> plist = null);

        public async Task<SqlRowSet> ReadAsync(string command, Dictionary<string, object> plist = null)
        {
            var cmd = CreateCommand(command, plist);
            return new SqlRowSet(cmd, await cmd.ExecuteReaderAsync());
        }

        public async Task<int> ExecuteAsync(string command, Dictionary<string, object> plist = null)
        {
            using (var cmd = CreateCommand(command, plist))
            {
                return await cmd.ExecuteNonQueryAsync();
            }
        }


        public abstract void Dispose();

        public abstract Task<List<SqlColumn>> GetCommonSchemaAsync(string tableName = null);
        public abstract Task SyncSchema(string name, List<SqlColumn> schemaTable);
        internal abstract Task SetupChangeTrackingAsync(List<SqlTable> tables);



        /*public async Task<IEnumerable<T>> FetchAsync<T>(string query, Dictionary<string, object> plist = null) {
            using (var rowSet = await ReadAsync(query, plist)) {

                Type type = typeof(T);
                Func<SqlRowSet,object, object> f = loader.GetOrAdd(type, LoadObject(type));


                while (await rowSet.ReadAsync()) {

                    var obj = Activator.CreateInstance<T>();

                }
            }
        }

        private static ConcurrentDictionary<Type, Func<SqlRowSet,object, object>> loader
            = new ConcurrentDictionary<Type, Func<SqlRowSet,object, object>>();*/

    }

}
