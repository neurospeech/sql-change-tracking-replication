using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;


namespace SqlReplicator
{

    public static class StringExtensions
    {

        //public static string Combine(this IEnumerable<string> en, string joinText = ", ") {
        //    return string.Join(joinText, en);
        //}

        public static string ToText<T>(this IEnumerable<T> en, string joinText, Func<T, string> selector)
        {
            return string.Join(joinText, en.Select(selector));
        }
    }

}


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

        public abstract DbCommand CreateCommand(String command, params KeyValuePair<string, object>[] plist);

        public async Task<SqlRowSet> ReadAsync(string command, params KeyValuePair<string, object>[] plist)
        {
            var cmd = CreateCommand(command, plist);
            return new SqlRowSet(cmd, await cmd.ExecuteReaderAsync());
        }

        public async Task<int> ExecuteAsync(string command, params KeyValuePair<string, object>[] plist)
        {
            using (var cmd = CreateCommand(command, plist))
            {
                return await cmd.ExecuteNonQueryAsync();
            }
        }

        public async Task<T> ExecuteScalarAsync<T>(string command, params KeyValuePair<string,object>[] plist) {
            using (var cmd = CreateCommand(command, plist)) {
                var obj = await cmd.ExecuteScalarAsync();
                if (obj == null)
                    return default(T);
                Type t = typeof(T);
                if (t == obj.GetType())
                    return (T)obj;
                return (T)Convert.ChangeType(obj, t);
            }
        }

        public abstract void Dispose();

        public abstract Task<List<SqlColumn>> GetCommonSchemaAsync(string tableName = null);
        public abstract Task SyncSchema(string name, List<SqlColumn> schemaTable);
        internal abstract Task SetupChangeTrackingAsync(List<SqlTable> tables);

        public abstract Task<SyncState> GetLastSyncVersion(SqlTable srcTable);
        public abstract Task UpdateSyncState(SyncState srcTable);

        public abstract Task<long> GetCurrentVersionAsync(SqlTable srcTable);

        public abstract Task ReadMaxPrimaryKeys(SqlTable srcTable);

        public abstract Task<IEnumerable<ChangedData>> ReadChangedRows(SqlTable srcTable, long lastVersion);

        public abstract Task<SqlRowSet> ReadObjectsAbovePrimaryKeys(SqlTable srcTable);
        public abstract Task<bool> WriteToServerAsync(SqlTable table,SqlRowSet r);
        internal abstract Task WriteToServerAsync(SqlTable srcTable, IEnumerable<ChangedData> changes);


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

    public class SyncState {
        public string TableName { get; set; }
        public DateTime? EndSync { get; set; }
        public DateTime? BeginSync { get; set; }
        public DateTime? LastFullSync { get; set; }
        public string LastSyncResult { get; set; }
        public long LastVersion { get; set; }
    }

}
