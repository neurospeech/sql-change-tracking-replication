using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlReplicator.Core
{
    public class MySqlQuery : SqlQuery
    {
        MySqlConnection conn;
        public MySqlQuery(ConfigDatabase config) : base(config)
        {

        }

        #region CreateCommand
        public override DbCommand CreateCommand(string command, params KeyValuePair<string, object>[] plist)
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = command;
            foreach (var item in plist)
            {
                cmd.Parameters.AddWithValue(item.Key, item.Value);
            }
            return cmd;
        }
        #endregion

        public override void Dispose()
        {
            conn.Dispose();
        }

        public override string Escape(string text)
        {
            return $"`{text}`";
        }

        #region GetCommonSchemaAsync
        public async override Task<List<SqlColumn>> GetCommonSchemaAsync(string tableName = null)
        {
            List<SqlColumn> columns = new List<SqlColumn>();
            using (var reader = await ReadAsync($"select * from INFORMATION_SCHEMA.COLUMNS where table_schema  = @TableName",
                new KeyValuePair<string, object>("@TableName", tableName)))
            {
                while (await reader.ReadAsync())
                {
                    SqlColumn col = new MySqlColumn();
                    col.TableName = reader.GetValue<string>("Table_Name");
                    col.TableID = reader.GetValue<long>("Table_Name");
                    col.ID = reader.GetValue<long>("Column_Name");
                    col.IsIdentity = reader.GetValue<string>("EXTRA")
                        .Equals("auto_increment", StringComparison.OrdinalIgnoreCase);
                    col.ColumnName = reader.GetValue<string>("Column_Name");
                    col.IsPrimaryKey = reader.GetValue<string>("Column_Key")
                        .Equals("pri", StringComparison.OrdinalIgnoreCase);
                    col.IsNullable = reader.GetValue<string>("Is_Nullable")
                        .Equals("YES", StringComparison.OrdinalIgnoreCase);
                    col.ColumnDefault = reader.GetValue<string>("Column_Default");
                    col.DataType = reader.GetValue<string>("Data_Type");
                    col.DataLength = reader.GetValue<int>("Character_Maximum_Length");
                    col.NumericPrecision = reader.GetValue<decimal?>("Numeric_Precision");
                    col.NumericScale = reader.GetValue<decimal?>("Numeric_Scale");

                    columns.Add(col);
                }

            }
            return columns;
        }
        #endregion

        public override Task<long> GetCurrentVersionAsync(SqlTable srcTable)
        {
            throw new NotImplementedException();
        }

        #region GetLastSyncVersion
        public async override Task<SyncState> GetLastSyncVersion(SqlTable srcTable)
        {
            string name = srcTable.Name;

            await ExecuteAsync(Scripts.BeginSyncRST,
                new KeyValuePair<string, object>("@TableName", name));



            string primaryKeyJoinOn = string.Join(" AND ",
                srcTable.PrimaryKey.Select(x => $"CT.{x.ColumnName} = T.{x.ColumnName}"));

            using (var r = await ReadAsync(
                "SELECT * FROM CT_REPLICATIONSTATE WHERE TableName=@TableName",
                new KeyValuePair<string, object>("@TableName", name)))
            {

                SyncState ss = new Core.SyncState();

                if (await r.ReadAsync())
                {
                    ss.TableName = r.GetValue<string>("TableName");
                    ss.BeginSync = r.GetValue<DateTime?>("BeginSync");
                    ss.EndSync = r.GetValue<DateTime?>("EndSync");
                    ss.LastFullSync = r.GetValue<DateTime>("LastFullSync");
                    ss.LastSyncResult = r.GetValue<string>("LastSyncResult");
                    ss.LastVersion = r.GetValue<long>("LastVersion");

                }
                return ss;
            }
        }
        #endregion

        #region Open
        public async override Task Open()
        {
            String connectionString = config.ConnectionString;
            if (string.IsNullOrEmpty(connectionString))
            {
                MySqlConnectionStringBuilder builder = new MySqlConnectionStringBuilder();
                builder.Server = config.Server;
                if (!string.IsNullOrEmpty(config.Username))
                {
                    builder.UserID = config.Username;
                    builder.Password = config.Password;
                }
                builder.Database = config.Database;
                builder.IntegratedSecurity = config.TrustedConnection;
                //builder.Encrypt = config.Encrypt;
                // builder.TrustServerCertificate = config.TrustCertificate;
                connectionString = builder.ConnectionString;
            }
            conn = new MySqlConnection(connectionString);

            await conn.OpenAsync();

            Connection = conn;
        }

        #endregion

        public override Task<IEnumerable<ChangedData>> ReadChangedRows(SqlTable srcTable, long lastVersion)
        {
            throw new NotImplementedException();
        }

        #region ReadMaxPrimaryKeys
        public async override Task ReadMaxPrimaryKeys(SqlTable srcTable)
        {
            foreach (var pk in srcTable.PrimaryKey)
            {
                pk.LastValue = null;
            }

            var pkNames = string.Join(",", srcTable.PrimaryKey.Select(x => $"{Escape(x.ColumnName)}"));
            var pkOrderBy = string.Join(",", srcTable.PrimaryKey.Select(x => $"{Escape(x.ColumnName)} DESC"));

            // get last primary keys....
            string spk = $"SELECT TOP 1 { pkNames } FROM {Escape(srcTable.Name)} ORDER BY {pkOrderBy}";
            using (var r = await ReadAsync(spk))
            {
                if (await r.ReadAsync())
                {
                    foreach (var pk in srcTable.PrimaryKey)
                    {
                        pk.LastValue = r.GetValue<object>(pk.ColumnName);
                    }
                }
            }
        }
        #endregion

        #region ReadObjectsAbovePrimaryKeys
        public override Task<SqlRowSet> ReadObjectsAbovePrimaryKeys(SqlTable srcTable)
        {
            string s = $"SELECT TOP 1000 * FROM {Escape(srcTable.Name)}";
            KeyValuePair<string, object>[] parray = null;



            if (srcTable.PrimaryKey.Any(x => x.LastValue != null))
            {
                s += $" WHERE { string.Join(" AND ", srcTable.PrimaryKey.Select(x => $"{Escape(x.ColumnName)} > @C{x.ID}"))} ";
                parray = srcTable.PrimaryKey.Select(x => new KeyValuePair<string, object>($"@C{x.ID}", x.LastValue)).ToArray();
            }

            s += $" ORDER BY { string.Join(",", srcTable.PrimaryKey.Select(x => $"{Escape(x.ColumnName)}")) }";

            return ReadAsync(s, parray);
        } 
        #endregion

        public override Task SyncSchema(string name, List<SqlColumn> schemaTable)
        {
            throw new NotImplementedException();
        }

        #region UpdateSyncState
        public override Task UpdateSyncState(SyncState s)
        {
            return ExecuteScalarAsync<int>(Scripts.UpdateRST,
                new KeyValuePair<string, object>("@TableName", s.TableName),
                new KeyValuePair<string, object>("@LastFullSync", s.LastFullSync),
                new KeyValuePair<string, object>("@LastSyncResult", s.LastSyncResult),
                new KeyValuePair<string, object>("@LastVersion", s.LastVersion));
        }
        #endregion

        public override Task<bool> WriteToServerAsync(SqlTable table, SqlRowSet r)
        {
            throw new NotImplementedException();
        }

        internal override Task SetupChangeTrackingAsync(List<SqlTable> tables)
        {
            throw new NotImplementedException();
        }

        internal override Task WriteToServerAsync(SqlTable srcTable, IEnumerable<ChangedData> changes, SyncState state)
        {
            throw new NotImplementedException();
        }
    }
}
