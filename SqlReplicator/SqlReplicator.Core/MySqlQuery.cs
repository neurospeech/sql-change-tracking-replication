using Microsoft.SqlServer.Types;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.Entity.Spatial;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace SqlReplicator.Core
{
    public class MySqlQuery : SqlServerQuery
    {
        MySqlConnection conn;
        public MySqlQuery(ConfigDatabase config) : base(config)
        {
            Log = true;
        }

        #region CreateCommand
        public override DbCommand CreateCommand(string command, IEnumerable<KeyValuePair<string, object>> plist = null)
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = command;
            if (plist != null)
            {
                foreach (var item in plist)
                {
                    cmd.Parameters.AddWithValue(item.Key, item.Value ?? DBNull.Value);
                }
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
            using (var reader = await ReadAsync($"select * from INFORMATION_SCHEMA.COLUMNS where table_schema = @Schema AND table_name = @TableName",
                new KeyValuePair<string, object>("@Schema", this.config.Database),
                new KeyValuePair<string, object>("@TableName", tableName)))
            {
                while (await reader.ReadAsync())
                {
                    SqlColumn col = new MySqlColumn();
                    col.TableName = reader.GetValue<string>("Table_Name");
                    if (!col.TableName.Equals(tableName, StringComparison.OrdinalIgnoreCase))
                        continue;
                    // col.TableID = reader.GetValue<long>("Table_Name");
                    // col.ID = reader.GetValue<long>("Column_Name");
                    col.IsIdentity = reader.GetValue<string>("EXTRA")
                        .Equals("auto_increment", StringComparison.OrdinalIgnoreCase);
                    col.ColumnName = reader.GetValue<string>("Column_Name");
                    col.IsPrimaryKey = reader.GetValue<string>("Column_Key")
                        .Equals("pri", StringComparison.OrdinalIgnoreCase);
                    col.IsNullable = reader.GetValue<string>("Is_Nullable")
                        .Equals("YES", StringComparison.OrdinalIgnoreCase);
                    col.ColumnDefault = reader.GetValue<string>("Column_Default");
                    col.DataType = reader.GetValue<string>("Data_Type");
                    col.DataLength = reader.GetValue<long>("Character_Maximum_Length");
                    if (col.DataLength >= int.MaxValue)
                    {
                        col.DataLength = -1;
                    }
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
            var beginSync = $"INSERT INTO CT_REPLICATIONSTATE(BeginSync, TableName) VALUES (UTC_TIMESTAMP(), @TableName) ON DUPLICATE KEY UPDATE BeginSync=UTC_TIMESTAMP()";
            await ExecuteAsync(beginSync,
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
                var tokens = config.Server.Split(':');
                builder.Server = tokens[0];
                if (tokens.Length > 1)
                {
                    builder.Port = uint.Parse(tokens[1]);
                }
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
            string spk = $"SELECT { pkNames } FROM {Escape(srcTable.Name)} ORDER BY {pkOrderBy} LIMIT 1";
            using (var r = await ReadAsync(spk))
            {
                if (await r.ReadAsync())
                {
                    foreach (var pk in srcTable.PrimaryKey)
                    {
                        pk.LastValue = r.GetValue<object>(pk.ColumnName);
                    }
                } else
                {
                    return;
                }
            }

            if (Log)
            {
                Trace.WriteLine($"PK Values: { string.Join(",", srcTable.PrimaryKey.Select(p => p.LastValue.ToString()) )}");
            }
        }
        #endregion

        #region ReadObjectsAbovePrimaryKeys
        public override Task<SqlRowSet> ReadObjectsAbovePrimaryKeys(SqlTable srcTable)
        {
            string s = $"SELECT TOP 1000 * FROM {Escape(srcTable.Name)}";
            IEnumerable<KeyValuePair<string, object>> parray = null;



            if (srcTable.PrimaryKey.Any(x => x.LastValue != null))
            {
                s += $" WHERE { string.Join(" AND ", srcTable.PrimaryKey.Select(x => $"{Escape(x.ColumnName)} > @C{x.ID}"))} ";
                parray = srcTable.PrimaryKey.Select(x => new KeyValuePair<string, object>($"@C{x.ID}", x.LastValue));
            }

            s += $" ORDER BY { string.Join(",", srcTable.PrimaryKey.Select(x => $"{Escape(x.ColumnName)}")) }";

            return ReadAsync(s, parray);
        }
        #endregion

        public override Task CreateReplicationStateTable()
        {
            return ExecuteAsync(Scripts.MySqlCreateReplicationTable);
        }

        public override async Task SyncSchema(string name, List<SqlColumn> columns)
        {
            var allColumns = new List<SqlColumn>();

            var destColumns = await GetCommonSchemaAsync(name);

            var primaryKeys = columns.Where(x => x.IsPrimaryKey).ToList();

            string createTable = $" CREATE TABLE IF NOT EXISTS {Escape(name)} " +
                $"({ string.Join(",", primaryKeys.Select(c => ToColumn(c))) }, ";

            if (destColumns.Count == 0)
            {
                await ExecuteAsync($"DROP TABLE IF EXISTS {Escape(name)}");
                var otherColumns = columns.Where(x => !x.IsPrimaryKey).Select(x => ToColumn(x));
                if (otherColumns.Any())
                {
                    createTable += string.Join(", \r\n", otherColumns) + ", ";
                }
            }

            createTable += $" PRIMARY KEY({ string.Join(",", primaryKeys.Select(x => Escape(x.ColumnName))) }) )";

            await ExecuteAsync(createTable);

            if (destColumns.Count == 0)
            {
                return;
            }

            List<SqlColumn> columnsToAdd = new List<SqlColumn>();

            foreach (var column in columns)
            {
                var dest = destColumns.FirstOrDefault(x => x.ColumnName == column.ColumnName);
                if (dest == null)
                {
                    columnsToAdd.Add(column);
                    continue;
                }
                if (dest.Equals(column))
                {
                    continue;
                }
                columnsToAdd.Add(column);
                // rename....

                long m = DateTime.UtcNow.Ticks;

                await ExecuteAsync($"ALTER TABLE {Escape(name)} RENAME COLUMN {Escape(dest.ColumnName)} TO " +
                    $" {Escape($"{dest.ColumnName}_{m}")}");

            }

            var columnsQuery = string.Join(",\r\n\t", columnsToAdd.Select(c => $"ADD {ToColumn(c)}"));
            await ExecuteAsync($"ALTER TABLE {Escape(name)} {columnsQuery}");

            Console.WriteLine($"Table {name} sync complete");
        }

        string ToDataType(SqlColumn c)
        {
            switch (c.DbType)
            {
                case System.Data.DbType.AnsiString:
                case System.Data.DbType.String:
                    return "longtext";
                case System.Data.DbType.AnsiStringFixedLength:
                case System.Data.DbType.StringFixedLength:
                    return "varchar";
                case System.Data.DbType.Int64:
                    return "bigint";
                case System.Data.DbType.Binary:
                    if (c.DataLength == -1)
                    {
                        return "longblob";
                    }
                    return "binary";
                case System.Data.DbType.Byte:
                    return "byte";
                case System.Data.DbType.Boolean:
                    return "boolean";
                case System.Data.DbType.Currency:
                    return "money";
                case System.Data.DbType.Date:
                    return "date";
                case System.Data.DbType.DateTime:
                    return "datetime";
                case System.Data.DbType.Decimal:
                    return "decimal";
                case System.Data.DbType.Double:
                    return "float";
                case System.Data.DbType.Guid:
                    return "varchar(36)";
                case System.Data.DbType.Int16:
                    return "int";
                case System.Data.DbType.Int32:
                    return "int";
                case System.Data.DbType.Object:
                    return "geometry";
                case System.Data.DbType.SByte:
                    break;
                case System.Data.DbType.Single:
                    return "float";
                case System.Data.DbType.Time:
                    return "TIMESTAMP";
                case System.Data.DbType.UInt16:
                    return "int";
                case System.Data.DbType.UInt32:
                    return "int";
                case System.Data.DbType.UInt64:
                    return "bigint";
                case System.Data.DbType.VarNumeric:
                    break;
                case System.Data.DbType.Xml:
                    break;
                case System.Data.DbType.DateTime2:
                case System.Data.DbType.DateTimeOffset:
                    return "DATETIME";
            }
            throw new NotImplementedException();
        }
        protected override string ToColumn(SqlColumn c)
        {
            var dt = ToDataType(c);

            var name = $"{Escape(c.ColumnName)} {dt}";
            if (IsText(c.DataType))
            {
                if (
                    c.DbType == System.Data.DbType.String || 
                    c.DbType == System.Data.DbType.AnsiString || (c.DbType == System.Data.DbType.Binary && c.DataLength  == -1)) { }
                else
                {
                    if (c.DataLength > 0 && c.DataLength < int.MaxValue)
                    {
                        name += $"({ c.DataLength })";
                    }
                    else
                    {
                        name += "(MAX)";
                    }
                }
            }
            if (IsDecimal(c.DataType))
            {
                name += $"({ c.NumericPrecision },{ c.NumericScale })";
            }
            if (!c.IsPrimaryKey)
            {
                // lets allow nullable to every field...
                if (c.IsNullable)
                {
                    name += " NULL ";
                }
                else
                {
                    name += " NOT NULL ";
                }
            }
            if (!string.IsNullOrWhiteSpace(c.ColumnDefault))
            {
                name += " DEFAULT " + GetDefaultColumnValue(c);
            }
            if (c.IsIdentity)
            {
                name += " AUTO_INCREMENT";
            }
            return name;
        }

        string GetDefaultColumnValue(SqlColumn c)
        {
            if (c.ColumnDefault.StartsWith("("))
            {
                c.ColumnDefault = c.ColumnDefault.Substring(1);
            }
            if (c.ColumnDefault.EndsWith(")"))
            {
                c.ColumnDefault = c.ColumnDefault.Substring(0, c.ColumnDefault.Length - 1 );
            }
            switch (c.DbType)
            {
                case System.Data.DbType.Int16:
                case System.Data.DbType.Int32:
                case System.Data.DbType.Int64:
                case System.Data.DbType.Boolean:
                case System.Data.DbType.Double:
                case System.Data.DbType.Decimal:
                    if (c.ColumnDefault == "(0)")
                        return " 0";
                    break;
                case System.Data.DbType.Guid:
                    return "( UUID())";
                case System.Data.DbType.Date:
                case System.Data.DbType.DateTime:
                case System.Data.DbType.DateTime2:
                case System.Data.DbType.DateTimeOffset:
                    // ((2016)-(12))-(1)
                    if (c.ColumnDefault.StartsWith("((") && c.ColumnDefault.EndsWith(")")) {
                        var tokens = c.ColumnDefault.Split('-')
                            .Select(x => x.Trim('(', ')'));
                        return "\"" + string.Join("-", tokens) + "\"";
                    }
                    if (c.ColumnDefault.ToLower().Contains("getutcdate"))
                    {
                        return " (UTC_TIMESTAMP())";
                    }
                    if (c.ColumnDefault.ToLower().Contains("getdate"))
                    {
                        return " (now())";
                    }
                    return c.ColumnDefault;
            }
            return c.ColumnDefault;
        }


        #region UpdateSyncState
        public override Task UpdateSyncState(SyncState s)
        {
            return ExecuteScalarAsync<int>($"REPLACE INTO CT_REPLICATIONSTATE SET" +
                $" EndSync = now(), TableName = @TableName, LastFullSync = @LastFullSync," +
                $" LastSyncResult = @LastSyncResult, LastVersion = @LastVersion",
                new KeyValuePair<string, object>("@TableName", s.TableName),
                new KeyValuePair<string, object>("@LastFullSync", s.LastFullSync),
                new KeyValuePair<string, object>("@LastSyncResult", s.LastSyncResult),
                new KeyValuePair<string, object>("@LastVersion", s.LastVersion));
        }
        #endregion

        public override async Task<bool> WriteToServerAsync(SqlTable table, SqlRowSet r)
        {
            var cmd = $"REPLACE INTO {Escape(table.Name)} ({ string.Join(",", table.Columns.Select(x => Escape(x.ColumnName))) }) VALUES ";
            StringBuilder batch = new StringBuilder();
            while (true)
            {
                batch.Clear();
                batch.Append(cmd);
                var reader = r.Reader;
                List<KeyValuePair<string, object>> plist = new List<KeyValuePair<string, object>>();
                for (int i = 0; i < 100; i++)
                {
                    if(await r.ReadAsync()) {
                        int n = 0;
                        if (i > 0)
                        {
                            batch.Append(',');
                        }
                        batch.Append('(');
                        foreach (var item in table.Columns)
                        {
                            if (n>0)
                            {
                                batch.Append(',');
                            }
                            var pn = $"@p{i}_{n++}";
                            var rv = r.GetRawValue(item.ColumnName);
                            if (rv is SqlGeography dg) {
                                
                                rv = dg.ToString();
                                batch.Append($"ST_GeomFromText({pn})");
                            }
                            else
                            {
                                batch.Append(pn);
                            }
                            plist.Add(new KeyValuePair<string, object>(pn, rv));
                        }
                        batch.Append(')');
                        continue;
                    }
                    break;
                }
                if (plist.Count == 0)
                {
                    break;
                }
                await ExecuteAsync(batch.ToString(), plist);
            }
            return true;
        }

        internal override Task SetupChangeTrackingAsync(List<SqlTable> tables)
        {
            throw new NotImplementedException();
        }

        internal override async Task WriteToServerAsync(SqlTable srcTable, IEnumerable<ChangedData> changes, SyncState state)
        {
            using(var tx = await conn.BeginTransactionAsync(System.Data.IsolationLevel.ReadCommitted)) {

                foreach(var change in changes)
                {
                    switch (change.Operation)
                    {
                        case ChangeOperation.Delete:
                            var pklist = change.PrimaryKeys.Select(x => $"{Escape(x.FieldName)} = {x.ParamName}");
                            var pkvlist = change.PrimaryKeys.Select(x => new KeyValuePair<string, object>(x.ParamName, x.Value));
                            await ExecuteAsync($"DELETE FROM {srcTable.Name} WHERE {string.Join(",", pklist)}", 
                                pkvlist);
                            break;
                        case ChangeOperation.Update:
                        case ChangeOperation.Insert:
                            var plist = change.ChangedValues.Select(x => $"{Escape(x.FieldName)} = {x.ParamName}");
                            var pvlist = change.ChangedValues.Select(x => new KeyValuePair<string, object>(x.ParamName, x.Value));
                            await ExecuteAsync($"REPLACE INTO {srcTable.Name} {string.Join(",", plist)}", pvlist);
                            break;
                    }
                }

                tx.Commit();
            }
        }
    }
}
