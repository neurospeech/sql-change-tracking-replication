using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlReplicator.Core
{
    public class SqlServerQuery : SqlQuery
    {

        SqlConnection conn;



        public SqlServerQuery(ConfigDatabase config) :base(config)
        {
            
        }

        public override async Task Open()
        {            
            String connectionString = config.ConnectionString;
            if (string.IsNullOrEmpty(connectionString))
            {
                SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
                builder.DataSource = config.Server;
                if (!string.IsNullOrEmpty(config.Username))
                {
                    builder.UserID = config.Username;
                    builder.Password = config.Password;
                }
                builder.InitialCatalog = config.Database;
                builder.IntegratedSecurity = config.TrustedConnection;
                builder.Encrypt = config.Encrypt;
                builder.TrustServerCertificate = config.TrustCertificate;
                connectionString = builder.ConnectionString;
            }
            conn = new SqlConnection(connectionString);

            await conn.OpenAsync();

            Connection = conn;
        }

        public override void Dispose()
        {
            conn?.Dispose();
            conn = null;
        }

        public override DbCommand CreateCommand(String command, params KeyValuePair<string, object>[] plist)
        {
            var cmd = conn.CreateCommand();
            cmd.CommandText = command;
            if (plist != null)
            {
                foreach (var p in plist)
                {
                    cmd.Parameters.AddWithValue(p.Key, p.Value ?? DBNull.Value);
                }
            }
            return cmd;
        }


        public override async Task<List<SqlColumn>> GetCommonSchemaAsync(string tableName = null)
        {


            List<SqlColumn> columns = new List<SqlColumn>();
            string sqlColumns = Scripts.SqlServerGetSchema;

            using (var reader = await ReadAsync(sqlColumns, 
                new KeyValuePair<string,object>( "@TableName", tableName )
            ))
            {


                while (await reader.ReadAsync())
                {
                    SqlColumn col = new SqlColumn();
                    col.TableName = reader.GetValue<string>("TableName");
                    col.ColumnName = reader.GetValue<string>("ColumnName");
                    col.IsPrimaryKey = reader.GetValue<bool>("IsPrimaryKey");
                    col.IsNullable = reader.GetValue<string>("IsNullable") == "YES";
                    col.ColumnDefault = reader.GetValue<string>("ColumnDefault");
                    col.DataType = reader.GetValue<string>("DataType");
                    col.DataLength = reader.GetValue<int>("DataLength");
                    col.NumericPrecision = reader.GetValue<decimal?>("NumericPrecision");
                    col.NumericScale = reader.GetValue<decimal?>("NumericScale");

                    columns.Add(col);
                }

            }
            return columns;
        }

        public async override Task SyncSchema(string name, List<SqlColumn> columns)
        {

            var allColumns = new List<SqlColumn>();

            var primaryKeys = columns.Where(x => x.IsPrimaryKey).ToList();

            string createTable = $"IF NOT EXISTS (SELECT * FROM sysobjects WHERE Name='{name}' AND xtype='U')"
                + $" CREATE TABLE {name} ({ string.Join(",", primaryKeys.Select(c => ToColumn(c))) }";

            createTable += $" CONSTRAINT PK_{name} PRIMARY KEY CLUSTERED ({ string.Join(", ", primaryKeys.Select(x=>$"[{x.ColumnName}]")) }))";

            using (var cmd = CreateCommand(createTable))
            {
                var n = await cmd.ExecuteNonQueryAsync();
            }

            var destColumns = await GetCommonSchemaAsync(name);

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

                using (var cmd = CreateCommand($"EXEC sp_rename '[{name}].[{dest.ColumnName}]', '[{dest.ColumnName}_{m}]'"))
                {
                    await cmd.ExecuteNonQueryAsync();
                }

            }

            foreach (var column in columnsToAdd)
            {
                using (var cmd = CreateCommand($"ALTER TABLE [{name}] ADD " + ToColumn(column)))
                {
                    await cmd.ExecuteNonQueryAsync();
                }
            }

            Console.WriteLine($"Table {name} sync complete");
        }

        internal async override Task SetupChangeTrackingAsync(List<SqlTable> tables)
        {

            await FetchIDsAsync(tables);

            foreach (var table in tables) {


                bool changeTrackingEnabled = false;
                bool columnTrackingEnabled = false;

                using (var reader = await ReadAsync($"select * from sys.change_tracking_tables where object_id={table.ID}")) {

                    changeTrackingEnabled = await reader.ReadAsync();
                    if (changeTrackingEnabled) {
                        columnTrackingEnabled = reader.GetValue<bool>("is_track_columns_updated_on");
                    }
                }

                if (!columnTrackingEnabled) {
                    if (changeTrackingEnabled) {
                        await ExecuteAsync($"ALTER TABLE [{table.Name}] DISABLE CHANGE_TRACKING");
                    }
                    await ExecuteAsync($"ALTER TABLE [{table.Name}] ENABLE CHANGE_TRACKING WITH(TRACK_COLUMNS_UPDATED = ON)");
                }

                using (var reader = await ReadAsync($"SELECT MIN(EXT.SYS_CHANGE_VERSION) as Value FROM CHANGETABLE(CHANGES [{table.Name}],0) AS EXT")) {
                    if (await reader.ReadAsync()) {
                        table.LastVersion = reader.GetValue<long>("Value");
                    }
                }

            }
        }

        private async Task FetchIDsAsync(List<SqlTable> tables)
        {
            using (var reader = await ReadAsync(Scripts.SqlServerGetColumns)) {
                while (await reader.ReadAsync()) {
                    long tableID = reader.GetValue<long>("TableID");
                    string tableName = reader.GetValue<string>("TableName");
                    long columnID = reader.GetValue<long>("ColumnID");
                    string columnName = reader.GetValue<string>("ColumnName");

                    var table = tables.FirstOrDefault(x => x.Name.Equals(tableName, StringComparison.OrdinalIgnoreCase));
                    table.ID = tableID;

                    var column = table.Columns.FirstOrDefault(x => x.ColumnName.Equals(columnName, StringComparison.OrdinalIgnoreCase));
                    column.ID = columnID;
                }
            }
        }

        private static string[] textTypes = new[] { "nvarchar", "varchar", "varbinary" };
        

        private static bool IsText(string n) => textTypes.Any(a => a.Equals(n, StringComparison.OrdinalIgnoreCase));

        private static bool IsDecimal(string n) => n.Equals("decimal", StringComparison.OrdinalIgnoreCase);

        private string ToColumn(SqlColumn c)
        {
            var name = $"[{c.ColumnName}] {c.DataType}";
            if (IsText(c.DataType))
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
                else {
                    name += " NOT NULL ";
                }
            }
            if (!string.IsNullOrWhiteSpace(c.ColumnDefault)) {
                name += " DEFAULT " + c.ColumnDefault;
            }
            return name;
        }

        public override async Task<SyncState> GetLastSyncVersion(SqlTable srcTable)
        {
            string name = srcTable.Name;

            await ExecuteAsync(Scripts.BeginSyncRST,
                new KeyValuePair<string, object>("@TableName", name));



            string primaryKeyJoinOn = string.Join(" AND ",
                srcTable.PrimaryKey.Select(x => $"CT.{x.ColumnName} = T.{x.ColumnName}"));

            var r = await ReadAsync(
                "SELECT * FROM CT_REPLICATIONSTATE WHERE TableName=@TableName",
                new KeyValuePair<string, object>("@TableName", name));

            SyncState ss = new Core.SyncState();

            if (await r.ReadAsync()) {
                ss.BeginSync = r.GetValue<DateTime?>("BeginSync");
                ss.EndSync = r.GetValue<DateTime?>("EndSync");
                ss.LastFullSync = r.GetValue<DateTime?>("LastFullSync");
                ss.LastSyncResult = r.GetValue<string>("LastSyncResult");
                ss.LastVersion = r.GetValue<long>("LastVersion");

            }

            return ss;
        }

        public override Task<long> GetCurrentVersionAsync(SqlTable srcTable)
        {
            return ExecuteScalarAsync<long>("SELECT CHANGE_TRACKING_CURRENT_VERSION ( ) ");
        }

        public override Task UpdateSyncState(SyncState s)
        {
            return ExecuteScalarAsync<int>(Scripts.UpdateRST,
                new KeyValuePair<string, object>("@TableName", s.TableName),
                new KeyValuePair<string, object>("@LastFullSync", s.LastFullSync),
                new KeyValuePair<string, object>("@LastSyncResult", s.LastSyncResult),
                new KeyValuePair<string, object>("@LastVersion", s.LastVersion));

        }

        public async override Task ReadMaxPrimaryKeys(SqlTable srcTable)
        {

            foreach (var pk in srcTable.PrimaryKey) {
                pk.LastValue = null;
            }

            var pkNames = string.Join(",", srcTable.PrimaryKey.Select(x => $"[{x.ColumnName}]"));
            var pkOrderBy = string.Join(",", srcTable.PrimaryKey.Select(x => $"[{x.ColumnName}] DESC"));

            // get last primary keys....
            string spk = $"SELECT TOP 1 { pkNames } FROM [{srcTable.Name}] ORDER BY {pkOrderBy}";
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

        public override async Task<IEnumerable<ChangedData>> ReadChangedRows(SqlTable srcTable, long lastVersion)
        {
            List<ChangedData> cdList = new List<Core.ChangedData>();
            string tableName = srcTable.Name;
            string primaryKeyJoinOn = string.Join(" AND ", srcTable.PrimaryKey.Select( x=> $"T.[{x.ColumnName}] = CT.[{x.ColumnName}]" ));

            var changeDetection = string.Join(",", srcTable.Columns.Select(
                x => x.IsPrimaryKey ?
                    $"T.{x.ColumnName} AS D_{x.ColumnName}" :
                    $"(CASE CHANGE_TRACKING_IS_COLUMN_IN_MASK({x.ID},CT.SYS_CHANGE_COLUMNS) WHEN 1 THEN T.[{x.ColumnName}]  ELSE NULL END) as [D_{x.ColumnName}], CHANGE_TRACKING_IS_COLUMN_IN_MASK({x.ID},CT.SYS_CHANGE_COLUMNS) AS [IC_{x.ColumnName}]")
                );

            string sq = $"SELECT TOP 100 {changeDetection}, CT.SYS_CHANGE_OPERATION AS C_OP, CT.SYS_CHANGE_VERSION AS C_V FROM CHANGETABLE(CHANGES {tableName},@lastVersion) AS CT "
                + $"JOIN [{tableName}] as T ON ({primaryKeyJoinOn})"
                + " ORDER BY CT.SYS_CHANGE_VERSION ASC";

            var otherColumns = srcTable.Columns.Where(x => !x.IsPrimaryKey).ToArray();

            using (var r = await ReadAsync(sq, new KeyValuePair<string, object>("@lastVersion", lastVersion))) {
                while (await r.ReadAsync()) {

                    var cd = new ChangedData { };

                    foreach (var pk in srcTable.PrimaryKey) {
                        object v = r.GetValue<object>($"D_{pk.ColumnName}");
                        cd.PrimaryKeys.Add(new KeyValuePair<string, object>(pk.ColumnName,v));
                    }

                    foreach (var c in otherColumns) {
                        if (r.GetValue<bool>("IC_" + c.ColumnName)) {
                            object v = r.GetValue<object>($"D_{c.ColumnName}");
                            cd.ChangedValues.Add(new KeyValuePair<string, object>(c.ColumnName,v));
                        }
                    }

                    cd.LastVersion = r.GetValue<long>("C_V");

                    cd.Operation = ChangeOperation.Update;
                    var op = r.GetValue<string>("C_OP");
                    if (op == "I")
                    {
                        cd.Operation = ChangeOperation.Insert;
                    }
                    else if(op == "D"){
                        cd.Operation = ChangeOperation.Delete;
                    }

                    cdList.Add(cd);

                }
            }
            return cdList;
        }

        public override Task<SqlRowSet> ReadObjectsAbovePrimaryKeys(SqlTable srcTable)
        {
            string s = $"SELECT TOP 1000 * FROM [{srcTable.Name}]";
            KeyValuePair<string, object>[] parray = null;

            

            if (srcTable.PrimaryKey.Any(x=>x.LastValue != null))
            {
                s += $" WHERE { string.Join(" AND ", srcTable.PrimaryKey.Select(x => $"[{x.ColumnName}] > @C{x.ID}"))} ";
                parray = srcTable.PrimaryKey.Select(x => new KeyValuePair<string, object>($"@C{x.ID}", x.LastValue)).ToArray();
            }

            s += $" ORDER BY { string.Join(",", srcTable.PrimaryKey.Select(x => $"[{x.ColumnName}]")) }";

            return ReadAsync(s, parray);
        }

        public async override Task<bool> WriteToServerAsync(SqlTable table,SqlRowSet r)
        {
            bool copied = false;
            using (SqlBulkCopy bcp = new SqlBulkCopy(this.conn, SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.UseInternalTransaction,null)) {

                bcp.SqlRowsCopied += (s, e) => {
                    copied = true;
                };

                bcp.DestinationTableName = table.Name;
                
                await bcp.WriteToServerAsync(r.Reader);
            }
            return copied;
        }

        internal override Task WriteToServerAsync(SqlTable srcTable, IEnumerable<ChangedData> changes)
        {
            throw new NotImplementedException();
        }
    }
}
