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

        }

        public override void Dispose()
        {
            conn?.Dispose();
            conn = null;
        }

        public override DbCommand CreateCommand(String command, Dictionary<string, object> plist = null)
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

            using (var reader = await ReadAsync(sqlColumns, new Dictionary<string, object> {
                { "@TableName", tableName }
            }))
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

            string createTable = $"IF NOT EXISTS (SELECT * FROM sysobjects WHERE Name='{name}' AND xtype='U')"
                + $" CREATE TABLE {name} ({ string.Join(",", columns.Where(x => x.IsPrimaryKey).Select(c => ToColumn(c))) })";

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

                using (var cmd = CreateCommand($"EXEC sp_rename '{name}.{dest.ColumnName}', '{dest.ColumnName}_{m}'"))
                {
                    await cmd.ExecuteNonQueryAsync();
                }

            }

            foreach (var column in columnsToAdd)
            {
                using (var cmd = CreateCommand($"ALTER TABLE {name} ADD " + ToColumn(column)))
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

                using (var reader = await ReadAsync($"select * from sys.change_tracking_tables where object_id={table.ID}")) {

                    changeTrackingEnabled = await reader.ReadAsync();
                }

                if (!changeTrackingEnabled) {
                    await ExecuteAsync($"ALTER TABLE [{table.Name}] ENABLE CHANGE_TRACKING");
                }

                using (var reader = await ReadAsync($"SELECT MAX(EXT.SYS_CHANGE_VERSION) as Value FROM CHANGETABLE(CHANGES [{table.Name}],0) AS EXT")) {
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

        private static string[] textTypes = new[] { "nvarchar", "varchar" };
        
        public SqlDatabase Database { get; private set; }

        private static bool IsText(string n) => textTypes.Any(a => a.Equals(n, StringComparison.OrdinalIgnoreCase));

        private static bool IsDecimal(string n) => n.Equals("decimal", StringComparison.OrdinalIgnoreCase);

        private string ToColumn(SqlColumn c)
        {
            var name = $"{c.ColumnName} {c.DataType}";
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
                name += " NULL ";
            }
            else
            {
                name += " PRIMARY KEY ";
            }
            return name;
        }
    }
}
