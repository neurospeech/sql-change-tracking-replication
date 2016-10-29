using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlReplicator.Core
{
    public class SqlRowSet : IDisposable
    {

        DbDataReader reader = null;
        DbCommand command = null;

        public SqlRowSet(DbCommand command, DbDataReader reader)
        {
            this.reader = reader;
            this.command = command;

            
        }

        public DbDataReader Reader => this.reader;

        public async Task<bool> ReadAsync()
        {
            return await reader.ReadAsync();
        }


        public DataTable GetSchemaTable() {
            return reader.GetSchemaTable();
        }

        public DataTable GetTable() {
            
            DataTable dt = new DataTable();
            dt.Load(this.reader);
            //if (dt.Rows.Count>0)
            //    Debugger.Break();
            return dt;
        }

        public T GetValue<T>(string name)
        {
            int ordinal = reader.GetOrdinal(name);
            if (reader.IsDBNull(ordinal))
            {
                return default(T);
            }
            object val = reader.GetValue(ordinal);
            Type type = Nullable.GetUnderlyingType(typeof(T));
            if (type == null)
            {
                type = typeof(T);
            }
            if (val.GetType() != type)
                val = (T)Convert.ChangeType(val, type);
            return (T)val;
        }

        public void Dispose()
        {

            this.reader.Dispose();
            this.command.Dispose();
        }

        internal object GetRawValue(string name)
        {
            int ordinal = reader.GetOrdinal(name);
            if (reader.IsDBNull(ordinal))
            {
                return null;
            }
            return reader.GetValue(ordinal);
        }
    }


    public class ChangedData {

        public long LastVersion { get; set; }

        public List<DataField> ChangedValues { get; }
            = new List<DataField>();

        public List<DataField> PrimaryKeys { get; }
            = new List<DataField>();

        public ChangeOperation Operation { get; set; }

    }

    public struct DataField {

        public DataField(string columnName, string v1, object v2) : this()
        {
            this.FieldName = columnName;
            this.ParamName = v1;
            this.Value = v2;
        }

        public string FieldName { get; set; }
        public string ParamName { get; set; }
        public object Value { get; set; }
    }

    public enum ChangeOperation {
        Insert,
        Update,
        Delete
    }
}
