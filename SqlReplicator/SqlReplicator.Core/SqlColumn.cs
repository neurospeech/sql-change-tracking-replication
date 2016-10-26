using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlReplicator.Core
{
    public class SqlColumn
    {

        public long ID { get; set; }

        public string TableName { get; set; }

        public int Ordinal { get; set; }

        public string ColumnName { get; set; }

        public string ColumnDefault { get; set; }

        public decimal? NumericScale { get; set; }

        public decimal? NumericPrecision { get; set; }


        public string DataType { get; set; }

        public int DataLength { get; set; }

        public bool IsNullable { get; set; }

        public Type CLRType { get; set; }

        public DbType DbType { get; set; }

        public bool IsPrimaryKey { get; set; }
        public string ParamName
        {
            get
            {
                return "@P" + ColumnName.Replace(" ", "_");
            }
        }

        public override int GetHashCode()
        {
            return $"{ColumnName}.{DbType}".GetHashCode();
        }

        public override bool Equals(object obj)
        {
            var dest = obj as SqlColumn;
            if (dest != null)
            {
                if (!DataType.Equals(dest.DataType, StringComparison.OrdinalIgnoreCase))
                    return false;

                if (DataLength != dest.DataLength)
                    return false;

                if (NumericPrecision != dest.NumericPrecision)
                    return false;
                if (NumericScale != NumericScale)
                    return false;

                return true;
            }
            return base.Equals(obj);
        }

    }
}
