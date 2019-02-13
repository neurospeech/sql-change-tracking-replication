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

        public long TableID { get; set; }

        public string TableName { get; set; }

        public int Ordinal { get; set; }

        public string ColumnName { get; set; }

        public string ColumnDefault { get; set; }

        public decimal? NumericScale { get; set; }

        public decimal? NumericPrecision { get; set; }


        public string DataType { get; set; }

        public long DataLength { get; set; }

        public bool IsNullable { get; set; }

        public Type CLRType { get; set; }

        public virtual DbType DbType {
            get {

                switch(DataType.ToLower())
                {
                    case "nvarchar":
                    case "varchar":
                        if (DataLength>0)
                        {
                            return DbType.StringFixedLength;
                        }
                        return DbType.String;
                    case "ntext":
                    case "text":
                    case "longtext":
                    case "mediumtext":
                        return DbType.String;
                    case "bit":
                    case "boolean":
                    case "tinyint":
                        return DbType.Boolean;
                    case "date":
                    case "datetime":
                    case "datetime2":
                    case "datetimeoffset":
                        return DbType.DateTime;
                    case "time":
                        return DbType.Time;
                    case "real":
                    case "float":
                    case "double":
                        return DbType.Double;
                    case "decimal":
                        return DbType.Decimal;
                    case "int":
                        return DbType.Int32;
                    case "bigint":
                        return DbType.Int64;
                    case "uniqueidentifier":
                    case "guid":
                        return DbType.Guid;
                    case "binary":
                        if (this.DataLength == 16 && 
                            (this.ColumnDefault?.ToLower()?.Contains("uuid") ?? false))
                        {
                            return DbType.Guid;
                        }
                        return DbType.Binary;
                    case "geometry":
                    case "geography":
                        return DbType.Object;

                    
                }
                return DbType.Binary;
            }
        }

        public bool IsPrimaryKey { get; set; }

        public bool IsIdentity { get; set; }

        public object LastValue { get; set; }

        public virtual string ParamName
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
                //if (!DataType.Equals(dest.DataType, StringComparison.OrdinalIgnoreCase))
                //    return false;

                if (this.DbType != dest.DbType)
                {
                    return false;
                }

                if (this.DbType == DbType.Boolean)
                    return true;

                if (this.DbType == DbType.Guid)
                {
                    return true;
                }

                if (this.DbType == DbType.String || this.DbType == DbType.StringFixedLength)
                {
                    if (this.DataLength == -1 || dest.DataLength == -1)
                        return true;
                    if (this.DataLength >= int.MaxValue || dest.DataLength >= int.MaxValue)
                    {
                        return true;
                    }
                }

                if (DataLength != dest.DataLength)
                    return false;

                if (NumericPrecision != dest.NumericPrecision)
                    return false;
                if (NumericScale != dest.NumericScale)
                    return false;
                if (IsNullable != dest.IsNullable)
                    return false;

                //if (ColumnDefault != dest.ColumnDefault)
                //    return false;

                return true;
            }
            return base.Equals(obj);
        }

    }
}
