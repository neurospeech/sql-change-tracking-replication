using System;
using System.ComponentModel;
using System.Xml.Serialization;
using System.Runtime.Serialization;
using System.Text;

using System.Collections.Generic;
using System.Reflection;
using System.Linq.Expressions;
using System.Web;
using System.Security.Cryptography;
using System.Data.SqlClient;
using System.Data.Common;

namespace SqlReplicator.Core
{
	public partial class Scripts{

				public static string SqlServerGetSchema = "SELECT  IC.TABLE_NAME as TableName, IC.ORDINAL_POSITION as Ordinal, IC.COLUMN_NAME as ColumnName,  IC.COLUMN_DEFAULT as ColumnDefault,  IC.IS_NULLABLE as IsNullable,  IC.DATA_TYPE as DataType, IC.CHARACTER_MAXIMUM_LENGTH as DataLength, IC.NUMERIC_PRECISION as NumericPrecision,  IC.NUMERIC_SCALE as NumericScale,  (SELECT 1 FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE as CCU WHERE  	CCU.COLUMN_NAME = IC.COLUMN_NAME AND  	CCU.TABLE_NAME = IC.TABLE_NAME AND EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS as TC WHERE 		TC.CONSTRAINT_NAME = CCU.CONSTRAINT_NAME AND 		TC.TABLE_NAME=IC.TABLE_NAME AND TC.CONSTRAINT_TYPE='PRIMARY KEY') ) as IsPrimaryKey  FROM INFORMATION_SCHEMA.COLUMNS AS IC  WHERE @TableName is null OR IC.TABLE_NAME = @TableName ORDER BY IC.TABLE_NAME;";
				public static string SqlServerGetColumns = "SELECT  	ST.object_id as TableID , 	ST.name as TableName, 	SC.column_id as ColumnID, 	SC.name as ColumnName from sys.tables as ST 	join sys.columns as SC on ST.object_id = SC.object_id";

	}
}
