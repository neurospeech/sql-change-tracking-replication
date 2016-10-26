SELECT 
	ST.object_id as TableID ,
	ST.name as TableName,
	SC.column_id as ColumnID,
	SC.name as ColumnName
from sys.tables as ST
	join sys.columns as SC on ST.object_id = SC.object_id