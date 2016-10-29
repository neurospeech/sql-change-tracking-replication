UPDATE CT_REPLICATIONSTATE SET
	EndSync = GETUTCDATE(),
	LastSyncResult = ISNULL(@LastSyncResult,LastSyncResult),
	LastFullSync = @LastFullSync,
	LastVersion = @LastVersion
WHERE 
	TableName = @TableName
