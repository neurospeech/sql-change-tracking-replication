UPDATE CT_REPLICATIONSTATE SET
	EndSync = GETUTCDATE(),
	LastSyncResult = ISNULL(@LastSyncResult,LastSyncResult),
	LastFullSync = ISNULL(@LastFullSync,LastFullSync),
	LastVersion = @LastVersion
WHERE 
	TableName = @TableName
