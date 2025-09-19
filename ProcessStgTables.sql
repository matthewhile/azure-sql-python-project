-- First step, loop through each staging table?
DECLARE @stgTableName SYSNAME;
DECLARE @sql NVARCHAR; 
DECLARE @finalTable SYSNAME;

DECLARE table_cursor CURSOR FAST_FORWARD FOR
-- get the names of all tables in the dbo schema whose names start with stg_.
-- sys.tables & sys.schemas are built in catalog views
SELECT t.name 
FROM sys.tables t 
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = 'dbo' AND t.name LIKE 'stg_%';

OPEN table_cursor;
FETCH NEXT FROM table_curosr INTO @stgTableName;

WHILE @@FETCH_STATUS = 0
BEGIN 
    PRINT 'Processing table: ' + @stgTableName;

    SET @finalTable = REPLACE(@stgTableName, 'stg_', 'final_');

    -- Drop final table if it exists
    SET @sql = N'DROP TABLE IF EXISTS dbo.' + QUOTENAME(@finalTable) + ';';
    EXEC sp_executesql @sql;


-- Infer correct data types

-- Create final table
    SET @sql = N'CREATE TABLE dbo.' + QUOTENAME(@finalTable) + ' (';

    SELECT @sql = @sql + QUOTENAME(c.name) + ' NVARCHAR(300),'
    FROM sys.columns c
    WHERE c.object_id = OBJECT_ID(@tableName);

    SET @sql = LEFT(@sql, LEN(@sql) - 1) + ');'; -- trim trailing comma
    EXEC sp_executesql @sql;

-- Insert into final table with NULLIF on every column
-- '\N' becomes NULL during the insert 

    SET @sql = N'INSERT INTO dbo.' + QUOTENAME(@finalTable) + ' SELECT ';

    SELECT @sql = @sql +
        'NULLIF(' + QUOTENAME(c.name) + ', ''\N''),'
    FROM sys.columns c 
    WHERE c.object_id = OBJECT_ID(@stgTableName);

     SET @sql = LEFT(@sql, LEN(@sql) - 1) -- remove trailing comma
         + ' FROM dbo.' + QUOTENAME(@stgTableName) + ';';

    EXEC sp_executesql @sql;

    FETCH NEXT FROM table_cursor INTO @stgTableName;

END