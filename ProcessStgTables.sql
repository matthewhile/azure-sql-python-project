-- First step, loop through each staging table?
DECLARE @stgTableName SYSNAME;
DECLARE @colName SYSNAME;
DECLARE @sql NVARCHAR(MAX); 
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
    DECLARE column_cursor CURSOR FAST_FORWARD FOR
    SELECT c.name
    FROM sys.columns c
    JOIN sys.tables t ON c.object_id = t.object_id
    WHERE t.name = @stgTableName
    ORDER BY c.column_id;

    OPEN column_cursor;
    FETCH NEXT FROM column_cursor INTO @colName

    WHILE @@FETCH_STATUS = 0
    BEGIN   
        PRINT '--- Profiling column: ' + @colName;

        SET @sql = N'
            SELECT 
                SUM(CASE WHEN TRY_CAST([' + @colName + N'] AS INT) IS NULL 
                          AND [' + @colName + N'] IS NOT NULL 
                          AND [' + @colName + N'] NOT IN ('/N','') 
                          THEN 1 ELSE 0 END) AS NotInt,
                SUM(CASE WHEN TRY_CAST([' + @colName + N'] AS BIT) IS NULL 
                          AND [' + @colName + N'] IS NOT NULL 
                          AND [' + @colName + N'] NOT IN ('/N','') 
                          THEN 1 ELSE 0 END) AS NotBit,
                SUM(CASE WHEN TRY_CAST([' + @colName + N'] AS DECIMAL(38,10)) IS NULL 
                          AND [' + @colName + N'] IS NOT NULL 
                          AND [' + @colName + N'] NOT IN ('/N','') 
                          THEN 1 ELSE 0 END) AS NotDecimal,
                SUM(CASE WHEN TRY_CAST([' + @colName + N'] AS DATE) IS NULL 
                          AND [' + @colName + N'] IS NOT NULL 
                          AND [' + @colName + N'] NOT IN ('/N','') 
                          THEN 1 ELSE 0 END) AS NotDate
            FROM (
                SELECT TOP (1000) [' + @colName + N']
                FROM dbo.' + QUOTENAME(@stgTableName) + N'
            ) t;';

        EXEC sp_executesql @sql;

-- Create final table
    SET @sql = N'CREATE TABLE dbo.' + QUOTENAME(@finalTable) + ' (';

    SELECT @sql = @sql + QUOTENAME(c.name) + ' NVARCHAR(MAX),'
    FROM sys.columns c
    WHERE c.object_id = OBJECT_ID(@tableName);

    SET @sql = LEFT(@sql, LEN(@sql) - 1) + ');'; -- trim trailing comma
    EXEC sp_executesql @sql;

-- Insert into final table with NULLIF on every column
-- '\N' values becomes NULL during the insert 

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