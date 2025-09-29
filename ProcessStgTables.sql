/*
This script loops through each staging table in dbo that begins with 'stg_'.
For each column, it attempts to infer the most appropriate SQL data type by running
TRY_CAST checks (INT, BIT, DECIMAL, DATE). If no type matches, it falls back to NVARCHAR(MAX).  

It then creates a corresponding final_* table with the inferred column types,
and inserts data from the staging table into the final table. During the insert,
any '/N' placeholder values are converted to NULL.
*/

DECLARE @stgTableName SYSNAME;
DECLARE @colName SYSNAME;
DECLARE @sql NVARCHAR(MAX); 
DECLARE @finalTable SYSNAME;
DECLARE @dataType NVARCHAR(100);
DECLARE @selectList NVARCHAR(MAX);

DECLARE table_cursor CURSOR FAST_FORWARD FOR
-- Get the names of all tables in the dbo schema that start with stg_.
SELECT t.name 
FROM sys.tables t 
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = 'dbo' AND t.name LIKE 'stg_%';

OPEN table_cursor;
FETCH NEXT FROM table_cursor INTO @stgTableName;

WHILE @@FETCH_STATUS = 0
BEGIN 
    PRINT 'Processing table: ' + @stgTableName;

    SET @finalTable = REPLACE(@stgTableName, 'stg_', 'final_');

    -- Drop final table if it exists
    SET @sql = N'DROP TABLE IF EXISTS dbo.' + QUOTENAME(@finalTable) + ';';
    EXEC sp_executesql @sql;

    -- Temporary storage for column definitions
    IF OBJECT_ID('tempdb..#Cols') IS NOT NULL DROP TABLE #Cols;
    
    CREATE TABLE #Cols (
        ColName SYSNAME,
        DataType NVARCHAR(100)
    );

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
        -- For the first 10,000 rows of each column, check if the data type is INT, BIT, DECIMAL, DATE or NVARCHAR
        SET @sql = N'
            SELECT 
                SUM(CASE WHEN TRY_CAST([' + @colName + N'] AS INT) IS NULL 
                          AND [' + @colName + N'] IS NOT NULL 
                          AND [' + @colName + N'] NOT IN (''\N'','''') 
                          THEN 1 ELSE 0 END) AS NotInt,
                SUM(CASE WHEN TRY_CAST([' + @colName + N'] AS BIT) IS NULL 
                          AND [' + @colName + N'] IS NOT NULL 
                          AND [' + @colName + N'] NOT IN (''\N'','''') 
                          THEN 1 ELSE 0 END) AS NotBit,
                SUM(CASE WHEN TRY_CAST([' + @colName + N'] AS DECIMAL(38,10)) IS NULL 
                          AND [' + @colName + N'] IS NOT NULL 
                          AND [' + @colName + N'] NOT IN (''\N'','''') 
                          THEN 1 ELSE 0 END) AS NotDecimal,
                SUM(CASE WHEN TRY_CAST([' + @colName + N'] AS DATE) IS NULL 
                          AND [' + @colName + N'] IS NOT NULL 
                          AND [' + @colName + N'] NOT IN (''\N'','''') 
                          THEN 1 ELSE 0 END) AS NotDate,
                MAX(LEN([' + @colName + N'])) AS MaxLen
            FROM (
                SELECT TOP (10000) [' + @colName + N'] 
                FROM dbo.' + QUOTENAME(@stgTableName) + N' 
            ) t;';

        -- Profile each column for specified data types
        CREATE TABLE #Profile (
            NotInt INT,
            NotBit INT,
            NotDecimal INT,
            NotDate INT,
            MaxLen INT
        );

        INSERT INTO #Profile
        EXEC sp_executesql @sql;

        -- Infer the data types 
        SELECT TOP 1
            @dataType = CASE
                WHEN NotInt = 0 THEN 'INT'
                WHEN NotBit = 0 THEN 'BIT'
                WHEN NotDecimal = 0 THEN 'DECIMAL(38,10)'
                WHEN NotDate = 0 THEN 'DATE'
                ELSE 'NVARCHAR(MAX)'
            END
        FROM #Profile;

        DROP TABLE #Profile;

        PRINT 'Inferred type for ' + @colName + ' = ' + @dataType;

        -- Save the column name and data type
        INSERT INTO #Cols (ColName, DataType)
        VALUES (QUOTENAME(@colName), @dataType);

        -- Fetch the next column in the table if one exists
        FETCH NEXT FROM column_cursor INTO @colName;
    END;

    CLOSE column_cursor;
    DEALLOCATE column_cursor;

    -- Create final table
    SET @sql = N'CREATE TABLE dbo.' + QUOTENAME(@finalTable) + ' (';
    SELECT @sql = @sql + STRING_AGG(ColName + ' ' + DataType, ', ')
    FROM #Cols;
    SET @sql = @sql + ');';

    EXEC sp_executesql @sql;
    PRINT '--- Created final table: ' + @finalTable;

    -- Build INSERT into final table using inferred types
    -- '\N' values become NULL during the insert
    SELECT @selectList = STRING_AGG(
        'CAST(NULLIF(' + ColName + ', ''\N'') AS ' + DataType + ')', ', '
    )
    FROM #Cols;

    IF @selectList IS NULL
        SET @selectList = ''; 

    SET @sql = N'INSERT INTO dbo.' + QUOTENAME(@finalTable) + ' SELECT ' 
            + @selectList
            + ' FROM dbo.' + QUOTENAME(@stgTableName) + ';';

    EXEC sp_executesql @sql;
    PRINT '--- Successfully inserted into final table: ' + @finalTable;

    -- Fetch the next staging table name if one exists
    FETCH NEXT FROM table_cursor INTO @stgTableName;
END;

CLOSE table_cursor;
DEALLOCATE table_cursor;
