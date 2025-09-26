-- First step, loop through each staging table?
DECLARE @stgTableName SYSNAME;
DECLARE @colName SYSNAME;
DECLARE @sql NVARCHAR(MAX); 
DECLARE @finalTable SYSNAME;
DECLARE @DataType NVARCHAR(100);

DECLARE @selectList NVARCHAR(MAX);

DECLARE table_cursor CURSOR FAST_FORWARD FOR
-- get the names of all tables in the dbo schema whose names start with stg_.
-- sys.tables & sys.schemas are built in catalog views
SELECT t.name 
FROM sys.tables t 
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = 'dbo' AND t.name LIKE 'stg_%';
-- WHERE s.name = 'dbo' AND t.name = 'stg_title_ratings'

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
    CREATE TABLE #Cols (ColDef NVARCHAR(MAX));

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

        CREATE TABLE #Profile (
            NotInt INT,
            NotBit INT,
            NotDecimal INT,
            NotDate INT,
            MaxLen INT
        );

        INSERT INTO #Profile
        EXEC sp_executesql @sql;

        -- Infer the data type
        SELECT TOP 1
            @DataType = CASE
                WHEN NotInt = 0 THEN 'INT'
                WHEN NotBit = 0 THEN 'BIT'
                WHEN NotDecimal = 0 THEN 'DECIMAL(38,10)'
                WHEN NotDate = 0 THEN 'DATE'
                ELSE 'NVARCHAR(MAX)'
            END
        FROM #Profile;

        DROP TABLE #Profile;

        PRINT 'Inferred type for ' + @colName + ' = ' + @DataType;

        -- Save the column definition
        INSERT INTO #Cols (ColDef)
        VALUES (QUOTENAME(@colName) + ' ' + @DataType);

        FETCH NEXT FROM column_cursor INTO @colName;
    END;

    CLOSE column_cursor;
    DEALLOCATE column_cursor;

-- Create final table
    SET @sql = N'CREATE TABLE dbo.' + QUOTENAME(@finalTable) + ' (';

    SELECT @sql = @sql + STRING_AGG(ColDef, ', ')
    FROM #Cols;

    SET @sql = @sql + ');';

    EXEC sp_executesql @sql;
    PRINT '--- Created final table: ' + @finalTable;

-- Insert into final table with NULLIF on every column
-- '\N' values becomes NULL during the insert 

-- Build INSERT into final table using inferred types
    SELECT @selectList = (
        SELECT STRING_AGG(
            'CAST(NULLIF('
                + LEFT(ColDef, CHARINDEX(' ', ColDef) - 1)          -- [ColumnName]
                + ', ''\N'') AS '
                + LTRIM(SUBSTRING(ColDef, CHARINDEX(' ', ColDef) + 1, 8000)) -- DataType
                + ')'
            , ', ')
        FROM #Cols
        WHERE CHARINDEX(' ', ColDef) > 0
    );

    IF @selectList IS NULL
        SET @selectList = ''; 

    SET @sql = N'INSERT INTO dbo.' + QUOTENAME(@finalTable) + ' SELECT ' 
            + @selectList
            + ' FROM dbo.' + QUOTENAME(@stgTableName) + ';';

    -- PRINT '--- Insert SQL:';
    -- PRINT @sql; 
    EXEC sp_executesql @sql;
    PRINT '--- Successfully inserted into final table: ' + @finalTable;

    FETCH NEXT FROM table_cursor INTO @stgTableName;
END;

CLOSE table_cursor;
DEALLOCATE table_cursor;
