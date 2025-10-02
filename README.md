# TSV to Azure SQL ETL Pipeline

The goal of this project was to create an ETL pipeline that takes large IMDB datasets, located in tab-separated value (TSV) files, and load them into an Azure SQL Database. The main entry point for running the pipeline is [`script2.py`](./script2.py).

IMDB datasets: https://developer.imdb.com/non-commercial-datasets/

<img width="760" height="296" alt="image" src="https://github.com/user-attachments/assets/3b975b72-e693-4318-acec-f7f055f37e6a" />

## Tech Stack
- Python 3
- T-SQL
- Azure SQL Database
- Azure Blob Storage

## Data Flow
1. Raw `.TSV` files are read from a local directory.
2. Each file is uploaded to a dedicated Azure Blob container.
3. Table schemas are generated in the target Azure SQL Database.
4. Data is `BULK INSERT`ed into staging tables (`NVARCHAR`).
5. `ProcessStgTables.sql` infers proper column types using `TRY_CAST`.
6. Final tables (`final_*`) are created with inferred datatypes.
7. Data is inserted into final tables with `NULL` handling.

## Notes
Data type inferencing: 
-	Because raw TSV files do not carry explicit column type information, it was necessary to come up with a way to infer appropriate SQL data types based on the contents of each column.
-	To accomplish this, I used T-SQL TRY_CAST to attempt to cast each column of each dataset into specified data types and see which one succeeded. If none succeeded, the column would be NVARCHAR.
-	This ensured that numeric, date, and text fields were accurately represented in the database.

Conversion of ‘\N’ values:
-	IMDB uses \N to represent missing values in the TSV files, but SQL can’t translate these values to NULL or empty strings by default. The presence of these values also made accurately inferring data types more complicated, for example \N in a column of integers causing the column to be misidentified as NVARCHAR instead of INT.
-	To solve this, I filtered out \N during the TRY_CAST statements and then used CAST(NULLIF()) to convert each instance of \N to NULL in the SQL tables.

Python Memory / Throughput Bottlenecks:
-	My first attempt [`script1.py`](./script1.py) involved using Pandas + SQLAlchemy to insert the data from the .TSV file into the staging table. 
-	With this approach, Python had to load all file data into memory when inserting into staging tables, resulting in a noticeable performance bottleneck when working with larger files 1-4GBs +. 
-	By using Blob Storage in [`script2.py`](./script2.py), there was a noticeable speedup in execution time.

Azure Blob Storage Config:
-	My target Azure SQL Database itself has no awareness of my Azure Storage account, so an external data source is needed to tell SQL the URL of my blob storage and how to authenticate.
-	This led me to create a “User-Assigned Managed Identity” to allow the DB to talk to blob storage, avoiding the need to generate and rotate SAS tokens manually.
-	I created the User-Assigned Managed Identity (sql-db-mi) and assigned it to my Storage Account via Access Control (IAM) with the role of "Storage Blob Data Reader".


## Future Improvements
- Explore using batch inserts, parallel processing, or external ETL tools like Azure Data Factory to increase throughput.
- Use clustered indexes to ensure data is inserted into the final tables in the same order it exists in the source `.TSV` files. 
- Additional candidate data types can be added to the inference logic step to improve accuracy.
- Allow for configurable sampling (first 10,000 rows vs. full table scan) to tradeoff between speed vs. accuracy during the inference step.

