# TSV to Azure SQL ETL Pipeline

This project implements an ETL pipeline that ingests IMDB datasets located in tab-separated value (TSV) files and loads them into an Azure SQL Database.


## Data Flow
1. Raw `.TSV` files are read from a local directory.
2. Each file is uploaded to a dedicated Azure Blob container.
3. Table schemas are generated in the target Azure SQL Database.
4. Data is `BULK INSERT`ed into staging tables (`NVARCHAR`).
5. `ProcessStgTables.sql` infers proper column types using `TRY_CAST`.
6. Final tables (`final_*`) are created with inferred datatypes.
7. Data is inserted into final tables with `NULL` handling.

## Notes
Because raw TSV files do not carry explicit column type information, it was necessary to come up with a way to infer appropriate SQL data types based on the contents of each column. This ensured that numeric, date, and text fields were accurately represented in the database. Another challenge was the presence of \N placeholders in the TSV files, which IMDB uses to represent missing values. I converted these into proper NULL values during the transfer process to maintain data integrity. Lastly, because some of the TSV files ranged from 1-4 GBs in size, I had to change approaches a couple times to decrease execution time.

Python Memory / Throughput Bottlenecks:
-	My first attempt involved using Pandas + SQLAlchemy to insert the data from the .TSV file into the staging table. 
-	With this approach, Python had to load all file data into memory when inserting into staging tables, resulting in a noticeable performance bottleneck when working with larger files 1-4GBs +. 
-	By using Blob Storage instead, there was a noticeable speedup in execution time.

Azure Blob Storage Config:
-	My target Azure SQL Database itself has no awareness of my Azure Storage account, so an external data source is needed to tell SQL the URL of my blob storage and how to authenticate.
-	This led me to create a “User-Assigned Managed Identity” to allow the DB to talk to blob storage, avoiding the need to generate and rotate SAS tokens manually.
-	I created the User-Assigned Managed Identity (sql-db-mi) and assigned it to my Storage Account via Access Control (IAM) with the role of "Storage Blob Data Reader".


## Future Improvements
- Explore using batch inserts, parallel processing, or external ETL tools like Azure Data Factory to increase throughput.
- Use clustered indexes to ensure data is inserted into the final tables in the same order it exists in the source `.TSV` files. 
- Additional candidate data types can be added to the inference logic step to improve accuracy.
- Allow for configurable sampling (first 10,000 rows vs. full table scan) to tradeoff between speed vs. accuracy during the inference step.

