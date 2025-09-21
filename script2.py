import os
import csv
import sqlalchemy as sa
from sqlalchemy import types
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from datetime import datetime

load_dotenv()

print(f"Script started at {datetime.now()}")

# Set the directory
directory = r"C:\Users\MatthewHile\Desktop\Labs\imdb_to_azure\imdb_tsv_files"

# Pass in the connection strings from .env
sql_conn_sting = os.getenv("AZURE_CONN_STRING")
storage_conn_string = os.getenv("AZURE_STORAGE_CONN_STRING")

container_name = "stagetsv"

# Create the BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(storage_conn_string)
container_client = blob_service_client.get_container_client(container_name)

# Create SQLAlchemy engine
engine = sa.create_engine(sql_conn_sting)

with engine.begin() as connection:
    print("Connected to Azure SQL Database")

    for file in os.listdir(directory):
        if file.endswith(".tsv"):
            file_path = os.path.join(directory, file) # read tsv

            blob_client = container_client.get_blob_client(file) # create the blob object
            
            print(f"\nUploading {file} to Azure Blob Storage")
            with open(file_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True) # upload .tsv file and overwrite
            print(f"Uploaded {file} to Azure Blob Storage")

            # Get staging table name
            base_name = os.path.splitext(file)[0].replace(".", "_")
            stg_table_name = f"stg_{base_name}"

             # --- Read first row (header) from TSV ---
            with open(file_path, newline="", encoding="utf-8") as tsvfile:
                reader = csv.reader(tsvfile, delimiter="\t")
                headers = next(reader)
            
            col_defs = ",\n ".join([f"[{col}] NVARCHAR(MAX)" for col in headers])

            # Delete staging table if already exists
            connection.execute(sa.text(f"""
            IF OBJECT_ID('dbo.{stg_table_name}', 'U') IS NOT NULL
                DROP TABLE dbo.{stg_table_name};
            """))

            # Create staging table for each file
            connection.execute(sa.text(f"""
            CREATE TABLE dbo.{stg_table_name} (
                {col_defs}
            );
            """))
            print(f"\nCreated table {stg_table_name}")

            print(f"\nStarted bulk insert from {file} into {stg_table_name} at {datetime.now()}")
            connection.execute(sa.text(f"""
            BULK INSERT dbo.{stg_table_name}
            FROM '{file}'
            WITH (
                DATA_SOURCE = 'AzureBlobStorage',
                FIELDTERMINATOR = '\t',
                ROWTERMINATOR = '0x0a',
                FIRSTROW = 2
            );
            """))
            print(f"Finished bulk insert from {file} into {stg_table_name} at {datetime.now()}")

connection.close()

print("\nAll files processed successfully.")
print(f"Script completed at {datetime.now()}")