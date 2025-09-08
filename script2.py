import os
import pandas as pd 
import pyodbc
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
#container_client.create_container(exist_ok=True) # Ensures the blob container exists (creates it if missing).

# Create SQLAlchemy engine
engine = sa.create_engine(sql_conn_sting)

with engine.connect() as connection:
    print("Connected to Azure SQL Database")

for file in os.listdir(directory):
    if file.endswith(".tsv"):
        file_path = os.path.join(directory, file) # read tsv

        blob_client = container_client.get_blob_client(file)
        
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        print(f"\nUploaded {file} to Azure Blob Storage")


connection.close()

#print("\n All files processed successfully.")