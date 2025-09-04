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

# Create the BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(storage_conn_string)

# Create SQLAlchemy engine
engine = sa.create_engine(sql_conn_sting)

with engine.connect() as connection:
    print("Connected to Azure SQL Database")



connection.close()

#print("\n All files processed successfully.")