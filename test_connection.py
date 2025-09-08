import pyodbc, os
import sqlalchemy as sa
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

load_dotenv()

# Pyodbc connection test:
# connection_string = os.getenv("AZURE_CONN_STRING")

# conn = pyodbc.connect(connection_string)
# print("Connected to Azure SQL Database!")
# conn.close()


# SQLAlchemy connection test:

connection_string = os.getenv("AZURE_CONN_STRING")

storage_conn_string = os.getenv("AZURE_STORAGE_CONN_STRING")

# Create the BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(storage_conn_string)

# Create SQLAlchemy engine
engine = sa.create_engine(connection_string)

with engine.connect() as connection:
    print("Connected to Azure SQL Database!")

print("\nTesting connection to Azure Blob Storae:")
try:
    containers = blob_service_client.list_containers()
    for c in containers:
        print(f"Container found: {c['name']}")
    print("Successfully connected to Blob Storage")
except Exception as e:
    print("Failed to connect to Blob Storage:", e)

connection.close()