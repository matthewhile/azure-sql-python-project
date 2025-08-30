import pyodbc, os
import sqlalchemy as sa
from dotenv import load_dotenv

load_dotenv()

# Pyodbc connection test:
# connection_string = os.getenv("AZURE_CONN_STRING")

# conn = pyodbc.connect(connection_string)
# print("Connected to Azure SQL Database!")
# conn.close()


# SQLAlchemy connection test:

connection_string = os.getenv("AZURE_CONN_STRING")

# Create SQLAlchemy engine
engine = sa.create_engine(connection_string)

with engine.connect() as connection:
    print("Connected to Azure SQL Database!")

connection.close()