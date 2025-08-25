import pyodbc, os
from dotenv import load_dotenv

load_dotenv()

# connection_string = (
#     "Driver={ODBC Driver 18 for SQL Server};"
#     "Server=tcp:crablab.database.windows.net,1433;"
#     "Database=imdb_to_azureTest;"
#     "Uid=mattadmin;"
#     "Pwd=*uwtQvBJ{AGT8B[[I&Uh;"
#     "Encrypt=yes;"
#     "TrustServerCertificate=no;"
#     "Connection Timeout=30;"
# )

connection_string = os.getenv("AZURE_CONN_STRING")


conn = pyodbc.connect(connection_string)
print("Connected to Azure SQL Database!")
conn.close()

