import os
import pandas as pd 
import pyodbc
from dotenv import load_dotenv

load_dotenv()

# Set the directory
directory = r"C:\Users\MatthewHile\Desktop\Labs\imdb_to_azure\imdb_gz_files"

# Pass in the connection string from .env
connection_string = os.getenv("AZURE_CONN_STRING")
conn = pyodbc.connect(connection_string)

cursor = conn.cursor()

print("Connected successfully.")

for file in os.listdir(directory):
    if file.endswith(".tsv"):
        table_name = os.path.splitext(file)[0]
        print(f"\n Processing file: {file} → Table: {table_name}")

        # read tsv
        file_path = os.path.join(directory, file)
        df = pd.read_csv(file_path, sep="\t")
        print(f"   → Loaded {len(df)} rows and {len(df.columns)} columns from {file}")

        # infer schema by mapping data types
        col_defs = []
        for col, dtype in df.dtypes.items():
            if "int" in str(dtype):
                sql_type = "INT"
            elif "float" in str(dtype):
                sql_type = "FLOAT"
            elif "bool" in str(dtype):
                sql_type = "BIT"
            else:
                sql_type = "NVARCHAR(500)"
            col_defs.append(f"[{col}] {sql_type}")
            print(f"      Column: {col} → {sql_type}")


cursor.close()
conn.close()
print("\n All files processed successfully.")