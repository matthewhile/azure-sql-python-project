import os
import pandas as pd 
import pyodbc
import sqlalchemy as sa
from sqlalchemy import types
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

print(f"Script started at {datetime.now()}")

# Set the directory
directory = r"C:\Users\MatthewHile\Desktop\Labs\imdb_to_azure\imdb_tsv_files"

# Pass in the connection string from .env

connection_string = os.getenv("AZURE_CONN_STRING")

# Create SQLAlchemy engine
engine = sa.create_engine(
    connection_string,
    connect_args={"fast_executemany": True}
)

with engine.connect() as connection:
    print("Connected to Azure SQL Database")

for file in os.listdir(directory):
    if file.endswith(".tsv"):

        # read tsv
        file_path = os.path.join(directory, file)

        # get the name for the staging table
        base_name = os.path.splitext(file)[0].replace(".", "_")
        stg_table_name = f"stg_{base_name}"
        print(f"\nProcessing file: {file} → Table: {stg_table_name}")

        df = pd.read_csv(file_path, sep="\t", low_memory=False)

        print(f"Loaded {len(df)} rows and {len(df.columns)} columns from {file}")
        print(f"Started processing {file} at {datetime.now()}")

        # define SQL types (all NVARCHAR for staging)
        dtype_map = {col: types.NVARCHAR(length=300) for col in df.columns}

        # write to SQL
        df.to_sql(
            name=stg_table_name,
            con=engine,
            schema="dbo",            
            if_exists="replace",     # drop/recreate staging table each run
            index=False,
            dtype=dtype_map,
            chunksize=40000,         # batch insert size
            # method="multi"         # send many rows per INSERT
        )

        print(f"Inserted {len(df)} rows into {stg_table_name}")
        print(f"Finished {file} at {datetime.now()}")

connection.close()

print("\n All files processed successfully.")