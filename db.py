import io
import os
from urllib.parse import quote_plus

import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

# set up credentials
username = os.environ.get('USERNAME_AZURE')
password = os.environ.get('PASSWORD')
server = os.environ.get('SERVER')
database = os.environ.get('DATABASE')
account_storage = os.environ.get('ACCOUNT_STORAGE')
connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

# set up schema
TARGET_SCHEMA = (os.environ.get('SQL_SCHEMA') or 'dbo').strip()

# format table name
def sql_table(quoted_name: str):
    return f'[{TARGET_SCHEMA}].[{quoted_name}]'

# build ODBC connection string
def _azure_sql_odbc_connect():
    if not all([username, password, server, database]):
        raise ValueError(
            "Missing SQL environment variables."
        )
    host = server.strip()
    if not host.lower().startswith("tcp:"):
        host = f"tcp:{host},1433"
    odbc_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={host};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=60;"
    )
    return quote_plus(odbc_str)


# set up database connection
engine = create_engine("mssql+pyodbc:///?odbc_connect=" + _azure_sql_odbc_connect())

class AzureDB():
    def __init__(self, local_path = "./data", account_storage = account_storage):
        self.local_path = local_path
        self.account_url = f"https://{account_storage}.blob.core.windows.net"
        self.default_credential = DefaultAzureCredential()
        self.blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # access a specific container or create if not exist
    def access_container(self, container_name):
        try:
            # create container if not exist
            self.container_client = self.blob_service_client.create_container(container_name)
            print(f"Creating container {container_name}")
            self.container_name = container_name

        except Exception as e:
            print(f"Accessing container {container_name}")
            # access the container
            self.container_client = self.blob_service_client.get_container_client(container=container_name)
            self.container_name = container_name

    # delete a container
    def delete_container(self):
        print("Deleting container...")
        self.container_client.delete_container()
        print("Done")

    # upload a blob to Azure Storage
    def upload_blob(self, blob_name, blob_data = None):
        # create a file locally to upload as blob to Azure Storage
        local_file_name = blob_name
        upload_file_path = os.path.join(self.local_path, local_file_name)
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=local_file_name)
        print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)

        if blob_data is not None:
            blob_client.create_blob_from_text(container_name=self.container_name, blob_name=blob_name, text=blob_data)
        else:
            # upload the file
            with open(file=upload_file_path, mode="rb") as data:
                blob_client.upload_blob(data)

    # list blobs in the container
    def list_blobs(self):
        print("\nListing blobs...")
        blob_list = self.container_client.list_blobs()
        for blob in blob_list:
            print("\t" + blob.name)

    # download a blob from Azure Storage
    def download_blob(self, blob_name):
        download_file_path = os.path.join(self.local_path, blob_name)
        print("\nDownloading blob to \n\t" + download_file_path)
        with open(file=download_file_path, mode="wb") as download_file:
                download_file.write(self.container_client.download_blob(blob_name).readall())

    # delete a blob from Azure Storage
    def delete_blob(self, container_name: str, blob_name: str):
        print("\nDeleting blob " + blob_name)
        blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        blob_client.delete_blob()

    # read a csv blob from Azure Storage and return as DataFrame
    def access_blob_csv(self, blob_name):
        try:
            print(f"Acessing blob {blob_name}")
            df = pd.read_csv(io.StringIO(self.container_client.download_blob(blob_name).readall().decode('utf-8')))
            return df
        except Exception as ex:
            print('Exception:')
            print(ex)

    # upload a DataFrame to Azure SQL Database as a table
    def upload_dataframe_sqldatabase(self, blob_name, blob_data):
        print("\nUploading to Azure SQL server as table:\n\t" + blob_name)
        blob_data.to_sql(blob_name, engine, schema=TARGET_SCHEMA, if_exists='replace', index=False)
        primary = blob_name.replace('dim', 'id')
        qt = sql_table(blob_name)
        if 'fact' in blob_name.lower():
            with engine.connect() as con:
                trans = con.begin()
                con.execute(text(f'ALTER TABLE {qt} alter column {blob_name}_id bigint NOT NULL'))
                con.execute(text(f'ALTER TABLE {qt} ADD CONSTRAINT [PK_{blob_name}] PRIMARY KEY CLUSTERED ([{blob_name}_id] ASC);'))
                trans.commit()
        else:
            with engine.connect() as con:
                trans = con.begin()
                con.execute(text(f'ALTER TABLE {qt} alter column {primary} bigint NOT NULL'))
                con.execute(text(f'ALTER TABLE {qt} ADD CONSTRAINT [PK_{blob_name}] PRIMARY KEY CLUSTERED ([{primary}] ASC);'))
                trans.commit()

    # append a DataFrame to an existing table in Azure SQL Database
    def append_dataframe_sqldatabase(self, blob_name, blob_data):
        print("\nAppending to table:\n\t" + blob_name)
        blob_data.to_sql(blob_name, engine, schema=TARGET_SCHEMA, if_exists='append', index=False)

    # delete a table from Azure SQL Database
    def delete_sqldatabase(self, table_name):
        with engine.connect() as con:
            trans = con.begin()
            con.execute(text(f"DROP TABLE {sql_table(table_name)}"))
            trans.commit()

    # execute a SQL query and return the result as a list of dictionaries
    def get_sql_table(self, query):
        # Create connection and fetch data
        df = pd.read_sql_query(query, engine)

        # Convert DataFrame to the specified JSON format
        result = df.to_dict(orient='records')
        return result
