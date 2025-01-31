import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os


def run_loading():
  # Load the datasets
  data = pd.read_csv(r'cleaneddata.csv')
  products = pd.read_csv(r'products.csv')
  customers = pd.read_csv(r'customers.csv')
  staff = pd.read_csv(r'staff.csv')
  transaction = pd.read_csv(r'transaction.csv')

    # Load the environment variables from the .env file
  load_dotenv()

  # Get the connection string for the Azure Blob Storage
  connection_string = os.getenv('AZURE_CONNECTION_STRING_VALUE')
  container_name = os.getenv('AZURE_CONTAINER_NAME')

  # Create a BlobServiceClient object
  blob_service_client = BlobServiceClient.from_connection_string(connection_string)
  container_client = blob_service_client.get_container_client(container_name)

  # Upload the data files to the Azure Blob Storage
  files = [
    (data, 'rawdata/cleaned_zipco_transaction_data.csv'),
    (products, 'cleaneddata/products.csv'),
    (customers, 'cleaneddata/customers.csv'),
    (staff, 'cleaneddata/staff.csv'),
    (transaction, 'cleaneddata/transaction.csv')

  ]

  for file , blob_name in files:
    blob_client = container_client.get_blob_client(blob_name)
    output = file.to_csv(index=False)
    blob_client.upload_blob(output, overwrite=True)
    print(f'{blob_name} loaded successfully to Azure Blob Storage')