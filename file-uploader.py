from azure.core.exceptions import ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
import os

connection_string = os.getenv("CONNECTION_STRING")

account_url = "https://dataengineerv1.blob.core.windows.net"
credential = DefaultAzureCredential()

# Define container name and blob name
container_name = "stavros-sotiropoulos-container"
blob_name = "test.txt"
file_path = "./samples/test.txt"


# Create a container if it doesn't exist
def create_container_if_not_exists(blob_service_client, container_name):
    container_client = blob_service_client.get_container_client(container_name)
    
    try:
        container_client.get_container_properties()
        print(f"Container '{container_name}' already exists.")
    except ResourceNotFoundError: 
        print(f"Container '{container_name}' does not exist. Creating it...")
        container_client.create_container()
        print(f"Container '{container_name}' created successfully.")
    except Exception as e:
        print(f"Another error occured {e}")
        return 0
    
    return container_client

# Upload the file
def upload_file_to_blob(container_client, blob_name, file_path):
    blob_client = container_client.get_blob_client(blob_name)
    
    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    
    print(f"File '{file_path}' uploaded successfully to container '{container_name}' as '{blob_name}'.")

def get_blob_client(connection_string, account_url, connection_type="constring"):
    
    # Initialize the BlobServiceClient
    if connection_string is None and connection_type == "constring":
        print("Provide connection string for default connection type or authenticate with credentials")
        print("Exiting..")
        return None
    
    blob_service_client = None

    if connection_type == "constring":
        print("Getting blob client by connection string")
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    else: 
        print("Getting blob client by default credentials")
        blob_service_client = BlobServiceClient(account_url, credential=credential)

    return blob_service_client

# Main function
if __name__ == "__main__":

    blob_service_client = get_blob_client(connection_string=connection_string, account_url=account_url, connection_type="constring")

    if blob_service_client is None:
        exit(1)
    # Create the container if it doesn't exist
    container_client = create_container_if_not_exists(blob_service_client, container_name)
    
    if container_client != 0:
        # Upload the file
        upload_file_to_blob(container_client, blob_name, file_path)
    else:
        print("Error occurred!")