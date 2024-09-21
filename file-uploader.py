from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient
import os

connection_string = ''

# Define container name and blob name
container_name = 'stavros-sotiropoulos-container'
blob_name = 'test.txt'
file_path = './samples/test.txt'


# Initialize the BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Create a container if it doesn't exist
def create_container_if_not_exists(container_name):
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

# Main function
if __name__ == "__main__":
    # Create the container if it doesn't exist
    container_client = create_container_if_not_exists(container_name)
    
    if container_client != 0:
        # Upload the file
        upload_file_to_blob(container_client, blob_name, file_path)
    else:
        print("Error occurred!")