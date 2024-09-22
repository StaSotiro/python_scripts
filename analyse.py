# This script downloads the tourism_dataset.csv file from 'raw' blob container, 
# performs an analysis, combines the results and finally uploads it to the container 
# named stavros-sotiropoulos under Stavros-Sotiropoulos.csv
# Please ensure you provide a CONNECTION_STRING variable 

from io import StringIO
import os
import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient
import importlib

fileUploader = importlib.import_module("file-uploader")

account_url = "https://dataengineerv1.blob.core.windows.net"
credential = DefaultAzureCredential()

raw_container = "raw"
blob_file = "tourism_dataset.csv"
export_container = "stavros-sotiropoulos"
export_blob = "Stavros-Sotiropoulos.csv"
file_path = "./results/Stavros-Sotiropoulos.csv"

calc_country_category = True


def download_blob_to_file(blob_service_client: BlobServiceClient, container_name):
    
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_file)
    download_stream = blob_client.download_blob(encoding="utf8")

    tourism_df = pd.read_csv(StringIO(download_stream.readall()), low_memory=False)
    return tourism_df


if __name__ == "__main__":
    try:

        if os.getenv("CONNECTION_STRING") is None:
            print("Please provide CONNECTION_STRING env variable... \nExiting...")
            exit(1)
        
        print("Connecting... ")
        # Create the BlobServiceClient object
        blob_service_client = fileUploader.get_blob_client(connection_string="", account_url=account_url, connection_type="creds")

        print("Downloading blob... ")
        tourism_df = download_blob_to_file(blob_service_client, raw_container)

        print("Calculating... ")
        grouped_countries = tourism_df.groupby("Country").agg(CountryRating = ("Rating", "mean")).reset_index()

        top_categories = None
        top_countries_categories = None
        if calc_country_category:
            top_categories = tourism_df.groupby(["Country", "Category"]).agg(CategoryRating=("Rating","mean")).reset_index().sort_values(by=["Country", "CategoryRating"], ascending=[True, False])
            top_categories = top_categories.groupby("Country").head(3).reset_index(drop=True)

            print("Combining results... ")

            top_categories["Country"] = top_categories["Country"].astype("str")
            grouped_countries["Country"] = grouped_countries["Country"].astype("str")
            top_countries_categories = pd.merge(grouped_countries, top_categories, on="Country", how='inner')
        else:
            top_categories = tourism_df.groupby("Category")["Rating"].mean().reset_index().sort_values(by="Rating")
            top_countries_categories = top_categories

        print("Uploading file...")
        top_countries_categories.to_csv(file_path, sep=",", header=True, index=False)
        
        export_blob_service_client = fileUploader.get_blob_client(connection_string=os.getenv("CONNECTION_STRING"), account_url=account_url, connection_type="constring")
        export_container_client = fileUploader.create_container_if_not_exists(export_blob_service_client, export_container)

        if export_container_client != 0:
            fileUploader.upload_file_to_blob(export_container_client, export_blob, file_path)
        else:
            print("Error occurred when fetching container!")
    
    except Exception as e:
        print(f"An error occured {e}")