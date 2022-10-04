from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import logging
from logging import INFO
import os
import sys
import typer

logging.basicConfig(format='[%(levelname)-5s][%(asctime)s][%(module)s:%(lineno)04d] : %(message)s',
                    level=INFO,
                    stream=sys.stderr)
logger: logging.Logger = logging

blobs = typer.Typer()

STORAGE_ACCOUNT = os.environ.get("REDDIT_STUFF_CONN_STR")
blob_service_client = BlobServiceClient.from_connection_string(STORAGE_ACCOUNT)

@blobs.command("containers")
def show_containers():
    """
    List all containers in a storage account
    """
    all_containers = blob_service_client.list_containers(include_metadata=True)
    for container in all_containers:
        print(f"Container: {container['name']}")

@blobs.command("download")
def blobs_download(filename: str, container_name: str):
    """
    Download a single file from an Azure blob container
    """
    try:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)
        path = f"./data/{filename}"

        with open(path, "wb") as download_path:
            logger.info(f"Downloading blob to: {path}")
            blob_data = blob_client.download_blob()
            download_path.write(blob_data.readall())
            logger.info(f"Downloaded {filename} successfully")
    except Exception as ex:
        print(f"Exception: \n{ex}")

@blobs.command("upload")
def blobs_upload(filename: str, container_name: str):
    """
    Upload a single file to an Azure blob container
    """
    try:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)
        path = f"./data/{filename}"

        with open(path, "rb") as data:
            logger.info(f"Uploading to Azure Storage as blob: {filename}")
            blob_client.upload_blob(data)
            logger.info(f"Uploaded {filename} successfully")
    except Exception as ex:
        print(f"Exception: \n{ex}")


if __name__ == "__main__":
    blobs()