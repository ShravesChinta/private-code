from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(filename='blob_processing.log', level=logging.INFO, format='%(asctime)s - %(message)s')

def list_blobs_from_container(connect_str, container_name):
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)
    blob_list = container_client.list_blobs()
    return blob_list

def process_large_csv(blob_client):
    cutoff_date = datetime.now() - timedelta(days=2*365)
    total_rows_removed = 0
    total_rows_remaining = 0

    output_stream = io.StringIO()
    downloader = blob_client.download_blob()

    try:
        # Process the blob in chunks
        stream = io.BytesIO()
        downloader.readinto(stream)
        stream.seek(0)

        # Read the CSV in chunks
        for chunk_df in pd.read_csv(stream, chunksize=10000):
            if 'date' in chunk_df.columns:
                chunk_df['date'] = pd.to_datetime(chunk_df['date'], errors='coerce')
                old_data = chunk_df[chunk_df['date'] < cutoff_date]
                chunk_df = chunk_df[chunk_df['date'] >= cutoff_date]

                total_rows_removed += len(old_data)
                total_rows_remaining += len(chunk_df)

                chunk_df.to_csv(output_stream, mode='a', index=False, header=output_stream.tell() == 0)
            else:
                logging.warning(f"No 'date' column found in chunk of blob: {blob_client.blob_name}")
                continue

        # Log the information
        logging.info(f"Processed blob: {blob_client.blob_name}")
        logging.info(f"Number of rows removed: {total_rows_removed}")
        logging.info(f"Remaining rows: {total_rows_remaining}")

        # Upload the processed data back to the blob
        output_stream.seek(0)
        blob_client.upload_blob(output_stream.getvalue(), overwrite=True)
    except Exception as e:
        logging.error(f"Error processing blob {blob_client.blob_name}: {str(e)}")

def check_age_of_the_data(container_name, connect_str):
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    blob_list = list_blobs_from_container(connect_str, container_name)
    
    for blob in blob_list:
        blob_client = blob_service_client.get_blob_client(container_name, blob.name)
        process_large_csv(blob_client)

# Example usage
connect_str = 'your_connection_string'
container_name = 'your_container_name'
check_age_of_the_data(container_name, connect_str)
