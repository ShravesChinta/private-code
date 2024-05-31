from azure.storage.blob import BlobServiceClient, StorageStreamDownloader
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
    # Define the cutoff date
    cutoff_date = datetime.now() - timedelta(days=2*365)

    # Initialize variables for logging
    total_rows_removed = 0
    total_rows_remaining = 0

    # Create a temporary blob for processed data
    temp_blob_client = blob_client.get_container_client().get_blob_client(blob_client.blob_name + '.temp')

    # Initialize stream for writing processed chunks
    output_stream = io.StringIO()

    # Download and process the blob in chunks
    downloader = blob_client.download_blob()
    stream = io.BytesIO()
    
    for chunk in downloader.chunks():
        stream.write(chunk)

        # Load chunk into a pandas DataFrame
        stream.seek(0)
        chunk_df = pd.read_csv(stream, chunksize=10000)

        for df_chunk in chunk_df:
            if 'date' in df_chunk.columns:
                df_chunk['date'] = pd.to_datetime(df_chunk['date'], errors='coerce')
                old_data = df_chunk[df_chunk['date'] < cutoff_date]
                df_chunk = df_chunk[df_chunk['date'] >= cutoff_date]

                total_rows_removed += len(old_data)
                total_rows_remaining += len(df_chunk)

                # Append filtered chunk to the output stream
                df_chunk.to_csv(output_stream, mode='a', index=False, header=output_stream.tell() == 0)
            else:
                logging.warning(f"No 'date' column found in the chunk")
                break

        # Clear stream for next chunk
        stream.truncate(0)
        stream.seek(0)

    # Log the information
    logging.info(f"Processed blob: {blob_client.blob_name}")
    logging.info(f"Number of rows removed: {total_rows_removed}")
    logging.info(f"Remaining rows: {total_rows_remaining}")

    # Upload the processed data back to the blob
    output_stream.seek(0)
    temp_blob_client.upload_blob(output_stream.getvalue(), overwrite=True)

    # Replace the original blob with the processed one
    temp_blob_client.rename_blob(blob_client.blob_name)

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
