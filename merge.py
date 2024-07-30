from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd
import io

def merge_csv_to_gcs(event, context):
    # Initialize BigQuery and GCS clients with service account credentials
    credentials = service_account.Credentials.from_service_account_file(
        r"service_account.json",
        scopes=['https://www.googleapis.com/auth/cloud-platform', 'https://www.googleapis.com/auth/bigquery']
    )
    bq_client = bigquery.Client(credentials=credentials)
    storage_client = storage.Client(credentials=credentials)

    # Define the GCS bucket and folder containing CSV files
    bucket_name = "important"
    folder_name = "device"

    # List all CSV files in the GCS bucket
    blobs = storage_client.list_blobs(bucket_name, prefix=folder_name + "/")

    # Temporary DataFrame to store merged data
    merged_df_list = []

    # Load data from CSV files into DataFrame
    for i, blob in enumerate(blobs):
        if blob.name.endswith('.csv'):
            # Download CSV content
            csv_content = blob.download_as_string().decode('utf-8')

            # Read CSV content into DataFrame
            csv_buffer = io.StringIO(csv_content)
            df = pd.read_csv(csv_buffer)

            # Append DataFrame to merged_df_list
            merged_df_list.append(df)

            print(f"Data from {blob.name} loaded into DataFrame.")

    # Concatenate all DataFrames in merged_df_list into a single DataFrame
    merged_df = pd.concat(merged_df_list, ignore_index=True)

    # Convert merged DataFrame to CSV format
    merged_csv_content = merged_df.to_csv(index=False)

    # Upload merged CSV content to GCS as a single file
    merged_blob = storage_client.bucket(bucket_name).blob("Explore/merged_data.csv")
    merged_blob.upload_from_string(merged_csv_content)

    print("Merged CSV file uploaded to GCS successfully.")

# Call the function to execute
merge_csv_to_gcs(None, None)
