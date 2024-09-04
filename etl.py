import requests
import urllib.request
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import gzip
from google.cloud import storage

def download_and_combine(url, num_threads, output):
    with requests.get(url, stream=True) as response:
        if response.status_code != 200:
            raise Exception(f"Failed to download file: {response.status_code}")

        site = urllib.request.urlopen(url)
        meta = site.info()
        total_size = int(meta["Content-Length"])

        chunk_size = total_size // num_threads if total_size else 1024 * 1024 

        combined_data = BytesIO()

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = []
            for chunk_num, chunk in enumerate(response.iter_content(chunk_size), start=1):
                futures.append(executor.submit(combined_data.write, chunk))

            for future in futures:
                future.result()

    with open(output, 'wb') as f:
        f.write(combined_data.getvalue())


def convert_ndjson_to_csv_gzip(input_file, output_file):
  df = pd.read_json(input_file, lines=True)

  with gzip.open(output_file, 'wt', encoding='utf-8') as f:
    df.to_csv(f, index=False)

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)

def main_task(url,output,bucket_name):
    print("-----Download with multi-thread and Combine------")
    download_and_combine(url, 10, output)

    output_file_csv_gzip = output.replace('.ndjson', '.csv.gz')
    print("-------------------------------------------------")
    print("-----Convert NDJSON to CSV GZIP------")
    convert_ndjson_to_csv_gzip(output, output_file_csv_gzip)

    print("-------------------------------------")
    print("-----Upload to GCS-----")
    upload_to_gcs(bucket_name, output_file_csv_gzip, output_file_csv_gzip)

    print("-------------------------------------")
    print("-----Task Successfully------")

url = "https://raw.githubusercontent.com/AA583/ndjson_download/main/data.ndjson"
output = "data.ndjson"
bucket_name = 'bucket-task-nexar'
main_task(url, output, bucket_name)






