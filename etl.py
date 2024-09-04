import requests
import urllib.request
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import gzip
from google.cloud import storage, bigquery

def download_and_combine(url, num_threads):
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
                
        combined_data.seek(0)
        return combined_data.getvalue()


def convert_ndjson_to_csv_gzip(data_bytes):
    # Chuyển đổi dữ liệu byte thành DataFrame
    df = pd.read_json(BytesIO(data_bytes), lines=True)

    # Chuyển DataFrame thành CSV trong bộ nhớ
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()

    # Nén dữ liệu CSV bằng gzip và lưu vào BytesIO
    gzip_buffer = BytesIO()
    with gzip.GzipFile(fileobj=gzip_buffer, mode='wb') as f_out:
        f_out.write(csv_data)
    
    # Trả về dữ liệu nén dưới dạng bytes
    return gzip_buffer.getvalue()

def upload_to_gcs(bucket_name, data_bytes, destination_blob_name):
    # Khởi tạo client GCS
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    data_buffer = BytesIO(data_bytes)
    data_content = data_buffer.getvalue()
    # Tải lên tệp Gzip
    blob.upload_from_string(data_content, content_type='application/gzip')

def import_to_bigquery(bucket_name, destination_blob_name, dataset_name, table_name):
    bigquery_client = bigquery.Client()
    uri = f"gs://{bucket_name}/{destination_blob_name}"

    # Cấu hình công việc tải lên
    job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,  # Bỏ qua dòng tiêu đề nếu có
    autodetect=True,  # Tự động phát hiện schema
    )

    # Tải lên BigQuery
    job = bigquery_client.load_table_from_uri(
    uri,
    f'{dataset_name}.{table_name}',
    location='asia-southeast1',  # Thay thế bằng vùng của bạn
    job_config=job_config
    )

def main_task(url, bucket_name, destination_blob_name, dataset_name, table_name):
    print("-----Download with multi-thread and Combine------")
    data_bytes = download_and_combine(url, 10)

    print("-------------------------------------------------")
    print("-----Convert NDJSON to CSV GZIP------")
    data_bytes_upload = convert_ndjson_to_csv_gzip(data_bytes)

    print("-------------------------------------")
    print("-----Upload to GCS-----")
    upload_to_gcs(bucket_name, data_bytes_upload, destination_blob_name)

    print("-------------------------------------")
    print("-----Import to bigquery-----")
    import_to_bigquery(bucket_name, destination_blob_name, dataset_name, table_name)

    print("-------------------------------------")
    print("-----Task Successfully------")

url = "https://raw.githubusercontent.com/AA583/ndjson_download/main/data.ndjson"
bucket_name = 'bucket-task-nexar'
destination_blob_name = 'data.csv.gz'
dataset_name = 'data_test_nexar'
table_name = 'data'
main_task(url, bucket_name, destination_blob_name, dataset_name, table_name)






