# Tổng Quan Pipeline

Dự án này bao gồm việc thiết lập một pipeline dữ liệu với các nhiệm vụ sau:

### Công Nghệ Sử Dụng:
- **Cloud Composer**: Công cụ điều phối để quản lý pipeline.
- **Google Cloud Storage (GCS)**: Dịch vụ lưu trữ cho các tệp CSV đã nén.
- **BigQuery**: Kho dữ liệu để nhập và truy vấn dữ liệu.
- **Data Fusion**: Để quản lý và giám sát pipeline dữ liệu.

### Xác định các nhiệm vụ:
**Xem các thư viện trong file etl.py**

1. **Tải Dữ Liệu NDJSON**:  
   - Tải dữ liệu dưới dạng NDJSON từ một nguồn bên thứ ba. 
   - *Lưu ý*: Đối với bài kiểm tra, dữ liệu demo được cung cấp từ url:https://raw.githubusercontent.com/AA583/ndjson_download/main/data.ndjson.  
   - **Yêu cầu**: Sử dụng đa luồng (multi-threading) để thực hiện quá trình tải dữ liệu.
   - **Ý tưởng**: 
      1. Xác định dung lượng file phân tách trong quá trình tải thông qua kích thước file ndjson gốc và số lượng thread lựa chọn
      2. Sử dụng ThreadPoolExecutor thực hiện quá trình tải các file phân tách
      3. Sau khi tải xong, kết hợp các file phân tách thành một file ndjson.
      ```py
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
      ```

2. **Chuyển Đổi Dữ Liệu Sang CSV**:  
   - Chuyển đổi dữ liệu NDJSON đã tải thành định dạng CSV. 
   - Nén tệp CSV bằng gzip.

3. **Upload Lên Google Cloud Storage (GCS)**:  
   - Trước khi upload, tạo một bucket trong GCS để lưu trữ tệp đã nén.
   - Upload tệp CSV đã nén lên bucket GCS.

4. **Nhập Dữ Liệu Vào BigQuery**:  
   - Tạo một bảng trong BigQuery.
   - Import dữ liệu từ bucket GCS vào bảng BigQuery.

