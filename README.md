# Tổng Quan Pipeline

Dự án này bao gồm việc thiết lập một pipeline dữ liệu với các nhiệm vụ sau:

### Xác định các nhiệm vụ:
1. **Tải Dữ Liệu NDJSON**:  
   - Tải dữ liệu dưới dạng NDJSON từ một nguồn bên thứ ba. 
   - *Lưu ý*: Đối với bài kiểm tra, dữ liệu demo được cung cấp từ url:https://raw.githubusercontent.com/AA583/ndjson_download/main/data.ndjson.  
   - **Yêu cầu**: Sử dụng đa luồng (multi-threading) để thực hiện quá trình tải dữ liệu.

2. **Chuyển Đổi Dữ Liệu Sang CSV**:  
   - Chuyển đổi dữ liệu NDJSON đã tải thành định dạng CSV. 
   - Nén tệp CSV bằng gzip.

3. **Upload Lên Google Cloud Storage (GCS)**:  
   - Trước khi upload, tạo một bucket trong GCS để lưu trữ tệp đã nén.
   - Upload tệp CSV đã nén lên bucket GCS.

4. **Nhập Dữ Liệu Vào BigQuery**:  
   - Tạo một bảng trong BigQuery.
   - Import dữ liệu từ bucket GCS vào bảng BigQuery.

### Công Nghệ Sử Dụng:
- **Cloud Composer**: Công cụ điều phối để quản lý pipeline.
- **Google Cloud Storage (GCS)**: Dịch vụ lưu trữ cho các tệp CSV đã nén.
- **BigQuery**: Kho dữ liệu để nhập và truy vấn dữ liệu.
- **Data Fusion**: Để quản lý và giám sát pipeline dữ liệu.
