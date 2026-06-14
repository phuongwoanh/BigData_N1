## ĐỀ TÀI: ỨNG DỤNG APACHE SPARK ĐỂ PHÂN TÍCH HÀNH VI NGƯỜI DÙNG VÀ DỰ ĐOÁN KHẢ NĂNG MUA HÀNG TRÊN NỀN TẢNG THƯƠNG MẠI ĐIỆN TỬ

Dự án tập trung xây dựng hệ thống phân tích dữ liệu lớn cho thương mại điện tử trên nền tảng Apache Spark và HDFS, bao gồm các bước làm sạch dữ liệu, phân tích SQL, xây dựng mô hình học máy để dự đoán hành vi mua hàng và phân cụm khách hàng.

### Tổng quan kiến trúc & Công nghệ

- **Ngôn ngữ lập trình:** Python
- **Xử lý phân tán:** Apache Spark (PySpark), Spark SQL, Spark ML
- **Lưu trữ dữ liệu:** Hadoop Distributed File System (HDFS)
- **Môi trường phân tích:** Jupyter Notebook
- **Thư viện phụ trợ:** Pandas, Numpy, Matplotlib, Scikit-learn

Dự án được kết nối và chạy trên Spark Master cluster (`spark://26.37.93.102:7077`) và HDFS (`hdfs://master:9000`).

---

### Cấu trúc dự án

Dự án được chia thành 4 giai đoạn chính, tương ứng với 4 file mã nguồn:

#### 1. `1_data_preparation.py` - Tiền xử lý và làm sạch dữ liệu
- Đọc dữ liệu dạng parquet từ HDFS.
- Xử lý các giá trị trùng lặp.
- Xử lý giá trị ngoại lệ của giá sản phẩm bằng các phương pháp thống kê.
- Điền khuyết thông minh: sử dụng giá trị trung vị của cùng loại sản phẩm, hoặc điền UUID cho session trống.
- Chuẩn hóa các giá trị chuỗi rỗng/NULL và lưu trữ tập dữ liệu sạch trở lại HDFS.

#### 2. `2_sql_analysis.ipynb` - Phân tích kinh doanh bằng Spark SQL
Thực hiện truy vấn SQL trên tập dữ liệu để trả lời 10 bài toán kinh doanh cốt lõi:
1. Phân tích phễu chuyển đổi (Conversion Funnel).
2. Tìm cơ hội bán chéo (Cross-selling).
3. Phân khúc khách hàng dựa trên giá trị cốt lõi (RFM/Core Value).
4. Phân tích xu hướng tăng trưởng bằng trung bình động (Moving Average).
5. Top 3 thương hiệu chủ lực theo ngành hàng.
6. Đo lường thời gian ra quyết định và chu kỳ mua sắm.
7. Phân tích giỏ hàng bị bỏ rơi (Abandoned Cart).
8. Định vị giá, phân khúc thương hiệu & độ nhạy giá.
9. Phân tích hành trình khách hàng & hành vi bất thường.
10. Bản đồ nhiệt (Heatmap) và chênh lệch doanh thu theo giờ.

#### 3. `3_machinelearning_1.ipynb` - Dự đoán hành vi mua hàng
- Sử dụng PySpark ML để xây dựng ML Pipeline.
- **Mục tiêu:** Dự đoán liệu một người dùng có thực hiện hành vi mua hàng hay không dựa trên các đặc trưng hành vi và session.
- **Mô hình sử dụng:** Logistic Regression, Random Forest, và Gradient Boosted Trees (GBTClassifier).
- **Đánh giá mô hình:** So sánh các chỉ số hiệu suất, độ chính xác, ROC-AUC, F1-score và in ra các biểu đồ trực quan hóa.

#### 4. `4_machinelearning_2.ipynb` - Phân cụm khách hàng
- Ứng dụng thuật toán học máy không giám sát để phân nhóm khách hàng.
- **Mục tiêu:** Tìm ra các tập khách hàng có chung đặc điểm hành vi để phục vụ chiến dịch Marketing cá nhân hóa.
- **Mô hình sử dụng:** K-Means Clustering kết hợp với PCA để giảm chiều dữ liệu và trực quan hóa các cụm.

---

### Hướng dẫn chạy dự án

1. **Khởi động Hadoop/Spark Cluster:**
   Đảm bảo HDFS và Spark Master đang chạy ổn định.
2. **Tiền xử lý:**
   ```bash
   spark-submit 1_data_preparation.py
   ```
3. **SQL:**
   Mở và chạy toàn bộ các cell trong `2_sql_analysis.ipynb`.
4. **Học máy (Machine Learning):**
   Tiếp tục chạy tuần tự `3_machinelearning_1.ipynb` và `4_machinelearning_2.ipynb` để huấn luyện, đánh giá mô hình và trực quan hóa kết quả.

---

*Lưu ý: Môi trường chạy yêu cầu cấu hình đủ bộ nhớ (RAM/Cores) để xử lý tập dữ liệu lớn với Spark. Các tham số bộ nhớ như `spark.executor.memory` hay `spark.driver.memory` đã được cấu hình sẵn trong mã nguồn nhưng có thể tinh chỉnh lại cho phù hợp với phần cứng thực tế.*

---
### Thành viên
1. Nguyễn Thị Phương Oanh (31231021807) @phuongwoanh
2. Trần Lê Hiếu Giang (31231027369) @TranLeHieuGiang
3. Trần Thị Ngọc (31231024952) @ngoctran31231024952