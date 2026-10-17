# CHƯƠNG 5: KẾT QUẢ THỰC NGHIỆM VÀ ĐÁNH GIÁ ĐỊNH LƯỢNG

---

## 5.1. Phương Pháp và Môi Trường Đo Kiểm Hiệu Năng

### 5.1.1. Môi trường thực nghiệm
* **Phần cứng kiểm thử:** Máy chủ thử nghiệm sử dụng vi xử lý Intel Core i7 đa nhân, 16GB RAM, ổ lưu trữ thể rắn NVMe SSD.
* **Môi trường phần mềm:** Hệ điều hành Windows/Linux 64-bit, Docker Engine phiên bản 24.x, Node.js Runtime phiên bản 22.x.
* **Công cụ đo kiểm tải (Load Testing Tools):** Sử dụng **Autocannon** và kịch bản đo kiểm **k6** để sinh tải đồng thời, tự động tính toán các phân vị độ trễ (Latency Percentiles: P50, P95, P99) và thông lượng xử lý.

### 5.1.2. Kịch bản kiểm thử (Test Scenarios)
Đề tài tiến hành đo kiểm trên 4 cấp độ tải đồng thời khác nhau nhằm mô phỏng từ mức tải thông thường đến các đợt tăng tải đột biến:
* **Cấp độ 1:** 10 requests đồng thời (Tải nhẹ - Baseline).
* **Cấp độ 2:** 100 requests đồng thời (Tải trung bình).
* **Cấp độ 3:** 500 requests đồng thời (Tải cao).
* **Cấp độ 4:** 1.000 requests đồng thời (Tải cực đại - Spike Traffic).

---

## 5.2. Kết Quả Đo Kiểm và Phân Tích Số Liệu

### 5.2.1. So sánh về Thời gian Phản hồi (Response Time / Latency)
Bảng số liệu thời gian phản hồi (đơn vị: mili-giây ms) phản hồi về cho người dùng qua các cấp độ tải:

| Mức tải đồng thời | Mô hình Đồng bộ (Không MQ) | Mô hình Bất đồng bộ (RabbitMQ) | Hệ số cải thiện |
| :--- | :--- | :--- | :--- |
| **10 requests** | P50: 1.520 ms \| P99: 1.680 ms | **P50: 5.8 ms \| P99: 12.4 ms** | **Nhanh hơn ~260 lần** |
| **100 requests** | P50: 2.150 ms \| P99: 3.420 ms | **P50: 7.2 ms \| P99: 18.6 ms** | **Nhanh hơn ~300 lần** |
| **500 requests** | P50: 4.800 ms \| P99: 7.950 ms | **P50: 11.5 ms \| P99: 28.2 ms** | **Nhanh hơn ~410 lần** |
| **1.000 requests** | P50: Timeout (>10s) \| P99: Lỗi | **P50: 18.2 ms \| P99: 46.5 ms** | **Hệ thống duy trì ổn định** |

> **Nhận xét:** 
> * Ở mô hình Đồng bộ, khi số lượng request tăng lên, thời gian phản hồi tăng theo cấp số nhân do các request phía sau phải xếp hàng đợi luồng xử lý (Thread Starvation). Ở mức 1.000 requests, đa số các yêu cầu rơi vào trạng thái chờ quá thời gian (Timeout).
> * Ở mô hình Bất đồng bộ với RabbitMQ, thời gian phản hồi của API luôn được duy trì ổn định ở mức **dưới 50 mili-giây** ngay cả khi chịu tải 1.000 requests đồng thời. Việc bóc tách các tác vụ nặng giúp luồng HTTP giải phóng ngay lập tức.

---

### 5.2.2. So sánh về Thông lượng Xử lý (Throughput - Requests/giây) và Tỷ lệ Lỗi (Error Rate)

| Chỉ số kỹ thuật | Mô hình Đồng bộ (Không MQ) | Mô hình Bất đồng bộ (RabbitMQ) |
| :--- | :--- | :--- |
| **Thông lượng trung bình (RPS)** | ~ 65 – 120 requests/giây | **~ 3.500 – 5.200 requests/giây** |
| **Tỷ lệ lỗi ở tải 100 req** | 0% | **0%** |
| **Tỷ lệ lỗi ở tải 500 req** | **18.6% (HTTP 504 / 502)** | **0%** |
| **Tỷ lệ lỗi ở tải 1.000 req (Spike)** | **42.8% (Hệ thống sập)** | **0% (100% thành công)** |
| **Hiện tượng sập dây chuyền** | **Có xảy ra** khi luồng bị nghẽn | **Không xảy ra** (Message lưu an toàn ở Queue) |

```
Biểu đồ Tỷ lệ Lỗi khi Tải Đột Biến (1.000 req):
Đồng bộ:      [████████████████████                    ] 42.8% LỖI TIMEOUT
Bất đồng bộ:  [                                        ] 0.0% LỖI
```

---

### 5.2.3. Mức Độ Chiếm Dụng Tài Nguyên Hệ Thống (CPU & RAM)
* **Mô hình Đồng bộ:**
  * CPU của API Gateway dao động mạnh (85% – 100%) do phải liên tục duy trì hàng nghìn kết nối HTTP treo và xử lý chuyển đổi ngữ cảnh (Context Switching).
  * Bộ nhớ RAM tăng vọt do tích tụ các buffer kết nối chưa hoàn tất.
* **Mô hình Bất đồng bộ:**
  * CPU của API Gateway ổn định ở mức thấp (15% – 25%) vì request được hoàn tất và đóng socket ngay lập tức.
  * Bộ nhớ RAM của RabbitMQ Broker duy trì ổn định (chỉ khoảng 120MB – 180MB) nhờ cơ chế quản lý bộ nhớ tối ưu của Erlang VM.
  * Các Worker tiêu thụ CPU đều đặn ở mức vừa phải (40% – 50%), xử lý tuần tự từng tác vụ mà không gây đột biến tài nguyên.

---

## 5.3. Đánh Giá Khả Năng Chịu Lỗi và Phục Hồi (Fault Tolerance Testing)

Đề tài tiến hành hai thử nghiệm phá hủy (Chaos Testing) có chủ đích để kiểm chứng độ tin cậy:

### 1. Thử nghiệm "Worker Crash" (Tiến trình xử lý bị tắt đột ngột)
* **Kịch bản:** Khi hệ thống đang có 200 tác vụ trong hàng đợi và một Worker đang trong quá trình thực thi một tác vụ nặng thì tiến trình Worker bị cưỡng bức tắt (`kill -9`).
* **Kết quả quan sát:**
  * Do cơ chế xác nhận thủ công (`noAck: false`), message đang xử lý dở dang chưa được gửi tín hiệu `basic.ack`.
  * Ngay khi kết nối TCP giữa Worker và RabbitMQ bị ngắt, Broker tự động thu hồi message đó và đưa lại vào đầu hàng đợi (*Re-queue*).
  * Worker thứ hai ngay lập tức nhận lại message đó và hoàn thành tác vụ. **Không có bất kỳ dữ liệu nào bị thất thoát (Zero Data Loss).**

### 2. Thử nghiệm "Broker Crash" (Máy chủ RabbitMQ bị khởi động lại đột ngột)
* **Kịch bản:** Khi hàng đợi đang chứa hàng nghìn thông điệp chưa được tiêu thụ, tiến trình container RabbitMQ bị dừng đột ngột (`docker restart`).
* **Kết quả quan sát:**
  * Nhờ cấu hình hàng đợi bền vững (`durable = true`) và thông điệp ghi đĩa (`persistent = true`), sau khi RabbitMQ khởi động lại thành công, 100% số lượng thông điệp trong hàng đợi được phục hồi nguyên vẹn từ đĩa cứng.
  * Các Worker tự động kết nối lại và tiếp tục xử lý bình thường.
