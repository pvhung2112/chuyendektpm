# CHƯƠNG 4: THIẾT KẾ VÀ XÂY DỰNG HỆ THỐNG THỰC NGHIỆM

---

## 4.1. Kiến Trúc Bài Toán Thực Nghiệm Tổng Quát

Để kiểm chứng một cách khách quan và chính xác các luận điểm lý thuyết, đề tài thiết kế một hệ thống backend thực nghiệm với mô hình xử lý tác vụ tổng quát, đại diện cho các hệ thống web và dịch vụ số hiện đại.

### 4.1.1. Luồng xử lý nghiệp vụ đối chứng
Hệ thống tiếp nhận yêu cầu từ người dùng và cần thực hiện một chuỗi các tác vụ:
1. Ghi nhận thông tin yêu cầu ban đầu (Validation & Record Persistence).
2. Chuỗi các tác vụ phụ trợ tốn nhiều I/O và thời gian tính toán:
   * Tác vụ 1: Gửi thông báo xác nhận qua dịch vụ bên ngoài (External Notification - mô phỏng trễ 500ms).
   * Tác vụ 2: Xử lý kết xuất tệp tài liệu/báo cáo chuyên sâu (Document/File Processing - mô phỏng trễ 600ms).
   * Tác vụ 3: Đồng bộ trạng thái và ghi nhật ký kiểm toán hệ thống (Audit Logging & Partner Sync - mô phỏng trễ 400ms).

```
                        KIẾN TRÚC ĐỐI CHỨNG THỰC NGHIỆM
                                       │
       ┌───────────────────────────────┴───────────────────────────────┐
       ▼                                                               ▼
CÁCH 1: MÔ HÌNH ĐỒNG BỘ (SYNC)                               CÁCH 2: BẤT ĐỒNG BỘ VỚI RABBITMQ (ASYNC)
       │                                                               │
[Client Request]                                                [Client Request]
       │                                                               │
[Core API Server]                                               [Core API Server]
       │                                                               │
 ├── Lưu dữ liệu (50ms)                                          ├── Lưu dữ liệu (50ms)
 ├── Gửi thông báo (500ms)                                       ├── Gửi Message vào RabbitMQ (5ms)
 ├── Xử lý tài liệu (600ms)                                      │
 └── Đồng bộ kiểm toán (400ms)                                   └── [Phản hồi Client HTTP 202: ~ 55ms]
       │                                                               │
<── [Phản hồi Client: ~ 1.55 giây]                                     [RabbitMQ Exchange & Queue]
                                                                       │ (Phân phối tác vụ)
                                                                       ▼
                                                                [Background Worker]
                                                                 ├── Gửi thông báo (500ms)
                                                                 ├── Xử lý tài liệu (600ms)
                                                                 └── Đồng bộ kiểm toán (400ms)
                                                                 └── Gửi Manual ACK
```

---

## 4.2. Thiết Kế Các Mẫu Kiến Trúc Đảm Bảo Độ Tin Cậy (Reliability Patterns)

Một hệ thống Message Queue trong môi trường phân tán thực tế luôn phải đối mặt với các nguy cơ lỗi mạng, dữ liệu hỏng hoặc tiến trình bị sập. Đề tài ứng dụng 3 mẫu thiết kế chuẩn công nghiệp:

### 4.2.1. Mẫu Dead Letter Queue (DLQ) - Cô Lập "Tin Nhắn Độc" (Poison Messages)
* **Vấn đề:** Khi một thông điệp chứa dữ liệu không hợp lệ hoặc gây lỗi logic nội bộ, Worker sẽ gặp ngoại lệ (Exception). Nếu thông điệp liên tục bị đẩy ngược lại hàng đợi (*re-queue*), Worker sẽ rơi vào vòng lặp lỗi vô tận (*Crash Loop*), chiếm dụng toàn bộ thời gian CPU và làm tắc nghẽn toàn bộ hàng đợi.
* **Cơ chế triển khai:**
  1. Thiết lập một **Dead Letter Exchange (DLX)** và liên kết với một **Dead Letter Queue (DLQ)** riêng biệt.
  2. Cấu hình tham số `x-dead-letter-exchange` vào hàng đợi chính.
  3. Khi Worker phát hiện thông điệp lỗi không thể phục hồi, nó gửi lệnh `channel.nack(msg, false, false)` (từ chối và không requeue). RabbitMQ sẽ tự động định tuyến gói tin này sang DLQ để lưu trữ phục vụ việc phân tích lỗi của quản trị viên.

### 4.2.2. Mẫu Retry với Exponential Backoff (Thử lại trễ tăng dần)
* **Vấn đề:** Đối với các sự cố tạm thời (Transient Faults) như mạng chập chờn hoặc dịch vụ bên thứ ba bị quá tải nhất thời, việc thử lại ngay lập tức sẽ làm tăng nguy cơ quá tải.
* **Cơ chế triển khai:** Sử dụng kiến trúc hàng đợi đệm có thời gian sống (TTL - Time To Live). Thông điệp gặp lỗi tạm thời sẽ được đẩy vào hàng đợi chờ (Wait Queue). Sau khoảng thời gian trễ tăng dần (ví dụ: $1s, 2s, 4s, 8s$), thông điệp sẽ tự động được RabbitMQ chuyển ngược lại hàng đợi chính để thực thi lại.

### 4.2.3. Mẫu Transactional Outbox Pattern - Bảo Toàn Tính Nhất Quán Dữ Liệu
* **Vấn đề Dual-Write:** Khi một dịch vụ phải vừa ghi dữ liệu vào cơ sở dữ liệu chính, vừa phát thông điệp vào Message Queue. Nếu một trong hai thao tác thành công còn thao tác kia thất bại, hệ thống sẽ rơi vào trạng thái bất nhất quán dữ liệu.
* **Cơ chế triển khai:**
  * Bổ sung bảng `outbox_events` vào cùng cơ sở dữ liệu với bảng nghiệp vụ chính.
  * Việc ghi dữ liệu nghiệp vụ và ghi bản tin sự kiện được thực hiện trong **cùng một Database Transaction duy nhất** (đảm bảo tính nguyên tử - Atomicity).
  * Một tiến trình nền độc lập (**Outbox Relay Process**) định kỳ quét các bản tin trong bảng Outbox để phát sang RabbitMQ. Chỉ khi nhận được xác nhận từ Broker (**Publisher Confirms**), bản tin trong bảng Outbox mới được cập nhật trạng thái đã hoàn tất.

---

## 4.3. Thiết Lập Hạ Tầng Triển Khai (Docker Compose)

Hệ thống được ảo hóa bằng tệp cấu hình `docker-compose.yml`, giúp việc khởi chạy môi trường kiểm thử diễn ra nhất quán và nhanh chóng:

* **RabbitMQ Server:** Khởi chạy image `rabbitmq:3-management-alpine`.
  * Cổng `5672`: Dành cho giao thức AMQP phục vụ truyền tin tốc độ cao giữa các tiến trình.
  * Cổng `15672`: Dành cho giao diện Web Management UI trực quan, cho phép theo dõi thời gian thực biểu đồ lưu lượng, số lượng message tồn đọng và trạng thái kết nối của Worker.

---

## 4.4. Triển Khai Mã Nguồn Ứng Dụng (Node.js)

Hệ thống mã nguồn thực nghiệm được xây dựng gồm các thành phần:
1. **API Gateway / Web Server (`server.js`):**
   * Endpoint `POST /api/sync/task`: Mô phỏng xử lý đồng bộ toàn bộ chuỗi tác vụ trên cùng một luồng HTTP.
   * Endpoint `POST /api/rabbitmq/task`: Tiếp nhận yêu cầu, đẩy dữ liệu vào `tasks_queue` với cờ `persistent: true`, trả về mã phản hồi `202 Accepted` ngay tức thì.
2. **Background Worker (`rabbitmq_worker.js`):**
   * Đăng ký tiêu thụ thông điệp từ `tasks_queue`.
   * Thiết lập `channel.prefetch(1)` để đảm bảo cơ chế phân phối công bằng (Fair Dispatch), không để dồn ứ việc vào một worker duy nhất.
   * Thực hiện các tác vụ nền giả lập, gửi tín hiệu `channel.ack(msg)` khi thành công, hoặc chuyển sang DLQ khi xảy ra lỗi.
