# HƯỚNG DẪN THỰC NGHIỆM CHUYÊN ĐỀ KỸ THUẬT PHẦN MỀM
## Đề tài: Nghiên cứu và Đánh giá giải pháp xử lý tác vụ bất đồng bộ và giảm tải hệ thống Backend sử dụng Message Queue

Dự án này bao gồm:
1. **Bộ tài liệu báo cáo nghiên cứu dạng mô-đun (thư mục `docs/`):**
   * [01-gioi-thieu.md](file:///d:/chuyendektpm/docs/01-gioi-thieu.md): Bối cảnh hệ thống backend phân tán, tính cấp thiết, mục tiêu và 3 câu hỏi nghiên cứu cốt lõi.
   * [02-co-so-ly-thuyet.md](file:///d:/chuyendektpm/docs/02-co-so-ly-thuyet.md): Rào cản của giao tiếp đồng bộ (REST/gRPC), bản chất Messaging Systems, 3 trụ cột (Decoupling, Load Leveling, Fault Tolerance).
   * [03-rabbitmq-va-kafka.md](file:///d:/chuyendektpm/docs/03-rabbitmq-va-kafka.md): Phân định học thuật giữa Message Queue (RabbitMQ - Task-based) và Event Streaming (Apache Kafka - Log-based), bảng so sánh 10 tiêu chí và luận giải kỹ thuật.
   * [04-thiet-ke-thuc-nghiem.md](file:///d:/chuyendektpm/docs/04-thiet-ke-thuc-nghiem.md): Thiết kế hệ thống thực nghiệm tổng quát, các mẫu thiết kế tin cậy (DLQ, Exponential Backoff Retry, Transactional Outbox Pattern).
   * [05-ket-qua-va-danh-gia.md](file:///d:/chuyendektpm/docs/05-ket-qua-va-danh-gia.md): Kết quả đo kiểm tải định lượng qua 4 cấp độ (10, 100, 500, 1.000 requests), đánh giá Latency P50/P95/P99, Throughput, Tỷ lệ lỗi và Chaos testing.
   * [06-ket-luan.md](file:///d:/chuyendektpm/docs/06-ket-luan.md): Tổng kết đề tài, bài học kinh nghiệm và khuyến nghị kiến trúc cho kỹ sư phần mềm.
2. **Bản Báo cáo Chuyên đề tổng hợp:** [BAO_CAO_CHUYEN_DE_KTPM.md](file:///d:/chuyendektpm/BAO_CAO_CHUYEN_DE_KTPM.md).
3. **Mã nguồn hệ thống thực nghiệm đối chứng:**
   * **Đồng bộ:** `POST /api/sync/task` (giả lập nghẽn luồng xử lý 1.5s).
   * **Bất đồng bộ với RabbitMQ:** `POST /api/rabbitmq/task` (AMQP, Durable Queue, Persistent Message, Dead Letter Queue).
   * **Bất đồng bộ với Redis Streams:** `POST /api/redis/task` (In-memory, Consumer Group, PEL, XACK).
4. **Mã nguồn tự động đo kiểm tải:** `benchmark.js`.

---

## 1. Yêu cầu môi trường
* Đã cài đặt **Node.js** (v18 trở lên).
* Đã cài đặt **Docker** & **Docker Compose** (để khởi chạy RabbitMQ và Redis).

---

## 2. Các bước triển khai

### Bước 1: Khởi chạy hạ tầng RabbitMQ và Redis
Mở terminal trong thư mục này và chạy:
```bash
docker compose up -d
```
* **RabbitMQ Web Management UI:** Truy cập `http://localhost:15672` (Tài khoản: `admin` / `admin123`).
* **Redis Server:** Chạy tại `localhost:6379`.

### Bước 2: Cài đặt thư viện Node.js
```bash
npm install
```

### Bước 3: Khởi chạy hệ thống

**Tab Terminal 1 - Chạy Web API Server:**
```bash
npm start
```

**Tab Terminal 2 - Chạy RabbitMQ Worker:**
```bash
npm run worker:rabbitmq
```

**Tab Terminal 3 - Chạy Redis Stream Worker:**
```bash
npm run worker:redis
```

---

## 3. Chạy đo kiểm hiệu năng (Benchmarking)

**Tab Terminal 4 - Chạy kịch bản test tải tự động:**
```bash
npm run benchmark
```
Chương trình sẽ tự động đo:
* **Throughput:** Số lượng yêu cầu hoàn thành/giây (req/s).
* **Latency:** Thời gian phản hồi về cho client (P50, P99).
* **Error Rate:** Số lượng request bị timeout hoặc gián đoạn kết nối.
