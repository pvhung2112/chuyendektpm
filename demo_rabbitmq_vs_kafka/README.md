# SO SÁNH TRỰC DIỆN: RABBITMQ (TASK ROUTING) VS APACHE KAFKA (EVENT STREAMING)

Thư mục này được tạo riêng để đối chứng sự khác biệt về **Mục đích thiết kế (Design Philosophy)** giữa hai công nghệ:

---

## 1. Cấu trúc thư mục:
* `1_rabbitmq_task_routing.js`: Minh họa cơ chế **Định tuyến tác vụ (Task Routing)**, phân chia Hàng đợi VIP/Thường, cô lập lỗi bằng **Dead Letter Queue (DLQ)** và dọn dẹp bộ nhớ bằng **Manual ACK**.
* `2_kafka_event_streaming.js`: Minh họa cơ chế **Dòng chảy sự kiện (Event Streaming)**, nhật ký nối đuôi (**Commit Log & Offset**), **Nhiều Consumer Group** cùng đọc độc lập và tính năng **Tua lại lịch sử (Event Replay qua Seek)**.
* `run_demo.js`: Kịch bản chạy liên hoàn cả 2 demo và in bảng tổng kết so sánh.

---

## 2. Cách chạy:
Mở Terminal tại thư mục gốc và gõ lệnh:
```powershell
node demo_rabbitmq_vs_kafka/run_demo.js
```
Hoặc chạy từng bài riêng lẻ:
```powershell
node demo_rabbitmq_vs_kafka/1_rabbitmq_task_routing.js
node demo_rabbitmq_vs_kafka/2_kafka_event_streaming.js
```
