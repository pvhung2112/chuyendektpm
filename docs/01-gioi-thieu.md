# CHƯƠNG 1: GIỚI THIỆU VÀ ĐẶT VẤN ĐỀ NGHIÊN CỨU

---

## 1.1. Bối cảnh và Tính cấp thiết của Đề tài

Trong kỹ thuật phần mềm hiện đại, các hệ thống backend ngày càng đóng vai trò huyết mạch trong việc vận hành các dịch vụ số — từ các cổng dịch vụ công trực tuyến, mạng xã hội, nền tảng tài chính số (Fintech), đến các hệ thống logistics và dịch vụ web đa người dùng. Cùng với sự bùng nổ của người dùng Internet và các thiết bị kết nối, các hệ thống backend phải thường xuyên đối mặt với các đợt tăng đột biến về lưu lượng truy cập (traffic spikes) và các yêu cầu xử lý phức tạp.

Về bản chất, mọi hệ thống backend đều phải xử lý hai nhóm tác vụ với tính chất hoàn toàn khác nhau:
1. **Nhóm tác vụ phản hồi nhanh (Critical/Fast Path):** Các thao tác kiểm tra tính hợp lệ dữ liệu (validation), xác thực người dùng (authentication), và ghi nhận trạng thái ban đầu vào cơ sở dữ liệu. Nhóm tác vụ này đòi hỏi thời gian phản hồi tức thì (tính bằng mili-giây) để người dùng không cảm thấy ứng dụng bị trễ.
2. **Nhóm tác vụ nặng và tiêu tốn I/O (Heavy/Long-running Path):** Các tác vụ xử lý tốn nhiều thời gian và tài nguyên, bao gồm:
   * Giao tiếp với các dịch vụ bên thứ ba (gửi Email, gửi SMS, đẩy thông báo qua Firebase/APNs, gọi cổng thanh toán).
   * Xử lý và chuyển đổi dữ liệu (tạo tệp tài liệu PDF/Excel, xử lý nén/chuyển mã hình ảnh, video).
   * Đồng bộ và tổng hợp dữ liệu phân tán (ghi log kiểm toán, cập nhật kho dữ liệu phân tích).

Trong các kiến trúc truyền thống sử dụng giao tiếp **Đồng bộ (Synchronous Communication)** qua HTTP/REST hoặc RPC:
* Hệ thống buộc phải gộp chung cả hai nhóm tác vụ này vào trong cùng một chu kỳ Request - Response.
* Khi luồng chính bị khóa (Thread Blocking) để chờ đợi các tác vụ nặng hoàn thành, tài nguyên máy chủ (CPU, RAM, Socket Connection Pool) bị chiếm dụng lãng phí.
* Khi lưu lượng người dùng tăng đột biến, sự tắc nghẽn ở tầng xử lý nặng sẽ lan rộng ngược lại tầng tiếp nhận, gây ra hiện tượng **sập đổ dây chuyền (Cascading Failure)** và dẫn đến các lỗi hệ thống nghiêm trọng như `504 Gateway Timeout` hay `Connection Refused`.

Do đó, việc nghiên cứu và ứng dụng giải pháp **Message Queue** nhằm bóc tách các tác vụ nặng ra khỏi chu trình phản hồi chính, chuyển sang cơ chế **Xử lý bất đồng bộ (Asynchronous Processing)** và **Dàn phẳng tải trọng (Load Leveling)** là một định hướng kiến trúc nền tảng và có giá trị áp dụng phổ quát cho mọi hệ thống backend phân tán hiện nay.

---

## 1.2. Mục tiêu Nghiên cứu

Đề tài tập trung nghiên cứu trên góc độ tổng quan hệ thống phân tán, giải quyết 3 mục tiêu cốt lõi:

1. **Về mặt lý thuyết & kiến trúc:** 
   * Làm rõ bản chất của giao tiếp bất đồng bộ và cơ chế đệm tải của Message Queue.
   * Phân tích vai trò của Message Queue trong việc giải quyết 3 bài toán lớn: Khử khớp nối (*Decoupling*), Dàn phẳng tải trọng (*Load Leveling*), và Tăng cường độ chịu lỗi (*Fault Tolerance / Resilience*).
2. **Về mặt so sánh công nghệ:** 
   * Phân định rõ ràng các hệ thống messaging phổ biến hiện nay: **Message Broker / Task Queue** (tiêu biểu là RabbitMQ) và **Distributed Event Streaming** (tiêu biểu là Apache Kafka), làm rõ sự khác biệt giữa triết lý *Task-driven* (quản lý công việc) và *Event-driven* (quản lý nhật ký sự kiện).
   * Phân tích các tiêu chí lựa chọn công nghệ phù hợp với từng bài toán kỹ thuật cụ thể.
3. **Về mặt thực nghiệm & định lượng:** 
   * Xây dựng mô hình thực nghiệm đối chứng giữa hai mô hình: Hệ thống đồng bộ (Không dùng MQ) và Hệ thống bất đồng bộ (Có tích hợp Message Queue).
   * Đo lường định lượng các chỉ số hiệu năng: Thời gian phản hồi (Response Time / Latency P50, P95, P99), Thông lượng xử lý (Throughput - RPS), Tỷ lệ lỗi (Error Rate) và mức độ sử dụng tài nguyên phần cứng (CPU, RAM) dưới nhiều cấp độ tải khác nhau.

---

## 1.3. Đối tượng và Phạm vi Nghiên cứu

* **Đối tượng nghiên cứu:**
  * Các giải pháp kiến trúc truyền thông điệp bất đồng bộ (Messaging Systems).
  * Công nghệ Message Broker chuyên dụng: **RabbitMQ** (giao thức chuẩn AMQP 0-9-1).
  * Công nghệ Distributed Event Streaming: **Apache Kafka** (đối tượng đối sánh lý thuyết).
* **Phạm vi nghiên cứu:**
  * **Phạm vi kiến trúc:** Kiến trúc hệ thống phân tán, mô hình Client – API Gateway – Message Queue – Background Worker.
  * **Phạm vi nghiệp vụ thực nghiệm:** Bài toán tổng quát về tiếp nhận yêu cầu từ người dùng và phân tải các tác vụ I/O nặng ngầm dưới nền.
  * **Phạm vi công nghệ thực nghiệm:** Ảo hóa hạ tầng bằng Docker/Docker Compose, phát triển dịch vụ bằng Node.js, sử dụng công cụ đo tải tự động (Autocannon / k6).

---

## 1.4. Phương Pháp Nghiên Cứu

Đề tài kết hợp hai phương pháp tiếp cận chính:
1. **Phương pháp nghiên cứu lý thuyết:** Khảo cứu tài liệu học thuật, chuẩn giao thức quốc tế (AMQP, RESP, Distributed Log), tài liệu kiến trúc của các tổ chức công nghệ hàng đầu (AWS Architecture Center, Martin Fowler, Chris Richardson).
2. **Phương pháp thực nghiệm định lượng (Empirical Benchmark):** Thiết lập môi trường đo kiểm có kiểm soát, tạo tải giả lập với các mức tải từ thấp đến cao (10, 100, 500, 1.000 requests đồng thời), thu thập số liệu khách quan, phân tích biểu đồ và kiểm thử các kịch bản chịu lỗi (Worker Crash, Broker Failure).
