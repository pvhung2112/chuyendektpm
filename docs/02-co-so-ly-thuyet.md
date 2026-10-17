# CHƯƠNG 2: CƠ SỞ LÝ THUYẾT VÀ CƠ CHẾ MESSAGE QUEUE

---

## 2.1. Mô hình Xử lý Tác vụ trong Hệ thống Backend

### 2.1.1. Phân loại Tác vụ trong Hệ thống Backend
Trong một ứng dụng Backend hiện đại, khi tiếp nhận một yêu cầu (Request) từ phía người dùng, hệ thống thường phải thực hiện nhiều thao tác xử lý khác nhau. Xét theo tính chất và yêu cầu về thời gian phản hồi, các tác vụ này được chia thành hai nhóm rõ rệt:

1. **Nhóm tác vụ phản hồi nhanh (Critical / Fast Path):**
   * Bao gồm các thao tác: Kiểm tra tính hợp lệ của dữ liệu (Validation), xác thực quyền truy cập (Authentication/Authorization), và ghi nhận trạng thái cơ bản của giao dịch vào cơ sở dữ liệu.
   * *Đặc điểm:* Đòi hỏi thời gian xử lý cực ngắn (tính bằng mili-giây) và bắt buộc phải trả kết quả tức thời để người dùng xác nhận thao tác đã được tiếp nhận.

2. **Nhóm tác vụ phụ trợ tiêu tốn I/O và thời gian tính toán (Heavy / Background Path):**
   * Bao gồm các công việc phát sinh sau khi tác vụ chính đã hoàn tất:
     * Gửi thông báo đa kênh: Gửi email xác nhận, gửi tin nhắn SMS OTP, đẩy thông báo qua ứng dụng di động (Push Notification).
     * Xử lý tài liệu và dữ liệu lớn: Kết xuất hóa đơn điện tử (PDF/Excel), nén/chuyển mã hình ảnh hoặc tệp đính kèm.
     * Tích hợp và đồng bộ đối tác thứ ba: Gọi các webhook đối tác, ghi log kiểm toán (Audit Log), đồng bộ kho dữ liệu phân tích.
   * *Đặc điểm:* Thời gian thực thi thường kéo dài (từ vài trăm mili-giây đến vài giây, phụ thuộc vào tốc độ mạng và dịch vụ bên thứ ba). Điều quan trọng là **người dùng không nhất thiết phải chờ đợi các tác vụ này hoàn tất** mới có thể tiếp tục trải nghiệm ứng dụng.

```
                    MÔ HÌNH PHÂN LOẠI TÁC VỤ BACKEND
                                   │
         ┌─────────────────────────┴─────────────────────────┐
         ▼                                                   ▼
   FAST PATH (Phản hồi nhanh)                         HEAVY PATH (Tác vụ nặng)
• Xác thực người dùng (Auth)                       • Gửi Email / SMS xác nhận
• Kiểm tra hợp lệ dữ liệu                          • Kết xuất tệp PDF / Excel
• Ghi nhận bản ghi chính vào DB                    • Đồng bộ dịch vụ bên thứ ba
  (Yêu cầu: < 50ms)                                  (Thời gian: 500ms - 2.000ms)
```

![Hình 2.1: Sơ đồ phân loại và điều phối tác vụ trong hệ thống Backend](file:///d:/chuyendektpm/images/hinh_2_1_phan_loai_tac_vu.jpg)
*Hình 2.1: Sơ đồ phân loại và điều phối tác vụ trong hệ thống Backend.*

### 2.1.2. Vòng đời của một Yêu cầu (Request Lifecycle) và Nguy cơ Nghẽn Cổ Chai
Trong mô hình máy chủ web tiêu chuẩn, mỗi kết nối từ người dùng được gắn với một luồng xử lý hoặc một ngữ cảnh tài nguyên (Thread / Socket Connection):
* Nếu Backend gộp chung cả hai nhóm tác vụ trên vào cùng một chu trình xử lý tuần tự, toàn bộ thời gian trễ của nhóm tác vụ nặng sẽ cộng dồn trực tiếp vào tổng thời gian phản hồi của yêu cầu.
* Trong suốt quá trình này, luồng xử lý của máy chủ bị giữ lại ở trạng thái rảnh rỗi chờ I/O (*I/O Wait*), không thể giải phóng để tiếp nhận các yêu cầu khác.
* Khi lưu lượng truy cập tăng cao, việc phân bổ tài nguyên xử lý cho các tác vụ phụ trợ không cấp thiết sẽ chiếm hết năng lực của hệ thống, biến tầng tiếp nhận request trở thành "nút thắt cổ chai" (Bottleneck) lớn nhất của Backend.

![Hình 2.2: Vòng đời của một yêu cầu trong mô hình xử lý đồng bộ](file:///d:/chuyendektpm/images/hinh_2_2_vong_doi_yeu_cau_dong_bo.jpg)
*Hình 2.2: Vòng đời của một yêu cầu trong mô hình xử lý đồng bộ.*

---

## 2.2. Hạn chế của Giao tiếp Đồng bộ (Synchronous Communication)

Giao tiếp đồng bộ (Synchronous Communication), điển hình qua giao thức HTTP/REST hoặc RPC/gRPC, là phương thức xử lý theo cơ chế Yêu cầu - Phản hồi (Request - Response). Client gửi một request và phải duy trì kết nối chờ đợi Server xử lý tuần tự toàn bộ các bước rồi mới trả lời.

```
[Client] ──(1. HTTP Request)──> [Backend Server]
                                       │
             (Chờ đợi tuần tự)         ├──(2. Lưu dữ liệu chính: 50ms)
                                       ├──(3. Gửi Email thông báo: 500ms)
                                       ├──(4. Kết xuất hóa đơn PDF: 600ms)
                                       └──(5. Gọi API đối tác thứ ba: 400ms)
                                       │
[Client] <──(6. HTTP Response)─────────┘  (Tổng thời gian chờ: ~ 1.55s)
```

Mặc dù có ưu điểm là đơn giản, dễ cài đặt và dễ gỡ lỗi, phương thức đồng bộ bộc lộ 3 hạn chế chí mạng khi hệ thống phải đối mặt với lưu lượng lớn:

### 2.2.1. Ràng buộc thời gian (Temporal Coupling)

Ràng buộc thời gian (Temporal Coupling) là sự phụ thuộc về thời gian giữa các thành phần trong quá trình xử lý. Khi một thành phần gọi trực tiếp một thành phần khác theo phương thức đồng bộ, thành phần gửi phải chờ thành phần nhận hoàn thành xử lý hoặc trả về kết quả.

* Trong mô hình đồng bộ trực tiếp, khi **Order Service** gửi yêu cầu (Request) sang **Email Service**, tiến trình gửi buộc phải rơi vào trạng thái chờ (Blocked) cho đến khi Email Service xử lý xong và trả về kết quả (Response). Trong trường hợp Email Service phản hồi chậm hoặc dịch vụ bên thứ ba bị nghẽn, Order Service cũng phải chờ theo, làm tăng thời gian chiếm dụng tài nguyên hệ thống.
* Với giải pháp Message Queue, sự ràng buộc trực tiếp về thời gian được triệt tiêu: **Order Service** chỉ cần gửi thông điệp (Message) vào hàng đợi trong vài mili-giây và có thể tiếp tục ngay các bước xử lý tiếp theo mà không cần chờ **Email Worker** hoàn thành tác vụ gửi email.

![Hình 2.5: So sánh sự ràng buộc thời gian giữa giao tiếp đồng bộ và qua Message Queue](file:///d:/chuyendektpm/images/hinh_2_5_temporal_coupling.jpg)
*Hình 2.5: So sánh sự ràng buộc thời gian giữa giao tiếp đồng bộ và qua Message Queue.*

### 2.2.2. Khóa luồng và Cạn kiệt tài nguyên (Thread Starvation)
Mỗi máy chủ web (như Tomcat, Kestrel hay Node.js Event Loop) đều có một giới hạn tài nguyên nhất định về nhóm luồng (*Thread Pool*) hoặc số lượng kết nối đồng thời:
* Khi các luồng bị khóa để chờ I/O từ các tác vụ phụ trợ kéo dài, nhóm luồng xử lý nhanh chóng bị lấp đầy.
* Tình trạng này dẫn đến hiện tượng **Đói luồng (Thread Starvation)**: các request mới gửi đến dù chỉ yêu cầu các tác vụ rất nhẹ cũng không thể được tiếp nhận và phải xếp hàng chờ đợi.
* Hậu quả là thời gian phản hồi của hệ thống tăng vọt theo cấp số nhân, máy chủ nhanh chóng cạn kiệt bộ nhớ đệm và kết nối socket dù dung lượng CPU có thể chưa sử dụng hết.

### 2.2.3. Hiệu ứng sập dây chuyền (Cascading Failure)
Khi xuất hiện các đợt tăng tải đột biến hoặc khi một dịch vụ phụ trợ bên ngoài gặp sự cố làm tăng độ trễ (ví dụ: thời gian gửi email tăng từ 500ms lên 5s do nghẽn mạng), hiệu ứng tích tụ thời gian chờ sẽ nhanh chóng lan truyền ngược lại toàn bộ hệ thống:
1. Dịch vụ phụ trợ phản hồi chậm $\rightarrow$
2. Tầng Backend chính bị ứ đọng hàng nghìn yêu cầu chưa thể giải phóng $\rightarrow$
3. Vùng kết nối cơ sở dữ liệu (Connection Pool) và bộ nhớ đệm bị chiếm dụng tối đa $\rightarrow$
4. Tầng API Gateway ngoài cùng bị tràn bộ đệm và buộc phải từ chối các kết nối mới, trả về hàng loạt lỗi `504 Gateway Timeout` hoặc `502 Bad Gateway`.
Toàn bộ hệ thống Backend bị tê liệt dây chuyền chỉ vì sự chậm trễ của các tác vụ phụ trợ không mang tính sống còn.

![Hình 2.7: Minh họa hiệu ứng sập dây chuyền (Cascading Failure) do tắc nghẽn chuỗi gọi đồng bộ](file:///d:/chuyendektpm/images/hinh_2_7_cascading_failure.jpg)
*Hình 2.7: Minh họa hiệu ứng sập dây chuyền (Cascading Failure) do tắc nghẽn chuỗi gọi đồng bộ.*

Để khắc phục hiện tượng sập dây chuyền, cơ chế bất đồng bộ thông qua Message Queue tách biệt hoàn toàn Order Service khỏi các dịch vụ phụ trợ. Thay vì gọi nối tiếp nhau, Order Service chỉ gửi message vào hàng đợi. Các Worker (Payment Worker, Notification Worker, Email Worker) sẽ độc lập tiếp nhận và thực thi tác vụ. Khi một Worker gặp sự cố, message vẫn được lưu trữ an toàn trong Queue mà không gây ảnh hưởng đến Order Service hay các Worker khác.

![Hình 2.8: Sử dụng Message Queue để phân tách các Worker, triệt tiêu hiệu ứng sập dây chuyền](file:///d:/chuyendektpm/images/hinh_2_8_mq_decoupling_workers.jpg)
*Hình 2.8: Sử dụng Message Queue để phân tách các Worker, triệt tiêu hiệu ứng sập dây chuyền.*

---

## 2.3. Cơ chế Giao tiếp Bất đồng bộ và Khái niệm Message Queue

### 2.3.1. Bản chất của Giao tiếp Bất đồng bộ
Bản chất cốt lõi phân biệt giữa giao tiếp Đồng bộ và Bất đồng bộ nằm ở **tính chất khóa luồng (Blocking)** và **sự ràng buộc về mặt thời gian (Temporal Coupling)**:

* **Giao tiếp Đồng bộ (Synchronous - ví dụ: HTTP REST, gRPC):**
  $$\text{Client gửi yêu cầu} \longrightarrow \text{Client bị khóa (Blocked) và chờ đợi} \longrightarrow \text{Server xử lý} \longrightarrow \text{Trả kết quả} \longrightarrow \text{Client tiếp tục chạy}$$
  * *Hạn chế:* Client và Server phụ thuộc chặt chẽ về mặt thời gian. Nếu Server xử lý chậm hoặc lỗi, Client phải gánh chịu toàn bộ thời gian chờ đó.

* **Giao tiếp Bất đồng bộ (Asynchronous - ví dụ: Message Queue, Event-driven):**
  $$\text{Client gửi Message} \longrightarrow \text{Đưa vào Hàng đợi trung gian} \longrightarrow \text{Client tiếp tục làm việc ngay lập tức} \longrightarrow \text{Worker nhận Message và xử lý ngầm (Background)}$$
  * *Ưu thế:* Thời điểm gửi yêu cầu và thời điểm thực thi công việc được **tách rời hoàn toàn khỏi nhau** (*Temporal Decoupling*). Client không bị khóa tài nguyên và không cần phải biết chính xác khi nào tác vụ nền hoàn thành.

| Tiêu chí | Giao tiếp Đồng bộ (Synchronous) | Giao tiếp Bất đồng bộ (Asynchronous) |
| :--- | :--- | :--- |
| **Trạng thái Client** | Bị khóa (Blocked), đứng chờ kết quả | Không khóa (Non-blocking), tiếp tục ngay |
| **Ràng buộc thời gian** | Bắt buộc gửi và xử lý đồng thời | Tách rời thời gian gửi và thời gian thực thi |
| **Thành phần trung gian** | Kết nối trực tiếp giữa hai bên | Có Message Broker / Queue làm vùng đệm |
| **Khả năng chịu tải** | Dễ cạn kiệt luồng (Thread Starvation) | Bộ đệm hấp thụ đột biến tải (Load Leveling) |
| **Xử lý lỗi** | Dễ sập dây chuyền (Cascading Failure) | Cô lập lỗi, hỗ trợ cơ chế Retry / Dead Letter |

![Hình 2.9: So sánh bản chất luồng xử lý giữa giao tiếp đồng bộ và giao tiếp bất đồng bộ](file:///d:/chuyendektpm/images/hinh_2_9_sync_vs_async_concept.jpg)
*Hình 2.9: So sánh bản chất luồng xử lý giữa giao tiếp đồng bộ và giao tiếp bất đồng bộ.*

### 2.3.2. Kiến trúc Producer – Broker – Consumer
Hệ thống Message Queue chuẩn hóa vận hành dựa trên 3 thành phần cốt lõi:

```
[Producer]  ───(1. Gửi Message)───>  [Message Broker]  ───(2. Phân phối)───>  [Consumer / Worker]
(API Backend)                        ┌───────────────┐                        (Xử lý ngầm)
                                     │ Queue 1 │ Q2  │                        ├── Gửi Email
                                     └───────────────┘                        ├── Xuất PDF
                                                                              └── Đồng bộ dữ liệu
```

![Hình 2.10: Kiến trúc cốt lõi Producer – Broker – Consumer với Hàng đợi (Queue) nằm bên trong Message Broker](file:///d:/chuyendektpm/images/hinh_2_10_kien_truc_producer_broker_consumer.jpg)
*Hình 2.10: Kiến trúc cốt lõi Producer – Broker – Consumer với Hàng đợi (Queue) nằm bên trong Message Broker.*

1. **Producer (Bên sản xuất):** Thành phần khởi tạo thông điệp (thường là API Server tiếp nhận yêu cầu từ người dùng). Producer chỉ đóng gói dữ liệu công việc và đẩy vào hàng đợi rồi giải phóng luồng xử lý ngay lập tức.
2. **Message Broker (Bộ điều phối thông điệp):** Phần mềm trung gian chịu trách nhiệm lưu trữ an toàn các thông điệp (trong bộ nhớ RAM hoặc ghi xuống đĩa cứng), quản lý hàng đợi và điều phối thông điệp đến người nhận. Hàng đợi (Queue) nằm trọn vẹn bên trong Broker.
3. **Consumer / Worker (Bên tiêu thụ):** Các tiến trình ứng dụng độc lập liên tục lắng nghe hàng đợi từ Broker, rút thông điệp ra và thực thi các logic nghiệp vụ nặng (gửi email, xử lý ảnh, tạo báo cáo).

### 2.3.3. Các Mô hình Truyền tin Cơ bản
Hệ thống Message Queue hỗ trợ hai mô hình phân phối thông điệp nền tảng:

* **Mô hình Điểm - Điểm (Point-to-Point / Task Queue):**
  * Trong mô hình này, Producer gửi thông điệp vào một Hàng đợi (Queue). Dù có nhiều Worker cùng kết nối và lắng nghe chung một hàng đợi, **mỗi thông điệp chỉ được giao cho duy nhất 1 Worker xử lý** (*Competing Consumers Pattern*).
  * Các Worker sẽ chia nhau khối lượng công việc: Worker 1 xử lý Tin nhắn 1, Worker 2 xử lý Tin nhắn 2. Khi một Worker hoàn tất tác vụ và gửi tín hiệu xác nhận (ACK), thông điệp sẽ lập tức bị xóa khỏi hàng đợi.
  * *Trường hợp sử dụng:* Phân chia và xử lý các tác vụ nền rời rạc (gửi email, xử lý video/ảnh, tạo tệp báo cáo Excel/PDF).

![Hình 2.11: Mô hình Điểm - Điểm (Point-to-Point / Competing Consumers)](file:///d:/chuyendektpm/images/hinh_2_11_mo_hinh_point_to_point.jpg)
*Hình 2.11: Mô hình Điểm - Điểm (Point-to-Point / Competing Consumers) - Mỗi thông điệp chỉ do đúng 1 Worker xử lý.*

* **Mô hình Xuất bản - Đăng ký (Publish/Subscribe):**
  * Trong mô hình này, Publisher phát ra một sự kiện (Event) vào một chủ đề (Topic / Exchange).
  * Broker sẽ tự động **nhân bản (sao chép) thông điệp đó thành nhiều bản** và phân phối tới **TẤT CẢ các Consumer (Subscriber)** đang đăng ký nhận tin từ Topic đó.
  * Mỗi Consumer sẽ thực hiện một mục đích nghiệp vụ hoàn toàn độc lập (ví dụ: một sự kiện `OrderCreated` được bắn ra thì cả hệ thống Gửi thông báo, hệ thống Phân tích số liệu và hệ thống Quản lý kho vận đều cùng nhận được 1 bản copy để xử lý theo logic riêng).
  * *Trường hợp sử dụng:* Phát sự kiện toàn hệ thống (Event-Driven Architecture), cập nhật dữ liệu đa dịch vụ.

![Hình 2.12: Mô hình Xuất bản - Đăng ký (Publish/Subscribe)](file:///d:/chuyendektpm/images/hinh_2_12_mo_hinh_pub_sub.jpg)
*Hình 2.12: Mô hình Xuất bản - Đăng ký (Publish/Subscribe) - 1 Thông điệp được sao chép và gửi tới TẤT CẢ các bên đăng ký.*

#### Bảng so sánh cốt lõi giữa hai mô hình:

| Tiêu chí | Mô hình Điểm - Điểm (Point-to-Point) | Mô hình Xuất bản - Đăng ký (Publish/Subscribe) |
| :--- | :--- | :--- |
| **Bản chất** | **Chia việc (1-to-1):** 1 việc chỉ giao cho 1 người làm | **Phát thanh (1-to-Many):** 1 tin được gửi cho tất cả |
| **Số lượng người nhận mỗi tin** | **Chính xác 1 Worker duy nhất** (các Worker khác không nhận lại tin đó) | **Tất cả các Subscriber** đang đăng ký kênh/Topic đó |
| **Xử lý thông điệp** | Xóa khỏi Queue ngay sau khi Worker xử lý xong (ACK) | Mỗi Subscriber có hộp thư riêng, nhận bản sao độc lập |
| **Hình tượng thực tế** | **Cuốc xe Grab / Đơn giao hàng:** 1 cuốc xe nổ ra, tài xế nào nhận thì cuốc đó biến mất | **Loa phát thanh / Đăng bài Facebook:** 1 thông báo phát ra, mọi người dân đều nghe thấy |
| **Ứng dụng tiêu biểu** | Gửi email, chuyển mã video, xuất file Excel/PDF (Task Queue) | Kiến trúc vi dịch vụ Event-Driven, đồng bộ dữ liệu đa hệ thống |

---

## 2.4. Ba Trụ Cột Giá Trị của Message Queue đối với Backend

Việc tích hợp Message Queue vào kiến trúc Backend mang lại 3 giá trị kỹ thuật cốt lõi giúp hệ thống đạt hiệu năng cao và khả năng chịu tải vượt trội:

### 2.4.1. Khử khớp nối (Decoupling)
Message Queue mang lại khả năng phân tách cả về mặt **không gian (Spatial Decoupling)** và **thời gian (Temporal Decoupling)**:
* **Tách biệt luồng xử lý:** Tầng API tiếp nhận không cần quan tâm tác vụ nền sẽ do ai xử lý, xử lý bằng ngôn ngữ gì và xử lý mất bao lâu.
* **Độc lập về trạng thái hoạt động:** Nếu dịch vụ gửi email hoặc toàn bộ các Worker bị tạm dừng để bảo trì, API tiếp nhận vẫn hoạt động bình thường và tiếp tục nạp message vào hàng đợi. Khi các Worker được khởi động lại, chúng sẽ tự động kéo các tác vụ tồn đọng ra xử lý mà không làm mất mát bất kỳ dữ liệu nào của người dùng.

### 2.4.2. Dàn phẳng Tải trọng và Đệm Lưu lượng (Load Leveling)
Trong thực tế vận hành, lưu lượng truy cập của người dùng luôn có xu hướng biến động mạnh và xuất hiện các đợt tăng tải đột biến (*Traffic Spikes*). Cơ sở dữ liệu và các hệ thống xử lý phía sau luôn có một giới hạn công suất tối đa.

Message Queue hoạt động như một **"hồ điều hòa thủy điện"**:
* Khi lưu lượng tăng đột ngột gấp hàng chục lần, thay vì để xung lực này đánh thẳng vào hệ thống gây sập máy chủ, hàng đợi sẽ hấp thụ và giữ lại toàn bộ các message an toàn trong bộ đệm (*Buffer*).
* Các Worker phía sau sẽ đều đặn rút các message ra xử lý theo đúng công suất tối ưu của mình (ví dụ: ổn định ở mức 200 tác vụ/giây). Nhờ đó, hệ thống Backend luôn được bảo vệ an toàn, tránh hoàn toàn nguy cơ quá tải cục bộ.

![Hình 2.13: Cơ chế Dàn phẳng tải trọng (Load Leveling) thông qua bộ đệm Message Queue](file:///d:/chuyendektpm/images/hinh_2_13_dan_phang_tai_load_leveling.jpg)
*Hình 2.13: Cơ chế Dàn phẳng tải trọng (Load Leveling) thông qua bộ đệm Message Queue.*

```
Lưu lượng Client (Tăng vọt đột biến)         Hàng đợi Message Queue (Hồ đệm)      Worker xử lý (Đều đặn, ổn định)
                ▲                                                                               ▲
                │   ┌───┐                                                                       │
                │  ┌┘   └┐                           ┌─────────────┐                            │  ┌───────────────┐
                │ ┌┘     └┐                ───>      │ ▓▓▓▓▓▓▓▓▓▓▓ │                  ───>      │  │               │
                │┌┘       └┐                         └─────────────┘                            │  └───────────────┘
                └───────────►                                                                   └──────────────────►
```

### 2.4.3. Khả năng Chịu lỗi và Tính Bền vững (Resilience & Fault Tolerance)
Message Queue cung cấp các cơ chế bảo vệ tính toàn vẹn dữ liệu ở cấp độ cao nhất:
* **Cơ chế xác nhận xử lý (Consumer Acknowledgement - ACK):** Khi Worker nhận message, Broker không xóa ngay mà đưa vào trạng thái chờ xác nhận. Chỉ khi Worker xử lý thành công toàn bộ nghiệp vụ và gửi tín hiệu ACK về, Broker mới chính thức xóa message khỏi hàng đợi.
* **Tự động tái phân phối (Automatic Re-queue):** Nếu Worker gặp sự cố tắt nguồn (`Crash / Out-Of-Memory`) hoặc mất kết nối mạng giữa chừng khi chưa kịp gửi ACK, Broker sẽ phát hiện và ngay lập tức trả message đó về hàng đợi để phân phối cho một Worker lành lặn khác xử lý lại.
* **Lưu trữ bền vững (Disk Persistence):** Các Message Broker cho phép cấu hình ghi message xuống đĩa cứng, bảo đảm các tác vụ đang chờ xử lý không bị mất ngay cả khi máy chủ Broker bị khởi động lại đột ngột.

---

## 2.5. Tóm tắt Chương 2

Chương 2 đã xây dựng cơ sở lý thuyết vững chắc cho đề tài bằng cách làm rõ mô hình xử lý tác vụ trong hệ thống Backend và chỉ ra 3 nguyên nhân cốt lõi khiến giao tiếp đồng bộ trở thành nút thắt cổ chai khi hệ thống chịu tải cao (ràng buộc thời gian, khóa luồng và sập dây chuyền). 

Từ đó, chương phân tích cơ chế giải quyết của Message Queue thông qua kiến trúc Producer – Broker – Consumer, làm rõ hai mô hình truyền tin nền tảng (Point-to-Point và Pub/Sub), cùng ba giá trị trụ cột không thể thay thế đối với kiến trúc Backend: **Khử khớp nối (Decoupling)**, **Dàn phẳng tải trọng (Load Leveling)** và **Nâng cao khả năng chịu lỗi (Resilience)**.

Sau khi đã xác định Message Queue là giải pháp tất yếu để giải quyết bài toán tải của Backend, vấn đề tiếp theo đặt ra là: *Trong bức tranh công nghệ hiện đại, hai hệ thống messaging tiêu biểu là RabbitMQ và Apache Kafka có kiến trúc, cơ chế hoạt động và mô hình xử lý khác nhau như thế nào, và đâu là giải pháp phù hợp nhất cho bài toán này?* Nội dung so sánh và đối chuẩn công nghệ sẽ được đi sâu nghiên cứu trong **Chương 3**.
