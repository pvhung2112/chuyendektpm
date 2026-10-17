const { runRabbitMQDemo } = require('./1_rabbitmq_task_routing');
const { runKafkaDemo } = require('./2_kafka_event_streaming');

async function main() {
    console.log(`
################################################################################
#  CHUYÊN ĐỀ KỸ THUẬT PHẦN MỀM: SO SÁNH TRỰC DIỆN MỤC ĐÍCH THIẾT KẾ            #
#          RABBITMQ (TASK ROUTING) VS APACHE KAFKA (EVENT STREAMING)           #
################################################################################
    `);

    // 1. Chạy Demo RabbitMQ
    await runRabbitMQDemo();

    // Dừng 1 giây cho dễ quan sát
    await new Promise(r => setTimeout(r, 1000));

    // 2. Chạy Demo Kafka
    await runKafkaDemo();

    console.log(`
================================================================================
🏆 KẾT LUẬN CUỐI CÙNG: KHÔNG PHẢI CÁI NÀO NHANH HƠN LÀ CHỌN!
================================================================================
1. Khi bài toán cần GIAO VIỆC RỜI RẠC & ĐỊNH TUYẾN THÔNG MINH (Task Queuing):
   -> CHỌN RABBITMQ (Gửi email, render file, xuất hóa đơn, cô lập tin lỗi DLQ).
   -> Lý do: Xong việc là xóa dọn dẹp bộ nhớ, điều hướng linh hoạt, cực kỳ nhẹ.

2. Khi bài toán cần LƯU DÒNG LỊCH SỬ SỰ KIỆN & PHÂN TÍCH DỮ LIỆU LỚN (Event Streaming):
   -> CHỌN APACHE KAFKA (Hành vi click chuột, phân tích AI, log giao dịch ngân hàng).
   -> Lý do: Lưu dữ liệu lâu dài, hỗ trợ tua lại lịch sử (Replay), chịu hàng triệu tin/s.
================================================================================
    `);
}

main().catch(err => console.error('Lỗi khi chạy demo:', err));
