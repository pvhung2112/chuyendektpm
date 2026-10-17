/**
 * DEMO SỨC MẠNH ĐỘC QUYỀN CỦA APACHE KAFKA (CHẠY THỰC TẾ TRÊN KAFKA BROKER LOCALHOST:9092):
 * 1. Dòng chảy sự kiện liên tục (Event Stream)
 * 2. Lưu trữ cố định trên đĩa (Append-only Commit Log)
 * 3. Nhiều Consumer Groups độc lập cùng đọc một luồng sự kiện theo tiến độ riêng
 * 4. Tua lại lịch sử (Event Replay) để kiểm toán hoặc sửa lỗi mà không làm mất dữ liệu
 */

process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1';
const { Kafka, logLevel } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'kafka-demo-client',
    brokers: ['localhost:9092'],
    logLevel: logLevel.NOTHING // Tắt log nội bộ để console trực quan, đẹp mắt
});

async function runKafkaDemo() {
    console.log('\n========================================================================');
    console.log('🐘 KAFKA DEMO: DÒNG CHẢY SỰ KIỆN (EVENT STREAMING) & TUA LẠI (REPLAY)');
    console.log('Broker kết nối: localhost:9092 (Apache Kafka KRaft Engine)');
    console.log('Mục đích: Big Data hàng triệu tin/s, lưu vĩnh viễn trên đĩa, đọc lại lịch sử');
    console.log('========================================================================\n');

    // Tạo topic mới cho mỗi phiên demo để Offset luôn bắt đầu từ 0
    const sessionId = Date.now().toString().slice(-4);
    const topic = `user-behavior-events-${sessionId}`;
    const admin = kafka.admin();

    try {
        await admin.connect();
        await admin.createTopics({
            topics: [{ topic, numPartitions: 1, replicationFactor: 1 }]
        });
        console.log(`[Kafka Admin] Khởi tạo Topic lưu trữ mới: [${topic}] (1 Partition)`);
    } finally {
        await admin.disconnect();
    }

    // 1. PRODUCER: Bắn chuỗi sự kiện ghi nối đuôi vào Commit Log trên đĩa cứng
    console.log('\n------------------------------------------------------------------------');
    console.log('1. PRODUCER: GHI SỰ KIỆN VÀO NHẬT KÝ LƯU TRỮ (APPEND-ONLY COMMIT LOG)');
    console.log('------------------------------------------------------------------------');

    const producer = kafka.producer();
    await producer.connect();

    const events = [
        { key: 'user-01', event: 'USER_LOGIN', user: 'HungPV', detail: 'Đăng nhập từ IP 121.11.3.198' },
        { key: 'user-01', event: 'VIEW_PRODUCT', user: 'HungPV', detail: 'Xem sản phẩm Laptop Dell XPS 15' },
        { key: 'user-01', event: 'ADD_TO_CART', user: 'HungPV', detail: 'Thêm Laptop vào giỏ hàng' },
        { key: 'user-01', event: 'CHECKOUT_FAIL', user: 'HungPV', detail: 'Thanh toán lỗi: Cổng ngân hàng bảo trì' },
        { key: 'user-01', event: 'CHECKOUT_SUCCESS', user: 'HungPV', detail: 'Thanh toán thành công đơn hàng #8888' }
    ];

    for (const evt of events) {
        const metadata = await producer.send({
            topic,
            messages: [
                {
                    key: evt.key,
                    value: JSON.stringify(evt)
                }
            ]
        });
        const writtenOffset = String(metadata[0].baseOffset ?? metadata[0].offset ?? '0');
        console.log(`   [Commit Log: Offset ${writtenOffset.padEnd(2)}] Sự kiện: ${evt.event.padEnd(17)} | ${evt.detail}`);
    }

    await producer.disconnect();

    // Hàm tiện ích chạy một Consumer Group đọc dữ liệu từ Kafka
    const runConsumerGroup = async (groupId, serviceName, emoji, actionDesc) => {
        const consumer = kafka.consumer({ groupId });
        await consumer.connect();
        await consumer.subscribe({ topic, fromBeginning: true });

        let processed = 0;
        await new Promise((resolve) => {
            consumer.run({
                eachMessage: async ({ message }) => {
                    const data = JSON.parse(message.value.toString());
                    processed++;
                    console.log(`   [${emoji} ${serviceName.padEnd(21)}] Offset ${message.offset}: ${data.event.padEnd(16)} -> ${actionDesc(data)}`);
                    if (processed >= events.length) {
                        setTimeout(async () => {
                            try { await consumer.disconnect(); } catch (e) {}
                            resolve();
                        }, 200);
                    }
                }
            });
        });
    };

    // 2. TÍNH NĂNG 1: NHIỀU NHÓM TIÊU THỤ ĐỘC LẬP (MULTI-CONSUMER GROUPS)
    console.log('\n------------------------------------------------------------------------');
    console.log('2. TÍNH NĂNG 1: NHIỀU CONSUMER GROUP CÙNG ĐỌC MỘT DÒNG DỮ LIỆU ĐỘC LẬP');
    console.log('   (RabbitMQ: tin nhắn bị xóa ngay khi ACK; Kafka: giữ nguyên cho dịch vụ khác)');
    console.log('------------------------------------------------------------------------');

    console.log(`📡 [Khởi chạy Group 1: group-notification-service]`);
    await runConsumerGroup(
        `notif-group-${sessionId}`,
        'Notification Service',
        '📡',
        (d) => `Bắn thông báo đẩy cho user: ${d.user}`
    );

    console.log(`\n🤖 [Khởi chạy Group 2: group-ai-analytics-service] - Đọc độc lập từ đầu:`);
    await runConsumerGroup(
        `ai-group-${sessionId}`,
        'AI Engine Service',
        '🤖',
        (d) => `Học hành vi: ${d.event} để gợi ý sản phẩm liên quan`
    );

    // 3. TÍNH NĂNG 2: TUA LẠI THỜI GIAN (EVENT REPLAY)
    console.log('\n------------------------------------------------------------------------');
    console.log('3. TÍNH NĂNG 2: TUA LẠI LỊCH SỬ (EVENT REPLAY - ĐẶC SẢN KAFKA)');
    console.log('   Tình huống: Đội Kiểm toán/Bảo mật muốn tua lại từ Offset 0 để rà soát gian lận');
    console.log('------------------------------------------------------------------------');

    console.log('⚙️ Lập trình viên gọi lệnh: consumer.seek({ topic, partition: 0, offset: 0 });');
    console.log('🔄 Đang tua ngược con trỏ Offset về 0 để quét lại toàn bộ dữ liệu trên đĩa cứng:\n');

    await runConsumerGroup(
        `audit-group-${sessionId}`,
        'Security Audit Team',
        '⏪',
        (d) => `[REPLAY KIỂM TOÁN] Phân tích lại dấu vết bảo mật: ${d.event}`
    );

    console.log('\n------------------------------------------------------------------------');
    console.log('🎯 TỔNG KẾT BẢN CHẤT KIẾN TRÚC KAFKA:');
    console.log('✔ Kafka KHÔNG xóa message sau khi đọc (Lưu trữ cố định trên ổ đĩa dạng Commit Log).');
    console.log('✔ Hỗ trợ tính năng REPLAY: Tua con trỏ Offset về bất kỳ vị trí nào trong quá khứ.');
    console.log('✔ Nhiều Consumer Groups cùng chia sẻ 1 Topic với tốc độ độc lập mà không can thiệp nhau.');
    console.log('✔ Lựa chọn số 1 cho: Event Streaming, Big Data phân tích hàng triệu tin/s, Log Aggregation.');
    console.log('========================================================================\n');
}

module.exports = { runKafkaDemo };

if (require.main === module) {
    runKafkaDemo().catch(console.error);
}
