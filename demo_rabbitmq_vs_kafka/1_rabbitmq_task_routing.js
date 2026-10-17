/**
 * DEMO SỨC MẠNH ĐỘC QUYỀN CỦA RABBITMQ:
 * 1. Định tuyến thông minh theo Routing Key (Task Routing)
 * 2. Phân chia việc cho đúng Worker chuyên môn
 * 3. Cô lập tin nhắn lỗi vào Dead Letter Queue (DLQ)
 * 4. Vòng đời Task: Xong việc bấm ACK là dọn dẹp sạch sẽ
 */

const amqp = require('amqplib');

const RABBIT_URL = process.env.RABBIT_URL || 'amqps://ohemaznj:Wrm17ng33aCW-rhCGeqQPFbUGceIbJBc@capybara.lmq.cloudamqp.com/ohemaznj';

const EXCHANGE_NAME = 'app_task_exchange';
const DLX_NAME = 'app_dead_letter_exchange';
const DLQ_NAME = 'error_poison_tasks_dlq';

const QUEUE_VIP = 'queue_priority_vip';
const QUEUE_NORMAL = 'queue_normal_tasks';

async function runRabbitMQDemo() {
    console.log('\n========================================================================');
    console.log('🐇 RABBITMQ DEMO: ĐỊNH TUYẾN THEO TÁC VỤ (TASK ROUTING) & DEAD LETTER QUEUE');
    console.log('Mục đích: Giao đúng việc cho đúng thợ, cô lập lỗi và xong việc là dọn dẹp');
    console.log('========================================================================\n');

    const conn = await amqp.connect(RABBIT_URL);
    const ch = await conn.createChannel();

    // 1. Tạo Dead Letter Exchange (DLX) để chứa rác/lỗi
    await ch.assertExchange(DLX_NAME, 'direct', { durable: true });
    await ch.assertQueue(DLQ_NAME, { durable: true });
    await ch.bindQueue(DLQ_NAME, DLX_NAME, 'poison_key');

    // 2. Tạo Direct Exchange chính
    await ch.assertExchange(EXCHANGE_NAME, 'direct', { durable: true });

    // 3. Tạo Hàng đợi VIP (cho khách VIP) và Hàng đợi Thường
    await ch.assertQueue(QUEUE_VIP, {
        durable: true,
        arguments: { 'x-dead-letter-exchange': DLX_NAME, 'x-dead-letter-routing-key': 'poison_key' }
    });
    await ch.assertQueue(QUEUE_NORMAL, {
        durable: true,
        arguments: { 'x-dead-letter-exchange': DLX_NAME, 'x-dead-letter-routing-key': 'poison_key' }
    });

    // Ràng buộc quy tắc định tuyến:
    // Nhãn 'task.vip' -> Đi vào Queue VIP
    // Nhãn 'task.normal' -> Đi vào Queue Thường
    await ch.bindQueue(QUEUE_VIP, EXCHANGE_NAME, 'task.vip');
    await ch.bindQueue(QUEUE_NORMAL, EXCHANGE_NAME, 'task.normal');

    console.log('[*] Đã cấu hình xong hệ thống Định tuyến (Exchange) và Hàng đợi (Queues).');

    // 4. PRODUCER BẮN 3 TÁC VỤ KHÁC NHAU VÀO CÙNG 1 EXCHANGE:
    const task1 = { id: 'TASK-001', type: 'Xử lý đơn hàng khách VIP Diamond', content: 'Gửi quà tặng + SMS' };
    const task2 = { id: 'TASK-002', type: 'Đơn hàng thông thường', content: 'Gửi email xác nhận' };
    const task3 = { id: 'TASK-003', type: 'Tác vụ chứa mã độc/dữ liệu hỏng (Poison Message)', isPoison: true };

    console.log('\n📤 [Producer] Bắt đầu phát việc vào Exchange:');
    ch.publish(EXCHANGE_NAME, 'task.vip', Buffer.from(JSON.stringify(task1)));
    console.log('   ├── Đẩy Task-001 với Routing Key [task.vip] -> RabbitMQ tự điều hướng sang Queue VIP');

    ch.publish(EXCHANGE_NAME, 'task.normal', Buffer.from(JSON.stringify(task2)));
    console.log('   ├── Đẩy Task-002 với Routing Key [task.normal] -> RabbitMQ tự điều hướng sang Queue Thường');

    ch.publish(EXCHANGE_NAME, 'task.vip', Buffer.from(JSON.stringify(task3)));
    console.log('   └── Đẩy Task-003 (Dữ liệu hỏng) với Routing Key [task.vip]');

    // 5. WORKER VIP LÀM VIỆC:
    console.log('\n👷‍♂️ [Worker VIP] Đang đón nhận tác vụ từ [queue_priority_vip]...');
    await new Promise((resolve) => {
        let processed = 0;
        ch.consume(QUEUE_VIP, async (msg) => {
            if (!msg) return;
            const data = JSON.parse(msg.content.toString());
            processed++;

            if (data.isPoison) {
                console.log(`   ❌ [Worker VIP] Phát hiện Task hỏng: ${data.id}! Gửi NACK...`);
                // Từ chối và không requeue -> RabbitMQ tự ném sang Dead Letter Queue
                ch.nack(msg, false, false);
                console.log(`   ➡️ [Dead Letter Queue] Tin nhắn ${data.id} đã bị cô lập vào [${DLQ_NAME}] an toàn!`);
            } else {
                console.log(`   ✅ [Worker VIP] Đã xử lý xong: ${data.id} - "${data.type}"`);
                ch.ack(msg); // Xóa khỏi queue
                console.log(`   🗑️ [RabbitMQ] Đã xác nhận ACK -> Xóa ${data.id} khỏi bộ nhớ để dọn chỗ.`);
            }

            if (processed >= 2) resolve();
        });
    });

    // 6. WORKER THƯỜNG LÀM VIỆC:
    console.log('\n👷‍♂️ [Worker Thường] Đang đón nhận tác vụ từ [queue_normal_tasks]...');
    await new Promise((resolve) => {
        ch.consume(QUEUE_NORMAL, async (msg) => {
            if (!msg) return;
            const data = JSON.parse(msg.content.toString());
            console.log(`   ✅ [Worker Thường] Đã xử lý xong: ${data.id} - "${data.type}"`);
            ch.ack(msg);
            console.log(`   🗑️ [RabbitMQ] Đã xác nhận ACK -> Xóa ${data.id} khỏi bộ nhớ.`);
            resolve();
        });
    });

    console.log('\n🎯 TỔNG KẾT RABBITMQ:');
    console.log('✔ Tự động phân loại tác vụ về đúng hàng đợi theo chuyên môn (Routing Key).');
    console.log('✔ Có sẵn Dead Letter Queue để cô lập lỗi, không làm nghẽn luồng chính.');
    console.log('✔ Xong việc là giải phóng bộ nhớ, cực kỳ thích hợp cho các bài toán Task/Job Backend!');

    await ch.close();
    await conn.close();
}

module.exports = { runRabbitMQDemo };

if (require.main === module) {
    runRabbitMQDemo();
}
