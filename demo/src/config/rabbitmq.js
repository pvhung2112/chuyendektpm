const amqp = require('amqplib');
const { broadcast } = require('../services/sseService');
const walletService = require('../services/walletService');
const { appendOrderLog } = require('./kafka');

const RABBIT_URL = 'amqps://ohemaznj:Wrm17ng33aCW-rhCGeqQPFbUGceIbJBc@capybara.lmq.cloudamqp.com/ohemaznj';
let channel = null;
let isConnecting = false;

async function initRabbitMQ() {
    if (isConnecting) return;
    isConnecting = true;
    try {
        const conn = await amqp.connect(RABBIT_URL);
        isConnecting = false;

        conn.on('error', (err) => {
            console.error('[RabbitMQ Connection Error]', err.message);
        });
        conn.on('close', () => {
            console.warn('[RabbitMQ Connection Closed] Mất kết nối, sẽ kết nối lại sau 5s...');
            channel = null;
            setTimeout(initRabbitMQ, 5000);
        });

        channel = await conn.createChannel();
        channel.on('error', (err) => {
            console.error('[RabbitMQ Channel Error]', err.message);
        });


        await channel.deleteQueue('q_payment').catch(() => {});
        await channel.deleteQueue('q_notification').catch(() => {});
        await channel.deleteQueue('q_dlq').catch(() => {});
        await channel.deleteQueue('q_vip').catch(() => {});
        await channel.deleteQueue('q_normal').catch(() => {});

        await channel.assertExchange('simple_ex', 'direct', { durable: false });
        await channel.assertExchange('payment_dlx', 'fanout', { durable: false });

        // 1. Dead Letter Queue: Hứng các đơn thanh toán thất bại
        await channel.assertQueue('q_dlq', { durable: false });
        await channel.bindQueue('q_dlq', 'payment_dlx', '');

        // 2. Queue Trừ Tiền Ví (gắn Dead Letter Exchange nếu thiếu tiền)
        await channel.assertQueue('q_payment', {
            durable: false,
            arguments: { 'x-dead-letter-exchange': 'payment_dlx' }
        });
        await channel.bindQueue('q_payment', 'simple_ex', 'order');

        // 3. Queue Bắn Thông Báo (nhận tác vụ từ kết quả thanh toán)
        await channel.assertQueue('q_notification', { durable: false });

        // WORKER 1: XỬ LÝ TRỪ TIỀN VÍ (Payment Worker)
        channel.consume('q_payment', async (msg) => {
            if (!msg) return;
            const data = JSON.parse(msg.content.toString());
            
            // Giả lập thời gian kết nối ví / ngân hàng (1 giây)
            await new Promise(r => setTimeout(r, 1000));

            const deductRes = walletService.tryDeduct(data.price);
            if (deductRes.success) {
                // Đủ tiền -> Trừ ví thành công -> Gửi ACK (Xóa khỏi RAM)
                channel.ack(msg);
                const resultData = {
                    ...data,
                    isError: false,
                    balance: deductRes.balance,
                    balanceFormatted: deductRes.balanceFormatted,
                    note: `Trừ ví thành công: -${data.priceFormatted} (Số dư còn: ${deductRes.balanceFormatted})`,
                    smsMessage: `Đã gửi SMS: Chúc mừng! Đơn hàng ${data.name} thành công (Đã trừ ${data.priceFormatted})`
                };
                broadcast('rabbit_payment_ack', resultData);

                // Phát sinh công việc gửi SMS thành công sang Hàng Đợi Thông Báo
                broadcast('rabbit_new_notify', {
                    id: data.id,
                    name: data.name,
                    status: 'success',
                    label: `SMS: ${data.name} (Thành công)`
                });
                channel.sendToQueue('q_notification', Buffer.from(JSON.stringify(resultData)));

                // Đồng thời ghi nhận vào Kafka
                try {
                    const offset = await appendOrderLog(resultData);
                    resultData.offset = offset;
                    broadcast('kafka_new', resultData);
                } catch (kErr) {
                    console.error('[Kafka Error]', kErr.message);
                }
            } else {
                // Không đủ tiền -> Gửi NACK -> Bắn sang Dead Letter Queue (DLQ)
                channel.nack(msg, false, false);
                const resultData = {
                    ...data,
                    isError: true,
                    balance: deductRes.balance,
                    balanceFormatted: deductRes.balanceFormatted,
                    note: `Số dư không đủ (Cần ${data.priceFormatted}, ví còn ${deductRes.balanceFormatted})`,
                    smsMessage: `Đã gửi SMS CẢNH BÁO: Đơn ${data.name} thất bại vì ví không đủ tiền!`
                };
                broadcast('rabbit_payment_nack', resultData);

                // Phát sinh công việc gửi SMS cảnh báo thiếu tiền sang Hàng Đợi Thông Báo
                broadcast('rabbit_new_notify', {
                    id: data.id,
                    name: data.name,
                    status: 'error',
                    label: `SMS: Cảnh báo ${data.name} (Thiếu tiền ví)`
                });
                channel.sendToQueue('q_notification', Buffer.from(JSON.stringify(resultData)));

                // Đồng thời ghi nhận sự cố vào Kafka để phục vụ đối soát
                try {
                    const offset = await appendOrderLog(resultData);
                    resultData.offset = offset;
                    broadcast('kafka_new', resultData);
                } catch (kErr) {
                    console.error('[Kafka Error]', kErr.message);
                }
            }
        });

        // WORKER 2: XỬ LÝ GỬI THÔNG BÁO (Notification Worker)
        channel.consume('q_notification', async (msg) => {
            if (!msg) return;
            const data = JSON.parse(msg.content.toString());

            // Giả lập thời gian gửi SMS qua nhà mạng viễn thông (1 giây)
            await new Promise(r => setTimeout(r, 1000));

            channel.ack(msg); // Xóa khỏi RAM
            broadcast('rabbit_notify_ack', {
                ...data,
                message: data.smsMessage || `Đã gửi SMS cho đơn ${data.name}`
            });
        });

        console.log('[RabbitMQ Config] Đã thiết lập Hàng Đợi Trừ Tiền & Hàng Đợi Thông Báo.');
    } catch (err) {
        console.error('[RabbitMQ Config] Lỗi kết nối:', err.message);
    }
}

function publishOrder(orderData) {
    if (!channel) throw new Error('RabbitMQ channel chưa sẵn sàng');
    channel.publish('simple_ex', 'order', Buffer.from(JSON.stringify(orderData)));
}

module.exports = {
    initRabbitMQ,
    publishOrder
};
