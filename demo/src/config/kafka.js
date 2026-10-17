process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1';
const { Kafka, logLevel } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'demo-modular-client',
    brokers: ['localhost:9092'],
    logLevel: logLevel.NOTHING
});

const topic = `order-audit-log-${Date.now().toString().slice(-4)}`;
let producer = null;

async function initKafka() {
    try {
        const admin = kafka.admin();
        await admin.connect();
        await admin.createTopics({
            topics: [{ topic, numPartitions: 1 }]
        });
        await admin.disconnect();

        producer = kafka.producer();
        await producer.connect();
        console.log(`[Kafka Config] Kết nối localhost:9092 & Topic [${topic}] thành công.`);
    } catch (err) {
        console.error('[Kafka Config] Lỗi kết nối:', err.message);
    }
}

async function appendOrderLog(orderData) {
    if (!producer) throw new Error('Kafka producer chưa sẵn sàng');

    const meta = await producer.send({
        topic,
        messages: [
            {
                key: orderData.id,
                value: JSON.stringify({
                    id: orderData.id,
                    name: orderData.name,
                    type: orderData.type,
                    price: orderData.price,
                    priceFormatted: orderData.priceFormatted,
                    isError: orderData.isError,
                    status: orderData.isError ? 'Thất bại (Không đủ số dư ví)' : 'Thành công (Đã trừ tiền ví)',
                    note: orderData.note,
                    time: orderData.time
                })
            }
        ]
    });

    const offset = String(meta[0].baseOffset ?? meta[0].offset ?? '0');
    return offset;
}

module.exports = {
    initKafka,
    appendOrderLog
};
