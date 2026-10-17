process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1';
const { Kafka, logLevel } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'demo-modular-client',
    brokers: ['127.0.0.1:9092', 'localhost:9092'],
    connectionTimeout: 5000,
    logLevel: logLevel.NOTHING
});

let topicVersion = 1;
let topic = `order-audit-log-v${topicVersion}`;
let producer = null;

async function initKafka(retries = 8, delay = 1500) {
    for (let i = 1; i <= retries; i++) {
        try {
            const admin = kafka.admin();
            await admin.connect();
            const existingTopics = await admin.listTopics();
            if (!existingTopics.includes(topic)) {
                await admin.createTopics({
                    topics: [{ topic, numPartitions: 1 }]
                });
            }
            await admin.disconnect();

            producer = kafka.producer();
            await producer.connect();
            console.log(`[Kafka Config] Kết nối 127.0.0.1:9092 & Topic [${topic}] thành công.`);
            return;
        } catch (err) {
            if (i < retries) {
                console.log(`[Kafka Config] Đang đợi Kafka sẵn sàng, thử lại lần ${i}/${retries}...`);
                await new Promise(r => setTimeout(r, delay));
            } else {
                console.error('[Kafka Config] Lỗi kết nối:', err.message);
            }
        }
    }
}

async function resetKafkaTopic() {
    try {
        topicVersion++;
        topic = `order-audit-log-v${topicVersion}`;
        const admin = kafka.admin();
        await admin.connect();
        await admin.createTopics({
            topics: [{ topic, numPartitions: 1 }]
        });
        await admin.disconnect();
        console.log(`[Kafka Config] Đã tạo Topic mới [${topic}] bắt đầu từ Offset 0.`);
        return { success: true, topic };
    } catch (err) {
        console.error('[Kafka Config] Lỗi reset topic:', err.message);
        return { success: false, error: err.message, topic };
    }
}

// 1. GHI NỐI ĐUÔI VÀO COMMIT LOG TRÊN ĐĨA CỨNG (Append-Only)
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

// 2. KAFKA CONSUMER THẬT: ĐỌC TỪNG BYTE TỪ FILE .LOG TRÊN ĐĨA CỨNG (Seek Offset 0)
async function replayFromBeginning(onMessage, onDone) {
    const consumer = kafka.consumer({ groupId: `audit-consumer-${Date.now()}` });
    await consumer.connect();
    // fromBeginning: true -> Kéo Offset về 0 để đọc từ đầu đĩa cứng
    await consumer.subscribe({ topic, fromBeginning: true });

    let count = 0;
    await consumer.run({
        eachMessage: async ({ message }) => {
            const data = JSON.parse(message.value.toString());
            data.offset = String(message.offset);
            if (onMessage) onMessage(data);
            count++;
        }
    });

    // Cho consumer đọc xong toàn bộ đĩa cứng rồi ngắt kết nối an toàn
    setTimeout(async () => {
        try { await consumer.disconnect(); } catch (e) {}
        if (onDone) onDone(count);
    }, 2500);
}

module.exports = {
    initKafka,
    appendOrderLog,
    replayFromBeginning,
    resetKafkaTopic,
    getTopic: () => topic
};
