/**
 * INTERACTIVE COMPARISON DASHBOARD SERVER: RABBITMQ VS APACHE KAFKA
 * Cung cấp API và Server-Sent Events (SSE) để điều khiển và hiển thị trực quan
 */

const express = require('express');
const path = require('path');
const amqp = require('amqplib');
process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1';
const { Kafka, logLevel } = require('kafkajs');

const app = express();
const PORT = 3000;

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// -------------------------------------------------------------
// SERVER-SENT EVENTS (SSE) BROADCASTER
// -------------------------------------------------------------
let sseClients = [];

function broadcast(eventType, data) {
    const payload = `event: ${eventType}\ndata: ${JSON.stringify(data)}\n\n`;
    sseClients.forEach(client => client.res.write(payload));
}

app.get('/api/stream', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders();

    const clientId = Date.now();
    const newClient = { id: clientId, res };
    sseClients.push(newClient);

    // Gửi trạng thái ban đầu
    res.write(`event: connected\ndata: ${JSON.stringify({ status: 'ok', time: new Date() })}\n\n`);

    req.on('close', () => {
        sseClients = sseClients.filter(c => c.id !== clientId);
    });
});

// -------------------------------------------------------------
// 1. RABBITMQ TOPOLOGY & WORKERS
// -------------------------------------------------------------
const CLOUD_AMQP_URL = 'amqps://ohemaznj:Wrm17ng33aCW-rhCGeqQPFbUGceIbJBc@capybara.lmq.cloudamqp.com/ohemaznj';
let rabbitChannel = null;
let rabbitConnection = null;

const EXCHANGE = 'web_topic_task_exchange';
const DLX_EXCHANGE = 'web_dead_letter_exchange';
const DLQ_QUEUE = 'web_error_poison_tasks_dlq';
const VIP_QUEUE = 'web_queue_priority_vip';
const NORMAL_QUEUE = 'web_queue_normal_tasks';

async function initRabbitMQ() {
    try {
        rabbitConnection = await amqp.connect(CLOUD_AMQP_URL);
        rabbitChannel = await rabbitConnection.createChannel();

        // 1. Tạo Dead Letter Exchange & Queue
        await rabbitChannel.assertExchange(DLX_EXCHANGE, 'fanout', { durable: true });
        await rabbitChannel.assertQueue(DLQ_QUEUE, { durable: true });
        await rabbitChannel.bindQueue(DLQ_QUEUE, DLX_EXCHANGE, '');

        // 2. Tạo Main Exchange
        await rabbitChannel.assertExchange(EXCHANGE, 'topic', { durable: true });

        // 3. Tạo VIP Queue kèm DLX
        await rabbitChannel.assertQueue(VIP_QUEUE, {
            durable: true,
            arguments: {
                'x-dead-letter-exchange': DLX_EXCHANGE,
                'x-dead-letter-routing-key': DLQ_QUEUE
            }
        });
        await rabbitChannel.bindQueue(VIP_QUEUE, EXCHANGE, 'task.vip');

        // 4. Tạo Normal Queue
        await rabbitChannel.assertQueue(NORMAL_QUEUE, { durable: true });
        await rabbitChannel.bindQueue(NORMAL_QUEUE, EXCHANGE, 'task.normal');

        // Xóa sạch message cũ trước khi vào dashboard
        await rabbitChannel.purgeQueue(VIP_QUEUE);
        await rabbitChannel.purgeQueue(NORMAL_QUEUE);
        await rabbitChannel.purgeQueue(DLQ_QUEUE);

        console.log('✅ [RabbitMQ] Kết nối CloudAMQP & thiết lập Topology thành công!');
        startRabbitWorkers();
    } catch (err) {
        console.error('❌ [RabbitMQ] Lỗi khởi tạo:', err.message);
    }
}

function startRabbitWorkers() {
    if (!rabbitChannel) return;

    // Worker VIP
    rabbitChannel.consume(VIP_QUEUE, async (msg) => {
        if (!msg) return;
        const task = JSON.parse(msg.content.toString());
        broadcast('rabbit_worker_pickup', { worker: 'VIP', task });

        await new Promise(r => setTimeout(r, 900));

        if (task.isPoison) {
            // Task hỏng -> NACK, không requeue -> vào DLQ
            rabbitChannel.nack(msg, false, false);
            broadcast('rabbit_worker_nack', { worker: 'VIP', task, target: 'DLQ' });
        } else {
            // Task hợp lệ -> ACK -> Xóa khỏi RAM
            rabbitChannel.ack(msg);
            broadcast('rabbit_worker_ack', { worker: 'VIP', task });
        }
    });

    // Worker Normal
    rabbitChannel.consume(NORMAL_QUEUE, async (msg) => {
        if (!msg) return;
        const task = JSON.parse(msg.content.toString());
        broadcast('rabbit_worker_pickup', { worker: 'NORMAL', task });

        await new Promise(r => setTimeout(r, 700));

        rabbitChannel.ack(msg);
        broadcast('rabbit_worker_ack', { worker: 'NORMAL', task });
    });
}

// -------------------------------------------------------------
// 2. APACHE KAFKA (KRAFT LOCALHOST:9092)
// -------------------------------------------------------------
const kafka = new Kafka({
    clientId: 'web-dashboard-client',
    brokers: ['localhost:9092'],
    logLevel: logLevel.NOTHING
});

let kafkaTopic = `user-behavior-web-${Date.now().toString().slice(-4)}`;
let kafkaProducer = null;

async function initKafka() {
    try {
        const admin = kafka.admin();
        await admin.connect();
        await admin.createTopics({
            topics: [{ topic: kafkaTopic, numPartitions: 1, replicationFactor: 1 }]
        });
        await admin.disconnect();

        kafkaProducer = kafka.producer();
        await kafkaProducer.connect();
        console.log(`✅ [Kafka] Kết nối localhost:9092 & khởi tạo Topic: [${kafkaTopic}] thành công!`);
    } catch (err) {
        console.error('❌ [Kafka] Lỗi khởi tạo:', err.message);
    }
}

// -------------------------------------------------------------
// REST APIS
// -------------------------------------------------------------

// RabbitMQ: Bắn Task
app.post('/api/rabbitmq/publish', async (req, res) => {
    try {
        const { type, title } = req.body;
        const isPoison = type === 'poison';
        const routingKey = type === 'normal' ? 'task.normal' : 'task.vip';
        const taskId = `TASK-${Math.floor(1000 + Math.random() * 9000)}`;

        const payload = {
            id: taskId,
            title: title || (isPoison ? 'Dữ liệu Corrupted Null Pointer' : (type === 'vip' ? 'Đơn hàng VIP Kim Cương' : 'Đơn hàng Thường')),
            type,
            isPoison,
            routingKey,
            createdAt: new Date().toLocaleTimeString()
        };

        rabbitChannel.publish(EXCHANGE, routingKey, Buffer.from(JSON.stringify(payload)), {
            persistent: true
        });

        broadcast('rabbit_task_published', payload);
        res.json({ success: true, task: payload });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

// Kafka: Ghi Sự Kiện vào Commit Log
app.post('/api/kafka/produce', async (req, res) => {
    try {
        const { eventType, user, detail } = req.body;
        const eventData = {
            id: `evt-${Date.now().toString().slice(-5)}`,
            eventType: eventType || 'VIEW_PRODUCT',
            user: user || 'HungPV',
            detail: detail || 'Xem chi tiết sản phẩm Laptop Dell XPS 15',
            timestamp: new Date().toLocaleTimeString()
        };

        const meta = await kafkaProducer.send({
            topic: kafkaTopic,
            messages: [{ key: 'user-01', value: JSON.stringify(eventData) }]
        });

        const offset = String(meta[0].baseOffset ?? meta[0].offset ?? '0');
        eventData.offset = offset;

        broadcast('kafka_event_produced', eventData);
        res.json({ success: true, event: eventData });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

// Kafka: Đọc Multi-Consumer
app.post('/api/kafka/consume', async (req, res) => {
    try {
        const { groupType } = req.body; // 'notification' hoặc 'ai'
        const groupName = groupType === 'notification' ? 'group-notification' : 'group-ai-analytics';
        const consumer = kafka.consumer({ groupId: `${groupName}-${Date.now()}` });
        
        await consumer.connect();
        await consumer.subscribe({ topic: kafkaTopic, fromBeginning: true });

        consumer.run({
            eachMessage: async ({ message }) => {
                const data = JSON.parse(message.value.toString());
                broadcast('kafka_consumer_read', {
                    group: groupType,
                    groupName,
                    offset: message.offset,
                    event: data
                });
            }
        });

        // Cho chạy đọc 2 giây rồi ngắt để demo
        setTimeout(async () => {
            try { await consumer.disconnect(); } catch (e) {}
        }, 2500);

        res.json({ success: true, group: groupType });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

// Kafka: Tua lại lịch sử (Replay) từ Offset 0
app.post('/api/kafka/replay', async (req, res) => {
    try {
        const consumer = kafka.consumer({ groupId: `audit-replay-${Date.now()}` });
        await consumer.connect();
        await consumer.subscribe({ topic: kafkaTopic, fromBeginning: true });

        broadcast('kafka_replay_start', { message: 'Đang tua con trỏ Offset về 0...' });

        consumer.run({
            eachMessage: async ({ message }) => {
                const data = JSON.parse(message.value.toString());
                broadcast('kafka_replay_read', {
                    offset: message.offset,
                    event: data
                });
            }
        });

        setTimeout(async () => {
            try { await consumer.disconnect(); } catch (e) {}
            broadcast('kafka_replay_done', {});
        }, 3000);

        res.json({ success: true });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

// Reset toàn bộ
app.post('/api/reset', async (req, res) => {
    try {
        if (rabbitChannel) {
            await rabbitChannel.purgeQueue(VIP_QUEUE);
            await rabbitChannel.purgeQueue(NORMAL_QUEUE);
            await rabbitChannel.purgeQueue(DLQ_QUEUE);
        }
        kafkaTopic = `user-behavior-web-${Date.now().toString().slice(-4)}`;
        const admin = kafka.admin();
        await admin.connect();
        await admin.createTopics({
            topics: [{ topic: kafkaTopic, numPartitions: 1, replicationFactor: 1 }]
        });
        await admin.disconnect();

        broadcast('system_reset', { topic: kafkaTopic });
        res.json({ success: true, newTopic: kafkaTopic });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
});

// Khởi chạy server
async function bootstrap() {
    await initRabbitMQ();
    await initKafka();

    app.listen(PORT, () => {
        console.log(`\n============================================================`);
        console.log(`🚀 WEB DASHBOARD SẴN SÀNG TẠI: http://localhost:${PORT}`);
        console.log(`============================================================\n`);
    });
}

bootstrap();
