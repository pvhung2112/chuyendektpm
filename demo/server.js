const app = require('./src/app');
const { initRabbitMQ } = require('./src/config/rabbitmq');
const { initKafka } = require('./src/config/kafka');

const PORT = 4000;

async function bootstrap() {
    await initRabbitMQ();
    await initKafka();

    const server = app.listen(PORT, () => {
        console.log(`\n========================================================`);
        console.log(`🚀 DEMO MODULAR SẴN SÀNG TẠI: http://localhost:${PORT}`);
        console.log(`========================================================\n`);
    });

    server.on('error', (err) => {
        if (err.code === 'EADDRINUSE') {
            console.error(`\n❌ Cổng ${PORT} đang bị chiếm giữ!`);
            console.error(`👉 Cách khắc phục: Tắt các cửa sổ terminal Node khác hoặc chạy lại.\n`);
        } else {
            console.error('Lỗi server:', err.message);
        }
    });
}

bootstrap();
