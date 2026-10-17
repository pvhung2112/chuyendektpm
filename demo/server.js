const app = require('./src/app');
const { initRabbitMQ } = require('./src/config/rabbitmq');
const { initKafka } = require('./src/config/kafka');

const PORT = 3001;

async function bootstrap() {
    await initRabbitMQ();
    await initKafka();

    const server = app.listen(PORT, () => {
        console.log(` http://localhost:${PORT}`);
    });

    server.on('error', (err) => {
        if (err.code === 'EADDRINUSE') {
            console.error(`\nCổng ${PORT} đang bị chiếm giữ!`);
            
        } else {
            console.error('Lỗi server:', err.message);
        }
    });
}

bootstrap();
