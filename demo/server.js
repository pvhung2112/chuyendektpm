const app = require('./src/app');
const { initRabbitMQ } = require('./src/config/rabbitmq');
const { initKafka } = require('./src/config/kafka');

const PORT = 4000;

async function bootstrap() {
    await initRabbitMQ();
    await initKafka();

    app.listen(PORT, () => {
        console.log(`http://localhost:${PORT}`);
    });
}

bootstrap();
