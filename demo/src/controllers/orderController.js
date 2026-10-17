const { publishOrder } = require('../config/rabbitmq');
const { replayFromBeginning, topic } = require('../config/kafka');
const { broadcast } = require('../services/sseService');
const walletService = require('../services/walletService');

let orderIndex = 1;

async function createOrder(req, res) {
    try {
        const { type } = req.body; // 'normal' (250.000 đ) hoặc 'vip' (1.500.000 đ)
        const price = type === 'vip' ? 1500000 : 250000;
        const name = type === 'vip' ? `Đơn VIP ${orderIndex++}` : `Đơn Thường ${orderIndex++}`;

        const orderData = {
            id: `ORD-${Date.now().toString().slice(-4)}`,
            name,
            type,
            price,
            priceFormatted: `${price.toLocaleString('vi-VN')} đ`,
            time: new Date().toLocaleTimeString()
        };

        // Gửi đơn hàng vào Hàng Đợi Trừ Tiền
        publishOrder(orderData);
        
        // Báo giao diện hiển thị đơn vào Hàng Đợi Trừ Tiền
        broadcast('rabbit_new_order', orderData);

        res.json({ success: true, order: orderData });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
}

function replayOrders(req, res) {
    broadcast('kafka_replay_start', { message: `Kafka Consumer đang mở file .log trên đĩa cứng từ Offset 0...` });
    
    replayFromBeginning(
        (data) => {
            broadcast('kafka_replay_step', data);
        },
        (totalCount) => {
            broadcast('kafka_replay_done', { total: totalCount });
        }
    );

    res.json({ success: true });
}

function rechargeWallet(req, res) {
    const newBalance = walletService.recharge();
    broadcast('balance_update', {
        balance: newBalance,
        balanceFormatted: walletService.getBalanceFormatted()
    });
    res.json({ success: true, balance: newBalance, balanceFormatted: walletService.getBalanceFormatted() });
}

function getBalance(req, res) {
    res.json({
        balance: walletService.getBalance(),
        balanceFormatted: walletService.getBalanceFormatted()
    });
}

async function clearLogs(req, res) {
    try {
        const { resetKafkaTopic } = require('../config/kafka');
        const resetRes = await resetKafkaTopic();
        orderIndex = 1;
        const newBalance = walletService.recharge();
        broadcast('balance_update', {
            balance: newBalance,
            balanceFormatted: walletService.getBalanceFormatted()
        });
        broadcast('logs_cleared', { 
            message: `Đã làm sạch sổ cái! Topic mới [${resetRes.topic}] bắt đầu từ Offset 0.`,
            topic: resetRes.topic
        });
        res.json({ 
            success: true, 
            topic: resetRes.topic, 
            balanceFormatted: walletService.getBalanceFormatted() 
        });
    } catch (err) {
        res.status(500).json({ success: false, error: err.message });
    }
}

module.exports = {
    createOrder,
    replayOrders,
    rechargeWallet,
    getBalance,
    clearLogs
};
