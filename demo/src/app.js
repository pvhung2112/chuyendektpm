const express = require('express');
const path = require('path');
const { registerClient, removeClient } = require('./services/sseService');
const { createOrder, replayOrders, rechargeWallet, getBalance, clearLogs } = require('./controllers/orderController');

const app = express();

app.use(express.json());
app.use(express.static(path.join(__dirname, '../public')));

// Server-Sent Events Endpoint
app.get('/events', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    registerClient(res);

    req.on('close', () => {
        removeClient(res);
    });
});

// APIs
app.post('/api/orders', createOrder);
app.post('/api/orders/replay', replayOrders);
app.post('/api/orders/clear-logs', clearLogs);
app.get('/api/wallet/balance', getBalance);
app.post('/api/wallet/recharge', rechargeWallet);

module.exports = app;
