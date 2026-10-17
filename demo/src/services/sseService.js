// Quản lý kết nối thời gian thực Server-Sent Events (SSE)
let clients = [];

function registerClient(res) {
    clients.push(res);
}

function removeClient(res) {
    clients = clients.filter(c => c !== res);
}

function broadcast(eventType, data) {
    const payload = `event: ${eventType}\ndata: ${JSON.stringify(data)}\n\n`;
    clients.forEach(c => {
        try {
            c.write(payload);
        } catch (e) {}
    });
}

module.exports = {
    registerClient,
    removeClient,
    broadcast
};
