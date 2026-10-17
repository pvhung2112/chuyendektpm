// -------------------------------------------------------------
// EVENT SOURCE & REAL-TIME STATE
// -------------------------------------------------------------
let evtSource = null;

function appendLog(elementId, text, className = 'info') {
    const logBody = document.getElementById(elementId);
    if (!logBody) return;
    const entry = document.createElement('div');
    entry.className = `log-entry ${className}`;
    const time = new Date().toLocaleTimeString();
    entry.innerHTML = `[${time}] ${text}`;
    logBody.appendChild(entry);
    logBody.scrollTop = logBody.scrollHeight;
}

function clearLog(elementId) {
    const logBody = document.getElementById(elementId);
    if (logBody) logBody.innerHTML = '';
}

// Khởi tạo SSE Stream
function initSSE() {
    evtSource = new EventSource('/api/stream');

    evtSource.addEventListener('connected', () => {
        appendLog('log-rabbit', 'Kết nối thành công với SSE Server.', 'success');
        appendLog('log-kafka', 'Kết nối thành công với SSE Server.', 'success');
    });

    // ----------------- RABBITMQ EVENTS -----------------
    evtSource.addEventListener('rabbit_task_published', (e) => {
        const task = JSON.parse(e.data);
        handleRabbitPublished(task);
    });

    evtSource.addEventListener('rabbit_worker_pickup', (e) => {
        const data = JSON.parse(e.data);
        handleRabbitPickup(data);
    });

    evtSource.addEventListener('rabbit_worker_ack', (e) => {
        const data = JSON.parse(e.data);
        handleRabbitAck(data);
    });

    evtSource.addEventListener('rabbit_worker_nack', (e) => {
        const data = JSON.parse(e.data);
        handleRabbitNack(data);
    });

    // ----------------- KAFKA EVENTS -----------------
    evtSource.addEventListener('kafka_event_produced', (e) => {
        const evt = JSON.parse(e.data);
        handleKafkaProduced(evt);
    });

    evtSource.addEventListener('kafka_consumer_read', (e) => {
        const data = JSON.parse(e.data);
        handleKafkaConsumerRead(data);
    });

    evtSource.addEventListener('kafka_replay_start', (e) => {
        appendLog('log-kafka', '⏪ Lập trình viên gọi seek(offset: 0) -> Bắt đầu Tua Lại Lịch Sử!', 'highlight');
        const badge = document.getElementById('action-audit');
        if (badge) badge.innerText = 'Đang tua lại từ Offset 0...';
    });

    evtSource.addEventListener('kafka_replay_read', (e) => {
        const data = JSON.parse(e.data);
        handleKafkaReplayRead(data);
    });

    evtSource.addEventListener('kafka_replay_done', () => {
        appendLog('log-kafka', '✔ Đã hoàn tất Tua lại. Dữ liệu trên đĩa cứng vẫn nguyên vẹn 100%!', 'success');
        const badge = document.getElementById('action-audit');
        if (badge) badge.innerText = 'Đã hoàn tất Replay';
    });

    evtSource.addEventListener('system_reset', () => {
        resetLocalUI();
    });

    evtSource.onerror = () => {
        console.warn('Mất kết nối SSE, đang thử kết nối lại...');
    };
}

// -------------------------------------------------------------
// RABBITMQ UI HANDLERS
// -------------------------------------------------------------
function handleRabbitPublished(task) {
    const queueSlotId = task.type === 'normal' ? 'slots-normal' : 'slots-vip';
    const queueSlots = document.getElementById(queueSlotId);
    
    // Xóa placeholder nếu có
    const placeholder = queueSlots.querySelector('.empty-placeholder');
    if (placeholder) placeholder.remove();

    const item = document.createElement('div');
    item.id = `task-el-${task.id}`;
    item.className = `task-item ${task.type}`;
    item.innerText = `${task.id}`;
    item.title = `${task.title} (${task.routingKey})`;
    queueSlots.appendChild(item);

    updateQueueCount();
    appendLog('log-rabbit', `[Producer] Phát ${task.id} (${task.title}) qua Routing Key [${task.routingKey}]`, 'info');
}

function handleRabbitPickup(data) {
    const workerCard = document.getElementById(data.worker === 'VIP' ? 'ui-worker-vip' : 'ui-worker-normal');
    const workerStatus = document.getElementById(data.worker === 'VIP' ? 'status-worker-vip' : 'status-worker-normal');
    
    if (workerCard) workerCard.classList.add('working');
    if (workerStatus) workerStatus.innerText = `Đang xử lý ${data.task.id}`;

    appendLog('log-rabbit', `[Worker ${data.worker}] Tiếp nhận ${data.task.id} từ hàng đợi...`, 'warn');
}

function handleRabbitAck(data) {
    const taskEl = document.getElementById(`task-el-${data.task.id}`);
    if (taskEl) {
        taskEl.style.transition = 'all 0.5s ease';
        taskEl.style.opacity = '0';
        taskEl.style.transform = 'scale(0.5)';
        setTimeout(() => {
            taskEl.remove();
            updateQueueCount();
        }, 500);
    }

    const workerCard = document.getElementById(data.worker === 'VIP' ? 'ui-worker-vip' : 'ui-worker-normal');
    const workerStatus = document.getElementById(data.worker === 'VIP' ? 'status-worker-vip' : 'status-worker-normal');
    if (workerCard) workerCard.classList.remove('working');
    if (workerStatus) workerStatus.innerText = 'Đang rảnh';

    appendLog('log-rabbit', `✅ [Worker ${data.worker}] Hoàn thành ${data.task.id} -> Gửi ACK -> 🗑️ Tin nhắn ĐÃ BỊ XÓA KHỎI BỘ NHỚ để giải phóng RAM!`, 'success');
}

function handleRabbitNack(data) {
    const taskEl = document.getElementById(`task-el-${data.task.id}`);
    if (taskEl) taskEl.remove();

    // Đẩy vào DLQ
    const dlqSlots = document.getElementById('slots-dlq');
    const placeholder = dlqSlots.querySelector('.empty-placeholder');
    if (placeholder) placeholder.remove();

    const dlqItem = document.createElement('div');
    dlqItem.className = 'task-item poison';
    dlqItem.innerText = `☠️ ${data.task.id}`;
    dlqSlots.appendChild(dlqItem);

    const workerCard = document.getElementById('ui-worker-vip');
    const workerStatus = document.getElementById('status-worker-vip');
    if (workerCard) workerCard.classList.remove('working');
    if (workerStatus) workerStatus.innerText = 'Đang rảnh';

    updateQueueCount();
    appendLog('log-rabbit', `❌ [Worker ${data.worker}] Phát hiện ${data.task.id} bị lỗi! Gửi NACK -> ➡️ Đã cô lập sang Dead Letter Queue (DLQ)!`, 'error');
}

function updateQueueCount() {
    const countVip = document.getElementById('slots-vip').querySelectorAll('.task-item').length;
    const countNormal = document.getElementById('slots-normal').querySelectorAll('.task-item').length;
    const countDlq = document.getElementById('slots-dlq').querySelectorAll('.task-item').length;

    document.getElementById('count-vip').innerText = `${countVip} task`;
    document.getElementById('count-normal').innerText = `${countNormal} task`;
    document.getElementById('count-dlq').innerText = `${countDlq} lỗi`;
}

// -------------------------------------------------------------
// KAFKA UI HANDLERS
// -------------------------------------------------------------
function handleKafkaProduced(evt) {
    const tape = document.getElementById('commit-log-tape');
    const emptyMsg = tape.querySelector('.tape-empty');
    if (emptyMsg) emptyMsg.remove();

    const segment = document.createElement('div');
    segment.id = `kafka-segment-${evt.offset}`;
    segment.className = 'log-segment';
    segment.innerHTML = `
        <div class="segment-offset">Offset ${evt.offset}</div>
        <div class="segment-event">${evt.eventType}</div>
        <div class="segment-user">${evt.user} • ${evt.timestamp}</div>
    `;
    tape.appendChild(segment);
    tape.scrollLeft = tape.scrollWidth;

    appendLog('log-kafka', `[Commit Log] 💾 Ghi nối đuôi Offset ${evt.offset}: ${evt.eventType} (${evt.detail})`, 'info');
}

function handleKafkaConsumerRead(data) {
    const offsetBadge = document.getElementById(data.group === 'notification' ? 'offset-notif' : 'offset-ai');
    const actionBadge = document.getElementById(data.group === 'notification' ? 'action-notif' : 'action-ai');

    if (offsetBadge) offsetBadge.innerText = data.offset;
    if (actionBadge) {
        actionBadge.innerText = data.group === 'notification'
            ? `Bắn thông báo: ${data.event.eventType}`
            : `Học hành vi: ${data.event.eventType}`;
    }

    // Highlight ô trên commit log
    const segment = document.getElementById(`kafka-segment-${data.offset}`);
    if (segment) {
        segment.classList.add('highlight-read');
        setTimeout(() => segment.classList.remove('highlight-read'), 600);
    }

    appendLog('log-kafka', `[${data.groupName}] Đọc Offset ${data.offset}: ${data.event.eventType} -> Xử lý (Dữ liệu vẫn còn trên đĩa)`, 'warn');
}

function handleKafkaReplayRead(data) {
    const offsetAudit = document.getElementById('offset-audit');
    const actionAudit = document.getElementById('action-audit');

    if (offsetAudit) offsetAudit.innerText = data.offset;
    if (actionAudit) actionAudit.innerText = `Rà soát lại: ${data.event.eventType}`;

    const segment = document.getElementById(`kafka-segment-${data.offset}`);
    if (segment) {
        segment.classList.add('highlight-replay');
        setTimeout(() => segment.classList.remove('highlight-replay'), 700);
    }

    appendLog('log-kafka', `[⏪ REPLAY KIỂM TOÁN] Tua lại đọc Offset ${data.offset}: ${data.event.eventType} (Người dùng: ${data.event.user})`, 'highlight');
}

// -------------------------------------------------------------
// USER BUTTON ACTIONS
// -------------------------------------------------------------
async function publishRabbitTask(type) {
    await fetch('/api/rabbitmq/publish', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type })
    });
}

const sampleKafkaEvents = [
    { eventType: 'USER_LOGIN', user: 'HungPV', detail: 'Đăng nhập từ IP 121.11.3.198' },
    { eventType: 'VIEW_PRODUCT', user: 'HungPV', detail: 'Xem sản phẩm Laptop Dell XPS 15' },
    { eventType: 'ADD_TO_CART', user: 'HungPV', detail: 'Thêm Laptop vào giỏ hàng' },
    { eventType: 'CHECKOUT_FAIL', user: 'HungPV', detail: 'Thanh toán lỗi ngân hàng' },
    { eventType: 'CHECKOUT_SUCCESS', user: 'HungPV', detail: 'Thanh toán thành công đơn #8888' }
];
let eventCycleIndex = 0;

async function produceKafkaEvent() {
    const evt = sampleKafkaEvents[eventCycleIndex % sampleKafkaEvents.length];
    eventCycleIndex++;
    await fetch('/api/kafka/produce', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(evt)
    });
}

async function consumeKafkaMultiGroup() {
    // Chạy Group Notification
    await fetch('/api/kafka/consume', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ groupType: 'notification' })
    });

    // Chạy Group AI độc lập sau 1s
    setTimeout(async () => {
        await fetch('/api/kafka/consume', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ groupType: 'ai' })
        });
    }, 1000);
}

async function replayKafkaLog() {
    await fetch('/api/kafka/replay', { method: 'POST' });
}

function resetLocalUI() {
    document.getElementById('slots-vip').innerHTML = '<div class="empty-placeholder">Đang chờ tác vụ...</div>';
    document.getElementById('slots-normal').innerHTML = '<div class="empty-placeholder">Đang chờ tác vụ...</div>';
    document.getElementById('slots-dlq').innerHTML = '<div class="empty-placeholder">Cô lập tác vụ lỗi ở đây...</div>';
    document.getElementById('commit-log-tape').innerHTML = '<div class="tape-empty">Chưa có sự kiện nào được ghi vào Topic...</div>';

    document.getElementById('offset-notif').innerText = '-';
    document.getElementById('offset-ai').innerText = '-';
    document.getElementById('offset-audit').innerText = '-';
    document.getElementById('action-notif').innerText = 'Chưa bắt đầu';
    document.getElementById('action-ai').innerText = 'Chưa bắt đầu';
    document.getElementById('action-audit').innerText = 'Sẵn sàng tua lại';

    updateQueueCount();
    clearLog('log-rabbit');
    clearLog('log-kafka');
    appendLog('log-rabbit', 'Đã đặt lại trạng thái hệ thống.', 'info');
    appendLog('log-kafka', 'Đã đặt lại trạng thái hệ thống.', 'info');
}

// -------------------------------------------------------------
// AUTO DEMO PRESENTATION SCENARIO
// -------------------------------------------------------------
async function runAutoDemo() {
    const btn = document.getElementById('btn-auto-demo');
    btn.disabled = true;
    btn.innerHTML = '<span>⏳ Đang chạy kịch bản so sánh...</span>';

    // 1. Reset
    await fetch('/api/reset', { method: 'POST' });
    await new Promise(r => setTimeout(r, 1200));

    // 2. Bắn Task VIP
    appendLog('log-rabbit', '--- BẮT ĐẦU TRÌNH DIỄN RABBITMQ ---', 'highlight');
    await publishRabbitTask('vip');
    await new Promise(r => setTimeout(r, 1600));

    // 3. Bắn Task Thường
    await publishRabbitTask('normal');
    await new Promise(r => setTimeout(r, 1600));

    // 4. Bắn Task Poison
    await publishRabbitTask('poison');
    await new Promise(r => setTimeout(r, 2200));

    // 5. Kafka: Bắn 5 sự kiện nối đuôi
    appendLog('log-kafka', '--- BẮT ĐẦU TRÌNH DIỄN APACHE KAFKA ---', 'highlight');
    for (const evt of sampleKafkaEvents) {
        await fetch('/api/kafka/produce', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(evt)
        });
        await new Promise(r => setTimeout(r, 600));
    }
    await new Promise(r => setTimeout(r, 1200));

    // 6. Kafka: 2 Consumer đọc
    await consumeKafkaMultiGroup();
    await new Promise(r => setTimeout(r, 3500));

    // 7. Kafka: Tua lại lịch sử (Replay)
    await replayKafkaLog();
    await new Promise(r => setTimeout(r, 4000));

    btn.disabled = false;
    btn.innerHTML = '<span class="btn-icon">▶</span><span>Chạy Kịch Bản Tự Động</span>';
    alert('🎉 Đã hoàn tất kịch bản so sánh thực tế giữa RabbitMQ và Apache Kafka!');
}

// Event Listeners
document.getElementById('btn-auto-demo').addEventListener('click', runAutoDemo);
document.getElementById('btn-reset').addEventListener('click', () => {
    fetch('/api/reset', { method: 'POST' });
});

// Khởi chạy khi load trang
window.addEventListener('DOMContentLoaded', () => {
    initSSE();
});
