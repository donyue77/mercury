const express = require('express');
const cors = require('cors');
const axios = require('axios');

const app = express();
app.use(cors());
app.use(express.json());

const LINE_TOKEN = process.env.LINE_TOKEN;
const phoneToUserId = {};
const userIdMap = {};

let sharedState = {
  A: { current: 0, lastIssued: 0, queue: [], history: [], servedToday: 0 },
  B: { current: 0, lastIssued: 0, queue: [], history: [], servedToday: 0 }
};
let sharedCfg = {
  systemName: '排隊系統',
  services: {
    A: { name: '心願瓶DIY', prefix: 'A', minutes: 12, concurrent: 5 },
    B: { name: '塔羅牌占卜', prefix: 'T', minutes: 15, concurrent: 2 }
  }
};

app.get('/api/state', (req, res) => res.json({ state: sharedState, cfg: sharedCfg }));
app.post('/api/state', (req, res) => {
  if (req.body.state) sharedState = req.body.state;
  if (req.body.cfg) sharedCfg = req.body.cfg;
  res.json({ success: true });
});
app.post('/api/issue', (req, res) => {
  const { svc, name, userId, phone, partySize } = req.body;
  if (!svc || !name) return res.status(400).json({ error: '缺少參數' });
  sharedState[svc].lastIssued++;
  const num = sharedState[svc].lastIssued;
  const size = Math.min(Math.max(parseInt(partySize) || 1, 1), 6);
  sharedState[svc].queue.push({ num, name, userId: userId || '—', phone: phone || null, partySize: size });
  res.json({ success: true, num });
});
app.post('/api/call-next', (req, res) => {
  const { svc } = req.body;
  const q = sharedState[svc].queue;
  if (q.length === 0) return res.status(400).json({ error: '無人候位' });
  const entry = q.shift();
  sharedState[svc].current = entry.num;
  sharedState[svc].servedToday++;
  sharedState[svc].history.unshift(entry.num);
  if (sharedState[svc].history.length > 10) sharedState[svc].history.pop();
  res.json({ success: true, called: entry });
});
app.post('/api/cancel', (req, res) => {
  const { svc, num } = req.body;
  sharedState[svc].queue = sharedState[svc].queue.filter(q => q.num !== num);
  res.json({ success: true });
});
app.post('/api/reset', (req, res) => {
  const { svc } = req.body;
  if (svc) {
    sharedState[svc] = { current: 0, lastIssued: 0, queue: [], history: [], servedToday: 0 };
  } else {
    sharedState = {
      A: { current: 0, lastIssued: 0, queue: [], history: [], servedToday: 0 },
      B: { current: 0, lastIssued: 0, queue: [], history: [], servedToday: 0 }
    };
  }
  res.json({ success: true });
});
app.post('/webhook', async (req, res) => {
  res.sendStatus(200);
  const events = req.body.events || [];
  for (const event of events) {
    if (event.type === 'message' && event.message.type === 'text') {
      const userId = event.source.userId;
      const text = event.message.text.trim();

      // 手機號碼綁定
      if (/^09\d{8}$/.test(text)) {
        phoneToUserId[text] = userId;
        await axios.post('https://api.line.me/v2/bot/message/reply', {
          replyToken: event.replyToken,
          messages: [{ type: 'text', text: `🫙 心願瓶DIY｜✅ 手機號碼 ${text} 綁定成功！結帳後工作人員會幫您登記候位，輪到您時我們會主動通知您 🙏` }]
        }, { headers: { Authorization: `Bearer ${LINE_TOKEN}` } }).catch(()=>{});
        continue;
      }

      // 查詢塔羅牌叫號狀況
      const isTarotQuery = ['查詢塔羅目前叫號', '塔羅目前叫號', '查詢塔羅', '塔羅叫號', '🔮查詢'].includes(text);
      if (isTarotQuery) {
        const q = sharedState.B;
        const cfg = sharedCfg.services.B;
        const cur = q.current > 0 ? cfg.prefix + String(q.current).padStart(3,'0') : '尚未開始';
        const waiting = q.queue.length;
        const estMins = waiting > 0 ? Math.max(0, Math.ceil(waiting / 2) - 1) * cfg.minutes : 0;
        const estText = waiting === 0 ? '目前無人候位' : estMins > 0 ? `預估等待約 ${estMins} 分鐘` : '即將輪到下一位';
        const replyMsg = `🔮 塔羅牌占卜｜目前叫號查詢

現在服務號：${cur}
等候人數：${waiting} 人
${estText}

輪到您時我們會主動通知您 🙏`;
        await axios.post('https://api.line.me/v2/bot/message/reply', {
          replyToken: event.replyToken,
          messages: [{ type: 'text', text: replyMsg }]
        }, { headers: { Authorization: `Bearer ${LINE_TOKEN}` } }).catch(()=>{});
        continue;
      }

      // 查詢心願瓶叫號狀況
      const isWishQuery = ['查詢心願瓶目前叫號', '心願瓶目前叫號', '查詢心願瓶', '心願瓶叫號', '🫙查詢'].includes(text);
      if (isWishQuery) {
        const q = sharedState.A;
        const cfg = sharedCfg.services.A;
        const cur = q.current > 0 ? cfg.prefix + String(q.current).padStart(3,'0') : '尚未開始';
        const waiting = q.queue.length;
        const totalCap = q.queue.reduce((sum, e) => sum + (e.partySize || 1), 0);
        const estMins = waiting > 0 ? Math.max(0, Math.ceil(totalCap / 5) - 1) * cfg.minutes : 0;
        const estText = waiting === 0 ? '目前無人候位' : estMins > 0 ? `預估等待約 ${estMins} 分鐘` : '即將輪到下一組';
        const replyMsg = `🫙 心願瓶DIY｜目前叫號查詢

現在服務號：${cur}
等候組數：${waiting} 組
${estText}

輪到您時我們會主動通知您 🙏`;
        await axios.post('https://api.line.me/v2/bot/message/reply', {
          replyToken: event.replyToken,
          messages: [{ type: 'text', text: replyMsg }]
        }, { headers: { Authorization: `Bearer ${LINE_TOKEN}` } }).catch(()=>{});
        continue;
      }
    }
  }
});
app.post('/api/register', (req, res) => {
  const { userId, name } = req.body;
  if (!userId) return res.status(400).json({ error: '缺少 userId' });
  userIdMap[userId] = { userId, name };
  res.json({ success: true });
});
app.post('/api/line-notify', async (req, res) => {
  const { userId, phone, name, message } = req.body;
  if (!message) return res.status(400).json({ error: '缺少 message' });
  const targetId = (userId && userId !== '—') ? userId : phoneToUserId[phone];
  if (!targetId) return res.status(404).json({ error: '找不到對應的 LINE 帳號' });
  try {
    await axios.post('https://api.line.me/v2/bot/message/push', {
      to: targetId,
      messages: [{ type: 'text', text: message }]
    }, { headers: { Authorization: `Bearer ${LINE_TOKEN}` } });
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});








app.get('/queue', (req, res) => { res.setHeader('Content-Type', 'text/html; charset=utf-8'); res.send(`<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>排隊取號</title>
<script src="https://static.line-scdn.net/liff/edge/versions/2.22.3/sdk.js"></script>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#fff;--bg2:#f6f5f2;--bg3:#eeede9;
  --text:#1a1a1a;--text2:#5a5a5a;--text3:#999;
  --border:rgba(0,0,0,0.1);--border2:rgba(0,0,0,0.2);
  --r:10px;--r-sm:6px;
  --sA:#3b5bdb;--sA-bg:#eef2ff;--sA-border:#a5b4fc;--sA-text:#1e3a8a;
  --sB:#6d28d9;--sB-bg:#f5f3ff;--sB-border:#c4b5fd;--sB-text:#3b0764;
  --green:#3b6d11;--green-bg:#eaf3de;--green-b:#97c459;
  --amber:#854f0b;--amber-bg:#faeeda;--amber-b:#ef9f27;
  --red:#a32d2d;--red-bg:#fcebeb;--red-b:#f09595;
}
@media(prefers-color-scheme:dark){
  :root{
    --bg:#1c1c1e;--bg2:#2c2c2e;--bg3:#3a3a3c;
    --text:#f2f2f7;--text2:#aeaeb2;--text3:#636366;
    --border:rgba(255,255,255,0.1);--border2:rgba(255,255,255,0.2);
    --sA:#818cf8;--sA-bg:#1e1b4b;--sA-border:#4338ca;--sA-text:#c7d2fe;
    --sB:#a78bfa;--sB-bg:#2e1065;--sB-border:#7c3aed;--sB-text:#ddd6fe;
    --green:#c0dd97;--green-bg:#173404;--green-b:#3b6d11;
    --amber:#fac775;--amber-bg:#412402;--amber-b:#854f0b;
    --red:#f7c1c1;--red-bg:#501313;--red-b:#a32d2d;
  }
}
body{font-family:-apple-system,BlinkMacSystemFont,'Noto Sans TC',sans-serif;background:var(--bg2);color:var(--text);min-height:100vh}
.app{max-width:480px;margin:0 auto;padding-bottom:48px}
.topbar{background:var(--bg);border-bottom:0.5px solid var(--border);padding:14px 16px;display:flex;align-items:center;gap:10px;position:sticky;top:0;z-index:20}
.topbar-title{font-size:15px;font-weight:500;flex:1}
.live-dot{width:7px;height:7px;background:#639922;border-radius:50%;animation:pulse 1.5s infinite;flex-shrink:0}
.tabs{display:flex;background:var(--bg);border-bottom:0.5px solid var(--border)}
.tab{flex:1;padding:12px 4px;border:none;background:transparent;font-size:13px;font-weight:500;color:var(--text3);cursor:pointer;border-bottom:2px solid transparent;transition:all .15s;font-family:inherit}
.tab.active{color:var(--text);border-bottom-color:var(--text)}
.panel{display:none;padding:14px}
.panel.active{display:block}
.card{background:var(--bg);border:0.5px solid var(--border);border-radius:var(--r);padding:16px;margin-bottom:12px}
.card-title{font-size:11px;font-weight:500;color:var(--text3);letter-spacing:.05em;text-transform:uppercase;margin-bottom:12px}
.big-num{font-size:80px;font-weight:500;line-height:1;letter-spacing:-2px;text-align:center}
.big-num.color-A{color:var(--sA)}
.big-num.color-B{color:var(--sB)}
.big-sub{font-size:12px;color:var(--text3);text-align:center;margin-top:4px}
.stat-row{display:flex;justify-content:space-between;align-items:center;padding:9px 0;border-bottom:0.5px solid var(--border)}
.stat-row:last-child{border-bottom:none}
.stat-label{font-size:13px;color:var(--text2)}
.stat-val{font-size:14px;font-weight:500;color:var(--text)}
.btn{display:inline-flex;align-items:center;justify-content:center;gap:6px;padding:12px 18px;border:0.5px solid var(--border2);border-radius:var(--r-sm);background:transparent;font-size:14px;font-weight:500;color:var(--text);cursor:pointer;font-family:inherit;transition:all .15s;width:100%;margin-bottom:8px}
.btn:hover{background:var(--bg2)}
.btn:active{transform:scale(.97)}
.btn-A{background:var(--sA-bg);color:var(--sA-text);border-color:var(--sA-border)}
.btn-B{background:var(--sB-bg);color:var(--sB-text);border-color:var(--sB-border)}
.btn-danger{color:var(--red);border-color:var(--red-b)}
.btn-danger:hover{background:var(--red-bg)}
.field{display:flex;flex-direction:column;gap:5px;margin-bottom:12px}
.field label{font-size:12px;color:var(--text2);font-weight:500}
.field input{padding:10px 12px;border:0.5px solid var(--border2);border-radius:var(--r-sm);background:var(--bg);color:var(--text);font-size:15px;font-family:inherit;width:100%}
.field input:focus{outline:none;border-color:var(--sA)}
.chips{display:flex;flex-wrap:wrap;gap:6px}
.chip{padding:4px 11px;border-radius:99px;font-size:12px;border:0.5px solid var(--border);background:var(--bg2);color:var(--text2)}
.chip.cur-A{background:var(--sA-bg);color:var(--sA-text);border-color:var(--sA-border);font-weight:500}
.chip.cur-B{background:var(--sB-bg);color:var(--sB-text);border-color:var(--sB-border);font-weight:500}
.chip.mine{border-width:1.5px}
.ticket{text-align:center;padding:24px 12px}
.ticket-num{font-size:88px;font-weight:500;line-height:1;letter-spacing:-2px}
.ticket-num.color-A{color:var(--sA)}
.ticket-num.color-B{color:var(--sB)}
.ticket-svc{display:inline-block;padding:4px 14px;border-radius:99px;font-size:12px;font-weight:500;margin-top:8px}
.ticket-svc.svc-A{background:var(--sA-bg);color:var(--sA-text);border:0.5px solid var(--sA-border)}
.ticket-svc.svc-B{background:var(--sB-bg);color:var(--sB-text);border:0.5px solid var(--sB-border)}
.ticket-name{font-size:16px;color:var(--text2);margin-top:8px}
.ticket-time{font-size:12px;color:var(--text3);margin-top:2px}
.wait-badge{display:inline-block;margin-top:14px;padding:8px 18px;border-radius:var(--r-sm);font-size:13px;font-weight:500}
.wait-badge.normal{background:var(--bg2);color:var(--text2)}
.wait-badge.soon{background:var(--amber-bg);color:var(--amber);border:0.5px solid var(--amber-b)}
.wait-badge.now{background:var(--green-bg);color:var(--green);border:0.5px solid var(--green-b)}
.svc-tabs{display:flex;gap:8px;margin-bottom:14px}
.svc-tab{flex:1;padding:10px;border-radius:var(--r-sm);border:0.5px solid var(--border2);background:transparent;font-size:13px;font-weight:500;color:var(--text2);cursor:pointer;font-family:inherit;transition:all .15s;text-align:center}
.svc-tab.active-A{background:var(--sA-bg);color:var(--sA-text);border-color:var(--sA-border)}
.svc-tab.active-B{background:var(--sB-bg);color:var(--sB-text);border-color:var(--sB-border)}
.wait-bar-wrap{margin-top:10px}
.wait-bar-label{display:flex;justify-content:space-between;font-size:11px;color:var(--text3);margin-bottom:4px}
.wait-bar-track{height:4px;background:var(--bg3);border-radius:99px;overflow:hidden}
.wait-bar-fill{height:100%;border-radius:99px;transition:width .5s}
.wait-bar-fill.A{background:var(--sA)}
.wait-bar-fill.B{background:var(--sB)}
.empty{font-size:13px;color:var(--text3);font-style:italic}
.loading{text-align:center;padding:40px;color:var(--text3);font-size:14px}
.toast{position:fixed;bottom:24px;left:50%;transform:translateX(-50%) translateY(80px);background:var(--text);color:var(--bg);padding:10px 22px;border-radius:99px;font-size:13px;font-weight:500;transition:transform .25s;z-index:999;white-space:nowrap;pointer-events:none}
.toast.show{transform:translateX(-50%) translateY(0)}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
</style>
</head>
<body>
<div class="app">
  <div class="topbar">
    <div class="live-dot"></div>
    <div class="topbar-title" id="topbar-title">排隊取號</div>
  </div>
  <div class="tabs">
    <button class="tab active" onclick="goTab('take')" id="tab-take">取號</button>
    <button class="tab" onclick="goTab('status')" id="tab-status">等候狀況</button>
  </div>

  <!-- 取號 -->
  <div class="panel active" id="panel-take">
    <div class="loading" id="loading-msg">載入中，請稍候...</div>
    <div id="take-content" style="display:none">

      <!-- 已取號 -->
      <div id="my-ticket-view" style="display:none">
        <div class="card">
          <div class="ticket">
            <div class="ticket-num" id="tk-num">—</div>
            <div id="tk-svc-badge"></div>
            <div class="ticket-name" id="tk-name"></div>
            <div class="ticket-time" id="tk-time"></div>
            <div class="wait-badge normal" id="tk-wait"></div>
          </div>
        </div>
        <button class="btn btn-danger" onclick="leaveQueue()">放棄候位</button>
      </div>

      <!-- 取號表單 -->
      <div id="take-form" style="display:none">
        <!-- 塔羅牌 -->
        <div class="card" style="text-align:center;padding:20px 16px;margin-bottom:12px">
          <div style="font-size:32px;margin-bottom:8px">🔮</div>
          <div style="font-size:16px;font-weight:500;margin-bottom:4px" id="svcB-name">塔羅牌占卜</div>
          <div style="font-size:13px;color:var(--text2)">填入姓名取號，輪到您時將傳送 LINE 通知</div>
        </div>
        <div class="card">
          <div class="field" style="margin-bottom:0">
            <label>姓名</label>
            <input type="text" id="inp-name" placeholder="請輸入您的姓名"/>
          </div>
        </div>
        <button class="btn btn-B" onclick="takeNumber('B')" id="take-btn-B">取得號碼牌</button>

        <!-- 心願瓶 -->
        <div class="card" style="text-align:center;padding:20px 16px;margin-top:8px">
          <div style="font-size:32px;margin-bottom:8px">🫙</div>
          <div style="font-size:16px;font-weight:500;margin-bottom:4px" id="svcA-name">心願瓶DIY</div>
          <div style="font-size:13px;color:var(--text2);line-height:1.7">請至服務台結帳後，由工作人員協助登記候位</div>
        </div>
        <div class="card" style="padding:12px 16px">
          <div class="stat-row">
            <span class="stat-label">目前等候</span>
            <span class="stat-val" id="svcA-waiting">0 人</span>
          </div>
          <div class="stat-row" style="border:none">
            <span class="stat-label">預估等待</span>
            <span class="stat-val" id="svcA-est">—</span>
          </div>
        </div>
      </div>
    </div>
  </div>

  <!-- 等候狀況 -->
  <div class="panel" id="panel-status">
    <div class="svc-tabs">
      <button class="svc-tab active-A" id="status-tab-A" onclick="setStatusSvc('A')" id="status-tab-A">心願瓶DIY</button>
      <button class="svc-tab" id="status-tab-B" onclick="setStatusSvc('B')">塔羅牌占卜</button>
    </div>
    <div class="card">
      <div style="text-align:center;padding:20px 0 14px">
        <div class="big-num" id="status-cur">—</div>
        <div class="big-sub" id="status-label">等待服務</div>
      </div>
      <div class="wait-bar-wrap">
        <div class="wait-bar-label"><span>等候人數</span><span id="status-bar-label">0 人</span></div>
        <div class="wait-bar-track"><div class="wait-bar-fill A" id="status-bar" style="width:0%"></div></div>
      </div>
    </div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px">
      <div class="card" style="padding:14px;text-align:center">
        <div style="font-size:11px;color:var(--text3);margin-bottom:4px">等候人數</div>
        <div style="font-size:28px;font-weight:500" id="status-waiting">0</div>
      </div>
      <div class="card" style="padding:14px;text-align:center">
        <div style="font-size:11px;color:var(--text3);margin-bottom:4px">預估等待</div>
        <div style="font-size:28px;font-weight:500" id="status-est">—</div>
        <div style="font-size:11px;color:var(--text3)">分鐘</div>
      </div>
    </div>
    <div class="card">
      <div class="card-title" style="margin-bottom:10px">等候序列</div>
      <div class="chips" id="status-chips"><span class="empty">目前無人候位</span></div>
    </div>
  </div>
</div>
<div class="toast" id="toast"></div>

<script>
const BACKEND_URL = 'https://mercury-gcac.onrender.com';
const LIFF_ID = '2006903949-Sbmw12xl';

let state = {
  A: { current:0, lastIssued:0, queue:[], history:[], servedToday:0 },
  B: { current:0, lastIssued:0, queue:[], history:[], servedToday:0 }
};
let cfg = {
  systemName: '排隊系統',
  services: {
    A: { name: '心願瓶DIY', prefix: 'A', minutes: 15 },
    B: { name: '塔羅牌占卜', prefix: 'T', minutes: 20 }
  }
};
let myTicket = null;
let lineUserId = null;
let currentStatusSvc = 'A';

function fmt(svc, n) { return cfg.services[svc].prefix + String(n).padStart(3,'0'); }
function nowTime() { return new Date().toLocaleTimeString('zh-TW',{hour:'2-digit',minute:'2-digit'}); }
function showToast(msg) {
  const el = document.getElementById('toast');
  el.textContent = msg; el.classList.add('show');
  setTimeout(()=>el.classList.remove('show'), 2400);
}

function goTab(name) {
  ['take','status'].forEach(t => {
    document.getElementById('panel-'+t).classList.toggle('active', t===name);
    document.getElementById('tab-'+t).classList.toggle('active', t===name);
  });
}

function setStatusSvc(s) {
  currentStatusSvc = s;
  ['A','B'].forEach(x => {
    document.getElementById('status-tab-'+x).className = 'svc-tab' + (x===s ? ' active-'+x : '');
  });
  renderStatus();
}

async function syncFromServer() {
  try {
    const res = await fetch(BACKEND_URL + '/api/state');
    const data = await res.json();
    if (data.state) state = data.state;
    if (data.cfg) cfg = data.cfg;
    render();
  } catch(e) {}
}

async function sendLineNotify(userId, name, message) {
  if (!userId || userId === '—') return;
  try {
    await fetch(BACKEND_URL + '/api/line-notify', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ userId, name, message })
    });
  } catch(e) {}
}

async function takeNumber(svc) {
  const name = document.getElementById('inp-name').value.trim();
  if (!name) { showToast('請輸入姓名'); return; }
  if (!lineUserId) { showToast('請在 LINE 內開啟此頁面'); return; }
  try {
    const res = await fetch(BACKEND_URL + '/api/issue', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ svc, name, userId: lineUserId, phone: null })
    });
    const data = await res.json();
    if (!data.success) { showToast('取號失敗，請再試一次'); return; }
    myTicket = { svc, num: data.num, name, userId: lineUserId, time: nowTime() };
    localStorage.setItem('qs_ticket', JSON.stringify(myTicket));
    const svcIcon = svc === 'B' ? '🔮' : '🫙';
    sendLineNotify(lineUserId, name, \`\${svcIcon} \${cfg.services[svc].name}｜您好 \${name}！您已取得 \${fmt(svc, data.num)} 號，輪到您前會再通知您，感謝耐心等候 🙏\`);
    await syncFromServer();
    showToast('取號成功：' + fmt(svc, data.num));
  } catch(e) { showToast('網路錯誤，請再試一次'); }
}

async function leaveQueue() {
  if (!myTicket || !confirm('確定要放棄候位嗎？')) return;
  try {
    await fetch(BACKEND_URL + '/api/cancel', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ svc: myTicket.svc, num: myTicket.num })
    });
    myTicket = null;
    localStorage.removeItem('qs_ticket');
    await syncFromServer();
    showToast('已放棄候位');
  } catch(e) { showToast('網路錯誤，請再試一次'); }
}

function render() {
  // 更新服務名稱
  document.getElementById('topbar-title').textContent = cfg.systemName;
  document.getElementById('svcA-name').textContent = cfg.services.A.name;
  document.getElementById('svcB-name').textContent = cfg.services.B.name;
  document.getElementById('status-tab-A').textContent = cfg.services.A.name;
  document.getElementById('status-tab-B').textContent = cfg.services.B.name;
  document.getElementById('take-btn-B').textContent = \`取得 \${cfg.services.B.name} 號碼牌\`;

  // 心願瓶等候資訊
  const qA = state.A.queue;
  document.getElementById('svcA-waiting').textContent = qA.length + ' 人';
  const estA = qA.length > 0 ? Math.max(0, Math.ceil(qA.length / 5) - 1) * cfg.services.A.minutes : 0;
  document.getElementById('svcA-est').textContent = qA.length > 0 ? (estA > 0 ? estA + ' 分鐘' : '即將輪到') : '無需等候';

  // 票券顯示
  if (myTicket) {
    document.getElementById('my-ticket-view').style.display = 'block';
    document.getElementById('take-form').style.display = 'none';
    const { svc, num, name, time } = myTicket;
    const numStr = fmt(svc, num);
    const el = document.getElementById('tk-num');
    el.textContent = numStr; el.className = 'ticket-num color-' + svc;
    document.getElementById('tk-svc-badge').innerHTML = \`<span class="ticket-svc svc-\${svc}">\${cfg.services[svc].name}</span>\`;
    document.getElementById('tk-name').textContent = name;
    document.getElementById('tk-time').textContent = '取號時間 ' + time;
    const q = state[svc].queue;
    const pos = q.findIndex(x => x.num === num);
    const wEl = document.getElementById('tk-wait');
    if (state[svc].current === num) {
      wEl.className='wait-badge now'; wEl.textContent='📢 叫到您了！請前往';
    } else if (pos === 0) {
      wEl.className='wait-badge soon'; wEl.textContent='您是下一位，請準備！';
    } else if (pos > 0) {
      wEl.className='wait-badge normal';
      const conc = svc === 'A' ? 5 : 2;
      const estW = Math.max(0, Math.ceil(pos / conc) - 1) * cfg.services[svc].minutes;
      wEl.textContent = estW > 0 ? \`前方 \${pos} 人，約 \${estW} 分鐘\` : \`前方 \${pos} 人，即將輪到\`;
    } else {
      wEl.className='wait-badge normal'; wEl.textContent='號碼已完成服務';
    }
  } else {
    document.getElementById('my-ticket-view').style.display = 'none';
    document.getElementById('take-form').style.display = 'block';
  }

  renderStatus();
}

function renderStatus() {
  const svc = currentStatusSvc;
  const q = state[svc].queue;
  const cur = state[svc].current;
  const mins = cfg.services[svc].minutes;
  const numStr = cur > 0 ? fmt(svc, cur) : '—';
  const el = document.getElementById('status-cur');
  el.textContent = numStr; el.className = 'big-num color-' + svc;
  document.getElementById('status-label').textContent = cur > 0 ? \`請 \${numStr} 號前往\` : '等待服務';
  document.getElementById('status-waiting').textContent = q.length;
  const concurrent = svc === 'A' ? 5 : 2;
  const estMins = q.length > 0 ? Math.max(0, Math.ceil(q.length / concurrent) - 1) * mins : 0;
  document.getElementById('status-est').textContent = q.length > 0 ? estMins || '< ' + mins : '—';
  const pct = Math.min(100, Math.round(q.length / 20 * 100));
  document.getElementById('status-bar').style.width = pct + '%';
  document.getElementById('status-bar').className = 'wait-bar-fill ' + svc;
  document.getElementById('status-bar-label').textContent = q.length + ' 人';
  const chips = document.getElementById('status-chips');
  if (q.length === 0) { chips.innerHTML = '<span class="empty">目前無人候位</span>'; return; }
  chips.innerHTML = q.map((entry, i) => {
    const isMine = myTicket && myTicket.svc === svc && myTicket.num === entry.num;
    let cls = 'chip' + (i===0 ? ' cur-'+svc : '') + (isMine ? ' mine' : '');
    return \`<span class="\${cls}">\${fmt(svc, entry.num)}\${isMine ? ' (我)' : ''}</span>\`;
  }).join('');
}

// 初始化
const saved = localStorage.getItem('qs_ticket');
if (saved) myTicket = JSON.parse(saved);

async function initLiff() {
  try {
    await liff.init({ liffId: LIFF_ID });
    if (liff.isLoggedIn()) {
      const profile = await liff.getProfile();
      lineUserId = profile.userId;
      await fetch(BACKEND_URL + '/api/register', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ userId: lineUserId, name: profile.displayName })
      });
      const nameEl = document.getElementById('inp-name');
      if (nameEl && !nameEl.value) nameEl.value = profile.displayName;
    } else {
      if (liff.isInClient()) liff.login();
    }
  } catch(e) { console.warn('LIFF 初始化失敗:', e.message); }
  document.getElementById('loading-msg').style.display = 'none';
  document.getElementById('take-content').style.display = 'block';
}

syncFromServer();
initLiff();
setInterval(syncFromServer, 4000);
</script>
</body>
</html>
`); });
app.get('/staff', (req, res) => { res.setHeader('Content-Type', 'text/html; charset=utf-8'); res.send(`<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>工作人員管理</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#fff;--bg2:#f6f5f2;--bg3:#eeede9;
  --text:#1a1a1a;--text2:#5a5a5a;--text3:#999;
  --border:rgba(0,0,0,0.1);--border2:rgba(0,0,0,0.2);
  --r:10px;--r-sm:6px;
  --sA:#3b5bdb;--sA-bg:#eef2ff;--sA-border:#a5b4fc;--sA-text:#1e3a8a;
  --sB:#6d28d9;--sB-bg:#f5f3ff;--sB-border:#c4b5fd;--sB-text:#3b0764;
  --green:#3b6d11;--green-bg:#eaf3de;--green-b:#97c459;
  --amber:#854f0b;--amber-bg:#faeeda;--amber-b:#ef9f27;
  --red:#a32d2d;--red-bg:#fcebeb;--red-b:#f09595;
}
@media(prefers-color-scheme:dark){
  :root{
    --bg:#1c1c1e;--bg2:#2c2c2e;--bg3:#3a3a3c;
    --text:#f2f2f7;--text2:#aeaeb2;--text3:#636366;
    --border:rgba(255,255,255,0.1);--border2:rgba(255,255,255,0.2);
    --sA:#818cf8;--sA-bg:#1e1b4b;--sA-border:#4338ca;--sA-text:#c7d2fe;
    --sB:#a78bfa;--sB-bg:#2e1065;--sB-border:#7c3aed;--sB-text:#ddd6fe;
    --green:#c0dd97;--green-bg:#173404;--green-b:#3b6d11;
    --amber:#fac775;--amber-bg:#412402;--amber-b:#854f0b;
    --red:#f7c1c1;--red-bg:#501313;--red-b:#a32d2d;
  }
}
body{font-family:-apple-system,BlinkMacSystemFont,'Noto Sans TC',sans-serif;background:var(--bg2);color:var(--text);min-height:100vh}
.app{max-width:520px;margin:0 auto;padding-bottom:48px}
.topbar{background:var(--bg);border-bottom:0.5px solid var(--border);padding:14px 16px;display:flex;align-items:center;gap:10px;position:sticky;top:0;z-index:20}
.topbar-title{font-size:15px;font-weight:500;flex:1}
.live-dot{width:7px;height:7px;background:#639922;border-radius:50%;animation:pulse 1.5s infinite;flex-shrink:0}
.tabs{display:flex;background:var(--bg);border-bottom:0.5px solid var(--border)}
.tab{flex:1;padding:12px 4px;border:none;background:transparent;font-size:12px;font-weight:500;color:var(--text3);cursor:pointer;border-bottom:2px solid transparent;transition:all .15s;font-family:inherit}
.tab.active{color:var(--text);border-bottom-color:var(--text)}
.panel{display:none;padding:14px}
.panel.active{display:block}
.card{background:var(--bg);border:0.5px solid var(--border);border-radius:var(--r);padding:16px;margin-bottom:12px}
.card-title{font-size:11px;font-weight:500;color:var(--text3);letter-spacing:.05em;text-transform:uppercase;margin-bottom:12px}
.stat-row{display:flex;justify-content:space-between;align-items:center;padding:9px 0;border-bottom:0.5px solid var(--border)}
.stat-row:last-child{border-bottom:none}
.stat-label{font-size:13px;color:var(--text2)}
.stat-val{font-size:14px;font-weight:500;color:var(--text)}
.btn{display:inline-flex;align-items:center;justify-content:center;gap:6px;padding:10px 18px;border:0.5px solid var(--border2);border-radius:var(--r-sm);background:transparent;font-size:14px;font-weight:500;color:var(--text);cursor:pointer;font-family:inherit;transition:all .15s;width:100%;margin-bottom:8px}
.btn:hover{background:var(--bg2)}
.btn:active{transform:scale(.97)}
.btn-primary{background:var(--text);color:var(--bg);border-color:var(--text)}
.btn-primary:hover{opacity:.85;background:var(--text)}
.btn-A{background:var(--sA-bg);color:var(--sA-text);border-color:var(--sA-border)}
.btn-B{background:var(--sB-bg);color:var(--sB-text);border-color:var(--sB-border)}
.btn-danger{color:var(--red);border-color:var(--red-b)}
.btn-danger:hover{background:var(--red-bg)}
.btn-sm{padding:6px 12px;font-size:12px;width:auto;margin-bottom:0}
.svc-tabs{display:flex;gap:8px;margin-bottom:14px}
.svc-tab{flex:1;padding:10px;border-radius:var(--r-sm);border:0.5px solid var(--border2);background:transparent;font-size:13px;font-weight:500;color:var(--text2);cursor:pointer;font-family:inherit;transition:all .15s;text-align:center}
.svc-tab.active-A{background:var(--sA-bg);color:var(--sA-text);border-color:var(--sA-border)}
.svc-tab.active-B{background:var(--sB-bg);color:var(--sB-text);border-color:var(--sB-border)}
.staff-entry{display:flex;align-items:center;gap:10px;padding:11px 0;border-bottom:0.5px solid var(--border)}
.staff-entry:last-child{border-bottom:none}
.staff-num{font-size:15px;font-weight:500;min-width:52px}
.staff-num.color-A{color:var(--sA)}
.staff-num.color-B{color:var(--sB)}
.staff-info{flex:1;min-width:0}
.staff-name{font-size:13px;font-weight:500;color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.staff-meta{font-size:11px;color:var(--text3);margin-top:1px}
.staff-btns{display:flex;gap:5px;flex-shrink:0}
.log-item{display:flex;gap:8px;padding:8px 0;border-bottom:0.5px solid var(--border);font-size:12px;align-items:flex-start}
.log-item:last-child{border-bottom:none}
.log-dot{width:6px;height:6px;border-radius:50%;margin-top:4px;flex-shrink:0}
.log-dot.A{background:var(--sA)}
.log-dot.B{background:var(--sB)}
.log-text{flex:1;color:var(--text2);line-height:1.5}
.log-time{color:var(--text3);flex-shrink:0}
.setting-row{display:flex;align-items:center;gap:10px;padding:10px 0;border-bottom:0.5px solid var(--border)}
.setting-row:last-child{border-bottom:none}
.setting-label{font-size:13px;color:var(--text2);flex:1}
.setting-input{padding:6px 10px;border:0.5px solid var(--border2);border-radius:var(--r-sm);background:var(--bg);color:var(--text);font-size:13px;font-family:inherit;width:80px;text-align:center}
.empty{font-size:13px;color:var(--text3);font-style:italic;padding:8px 0}
.divider{height:0.5px;background:var(--border);margin:12px 0}
/* 登入畫面 */
#login-screen{display:flex;align-items:center;justify-content:center;min-height:100vh;padding:20px}
.login-box{background:var(--bg);border:0.5px solid var(--border);border-radius:var(--r);padding:32px 24px;width:100%;max-width:320px;text-align:center}
.login-box h2{font-size:18px;font-weight:500;margin-bottom:6px}
.login-box p{font-size:13px;color:var(--text2);margin-bottom:24px}
.login-input{width:100%;padding:12px;border:0.5px solid var(--border2);border-radius:var(--r-sm);background:var(--bg2);color:var(--text);font-size:16px;font-family:inherit;text-align:center;letter-spacing:4px;margin-bottom:12px}
.toast{position:fixed;bottom:24px;left:50%;transform:translateX(-50%) translateY(80px);background:var(--text);color:var(--bg);padding:10px 22px;border-radius:99px;font-size:13px;font-weight:500;transition:transform .25s;z-index:999;white-space:nowrap;pointer-events:none}
.toast.show{transform:translateX(-50%) translateY(0)}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
</style>
</head>
<body>

<!-- 登入畫面 -->
<div id="login-screen">
  <div class="login-box">
    <div style="font-size:32px;margin-bottom:12px">🔒</div>
    <h2>工作人員登入</h2>
    <p>請輸入管理密碼</p>
    <input class="login-input" type="password" id="pwd-input" placeholder="••••" maxlength="20"/>
    <button class="btn btn-primary" style="margin-bottom:0" onclick="doLogin()">登入</button>
    <div id="login-err" style="font-size:12px;color:var(--red);margin-top:10px;display:none">密碼錯誤，請再試一次</div>
  </div>
</div>

<!-- 主畫面（登入後顯示） -->
<div id="main-screen" style="display:none">
  <div class="app">
    <div class="topbar">
      <div class="live-dot"></div>
      <div class="topbar-title">工作人員管理</div>
      <button onclick="doLogout()" style="font-size:12px;color:var(--text3);border:none;background:transparent;cursor:pointer;padding:4px 8px">登出</button>
    </div>
    <div class="tabs">
      <button class="tab active" onclick="goTab('queue')" id="tab-queue">叫號</button>
      <button class="tab" onclick="goTab('log')" id="tab-log">記錄</button>
      <button class="tab" onclick="goTab('settings')" id="tab-settings">設定</button>
    </div>

    <!-- 叫號 -->
    <div class="panel active" id="panel-queue">
      <div class="svc-tabs">
        <button class="svc-tab active-A" id="staff-tab-A" onclick="setStaffSvc('A')"></button>
        <button class="svc-tab" id="staff-tab-B" onclick="setStaffSvc('B')"></button>
      </div>

      <div class="card">
        <div class="card-title">叫號操作</div>
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:14px">
          <div style="text-align:center;min-width:80px">
            <div style="font-size:11px;color:var(--text3);margin-bottom:2px">目前服務</div>
            <div style="font-size:36px;font-weight:500;line-height:1" id="staff-cur">—</div>
          </div>
          <div style="flex:1">
            <button class="btn btn-primary" style="margin-bottom:8px" onclick="callNext()">叫下一號 →</button>
            <button class="btn" style="margin-bottom:0" onclick="repeatCall()">重複叫號</button>
          </div>
        </div>
        <div id="staff-register-section">
          <div class="divider" style="margin:0 0 12px"></div>
          <div style="font-size:12px;color:var(--text3);margin-bottom:8px" id="staff-register-label">結帳後幫客人登記候位</div>
          <div style="display:flex;gap:8px;margin-bottom:8px">
            <input type="text" id="staff-inp-name" placeholder="客人姓名" style="flex:1;padding:8px 10px;border:0.5px solid var(--border2);border-radius:var(--r-sm);background:var(--bg);color:var(--text);font-size:13px;font-family:inherit"/>
            <input type="tel" id="staff-inp-phone" placeholder="09xxxxxxxx" style="flex:1;padding:8px 10px;border:0.5px solid var(--border2);border-radius:var(--r-sm);background:var(--bg);color:var(--text);font-size:13px;font-family:inherit"/>
          </div>
          <button class="btn btn-A" id="staff-take-btn" onclick="staffTakeNumber()" style="margin-bottom:0"></button>
        </div>
      </div>

      <div class="card">
        <div class="card-title" style="margin-bottom:10px">候位名單</div>
        <div id="staff-list"><span class="empty">目前無人候位</span></div>
      </div>

      <div class="card">
        <div class="card-title">統計</div>
        <div class="stat-row"><span class="stat-label">等候人數</span><span class="stat-val" id="staff-waiting">0</span></div>
        <div class="stat-row"><span class="stat-label">今日已服務</span><span class="stat-val" id="staff-served">0</span></div>
        <div class="stat-row" style="border:none"><span class="stat-label">最後發號</span><span class="stat-val" id="staff-last">—</span></div>
      </div>

      <button class="btn btn-danger" onclick="resetSvc()" style="margin-bottom:6px">重置此服務今日號碼</button>
      <button class="btn btn-danger" onclick="resetAll()" style="opacity:.7">重置所有服務</button>
    </div>

    <!-- 記錄 -->
    <div class="panel" id="panel-log">
      <div class="card">
        <div class="card-title" style="margin-bottom:8px">操作記錄</div>
        <div id="notify-log"><span class="empty">尚無記錄</span></div>
      </div>
    </div>

    <!-- 設定 -->
    <div class="panel" id="panel-settings">
      <div class="card">
        <div class="card-title">系統名稱</div>
        <div class="setting-row" style="border:none">
          <span class="setting-label">顯示名稱</span>
          <input class="setting-input" style="width:160px" id="set-system-name"/>
        </div>
      </div>
      <div class="card">
        <div class="card-title">服務 A</div>
        <div class="setting-row"><span class="setting-label">名稱</span><input class="setting-input" style="width:120px" id="set-nameA"/></div>
        <div class="setting-row"><span class="setting-label">號碼前綴</span><input class="setting-input" id="set-prefixA" maxlength="3"/></div>
        <div class="setting-row" style="border:none"><span class="setting-label">每號時間（分）</span><input class="setting-input" type="number" id="set-timeA" min="1" max="120"/></div>
      </div>
      <div class="card">
        <div class="card-title">服務 B</div>
        <div class="setting-row"><span class="setting-label">名稱</span><input class="setting-input" style="width:120px" id="set-nameB"/></div>
        <div class="setting-row"><span class="setting-label">號碼前綴</span><input class="setting-input" id="set-prefixB" maxlength="3"/></div>
        <div class="setting-row" style="border:none"><span class="setting-label">每號時間（分）</span><input class="setting-input" type="number" id="set-timeB" min="1" max="120"/></div>
      </div>
      <div class="card">
        <div class="card-title">管理密碼</div>
        <div class="setting-row" style="border:none"><span class="setting-label">新密碼</span><input class="setting-input" type="password" id="set-pwd" placeholder="輸入新密碼" style="width:120px"/></div>
      </div>
      <button class="btn btn-primary" onclick="saveSettings()">儲存設定</button>
    </div>
  </div>
</div>

<div class="toast" id="toast"></div>

<script>
const BACKEND_URL = 'https://mercury-gcac.onrender.com';
const DEFAULT_PWD = '1234';

let state = {
  A: { current:0, lastIssued:0, queue:[], history:[], servedToday:0 },
  B: { current:0, lastIssued:0, queue:[], history:[], servedToday:0 }
};
let cfg = {
  systemName: '排隊系統',
  services: {
    A: { name: '心願瓶DIY', prefix: 'A', minutes: 15 },
    B: { name: '塔羅牌占卜', prefix: 'T', minutes: 20 }
  }
};
let notifyLog = [];
let currentStaffSvc = 'A';

function fmt(svc, n) { return cfg.services[svc].prefix + String(n).padStart(3,'0'); }
function nowTime() { return new Date().toLocaleTimeString('zh-TW',{hour:'2-digit',minute:'2-digit'}); }
function showToast(msg) {
  const el = document.getElementById('toast');
  el.textContent = msg; el.classList.add('show');
  setTimeout(()=>el.classList.remove('show'), 2400);
}

// ── 登入 ──────────────────────────────────────────
function doLogin() {
  const pwd = document.getElementById('pwd-input').value;
  const savedPwd = localStorage.getItem('staff_pwd') || DEFAULT_PWD;
  if (pwd === savedPwd) {
    document.getElementById('login-screen').style.display = 'none';
    document.getElementById('main-screen').style.display = 'block';
    syncFromServer();
  } else {
    document.getElementById('login-err').style.display = 'block';
    document.getElementById('pwd-input').value = '';
  }
}
function doLogout() {
  document.getElementById('login-screen').style.display = 'flex';
  document.getElementById('main-screen').style.display = 'none';
  document.getElementById('pwd-input').value = '';
}
document.getElementById('pwd-input').addEventListener('keydown', e => {
  if (e.key === 'Enter') doLogin();
});

// ── Tab ──────────────────────────────────────────
function goTab(name) {
  ['queue','log','settings'].forEach(t => {
    document.getElementById('panel-'+t).classList.toggle('active', t===name);
    document.getElementById('tab-'+t).classList.toggle('active', t===name);
  });
}
function setStaffSvc(s) { currentStaffSvc = s; renderStaff(); }

// ── 同步 ──────────────────────────────────────────
async function syncFromServer() {
  try {
    const res = await fetch(BACKEND_URL + '/api/state');
    const data = await res.json();
    if (data.state) state = data.state;
    if (data.cfg) { cfg = data.cfg; loadSettingsUI(); }
    renderStaff();
  } catch(e) {}
}

async function sendLineNotify(userId, phone, name, message) {
  if ((!userId || userId==='—') && !phone) return;
  try {
    await fetch(BACKEND_URL + '/api/line-notify', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ userId: userId!=='—'?userId:null, phone, name, message })
    });
  } catch(e) {}
}

function addLog(svc, msg) {
  notifyLog.unshift({ svc, msg, time: nowTime() });
  renderLog();
}

// ── 叫號 ──────────────────────────────────────────
async function callNext() {
  if (state[currentStaffSvc].queue.length === 0) { showToast('目前無人候位'); return; }
  try {
    const res = await fetch(BACKEND_URL + '/api/call-next', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ svc: currentStaffSvc })
    });
    const data = await res.json();
    if (!data.success) { showToast(data.error || '叫號失敗'); return; }
    const entry = data.called;
    addLog(currentStaffSvc, \`叫號 \${fmt(currentStaffSvc, entry.num)}（\${entry.name}）\`);
    const svcIcon = currentStaffSvc === 'B' ? '🔮' : '🫙';
    const svcName = cfg.services[currentStaffSvc].name;
    sendLineNotify(entry.userId, entry.phone, entry.name,
      \`\${svcIcon} \${svcName}｜📢 \${entry.name} 您好！現在叫到 \${fmt(currentStaffSvc, entry.num)} 號，請立即回到現場，謝謝！\`);
    await syncFromServer();
    if (state[currentStaffSvc].queue.length > 0) {
      const next = state[currentStaffSvc].queue[0];
      addLog(currentStaffSvc, \`提醒 \${next.name}（\${fmt(currentStaffSvc, next.num)}）準備\`);
      sendLineNotify(next.userId, next.phone, next.name,
        \`\${svcIcon} \${svcName}｜⏰ \${next.name} 您好！您是下一位（\${fmt(currentStaffSvc, next.num)} 號），請提前回到現場準備。\`);
    }
    showToast('已叫號：' + fmt(currentStaffSvc, entry.num));
  } catch(e) { showToast('網路錯誤'); }
}

function repeatCall() {
  const cur = state[currentStaffSvc].current;
  if (!cur) { showToast('尚未開始叫號'); return; }
  addLog(currentStaffSvc, \`重複叫號 \${fmt(currentStaffSvc, cur)}\`);
  showToast('已重複叫號');
}

async function staffTakeNumber() {
  const name = document.getElementById('staff-inp-name').value.trim();
  const phone = document.getElementById('staff-inp-phone').value.trim();
  if (!name) { showToast('請輸入客人姓名'); return; }
  if (!phone || !/^09\d{8}$/.test(phone)) { showToast('請輸入客人手機號碼'); return; }
  try {
    const res = await fetch(BACKEND_URL + '/api/issue', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ svc: currentStaffSvc, name, phone, userId: null })
    });
    const data = await res.json();
    if (!data.success) { showToast('登記失敗'); return; }
    addLog(currentStaffSvc, \`登記 \${name}（\${fmt(currentStaffSvc, data.num)}）\`);
    document.getElementById('staff-inp-name').value = '';
    document.getElementById('staff-inp-phone').value = '';
    await syncFromServer();
    showToast(\`已為 \${name} 登記 \${fmt(currentStaffSvc, data.num)} 號\`);
  } catch(e) { showToast('網路錯誤'); }
}

async function notifyPerson(svc, num) {
  const entry = state[svc].queue.find(q => q.num === num);
  if (!entry) return;
  const pos = state[svc].queue.indexOf(entry);
  const concurrent = svc === 'A' ? 5 : 2;
  const est = Math.max(0, Math.ceil((pos + 1) / concurrent) - 1) * cfg.services[svc].minutes || cfg.services[svc].minutes;
  const svcIcon2 = svc === 'B' ? '🔮' : '🫙';
  sendLineNotify(entry.userId, entry.phone, entry.name,
    \`\${svcIcon2} \${cfg.services[svc].name}｜⏰ \${entry.name} 您好！您的 \${fmt(svc, num)} 號預計約 \${est} 分鐘後叫號，請提前回到現場準備。\`);
  addLog(svc, \`提醒 \${entry.name}（\${fmt(svc, num)}），約 \${est} 分鐘後\`);
  showToast('已傳送提醒');
}

async function cancelPerson(svc, num) {
  const entry = state[svc].queue.find(q => q.num === num);
  if (!entry || !confirm(\`確定取消 \${fmt(svc, num)} 號（\${entry.name}）？\`)) return;
  await fetch(BACKEND_URL + '/api/cancel', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ svc, num })
  });
  addLog(svc, \`取消 \${entry.name}（\${fmt(svc, num)}）候位\`);
  await syncFromServer();
  showToast('已取消候位');
}

async function resetSvc() {
  if (!confirm(\`確定重置「\${cfg.services[currentStaffSvc].name}」？\`)) return;
  await fetch(BACKEND_URL + '/api/reset', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ svc: currentStaffSvc })
  });
  await syncFromServer(); showToast('已重置');
}
async function resetAll() {
  if (!confirm('確定重置所有服務？')) return;
  await fetch(BACKEND_URL + '/api/reset', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({})
  });
  notifyLog = [];
  await syncFromServer(); showToast('已重置所有服務');
}

// ── 設定 ──────────────────────────────────────────
function loadSettingsUI() {
  document.getElementById('set-system-name').value = cfg.systemName || '';
  document.getElementById('set-nameA').value = cfg.services.A.name || '';
  document.getElementById('set-prefixA').value = cfg.services.A.prefix || '';
  document.getElementById('set-timeA').value = cfg.services.A.minutes || 15;
  document.getElementById('set-nameB').value = cfg.services.B.name || '';
  document.getElementById('set-prefixB').value = cfg.services.B.prefix || '';
  document.getElementById('set-timeB').value = cfg.services.B.minutes || 20;
}
async function saveSettings() {
  cfg.systemName = document.getElementById('set-system-name').value.trim() || '排隊系統';
  cfg.services.A.name = document.getElementById('set-nameA').value.trim() || '服務A';
  cfg.services.A.prefix = document.getElementById('set-prefixA').value.trim() || 'A';
  cfg.services.A.minutes = parseInt(document.getElementById('set-timeA').value) || 15;
  cfg.services.B.name = document.getElementById('set-nameB').value.trim() || '服務B';
  cfg.services.B.prefix = document.getElementById('set-prefixB').value.trim() || 'T';
  cfg.services.B.minutes = parseInt(document.getElementById('set-timeB').value) || 20;
  const newPwd = document.getElementById('set-pwd').value.trim();
  if (newPwd) { localStorage.setItem('staff_pwd', newPwd); document.getElementById('set-pwd').value = ''; }
  await fetch(BACKEND_URL + '/api/state', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ cfg })
  });
  renderStaff(); showToast('設定已儲存');
}

// ── 渲染 ──────────────────────────────────────────
function renderStaff() {
  const svc = currentStaffSvc;
  ['A','B'].forEach(s => {
    const el = document.getElementById('staff-tab-'+s);
    el.textContent = cfg.services[s].name;
    el.className = 'svc-tab' + (s===svc ? ' active-'+s : '');
  });
  document.getElementById('staff-take-btn').textContent = \`登記 \${cfg.services[svc].name} 候位\`;
  document.getElementById('staff-register-label').textContent = '結帳後幫客人登記候位';
  // 塔羅牌（B）不需要手動登記欄位
  const showRegister = svc === 'A';
  document.getElementById('staff-register-section').style.display = showRegister ? 'block' : 'none';

  const cur = state[svc].current;
  const curEl = document.getElementById('staff-cur');
  curEl.textContent = cur > 0 ? fmt(svc, cur) : '—';
  curEl.style.color = cur > 0 ? (svc==='A'?'var(--sA)':'var(--sB)') : 'var(--text)';

  document.getElementById('staff-waiting').textContent = state[svc].queue.length;
  document.getElementById('staff-served').textContent = state[svc].servedToday;
  document.getElementById('staff-last').textContent = state[svc].lastIssued > 0 ? fmt(svc, state[svc].lastIssued) : '—';

  const list = document.getElementById('staff-list');
  const q = state[svc].queue;
  if (q.length === 0) { list.innerHTML = '<span class="empty">目前無人候位</span>'; return; }
  list.innerHTML = q.map((entry, i) => {
    const concurrent = svc === 'A' ? 5 : 2;
    const est = Math.max(0, Math.ceil((i + 1) / concurrent) - 1) * cfg.services[svc].minutes || cfg.services[svc].minutes;
    return \`<div class="staff-entry">
      <div class="staff-num color-\${svc}">\${fmt(svc, entry.num)}</div>
      <div class="staff-info">
        <div class="staff-name">\${entry.name}</div>
        <div class="staff-meta">\${entry.phone||''}｜\${i===0?'下一位':'約 '+est+' 分鐘'}</div>
      </div>
      <div class="staff-btns">
        <button class="btn btn-sm" style="color:var(--amber);border-color:var(--amber-b);background:var(--amber-bg)"
          onclick="notifyPerson('\${svc}',\${entry.num})">提醒</button>
        <button class="btn btn-sm" style="color:var(--red);border-color:var(--red-b)"
          onclick="cancelPerson('\${svc}',\${entry.num})">取消</button>
      </div>
    </div>\`;
  }).join('');
}

function renderLog() {
  const el = document.getElementById('notify-log');
  if (!el) return;
  if (notifyLog.length === 0) { el.innerHTML = '<span class="empty">尚無記錄</span>'; return; }
  el.innerHTML = notifyLog.slice(0,20).map(l =>
    \`<div class="log-item">
      <div class="log-dot \${l.svc}"></div>
      <div class="log-text">[\${cfg.services[l.svc]?.name||l.svc}] \${l.msg}</div>
      <div class="log-time">\${l.time}</div>
    </div>\`
  ).join('');
}

// 初始化
syncFromServer();
setInterval(syncFromServer, 4000);
</script>
</body>
</html>
`); });
app.get('/staff/checkout', (req, res) => { res.setHeader('Content-Type', 'text/html; charset=utf-8'); res.send(`<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>結帳櫃檯｜心願瓶登記</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#fff;--bg2:#f6f5f2;--bg3:#eeede9;
  --text:#1a1a1a;--text2:#5a5a5a;--text3:#999;
  --border:rgba(0,0,0,0.1);--border2:rgba(0,0,0,0.2);
  --r:10px;--r-sm:6px;
  --sA:#3b5bdb;--sA-bg:#eef2ff;--sA-border:#a5b4fc;--sA-text:#1e3a8a;
  --green:#3b6d11;--green-bg:#eaf3de;--green-b:#97c459;
  --amber:#854f0b;--amber-bg:#faeeda;--amber-b:#ef9f27;
}
@media(prefers-color-scheme:dark){
  :root{
    --bg:#1c1c1e;--bg2:#2c2c2e;--bg3:#3a3a3c;
    --text:#f2f2f7;--text2:#aeaeb2;--text3:#636366;
    --border:rgba(255,255,255,0.1);--border2:rgba(255,255,255,0.2);
    --sA:#818cf8;--sA-bg:#1e1b4b;--sA-border:#4338ca;--sA-text:#c7d2fe;
    --green:#c0dd97;--green-bg:#173404;--green-b:#3b6d11;
    --amber:#fac775;--amber-bg:#412402;--amber-b:#854f0b;
  }
}
body{font-family:-apple-system,BlinkMacSystemFont,'Noto Sans TC',sans-serif;background:var(--bg2);color:var(--text);min-height:100vh}
.app{max-width:480px;margin:0 auto;padding-bottom:48px}
.topbar{background:var(--bg);border-bottom:0.5px solid var(--border);padding:14px 16px;display:flex;align-items:center;gap:10px;position:sticky;top:0;z-index:20}
.topbar-title{font-size:15px;font-weight:500;flex:1}
.topbar-sub{font-size:12px;color:var(--text3)}
.live-dot{width:7px;height:7px;background:#639922;border-radius:50%;animation:pulse 1.5s infinite;flex-shrink:0}
.card{background:var(--bg);border:0.5px solid var(--border);border-radius:var(--r);padding:16px;margin:14px 14px 0}
.card-title{font-size:11px;font-weight:500;color:var(--text3);letter-spacing:.05em;text-transform:uppercase;margin-bottom:12px}
.field{display:flex;flex-direction:column;gap:5px;margin-bottom:12px}
.field label{font-size:12px;color:var(--text2);font-weight:500}
.field input{padding:12px;border:0.5px solid var(--border2);border-radius:var(--r-sm);background:var(--bg);color:var(--text);font-size:16px;font-family:inherit;width:100%}
.field input:focus{outline:none;border-color:var(--sA)}
.btn{display:flex;align-items:center;justify-content:center;padding:14px 18px;border:0.5px solid var(--border2);border-radius:var(--r-sm);background:transparent;font-size:15px;font-weight:500;color:var(--text);cursor:pointer;font-family:inherit;transition:all .15s;width:100%}
.btn:active{transform:scale(.97)}
.btn-A{background:var(--sA);color:#fff;border-color:var(--sA)}
.btn-A:hover{opacity:.9}
.stat-row{display:flex;justify-content:space-between;align-items:center;padding:10px 0;border-bottom:0.5px solid var(--border)}
.stat-row:last-child{border-bottom:none}
.stat-label{font-size:13px;color:var(--text2)}
.stat-val{font-size:15px;font-weight:500;color:var(--text)}
.success-banner{background:var(--green-bg);border:0.5px solid var(--green-b);border-radius:var(--r-sm);padding:12px 16px;margin:14px 14px 0;display:none}
.success-banner.show{display:block}
.success-num{font-size:32px;font-weight:500;color:var(--green);text-align:center;margin-bottom:4px}
.success-sub{font-size:13px;color:var(--green);text-align:center}
.toast{position:fixed;bottom:24px;left:50%;transform:translateX(-50%) translateY(80px);background:var(--text);color:var(--bg);padding:10px 22px;border-radius:99px;font-size:13px;font-weight:500;transition:transform .25s;z-index:999;white-space:nowrap;pointer-events:none}
.toast.show{transform:translateX(-50%) translateY(0)}
.party-btn{padding:8px 16px;border:0.5px solid var(--border2);border-radius:var(--r-sm);background:transparent;font-size:14px;font-weight:500;color:var(--text2);cursor:pointer;font-family:inherit;transition:all .15s}
.party-btn.active{background:var(--sA);color:#fff;border-color:var(--sA)}
.party-btn:hover:not(.active){background:var(--bg3)}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
</style>
</head>
<body>
<div class="app">
  <div class="topbar">
    <div class="live-dot"></div>
    <div class="topbar-title">🫙 結帳櫃檯</div>
    <div class="topbar-sub">心願瓶候位登記</div>
  </div>

  <!-- 成功提示 -->
  <div class="success-banner" id="success-banner">
    <div class="success-num" id="success-num">A001</div>
    <div class="success-sub" id="success-sub">登記成功，請告知客人號碼</div>
  </div>

  <!-- 提醒提示 -->
  <div style="background:var(--amber-bg);border:0.5px solid var(--amber-b);border-radius:var(--r);padding:14px 16px;margin:14px 14px 0">
    <div style="font-size:13px;font-weight:500;color:var(--amber);margin-bottom:6px">📋 結帳前請提醒客人</div>
    <div style="font-size:13px;color:var(--amber);line-height:1.8">
      1. 加入 LINE 官方帳號好友<br/>
      2. 在聊天室傳送手機號碼（例：0912345678）<br/>
      3. 收到綁定成功訊息後再進行結帳登記
    </div>
  </div>

  <!-- 登記表單 -->
  <div class="card">
    <div class="card-title">登記候位</div>
    <div class="field">
      <label>客人姓名</label>
      <input type="text" id="inp-name" placeholder="請輸入姓名" autocomplete="off"/>
    </div>
    <div class="field">
      <label>人數</label>
      <div style="display:flex;gap:8px;flex-wrap:wrap" id="party-btns">
        <button type="button" class="party-btn active" onclick="setParty(1)">1 人</button>
        <button type="button" class="party-btn" onclick="setParty(2)">2 人</button>
        <button type="button" class="party-btn" onclick="setParty(3)">3 人</button>
        <button type="button" class="party-btn" onclick="setParty(4)">4 人</button>
        <button type="button" class="party-btn" onclick="setParty(5)">5 人</button>
        <button type="button" class="party-btn" onclick="setParty(6)">6 人</button>
      </div>
    </div>
    <div class="field" style="margin-bottom:16px">
      <label>手機號碼（用於 LINE 通知）</label>
      <input type="tel" id="inp-phone" placeholder="09xxxxxxxx"/>
    </div>
    <button class="btn btn-A" onclick="register()">登記候位</button>
  </div>

  <!-- 等候狀況 -->
  <div class="card">
    <div class="card-title">目前狀況</div>
    <div class="stat-row">
      <span class="stat-label">等候人數</span>
      <span class="stat-val" id="waiting">0 人</span>
    </div>
    <div class="stat-row">
      <span class="stat-label">預估等待</span>
      <span class="stat-val" id="est">無需等候</span>
    </div>
    <div class="stat-row" style="border:none">
      <span class="stat-label">目前服務號</span>
      <span class="stat-val" id="current">—</span>
    </div>
  </div>
</div>
<div class="toast" id="toast"></div>

<script>
const BACKEND_URL = 'https://mercury-gcac.onrender.com';
let partySize = 1;

function setParty(n) {
  partySize = n;
  document.querySelectorAll('.party-btn').forEach((btn, i) => {
    btn.classList.toggle('active', i + 1 === n);
  });
}
let cfg = { services: { A: { name: '心願瓶DIY', prefix: 'A', minutes: 15 } } };
let state = { A: { current: 0, lastIssued: 0, queue: [], servedToday: 0 } };

function fmt(n) { return cfg.services.A.prefix + String(n).padStart(3,'0'); }
function showToast(msg) {
  const el = document.getElementById('toast');
  el.textContent = msg; el.classList.add('show');
  setTimeout(() => el.classList.remove('show'), 2400);
}

async function syncFromServer() {
  try {
    const res = await fetch(BACKEND_URL + '/api/state');
    const data = await res.json();
    if (data.state) state = data.state;
    if (data.cfg) cfg = data.cfg;
    renderStatus();
  } catch(e) {}
}

async function sendLineNotify(phone, name, message) {
  if (!phone) return;
  try {
    await fetch(BACKEND_URL + '/api/line-notify', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ phone, name, message })
    });
  } catch(e) {}
}

function renderStatus() {
  const q = state.A.queue;
  const mins = cfg.services.A.minutes;
  document.getElementById('waiting').textContent = q.length + ' 人';
  const totalCap = q.reduce((sum, e) => sum + (e.partySize || 1), 0);
  const estA = q.length > 0 ? Math.max(0, Math.ceil(totalCap / 5) - 1) * mins : 0;
  document.getElementById('est').textContent = q.length > 0 ? (estA > 0 ? '約 ' + estA + ' 分鐘' : '即將輪到') : '無需等候';
  document.getElementById('current').textContent = state.A.current > 0 ? fmt(state.A.current) : '—';
}

async function register() {
  const name = document.getElementById('inp-name').value.trim();
  const rawPhone = document.getElementById('inp-phone').value.trim();
  const cleanPhone = rawPhone.split('').filter(c => c >= '0' && c <= '9').join('');
  if (!name) { showToast('請輸入客人姓名'); return; }
  if (cleanPhone.length !== 10 || cleanPhone.slice(0,2) !== '09') {
    showToast('請輸入有效手機號碼（格式：09xxxxxxxx）'); return;
  }
  try {
    const res = await fetch(BACKEND_URL + '/api/issue', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ svc: 'A', name, phone: cleanPhone, userId: null, partySize })
    });
    const data = await res.json();
    if (!data.success) { showToast('登記失敗，請再試一次'); return; }
    const numStr = fmt(data.num);
    await syncFromServer();
    const waiting = state.A.queue.length;
    const totalCapAfter = state.A.queue.reduce((sum, e) => sum + (e.partySize || 1), 0);
    const est = Math.max(0, Math.ceil(totalCapAfter / 5) - 1) * cfg.services.A.minutes;
    const waitMsg = totalCapAfter <= 5 ? '目前正在服務中，請稍候片刻！' : \`預計約 \${est} 分鐘後輪到您。\`;
    sendLineNotify(cleanPhone, name,
      \`🫙 心願瓶DIY｜✅ \${name} 您好！已成功登記候位，您的號碼是 \${numStr}（\${partySize} 人）。\${waitMsg}輪到您時我們會再通知您 🙏\`);
    document.getElementById('success-num').textContent = numStr;
    document.getElementById('success-sub').textContent = \`已傳送 LINE 通知給 \${name}\`;
    document.getElementById('success-banner').classList.add('show');
    document.getElementById('inp-name').value = '';
    document.getElementById('inp-phone').value = '';
    document.getElementById('inp-name').focus();
    setParty(1);
    setTimeout(() => document.getElementById('success-banner').classList.remove('show'), 5000);
  } catch(e) { showToast('網路錯誤，請再試一次'); }
}
// Enter 鍵送出
document.addEventListener('keydown', e => {
  if (e.key === 'Enter') register();
});

syncFromServer();
setInterval(syncFromServer, 4000);
</script>
</body>
</html>
`); });
app.get('/staff/wishbottle', (req, res) => { res.setHeader('Content-Type', 'text/html; charset=utf-8'); res.send(`<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>領瓶處｜心願瓶叫號</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#fff;--bg2:#f6f5f2;--bg3:#eeede9;
  --text:#1a1a1a;--text2:#5a5a5a;--text3:#999;
  --border:rgba(0,0,0,0.1);--border2:rgba(0,0,0,0.2);
  --r:10px;--r-sm:6px;
  --sA:#3b5bdb;--sA-bg:#eef2ff;--sA-border:#a5b4fc;--sA-text:#1e3a8a;
  --green:#3b6d11;--green-bg:#eaf3de;--green-b:#97c459;
  --amber:#854f0b;--amber-bg:#faeeda;--amber-b:#ef9f27;
  --red:#a32d2d;--red-bg:#fcebeb;--red-b:#f09595;
}
@media(prefers-color-scheme:dark){
  :root{
    --bg:#1c1c1e;--bg2:#2c2c2e;--bg3:#3a3a3c;
    --text:#f2f2f7;--text2:#aeaeb2;--text3:#636366;
    --border:rgba(255,255,255,0.1);--border2:rgba(255,255,255,0.2);
    --sA:#818cf8;--sA-bg:#1e1b4b;--sA-border:#4338ca;--sA-text:#c7d2fe;
    --green:#c0dd97;--green-bg:#173404;--green-b:#3b6d11;
    --amber:#fac775;--amber-bg:#412402;--amber-b:#854f0b;
    --red:#f7c1c1;--red-bg:#501313;--red-b:#a32d2d;
  }
}
body{font-family:-apple-system,BlinkMacSystemFont,'Noto Sans TC',sans-serif;background:var(--bg2);color:var(--text);min-height:100vh}
.app{max-width:480px;margin:0 auto;padding-bottom:48px}
.topbar{background:var(--bg);border-bottom:0.5px solid var(--border);padding:14px 16px;display:flex;align-items:center;gap:10px;position:sticky;top:0;z-index:20}
.topbar-title{font-size:15px;font-weight:500;flex:1}
.topbar-sub{font-size:12px;color:var(--text3)}
.live-dot{width:7px;height:7px;background:#639922;border-radius:50%;animation:pulse 1.5s infinite;flex-shrink:0}
.card{background:var(--bg);border:0.5px solid var(--border);border-radius:var(--r);padding:16px;margin:14px 14px 0}
.card-title{font-size:11px;font-weight:500;color:var(--text3);letter-spacing:.05em;text-transform:uppercase;margin-bottom:12px}
.big-num{font-size:80px;font-weight:500;line-height:1;letter-spacing:-2px;text-align:center;color:var(--sA)}
.big-sub{font-size:12px;color:var(--text3);text-align:center;margin-top:6px}
.btn{display:flex;align-items:center;justify-content:center;padding:14px 18px;border:0.5px solid var(--border2);border-radius:var(--r-sm);background:transparent;font-size:15px;font-weight:500;color:var(--text);cursor:pointer;font-family:inherit;transition:all .15s;width:100%;margin-bottom:8px}
.btn:last-child{margin-bottom:0}
.btn:active{transform:scale(.97)}
.btn-primary{background:var(--sA);color:#fff;border-color:var(--sA)}
.btn-primary:hover{opacity:.9}
.btn-sm{padding:6px 12px;font-size:12px;width:auto;margin-bottom:0}
.staff-entry{display:flex;align-items:center;gap:10px;padding:11px 0;border-bottom:0.5px solid var(--border)}
.staff-entry:last-child{border-bottom:none}
.staff-num{font-size:15px;font-weight:500;min-width:52px;color:var(--sA)}
.staff-info{flex:1;min-width:0}
.staff-name{font-size:13px;font-weight:500;color:var(--text)}
.staff-meta{font-size:11px;color:var(--text3);margin-top:1px}
.staff-btns{display:flex;gap:5px;flex-shrink:0}
.stat-row{display:flex;justify-content:space-between;align-items:center;padding:9px 0;border-bottom:0.5px solid var(--border)}
.stat-row:last-child{border-bottom:none}
.stat-label{font-size:13px;color:var(--text2)}
.stat-val{font-size:14px;font-weight:500;color:var(--text)}
.empty{font-size:13px;color:var(--text3);font-style:italic}
.toast{position:fixed;bottom:24px;left:50%;transform:translateX(-50%) translateY(80px);background:var(--text);color:var(--bg);padding:10px 22px;border-radius:99px;font-size:13px;font-weight:500;transition:transform .25s;z-index:999;white-space:nowrap;pointer-events:none}
.toast.show{transform:translateX(-50%) translateY(0)}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
</style>
</head>
<body>
<div class="app">
  <div class="topbar">
    <div class="live-dot"></div>
    <div class="topbar-title">🫙 領瓶處</div>
    <div class="topbar-sub">心願瓶叫號</div>
  </div>

  <!-- 目前叫號 -->
  <div class="card">
    <div style="padding:16px 0 12px">
      <div class="big-num" id="cur-num">—</div>
      <div class="big-sub" id="cur-label">等待開始</div>
    </div>
    <button class="btn btn-primary" onclick="callNext()">叫下一號 →</button>
    <button class="btn" onclick="repeatCall()">重複叫號</button>
  </div>

  <!-- 統計 -->
  <div class="card">
    <div class="card-title">今日狀況</div>
    <div class="stat-row"><span class="stat-label">等候人數</span><span class="stat-val" id="waiting">0</span></div>
    <div class="stat-row"><span class="stat-label">今日已服務</span><span class="stat-val" id="served">0</span></div>
    <div class="stat-row" style="border:none"><span class="stat-label">預估等待</span><span class="stat-val" id="est">—</span></div>
  </div>

  <!-- 候位名單 -->
  <div class="card">
    <div class="card-title" style="margin-bottom:10px">候位名單</div>
    <div id="queue-list"><span class="empty">目前無人候位</span></div>
  </div>

  <!-- 重置 -->
  <div style="padding:14px 14px 0">
    <button class="btn" style="color:var(--red);border-color:var(--red-b);font-size:13px" onclick="resetSvc()">重置今日號碼</button>
  </div>
</div>
<div class="toast" id="toast"></div>

<script>
const BACKEND_URL = 'https://mercury-gcac.onrender.com';
let cfg = { services: { A: { name: '心願瓶DIY', prefix: 'A', minutes: 15 } } };
let state = { A: { current: 0, lastIssued: 0, queue: [], servedToday: 0 } };

function fmt(n) { return cfg.services.A.prefix + String(n).padStart(3,'0'); }
function showToast(msg) {
  const el = document.getElementById('toast');
  el.textContent = msg; el.classList.add('show');
  setTimeout(() => el.classList.remove('show'), 2400);
}

async function syncFromServer() {
  try {
    const res = await fetch(BACKEND_URL + '/api/state');
    const data = await res.json();
    if (data.state) state = data.state;
    if (data.cfg) cfg = data.cfg;
    render();
  } catch(e) {}
}

async function sendLineNotify(userId, phone, name, message) {
  if ((!userId || userId === '—') && !phone) return;
  try {
    await fetch(BACKEND_URL + '/api/line-notify', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ userId: userId !== '—' ? userId : null, phone, name, message })
    });
  } catch(e) {}
}

async function callNext() {
  if (state.A.queue.length === 0) { showToast('目前無人候位'); return; }
  try {
    const res = await fetch(BACKEND_URL + '/api/call-next', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ svc: 'A' })
    });
    const data = await res.json();
    if (!data.success) { showToast(data.error || '叫號失敗'); return; }
    const entry = data.called;
    sendLineNotify(entry.userId, entry.phone, entry.name,
      \`🫙 心願瓶DIY｜📢 \${entry.name} 您好！現在叫到 \${fmt(entry.num)} 號，請至領瓶處，謝謝！\`);
    await syncFromServer();
    if (state.A.queue.length > 0) {
      const next = state.A.queue[0];
      sendLineNotify(next.userId, next.phone, next.name,
        \`🫙 心願瓶DIY｜⏰ \${next.name} 您好！您是下一位（\${fmt(next.num)} 號），請提前回到現場準備。\`);
    }
    showToast('已叫號：' + fmt(entry.num));
  } catch(e) { showToast('網路錯誤'); }
}

function repeatCall() {
  if (!state.A.current) { showToast('尚未開始叫號'); return; }
  showToast('已重複叫號 ' + fmt(state.A.current));
}

async function notifyPerson(num) {
  const entry = state.A.queue.find(q => q.num === num);
  if (!entry) return;
  const pos = state.A.queue.indexOf(entry);
  const capSoFar = state.A.queue.slice(0, pos + 1).reduce((sum, e) => sum + (e.partySize || 1), 0);
  const est = Math.max(0, Math.ceil(capSoFar / 5) - 1) * cfg.services.A.minutes || cfg.services.A.minutes;
  sendLineNotify(entry.userId, entry.phone, entry.name,
    \`🫙 心願瓶DIY｜⏰ \${entry.name} 您好！您的 \${fmt(num)} 號預計約 \${est} 分鐘後叫號，請提前回到現場準備。\`);
  showToast('已傳送提醒給 ' + entry.name);
}

async function cancelPerson(num) {
  const entry = state.A.queue.find(q => q.num === num);
  if (!entry || !confirm(\`確定取消 \${fmt(num)} 號（\${entry.name}）？\`)) return;
  await fetch(BACKEND_URL + '/api/cancel', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ svc: 'A', num })
  });
  await syncFromServer();
  showToast('已取消候位');
}

async function resetSvc() {
  if (!confirm('確定重置今日心願瓶所有號碼？')) return;
  await fetch(BACKEND_URL + '/api/reset', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ svc: 'A' })
  });
  await syncFromServer();
  showToast('已重置');
}

function render() {
  const q = state.A.queue;
  const cur = state.A.current;
  const mins = cfg.services.A.minutes;
  document.getElementById('cur-num').textContent = cur > 0 ? fmt(cur) : '—';
  document.getElementById('cur-label').textContent = cur > 0 ? \`請 \${fmt(cur)} 號前往領瓶\` : '等待開始';
  document.getElementById('served').textContent = state.A.servedToday;
  document.getElementById('waiting').textContent = q.length + (totalCap > q.length ? \` (共 \${totalCap} 人)\` : '');
  const totalCap = q.reduce((sum, e) => sum + (e.partySize || 1), 0);
  const estA = q.length > 0 ? Math.max(0, Math.ceil(totalCap / 5) - 1) * mins : 0;
  document.getElementById('est').textContent = q.length > 0 ? (estA > 0 ? '約 ' + estA + ' 分鐘' : '即將輪到') : '—';

  const list = document.getElementById('queue-list');
  if (q.length === 0) { list.innerHTML = '<span class="empty">目前無人候位</span>'; return; }
  list.innerHTML = q.map((entry, i) => {
    const capSoFar = q.slice(0, i + 1).reduce((sum, e) => sum + (e.partySize || 1), 0);
    const est = Math.max(0, Math.ceil(capSoFar / 5) - 1) * mins || mins;
    return \`<div class="staff-entry">
      <div class="staff-num">\${fmt(entry.num)}</div>
      <div class="staff-info">
        <div class="staff-name">\${entry.name}</div>
        <div class="staff-meta">\${entry.phone || ''}｜\${i === 0 ? '下一位' : '約 ' + est + ' 分鐘'}</div>
      </div>
      <div class="staff-btns">
        <button class="btn btn-sm" style="color:var(--amber);border-color:var(--amber-b);background:var(--amber-bg)"
          onclick="notifyPerson(\${entry.num})">提醒</button>
        <button class="btn btn-sm" style="color:var(--red);border-color:var(--red-b)"
          onclick="cancelPerson(\${entry.num})">取消</button>
      </div>
    </div>\`;
  }).join('');
}

syncFromServer();
setInterval(syncFromServer, 4000);
</script>
</body>
</html>
`); });
app.get('/staff/tarot', (req, res) => { res.setHeader('Content-Type', 'text/html; charset=utf-8'); res.send(`<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>塔羅牌引導｜叫號</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#fff;--bg2:#f6f5f2;--bg3:#eeede9;
  --text:#1a1a1a;--text2:#5a5a5a;--text3:#999;
  --border:rgba(0,0,0,0.1);--border2:rgba(0,0,0,0.2);
  --r:10px;--r-sm:6px;
  --sB:#6d28d9;--sB-bg:#f5f3ff;--sB-border:#c4b5fd;--sB-text:#3b0764;
  --green:#3b6d11;--green-bg:#eaf3de;--green-b:#97c459;
  --amber:#854f0b;--amber-bg:#faeeda;--amber-b:#ef9f27;
  --red:#a32d2d;--red-bg:#fcebeb;--red-b:#f09595;
}
@media(prefers-color-scheme:dark){
  :root{
    --bg:#1c1c1e;--bg2:#2c2c2e;--bg3:#3a3a3c;
    --text:#f2f2f7;--text2:#aeaeb2;--text3:#636366;
    --border:rgba(255,255,255,0.1);--border2:rgba(255,255,255,0.2);
    --sB:#a78bfa;--sB-bg:#2e1065;--sB-border:#7c3aed;--sB-text:#ddd6fe;
    --green:#c0dd97;--green-bg:#173404;--green-b:#3b6d11;
    --amber:#fac775;--amber-bg:#412402;--amber-b:#854f0b;
    --red:#f7c1c1;--red-bg:#501313;--red-b:#a32d2d;
  }
}
body{font-family:-apple-system,BlinkMacSystemFont,'Noto Sans TC',sans-serif;background:var(--bg2);color:var(--text);min-height:100vh}
.app{max-width:480px;margin:0 auto;padding-bottom:48px}
.topbar{background:var(--bg);border-bottom:0.5px solid var(--border);padding:14px 16px;display:flex;align-items:center;gap:10px;position:sticky;top:0;z-index:20}
.topbar-title{font-size:15px;font-weight:500;flex:1}
.topbar-sub{font-size:12px;color:var(--text3)}
.live-dot{width:7px;height:7px;background:#639922;border-radius:50%;animation:pulse 1.5s infinite;flex-shrink:0}
.card{background:var(--bg);border:0.5px solid var(--border);border-radius:var(--r);padding:16px;margin:14px 14px 0}
.card-title{font-size:11px;font-weight:500;color:var(--text3);letter-spacing:.05em;text-transform:uppercase;margin-bottom:12px}
.big-num{font-size:80px;font-weight:500;line-height:1;letter-spacing:-2px;text-align:center;color:var(--sB)}
.big-sub{font-size:12px;color:var(--text3);text-align:center;margin-top:6px}
.btn{display:flex;align-items:center;justify-content:center;padding:14px 18px;border:0.5px solid var(--border2);border-radius:var(--r-sm);background:transparent;font-size:15px;font-weight:500;color:var(--text);cursor:pointer;font-family:inherit;transition:all .15s;width:100%;margin-bottom:8px}
.btn:last-child{margin-bottom:0}
.btn:active{transform:scale(.97)}
.btn-primary{background:var(--sB);color:#fff;border-color:var(--sB)}
.btn-primary:hover{opacity:.9}
.btn-sm{padding:6px 12px;font-size:12px;width:auto;margin-bottom:0}
.staff-entry{display:flex;align-items:center;gap:10px;padding:11px 0;border-bottom:0.5px solid var(--border)}
.staff-entry:last-child{border-bottom:none}
.staff-num{font-size:15px;font-weight:500;min-width:52px;color:var(--sB)}
.staff-info{flex:1;min-width:0}
.staff-name{font-size:13px;font-weight:500;color:var(--text)}
.staff-meta{font-size:11px;color:var(--text3);margin-top:1px}
.staff-btns{display:flex;gap:5px;flex-shrink:0}
.stat-row{display:flex;justify-content:space-between;align-items:center;padding:9px 0;border-bottom:0.5px solid var(--border)}
.stat-row:last-child{border-bottom:none}
.stat-label{font-size:13px;color:var(--text2)}
.stat-val{font-size:14px;font-weight:500;color:var(--text)}
.empty{font-size:13px;color:var(--text3);font-style:italic}
.toast{position:fixed;bottom:24px;left:50%;transform:translateX(-50%) translateY(80px);background:var(--text);color:var(--bg);padding:10px 22px;border-radius:99px;font-size:13px;font-weight:500;transition:transform .25s;z-index:999;white-space:nowrap;pointer-events:none}
.toast.show{transform:translateX(-50%) translateY(0)}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
</style>
</head>
<body>
<div class="app">
  <div class="topbar">
    <div class="live-dot"></div>
    <div class="topbar-title">🔮 塔羅牌引導</div>
    <div class="topbar-sub">叫號管理</div>
  </div>

  <!-- 目前叫號 -->
  <div class="card">
    <div style="padding:16px 0 12px">
      <div class="big-num" id="cur-num">—</div>
      <div class="big-sub" id="cur-label">等待開始</div>
    </div>
    <button class="btn btn-primary" onclick="callNext()">叫下一號 →</button>
    <button class="btn" onclick="repeatCall()">重複叫號</button>
  </div>

  <!-- 統計 -->
  <div class="card">
    <div class="card-title">今日狀況</div>
    <div class="stat-row"><span class="stat-label">等候人數</span><span class="stat-val" id="waiting">0</span></div>
    <div class="stat-row"><span class="stat-label">今日已服務</span><span class="stat-val" id="served">0</span></div>
    <div class="stat-row" style="border:none"><span class="stat-label">預估等待</span><span class="stat-val" id="est">—</span></div>
  </div>

  <!-- 候位名單 -->
  <div class="card">
    <div class="card-title" style="margin-bottom:10px">候位名單</div>
    <div id="queue-list"><span class="empty">目前無人候位</span></div>
  </div>

  <!-- 重置 -->
  <div style="padding:14px 14px 0">
    <button class="btn" style="color:var(--red);border-color:var(--red-b);font-size:13px" onclick="resetSvc()">重置今日號碼</button>
  </div>
</div>
<div class="toast" id="toast"></div>

<script>
const BACKEND_URL = 'https://mercury-gcac.onrender.com';
let cfg = { services: { B: { name: '塔羅牌占卜', prefix: 'T', minutes: 20 } } };
let state = { B: { current: 0, lastIssued: 0, queue: [], servedToday: 0 } };

function fmt(n) { return cfg.services.B.prefix + String(n).padStart(3,'0'); }
function showToast(msg) {
  const el = document.getElementById('toast');
  el.textContent = msg; el.classList.add('show');
  setTimeout(() => el.classList.remove('show'), 2400);
}

async function syncFromServer() {
  try {
    const res = await fetch(BACKEND_URL + '/api/state');
    const data = await res.json();
    if (data.state) state = data.state;
    if (data.cfg) cfg = data.cfg;
    render();
  } catch(e) {}
}

async function sendLineNotify(userId, name, message) {
  if (!userId || userId === '—') return;
  try {
    await fetch(BACKEND_URL + '/api/line-notify', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ userId, name, message })
    });
  } catch(e) {}
}

async function callNext() {
  if (state.B.queue.length === 0) { showToast('目前無人候位'); return; }
  try {
    const res = await fetch(BACKEND_URL + '/api/call-next', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ svc: 'B' })
    });
    const data = await res.json();
    if (!data.success) { showToast(data.error || '叫號失敗'); return; }
    const entry = data.called;
    sendLineNotify(entry.userId, entry.name,
      \`🔮 塔羅牌占卜｜📢 \${entry.name} 您好！現在叫到 \${fmt(entry.num)} 號，請至塔羅牌區入座，謝謝！\`);
    await syncFromServer();
    if (state.B.queue.length > 0) {
      const next = state.B.queue[0];
      sendLineNotify(next.userId, next.name,
        \`🔮 塔羅牌占卜｜⏰ \${next.name} 您好！您是下一位（\${fmt(next.num)} 號），請提前回到現場準備。\`);
    }
    showToast('已叫號：' + fmt(entry.num));
  } catch(e) { showToast('網路錯誤'); }
}

function repeatCall() {
  if (!state.B.current) { showToast('尚未開始叫號'); return; }
  showToast('已重複叫號 ' + fmt(state.B.current));
}

async function notifyPerson(num) {
  const entry = state.B.queue.find(q => q.num === num);
  if (!entry) return;
  const pos = state.B.queue.indexOf(entry);
  const est = Math.max(0, Math.ceil((pos + 1) / 2) - 1) * cfg.services.B.minutes || cfg.services.B.minutes;
  sendLineNotify(entry.userId, entry.name,
    \`🔮 塔羅牌占卜｜⏰ \${entry.name} 您好！您的 \${fmt(num)} 號預計約 \${est} 分鐘後叫號，請提前回到現場準備。\`);
  showToast('已傳送提醒給 ' + entry.name);
}

async function cancelPerson(num) {
  const entry = state.B.queue.find(q => q.num === num);
  if (!entry || !confirm(\`確定取消 \${fmt(num)} 號（\${entry.name}）？\`)) return;
  await fetch(BACKEND_URL + '/api/cancel', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ svc: 'B', num })
  });
  await syncFromServer();
  showToast('已取消候位');
}

async function resetSvc() {
  if (!confirm('確定重置今日塔羅牌所有號碼？')) return;
  await fetch(BACKEND_URL + '/api/reset', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ svc: 'B' })
  });
  await syncFromServer();
  showToast('已重置');
}

function render() {
  const q = state.B.queue;
  const cur = state.B.current;
  const mins = cfg.services.B.minutes;
  document.getElementById('cur-num').textContent = cur > 0 ? fmt(cur) : '—';
  document.getElementById('cur-label').textContent = cur > 0 ? \`請 \${fmt(cur)} 號入座\` : '等待開始';
  document.getElementById('waiting').textContent = q.length;
  document.getElementById('served').textContent = state.B.servedToday;
  const estB = q.length > 0 ? Math.max(0, Math.ceil(q.length / 2) - 1) * mins : 0;
  document.getElementById('est').textContent = q.length > 0 ? (estB > 0 ? '約 ' + estB + ' 分鐘' : '即將輪到') : '—';

  const list = document.getElementById('queue-list');
  if (q.length === 0) { list.innerHTML = '<span class="empty">目前無人候位</span>'; return; }
  list.innerHTML = q.map((entry, i) => {
    const est = Math.max(0, Math.ceil((i + 1) / 2) - 1) * mins || mins;
    return \`<div class="staff-entry">
      <div class="staff-num">\${fmt(entry.num)}</div>
      <div class="staff-info">
        <div class="staff-name">\${entry.name}</div>
        <div class="staff-meta">\${i === 0 ? '下一位' : '約 ' + est + ' 分鐘'}</div>
      </div>
      <div class="staff-btns">
        <button class="btn btn-sm" style="color:var(--amber);border-color:var(--amber-b);background:var(--amber-bg)"
          onclick="notifyPerson(\${entry.num})">提醒</button>
        <button class="btn btn-sm" style="color:var(--red);border-color:var(--red-b)"
          onclick="cancelPerson(\${entry.num})">取消</button>
      </div>
    </div>\`;
  }).join('');
}

syncFromServer();
setInterval(syncFromServer, 4000);
</script>
</body>
</html>
`); });

app.get('/', (req, res) => res.send('排隊系統後端運作中'));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
