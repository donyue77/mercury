const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { Pool } = require('pg');

const app = express();
app.use(cors());
app.use(express.json());

const LINE_TOKEN = process.env.LINE_TOKEN;

// ── 資料庫連線 ────────────────────────────────────
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

// ── 叫號鎖（防止兩個包廂同時叫到同一號）────────────
const callLocks = { B: false };
async function withLock(svc, fn) {
  if (callLocks[svc]) {
    return null; // 鎖定中，拒絕此次請求
  }
  callLocks[svc] = true;
  try {
    return await fn();
  } finally {
    setTimeout(() => { callLocks[svc] = false; }, 1000); // 1 秒後解鎖
  }
}

// ── 初始化資料庫 ──────────────────────────────────
async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS queue_state (
      key TEXT PRIMARY KEY,
      value JSONB NOT NULL,
      updated_at TIMESTAMP DEFAULT NOW()
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS phone_binding (
      phone TEXT PRIMARY KEY,
      user_id TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);
  // 初始化或遷移狀態
  const existing = await pool.query("SELECT key, value FROM queue_state WHERE key = 'main'");
  if (existing.rows.length === 0) {
    const defaultState = {
      state: {
        A: { current: 0, lastIssued: 0, queue: [], history: [], servedToday: 0, lastCalledEntry: null, inProgress: [], lastNotifiedNum: 0 },
        B: { current: 0, lastIssued: 0, queue: [], history: [], servedToday: 0, lastCalledEntry: null,
             cabins: { sun: { current: 0, lastEntry: null, servedToday: 0 }, moon: { current: 0, lastEntry: null, servedToday: 0 } } }
      },
      cfg: {
        systemName: '排隊系統',
        services: {
          A: { name: '心願瓶DIY', prefix: 'W', minutes: 15, concurrent: 6 },
          B: { name: '塔羅牌占卜', prefix: 'T', minutes: 15, concurrent: 2 }
        },
        tarotNotifyMins: 10
      }
    };
    await pool.query("INSERT INTO queue_state (key, value) VALUES ('main', $1)", [defaultState]);
  } else {
    // 遷移：確保 cabins 欄位存在
    const data = existing.rows[0].value;
    let updated = false;
    if (!data.state.A.inProgress) data.state.A.inProgress = [];
  if (data.cfg.tarotNotifyMins === undefined) data.cfg.tarotNotifyMins = 10;
  if (data.state.A.lastNotifiedNum === undefined) data.state.A.lastNotifiedNum = 0;
  if (!data.state.B.cabins) {
      data.state.B.cabins = { sun: { current: 0, lastEntry: null }, moon: { current: 0, lastEntry: null } };
      updated = true;
    }
    // 確保心願瓶有 inProgress 欄位
    if (!data.state.A.inProgress) {
      data.state.A.inProgress = [];
      updated = true;
    }
    if (!data.state.B.cabins.sun) {
      data.state.B.cabins.sun = { current: 0, lastEntry: null, servedToday: 0 };
      updated = true;
    } else if (data.state.B.cabins.sun.servedToday === undefined) {
      data.state.B.cabins.sun.servedToday = 0;
      updated = true;
    }
    if (!data.state.B.cabins.moon) {
      data.state.B.cabins.moon = { current: 0, lastEntry: null, servedToday: 0 };
      updated = true;
    } else if (data.state.B.cabins.moon.servedToday === undefined) {
      data.state.B.cabins.moon.servedToday = 0;
      updated = true;
    }
    if (updated) {
      await pool.query("UPDATE queue_state SET value = $1 WHERE key = 'main'", [data]);
      console.log('DB migrated: cabins field added');
    }
  }
  console.log('DB initialized');
}

// ── 讀寫資料庫 ────────────────────────────────────
async function getState() {
  const res = await pool.query("SELECT value FROM queue_state WHERE key = 'main'");
  const data = res.rows[0].value;
  // 確保 cabins 欄位永遠存在
  if (!data.state.A.inProgress) data.state.A.inProgress = [];
  if (data.cfg.tarotNotifyMins === undefined) data.cfg.tarotNotifyMins = 10;
  if (data.state.A.lastNotifiedNum === undefined) data.state.A.lastNotifiedNum = 0;
  if (!data.state.B.cabins) {
    data.state.B.cabins = { sun: { current: 0, lastEntry: null, servedToday: 0 }, moon: { current: 0, lastEntry: null, servedToday: 0 } };
  }
  if (!data.state.B.cabins.sun) data.state.B.cabins.sun = { current: 0, lastEntry: null, servedToday: 0 };
  if (!data.state.B.cabins.moon) data.state.B.cabins.moon = { current: 0, lastEntry: null, servedToday: 0 };
  return data;
}
async function saveState(data) {
  await pool.query(
    "UPDATE queue_state SET value = $1, updated_at = NOW() WHERE key = 'main'",
    [data]
  );
}
async function getPhoneUserId(phone) {
  const res = await pool.query('SELECT user_id FROM phone_binding WHERE phone = $1', [phone]);
  return res.rows[0]?.user_id || null;
}
async function savePhoneBinding(phone, userId) {
  await pool.query(
    'INSERT INTO phone_binding (phone, user_id) VALUES ($1, $2) ON CONFLICT (phone) DO UPDATE SET user_id = $2',
    [phone, userId]
  );
}

// ── API ──────────────────────────────────────────
app.get('/api/state', async (req, res) => {
  try {
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
    res.setHeader('Pragma', 'no-cache');
    const data = await getState();
    res.json(data);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/state', async (req, res) => {
  try {
    const data = await getState();
    if (req.body.state) data.state = req.body.state;
    if (req.body.cfg) data.cfg = req.body.cfg;
    await saveState(data);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/issue', async (req, res) => {
  try {
    const { svc, name, userId, phone, partySize } = req.body;
    if (!svc || !name) return res.status(400).json({ error: '缺少參數' });
    const data = await getState();
    data.state[svc].lastIssued++;
    const num = data.state[svc].lastIssued;
    const size = Math.min(Math.max(parseInt(partySize) || 1, 1), 6);
    const prefix = data.cfg.services[svc].prefix;
    const numStr = prefix + String(num).padStart(3, '0');
    const svcName = data.cfg.services[svc].name;
    const svcIcon = svc === 'B' ? '🔮' : '🫙';
    data.state[svc].queue.push({ num, name, userId: userId || '—', phone: phone || null, partySize: size });
    await saveState(data);
    // 後端直接發送取號確認通知
    const targetId = (userId && userId !== '—') ? userId : null;
    let targetPhone = phone || null;
    if (!targetId && !targetPhone) {
      // 無法通知，但仍成功取號
    } else {
      const message = svc === 'B'
        ? `${svcIcon} ${svcName}｜${name} 您好！您已成功取得 ${numStr} 號，輪到您前會再通知您，感謝耐心等候 🙏`
        : `${svcIcon} ${svcName}｜✅ ${name} 您好！已成功登記候位，您的號碼是 ${numStr}（${size} 人），輪到您時我們會再通知您，感謝耐心等候 🙏`;
      try {
        if (targetId) {
          await axios.post('https://api.line.me/v2/bot/message/push', {
            to: targetId,
            messages: [{ type: 'text', text: message }]
          }, { headers: { Authorization: `Bearer ${LINE_TOKEN}` } });
        } else if (targetPhone) {
          const uid = await getPhoneUserId(targetPhone);
          if (uid) {
            await axios.post('https://api.line.me/v2/bot/message/push', {
              to: uid,
              messages: [{ type: 'text', text: message }]
            }, { headers: { Authorization: `Bearer ${LINE_TOKEN}` } });
          }
        }
      } catch(notifyErr) {
        console.error('取號通知發送失敗:', notifyErr.message);
      }
    }
    res.json({ success: true, num });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/call-next', async (req, res) => {
  try {
    const { svc, cabin } = req.body;
    // 塔羅牌使用鎖機制，防止兩個包廂同時叫號
    if (svc === 'B') {
      const result = await withLock(svc, async () => {
        const data = await getState();
        const q = data.state[svc].queue;
        if (q.length === 0) return { error: '無人候位' };
        const entry = q.shift();
        data.state[svc].current = entry.num;
        data.state[svc].lastCalledEntry = { ...entry, cabin: cabin || null };
        data.state[svc].servedToday++;
        // Store per-cabin tracking for tarot
        if (svc === 'B' && cabin && data.state[svc].cabins) {
          const prevServed = data.state[svc].cabins[cabin]?.servedToday || 0;
          data.state[svc].cabins[cabin] = {
            current: entry.num,
            lastEntry: { ...entry, cabin },
            servedToday: prevServed + 1
          };
        }
        data.state[svc].history.unshift(entry.num);
        if (data.state[svc].history.length > 10) data.state[svc].history.pop();
        await saveState(data);
        return { success: true, called: { ...entry, cabin: cabin || null } };
      });
      if (result === null) {
        return res.status(429).json({ error: '系統繁忙，請稍後再試（另一個包廂正在叫號）' });
      }
      if (result.error) return res.status(400).json({ error: result.error });
      return res.json(result);
    }
    // 心願瓶不需要鎖
    const data = await getState();
    const q = data.state[svc].queue;
    if (q.length === 0) return res.status(400).json({ error: '無人候位' });

    const entry = q.shift();
    data.state[svc].current = entry.num;
    data.state[svc].lastCalledEntry = { ...entry, cabin: cabin || null };
    data.state[svc].servedToday++;
    data.state[svc].history.unshift(entry.num);
    if (data.state[svc].history.length > 10) data.state[svc].history.pop();
    await saveState(data);
    res.json({ success: true, called: { ...entry, cabin: cabin || null } });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/cancel', async (req, res) => {
  try {
    const { svc, num } = req.body;
    const data = await getState();
    data.state[svc].queue = data.state[svc].queue.filter(q => q.num !== num);
    await saveState(data);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// 已確認領瓶 → 移入製作中，從候位名單移除
app.post('/api/confirm-pickup', async (req, res) => {
  try {
    const { num } = req.body;
    const data = await getState();
    // 從 lastCalledEntry 取得資料
    const entry = data.state.A.lastCalledEntry;
    if (!entry || entry.num !== num) return res.status(404).json({ error: '找不到此號碼' });
    // 加入製作中（避免重複）
    if (!data.state.A.inProgress.find(e => e.num === num)) {
      data.state.A.inProgress.push({ ...entry, startTime: Date.now() });
    }
    // 從候位名單移除（以防還在裡面）
    data.state.A.queue = data.state.A.queue.filter(e => e.num !== num);
    // 清空目前服務號
    data.state.A.current = 0;
    await saveState(data);
    res.json({ success: true, entry });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// 製作完成 → 移除製作中，回傳下一位候位資訊
app.post('/api/complete-making', async (req, res) => {
  try {
    const { num } = req.body;
    const data = await getState();
    const entry = data.state.A.inProgress.find(e => e.num === num);
    if (!entry) return res.status(404).json({ error: '找不到此號碼' });
    // 計算完成前的剩餘容量
    const capBefore = data.state.A.inProgress.reduce((s, e) => s + (e.partySize || 1), 0);
    const slotsBefore = 6 - capBefore;
    // 移除該組
    data.state.A.inProgress = data.state.A.inProgress.filter(e => e.num !== num);
    // 計算完成後的剩餘容量
    const capAfter = data.state.A.inProgress.reduce((s, e) => s + (e.partySize || 1), 0);
    const slotsAfter = 6 - capAfter;
    const nextInQueue = data.state.A.queue.length > 0 ? data.state.A.queue[0] : null;
    await saveState(data);
    res.json({ success: true, nextInQueue, availableSlots: slotsAfter });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// 重置通知記錄（每次新叫號時呼叫）
app.post('/api/reset-notify', async (req, res) => {
  try {
    const data = await getState();
    data.state.A.lastNotifiedNum = 0;
    await saveState(data);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/noshow', async (req, res) => {
  try {
    const { svc, num, requeue } = req.body;
    const data = await getState();
    const q = data.state[svc].queue;
    let entry = q.find(e => e.num === num);
    data.state[svc].queue = q.filter(e => e.num !== num);
    if (!entry) entry = data.state[svc].lastCalledEntry || null;
    if (requeue && entry) {
      // 扣除已服務數量
      if (data.state[svc].servedToday > 0) {
        data.state[svc].servedToday--;
      }
      // 塔羅牌：扣除包廂計數 + 清空目前服務號
      if (svc === 'B' && data.state[svc].cabins) {
        const cabinId = entry.cabin;
        if (cabinId && data.state[svc].cabins[cabinId]) {
          if (data.state[svc].cabins[cabinId].servedToday > 0) {
            data.state[svc].cabins[cabinId].servedToday--;
          }
          data.state[svc].cabins[cabinId].current = 0;
        }
      }
      // 重排
      // 心願瓶：若在製作中則移除
      if (svc === 'A' && data.state[svc].inProgress) {
        data.state[svc].inProgress = data.state[svc].inProgress.filter(e => e.num !== num);
      }
      if (svc === 'A') {
        const newQ = [...data.state[svc].queue];
        newQ.splice(1, 0, entry);
        data.state[svc].queue = newQ;
      } else {
        data.state[svc].queue.push(entry);
      }
    }
    await saveState(data);
    res.json({ success: true, entry: entry || null });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/reset', async (req, res) => {
  try {
    const { svc } = req.body;
    const data = await getState();
    const empty = { current: 0, lastIssued: 0, queue: [], history: [], servedToday: 0, lastCalledEntry: null };
    if (svc) {
      if (svc === 'A') {
        data.state[svc] = { ...empty, inProgress: [], lastNotifiedNum: 0 };
      } else {
        data.state[svc] = { ...empty,
          cabins: { sun: { current: 0, lastEntry: null, servedToday: 0 }, moon: { current: 0, lastEntry: null, servedToday: 0 } }
        };
      }
    } else {
      data.state = { A: { ...empty }, B: { ...empty } };
    }
    await saveState(data);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── LINE Webhook ──────────────────────────────────
app.post('/webhook', async (req, res) => {
  res.sendStatus(200);
  const events = req.body.events || [];
  for (const event of events) {
    if (event.type === 'message' && event.message.type === 'text') {
      const userId = event.source.userId;
      const text = event.message.text.trim();

      // 手機號碼綁定
      if (/^09\d{8}$/.test(text)) {
        await savePhoneBinding(text, userId);
        await axios.post('https://api.line.me/v2/bot/message/reply', {
          replyToken: event.replyToken,
          messages: [{ type: 'text', text: `🫙 心願瓶DIY｜✅ 手機號碼 ${text} 綁定成功！結帳後工作人員會幫您登記候位，輪到您時我們會主動通知您 🙏` }]
        }, { headers: { Authorization: `Bearer ${LINE_TOKEN}` } }).catch(()=>{});
        continue;
      }

      // 查詢塔羅牌
      const isTarotQuery = ['查詢塔羅目前叫號','塔羅目前叫號','查詢塔羅','塔羅叫號','🔮查詢'].includes(text);
      if (isTarotQuery) {
        const data = await getState();
        const q = data.state.B;
        const cfg = data.cfg.services.B;
        const cabins = q.cabins || {};
        const sunCur = cabins.sun?.current > 0 ? cfg.prefix + String(cabins.sun.current).padStart(3,'0') : '尚未開始';
        const moonCur = cabins.moon?.current > 0 ? cfg.prefix + String(cabins.moon.current).padStart(3,'0') : '尚未開始';
        const waiting = q.queue.length;
        const estMins = waiting > 0 ? Math.max(0, Math.ceil(waiting / 2) - 1) * cfg.minutes : 0;
        const estText = waiting === 0 ? '目前無人候位' : estMins > 0 ? `預估等待約 ${estMins} 分鐘` : '即將輪到下一位';
        const replyMsg = `🔮 塔羅牌占卜｜目前叫號查詢\n\n☀️ 太陽包廂：${sunCur}\n🌙 月亮包廂：${moonCur}\n\n等候人數：${waiting} 人\n${estText}\n\n輪到您時我們會主動通知您 🙏`;
        await axios.post('https://api.line.me/v2/bot/message/reply', {
          replyToken: event.replyToken,
          messages: [{ type: 'text', text: replyMsg }]
        }, { headers: { Authorization: `Bearer ${LINE_TOKEN}` } }).catch(()=>{});
        continue;
      }

      // 查詢心願瓶
      const isWishQuery = ['查詢心願瓶目前叫號','心願瓶目前叫號','查詢心願瓶','心願瓶叫號','🫙查詢'].includes(text);
      if (isWishQuery) {
        const data = await getState();
        const q = data.state.A;
        const cfg = data.cfg.services.A;
        const cur = q.current > 0 ? cfg.prefix + String(q.current).padStart(3,'0') : '尚未開始';
        const waiting = q.queue.length;
        const totalCap = q.queue.reduce((sum, e) => sum + (e.partySize || 1), 0);
        const estMins = waiting > 0 ? Math.max(0, Math.ceil(totalCap / 5) - 1) * cfg.minutes : 0;
        const estText = waiting === 0 ? '目前無人候位' : estMins > 0 ? `預估等待約 ${estMins} 分鐘` : '即將輪到下一組';
        await axios.post('https://api.line.me/v2/bot/message/reply', {
          replyToken: event.replyToken,
          messages: [{ type: 'text', text: `🫙 心願瓶DIY｜目前叫號查詢\n\n現在服務號：${cur}\n等候組數：${waiting} 組\n${estText}\n\n輪到您時我們會主動通知您 🙏` }]
        }, { headers: { Authorization: `Bearer ${LINE_TOKEN}` } }).catch(()=>{});
        continue;
      }
    }
  }
});

app.post('/api/register', async (req, res) => {
  const { userId, name } = req.body;
  if (!userId) return res.status(400).json({ error: '缺少 userId' });
  res.json({ success: true });
});

app.post('/api/line-notify', async (req, res) => {
  try {
    const { userId, phone, name, message } = req.body;
    if (!message) return res.status(400).json({ error: '缺少 message' });
    let targetId = (userId && userId !== '—') ? userId : null;
    if (!targetId && phone) targetId = await getPhoneUserId(phone);
    if (!targetId) return res.status(404).json({ error: '找不到對應的 LINE 帳號' });
    await axios.post('https://api.line.me/v2/bot/message/push', {
      to: targetId,
      messages: [{ type: 'text', text: message }]
    }, { headers: { Authorization: `Bearer ${LINE_TOKEN}` } });
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── 頁面路由 ──────────────────────────────────────


































































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
.loading-overlay{position:fixed;inset:0;background:var(--bg);z-index:999;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:16px}
.loading-spinner{width:40px;height:40px;border:3px solid var(--border);border-top-color:var(--sB);border-radius:50%;animation:spin 1s linear infinite}
.loading-title{font-size:16px;font-weight:600;color:var(--text)}
.loading-sub{font-size:13px;color:var(--text3);text-align:center;line-height:1.6;max-width:240px}
.loading-dot{display:inline-block;animation:pulse 1.5s infinite}
@keyframes spin{to{transform:rotate(360deg)}}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
</style>
</head>
<body>
<div class="loading-overlay" id="loading-overlay">
  <div class="loading-spinner"></div>
  <div class="loading-title">系統啟動中</div>
  <div class="loading-sub">伺服器喚醒中，請稍候片刻<br/>通常需要 30～50 秒</div>
</div>
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
        <!-- 塔羅牌才顯示取消按鈕 -->
        <button class="btn btn-danger" id="cancel-btn" onclick="leaveQueue()" style="display:none">取消候位</button>
        <!-- 心願瓶提示 -->
        <div id="wishbottle-notice" style="display:none;font-size:13px;color:var(--text3);text-align:center;padding:8px 0">如需取消候位，請至服務台洽詢工作人員</div>
      </div>

      <!-- 另一個服務的等候狀況 -->
      <div id="other-svc-status" style="display:none">
        <div style="font-size:11px;font-weight:500;color:var(--text3);letter-spacing:.05em;text-transform:uppercase;margin:4px 0 8px">其他服務狀況</div>
        <div class="card" style="padding:14px 16px">
          <div style="display:flex;align-items:center;gap:10px;margin-bottom:10px">
            <span id="other-svc-icon" style="font-size:20px"></span>
            <span id="other-svc-name" style="font-size:14px;font-weight:500;color:var(--text)"></span>
          </div>
          <div class="stat-row">
            <span class="stat-label">目前服務號</span>
            <span class="stat-val" id="other-svc-cur">—</span>
          </div>
          <div class="stat-row">
            <span class="stat-label">等候人數</span>
            <span class="stat-val" id="other-svc-waiting">0</span>
          </div>
          <div class="stat-row" style="border:none">
            <span class="stat-label">預估等待</span>
            <span class="stat-val" id="other-svc-est">—</span>
          </div>
        </div>
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
    <!-- 塔羅牌兩包廂狀態 -->
    <div id="tarot-cabins" style="display:none;margin-bottom:12px">
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">
        <div class="card" style="padding:14px;text-align:center">
          <div style="font-size:16px;margin-bottom:4px">☀️</div>
          <div style="font-size:11px;color:var(--text3);margin-bottom:4px">太陽包廂</div>
          <div style="font-size:24px;font-weight:600;color:var(--sB)" id="sun-cur">—</div>
        </div>
        <div class="card" style="padding:14px;text-align:center">
          <div style="font-size:16px;margin-bottom:4px">🌙</div>
          <div style="font-size:11px;color:var(--text3);margin-bottom:4px">月亮包廂</div>
          <div style="font-size:24px;font-weight:600;color:var(--sB)" id="moon-cur">—</div>
        </div>
      </div>
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

let serverReady = false;
let loadingTimer = null;

async function syncFromServer() {
  try {
    const res = await fetch(BACKEND_URL + '/api/state', { cache: 'no-store' });
    const data = await res.json();
    if (data.state) state = data.state;
    if (data.cfg) cfg = data.cfg;
    if (!serverReady) {
      serverReady = true;
      if (loadingTimer) clearTimeout(loadingTimer);
      const overlay = document.getElementById('loading-overlay');
      if (overlay) overlay.style.display = 'none';
    }
    render();
  } catch(e) {
    // Server not ready yet, keep showing loading
  }
}

// Show loading overlay if server doesn't respond in 2 seconds
loadingTimer = setTimeout(() => {
  const loadingOverlay = document.getElementById('loading-overlay');
  if (loadingOverlay && !serverReady) overlay.style.display = 'flex';
}, 2000);

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
  if (!myTicket) return;
  if (myTicket.svc === 'A') { showToast('心願瓶取消候位請至服務台洽詢'); return; }
  const svcName = cfg.services[myTicket.svc].name;
  const cancelNumStr = myTicket.svc === 'B'
    ? cfg.services.B.prefix + String(myTicket.num).padStart(3,'0')
    : cfg.services.A.prefix + String(myTicket.num).padStart(3,'0');
  if (!confirm(\`確定要取消 \${svcName} \${cancelNumStr} 號的候位嗎？\n取消後無法恢復。\`)) return;
  try {
    await fetch(BACKEND_URL + '/api/cancel', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ svc: myTicket.svc, num: myTicket.num })
    });
    const cancelTicketNumStr = fmt(myTicket.svc, myTicket.num);
    const cancelSvcName = cfg.services[myTicket.svc].name;
    const cancelUserId = myTicket.userId;
    const cancelName = myTicket.name;
    myTicket = null;
    localStorage.removeItem('qs_ticket');
    // 傳送 LINE 取消確認
    if (cancelUserId && cancelUserId !== '—') {
      await fetch(BACKEND_URL + '/api/line-notify', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          userId: cancelUserId,
          message: \`✅ \${cancelName} 您好！您的\${cancelSvcName} \${cancelTicketNumStr} 號候位已成功取消。\n\n如有需要歡迎重新取號，感謝您！\`
        })
      }).catch(()=>{});
    }
    await syncFromServer();
    showToast('已取消候位');
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
    // 只有塔羅牌才顯示取消按鈕
    document.getElementById('cancel-btn').style.display = myTicket.svc === 'B' ? 'flex' : 'none';
    document.getElementById('wishbottle-notice').style.display = myTicket.svc === 'A' ? 'block' : 'none';
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
      const cabin = state[svc].lastCalledEntry?.cabin;
      const cabinText = cabin === 'sun' ? '☀️ 太陽包廂' : cabin === 'moon' ? '🌙 月亮包廂' : '塔羅牌區';
      wEl.className='wait-badge now';
      wEl.textContent = svc === 'B' ? \`📢 叫到您了！請前往 \${cabinText}\` : '📢 叫到您了！請前往';
    } else if (pos === 0) {
      wEl.className='wait-badge soon'; wEl.textContent='您是下一位，請準備！';
    } else if (pos > 0) {
      wEl.className='wait-badge normal';
      const conc = svc === 'A' ? 5 : 2;
      const estW = Math.max(0, Math.ceil(pos / conc) - 1) * cfg.services[svc].minutes;
      wEl.textContent = estW > 0 ? \`前方 \${pos} 人，約 \${estW} 分鐘\` : \`前方 \${pos} 人，即將輪到\`;
    } else {
      wEl.className='wait-badge normal'; wEl.textContent='號碼已完成服務或已取消';
    }
    // 顯示另一個服務的狀況
    const otherSvc = myTicket.svc === 'B' ? 'A' : 'B';
    const otherIcon = otherSvc === 'B' ? '🔮' : '🫙';
    const otherState = state[otherSvc];
    const otherCfg = cfg.services[otherSvc];
    const otherCur = otherState.current > 0 ? otherCfg.prefix + String(otherState.current).padStart(3,'0') : '—';
    const otherQ = otherState.queue.length;
    const otherConc = otherSvc === 'A' ? 5 : 2;
    const otherTotalCap = otherSvc === 'A'
      ? otherState.queue.reduce((sum, e) => sum + (e.partySize || 1), 0)
      : otherQ;
    const otherEst = otherQ > 0 ? Math.max(0, Math.ceil(otherTotalCap / otherConc) - 1) * otherCfg.minutes : 0;
    document.getElementById('other-svc-status').style.display = 'block';
    document.getElementById('other-svc-icon').textContent = otherIcon;
    document.getElementById('other-svc-name').textContent = otherCfg.name;
    document.getElementById('other-svc-cur').textContent = otherCur;
    document.getElementById('other-svc-waiting').textContent = otherQ + ' 人';
    document.getElementById('other-svc-est').textContent = otherQ > 0 ? (otherEst > 0 ? '約 ' + otherEst + ' 分鐘' : '即將輪到') : '無需等候';
  } else {
    document.getElementById('my-ticket-view').style.display = 'none';
    document.getElementById('take-form').style.display = 'block';
    document.getElementById('other-svc-status').style.display = 'none';
  }

  renderStatus();
}

function renderStatus() {
  const svc = currentStatusSvc;
  const q = state[svc].queue;
  const cur = state[svc].current;
  const mins = cfg.services[svc].minutes;
  const statusNumStr = cur > 0 ? fmt(svc, cur) : '—';
  const el = document.getElementById('status-cur');
  el.textContent = statusNumStr; el.className = 'big-num color-' + svc;
  document.getElementById('status-label').textContent = svc === 'B'
    ? (cur > 0 ? '最新叫號' : '等待服務')
    : (cur > 0 ? \`請 \${statusNumStr} 號前往\` : '等待服務');
  document.getElementById('status-waiting').textContent = q.length;
  // 等待時間計算
  let estMins = 0;
  if (svc === 'A') {
    const totalCapA = q.reduce((s, e) => s + (e.partySize || 1), 0);
    const inProgCapA = (state.A.inProgress || []).reduce((s, e) => s + (e.partySize || 1), 0);
    const overCapA = Math.max(0, inProgCapA + totalCapA - 6);
    estMins = q.length > 0 ? Math.ceil(overCapA / 6) * mins : 0;
  } else {
    estMins = q.length > 0 ? Math.max(0, Math.ceil(q.length / 2) - 1) * mins : 0;
  }
  document.getElementById('status-est').textContent = q.length > 0 ? (estMins > 0 ? estMins : '即將輪到') : '—';
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
setInterval(syncFromServer, 2000);
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
      <button class="tab active" onclick="goTab('queue')" id="tab-queue">總覽</button>
      <button class="tab" onclick="goTab('log')" id="tab-log">記錄</button>
      <button class="tab" onclick="goTab('settings')" id="tab-settings">設定</button>
    </div>

    <!-- 叫號 -->
    <div class="panel active" id="panel-queue">

      <!-- 今日服務統計 -->
      <div class="card" style="margin-top:14px">
        <div class="card-title">今日服務統計</div>
        <div class="stat-row">
          <span class="stat-label">🫙 心願瓶DIY</span>
          <span class="stat-val" id="st-served-a">0 人</span>
        </div>
        <div class="stat-row">
          <span class="stat-label">☀️ 太陽包廂</span>
          <span class="stat-val" id="st-served-sun">0 人</span>
        </div>
        <div class="stat-row">
          <span class="stat-label">🌙 月亮包廂</span>
          <span class="stat-val" id="st-served-moon">0 人</span>
        </div>
        <div class="stat-row" style="border-top:1.5px solid var(--border2);margin-top:4px;padding-top:12px;border-bottom:none">
          <span class="stat-label" style="font-weight:600">今日總計</span>
          <span class="stat-val" style="font-size:18px;font-weight:700" id="st-served-total">0 人</span>
        </div>
      </div>

      <!-- 心願瓶狀況 -->
      <div class="card">
        <div class="card-title">🫙 心願瓶DIY 狀況</div>
        <div class="stat-row">
          <span class="stat-label">目前服務號</span>
          <span class="stat-val" id="wb-cur">—</span>
        </div>
        <div class="stat-row">
          <span class="stat-label">等候組數</span>
          <span class="stat-val" id="wb-waiting">0</span>
        </div>
        <div class="stat-row" style="border:none">
          <span class="stat-label">預估等待</span>
          <span class="stat-val" id="wb-est">—</span>
        </div>
      </div>

      <!-- 塔羅牌狀況 -->
      <div class="card">
        <div class="card-title">🔮 塔羅牌占卜 狀況</div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px">
          <div style="text-align:center;padding:10px;background:var(--bg2);border-radius:var(--r-sm)">
            <div style="font-size:11px;color:var(--text3);margin-bottom:4px">☀️ 太陽包廂</div>
            <div style="font-size:22px;font-weight:600;color:var(--text)" id="sun-cur-staff">—</div>
          </div>
          <div style="text-align:center;padding:10px;background:var(--bg2);border-radius:var(--r-sm)">
            <div style="font-size:11px;color:var(--text3);margin-bottom:4px">🌙 月亮包廂</div>
            <div style="font-size:22px;font-weight:600;color:var(--text)" id="moon-cur-staff">—</div>
          </div>
        </div>
        <div class="stat-row">
          <span class="stat-label">等候人數</span>
          <span class="stat-val" id="tarot-waiting">0</span>
        </div>
        <div class="stat-row" style="border:none">
          <span class="stat-label">預估等待</span>
          <span class="stat-val" id="tarot-est">—</span>
        </div>
      </div>

      <!-- 重置 -->
      <div class="card">
        <div class="card-title">重置號碼</div>
        <div style="font-size:12px;color:var(--text3);margin-bottom:12px">每日活動結束後或重新開始前使用</div>
        <button class="btn" style="color:var(--red);border-color:var(--red-b);margin-bottom:8px;font-size:13px" onclick="resetWishbottle()">🫙 重置心願瓶號碼</button>
        <button class="btn" style="color:var(--red);border-color:var(--red-b);margin-bottom:8px;font-size:13px" onclick="resetTarot()">🔮 重置塔羅牌號碼</button>
        <button class="btn" style="color:var(--red);border-color:var(--red-b);font-size:13px;opacity:.7" onclick="resetAll()">重置全部</button>
      </div>

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
        <div class="setting-row"><span class="setting-label">每號時間（分）</span><input class="setting-input" type="number" id="set-timeB" min="1" max="120"/></div>
        <div class="setting-row" style="border:none">
          <span class="setting-label">自動提醒時間（分）</span>
          <input class="setting-input" type="number" id="set-tarot-notify" min="1" max="60"/>
          <div style="font-size:11px;color:var(--text3);margin-top:4px;grid-column:1/-1">叫號後幾分鐘自動提醒下一位準備回場</div>
        </div>
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
    const res = await fetch(BACKEND_URL + '/api/state', { cache: 'no-store' });
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
async function resetWishbottle() {
  if (!confirm('確定重置心願瓶今日所有號碼？')) return;
  await fetch(BACKEND_URL + '/api/reset', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ svc: 'A' })
  });
  await syncFromServer();
  showToast('心願瓶號碼已重置');
}

async function resetTarot() {
  if (!confirm('確定重置塔羅牌今日所有號碼？')) return;
  await fetch(BACKEND_URL + '/api/reset', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ svc: 'B' })
  });
  await syncFromServer();
  showToast('塔羅牌號碼已重置');
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
  document.getElementById('set-tarot-notify').value = cfg.tarotNotifyMins || 10;
}
async function saveSettings() {
  cfg.systemName = document.getElementById('set-system-name').value.trim() || '排隊系統';
  cfg.services.A.name = document.getElementById('set-nameA').value.trim() || '服務A';
  cfg.services.A.prefix = document.getElementById('set-prefixA').value.trim() || 'A';
  cfg.services.A.minutes = parseInt(document.getElementById('set-timeA').value) || 15;
  cfg.services.B.name = document.getElementById('set-nameB').value.trim() || '服務B';
  cfg.services.B.prefix = document.getElementById('set-prefixB').value.trim() || 'T';
  cfg.services.B.minutes = parseInt(document.getElementById('set-timeB').value) || 20;
  cfg.tarotNotifyMins = parseInt(document.getElementById('set-tarot-notify').value) || 10;
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
  const setEl = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };

  // 今日服務統計
  const sunServed = state.B?.cabins?.sun?.servedToday || 0;
  const moonServed = state.B?.cabins?.moon?.servedToday || 0;
  const aServed = state.A?.servedToday || 0;
  const total = aServed + sunServed + moonServed;
  setEl('st-served-a', aServed + ' 人');
  setEl('st-served-sun', sunServed + ' 人');
  setEl('st-served-moon', moonServed + ' 人');
  setEl('st-served-total', total + ' 人');

  // 心願瓶狀況
  const aq = state.A?.queue || [];
  const aCur = state.A?.current || 0;
  const aMins = cfg.services?.A?.minutes || 12;
  const aTotalCap = aq.reduce((s, e) => s + (e.partySize || 1), 0);
  const aEst = aq.length > 0 ? Math.max(0, Math.ceil(aTotalCap / 5) - 1) * aMins : 0;
  setEl('wb-cur', aCur > 0 ? (cfg.services?.A?.prefix || 'A') + String(aCur).padStart(3,'0') : '—');
  setEl('wb-waiting', aq.length + ' 組' + (aTotalCap > aq.length ? '（共 ' + aTotalCap + ' 人）' : ''));
  setEl('wb-est', aq.length > 0 ? (aEst > 0 ? '約 ' + aEst + ' 分鐘' : '即將輪到') : '—');

  // 塔羅牌狀況
  const bq = state.B?.queue || [];
  const bMins = cfg.services?.B?.minutes || 15;
  const bEst = bq.length > 0 ? Math.max(0, Math.ceil(bq.length / 2) - 1) * bMins : 0;
  const sunCur = state.B?.cabins?.sun?.current || 0;
  const moonCur = state.B?.cabins?.moon?.current || 0;
  const bPrefix = cfg.services?.B?.prefix || 'T';
  setEl('sun-cur-staff', sunCur > 0 ? bPrefix + String(sunCur).padStart(3,'0') : '—');
  setEl('moon-cur-staff', moonCur > 0 ? bPrefix + String(moonCur).padStart(3,'0') : '—');
  setEl('tarot-waiting', bq.length + ' 人');
  setEl('tarot-est', bq.length > 0 ? (bEst > 0 ? '約 ' + bEst + ' 分鐘' : '即將輪到') : '—');
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
setInterval(syncFromServer, 2000);
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
.title-btn{padding:10px 16px;border:0.5px solid var(--border2);border-radius:var(--r-sm);background:transparent;font-size:15px;font-weight:500;color:var(--text2);cursor:pointer;font-family:inherit;transition:all .15s;white-space:nowrap}
.title-btn.active{background:var(--sA);color:#fff;border-color:var(--sA)}
.empty{font-size:13px;color:var(--text3);font-style:italic}
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
      <label>客人稱謂</label>
      <div style="display:flex;gap:8px">
        <input type="text" id="inp-surname" placeholder="請輸入姓氏" autocomplete="off"
          style="flex:1;padding:12px;border:0.5px solid var(--border2);border-radius:var(--r-sm);background:var(--bg);color:var(--text);font-size:16px;font-family:inherit"/>
        <div style="display:flex;gap:6px;flex-shrink:0" id="title-btns">
          <button type="button" class="title-btn active" onclick="setTitle('先生')">先生</button>
          <button type="button" class="title-btn" onclick="setTitle('小姐')">小姐</button>
        </div>
      </div>
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
      <span class="stat-label">等候組數</span>
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

  <!-- 候位名單確認 -->
  <div class="card">
    <div class="card-title" style="margin-bottom:10px">目前候位名單</div>
    <div id="checkout-queue-list"><span class="empty">目前無人候位</span></div>
  </div>

  <!-- 取消候位 -->
  <div class="card">
    <div class="card-title" style="margin-bottom:12px">取消客人候位</div>
    <div style="font-size:12px;color:var(--text3);margin-bottom:10px">客人退款後，輸入手機號碼取消候位</div>
    <input type="tel" id="cancel-inp-phone" placeholder="09xxxxxxxx"
      oninput="lookupByPhone()"
      style="width:100%;padding:12px;border:0.5px solid var(--border2);border-radius:var(--r-sm);background:var(--bg);color:var(--text);font-size:16px;font-family:inherit;margin-bottom:8px"/>
    <div id="cancel-lookup-result" style="display:none;padding:10px 12px;border-radius:var(--r-sm);margin-bottom:8px;font-size:13px"></div>
    <button class="btn" id="cancel-confirm-btn" style="color:var(--red);border-color:var(--red-b);margin-bottom:0;display:none" onclick="cancelByPhone()">確認取消候位</button>
  </div>
</div>
<div class="toast" id="toast"></div>

<script>
const BACKEND_URL = 'https://mercury-gcac.onrender.com';
let partySize = 1;
let titleStr = '先生';

function setTitle(t) {
  titleStr = t;
  document.querySelectorAll('.title-btn').forEach(btn => {
    btn.classList.toggle('active', btn.textContent === t);
  });
}

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

async function cancelEntry(num) {
  const entry = state.A.queue.find(e => e.num === num);
  if (!entry) { showToast('找不到此候位'); return; }
  if (!confirm('確定取消 ' + entry.name + '（' + fmt(entry.num) + '）的候位？')) return;
  try {
    await fetch(BACKEND_URL + '/api/cancel', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ svc: 'A', num })
    });
    await syncFromServer();
    showToast('已取消 ' + entry.name + ' 的候位');
  } catch(e) { showToast('網路錯誤，請再試一次'); }
}

function lookupByPhone() {
  const lookupRawPhone = document.getElementById('cancel-inp-phone').value.trim();
  const phone = lookupRawPhone.split('').filter(c => c >= '0' && c <= '9').join('');
  const resultEl = document.getElementById('cancel-lookup-result');
  const btnEl = document.getElementById('cancel-confirm-btn');

  if (phone.length < 10) {
    resultEl.style.display = 'none';
    btnEl.style.display = 'none';
    return;
  }
  if (phone.length === 10 && phone.startsWith('09')) {
    const entry = state.A.queue.find(e => e.phone === phone);
    if (entry) {
      resultEl.style.display = 'block';
      resultEl.style.background = 'var(--red-bg)';
      resultEl.style.border = '0.5px solid var(--red-b)';
      resultEl.style.color = 'var(--red)';
      resultEl.innerHTML = \`找到：<strong>\${entry.name}</strong>　\${fmt(entry.num)} 號　\${entry.partySize || 1} 人\`;
      btnEl.style.display = 'flex';
    } else {
      resultEl.style.display = 'block';
      resultEl.style.background = 'var(--bg3)';
      resultEl.style.border = '0.5px solid var(--border)';
      resultEl.style.color = 'var(--text3)';
      resultEl.textContent = '查無此號碼的候位記錄';
      btnEl.style.display = 'none';
    }
  }
}

async function cancelByPhone() {
  const cancelRawPhone2 = document.getElementById('cancel-inp-phone').value.trim();
  const cancelPhone = cancelRawPhone2.split('').filter(c => c >= '0' && c <= '9').join('');
  const entry = state.A.queue.find(e => e.phone === cancelPhone);
  if (!entry) { showToast('找不到此客人的候位'); return; }
  try {
    await fetch(BACKEND_URL + '/api/cancel', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ svc: 'A', num: entry.num })
    });
    document.getElementById('cancel-inp-phone').value = '';
    document.getElementById('cancel-lookup-result').style.display = 'none';
    document.getElementById('cancel-confirm-btn').style.display = 'none';
    await syncFromServer();
    showToast(\`已取消 \${entry.name} 的候位\`);
  } catch(e) { showToast('網路錯誤，請再試一次'); }
}

async function syncFromServer() {
  try {
    const res = await fetch(BACKEND_URL + '/api/state', { cache: 'no-store' });
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
  const totalCap = q.reduce((sum, e) => sum + (e.partySize || 1), 0);
  const estA = q.length > 0 ? Math.max(0, Math.ceil(totalCap / 5) - 1) * mins : 0;
  const totalCapDisp = q.reduce((sum, e) => sum + (e.partySize || 1), 0);
  document.getElementById('waiting').textContent = q.length + ' 組' + (totalCapDisp > 0 ? '（共 ' + totalCapDisp + ' 人）' : '');
  document.getElementById('est').textContent = q.length > 0 ? (estA > 0 ? '約 ' + estA + ' 分鐘' : '即將輪到') : '無需等候';
  document.getElementById('current').textContent = state.A.current > 0 ? fmt(state.A.current) : '—';

  // 候位名單（最多10組，含取消按鈕）
  const list = document.getElementById('checkout-queue-list');
  if (!list) return;
  if (q.length === 0) { list.innerHTML = '<span style="font-size:13px;color:var(--text3);font-style:italic">目前無人候位</span>'; return; }
  const displayQ = q.slice(0, 10);
  const remaining = q.length - 10;
  let listHtml = displayQ.map((entry, i) => {
    const cumCapBefore = q.slice(0, i).reduce((sum, e) => sum + (e.partySize || 1), 0);
    const batchesBefore = Math.floor(cumCapBefore / 6);
    const estEntry = batchesBefore * mins;
    const posLabel = i === 0 ? '下一組' : (estEntry === 0 ? '即將輪到' : '約 ' + estEntry + ' 分鐘');
    const sizeLabel = (entry.partySize || 1) + ' 人';
    const isLast = i === displayQ.length - 1 && remaining === 0;
    return '<div style="display:flex;align-items:center;gap:8px;padding:10px 0;' + (isLast ? '' : 'border-bottom:0.5px solid var(--border)') + '">'
      + '<div style="font-size:14px;font-weight:600;min-width:48px;color:var(--sA)">' + fmt(entry.num) + '</div>'
      + '<div style="flex:1;min-width:0">'
      + '<div style="font-size:13px;font-weight:600;color:var(--text)">' + entry.name + '</div>'
      + '<div style="font-size:11px;color:var(--text3);margin-top:2px">' + sizeLabel + '｜' + posLabel + '</div>'
      + '</div>'
      + '<button onclick="cancelEntry(' + entry.num + ')" style="flex-shrink:0;padding:5px 10px;font-size:11px;font-weight:600;color:var(--red);border:0.5px solid var(--red-b);border-radius:var(--r-sm);background:transparent;cursor:pointer;font-family:inherit">取消</button>'
      + '</div>';
  }).join('');
  if (remaining > 0) {
    listHtml += '<div style="padding:8px 0 0;border-top:0.5px solid var(--border);margin-top:4px;font-size:12px;color:var(--text3)">還有 ' + remaining + ' 組，請輸入手機號碼取消</div>';
  }
  list.innerHTML = listHtml;
}

async function register() {
  const surname = document.getElementById('inp-surname').value.trim();
  const name = surname ? surname + titleStr : '';
  const rawPhone = document.getElementById('inp-phone').value.trim();
  const cleanPhone = rawPhone.split('').filter(c => c >= '0' && c <= '9').join('');
  if (!surname) { showToast('請輸入客人姓氏'); return; }
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
    sendLineNotify(cleanPhone, name,
      \`🫙 心願瓶DIY｜✅ \${name} 您好！已成功登記候位，您的號碼是 \${numStr}（\${partySize} 人），輪到您時我們會再通知您，感謝耐心等候 🙏\`);
    document.getElementById('success-num').textContent = numStr;
    document.getElementById('success-sub').textContent = \`已傳送 LINE 通知給 \${name}\`;
    document.getElementById('success-banner').classList.add('show');
    document.getElementById('inp-surname').value = '';
    document.getElementById('inp-phone').value = '';
    document.getElementById('inp-surname').focus();
    setParty(1);
    setTitle('先生');
    setTimeout(() => document.getElementById('success-banner').classList.remove('show'), 5000);
  } catch(e) { showToast('網路錯誤，請再試一次'); }
}


syncFromServer();
setInterval(syncFromServer, 2000);
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
      <div id="cur-name" style="display:none;font-size:14px;font-weight:600;color:var(--text);margin-top:6px"></div>
    </div>
    <button class="btn btn-primary" onclick="callNext()">叫下一號 →</button>
    <button class="btn" onclick="repeatCall()">重複叫號</button>
    <button class="btn" id="confirm-pickup-btn" onclick="confirmPickup()" style="display:none;background:var(--green-bg);color:var(--green);border-color:var(--green-b);margin-bottom:0">已確認領瓶 ✅</button>
  </div>

  <!-- 製作中 -->
  <div class="card" id="in-progress-card" style="display:none">
    <div class="card-title" style="margin-bottom:10px">🫙 正在進行心願瓶製作中</div>
    <div id="in-progress-list"></div>
  </div>

  <!-- 統計 -->
  <!-- 製作區容量 -->
  <div class="card" id="capacity-card">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
      <span style="font-size:12px;color:var(--text3)">製作區容量</span>
      <span style="font-size:12px;font-weight:600;color:var(--text)" id="cap-text">0 / 6 人</span>
    </div>
    <div style="height:6px;background:var(--bg3);border-radius:3px;overflow:hidden">
      <div id="cap-bar" style="height:100%;background:var(--sA);border-radius:3px;transition:width .3s;width:0%"></div>
    </div>
  </div>

  <div class="card">
    <div class="card-title">今日狀況</div>
    <div class="stat-row"><span class="stat-label">等候組數</span><span class="stat-val" id="waiting">0</span></div>
    <div class="stat-row"><span class="stat-label">今日已服務</span><span class="stat-val" id="served">0</span></div>
    <div class="stat-row" style="border:none"><span class="stat-label">預估等待</span><span class="stat-val" id="est">—</span></div>
  </div>

  <!-- 候位名單 -->
  <div class="card">
    <div class="card-title" style="margin-bottom:10px">候位名單</div>
    <div id="queue-list"><span class="empty">目前無人候位</span></div>
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
    const res = await fetch(BACKEND_URL + '/api/state', { cache: 'no-store' });
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
    // 重置通知記錄，讓新的一輪可以重新通知
    await fetch(BACKEND_URL + '/api/reset-notify', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }
    }).catch(() => {});
    await syncFromServer();
    showToast('已叫號：' + fmt(entry.num));
  } catch(e) { showToast('網路錯誤'); }
}

async function confirmPickup() {
  const cur = state.A.current;
  if (!cur) { showToast('尚未叫號'); return; }
  try {
    const res = await fetch(BACKEND_URL + '/api/confirm-pickup', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ num: cur })
    });
    const data = await res.json();
    if (!data.success) { showToast(data.error || '確認失敗'); return; }
    await syncFromServer();
    document.getElementById('confirm-pickup-btn').style.display = 'none';
    showToast(\`\${fmt(cur)} 號已確認領瓶，開始製作\`);
  } catch(e) { showToast('網路錯誤'); }
}

async function completeMaking(num) {
  const entry = state.A.inProgress?.find(e => e.num === num);
  if (!entry) return;
  try {
    const res = await fetch(BACKEND_URL + '/api/complete-making', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ num })
    });
    const data = await res.json();
    await syncFromServer();
    showToast(\`\${fmt(num)} 號製作完成\`);
  } catch(e) { showToast('網路錯誤'); }
}

async function repeatCall() {
  const cur = state.A.current;
  if (!cur) { showToast('尚未開始叫號'); return; }
  const entry = state.A.lastCalledEntry || null;
  if (entry) {
    sendLineNotify(entry.userId, entry.phone, entry.name,
      \`🫙 心願瓶DIY｜📢 再次提醒 \${entry.name} 您好！請 \${fmt(cur)} 號前往領瓶處，謝謝！\`);
  }
  showToast('已重複叫號 ' + fmt(cur));
}

async function notifyPerson(num) {
  const entry = state.A.queue.find(q => q.num === num);
  if (!entry) return;
  sendLineNotify(entry.userId, entry.phone, entry.name,
    \`🫙 心願瓶DIY｜\${entry.name} 您好！您的 \${fmt(num)} 號快輪到囉，可以慢慢回到現場等候，叫到您的號碼時我們會再通知您 🙏\`);
  showToast('已傳送提醒給 ' + entry.name);
}



async function noShowCurrent() {
  const num = state.A.current;
  if (!num) return;
  const numStr = fmt(num);
  const svcName = cfg.services.A.name;
  const res = await fetch(BACKEND_URL + '/api/noshow', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ svc: 'A', num, requeue: true })
  });
  const data = await res.json();
  await syncFromServer();
  if (data.entry) {
    sendLineNotify(data.entry.userId, data.entry.phone, data.entry.name,
      \`🫙 \${svcName}｜\${data.entry.name} 您好！為維持現場服務順暢，您的 \${numStr} 號已稍作順延，將於下下組為您服務，感謝您的體諒與耐心等候，請留意叫號通知 🙏\`);
  }
  showToast(\`\${numStr} 已順延至第 2 位，已通知客人\`);
}



function render() {
  const q = state.A.queue;
  const cur = state.A.current;
  const mins = cfg.services.A.minutes;
  const inProg = state.A.inProgress || [];
  const totalCap = q.reduce((sum, e) => sum + (e.partySize || 1), 0);
  const inProgressCap = inProg.reduce((sum, e) => sum + (e.partySize || 1), 0);
  const availableNow = Math.max(0, 6 - inProgressCap);
  // 容量顯示
  const capBar = document.getElementById('cap-bar');
  const capText = document.getElementById('cap-text');
  const callBtn = document.querySelector('.btn-primary');
  if (capBar) { capBar.style.width = Math.min(100, inProgressCap / 6 * 100) + '%'; }
  if (capText) { capText.textContent = inProgressCap + ' / 6 人'; }
  if (capBar) { capBar.style.background = inProgressCap >= 6 ? 'var(--red)' : inProgressCap >= 4 ? 'var(--amber)' : 'var(--sA)'; }
  // 製作區容量僅供參考，不限制叫號
  if (callBtn) {
    callBtn.disabled = false;
    callBtn.style.opacity = '1';
    callBtn.title = '';
  }

  document.getElementById('cur-num').textContent = cur > 0 ? fmt(cur) : '—';
  const alreadyConfirmed = cur > 0 && inProg.find(e => e.num === cur);
  if (cur > 0) {
    const calledEntry = state.A.lastCalledEntry;
    const nameLabel = calledEntry ? calledEntry.name : '';
    const partySizeLabel = calledEntry && calledEntry.partySize > 1 ? \`（\${calledEntry.partySize} 人）\` : '';
    document.getElementById('cur-label').textContent = \`請 \${fmt(cur)} 號前往領瓶\`;
    document.getElementById('cur-name').textContent = nameLabel + partySizeLabel;
    document.getElementById('cur-name').style.display = nameLabel ? 'block' : 'none';
    // 顯示已確認領瓶按鈕（若尚未確認）
    const pickupBtn = document.getElementById('confirm-pickup-btn');
    if (pickupBtn) pickupBtn.style.display = alreadyConfirmed ? 'none' : 'flex';
  } else {
    document.getElementById('cur-label').textContent = '等待開始';
    const curNameEl = document.getElementById('cur-name');
    if (curNameEl) curNameEl.style.display = 'none';
    const pickupBtn2 = document.getElementById('confirm-pickup-btn');
    if (pickupBtn2) pickupBtn2.style.display = 'none';
  }

  // 製作中欄位
  const inProgressCard = document.getElementById('in-progress-card');
  const inProgressList = document.getElementById('in-progress-list');
  if (inProg.length > 0) {
    inProgressCard.style.display = 'block';
    inProgressList.innerHTML = inProg.map(entry => {
      return \`<div class="staff-entry">
        <div class="staff-num">\${fmt(entry.num)}</div>
        <div class="staff-info">
          <div class="staff-name">\${entry.name}</div>
          <div class="staff-meta">\${entry.partySize || 1} 人</div>
        </div>
        <div class="staff-btns">
          <button class="btn btn-sm" style="color:var(--green);border-color:var(--green-b);background:var(--green-bg);white-space:nowrap"
            onclick="completeMaking(\${entry.num})">製作完成 🆗</button>
        </div>
      </div>\`;
    }).join('');
  } else {
    inProgressCard.style.display = 'none';
  }
  document.getElementById('served').textContent = state.A.servedToday;
  document.getElementById('waiting').textContent = q.length + (totalCap > q.length ? \` (共 \${totalCap} 人)\` : '');
  const overCapacity = Math.max(0, inProgressCap + totalCap - 6);
  const estA = q.length > 0 ? Math.ceil(overCapacity / 6) * mins : 0;
  document.getElementById('est').textContent = q.length > 0 ? (estA > 0 ? '約 ' + estA + ' 分鐘' : '即將輪到') : '—';

  const list = document.getElementById('queue-list');
  // 顯示已叫號但未到場的提示：叫號後尚未確認領瓶，且不在製作中才顯示
  const lastCalled = state.A.lastCalledEntry;
  const calledNum = lastCalled?.num || 0;
  let html = '';
  const isInProgress = calledNum > 0 && inProg.find(e => e.num === calledNum);
  const isInQueue = calledNum > 0 && q.find(e => e.num === calledNum);
  if (calledNum > 0 && !isInProgress && !isInQueue) {
    html += \`<div class="staff-entry" style="background:var(--amber-bg);border-radius:var(--r-sm);padding:8px 10px;margin-bottom:8px;border:0.5px solid var(--amber-b)">
      <div class="staff-num" style="color:var(--amber)">\${fmt(calledNum)}</div>
      <div class="staff-info">
        <div class="staff-name" style="color:var(--amber)">\${lastCalled?.name || ''} 叫號後未到場</div>
        <div class="staff-meta">可標記未到場重排至第 2 位</div>
      </div>
      <div class="staff-btns">
        <button class="btn btn-sm" style="color:var(--amber);border-color:var(--amber-b);background:#fff"
          onclick="noShowCurrent()">未到場</button>
      </div>
    </div>\`;
  }
  if (q.length === 0) {
    list.innerHTML = html || '<span class="empty">目前無人候位</span>';
    return;
  }
  const displayQ = q.slice(0, 10);
  const remaining = q.length - 10;
  html += displayQ.map((entry, i) => {
    const cumCapBeforeWb = inProgressCap + q.slice(0, i).reduce((sum, e) => sum + (e.partySize || 1), 0);
    const batchesBeforeWb = Math.floor(cumCapBeforeWb / 6);
    const est = batchesBeforeWb * mins;
    const sizeLabel = (entry.partySize || 1) + ' 人';
    const posLabel = i === 0 ? '下一組' : (est === 0 ? '即將輪到' : \`約 \${est} 分鐘\`);
    return \`<div class="staff-entry">
      <div class="staff-num">\${fmt(entry.num)}</div>
      <div class="staff-info">
        <div class="staff-name">\${entry.name}</div>
        <div class="staff-meta">\${sizeLabel}｜\${posLabel}</div>
      </div>
      <div class="staff-btns">
        <button class="btn btn-sm" style="color:var(--amber);border-color:var(--amber-b);background:var(--amber-bg)"
          onclick="notifyPerson(\${entry.num})">提醒</button>
      </div>
    </div>\`;
  }).join('');
  if (remaining > 0) {
    html += \`<div style="text-align:center;padding:8px 0;font-size:12px;color:var(--text3)">還有 \${remaining} 組未顯示</div>\`;
  }
  list.innerHTML = html || '<span class="empty">目前無人候位</span>';
}

syncFromServer();
setInterval(syncFromServer, 2000);
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
    const res = await fetch(BACKEND_URL + '/api/state', { cache: 'no-store' });
    const data = await res.json();
    if (data.state) state = data.state;
    if (data.cfg) cfg = data.cfg;
    render();
  } catch(e) {}
}

function getLastCalled() {
  return state.B.lastCalledEntry || null;
}

async function sendLineNotify(userId, message) {
  if (!userId || userId === '—') return;
  try {
    await fetch(BACKEND_URL + '/api/line-notify', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ userId, message })
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
    sendLineNotify(entry.userId,
      \`🔮 塔羅牌占卜｜📢 \${entry.name} 您好！現在叫到 \${fmt(entry.num)} 號，請至塔羅牌區入座，謝謝！\`);
    await syncFromServer();
    if (state.B.queue.length > 0) {
      const next = state.B.queue[0];
      sendLineNotify(next.userId,
        \`🔮 塔羅牌占卜｜⏰ \${next.name} 您好！您是下一位（\${fmt(next.num)} 號），請提前回到現場準備。\`);
    }
    showToast('已叫號：' + fmt(entry.num));
  } catch(e) { showToast('網路錯誤'); }
}

async function repeatCall() {
  const cur = state.B.current;
  if (!cur) { showToast('尚未開始叫號'); return; }
  const entry = getLastCalled();
  if (entry) {
    sendLineNotify(entry.userId,
      \`🔮 塔羅牌占卜｜📢 再次提醒 \${entry.name} 您好！請 \${fmt(cur)} 號前往塔羅牌區入座，謝謝！\`);
  }
  showToast('已重複叫號 ' + fmt(cur));
}

async function notifyPerson(num) {
  const entry = state.B.queue.find(q => q.num === num);
  if (!entry) return;
  const pos = state.B.queue.indexOf(entry);
  const est = Math.max(0, Math.ceil((pos + 1) / 2) - 1) * cfg.services.B.minutes || cfg.services.B.minutes;
  sendLineNotify(entry.userId,
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

async function noShowCurrent() {
  const num = state.B.current;
  if (!num) return;
  const numStr = fmt(num);
  const svcName = cfg.services.B.name;
  const res = await fetch(BACKEND_URL + '/api/noshow', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ svc: 'B', num, requeue: true })
  });
  const data = await res.json();
  await syncFromServer();
  if (data.entry) {
    sendLineNotify(data.entry.userId,
      \`🔮 \${svcName}｜\${data.entry.name} 您好！叫號時暫時未見到您，已為您保留候位並重新安排至末位。若您仍在現場附近，請留意後續叫號通知；如需取消候位，可至取號頁面點取消按鈕，感謝您的配合 🙏\`);
  }
  showToast(\`\${numStr} 已重排至末位，已通知客人\`);
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
  const calledNum = state.B.current;
  let html = '';
  if (calledNum > 0 && !q.find(e => e.num === calledNum)) {
    html += \`<div class="staff-entry" style="background:var(--amber-bg);border-radius:var(--r-sm);padding:8px 10px;margin-bottom:8px;border:0.5px solid var(--amber-b)">
      <div class="staff-num" style="color:var(--amber)">\${fmt(calledNum)}</div>
      <div class="staff-info">
        <div class="staff-name" style="color:var(--amber)">剛剛叫號，等待中</div>
        <div class="staff-meta">若客人未到場可標記</div>
      </div>
      <div class="staff-btns">
        <button class="btn btn-sm" style="color:var(--amber);border-color:var(--amber-b);background:#fff"
          onclick="noShowCurrent()">未到場</button>
      </div>
    </div>\`;
  }
  if (q.length === 0 && !calledNum) { list.innerHTML = '<span class="empty">目前無人候位</span>'; return; }
  html += q.map((entry, i) => {
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
  list.innerHTML = html || '<span class="empty">目前無人候位</span>';
}

syncFromServer();
setInterval(syncFromServer, 2000);
</script>
</body>
</html>
`); });
app.get('/staff/tarot-sun', (req, res) => { res.setHeader('Content-Type', 'text/html; charset=utf-8'); res.send(`<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>🔮 ☀️ 太陽包廂</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#fff;--bg2:#f6f5f2;--bg3:#eeede9;
  --text:#1a1a1a;--text2:#5a5a5a;--text3:#999;
  --border:rgba(0,0,0,0.1);--border2:rgba(0,0,0,0.2);
  --r:10px;--r-sm:6px;
  --sB:#6d28d9;
  --amber:#854f0b;--amber-bg:#faeeda;--amber-b:#ef9f27;
  --green:#3b6d11;--green-bg:#eaf3de;--green-b:#97c459;
}
@media(prefers-color-scheme:dark){
  :root{
    --bg:#1c1c1e;--bg2:#2c2c2e;
    --text:#f2f2f7;--text2:#aeaeb2;--text3:#636366;
    --border:rgba(255,255,255,0.1);--border2:rgba(255,255,255,0.2);
  }
}
body{font-family:-apple-system,BlinkMacSystemFont,'Noto Sans TC',sans-serif;background:var(--bg2);color:var(--text);min-height:100vh;padding-top:72px}
.cabin-header{position:fixed;top:0;left:0;right:0;z-index:100;background:#fef3c7;border-bottom:3px solid #f59e0b;padding:12px 16px;display:flex;align-items:center;justify-content:center;gap:8px}
.live-dot{width:7px;height:7px;background:#639922;border-radius:50%;animation:pulse 1.5s infinite;position:absolute;left:16px;top:50%;transform:translateY(-50%)}
.cabin-title{font-size:18px;font-weight:800;color:#854f0b;letter-spacing:.02em}
.cabin-sub{font-size:11px;color:#854f0b;opacity:.7;position:absolute;right:16px;top:50%;transform:translateY(-50%);font-weight:500}
.app{max-width:480px;margin:0 auto;padding:14px 14px 60px}
.card{background:var(--bg);border:0.5px solid var(--border);border-radius:var(--r);padding:16px;margin-bottom:12px}
.card-title{font-size:11px;font-weight:500;color:var(--text3);letter-spacing:.05em;text-transform:uppercase;margin-bottom:12px}
.big-num{font-size:72px;font-weight:500;line-height:1;letter-spacing:-2px;text-align:center;color:var(--sB)}
.big-sub{font-size:12px;color:var(--text3);text-align:center;margin-top:6px}
.btn{display:flex;align-items:center;justify-content:center;padding:14px 18px;border:0.5px solid var(--border2);border-radius:var(--r-sm);background:transparent;font-size:15px;font-weight:500;color:var(--text);cursor:pointer;font-family:inherit;transition:all .15s;width:100%;margin-bottom:8px}
.btn:last-child{margin-bottom:0}
.btn:active{transform:scale(.97)}
.btn-primary{background:var(--sB);color:#fff;border-color:var(--sB)}
.other-cabin{background:var(--bg);border:0.5px solid var(--border);border-radius:var(--r);padding:12px 16px;margin-bottom:12px;display:flex;align-items:center;gap:12px}
.queue-item{display:flex;align-items:center;gap:10px;padding:11px 0;border-bottom:0.5px solid var(--border)}
.queue-item:last-child{border-bottom:none}
.queue-num{font-size:15px;font-weight:500;min-width:52px;color:var(--sB)}
.queue-info{flex:1}
.queue-name{font-size:13px;font-weight:500;color:var(--text)}
.queue-meta{font-size:11px;color:var(--text3);margin-top:1px}
.empty{font-size:13px;color:var(--text3);font-style:italic;padding:4px 0}
.noshow-bar{background:var(--amber-bg);border:0.5px solid var(--amber-b);border-radius:var(--r-sm);padding:10px 14px;margin-bottom:8px;display:flex;align-items:center;justify-content:space-between;display:none}
.noshow-btn{color:var(--amber);border:0.5px solid var(--amber-b);background:#fff;border-radius:var(--r-sm);padding:6px 14px;font-size:12px;font-weight:600;cursor:pointer;font-family:inherit}
.auto-bar{background:var(--green-bg);border:0.5px solid var(--green-b);border-radius:var(--r-sm);padding:8px 14px;margin-bottom:8px;font-size:12px;color:var(--green);display:none;line-height:1.5}
.toast{position:fixed;bottom:24px;left:50%;transform:translateX(-50%) translateY(80px);background:var(--text);color:var(--bg);padding:10px 22px;border-radius:99px;font-size:13px;font-weight:500;transition:transform .25s;z-index:999;white-space:nowrap;pointer-events:none}
.toast.show{transform:translateX(-50%) translateY(0)}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
</style>
</head>
<body>

<div class="cabin-header">
  <div class="live-dot"></div>
  <div style="font-size:24px;line-height:1">☀️</div>
  <div class="cabin-title">太陽包廂</div>
  <div class="cabin-sub">🔮 塔羅牌占卜</div>
</div>

<div class="app">

  <!-- 叫號操作 -->
  <div class="card">
    <div style="text-align:center;padding:12px 0 16px">
      <div style="font-size:11px;color:var(--text3);margin-bottom:4px">此包廂目前服務</div>
      <div class="big-num" id="cur-num">—</div>
      <div class="big-sub" id="cur-label">等待開始</div>
    </div>
    <button class="btn btn-primary" onclick="callNext()">叫下一號 →</button>
    <button class="btn" onclick="repeatCall()" style="margin-bottom:0">重複叫號</button>
  </div>

  <!-- 對方包廂 -->
  <div class="other-cabin">
    <div style="font-size:22px">🌙</div>
    <div>
      <div style="font-size:11px;color:var(--text3);margin-bottom:2px">月亮包廂 目前服務</div>
      <div style="font-size:20px;font-weight:700;color:var(--text)" id="other-cur">—</div>
    </div>
  </div>

  <!-- 今日已服務 -->
  <div class="card">
    <div class="card-title" style="margin-bottom:8px">今日已服務</div>
    <div style="display:flex;justify-content:space-between;align-items:center">
      <span style="font-size:13px;color:var(--text2)">已完成服務人數</span>
      <span style="font-size:14px;font-weight:500" id="served-count">0 人</span>
    </div>
  </div>

  <!-- 候位即時動態 -->
  <div class="card">
    <div class="card-title">候位即時動態</div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px">
      <div style="text-align:center;padding:10px;background:var(--bg2);border-radius:var(--r-sm)">
        <div style="font-size:11px;color:var(--text3);margin-bottom:4px">等候人數</div>
        <div style="font-size:28px;font-weight:600;color:var(--text)" id="waiting-count">0</div>
        <div style="font-size:11px;color:var(--text3)">人</div>
      </div>
      <div style="text-align:center;padding:10px;background:var(--bg2);border-radius:var(--r-sm)">
        <div style="font-size:11px;color:var(--text3);margin-bottom:4px">預估等待</div>
        <div style="font-size:28px;font-weight:600;color:var(--text)" id="est-wait">—</div>
        <div style="font-size:11px;color:var(--text3)">分鐘</div>
      </div>
    </div>

    <!-- 10分鐘自動提醒倒數 -->
    <div class="auto-bar" id="auto-bar">
      ⏰ <span id="auto-text"></span>
    </div>

    <!-- 未到場提示 -->
    <div class="noshow-bar" id="noshow-bar">
      <div>
        <div style="font-size:13px;font-weight:600;color:var(--amber)" id="noshow-label">—</div>
        <div style="font-size:11px;color:var(--amber);opacity:.8">叫號後未出現</div>
      </div>
      <button class="noshow-btn" onclick="noShowCurrent()">未到場</button>
    </div>

    <!-- 候位名單（純顯示）-->
    <div id="queue-list"><span class="empty">目前無人候位</span></div>
  </div>

</div>
<div class="toast" id="toast"></div>

<script>
const BACKEND_URL = 'https://mercury-gcac.onrender.com';
const CABIN_ID = 'sun';
const OTHER_CABIN_ID = 'moon';
const CABIN_NAME = '☀️ 太陽包廂';
let AUTO_NOTIFY_MS = (cfg.tarotNotifyMins || 10) * 60 * 1000;

let state = { B: { current: 0, lastIssued: 0, queue: [], servedToday: 0, lastCalledEntry: null, cabins: { sun: {current:0,lastEntry:null}, moon: {current:0,lastEntry:null} } } };
let cfg = { services: { B: { name: '塔羅牌占卜', prefix: 'T', minutes: 15 } } };
let autoTimer = null;
let autoTargetNum = null;

function fmt(n) { return cfg.services.B.prefix + String(n).padStart(3,'0'); }
function showToast(msg) {
  const el = document.getElementById('toast');
  el.textContent = msg; el.classList.add('show');
  setTimeout(() => el.classList.remove('show'), 2400);
}

async function syncFromServer() {
  try {
    const res = await fetch(BACKEND_URL + '/api/state', { cache: 'no-store' });
    const data = await res.json();
    if (data.state) state = data.state;
    if (data.cfg) { cfg = data.cfg; AUTO_NOTIFY_MS = (cfg.tarotNotifyMins || 10) * 60 * 1000; }
    render();
  } catch(e) {}
}

async function sendLineNotify(userId, message) {
  if (!userId || userId === '—') return;
  try {
    await fetch(BACKEND_URL + '/api/line-notify', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ userId, message })
    });
  } catch(e) {}
}

// 10 分鐘後自動提醒下一位
function scheduleAutoNotify(nextEntry) {
  if (autoTimer) clearTimeout(autoTimer);
  autoTargetNum = null;
  const bar = document.getElementById('auto-bar');
  const text = document.getElementById('auto-text');
  if (!nextEntry) { if (bar) bar.style.display = 'none'; return; }
  autoTargetNum = nextEntry.num;
  if (bar) {
    bar.style.display = 'block';
    text.textContent = \`將於 \${cfg.tarotNotifyMins || 10} 分鐘後自動提醒 \${nextEntry.name}（\${fmt(nextEntry.num)}）準備回場\`;
  }
  autoTimer = setTimeout(async () => {
    await syncFromServer();
    const still = state.B.queue.find(e => e.num === autoTargetNum);
    if (still) {
      sendLineNotify(still.userId,
        \`🔮 塔羅牌占卜｜⏰ \${still.name} 您好！您的 \${fmt(still.num)} 號快輪到了，請先回到現場附近準備，我們將在您的號碼叫到時再次通知您 🙏\`);
      if (bar) {
        text.textContent = \`✅ 已自動提醒 \${still.name}（\${fmt(still.num)}）\`;
        setTimeout(() => { bar.style.display = 'none'; }, 8000);
      }
    } else {
      if (bar) bar.style.display = 'none';
    }
    autoTimer = null;
    autoTargetNum = null;
  }, AUTO_NOTIFY_MS);
}

async function callNext() {
  if (state.B.queue.length === 0) { showToast('目前無人候位'); return; }
  try {
    const res = await fetch(BACKEND_URL + '/api/call-next', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ svc: 'B', cabin: CABIN_ID })
    });
    const data = await res.json();
    if (!data.success) {
      showToast(data.error?.includes('繁忙') ? '⚠️ 另一個包廂正在叫號，請稍後再試' : data.error || '叫號失敗');
      return;
    }
    const entry = data.called;
    sendLineNotify(entry.userId,
      \`🔮 塔羅牌占卜｜📢 \${entry.name} 您好！現在叫到 \${fmt(entry.num)} 號，請前往 \${CABIN_NAME} 入座，謝謝！\`);
    await syncFromServer();
    // 設定 10 分鐘後提醒下一位
    const nextInQueue = state.B.queue.length > 0 ? state.B.queue[0] : null;
    scheduleAutoNotify(nextInQueue);
    showToast('已叫號：' + fmt(entry.num));
  } catch(e) { showToast('網路錯誤'); }
}

async function repeatCall() {
  const repeatEntry = state.B.cabins?.[CABIN_ID]?.lastEntry;
  // 本包廂從未叫過號
  if (!repeatEntry) { showToast('此包廂尚未叫號'); return; }
  // 重複通知本包廂最後叫出的號
  sendLineNotify(repeatEntry.userId,
    \`🔮 塔羅牌占卜｜📢 再次提醒 \${repeatEntry.name} 您好！請 \${fmt(repeatEntry.num)} 號前往 \${CABIN_NAME} 入座，謝謝！\`);
  showToast('已重複叫號 ' + fmt(repeatEntry.num));
}

async function noShowCurrent() {
  const noshowEntry = state.B.cabins?.[CABIN_ID]?.lastEntry;
  if (!noshowEntry) { showToast('尚未叫號'); return; }
  const num = noshowEntry.num;
  const numStr = fmt(num);
  const res = await fetch(BACKEND_URL + '/api/noshow', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ svc: 'B', num, requeue: true })
  });
  const data = await res.json();
  await syncFromServer();
  if (data.entry) {
    sendLineNotify(data.entry.userId,
      \`🔮 塔羅牌占卜｜\${data.entry.name} 您好！叫號時暫時未見到您，已為您保留候位並重新安排至末位。若您仍在現場附近，請留意後續叫號通知；如需取消候位，可至取號頁面點取消按鈕，感謝您的配合 🙏\`);
  }
  // 重新安排自動提醒給新的下一位
  const noshowNextInQueue = state.B.queue.length > 0 ? state.B.queue[0] : null;
  scheduleAutoNotify(noshowNextInQueue);
  showToast(\`\${numStr} 已重排至末位，已通知客人\`);
}

function render() {
  const q = state.B.queue;
  const mins = cfg.services.B.minutes;

  // 本包廂目前服務號
  const myCurrent = state.B.cabins?.[CABIN_ID]?.current || 0;
  document.getElementById('cur-num').textContent = myCurrent > 0 ? fmt(myCurrent) : '—';
  document.getElementById('cur-label').textContent = myCurrent > 0 ? \`請 \${fmt(myCurrent)} 號入座\` : '等待開始';

  // 對方包廂目前服務號
  const otherCurrent = state.B.cabins?.[OTHER_CABIN_ID]?.current || 0;
  document.getElementById('other-cur').textContent = otherCurrent > 0 ? fmt(otherCurrent) : '—';

  // 今日已服務
  const myServed = state.B.cabins?.[CABIN_ID]?.servedToday || 0;
  document.getElementById('served-count').textContent = myServed + ' 人';

  // 等候人數與預估
  document.getElementById('waiting-count').textContent = q.length;
  const estMins = q.length > 0 ? Math.max(0, Math.ceil(q.length / 2) - 1) * mins : 0;
  document.getElementById('est-wait').textContent = q.length > 0 ? (estMins > 0 ? estMins : '即將') : '—';

  // 未到場提示（只顯示本包廂叫出的號）
  const noshowBar = document.getElementById('noshow-bar');
  const noshowLabel = document.getElementById('noshow-label');
  const renderMyCabinEntry = state.B.cabins?.[CABIN_ID]?.lastEntry;
  const myLastNum = renderMyCabinEntry?.num || 0;
  const myCabinCurrent = state.B.cabins?.[CABIN_ID]?.current || 0;
  // 只有本包廂叫的號、且不在候位序列中、且尚未被處理才顯示
  if (myLastNum > 0 && myCabinCurrent === myLastNum && !q.find(e => e.num === myLastNum)) {
    noshowBar.style.display = 'flex';
    noshowLabel.textContent = \`\${fmt(myLastNum)} 號叫號後未出現\`;
  } else {
    noshowBar.style.display = 'none';
  }

  // 候位名單（純顯示，最多10位）
  const list = document.getElementById('queue-list');
  if (q.length === 0) { list.innerHTML = '<span class="empty">目前無人候位</span>'; return; }
  const displayQ = q.slice(0, 10);
  const remaining = q.length - 10;
  let html = displayQ.map((entry, i) => {
    const est = Math.max(0, Math.ceil((i + 1) / 2) - 1) * mins || mins;
    const posLabel = i === 0 ? '下一位' : \`第 \${i + 1} 位，約 \${est} 分鐘\`;
    return \`<div class="queue-item">
      <div class="queue-num">\${fmt(entry.num)}</div>
      <div class="queue-info">
        <div class="queue-name">\${entry.name}</div>
        <div class="queue-meta">\${posLabel}</div>
      </div>
    </div>\`;
  }).join('');
  if (remaining > 0) {
    html += \`<div style="text-align:center;padding:8px 0;font-size:12px;color:var(--text3)">還有 \${remaining} 人未顯示</div>\`;
  }
  list.innerHTML = html;
}

syncFromServer();
setInterval(syncFromServer, 2000);
</script>
</body>
</html>`); });
app.get('/staff/tarot-moon', (req, res) => { res.setHeader('Content-Type', 'text/html; charset=utf-8'); res.send(`<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>🔮 🌙 月亮包廂</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#fff;--bg2:#f6f5f2;--bg3:#eeede9;
  --text:#1a1a1a;--text2:#5a5a5a;--text3:#999;
  --border:rgba(0,0,0,0.1);--border2:rgba(0,0,0,0.2);
  --r:10px;--r-sm:6px;
  --sB:#6d28d9;
  --amber:#854f0b;--amber-bg:#faeeda;--amber-b:#ef9f27;
  --green:#3b6d11;--green-bg:#eaf3de;--green-b:#97c459;
}
@media(prefers-color-scheme:dark){
  :root{
    --bg:#1c1c1e;--bg2:#2c2c2e;
    --text:#f2f2f7;--text2:#aeaeb2;--text3:#636366;
    --border:rgba(255,255,255,0.1);--border2:rgba(255,255,255,0.2);
  }
}
body{font-family:-apple-system,BlinkMacSystemFont,'Noto Sans TC',sans-serif;background:var(--bg2);color:var(--text);min-height:100vh;padding-top:72px}
.cabin-header{position:fixed;top:0;left:0;right:0;z-index:100;background:#dbeafe;border-bottom:3px solid #93c5fd;padding:12px 16px;display:flex;align-items:center;justify-content:center;gap:8px}
.live-dot{width:7px;height:7px;background:#639922;border-radius:50%;animation:pulse 1.5s infinite;position:absolute;left:16px;top:50%;transform:translateY(-50%)}
.cabin-title{font-size:18px;font-weight:800;color:#1e40af;letter-spacing:.02em}
.cabin-sub{font-size:11px;color:#1e40af;opacity:.7;position:absolute;right:16px;top:50%;transform:translateY(-50%);font-weight:500}
.app{max-width:480px;margin:0 auto;padding:14px 14px 60px}
.card{background:var(--bg);border:0.5px solid var(--border);border-radius:var(--r);padding:16px;margin-bottom:12px}
.card-title{font-size:11px;font-weight:500;color:var(--text3);letter-spacing:.05em;text-transform:uppercase;margin-bottom:12px}
.big-num{font-size:72px;font-weight:500;line-height:1;letter-spacing:-2px;text-align:center;color:var(--sB)}
.big-sub{font-size:12px;color:var(--text3);text-align:center;margin-top:6px}
.btn{display:flex;align-items:center;justify-content:center;padding:14px 18px;border:0.5px solid var(--border2);border-radius:var(--r-sm);background:transparent;font-size:15px;font-weight:500;color:var(--text);cursor:pointer;font-family:inherit;transition:all .15s;width:100%;margin-bottom:8px}
.btn:last-child{margin-bottom:0}
.btn:active{transform:scale(.97)}
.btn-primary{background:var(--sB);color:#fff;border-color:var(--sB)}
.other-cabin{background:var(--bg);border:0.5px solid var(--border);border-radius:var(--r);padding:12px 16px;margin-bottom:12px;display:flex;align-items:center;gap:12px}
.queue-item{display:flex;align-items:center;gap:10px;padding:11px 0;border-bottom:0.5px solid var(--border)}
.queue-item:last-child{border-bottom:none}
.queue-num{font-size:15px;font-weight:500;min-width:52px;color:var(--sB)}
.queue-info{flex:1}
.queue-name{font-size:13px;font-weight:500;color:var(--text)}
.queue-meta{font-size:11px;color:var(--text3);margin-top:1px}
.empty{font-size:13px;color:var(--text3);font-style:italic;padding:4px 0}
.noshow-bar{background:var(--amber-bg);border:0.5px solid var(--amber-b);border-radius:var(--r-sm);padding:10px 14px;margin-bottom:8px;display:flex;align-items:center;justify-content:space-between;display:none}
.noshow-btn{color:var(--amber);border:0.5px solid var(--amber-b);background:#fff;border-radius:var(--r-sm);padding:6px 14px;font-size:12px;font-weight:600;cursor:pointer;font-family:inherit}
.auto-bar{background:var(--green-bg);border:0.5px solid var(--green-b);border-radius:var(--r-sm);padding:8px 14px;margin-bottom:8px;font-size:12px;color:var(--green);display:none;line-height:1.5}
.toast{position:fixed;bottom:24px;left:50%;transform:translateX(-50%) translateY(80px);background:var(--text);color:var(--bg);padding:10px 22px;border-radius:99px;font-size:13px;font-weight:500;transition:transform .25s;z-index:999;white-space:nowrap;pointer-events:none}
.toast.show{transform:translateX(-50%) translateY(0)}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
</style>
</head>
<body>

<div class="cabin-header">
  <div class="live-dot"></div>
  <div style="font-size:24px;line-height:1">🌙</div>
  <div class="cabin-title">月亮包廂</div>
  <div class="cabin-sub">🔮 塔羅牌占卜</div>
</div>

<div class="app">

  <!-- 叫號操作 -->
  <div class="card">
    <div style="text-align:center;padding:12px 0 16px">
      <div style="font-size:11px;color:var(--text3);margin-bottom:4px">此包廂目前服務</div>
      <div class="big-num" id="cur-num">—</div>
      <div class="big-sub" id="cur-label">等待開始</div>
    </div>
    <button class="btn btn-primary" onclick="callNext()">叫下一號 →</button>
    <button class="btn" onclick="repeatCall()" style="margin-bottom:0">重複叫號</button>
  </div>

  <!-- 對方包廂 -->
  <div class="other-cabin">
    <div style="font-size:22px">☀️</div>
    <div>
      <div style="font-size:11px;color:var(--text3);margin-bottom:2px">太陽包廂 目前服務</div>
      <div style="font-size:20px;font-weight:700;color:var(--text)" id="other-cur">—</div>
    </div>
  </div>

  <!-- 今日已服務 -->
  <div class="card">
    <div class="card-title" style="margin-bottom:8px">今日已服務</div>
    <div style="display:flex;justify-content:space-between;align-items:center">
      <span style="font-size:13px;color:var(--text2)">已完成服務人數</span>
      <span style="font-size:14px;font-weight:500" id="served-count">0 人</span>
    </div>
  </div>

  <!-- 候位即時動態 -->
  <div class="card">
    <div class="card-title">候位即時動態</div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px">
      <div style="text-align:center;padding:10px;background:var(--bg2);border-radius:var(--r-sm)">
        <div style="font-size:11px;color:var(--text3);margin-bottom:4px">等候人數</div>
        <div style="font-size:28px;font-weight:600;color:var(--text)" id="waiting-count">0</div>
        <div style="font-size:11px;color:var(--text3)">人</div>
      </div>
      <div style="text-align:center;padding:10px;background:var(--bg2);border-radius:var(--r-sm)">
        <div style="font-size:11px;color:var(--text3);margin-bottom:4px">預估等待</div>
        <div style="font-size:28px;font-weight:600;color:var(--text)" id="est-wait">—</div>
        <div style="font-size:11px;color:var(--text3)">分鐘</div>
      </div>
    </div>

    <!-- 10分鐘自動提醒倒數 -->
    <div class="auto-bar" id="auto-bar">
      ⏰ <span id="auto-text"></span>
    </div>

    <!-- 未到場提示 -->
    <div class="noshow-bar" id="noshow-bar">
      <div>
        <div style="font-size:13px;font-weight:600;color:var(--amber)" id="noshow-label">—</div>
        <div style="font-size:11px;color:var(--amber);opacity:.8">叫號後未出現</div>
      </div>
      <button class="noshow-btn" onclick="noShowCurrent()">未到場</button>
    </div>

    <!-- 候位名單（純顯示）-->
    <div id="queue-list"><span class="empty">目前無人候位</span></div>
  </div>

</div>
<div class="toast" id="toast"></div>

<script>
const BACKEND_URL = 'https://mercury-gcac.onrender.com';
const CABIN_ID = 'moon';
const OTHER_CABIN_ID = 'sun';
const CABIN_NAME = '🌙 月亮包廂';
let AUTO_NOTIFY_MS = (cfg.tarotNotifyMins || 10) * 60 * 1000;

let state = { B: { current: 0, lastIssued: 0, queue: [], servedToday: 0, lastCalledEntry: null, cabins: { sun: {current:0,lastEntry:null}, moon: {current:0,lastEntry:null} } } };
let cfg = { services: { B: { name: '塔羅牌占卜', prefix: 'T', minutes: 15 } } };
let autoTimer = null;
let autoTargetNum = null;

function fmt(n) { return cfg.services.B.prefix + String(n).padStart(3,'0'); }
function showToast(msg) {
  const el = document.getElementById('toast');
  el.textContent = msg; el.classList.add('show');
  setTimeout(() => el.classList.remove('show'), 2400);
}

async function syncFromServer() {
  try {
    const res = await fetch(BACKEND_URL + '/api/state', { cache: 'no-store' });
    const data = await res.json();
    if (data.state) state = data.state;
    if (data.cfg) { cfg = data.cfg; AUTO_NOTIFY_MS = (cfg.tarotNotifyMins || 10) * 60 * 1000; }
    render();
  } catch(e) {}
}

async function sendLineNotify(userId, message) {
  if (!userId || userId === '—') return;
  try {
    await fetch(BACKEND_URL + '/api/line-notify', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ userId, message })
    });
  } catch(e) {}
}

// 10 分鐘後自動提醒下一位
function scheduleAutoNotify(nextEntry) {
  if (autoTimer) clearTimeout(autoTimer);
  autoTargetNum = null;
  const bar = document.getElementById('auto-bar');
  const text = document.getElementById('auto-text');
  if (!nextEntry) { if (bar) bar.style.display = 'none'; return; }
  autoTargetNum = nextEntry.num;
  if (bar) {
    bar.style.display = 'block';
    text.textContent = \`將於 \${cfg.tarotNotifyMins || 10} 分鐘後自動提醒 \${nextEntry.name}（\${fmt(nextEntry.num)}）準備回場\`;
  }
  autoTimer = setTimeout(async () => {
    await syncFromServer();
    const still = state.B.queue.find(e => e.num === autoTargetNum);
    if (still) {
      sendLineNotify(still.userId,
        \`🔮 塔羅牌占卜｜⏰ \${still.name} 您好！您的 \${fmt(still.num)} 號快輪到了，請先回到現場附近準備，我們將在您的號碼叫到時再次通知您 🙏\`);
      if (bar) {
        text.textContent = \`✅ 已自動提醒 \${still.name}（\${fmt(still.num)}）\`;
        setTimeout(() => { bar.style.display = 'none'; }, 8000);
      }
    } else {
      if (bar) bar.style.display = 'none';
    }
    autoTimer = null;
    autoTargetNum = null;
  }, AUTO_NOTIFY_MS);
}

async function callNext() {
  if (state.B.queue.length === 0) { showToast('目前無人候位'); return; }
  try {
    const res = await fetch(BACKEND_URL + '/api/call-next', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ svc: 'B', cabin: CABIN_ID })
    });
    const data = await res.json();
    if (!data.success) {
      showToast(data.error?.includes('繁忙') ? '⚠️ 另一個包廂正在叫號，請稍後再試' : data.error || '叫號失敗');
      return;
    }
    const entry = data.called;
    sendLineNotify(entry.userId,
      \`🔮 塔羅牌占卜｜📢 \${entry.name} 您好！現在叫到 \${fmt(entry.num)} 號，請前往 \${CABIN_NAME} 入座，謝謝！\`);
    await syncFromServer();
    // 設定 10 分鐘後提醒下一位
    const nextInQueue = state.B.queue.length > 0 ? state.B.queue[0] : null;
    scheduleAutoNotify(nextInQueue);
    showToast('已叫號：' + fmt(entry.num));
  } catch(e) { showToast('網路錯誤'); }
}

async function repeatCall() {
  const repeatEntry = state.B.cabins?.[CABIN_ID]?.lastEntry;
  // 本包廂從未叫過號
  if (!repeatEntry) { showToast('此包廂尚未叫號'); return; }
  // 重複通知本包廂最後叫出的號
  sendLineNotify(repeatEntry.userId,
    \`🔮 塔羅牌占卜｜📢 再次提醒 \${repeatEntry.name} 您好！請 \${fmt(repeatEntry.num)} 號前往 \${CABIN_NAME} 入座，謝謝！\`);
  showToast('已重複叫號 ' + fmt(repeatEntry.num));
}

async function noShowCurrent() {
  const noshowEntry = state.B.cabins?.[CABIN_ID]?.lastEntry;
  if (!noshowEntry) { showToast('尚未叫號'); return; }
  const num = noshowEntry.num;
  const numStr = fmt(num);
  const res = await fetch(BACKEND_URL + '/api/noshow', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ svc: 'B', num, requeue: true })
  });
  const data = await res.json();
  await syncFromServer();
  if (data.entry) {
    sendLineNotify(data.entry.userId,
      \`🔮 塔羅牌占卜｜\${data.entry.name} 您好！叫號時暫時未見到您，已為您保留候位並重新安排至末位。若您仍在現場附近，請留意後續叫號通知；如需取消候位，可至取號頁面點取消按鈕，感謝您的配合 🙏\`);
  }
  // 重新安排自動提醒給新的下一位
  const noshowNextInQueue = state.B.queue.length > 0 ? state.B.queue[0] : null;
  scheduleAutoNotify(noshowNextInQueue);
  showToast(\`\${numStr} 已重排至末位，已通知客人\`);
}

function render() {
  const q = state.B.queue;
  const mins = cfg.services.B.minutes;

  // 本包廂目前服務號
  const myCurrent = state.B.cabins?.[CABIN_ID]?.current || 0;
  document.getElementById('cur-num').textContent = myCurrent > 0 ? fmt(myCurrent) : '—';
  document.getElementById('cur-label').textContent = myCurrent > 0 ? \`請 \${fmt(myCurrent)} 號入座\` : '等待開始';

  // 對方包廂目前服務號
  const otherCurrent = state.B.cabins?.[OTHER_CABIN_ID]?.current || 0;
  document.getElementById('other-cur').textContent = otherCurrent > 0 ? fmt(otherCurrent) : '—';

  // 今日已服務
  const myServed = state.B.cabins?.[CABIN_ID]?.servedToday || 0;
  document.getElementById('served-count').textContent = myServed + ' 人';

  // 等候人數與預估
  document.getElementById('waiting-count').textContent = q.length;
  const estMins = q.length > 0 ? Math.max(0, Math.ceil(q.length / 2) - 1) * mins : 0;
  document.getElementById('est-wait').textContent = q.length > 0 ? (estMins > 0 ? estMins : '即將') : '—';

  // 未到場提示（只顯示本包廂叫出的號）
  const noshowBar = document.getElementById('noshow-bar');
  const noshowLabel = document.getElementById('noshow-label');
  const renderMyCabinEntry = state.B.cabins?.[CABIN_ID]?.lastEntry;
  const myLastNum = renderMyCabinEntry?.num || 0;
  const myCabinCurrent = state.B.cabins?.[CABIN_ID]?.current || 0;
  // 只有本包廂叫的號、且不在候位序列中、且尚未被處理才顯示
  if (myLastNum > 0 && myCabinCurrent === myLastNum && !q.find(e => e.num === myLastNum)) {
    noshowBar.style.display = 'flex';
    noshowLabel.textContent = \`\${fmt(myLastNum)} 號叫號後未出現\`;
  } else {
    noshowBar.style.display = 'none';
  }

  // 候位名單（純顯示，最多10位）
  const list = document.getElementById('queue-list');
  if (q.length === 0) { list.innerHTML = '<span class="empty">目前無人候位</span>'; return; }
  const displayQ = q.slice(0, 10);
  const remaining = q.length - 10;
  let html = displayQ.map((entry, i) => {
    const est = Math.max(0, Math.ceil((i + 1) / 2) - 1) * mins || mins;
    const posLabel = i === 0 ? '下一位' : \`第 \${i + 1} 位，約 \${est} 分鐘\`;
    return \`<div class="queue-item">
      <div class="queue-num">\${fmt(entry.num)}</div>
      <div class="queue-info">
        <div class="queue-name">\${entry.name}</div>
        <div class="queue-meta">\${posLabel}</div>
      </div>
    </div>\`;
  }).join('');
  if (remaining > 0) {
    html += \`<div style="text-align:center;padding:8px 0;font-size:12px;color:var(--text3)">還有 \${remaining} 人未顯示</div>\`;
  }
  list.innerHTML = html;
}

syncFromServer();
setInterval(syncFromServer, 2000);
</script>
</body>
</html>`); });
app.get('/manual', (req, res) => { res.setHeader('Content-Type', 'text/html; charset=utf-8'); res.send(`<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>點晶礦活動｜排隊叫號系統使用手冊</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#f8fafc;
  --card:#ffffff;
  --text:#0f172a;
  --text2:#475569;
  --text3:#94a3b8;
  --border:#e2e8f0;
  --blue:#1d4ed8;
  --blue2:#3b82f6;
  --blue-bg:#eff6ff;
  --blue-light:#dbeafe;
  --teal:#0f766e;
  --teal-bg:#f0fdfa;
  --purple:#6d28d9;
  --purple-bg:#f5f3ff;
  --green:#15803d;
  --green-bg:#f0fdf4;
  --green-light:#dcfce7;
  --amber:#92400e;
  --amber-bg:#fffbeb;
  --amber-light:#fef3c7;
  --red:#b91c1c;
  --red-bg:#fff5f5;
  --red-light:#fee2e2;
  --r:12px;--r-sm:8px;--r-xs:4px;
}
html{scroll-behavior:smooth}
body{font-family:-apple-system,BlinkMacSystemFont,'Noto Sans TC','PingFang TC',sans-serif;background:var(--bg);color:var(--text);line-height:1.65;font-size:15px}

/* ── NAV ── */
.nav{position:sticky;top:0;z-index:200;background:rgba(255,255,255,.96);backdrop-filter:blur(12px);border-bottom:1px solid var(--border)}
.nav-inner{max-width:960px;margin:0 auto;padding:0 20px;display:flex;align-items:center;gap:16px;height:56px}
.nav-logo{font-size:15px;font-weight:800;color:var(--blue);white-space:nowrap;display:flex;align-items:center;gap:6px}
.nav-scroll{display:flex;gap:2px;overflow-x:auto;scrollbar-width:none;flex:1}
.nav-scroll::-webkit-scrollbar{display:none}
.nav-btn{padding:6px 14px;border-radius:99px;font-size:13px;font-weight:500;color:var(--text2);white-space:nowrap;cursor:pointer;border:none;background:none;text-decoration:none;transition:all .15s;display:inline-block}
.nav-btn:hover{background:var(--blue-light);color:var(--blue)}
.nav-btn.active{background:var(--blue);color:#fff}

/* ── LAYOUT ── */
.wrap{max-width:960px;margin:0 auto;padding:0 20px 100px}

/* ── COVER ── */
.cover{border-radius:20px;background:linear-gradient(140deg,#0c1445 0%,#1a3aad 40%,#2e5ce5 70%,#4f83f0 100%);padding:56px 40px 48px;margin:32px 0 48px;color:#fff;position:relative;overflow:hidden}
.cover::before{content:'';position:absolute;inset:0;background:url("data:image/svg+xml,%3Csvg width='60' height='60' viewBox='0 0 60 60' xmlns='http://www.w3.org/2000/svg'%3E%3Cg fill='none' fill-rule='evenodd'%3E%3Cg fill='%23ffffff' fill-opacity='0.03'%3E%3Ccircle cx='30' cy='30' r='20'/%3E%3C/g%3E%3C/g%3E%3C/svg%3E") repeat}
.cover-inner{position:relative;z-index:1}
.cover-tag{display:inline-block;background:rgba(255,255,255,.18);border:1px solid rgba(255,255,255,.3);border-radius:99px;padding:4px 14px;font-size:12px;font-weight:600;letter-spacing:.08em;margin-bottom:20px}
.cover h1{font-size:36px;font-weight:900;letter-spacing:-.03em;line-height:1.15;margin-bottom:10px}
.cover-sub{font-size:16px;opacity:.75;margin-bottom:36px}
.cover-cards{display:grid;grid-template-columns:1fr 1fr;gap:14px;max-width:520px}
.cover-card{background:rgba(255,255,255,.12);border:1px solid rgba(255,255,255,.22);border-radius:var(--r);padding:18px 20px;display:flex;gap:14px;align-items:flex-start;transition:background .2s}
.cover-card:hover{background:rgba(255,255,255,.2)}
.cover-card-icon{font-size:26px;flex-shrink:0;margin-top:2px}
.cover-card-title{font-size:15px;font-weight:700;margin-bottom:3px}
.cover-card-sub{font-size:12px;opacity:.65;line-height:1.5}

/* ── CHAPTER HERO ── */
.chapter-hero{border-radius:16px;padding:36px 32px;margin:48px 0 32px;color:#fff;position:relative;overflow:hidden}
.chapter-hero.wb{background:linear-gradient(135deg,#134e4a,#0f766e,#0d9488)}
.chapter-hero.tarot{background:linear-gradient(135deg,#1e1b4b,#4338ca,#6d28d9)}
.chapter-hero::after{content:'';position:absolute;right:-20px;bottom:-20px;width:160px;height:160px;border-radius:50%;background:rgba(255,255,255,.06)}
.chapter-hero-inner{position:relative;z-index:1}
.chapter-num{font-size:11px;font-weight:700;letter-spacing:.15em;text-transform:uppercase;opacity:.6;margin-bottom:8px}
.chapter-hero h2{font-size:28px;font-weight:900;margin-bottom:6px}
.chapter-hero p{font-size:14px;opacity:.75;max-width:520px}
.chapter-hero-badges{display:flex;gap:8px;margin-top:16px;flex-wrap:wrap}
.chapter-badge{background:rgba(255,255,255,.18);border:1px solid rgba(255,255,255,.25);border-radius:99px;padding:4px 12px;font-size:12px;font-weight:500}

/* ── SECTION ── */
.section{margin-bottom:40px}
.section-title{font-size:18px;font-weight:800;color:var(--text);margin-bottom:20px;display:flex;align-items:center;gap:10px}
.section-title::after{content:'';flex:1;height:1px;background:var(--border)}
.sub-title{font-size:15px;font-weight:700;color:var(--text);margin:24px 0 12px;display:flex;align-items:center;gap:8px}
.sub-title span{width:3px;height:16px;background:var(--blue);border-radius:2px;display:inline-block}

/* ── URL BADGE ── */
.url-badge{display:inline-flex;align-items:center;gap:8px;background:var(--blue-bg);border:1px solid var(--blue-light);color:var(--blue);border-radius:var(--r-sm);padding:8px 14px;font-size:13px;font-weight:500;margin-bottom:20px;word-break:break-all}
.url-badge::before{content:'🔗';flex-shrink:0}

/* ── FLOW ── */
.flow{display:flex;flex-direction:column;gap:0;padding-left:4px}
.flow-item{display:flex;gap:0;position:relative}
.flow-connector{display:flex;flex-direction:column;align-items:center;width:44px;flex-shrink:0}
.flow-dot{width:40px;height:40px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:18px;flex-shrink:0;z-index:1;box-shadow:0 2px 8px rgba(0,0,0,.1)}
.flow-line{width:2px;flex:1;min-height:16px;background:linear-gradient(to bottom,var(--border),transparent)}
.flow-body{padding:6px 0 24px 16px;flex:1}
.flow-title{font-size:14px;font-weight:700;color:var(--text);margin-bottom:3px}
.flow-desc{font-size:13px;color:var(--text2);line-height:1.6}
.flow-tag{display:inline-block;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600;margin-top:6px}
/* colours */
.fc-blue .flow-dot{background:var(--blue-light)}
.fc-green .flow-dot{background:var(--green-light)}
.fc-amber .flow-dot{background:var(--amber-light)}
.fc-red .flow-dot{background:var(--red-light)}
.fc-purple .flow-dot{background:#ede9fe}
.ft-blue{background:var(--blue-bg);color:var(--blue)}
.ft-green{background:var(--green-bg);color:var(--green)}
.ft-amber{background:var(--amber-bg);color:var(--amber)}
.ft-red{background:var(--red-bg);color:var(--red)}

/* ── STEPS ── */
.steps{display:flex;flex-direction:column;gap:10px;margin:12px 0}
.step-item{background:var(--card);border:1px solid var(--border);border-radius:var(--r);padding:14px 16px;display:flex;gap:14px;align-items:flex-start;transition:box-shadow .15s}
.step-item:hover{box-shadow:0 2px 12px rgba(0,0,0,.06)}
.step-num{width:30px;height:30px;border-radius:50%;background:var(--blue);color:#fff;display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:800;flex-shrink:0}
.step-text{}
.step-title{font-size:14px;font-weight:700;color:var(--text);margin-bottom:3px}
.step-detail{font-size:13px;color:var(--text2);line-height:1.6}
.step-example{margin-top:6px;background:var(--blue-bg);border-radius:6px;padding:6px 10px;font-size:12px;color:var(--blue);font-style:italic}

/* ── NOTE ── */
.note{border-radius:var(--r-sm);padding:14px 16px;margin:14px 0;font-size:13px;line-height:1.7;display:flex;gap:10px;align-items:flex-start}
.note-icon{font-size:16px;flex-shrink:0;margin-top:1px}
.note.info{background:var(--blue-bg);border-left:3px solid var(--blue2);color:#1e40af}
.note.success{background:var(--green-bg);border-left:3px solid #22c55e;color:#166534}
.note.warn{background:var(--amber-bg);border-left:3px solid #f59e0b;color:var(--amber)}
.note.danger{background:var(--red-bg);border-left:3px solid #ef4444;color:var(--red)}

/* ── TABLE ── */
.tbl-wrap{border-radius:var(--r);overflow:hidden;border:1px solid var(--border);margin:12px 0}
.tbl{width:100%;border-collapse:collapse}
.tbl th{background:#1e3a8a;color:#fff;padding:11px 16px;text-align:left;font-size:13px;font-weight:600}
.tbl td{padding:11px 16px;font-size:13px;color:var(--text);border-bottom:1px solid var(--border);vertical-align:top;line-height:1.6}
.tbl tr:last-child td{border-bottom:none}
.tbl tr:nth-child(even) td{background:#f8fafc}
.tbl td:first-child{font-weight:600;white-space:nowrap;color:#1e40af}

/* ── BUTTON DEMO ── */
.btn-demo{display:flex;flex-wrap:wrap;gap:8px;margin:14px 0;padding:16px;background:var(--bg);border-radius:var(--r);border:1px solid var(--border)}
.btn-demo-label{width:100%;font-size:11px;color:var(--text3);font-weight:600;letter-spacing:.06em;text-transform:uppercase;margin-bottom:4px}
.b{display:inline-flex;align-items:center;gap:5px;padding:9px 18px;border-radius:8px;font-size:13px;font-weight:700;border:1.5px solid;cursor:default}
.b-primary{background:#1d4ed8;color:#fff;border-color:#1d4ed8}
.b-secondary{background:#fff;color:#334155;border-color:#cbd5e1}
.b-green{background:#dcfce7;color:#15803d;border-color:#86efac}
.b-amber{background:#fef3c7;color:#92400e;border-color:#fcd34d}
.b-red{background:#fee2e2;color:#b91c1c;border-color:#fca5a5}

/* ── SCENARIO BOX ── */
.scenario{background:var(--card);border:1px solid var(--border);border-radius:var(--r);overflow:hidden;margin:16px 0}
.scenario-head{background:linear-gradient(90deg,#1e3a8a,#2563eb);color:#fff;padding:12px 16px;font-size:13px;font-weight:700;display:flex;align-items:center;gap:8px}
.scenario-body{padding:16px}
.scenario-row{display:flex;gap:10px;margin-bottom:10px;font-size:13px;color:var(--text2);align-items:flex-start}
.scenario-row:last-child{margin-bottom:0}
.scenario-row b{color:var(--text);white-space:nowrap;min-width:60px}
.msg-bubble{background:#f1f5f9;border-radius:12px 12px 12px 4px;padding:8px 12px;font-size:12px;color:var(--text);line-height:1.6;border:1px solid var(--border);margin-top:6px}
.msg-bubble.line{background:#06c755;color:#fff;border-color:#06c755;border-radius:12px 12px 4px 12px}

/* ── CAPACITY ── */
.cap-visual{background:var(--card);border:1px solid var(--border);border-radius:var(--r);padding:20px;margin:14px 0}
.cap-title{font-size:13px;font-weight:700;color:var(--text2);margin-bottom:16px;text-transform:uppercase;letter-spacing:.06em}
.cap-row{display:flex;align-items:center;gap:12px;margin-bottom:12px}
.cap-row:last-child{margin-bottom:0}
.cap-emoji{font-size:16px;width:24px;text-align:center}
.cap-label{font-size:13px;color:var(--text2);width:100px;flex-shrink:0}
.cap-track{flex:1;height:10px;background:var(--border);border-radius:5px;overflow:hidden}
.cap-fill{height:100%;border-radius:5px;transition:width .4s}
.cap-green{background:linear-gradient(90deg,#22c55e,#4ade80)}
.cap-amber{background:linear-gradient(90deg,#f59e0b,#fbbf24)}
.cap-red{background:linear-gradient(90deg,#ef4444,#f87171)}
.cap-count{font-size:12px;font-weight:700;min-width:50px;text-align:right;color:var(--text2)}

/* ── URL TABLE ── */
.url-list{display:grid;gap:10px;margin:14px 0}
.url-card{background:var(--card);border:1px solid var(--border);border-radius:var(--r);padding:14px 16px;display:flex;flex-direction:column;gap:4px;transition:border-color .15s}
.url-card:hover{border-color:var(--blue2)}
.url-card-role{font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:.06em;color:var(--text3)}
.url-card-name{font-size:15px;font-weight:700;color:var(--text)}
.url-card-url{font-size:12px;color:var(--blue);word-break:break-all;font-family:monospace}

/* ── FAQ ── */
.faq{display:flex;flex-direction:column;gap:8px;margin:14px 0}
.faq-item{border:1px solid var(--border);border-radius:var(--r);background:var(--card);overflow:hidden}
.faq-q{padding:14px 16px;font-size:14px;font-weight:600;color:var(--text);cursor:pointer;display:flex;justify-content:space-between;align-items:center;gap:12px;user-select:none;transition:background .15s}
.faq-q:hover{background:var(--bg)}
.faq-chevron{color:var(--text3);font-size:16px;transition:transform .25s;flex-shrink:0}
.faq-item.open .faq-chevron{transform:rotate(180deg)}
.faq-a{display:none;padding:0 16px 14px;font-size:13px;color:var(--text2);line-height:1.75}
.faq-item.open .faq-a{display:block}

/* ── SOP ── */
.sop-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin:14px 0}
.sop-card{background:var(--card);border:1px solid var(--border);border-radius:var(--r);padding:18px;display:flex;flex-direction:column;gap:0}
.sop-phase{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:var(--text3);margin-bottom:10px}
.sop-item{display:flex;gap:10px;align-items:flex-start;padding:8px 0;border-bottom:1px solid var(--border);font-size:13px;color:var(--text2)}
.sop-item:last-child{border-bottom:none}
.sop-n{width:22px;height:22px;border-radius:50%;background:var(--blue-light);color:var(--blue);font-size:11px;font-weight:800;display:flex;align-items:center;justify-content:center;flex-shrink:0;margin-top:1px}

/* ── TWO-COL ── */
.two-col{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin:14px 0}
.cabin-card{border-radius:var(--r);padding:18px;border:1px solid}
.cabin-sun{background:#fffbeb;border-color:#fde68a}
.cabin-moon{background:#eff6ff;border-color:#bfdbfe}
.cabin-emoji{font-size:32px;margin-bottom:10px}
.cabin-name{font-size:17px;font-weight:800;margin-bottom:4px}
.cabin-url{font-size:11px;font-family:monospace;word-break:break-all}

/* ── NOTIFY TABLE ── */
.notify-list{display:flex;flex-direction:column;gap:10px;margin:14px 0}
.notify-item{background:var(--card);border:1px solid var(--border);border-radius:var(--r);padding:14px 16px;display:grid;grid-template-columns:auto 1fr;gap:10px 14px;align-items:start}
.notify-when{font-size:12px;font-weight:700;color:var(--text2);background:var(--bg);border-radius:99px;padding:3px 10px;white-space:nowrap;align-self:start;margin-top:1px}
.notify-msg{font-size:13px;color:var(--text);line-height:1.65}

/* ── DIVIDER ── */
.divider{border:none;border-top:2px solid var(--border);margin:48px 0;position:relative}
.divider::after{content:attr(data-label);position:absolute;left:50%;top:50%;transform:translate(-50%,-50%);background:var(--bg);padding:4px 16px;font-size:12px;color:var(--text3);font-weight:600;letter-spacing:.06em}

/* ── RESPONSIVE ── */
@media(max-width:640px){
  .cover{padding:36px 24px 32px}
  .cover h1{font-size:26px}
  .cover-cards{grid-template-columns:1fr}
  .chapter-hero{padding:28px 22px}
  .chapter-hero h2{font-size:22px}
  .sop-grid{grid-template-columns:1fr}
  .two-col{grid-template-columns:1fr}
}
</style>
</head>
<body>

<!-- NAV -->
<nav class="nav">
  <div class="nav-inner">
    <div class="nav-logo">🔮 使用手冊</div>
    <div class="nav-scroll">
      <a class="nav-btn" href="#overview">總覽</a>
      <a class="nav-btn" href="#wb">🫙 心願瓶</a>
      <a class="nav-btn" href="#checkout">結帳登記</a>
      <a class="nav-btn" href="#leadbottle">領瓶叫號</a>
      <a class="nav-btn" href="#tarot">🔮 塔羅牌</a>
      <a class="nav-btn" href="#liff">客人取號</a>
      <a class="nav-btn" href="#cabin">包廂叫號</a>
      <a class="nav-btn" href="#faq">FAQ</a>
      <a class="nav-btn" href="#sop">SOP</a>
    </div>
  </div>
</nav>

<div class="wrap">

<!-- COVER -->
<div class="cover" id="overview">
  <div class="cover-inner">
    <div class="cover-tag">✦ 工作人員使用手冊</div>
    <h1>點晶礦活動<br>排隊叫號系統</h1>
    <p class="cover-sub">即時候位 × LINE 通知 × 雙服務管理<br>請依照您的崗位，閱讀對應章節</p>
    <div class="cover-cards">
      <div class="cover-card">
        <div class="cover-card-icon">🫙</div>
        <div>
          <div class="cover-card-title">第一章｜心願瓶 DIY</div>
          <div class="cover-card-sub">結帳櫃檯 — 登記候位<br>領瓶處 — 叫號與製作追蹤</div>
        </div>
      </div>
      <div class="cover-card">
        <div class="cover-card-icon">🔮</div>
        <div>
          <div class="cover-card-title">第二章｜塔羅牌占卜</div>
          <div class="cover-card-sub">客人 LINE 自行取號<br>☀️ 太陽 & 🌙 月亮包廂叫號</div>
        </div>
      </div>
    </div>
  </div>
</div>

<!-- 系統總覽 -->
<div class="section">
  <div class="section-title">⚙️ 系統頁面速查</div>
  <div class="url-list">
    <div class="url-card">
      <div class="url-card-role">🫙 心願瓶</div>
      <div class="url-card-name">結帳櫃檯</div>
      <div class="url-card-url">https://mercury-gcac.onrender.com/staff/checkout</div>
    </div>
    <div class="url-card">
      <div class="url-card-role">🫙 心願瓶</div>
      <div class="url-card-name">領瓶處</div>
      <div class="url-card-url">https://mercury-gcac.onrender.com/staff/wishbottle</div>
    </div>
    <div class="url-card">
      <div class="url-card-role">🔮 塔羅牌</div>
      <div class="url-card-name">☀️ 太陽包廂</div>
      <div class="url-card-url">https://mercury-gcac.onrender.com/staff/tarot-sun</div>
    </div>
    <div class="url-card">
      <div class="url-card-role">🔮 塔羅牌</div>
      <div class="url-card-name">🌙 月亮包廂</div>
      <div class="url-card-url">https://mercury-gcac.onrender.com/staff/tarot-moon</div>
    </div>
    <div class="url-card">
      <div class="url-card-role">⚙️ 管理員</div>
      <div class="url-card-name">管理後台（重置 / 統計 / 設定）</div>
      <div class="url-card-url">https://mercury-gcac.onrender.com/staff</div>
    </div>
    <div class="url-card">
      <div class="url-card-role">📱 客人</div>
      <div class="url-card-name">LIFF 取號頁面（需在 LINE 內開啟）</div>
      <div class="url-card-url">https://liff.line.me/2006903949-Sbmw12xl</div>
    </div>
  </div>
  <div class="note info"><span class="note-icon">💡</span>建議每個崗位將對應網址加入書籤。活動前請提早 <b>5 分鐘</b>開啟頁面喚醒伺服器，首次載入約需 30–50 秒，頁面顯示「系統啟動中」為正常現象。</div>
</div>

<hr class="divider" data-label="第一章">

<!-- ═══════════════ 心願瓶 ═══════════════ -->
<div id="wb">
  <div class="chapter-hero wb">
    <div class="chapter-hero-inner">
      <div class="chapter-num">第一章</div>
      <h2>🫙 心願瓶 DIY 服務</h2>
      <p>結帳櫃檯負責登記候位與名單管理，領瓶處負責叫號、確認領瓶與製作流程追蹤。</p>
      <div class="chapter-hero-badges">
        <span class="chapter-badge">結帳櫃檯</span>
        <span class="chapter-badge">領瓶處</span>
        <span class="chapter-badge">號碼前綴 W</span>
        <span class="chapter-badge">容量上限 6 人</span>
      </div>
    </div>
  </div>
</div>

<!-- 心願瓶整體流程 -->
<div class="section">
  <div class="section-title">心願瓶整體服務流程</div>
  <div class="flow">
    <div class="flow-item fc-blue">
      <div class="flow-connector"><div class="flow-dot">💳</div><div class="flow-line"></div></div>
      <div class="flow-body">
        <div class="flow-title">客人結帳完成</div>
        <div class="flow-desc">客人完成購物結帳，前往結帳櫃檯辦理心願瓶 DIY 候位登記。</div>
      </div>
    </div>
    <div class="flow-item fc-blue">
      <div class="flow-connector"><div class="flow-dot">📝</div><div class="flow-line"></div></div>
      <div class="flow-body">
        <div class="flow-title">結帳櫃檯工作人員登記候位</div>
        <div class="flow-desc">輸入姓氏、選擇稱謂（先生 / 小姐）、人數（1–6 人）、手機號碼。<br>系統自動取號並發送 LINE 確認通知。</div>
        <span class="flow-tag ft-blue">結帳櫃檯操作</span>
      </div>
    </div>
    <div class="flow-item fc-green">
      <div class="flow-connector"><div class="flow-dot">📱</div><div class="flow-line"></div></div>
      <div class="flow-body">
        <div class="flow-title">客人收到 LINE 候位確認通知</div>
        <div class="flow-desc">客人在 LINE 看到「已成功登記候位，號碼 W003（2 人），輪到您時我們會再通知您」。<br>客人可在附近自由活動，等待叫號通知即可。</div>
      </div>
    </div>
    <div class="flow-item fc-blue">
      <div class="flow-connector"><div class="flow-dot">📢</div><div class="flow-line"></div></div>
      <div class="flow-body">
        <div class="flow-title">領瓶處工作人員叫號</div>
        <div class="flow-desc">工作人員評估製作區容量，按「叫下一號 →」叫出候位第一組。<br>客人立即收到 LINE 叫號通知。</div>
        <span class="flow-tag ft-blue">領瓶處操作</span>
      </div>
    </div>
    <div class="flow-item fc-green">
      <div class="flow-connector"><div class="flow-dot">✅</div><div class="flow-line"></div></div>
      <div class="flow-body">
        <div class="flow-title">客人到場，工作人員確認領瓶</div>
        <div class="flow-desc">客人前往領瓶處，工作人員確認後按「已確認領瓶 ✅」。<br>該組移入「正在進行心願瓶製作中」列表，叫號區清空。</div>
        <span class="flow-tag ft-green">製作開始</span>
      </div>
    </div>
    <div class="flow-item fc-amber">
      <div class="flow-connector"><div class="flow-dot">🫙</div><div class="flow-line"></div></div>
      <div class="flow-body">
        <div class="flow-title">進行心願瓶製作</div>
        <div class="flow-desc">工作人員陪同製作。若後方還有候位客人，可視情況按「提醒」通知下一組慢慢回場。</div>
      </div>
    </div>
    <div class="flow-item fc-green">
      <div class="flow-connector"><div class="flow-dot">🆗</div><div class="flow-line"></div></div>
      <div class="flow-body">
        <div class="flow-title">製作完成，按「製作完成 🆗」</div>
        <div class="flow-desc">工作人員協助蓋瓶、貼封口後，在製作中列表對應的組別按下「製作完成 🆗」，完成本組服務，可繼續叫下一號。</div>
        <span class="flow-tag ft-green">服務完成 ＋1</span>
      </div>
    </div>
  </div>
</div>

<!-- ── 結帳櫃檯 ── -->
<div id="checkout" class="section">
  <div class="section-title">📋 結帳櫃檯操作</div>
  <div class="url-badge">mercury-gcac.onrender.com/staff/checkout</div>

  <div class="sub-title"><span></span>登記候位步驟</div>
  <div class="steps">
    <div class="step-item">
      <div class="step-num">1</div>
      <div class="step-text">
        <div class="step-title">輸入客人姓氏</div>
        <div class="step-detail">在「請輸入姓氏」欄位輸入客人姓氏，系統會自動組合成完整稱謂。</div>
        <div class="step-example">範例：輸入「陳」→ 稱謂將顯示為「陳先生」</div>
      </div>
    </div>
    <div class="step-item">
      <div class="step-num">2</div>
      <div class="step-text">
        <div class="step-title">選擇先生 / 小姐</div>
        <div class="step-detail">點選右側「先生」或「小姐」按鈕，選中的按鈕會變為藍色。預設為「先生」。</div>
      </div>
    </div>
    <div class="step-item">
      <div class="step-num">3</div>
      <div class="step-text">
        <div class="step-title">選擇本組人數</div>
        <div class="step-detail">點選 1–6 人按鈕，選中的按鈕會變為藍色。人數影響製作區容量計算與等待時間預估。</div>
        <div class="step-example">一家三口選「3 人」，系統計算等待時間時會納入人數</div>
      </div>
    </div>
    <div class="step-item">
      <div class="step-num">4</div>
      <div class="step-text">
        <div class="step-title">輸入手機號碼</div>
        <div class="step-detail">格式為 09xxxxxxxx（10 碼），用於發送 LINE 候位通知與叫號通知。</div>
        <div class="step-example">若客人無 LINE 或不方便提供，可填寫 0900000000 仍可正常登記，但不會收到通知</div>
      </div>
    </div>
    <div class="step-item">
      <div class="step-num">5</div>
      <div class="step-text">
        <div class="step-title">按下「登記候位」按鈕</div>
        <div class="step-detail">確認資料無誤後按此按鈕（請勿按 Enter，需明確點擊按鈕）。<br>系統自動取號，客人收到 LINE 確認通知，表單自動清空。</div>
      </div>
    </div>
  </div>

  <div class="note success">
    <span class="note-icon">✅</span>
    <div><b>登記成功後，客人收到的 LINE 通知：</b><div class="msg-bubble line">🫙 心願瓶DIY｜✅ 陳先生 您好！已成功登記候位，您的號碼是 W003（2 人），輪到您時我們會再通知您，感謝耐心等候 🙏</div></div>
  </div>

  <div class="sub-title"><span></span>目前候位名單</div>
  <p style="font-size:13px;color:var(--text2);margin-bottom:12px">登記欄位正下方即時顯示候位名單，頁面每 2 秒自動更新。</p>
  <div class="tbl-wrap"><table class="tbl">
    <tr><th>欄位</th><th>說明</th></tr>
    <tr><td>號碼</td><td>W001、W002、W003…（W 為心願瓶前綴）</td></tr>
    <tr><td>姓名</td><td>陳先生、林小姐（含稱謂）</td></tr>
    <tr><td>人數與順序</td><td>3 人｜下一組 / 第 2 組，約 15 分鐘 / 即將輪到</td></tr>
    <tr><td>取消按鈕</td><td>直接取消該組候位（僅前 10 組顯示）</td></tr>
  </table></div>

  <div class="sub-title"><span></span>使用情境：客人臨時想取消</div>
  <div class="scenario">
    <div class="scenario-head">📌 情境：客人登記後想取消候位</div>
    <div class="scenario-body">
      <div class="scenario-row"><b>候位名單前 10 組</b>直接在名單旁按「取消」按鈕，跳出確認視窗後即取消。</div>
      <div class="scenario-row"><b>超過第 10 組：</b>使用頁面下方的「手機號碼查詢取消」功能：</div>
    </div>
  </div>
  <div class="steps">
    <div class="step-item">
      <div class="step-num">1</div>
      <div class="step-text">
        <div class="step-title">輸入手機號碼</div>
        <div class="step-detail">在下方取消欄位輸入 09xxxxxxxx，系統邊輸入邊即時搜尋候位名單。</div>
      </div>
    </div>
    <div class="step-item">
      <div class="step-num">2</div>
      <div class="step-text">
        <div class="step-title">確認查詢結果</div>
        <div class="step-detail">找到時：紅色框顯示「找到：陳先生 W003 號 2 人」並出現「確認取消候位」按鈕。<br>找不到：顯示「查無此號碼的候位記錄」。</div>
      </div>
    </div>
    <div class="step-item">
      <div class="step-num">3</div>
      <div class="step-text">
        <div class="step-title">按「確認取消候位」</div>
        <div class="step-detail">系統立即取消並從名單移除，欄位自動清空。</div>
      </div>
    </div>
  </div>
</div>

<!-- ── 領瓶處 ── -->
<div id="leadbottle" class="section">
  <div class="section-title">🫙 領瓶處操作</div>
  <div class="url-badge">mercury-gcac.onrender.com/staff/wishbottle</div>

  <div class="sub-title"><span></span>頁面按鈕一覽</div>
  <div class="btn-demo">
    <div class="btn-demo-label">主要按鈕</div>
    <div class="b b-primary">叫下一號 →</div>
    <div class="b b-secondary">重複叫號</div>
    <div class="b b-green">已確認領瓶 ✅</div>
    <div class="b b-amber">未到場</div>
    <div class="b b-green">製作完成 🆗</div>
    <div class="b b-amber">提醒</div>
  </div>
  <div class="tbl-wrap"><table class="tbl">
    <tr><th>按鈕</th><th>觸發時機</th><th>動作說明</th></tr>
    <tr><td>叫下一號 →</td><td>有人候位時</td><td>叫出候位第一組，發送 LINE 叫號通知</td></tr>
    <tr><td>重複叫號</td><td>叫號後客人未到</td><td>再次發送叫號通知給剛叫到的號</td></tr>
    <tr><td>已確認領瓶 ✅</td><td>客人到場（叫號後出現）</td><td>該組移入製作中，叫號區清空</td></tr>
    <tr><td>未到場</td><td>叫號後客人長時間未現身</td><td>該組重排至候位第 2 位，客人收到通知</td></tr>
    <tr><td>製作完成 🆗</td><td>製作完畢（製作中列表）</td><td>該組從製作中移除，服務 +1</td></tr>
    <tr><td>提醒（候位名單）</td><td>製作中或叫號前</td><td>通知候位客人「快輪到囉，可慢慢回場」</td></tr>
  </table></div>

  <div class="sub-title"><span></span>叫號完整流程</div>
  <div class="flow">
    <div class="flow-item fc-blue">
      <div class="flow-connector"><div class="flow-dot">📢</div><div class="flow-line"></div></div>
      <div class="flow-body">
        <div class="flow-title">按「叫下一號 →」</div>
        <div class="flow-desc">確認製作區有足夠空位後按下。叫號後頁面顯示號碼與客人姓名人數。</div>
      </div>
    </div>
    <div class="flow-item fc-red">
      <div class="flow-connector"><div class="flow-dot">⚠️</div><div class="flow-line"></div></div>
      <div class="flow-body">
        <div class="flow-title">若客人遲遲未到 → 按「未到場」</div>
        <div class="flow-desc">客人重排至候位第 2 位（非末位），並收到 LINE 通知。叫號區清空，可再叫下一號。</div>
        <span class="flow-tag ft-red">重排第 2 位</span>
      </div>
    </div>
    <div class="flow-item fc-green">
      <div class="flow-connector"><div class="flow-dot">✅</div><div class="flow-line"></div></div>
      <div class="flow-body">
        <div class="flow-title">客人到場後按「已確認領瓶 ✅」</div>
        <div class="flow-desc">該組從叫號區移入「正在進行心願瓶製作中」欄位。叫號區清空，可立即叫下一號。</div>
        <span class="flow-tag ft-green">進入製作中</span>
      </div>
    </div>
    <div class="flow-item fc-amber">
      <div class="flow-connector"><div class="flow-dot">🔔</div><div class="flow-line"></div></div>
      <div class="flow-body">
        <div class="flow-title">視情況在候位名單按「提醒」</div>
        <div class="flow-desc">當製作區接近滿載，可提前通知候位客人「快輪到囉，可以慢慢回到現場等候」。</div>
      </div>
    </div>
    <div class="flow-item fc-green">
      <div class="flow-connector"><div class="flow-dot">🆗</div><div class="flow-line"></div></div>
      <div class="flow-body">
        <div class="flow-title">製作完成後按「製作完成 🆗」</div>
        <div class="flow-desc">蓋瓶、貼封口完成後，在製作中列表對應組別按此。該組移除，今日已服務 +1。</div>
        <span class="flow-tag ft-green">服務完成</span>
      </div>
    </div>
  </div>

  <div class="sub-title"><span></span>製作區容量說明</div>
  <div class="cap-visual">
    <div class="cap-title">製作區容量進度條（上限 6 人）</div>
    <div class="cap-row">
      <div class="cap-emoji">🟢</div>
      <div class="cap-label">1–3 人（充裕）</div>
      <div class="cap-track"><div class="cap-fill cap-green" style="width:40%"></div></div>
      <div class="cap-count">2 / 6</div>
    </div>
    <div class="cap-row">
      <div class="cap-emoji">🟠</div>
      <div class="cap-label">4–5 人（接近滿）</div>
      <div class="cap-track"><div class="cap-fill cap-amber" style="width:75%"></div></div>
      <div class="cap-count">5 / 6</div>
    </div>
    <div class="cap-row">
      <div class="cap-emoji">🔴</div>
      <div class="cap-label">6 人（滿載）</div>
      <div class="cap-track"><div class="cap-fill cap-red" style="width:100%"></div></div>
      <div class="cap-count">6 / 6</div>
    </div>
  </div>
  <div class="note info"><span class="note-icon">💡</span>容量進度條僅供參考，系統不強制限制叫號。請依現場實際狀況彈性判斷何時叫號。</div>

  <div class="sub-title"><span></span>使用情境：客人叫到後未出現</div>
  <div class="scenario">
    <div class="scenario-head">📌 情境：叫到 W003 號，等了 3 分鐘客人沒來</div>
    <div class="scenario-body">
      <div class="scenario-row"><b>①</b>可先按「重複叫號」再次發送 LINE 通知</div>
      <div class="scenario-row"><b>②</b>若仍沒有出現，按「未到場」按鈕</div>
      <div class="scenario-row"><b>結果：</b>W003 自動重排至候位第 2 位，客人收到通知：<div class="msg-bubble line">🫙 心願瓶DIY｜王先生 您好！叫號時暫時未見到您，已為您保留候位並重新安排至第 2 位，若需取消請至結帳櫃檯洽詢 🙏</div></div>
    </div>
  </div>

  <div class="sub-title"><span></span>LINE 通知時機對照表</div>
  <div class="notify-list">
    <div class="notify-item">
      <div class="notify-when">登記成功</div>
      <div class="notify-msg">✅ 王先生 您好！已成功登記候位，號碼 W003（2 人），輪到您時再通知，感謝耐心等候 🙏</div>
    </div>
    <div class="notify-item">
      <div class="notify-when">正式叫號</div>
      <div class="notify-msg">📢 王先生 您好！現在叫到 W003 號，請至領瓶處，謝謝！</div>
    </div>
    <div class="notify-item">
      <div class="notify-when">重複叫號</div>
      <div class="notify-msg">📢 再次提醒 王先生 您好！請 W003 號前往領瓶處，謝謝！</div>
    </div>
    <div class="notify-item">
      <div class="notify-when">未到場</div>
      <div class="notify-msg">王先生 您好！叫號時暫時未見到您，已為您保留候位並重新安排至第 2 位，請留意後續叫號通知 🙏</div>
    </div>
    <div class="notify-item">
      <div class="notify-when">手動提醒</div>
      <div class="notify-msg">⏰ 王先生 您好！W003 號快輪到囉，可以慢慢回到現場等候，叫到您的號碼時我們會再通知您 🙏</div>
    </div>
  </div>
</div>

<hr class="divider" data-label="第二章">

<!-- ═══════════════ 塔羅牌 ═══════════════ -->
<div id="tarot">
  <div class="chapter-hero tarot">
    <div class="chapter-hero-inner">
      <div class="chapter-num">第二章</div>
      <h2>🔮 塔羅牌占卜服務</h2>
      <p>客人透過 LINE 自行掃碼取號，☀️ 太陽與 🌙 月亮兩個包廂共用同一候位序列，各自獨立叫號。</p>
      <div class="chapter-hero-badges">
        <span class="chapter-badge">客人自助取號</span>
        <span class="chapter-badge">雙包廂共用序列</span>
        <span class="chapter-badge">號碼前綴 T</span>
        <span class="chapter-badge">自動提醒功能</span>
      </div>
    </div>
  </div>
</div>

<!-- 整體流程 -->
<div class="section">
  <div class="section-title">塔羅牌整體服務流程</div>
  <div class="flow">
    <div class="flow-item fc-purple">
      <div class="flow-connector"><div class="flow-dot">📱</div><div class="flow-line"></div></div>
      <div class="flow-body">
        <div class="flow-title">客人掃描現場 QR Code</div>
        <div class="flow-desc">在 LINE 內開啟 LIFF 頁面（非 Line 不可）。頁面顯示兩個服務選項：心願瓶 DIY 與塔羅牌占卜。</div>
      </div>
    </div>
    <div class="flow-item fc-purple">
      <div class="flow-connector"><div class="flow-dot">🔢</div><div class="flow-line"></div></div>
      <div class="flow-body">
        <div class="flow-title">客人自行取號</div>
        <div class="flow-desc">點選「塔羅牌占卜」→ 輸入姓氏、選擇稱謂 → 取號。系統分配 T 開頭的號碼。</div>
        <span class="flow-tag ft-blue">無需工作人員協助</span>
      </div>
    </div>
    <div class="flow-item fc-green">
      <div class="flow-connector"><div class="flow-dot">📱</div><div class="flow-line"></div></div>
      <div class="flow-body">
        <div class="flow-title">客人收到 LINE 確認通知</div>
        <div class="flow-desc">確認號碼並等待，可在 LIFF 頁面即時查看候位狀況、兩包廂服務號、預估等待時間。</div>
      </div>
    </div>
    <div class="flow-item fc-blue">
      <div class="flow-connector"><div class="flow-dot">📢</div><div class="flow-line"></div></div>
      <div class="flow-body">
        <div class="flow-title">包廂工作人員按「叫下一號 →」</div>
        <div class="flow-desc">系統從共用候位序列叫出下一位，通知中告知客人前往哪個包廂（☀️ 或 🌙）。</div>
        <span class="flow-tag ft-blue">包廂操作</span>
      </div>
    </div>
    <div class="flow-item fc-amber">
      <div class="flow-connector"><div class="flow-dot">⏰</div><div class="flow-line"></div></div>
      <div class="flow-body">
        <div class="flow-title">系統自動倒數提醒下一位</div>
        <div class="flow-desc">叫號後 N 分鐘（預設 10 分鐘），系統自動通知候位下一位「快輪到了，請回到現場附近準備」。</div>
        <span class="flow-tag ft-amber">自動提醒</span>
      </div>
    </div>
    <div class="flow-item fc-red">
      <div class="flow-connector"><div class="flow-dot">🔄</div><div class="flow-line"></div></div>
      <div class="flow-body">
        <div class="flow-title">若客人未到場 → 按「未到場」</div>
        <div class="flow-desc">客人重排至候位末位（塔羅牌排末位，與心願瓶的第 2 位不同），並收到 LINE 通知。</div>
        <span class="flow-tag ft-red">重排末位</span>
      </div>
    </div>
  </div>
</div>

<!-- 客人取號 -->
<div id="liff" class="section">
  <div class="section-title">📱 客人端 LIFF 取號操作</div>
  <div class="url-badge">liff.line.me/2006903949-Sbmw12xl（需在 LINE 內開啟）</div>

  <div class="sub-title"><span></span>取號步驟</div>
  <div class="steps">
    <div class="step-item">
      <div class="step-num">1</div>
      <div class="step-text">
        <div class="step-title">在 LINE 內掃描 QR Code 或點連結</div>
        <div class="step-detail">必須在 LINE App 內開啟，一般瀏覽器無法使用（需要 LINE 登入識別）。</div>
      </div>
    </div>
    <div class="step-item">
      <div class="step-num">2</div>
      <div class="step-text">
        <div class="step-title">點選「塔羅牌占卜」服務</div>
        <div class="step-detail">頁面頂部有兩個服務標籤，點選「塔羅牌占卜」切換至塔羅候位頁面。</div>
      </div>
    </div>
    <div class="step-item">
      <div class="step-num">3</div>
      <div class="step-text">
        <div class="step-title">輸入姓氏並選擇稱謂</div>
        <div class="step-detail">輸入姓氏（例：陳），選擇先生或小姐。系統組合為完整稱謂（陳先生）。</div>
      </div>
    </div>
    <div class="step-item">
      <div class="step-num">4</div>
      <div class="step-text">
        <div class="step-title">按下「取號」</div>
        <div class="step-detail">系統分配號碼（例：T003），頁面切換為票券畫面並收到 LINE 通知。</div>
      </div>
    </div>
    <div class="step-item">
      <div class="step-num">5</div>
      <div class="step-text">
        <div class="step-title">等待叫號通知</div>
        <div class="step-detail">票券頁面每 2 秒自動更新，顯示等候人數、預估等待、兩包廂各自服務號。被叫到後票券會顯示「請前往 ☀️ 太陽包廂」或「請前往 🌙 月亮包廂」。</div>
      </div>
    </div>
  </div>
  <div class="note success">
    <span class="note-icon">✅</span>
    <div><b>取號成功，客人收到：</b><div class="msg-bubble line">🔮 塔羅牌占卜｜陳先生 您好！您已成功取得 T003 號，輪到您前會再通知您，感謝耐心等候 🙏</div></div>
  </div>

  <div class="sub-title"><span></span>客人可在 LINE 傳送查詢指令</div>
  <div class="tbl-wrap"><table class="tbl">
    <tr><th>傳送指令</th><th>系統回覆內容</th></tr>
    <tr><td>查詢塔羅目前叫號</td><td>☀️ 太陽包廂：T003 / 🌙 月亮包廂：T005 / 等候人數：4 人 / 預估等待：約 30 分鐘</td></tr>
    <tr><td>查詢心願瓶目前叫號</td><td>目前服務號、等候組數（共幾人）、預估等待時間</td></tr>
  </table></div>

  <div class="note warn"><span class="note-icon">⚠️</span>客人若想取消塔羅牌候位，可在 LIFF 票券頁面點「取消候位」自行取消。心願瓶取消則需至結帳櫃檯處理。</div>
</div>

<!-- 包廂叫號 -->
<div id="cabin" class="section">
  <div class="section-title">🔮 包廂工作人員操作</div>
  <div class="two-col">
    <div class="cabin-card cabin-sun">
      <div class="cabin-emoji">☀️</div>
      <div class="cabin-name" style="color:#92400e">太陽包廂</div>
      <div class="cabin-url" style="color:#b45309">mercury-gcac.onrender.com/staff/tarot-sun</div>
    </div>
    <div class="cabin-card cabin-moon">
      <div class="cabin-emoji">🌙</div>
      <div class="cabin-name" style="color:#1e40af">月亮包廂</div>
      <div class="cabin-url" style="color:#1d4ed8">mercury-gcac.onrender.com/staff/tarot-moon</div>
    </div>
  </div>
  <div class="note info"><span class="note-icon">💡</span>兩個包廂頁面設計完全相同，各自獨立追蹤本包廂叫出的號碼。共用同一候位序列，但各自叫號不互相干擾。</div>

  <div class="sub-title"><span></span>頁面區塊說明</div>
  <div class="tbl-wrap"><table class="tbl">
    <tr><th>區塊</th><th>說明</th></tr>
    <tr><td>此包廂目前服務</td><td>本包廂叫出的號碼（大字顯示）與「請 T003 號入座」提示</td></tr>
    <tr><td>對方包廂目前服務</td><td>另一個包廂目前服務號（即時同步，每 2 秒更新）</td></tr>
    <tr><td>今日已服務</td><td>本包廂今日完成叫號的人次</td></tr>
    <tr><td>自動提醒倒數提示</td><td>叫號後顯示綠色「N 分鐘後自動提醒 XXX 準備回場」倒數</td></tr>
    <tr><td>未到場提示</td><td>叫號後若客人未確認，顯示橘色未到場提示與按鈕</td></tr>
    <tr><td>候位即時動態</td><td>等候人數、預估等待時間、候位名單（純顯示，無取消按鈕）</td></tr>
  </table></div>

  <div class="sub-title"><span></span>叫號流程</div>
  <div class="flow">
    <div class="flow-item fc-blue">
      <div class="flow-connector"><div class="flow-dot">📢</div><div class="flow-line"></div></div>
      <div class="flow-body">
        <div class="flow-title">按「叫下一號 →」</div>
        <div class="flow-desc">從共用候位序列取出第一位，客人收到通知並告知前往本包廂。<br>頁面顯示該號碼，自動提醒倒數開始計時。</div>
      </div>
    </div>
    <div class="flow-item fc-amber">
      <div class="flow-connector"><div class="flow-dot">⏰</div><div class="flow-line"></div></div>
      <div class="flow-body">
        <div class="flow-title">自動提醒倒數進行中</div>
        <div class="flow-desc">頁面顯示綠色提示「將於 10 分鐘後自動提醒 陳先生（T003）準備回場」。<br>倒數結束後系統確認客人仍在候位，才自動發送 LINE 提醒。</div>
        <span class="flow-tag ft-amber">自動執行</span>
      </div>
    </div>
    <div class="flow-item fc-red">
      <div class="flow-connector"><div class="flow-dot">🔄</div><div class="flow-line"></div></div>
      <div class="flow-body">
        <div class="flow-title">若客人未到場 → 按「未到場」</div>
        <div class="flow-desc">客人重排至候位末位，系統發送 LINE 通知，自動提醒計時器重新開始計時給新的下一位。</div>
        <span class="flow-tag ft-red">塔羅牌排末位</span>
      </div>
    </div>
  </div>

  <div class="sub-title"><span></span>使用情境：兩個包廂協調叫號</div>
  <div class="scenario">
    <div class="scenario-head">📌 情境：目前候位有 T001、T002、T003 共三組</div>
    <div class="scenario-body">
      <div class="scenario-row"><b>太陽包廂</b>按「叫下一號」→ 叫出 T001，通知「請前往 ☀️ 太陽包廂」</div>
      <div class="scenario-row"><b>同時</b>月亮包廂也可按「叫下一號」→ 叫出 T002，通知「請前往 🌙 月亮包廂」</div>
      <div class="scenario-row"><b>注意：</b>兩包廂同時按叫號時，系統有 1 秒鎖防止衝突，若另一包廂剛按過，會提示「另一個包廂正在叫號，請稍後再試」。</div>
      <div class="scenario-row"><b>結果：</b>T001 在太陽、T002 在月亮，T003 看到候位顯示「下一位」等待叫號。</div>
    </div>
  </div>

  <div class="sub-title"><span></span>自動提醒時間設定</div>
  <p style="font-size:13px;color:var(--text2);margin-bottom:10px">自動提醒時間預設 10 分鐘，可由管理員在後台調整：</p>
  <div class="steps">
    <div class="step-item">
      <div class="step-num">1</div>
      <div class="step-text">
        <div class="step-title">前往管理後台</div>
        <div class="step-detail">mercury-gcac.onrender.com/staff</div>
      </div>
    </div>
    <div class="step-item">
      <div class="step-num">2</div>
      <div class="step-text">
        <div class="step-title">點選「設定」頁籤</div>
        <div class="step-detail">找到「服務 B」區塊下的「自動提醒時間（分）」欄位</div>
      </div>
    </div>
    <div class="step-item">
      <div class="step-num">3</div>
      <div class="step-text">
        <div class="step-title">輸入分鐘數後按「儲存設定」</div>
        <div class="step-detail">包廂頁面下次同步（約 2 秒）後自動套用新設定。</div>
      </div>
    </div>
  </div>

  <div class="sub-title"><span></span>LINE 通知時機對照表</div>
  <div class="notify-list">
    <div class="notify-item">
      <div class="notify-when">取號成功</div>
      <div class="notify-msg">🔮 塔羅牌占卜｜陳先生 您好！您已成功取得 T003 號，輪到您前會再通知您，感謝耐心等候 🙏</div>
    </div>
    <div class="notify-item">
      <div class="notify-when">正式叫號</div>
      <div class="notify-msg">🔮 塔羅牌占卜｜📢 陳先生 您好！現在叫到 T003 號，請前往 ☀️ 太陽包廂 入座，謝謝！</div>
    </div>
    <div class="notify-item">
      <div class="notify-when">重複叫號</div>
      <div class="notify-msg">🔮 塔羅牌占卜｜📢 再次提醒 陳先生 您好！請 T003 號前往 ☀️ 太陽包廂 入座，謝謝！</div>
    </div>
    <div class="notify-item">
      <div class="notify-when">自動提醒</div>
      <div class="notify-msg">🔮 塔羅牌占卜｜⏰ 陳先生 您好！您的 T003 號快輪到了，請回到現場附近準備，感謝您 🙏</div>
    </div>
    <div class="notify-item">
      <div class="notify-when">未到場重排</div>
      <div class="notify-msg">🔮 塔羅牌占卜｜陳先生 您好！叫號時暫時未見到您，已為您保留候位並重新安排至末位，請留意後續叫號通知 🙏</div>
    </div>
  </div>
</div>

<hr class="divider" data-label="常見問題">

<!-- FAQ -->
<div id="faq" class="section">
  <div class="section-title">❓ 常見問題 FAQ</div>

  <div class="sub-title"><span></span>🫙 心願瓶相關</div>
  <div class="faq">
    <div class="faq-item">
      <div class="faq-q">客人沒有 LINE，無法接收通知怎麼辦？<span class="faq-chevron">▾</span></div>
      <div class="faq-a">仍可正常登記候位。手機號碼可填 0900000000 作為占位，系統不會寄送 LINE 通知，但候位記錄正常運作。請工作人員口頭告知客人等待，並在叫號時主動通知。</div>
    </div>
    <div class="faq-item">
      <div class="faq-q">客人說沒收到 LINE 通知怎麼辦？<span class="faq-chevron">▾</span></div>
      <div class="faq-a">可能原因：① 客人封鎖了官方帳號 ② 手機未聯網 ③ LINE 通知權限關閉。請工作人員在領瓶處按「重複叫號」再次發送，或請客人開啟 LINE 查看。</div>
    </div>
    <div class="faq-item">
      <div class="faq-q">「已確認領瓶」按鈕不見了？<span class="faq-chevron">▾</span></div>
      <div class="faq-a">此按鈕只在「叫號後、客人尚未確認領瓶前」顯示。若已按下確認或尚未叫號，按鈕不會出現。請先按「叫下一號 →」叫號後，再等客人到場按確認。</div>
    </div>
    <div class="faq-item">
      <div class="faq-q">今日已服務人數計算方式是？<span class="faq-chevron">▾</span></div>
      <div class="faq-a">叫號時 +1，按下「未到場」時 -1。製作完成不額外計算。因此「已服務」代表實際叫號並確認到場的組數，可作為活動人次參考。</div>
    </div>
    <div class="faq-item">
      <div class="faq-q">客人想取消但在候位名單 10 組以後？<span class="faq-chevron">▾</span></div>
      <div class="faq-a">使用結帳頁面下方的「輸入手機號碼取消」功能，輸入客人手機號碼後系統即時查詢，找到後點「確認取消候位」即可。</div>
    </div>
  </div>

  <div class="sub-title"><span></span>🔮 塔羅牌相關</div>
  <div class="faq">
    <div class="faq-item">
      <div class="faq-q">兩個包廂可以同時叫號嗎？<span class="faq-chevron">▾</span></div>
      <div class="faq-a">可以，但有 1 秒鎖防止完全同步衝突。若另一個包廂剛按過叫號，會顯示「另一個包廂正在叫號，請稍後再試」。等 1 秒再按即可。</div>
    </div>
    <div class="faq-item">
      <div class="faq-q">重複叫號沒有效果？<span class="faq-chevron">▾</span></div>
      <div class="faq-a">重複叫號只對「本包廂叫出的號碼」有效。若該號碼是另一個包廂叫出的，會顯示「此包廂尚未叫號」。請確認是本包廂叫出的號才按。</div>
    </div>
    <div class="faq-item">
      <div class="faq-q">如何調整自動提醒時間？<span class="faq-chevron">▾</span></div>
      <div class="faq-a">前往管理後台（/staff）→ 設定頁籤 → 服務 B → 自動提醒時間（分），輸入數字後按「儲存設定」，約 2 秒後所有包廂頁面自動套用。</div>
    </div>
    <div class="faq-item">
      <div class="faq-q">客人在 LIFF 取消候位後會怎樣？<span class="faq-chevron">▾</span></div>
      <div class="faq-a">系統立即從候位序列移除，後方號碼自動往前遞補，候位名單即時更新。工作人員不需要任何操作。</div>
    </div>
    <div class="faq-item">
      <div class="faq-q">頁面的對方包廂號碼沒有更新？<span class="faq-chevron">▾</span></div>
      <div class="faq-a">頁面每 2 秒自動同步。若長時間未更新，請強制重新整理：Mac 按 Command+Shift+R，Windows 按 Ctrl+Shift+R。</div>
    </div>
  </div>

  <div class="sub-title"><span></span>⚙️ 系統管理相關</div>
  <div class="faq">
    <div class="faq-item">
      <div class="faq-q">活動開始前需要做什麼準備？<span class="faq-chevron">▾</span></div>
      <div class="faq-a">① 提前 5 分鐘開啟所有崗位頁面喚醒伺服器 ② 前往管理後台按「重置全部」清除前次資料 ③ 確認設定頁籤中的服務名稱、號碼前綴、自動提醒時間無誤。</div>
    </div>
    <div class="faq-item">
      <div class="faq-q">如何重置系統號碼？<span class="faq-chevron">▾</span></div>
      <div class="faq-a">前往管理後台（/staff）→ 總覽 → 重置號碼。可選擇「重置心願瓶」「重置塔羅牌」或「重置全部」，重置後所有號碼歸零，今日統計也同步清空。</div>
    </div>
    <div class="faq-item">
      <div class="faq-q">頁面一直沒有更新怎麼辦？<span class="faq-chevron">▾</span></div>
      <div class="faq-a">① 強制重新整理（Command+Shift+R / Ctrl+Shift+R）② 清除瀏覽器快取後重新開啟 ③ 若有 Console 出現紅色錯誤，截圖回報給系統管理員。</div>
    </div>
  </div>
</div>

<hr class="divider" data-label="活動當天 SOP">

<!-- SOP -->
<div id="sop" class="section">
  <div class="section-title">📋 活動當天操作 SOP</div>
  <div class="sop-grid">
    <div class="sop-card">
      <div class="sop-phase">活動開始前</div>
      <div class="sop-item"><div class="sop-n">1</div><div>提前 5 分鐘開啟所有崗位頁面，等待伺服器喚醒（首次 30–50 秒）</div></div>
      <div class="sop-item"><div class="sop-n">2</div><div>確認各頁面左上角顯示 <b>● 綠色圓點</b>（代表連線正常）</div></div>
      <div class="sop-item"><div class="sop-n">3</div><div>管理後台按「重置全部」清除昨日資料</div></div>
      <div class="sop-item"><div class="sop-n">4</div><div>確認設定頁籤中號碼前綴（W / T）、自動提醒時間正確</div></div>
    </div>
    <div class="sop-card">
      <div class="sop-phase">活動進行中</div>
      <div class="sop-item"><div class="sop-n">1</div><div>結帳後立即登記候位，手機號碼務必確認正確</div></div>
      <div class="sop-item"><div class="sop-n">2</div><div>領瓶處依序叫號，確認領瓶後按確認按鈕進入製作中</div></div>
      <div class="sop-item"><div class="sop-n">3</div><div>塔羅包廂各自叫號，製作完成後可立即叫下一位</div></div>
      <div class="sop-item"><div class="sop-n">4</div><div>容量接近時，提前按「提醒」通知下一組客人回場</div></div>
    </div>
    <div class="sop-card">
      <div class="sop-phase">活動結束後</div>
      <div class="sop-item"><div class="sop-n">1</div><div>確認製作中欄位清空、候位名單為空</div></div>
      <div class="sop-item"><div class="sop-n">2</div><div>管理後台查看今日統計（心願瓶 / 太陽 / 月亮 / 總計）</div></div>
      <div class="sop-item"><div class="sop-n">3</div><div>視需求重置號碼，或保留至隔日活動再重置</div></div>
    </div>
  </div>
  <div class="note success"><span class="note-icon">🎉</span>感謝所有工作人員的辛勞！如有系統問題請聯繫系統管理員。</div>
</div>

</div><!-- /wrap -->

<script>
// FAQ toggle
document.querySelectorAll('.faq-q').forEach(q => {
  q.addEventListener('click', () => {
    const item = q.parentElement;
    item.classList.toggle('open');
  });
});

// Nav active on scroll
const ids = ['overview','wb','checkout','leadbottle','tarot','liff','cabin','faq','sop'];
const links = {};
ids.forEach(id => { links[id] = document.querySelector(\`.nav-btn[href="#\${id}"]\`); });

const observer = new IntersectionObserver(entries => {
  entries.forEach(e => {
    if (e.isIntersecting) {
      Object.values(links).forEach(l => l && l.classList.remove('active'));
      if (links[e.target.id]) links[e.target.id].classList.add('active');
    }
  });
}, { rootMargin: '-20% 0px -60% 0px' });

ids.forEach(id => {
  const el = document.getElementById(id);
  if (el) observer.observe(el);
});
</script>
</body>
</html>
`); });

app.get('/', (req, res) => res.send('排隊系統後端運作中'));

const PORT = process.env.PORT || 3000;
initDB().then(() => {
  app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
});
