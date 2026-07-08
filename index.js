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
// DB keepalive：每 5 分鐘 ping 一次，避免閒置斷線
setInterval(async () => {
  try { await pool.query('SELECT 1'); } catch(e) { console.warn('DB keepalive failed:', e.message); }
}, 5 * 60 * 1000);

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
  if (data.state.A.lastCalledEntry === undefined) data.state.A.lastCalledEntry = null;
  if (data.state.B.lastCalledEntry === undefined) data.state.B.lastCalledEntry = null;
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
  let res;
  try {
    res = await pool.query("SELECT value FROM queue_state WHERE key = 'main'");
  } catch(e) {
    throw new Error('資料庫連線失敗，請稍後再試：' + e.message);
  }
  if (!res.rows.length) throw new Error('資料庫無資料，請重新初始化');
  const data = res.rows[0].value;
  // 確保 cabins 欄位永遠存在
  if (!data.state.A.inProgress) data.state.A.inProgress = [];
  if (data.state.A.lastCalledEntry === undefined) data.state.A.lastCalledEntry = null;
  if (data.state.B.lastCalledEntry === undefined) data.state.B.lastCalledEntry = null;
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
    // 叫號後重置通知記錄
    data.state[svc].lastNotifiedNum = 0;
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
    // 製作完成後清除 lastCalledEntry，避免誤顯示未到場按鈕
    data.state.A.lastCalledEntry = null;
    data.state.A.current = 0;
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
    // 清除 lastCalledEntry 避免未到場按鈕殘留
    data.state[svc].lastCalledEntry = null;
    data.state[svc].current = 0;
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
    <img src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAACcQAAAOqCAYAAAC4/cRZAAAACXBIWXMAAC4jAAAuIwF4pT92AAAgAElEQVR4nOzdT1Ibadru4bs7avKN8A7MDvC3AjgrMGcFUBFnlBPTo5ylqZzlyHxnoMkZgFdgegUlr6DwCpragVlBnUGm2tjtP2AkvdKr64pwuI0FPCFXA1L+9Lx/++uvvwIAAAAAAAAAAADb7u+lBwAAAAAAAAAAAIBlEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAAAAAAAFUQxAEAAAAAAAAAAFAFQRwAAAAAAAAAAABVEMQBAAAAAAAAAABQBUEcAAAAAAAAAAAAVRDEAQAAAAAAAAAAUAVBHAAAAAAA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wHD9f//9739T1wAAAAAAAAAAAAB7+5/UBQAAAAAAAAAAAEAbBOIAAAAAAAAAAACYBIE4AAAAAAAAAAAAJkEgDgAAAAAAAAAAgEkQiAMAAAAAAAAAAGASBOIAAAAAAAAAAACYBIE4AAAAAAAAAAAAJkEgDgAAAAAAAAAAgEkQiAMAAAAAAAAAAGASBOIAAAAAAAAAAACYBIE4AAAAAAAAAAAAJkEgDgAAAAAAAAAAgEkQiAMAAAAAAAAAAGASBOIAAAAAAAAAAACYBIE4AAAAAAAAAAAAJkEgDgAAAAAAAAAAgEkQiAMAAAAAAAAAAGASBOIAAAAAAAAAAACYBIE4AAAAAAAAAAAAJkEgDgAAAAAAAAAAgEkQiAMAAAAAAAAAAGASBOIAAAAAAAAAAACYBIE4AAAAAAAAAAAAJkEgDgAAAAAAAAAAgEkQiAMAAAAAAAAAAGASBOIAAAAAAAAAAACYBIE4AAAAAAAAAAAAJkEgDgAAAAAAAAAAgEkQiAMAAAAAAAAAAGASBOIAAAAAAAAAAACYBIE4AAAAAAAAAAAAJkEgDgAAAAAAAAAAgEkQiAMAAAAAAAAAAGASBOIAAAAAAAAAAACYBIE4AAAAAAAAAAAAJkEgDgAAAAAAAAAAgEkQiAMAAAAAAAAAAGASBOIAAAAAAAAAAACYBIE4AAAAAAAAAAAAJkEgDgAAAAAAAAAAgEkQiAMAAAAAAAAAAGASBOIAAAAAAAAAAACYBIE4AAAAAAAAAAAAJkEgDgAAAAAAAAAAgEkQiAMAAAAAAAAAAGASBOIAAAAAAAAAAACYBIE4AAAAAAAAAAAAJkEgDgAAAAAAAAAAgEkQiAMAAAAAAAAAAGASBOIAAAAAAAAAAACYBIE4AAAAAAAAAAAAJkEgDgAAAAAAAAAAgEkQiAMAAAAAAAAAAGASBOIAAAAAAAAAAACYBIE4AAAAAAAAAAAAJkEgDgAAAAAAAAAAgEkQiAMAAAAAAAAAAGASBOIAAAAAAAAAAACYBIE4AAAAAAAAAAAAJkEgDgAAAAAAAAAAgEkQiAMAAAAAAAAAAGASBOIAAAAAAAAAAACYBIE4AAAAAAAAAAAAJkEgDgAAAAAAAAAAgEkQiAMAAAAAAAAAAGASBOIAAAAAAAAAAACYBIE4AAAAAAAAAAAAJkEgDgAAAAAAAAAAgEkQiAMAAAAAAAAAAGASBOIAAAAAAAAAAACYBIE4AAAAAAAAAAAAJkEgDgAAAAAAAAAAgEkQiAMAAAAAAAAAAGASBOIAAAAAAAAAAACYBIE4AAAAAAAAAAAAJkEgDgAAAAAAAAAAgEkQiAMAAAAAAAAAAGASBOIAAAAAAAAAAACYBIE4AAAAAAAAAAAAJkEgDgAAAAAAAAAAgEkQiAMAAAAAAAAAAGASBOIAAAAAAAAAAACYBIE4AAAAAAAAAAAAJkEgDgAAAAAAAAAAgEkQiAMAAAAAAAAAAGASBOIAAAAAAACA/79dO5ABAAAAGORvfY+vOAIAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgAUhDgAAAAAAAAAAgIUAuqHZOq1VaiIAAAAASUVORK5CYII=" alt="點晶礦" style="height:32px;object-fit:contain;max-width:160px"/>
    <div style="font-size:12px;color:var(--text3);font-weight:500;margin-left:4px;white-space:nowrap">服務排隊系統</div>
  </div>
  <div class="tabs">
    <button class="tab active" onclick="goTab('take')" id="tab-take">🔮 塔羅取號</button>
    <button class="tab" onclick="goTab('status')" id="tab-status">📊 候位狀況</button>
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
          <div style="font-size:16px;font-weight:700;margin-bottom:2px">神諭塔羅屋</div>
          <div style="font-size:14px;color:var(--text2);margin-bottom:0" id="svcB-name">塔羅占卜服務</div>
        </div>
        <!-- 注意事項 -->
        <div style="background:#fff8e1;border:1.5px solid #f59e0b;border-radius:10px;padding:14px 16px;margin-bottom:12px;display:flex;gap:10px;align-items:flex-start">
          <div style="font-size:20px;flex-shrink:0">⚠️</div>
          <div>
            <div style="font-size:13px;font-weight:700;color:#92400e;margin-bottom:4px">請注意｜僅限現場參與者取號</div>
            <div style="font-size:12px;color:#b45309;line-height:1.7">此號碼牌系統<b>僅供在活動現場的朋友使用</b>。若您目前不在現場，請勿取號，以免佔用現場候位名額，影響其他朋友的等候體驗，感謝您的配合 🙏</div>
          </div>
        </div>
        <div class="card">
          <div class="field" style="margin-bottom:0">
            <label>姓名</label>
            <input type="text" id="inp-name" placeholder="請輸入您的姓名"/>
          </div>
        </div>
        <button class="btn btn-B" onclick="takeNumber('B')" id="take-btn-B">取得號碼牌</button>


      </div>
    </div>
  </div>

  <!-- 等候狀況 -->
  <div class="panel" id="panel-status">

    <!-- 心願瓶 DIY 狀況 -->
    <div style="font-size:11px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--text3);margin:4px 0 10px;display:flex;align-items:center;gap:6px">
      <span style="display:inline-block;width:3px;height:14px;background:#3b82f6;border-radius:2px"></span>
      🫙 心願瓶 DIY 候位狀況
    </div>
    <div style="background:#eff6ff;border:1px solid #bfdbfe;border-radius:10px;padding:12px 14px;margin-bottom:8px">
      <div style="font-size:12px;color:#1e40af;margin-bottom:10px">📋 欲登記心願瓶DIY，請至快閃店櫃檯結帳後，會由工作人員協助登記候位</div>
      <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-bottom:10px">
        <div style="text-align:center">
          <div style="font-size:11px;color:var(--text3);margin-bottom:2px">目前叫號</div>
          <div style="font-size:22px;font-weight:700;color:#dc2626" id="wb-cur-num">—</div>
        </div>
        <div style="text-align:center">
          <div style="font-size:11px;color:var(--text3);margin-bottom:2px">等候組數</div>
          <div style="font-size:22px;font-weight:700;color:var(--text)" id="svcA-waiting">0</div>
        </div>
        <div style="text-align:center">
          <div style="font-size:11px;color:var(--text3);margin-bottom:2px">預估等待</div>
          <div style="font-size:22px;font-weight:700;color:var(--text)" id="svcA-est">—</div>
        </div>
      </div>
      <div id="wb-inprogress-wrap" style="display:none;border-top:1px solid #bfdbfe;padding-top:8px;margin-top:2px">
        <div style="font-size:11px;color:#1e40af;margin-bottom:6px">🫙 製作中</div>
        <div id="wb-inprogress-chips" style="display:flex;flex-wrap:wrap;gap:6px"></div>
      </div>
    </div>

    <div style="height:20px"></div>

    <!-- 塔羅牌占卜狀況 -->
    <div style="font-size:11px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--text3);margin:4px 0 10px;display:flex;align-items:center;gap:6px">
      <span style="display:inline-block;width:3px;height:14px;background:var(--sB);border-radius:2px"></span>
      🔮 塔羅牌占卜候位狀況
    </div>
    <!-- 雙包廂 -->
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:10px">
      <div class="card" style="padding:14px;text-align:center">
        <div style="font-size:18px;margin-bottom:4px">☀️</div>
        <div style="font-size:11px;color:var(--text3);margin-bottom:6px">太陽包廂</div>
        <div style="font-size:26px;font-weight:700;color:var(--sB)" id="sun-cur">—</div>
        <div style="font-size:11px;color:var(--text3);margin-top:2px">目前叫號</div>
      </div>
      <div class="card" style="padding:14px;text-align:center">
        <div style="font-size:18px;margin-bottom:4px">🌙</div>
        <div style="font-size:11px;color:var(--text3);margin-bottom:6px">月亮包廂</div>
        <div style="font-size:26px;font-weight:700;color:var(--sB)" id="moon-cur">—</div>
        <div style="font-size:11px;color:var(--text3);margin-top:2px">目前叫號</div>
      </div>
    </div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px">
      <div class="card" style="padding:14px;text-align:center">
        <div style="font-size:11px;color:var(--text3);margin-bottom:4px">等候人數</div>
        <div style="font-size:28px;font-weight:700" id="status-waiting">0</div>
      </div>
      <div class="card" style="padding:14px;text-align:center">
        <div style="font-size:11px;color:var(--text3);margin-bottom:4px">預估等待</div>
        <div style="font-size:28px;font-weight:700" id="status-est">—</div>
        <div style="font-size:11px;color:var(--text3)">分鐘</div>
      </div>
    </div>
    <div class="card">
      <div class="card-title" style="margin-bottom:10px">塔羅牌等候序列</div>
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
  // 等候狀況頁現在同時顯示兩個服務，無需切換
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
    await syncFromServer(); // 後端已直接發送取號通知，前端不重複發送
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
  // 更新服務名稱（null 保護避免頁面崩潰）
  const topbarEl = document.getElementById('topbar-title');
  if (topbarEl) topbarEl.textContent = cfg.systemName;
  const svcANameEl = document.getElementById('svcA-name');
  if (svcANameEl) svcANameEl.textContent = cfg.services.A.name;
  const svcBNameEl = document.getElementById('svcB-name');
  if (svcBNameEl) svcBNameEl.textContent = cfg.services.B.name;
  const takeBtnEl = document.getElementById('take-btn-B');
  if (takeBtnEl) takeBtnEl.textContent = \`取得 \${cfg.services.B.name} 號碼牌\`;

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
  // ── 心願瓶 DIY 狀況 ──
  const qA = state.A.queue;
  const curA = state.A.lastCalledEntry?.num || state.A.current;
  const minsA = cfg.services.A.minutes;
  const totalCapA = qA.reduce((s, e) => s + (e.partySize || 1), 0);
  const inProgCapA = (state.A.inProgress || []).reduce((s, e) => s + (e.partySize || 1), 0);
  const overCapA = Math.max(0, inProgCapA + totalCapA - 6);
  const estMinsA = qA.length > 0 ? Math.ceil(overCapA / 6) * minsA : 0;
  const wbCurEl = document.getElementById('wb-cur-num');
  if (wbCurEl) wbCurEl.textContent = curA > 0 ? fmt('A', curA) : '—';
  const wbWaitEl = document.getElementById('svcA-waiting');
  if (wbWaitEl) wbWaitEl.textContent = qA.length;
  const wbEstEl = document.getElementById('svcA-est');
  if (wbEstEl) wbEstEl.textContent = qA.length > 0 ? (estMinsA > 0 ? estMinsA + ' 分' : '即將輪到') : '—';
  // 製作中號碼清單
  const inProg = state.A.inProgress || [];
  const wbWrap = document.getElementById('wb-inprogress-wrap');
  const wbChips = document.getElementById('wb-inprogress-chips');
  if (wbWrap && wbChips) {
    if (inProg.length === 0) {
      wbWrap.style.display = 'none';
    } else {
      wbWrap.style.display = 'block';
      wbChips.innerHTML = inProg.map(e =>
        \`<span style="display:inline-block;background:#dbeafe;color:#1d4ed8;border:1px solid #93c5fd;border-radius:99px;padding:3px 10px;font-size:13px;font-weight:600">\${fmt('A', e.num)}</span>\`
      ).join('');
    }
  }

  // ── 塔羅牌占卜狀況 ──
  const qB = state.B.queue;
  const minsB = cfg.services.B.minutes;
  const estMinsB = qB.length > 0 ? Math.max(0, Math.ceil(qB.length / 2) - 1) * minsB : 0;
  const waitEl = document.getElementById('status-waiting');
  if (waitEl) waitEl.textContent = qB.length;
  const estEl = document.getElementById('status-est');
  if (estEl) estEl.textContent = qB.length > 0 ? (estMinsB > 0 ? estMinsB : '即將輪到') : '—';
  // 塔羅牌等候序列
  const chips = document.getElementById('status-chips');
  if (chips) {
    if (qB.length === 0) { chips.innerHTML = '<span class="empty">目前無人候位</span>'; }
    else {
      chips.innerHTML = qB.map((entry, i) => {
        const isMine = myTicket && myTicket.svc === 'B' && myTicket.num === entry.num;
        let cls = 'chip' + (i === 0 ? ' cur-B' : '') + (isMine ? ' mine' : '');
        return \`<span class="\${cls}">\${fmt('B', entry.num)}\${isMine ? ' (我)' : ''}</span>\`;
      }).join('');
    }
  }
  // 雙包廂顯示
  const sunEl = document.getElementById('sun-cur');
  const moonEl = document.getElementById('moon-cur');
  if (sunEl) sunEl.textContent = state.B.cabins?.sun?.current > 0 ? fmt('B', state.B.cabins.sun.current) : '—';
  if (moonEl) moonEl.textContent = state.B.cabins?.moon?.current > 0 ? fmt('B', state.B.cabins.moon.current) : '—';
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
let AUTO_NOTIFY_MS = 10 * 60 * 1000; // 預設10分鐘，同步後更新

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
function cancelAutoNotify() {
  if (autoTimer) { clearTimeout(autoTimer); autoTimer = null; autoTargetNum = null; }
  const bar = document.getElementById('auto-bar');
  if (bar) bar.style.display = 'none';
}

function scheduleAutoNotify(nextEntry) {
  cancelAutoNotify();
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
  cancelAutoNotify();
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
let AUTO_NOTIFY_MS = 10 * 60 * 1000; // 預設10分鐘，同步後更新

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
function cancelAutoNotify() {
  if (autoTimer) { clearTimeout(autoTimer); autoTimer = null; autoTargetNum = null; }
  const bar = document.getElementById('auto-bar');
  if (bar) bar.style.display = 'none';
}

function scheduleAutoNotify(nextEntry) {
  cancelAutoNotify();
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
  cancelAutoNotify();
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
</div>
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
</html>`); });

app.get('/', (req, res) => res.send('排隊系統後端運作中'));

const PORT = process.env.PORT || 3000;
initDB().then(() => {
  app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
});
