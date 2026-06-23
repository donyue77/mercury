const express = require('express');
const cors = require('cors');
const axios = require('axios');

const app = express();
app.use(cors());
app.use(express.json());

const LINE_TOKEN = process.env.LINE_TOKEN;
const userMap = {};

app.get('/queue', (req, res) => {
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.send(`<!DOCTYPE html>
<html lang="zh-TW">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title id="page-title">排隊叫號系統</title>
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
.app{max-width:520px;margin:0 auto;padding-bottom:48px}

/* topbar */
.topbar{background:var(--bg);border-bottom:0.5px solid var(--border);padding:14px 16px;display:flex;align-items:center;gap:10px;position:sticky;top:0;z-index:20}
.topbar-title{font-size:15px;font-weight:500;flex:1}
.live-dot{width:7px;height:7px;background:#639922;border-radius:50%;animation:pulse 1.5s infinite;flex-shrink:0}

/* tabs */
.tabs{display:flex;background:var(--bg);border-bottom:0.5px solid var(--border)}
.tab{flex:1;padding:12px 4px;border:none;background:transparent;font-size:12px;font-weight:500;color:var(--text3);cursor:pointer;border-bottom:2px solid transparent;transition:all .15s;font-family:inherit}
.tab.active{color:var(--text);border-bottom-color:var(--text)}

/* panels */
.panel{display:none;padding:14px}
.panel.active{display:block}

/* cards */
.card{background:var(--bg);border:0.5px solid var(--border);border-radius:var(--r);padding:16px;margin-bottom:12px}
.card-title{font-size:11px;font-weight:500;color:var(--text3);letter-spacing:.05em;text-transform:uppercase;margin-bottom:12px}

/* service selector tabs */
.svc-tabs{display:flex;gap:8px;margin-bottom:14px}
.svc-tab{flex:1;padding:10px;border-radius:var(--r-sm);border:0.5px solid var(--border2);background:transparent;font-size:13px;font-weight:500;color:var(--text2);cursor:pointer;font-family:inherit;transition:all .15s;text-align:center}
.svc-tab.active-A{background:var(--sA-bg);color:var(--sA-text);border-color:var(--sA-border)}
.svc-tab.active-B{background:var(--sB-bg);color:var(--sB-text);border-color:var(--sB-border)}

/* big number */
.big-wrap{text-align:center;padding:20px 0 14px}
.big-num{font-size:80px;font-weight:500;line-height:1;letter-spacing:-2px}
.big-num.color-A{color:var(--sA)}
.big-num.color-B{color:var(--sB)}
.big-sub{font-size:12px;color:var(--text3);margin-top:4px}

/* stat rows */
.stat-row{display:flex;justify-content:space-between;align-items:center;padding:9px 0;border-bottom:0.5px solid var(--border)}
.stat-row:last-child{border-bottom:none}
.stat-label{font-size:13px;color:var(--text2)}
.stat-val{font-size:14px;font-weight:500;color:var(--text)}

/* buttons */
.btn{display:inline-flex;align-items:center;justify-content:center;gap:6px;padding:10px 18px;border:0.5px solid var(--border2);border-radius:var(--r-sm);background:transparent;font-size:14px;font-weight:500;color:var(--text);cursor:pointer;font-family:inherit;transition:all .15s;width:100%;margin-bottom:8px}
.btn:hover{background:var(--bg2)}
.btn:active{transform:scale(.97)}
.btn-A{background:var(--sA-bg);color:var(--sA-text);border-color:var(--sA-border)}
.btn-B{background:var(--sB-bg);color:var(--sB-text);border-color:var(--sB-border)}
.btn-primary{background:var(--text);color:var(--bg);border-color:var(--text)}
.btn-primary:hover{opacity:.85;background:var(--text)}
.btn-danger{color:var(--red);border-color:var(--red-b)}
.btn-danger:hover{background:var(--red-bg)}
.btn-sm{padding:6px 12px;font-size:12px;width:auto;margin-bottom:0}
.btn-row{display:flex;gap:8px;flex-wrap:wrap}

/* inputs */
.field{display:flex;flex-direction:column;gap:5px;margin-bottom:12px}
.field label{font-size:12px;color:var(--text2);font-weight:500}
.field input{padding:10px 12px;border:0.5px solid var(--border2);border-radius:var(--r-sm);background:var(--bg);color:var(--text);font-size:15px;font-family:inherit;width:100%}
.field input:focus{outline:none;border-color:var(--sA)}

/* chips */
.chips{display:flex;flex-wrap:wrap;gap:6px}
.chip{padding:4px 11px;border-radius:99px;font-size:12px;border:0.5px solid var(--border);background:var(--bg2);color:var(--text2)}
.chip.cur-A{background:var(--sA-bg);color:var(--sA-text);border-color:var(--sA-border);font-weight:500}
.chip.cur-B{background:var(--sB-bg);color:var(--sB-text);border-color:var(--sB-border);font-weight:500}
.chip.mine{border-width:1.5px}

/* ticket */
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

/* staff list */
.staff-entry{display:flex;align-items:center;gap:10px;padding:11px 0;border-bottom:0.5px solid var(--border)}
.staff-entry:last-child{border-bottom:none}
.staff-num{font-size:15px;font-weight:500;min-width:52px}
.staff-num.color-A{color:var(--sA)}
.staff-num.color-B{color:var(--sB)}
.staff-info{flex:1;min-width:0}
.staff-name{font-size:13px;font-weight:500;color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.staff-meta{font-size:11px;color:var(--text3);margin-top:1px}
.staff-btns{display:flex;gap:5px;flex-shrink:0}

/* notify log */
.log-item{display:flex;gap:8px;padding:8px 0;border-bottom:0.5px solid var(--border);font-size:12px;align-items:flex-start}
.log-item:last-child{border-bottom:none}
.log-dot{width:6px;height:6px;border-radius:50%;margin-top:4px;flex-shrink:0}
.log-dot.A{background:var(--sA)}
.log-dot.B{background:var(--sB)}
.log-text{flex:1;color:var(--text2);line-height:1.5}
.log-time{color:var(--text3);flex-shrink:0}

/* settings */
.setting-row{display:flex;align-items:center;gap:10px;padding:10px 0;border-bottom:0.5px solid var(--border)}
.setting-row:last-child{border-bottom:none}
.setting-label{font-size:13px;color:var(--text2);flex:1}
.setting-input{padding:6px 10px;border:0.5px solid var(--border2);border-radius:var(--r-sm);background:var(--bg);color:var(--text);font-size:13px;font-family:inherit;width:80px;text-align:center}

/* wait time bar */
.wait-bar-wrap{margin-top:10px}
.wait-bar-label{display:flex;justify-content:space-between;font-size:11px;color:var(--text3);margin-bottom:4px}
.wait-bar-track{height:4px;background:var(--bg3);border-radius:99px;overflow:hidden}
.wait-bar-fill{height:100%;border-radius:99px;transition:width .5s}
.wait-bar-fill.A{background:var(--sA)}
.wait-bar-fill.B{background:var(--sB)}

.empty{font-size:13px;color:var(--text3);font-style:italic;padding:8px 0}
.divider{height:0.5px;background:var(--border);margin:12px 0}
.toast{position:fixed;bottom:24px;left:50%;transform:translateX(-50%) translateY(80px);background:var(--text);color:var(--bg);padding:10px 22px;border-radius:99px;font-size:13px;font-weight:500;transition:transform .25s;z-index:999;white-space:nowrap;pointer-events:none}
.toast.show{transform:translateX(-50%) translateY(0)}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
</style>
</head>
<body>
<div class="app">

  <div class="topbar">
    <div class="live-dot"></div>
    <div class="topbar-title" id="topbar-name">活動排隊系統</div>
    <div style="font-size:11px;color:var(--text3)" id="topbar-mode">客戶端</div>
  </div>

  <div class="tabs">
    <button class="tab active" onclick="goTab('take')" id="tab-take">取號</button>
    <button class="tab" onclick="goTab('status')" id="tab-status">等候狀況</button>
    <button class="tab" onclick="goTab('staff')" id="tab-staff">工作人員</button>
    <button class="tab" onclick="goTab('settings')" id="tab-settings">設定</button>
  </div>

  <!-- 取號 -->
  <div class="panel active" id="panel-take">
    <!-- 已有票 -->
    <div id="my-ticket-view" style="display:none">
      <div class="card">
        <div class="ticket" id="ticket-inner">
          <div class="ticket-num" id="tk-num">—</div>
          <div id="tk-svc-badge"></div>
          <div class="ticket-name" id="tk-name"></div>
          <div class="ticket-time" id="tk-time"></div>
          <div class="wait-badge normal" id="tk-wait"></div>
        </div>
      </div>
      <button class="btn btn-danger" onclick="leaveQueue()">放棄候位</button>
    </div>

    <!-- 取號表單（塔羅牌） -->
    <div id="form-tarot" style="display:none">
      <div class="card" style="text-align:center;padding:20px 16px">
        <div style="font-size:32px;margin-bottom:8px">🔮</div>
        <div style="font-size:16px;font-weight:500;margin-bottom:4px" id="svcB-hero-name">塔羅牌占卜</div>
        <div style="font-size:13px;color:var(--text2)">填入資料後取號，輪到您時將傳送 LINE 通知</div>
      </div>
      <div class="card">
        <div class="field">
          <label>姓名</label>
          <input type="text" id="inp-name" placeholder="請輸入您的姓名"/>
        </div>
        <div class="field" style="margin-bottom:0;display:none">
          <label>手機號碼</label>
          <input type="tel" id="inp-phone" placeholder="09xxxxxxxx"/>
        </div>
      </div>
      <button class="btn btn-B" onclick="takeNumber('B')" id="take-btn-B">取得號碼牌</button>
    </div>

    <!-- 服務選擇（兩個服務都是客人取號時用） -->
    <div id="form-choose" style="display:none">
      <div class="card" style="text-align:center;padding:16px">
        <div style="font-size:14px;color:var(--text2)">請選擇服務項目</div>
      </div>
      <div class="field">
        <label>姓名</label>
        <input type="text" id="inp-name2" placeholder="請輸入您的姓名"/>
      </div>
      <div class="field">
        <label>手機號碼</label>
        <input type="tel" id="inp-phone2" placeholder="09xxxxxxxx"/>
      </div>
      <button class="btn btn-A" onclick="takeNumberFromForm('A')" id="take-btn-A2"></button>
      <button class="btn btn-B" onclick="takeNumberFromForm('B')" id="take-btn-B2"></button>
    </div>

    <!-- 預設提示（由工作人員取號） -->
    <div id="form-staffonly">
      <div class="card" style="text-align:center;padding:28px 16px">
        <div style="font-size:32px;margin-bottom:12px">🎪</div>
        <div style="font-size:16px;font-weight:500;margin-bottom:6px" id="svcA-hero-name">心願瓶DIY</div>
        <div style="font-size:13px;color:var(--text2);line-height:1.7">請至服務台由工作人員協助登記候位</div>
      </div>
      <div class="card" style="padding:12px 16px">
        <div class="stat-row">
          <span class="stat-label">目前等候</span>
          <span class="stat-val" id="staffonly-waiting">0 人</span>
        </div>
        <div class="stat-row">
          <span class="stat-label">預估等待</span>
          <span class="stat-val" id="staffonly-est">— 分鐘</span>
        </div>
      </div>
    </div>
  </div>

  <!-- 等候狀況 -->
  <div class="panel" id="panel-status">
    <div class="svc-tabs">
      <button class="svc-tab" id="status-tab-A" onclick="setStatusSvc('A')"></button>
      <button class="svc-tab" id="status-tab-B" onclick="setStatusSvc('B')"></button>
    </div>

    <div class="card">
      <div class="big-wrap">
        <div class="big-num" id="status-cur">—</div>
        <div class="big-sub" id="status-cur-label">等待服務</div>
      </div>
      <div class="wait-bar-wrap">
        <div class="wait-bar-label">
          <span>等候人數</span>
          <span id="status-bar-label">0 人</span>
        </div>
        <div class="wait-bar-track">
          <div class="wait-bar-fill" id="status-bar" style="width:0%"></div>
        </div>
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

  <!-- 工作人員 -->
  <div class="panel" id="panel-staff">
    <div class="svc-tabs">
      <button class="svc-tab" id="staff-tab-A" onclick="setStaffSvc('A')"></button>
      <button class="svc-tab" id="staff-tab-B" onclick="setStaffSvc('B')"></button>
    </div>

    <!-- 叫號區 -->
    <div class="card">
      <div class="card-title">叫號操作</div>
      <div style="display:flex;align-items:center;gap:12px;margin-bottom:14px">
        <div style="text-align:center">
          <div style="font-size:11px;color:var(--text3);margin-bottom:2px">目前</div>
          <div style="font-size:36px;font-weight:500;line-height:1" id="staff-cur">—</div>
        </div>
        <div style="flex:1">
          <button class="btn btn-primary" style="margin-bottom:8px" onclick="callNext()">叫下一號 →</button>
          <button class="btn" style="margin-bottom:0" onclick="repeatCall()">重複叫號</button>
        </div>
      </div>
      <div class="divider" style="margin:0 0 12px"></div>
      <div style="font-size:12px;color:var(--text3);margin-bottom:8px">手動登記客人取號</div>
      <div style="display:flex;gap:8px;margin-bottom:8px">
        <input type="text" id="staff-inp-name" placeholder="姓名" style="flex:1;padding:8px 10px;border:0.5px solid var(--border2);border-radius:var(--r-sm);background:var(--bg);color:var(--text);font-size:13px;font-family:inherit"/>
        <input type="tel" id="staff-inp-phone" placeholder="09xxxxxxxx" style="flex:1;padding:8px 10px;border:0.5px solid var(--border2);border-radius:var(--r-sm);background:var(--bg);color:var(--text);font-size:13px;font-family:inherit"/>
      </div>
      <button class="btn btn-A" id="staff-take-btn" onclick="staffTakeNumber()" style="margin-bottom:0"></button>
    </div>

    <!-- 候位名單 -->
    <div class="card">
      <div class="card-title" style="margin-bottom:10px">候位名單</div>
      <div id="staff-list"><span class="empty">目前無人候位</span></div>
    </div>

    <!-- 通知記錄 -->
    <div class="card">
      <div class="card-title" style="margin-bottom:8px">通知記錄</div>
      <div id="notify-log"><span class="empty">尚無記錄</span></div>
    </div>

    <!-- 統計 -->
    <div class="card">
      <div class="card-title">今日統計</div>
      <div class="stat-row"><span class="stat-label">已服務</span><span class="stat-val" id="staff-served">0</span></div>
      <div class="stat-row"><span class="stat-label">等候人數</span><span class="stat-val" id="staff-waiting">0</span></div>
      <div class="stat-row" style="border:none"><span class="stat-label">最後發號</span><span class="stat-val" id="staff-last">—</span></div>
    </div>

    <button class="btn btn-danger" onclick="resetSvc()">重置此服務今日號碼</button>
    <button class="btn btn-danger" onclick="resetAll()" style="margin-top:4px;opacity:.7">重置所有服務</button>
  </div>

  <!-- 設定 -->
  <div class="panel" id="panel-settings">
    <div class="card">
      <div class="card-title">系統名稱</div>
      <div class="setting-row">
        <span class="setting-label">顯示名稱</span>
        <input class="setting-input" style="width:160px" id="set-system-name" value="活動排隊系統"/>
      </div>
    </div>

    <div class="card">
      <div class="card-title">服務 A 設定</div>
      <div class="setting-row">
        <span class="setting-label">服務名稱</span>
        <input class="setting-input" style="width:130px" id="set-nameA" value="心願瓶DIY"/>
      </div>
      <div class="setting-row">
        <span class="setting-label">號碼前綴</span>
        <input class="setting-input" id="set-prefixA" value="A" maxlength="3"/>
      </div>
      <div class="setting-row">
        <span class="setting-label">每號服務時間（分）</span>
        <input class="setting-input" type="number" id="set-timeA" value="15" min="1" max="120"/>
      </div>
      <div class="setting-row" style="border:none">
        <span class="setting-label">取號方式</span>
        <span style="font-size:12px;color:var(--text3)">工作人員操作</span>
      </div>
    </div>

    <div class="card">
      <div class="card-title">服務 B 設定</div>
      <div class="setting-row">
        <span class="setting-label">服務名稱</span>
        <input class="setting-input" style="width:130px" id="set-nameB" value="塔羅牌占卜"/>
      </div>
      <div class="setting-row">
        <span class="setting-label">號碼前綴</span>
        <input class="setting-input" id="set-prefixB" value="T" maxlength="3"/>
      </div>
      <div class="setting-row" style="border:none">
        <span class="setting-label">每號服務時間（分）</span>
        <input class="setting-input" type="number" id="set-timeB" value="20" min="1" max="120"/>
      </div>
    </div>

    <button class="btn btn-primary" onclick="saveSettings()">儲存設定</button>
    <div style="font-size:12px;color:var(--text3);margin-top:8px;text-align:center">設定儲存於本機，不影響已取號的客人</div>
  </div>
</div>

<div class="toast" id="toast"></div>

<script>
// ── 狀態 ──────────────────────────────────────────
let cfg = {
  systemName: '活動排隊系統',
  services: {
    A: { name: '心願瓶DIY', prefix: 'A', minutes: 15 },
    B: { name: '塔羅牌占卜', prefix: 'T', minutes: 20 }
  }
};

let state = {
  A: { current: 0, lastIssued: 0, queue: [], history: [], servedToday: 0 },
  B: { current: 0, lastIssued: 0, queue: [], history: [], servedToday: 0 }
};

let myTicket = null;
let notifyLog = [];
let lineUserId = null;
let currentStatusSvc = 'A';
let currentStaffSvc = 'A';

// ── 持久化 ────────────────────────────────────────
function loadAll() {
  try {
    const c = localStorage.getItem('qs2_cfg'); if (c) cfg = JSON.parse(c);
    const s = localStorage.getItem('qs2_state'); if (s) state = JSON.parse(s);
    const t = localStorage.getItem('qs2_ticket'); if (t) myTicket = JSON.parse(t);
    const l = localStorage.getItem('qs2_log'); if (l) notifyLog = JSON.parse(l);
  } catch(e) {}
}
function saveAll() {
  try {
    localStorage.setItem('qs2_cfg', JSON.stringify(cfg));
    localStorage.setItem('qs2_state', JSON.stringify(state));
    if (myTicket) localStorage.setItem('qs2_ticket', JSON.stringify(myTicket));
    else localStorage.removeItem('qs2_ticket');
    localStorage.setItem('qs2_log', JSON.stringify(notifyLog.slice(0,30)));
  } catch(e) {}
}

// ── 工具 ──────────────────────────────────────────
function fmt(svc, n) { return cfg.services[svc].prefix + String(n).padStart(3,'0'); }
function nowTime() { return new Date().toLocaleTimeString('zh-TW',{hour:'2-digit',minute:'2-digit'}); }
function showToast(msg) {
  const el = document.getElementById('toast');
  el.textContent = msg; el.classList.add('show');
  setTimeout(()=>el.classList.remove('show'), 2400);
}
const BACKEND_URL = 'https://mercury-gcac.onrender.com';

async function sendLineNotify(userId, name, message) {
  if (!userId || userId === '—') return;
  try {
    await fetch(BACKEND_URL + '/api/line-notify', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ userId, name, message })
    });
  } catch(e) { console.warn('LINE 通知失敗:', e.message); }
}

function addLog(svc, msg) {
  notifyLog.unshift({ svc, msg, time: nowTime() });
  saveAll(); renderLog();
}

// ── Tab 切換 ──────────────────────────────────────
function goTab(name) {
  ['take','status','staff','settings'].forEach(t => {
    document.getElementById('panel-'+t).classList.toggle('active', t===name);
    document.getElementById('tab-'+t).classList.toggle('active', t===name);
  });
  const modeEl = document.getElementById('topbar-mode');
  modeEl.textContent = name === 'staff' || name === 'settings' ? '工作人員端' : '客戶端';
  render();
}

function setStatusSvc(s) { currentStatusSvc = s; renderStatus(); }
function setStaffSvc(s) { currentStaffSvc = s; renderStaff(); }

// ── 取號邏輯 ──────────────────────────────────────
function takeNumber(svc) {
  const name = document.getElementById('inp-name').value.trim();
  if (!name) { showToast('請輸入姓名'); return; }
  if (!lineUserId) { showToast('請在 LINE 內開啟此頁面'); return; }
  _issueTicket(svc, name, lineUserId);
}
function takeNumberFromForm(svc) {
  const name = document.getElementById('inp-name2').value.trim();
  const phone = document.getElementById('inp-phone2').value.trim();
  if (!name) { showToast('請輸入姓名'); return; }
  if (!phone || !/^09\\d{8}$/.test(phone)) { showToast('請輸入有效手機號碼'); return; }
  _issueTicket(svc, name, phone);
}
function staffTakeNumber() {
  const name = document.getElementById('staff-inp-name').value.trim();
  const phone = document.getElementById('staff-inp-phone').value.trim();
  if (!name) { showToast('請輸入客人姓名'); return; }
  _issueTicket(currentStaffSvc, name, phone || '—');
  document.getElementById('staff-inp-name').value = '';
  document.getElementById('staff-inp-phone').value = '';
}
function _issueTicket(svc, name, phone) {
  state[svc].lastIssued++;
  const num = state[svc].lastIssued;
  const uid = lineUserId || phone;
  state[svc].queue.push({ num, name, userId: uid });
  myTicket = { svc, num, name, userId: uid, time: nowTime() };
  addLog(svc, \`\${name} 取得 \${fmt(svc, num)} 號\`);
  if (lineUserId) {
    sendLineNotify(lineUserId, name, \`您好 \${name}！您已成功取得 \${fmt(svc, num)} 號（\${cfg.services[svc].name}），輪到您前會再通知您，感謝耐心等候。\`);
  }
  saveAll(); render();
  showToast('取號成功：' + fmt(svc, num));
}

function leaveQueue() {
  if (!myTicket) return;
  if (!confirm('確定要放棄候位嗎？')) return;
  const { svc, num, name } = myTicket;
  state[svc].queue = state[svc].queue.filter(q => q.num !== num);
  addLog(svc, \`\${name} 放棄 \${fmt(svc, num)} 號\`);
  myTicket = null; saveAll(); render();
  showToast('已放棄候位');
}

// ── 叫號 ──────────────────────────────────────────
function callNext() {
  const svc = currentStaffSvc;
  const q = state[svc].queue;
  if (q.length === 0) { showToast('目前無人候位'); return; }
  const entry = q.shift();
  state[svc].current = entry.num;
  state[svc].servedToday++;
  state[svc].history.unshift(entry.num);
  if (state[svc].history.length > 10) state[svc].history.pop();
  addLog(svc, \`叫號 \${fmt(svc, entry.num)}（\${entry.name}）\`);
  sendLineNotify(entry.userId, entry.name, \`📢 \${entry.name} 您好！現在叫到 \${fmt(svc, entry.num)} 號，請立即回到現場，謝謝！\`);
  if (q.length > 0) {
    addLog(svc, \`提醒 \${q[0].name}（\${fmt(svc, q[0].num)}）準備\`);
    sendLineNotify(q[0].userId, q[0].name, \`⏰ \${q[0].name} 您好！您是下一位（\${fmt(svc, q[0].num)} 號），請提前回到現場準備。\`);
  }
  saveAll(); render();
  showToast('已叫號：' + fmt(svc, entry.num));
}
function repeatCall() {
  const svc = currentStaffSvc;
  if (!state[svc].current) { showToast('尚未開始叫號'); return; }
  addLog(svc, \`重複叫號 \${fmt(svc, state[svc].current)}\`);
  showToast('已重複叫號');
}
function notifyPerson(svc, num) {
  const entry = state[svc].queue.find(q => q.num === num);
  if (!entry) return;
  const pos = state[svc].queue.indexOf(entry);
  const est = Math.round((pos + 1) * cfg.services[svc].minutes);
  addLog(svc, \`提醒 \${entry.name}（\${fmt(svc, num)}），約 \${est} 分鐘後\`);
  sendLineNotify(entry.userId, entry.name, \`⏰ \${entry.name} 您好！您的 \${fmt(svc, num)} 號預計約 \${est} 分鐘後叫號，請提前回到現場準備。\`);
  showToast('已傳送提醒');
}
function cancelPerson(svc, num) {
  const entry = state[svc].queue.find(q => q.num === num);
  if (!entry || !confirm(\`確定取消 \${fmt(svc, num)} 號（\${entry.name}）？\`)) return;
  state[svc].queue = state[svc].queue.filter(q => q.num !== num);
  if (myTicket && myTicket.svc === svc && myTicket.num === num) myTicket = null;
  addLog(svc, \`取消 \${entry.name}（\${fmt(svc, num)}）候位\`);
  saveAll(); render();
  showToast('已取消候位');
}

// ── 重置 ──────────────────────────────────────────
function resetSvc() {
  const svc = currentStaffSvc;
  if (!confirm(\`確定重置「\${cfg.services[svc].name}」今日所有號碼？\`)) return;
  state[svc] = { current:0, lastIssued:0, queue:[], history:[], servedToday:0 };
  if (myTicket && myTicket.svc === svc) myTicket = null;
  saveAll(); render(); showToast('已重置');
}
function resetAll() {
  if (!confirm('確定重置所有服務？')) return;
  state = {
    A: { current:0, lastIssued:0, queue:[], history:[], servedToday:0 },
    B: { current:0, lastIssued:0, queue:[], history:[], servedToday:0 }
  };
  myTicket = null; notifyLog = [];
  saveAll(); render(); showToast('已重置所有服務');
}

// ── 設定 ──────────────────────────────────────────
function loadSettingsUI() {
  document.getElementById('set-system-name').value = cfg.systemName;
  document.getElementById('set-nameA').value = cfg.services.A.name;
  document.getElementById('set-prefixA').value = cfg.services.A.prefix;
  document.getElementById('set-timeA').value = cfg.services.A.minutes;
  document.getElementById('set-nameB').value = cfg.services.B.name;
  document.getElementById('set-prefixB').value = cfg.services.B.prefix;
  document.getElementById('set-timeB').value = cfg.services.B.minutes;
}
function saveSettings() {
  cfg.systemName = document.getElementById('set-system-name').value.trim() || '排隊系統';
  cfg.services.A.name = document.getElementById('set-nameA').value.trim() || '服務A';
  cfg.services.A.prefix = document.getElementById('set-prefixA').value.trim() || 'A';
  cfg.services.A.minutes = parseInt(document.getElementById('set-timeA').value) || 15;
  cfg.services.B.name = document.getElementById('set-nameB').value.trim() || '服務B';
  cfg.services.B.prefix = document.getElementById('set-prefixB').value.trim() || 'B';
  cfg.services.B.minutes = parseInt(document.getElementById('set-timeB').value) || 20;
  saveAll(); applyConfig(); render();
  showToast('設定已儲存');
}

// ── 套用設定到 UI ──────────────────────────────────
function applyConfig() {
  document.getElementById('page-title').textContent = cfg.systemName;
  document.getElementById('topbar-name').textContent = cfg.systemName;
  document.getElementById('svcA-hero-name').textContent = cfg.services.A.name;
  document.getElementById('svcB-hero-name').textContent = cfg.services.B.name;
  document.getElementById('status-tab-A').textContent = cfg.services.A.name;
  document.getElementById('status-tab-B').textContent = cfg.services.B.name;
  document.getElementById('staff-tab-A').textContent = cfg.services.A.name;
  document.getElementById('staff-tab-B').textContent = cfg.services.B.name;
  document.getElementById('staff-take-btn').textContent = \`登記 \${cfg.services[currentStaffSvc].name} 取號\`;
  document.getElementById('take-btn-B').textContent = \`取得 \${cfg.services.B.name} 號碼牌\`;
  const ab2 = document.getElementById('take-btn-A2');
  const bb2 = document.getElementById('take-btn-B2');
  if (ab2) ab2.textContent = cfg.services.A.name + ' 取號';
  if (bb2) bb2.textContent = cfg.services.B.name + ' 取號';
}

// ── 渲染 ──────────────────────────────────────────
function render() {
  applyConfig();
  renderTake();
  renderStatus();
  renderStaff();
}

function renderTake() {
  // 取號面板顯示邏輯
  const hasTicket = !!myTicket;
  document.getElementById('my-ticket-view').style.display = hasTicket ? 'block' : 'none';
  document.getElementById('form-tarot').style.display = (!hasTicket) ? 'block' : 'none';
  document.getElementById('form-staffonly').style.display = (!hasTicket) ? 'block' : 'none';
  document.getElementById('form-choose').style.display = 'none';

  // 更新服務A等候資訊（在取號頁的 staff-only 區塊）
  const qA = state.A.queue;
  document.getElementById('staffonly-waiting').textContent = qA.length + ' 人';
  document.getElementById('staffonly-est').textContent = qA.length > 0
    ? Math.round(qA.length * cfg.services.A.minutes) + ' 分鐘'
    : '無需等候';

  if (!hasTicket) return;

  // 顯示票券
  const { svc, num, name, time } = myTicket;
  const numStr = fmt(svc, num);
  const el = document.getElementById('tk-num');
  el.textContent = numStr;
  el.className = 'ticket-num color-' + svc;
  const badge = document.getElementById('tk-svc-badge');
  badge.innerHTML = \`<span class="ticket-svc svc-\${svc}">\${cfg.services[svc].name}</span>\`;
  document.getElementById('tk-name').textContent = name;
  document.getElementById('tk-time').textContent = '取號時間 ' + time;

  // 等待資訊
  const q = state[svc].queue;
  const pos = q.findIndex(x => x.num === num);
  const wEl = document.getElementById('tk-wait');
  if (state[svc].current === num) {
    wEl.className = 'wait-badge now'; wEl.textContent = '叫到您了！請前往';
  } else if (pos === 0) {
    wEl.className = 'wait-badge soon'; wEl.textContent = '下一位，請準備！';
  } else if (pos > 0) {
    const est = Math.round(pos * cfg.services[svc].minutes);
    wEl.className = 'wait-badge normal';
    wEl.textContent = \`前方 \${pos} 人，約 \${est} 分鐘\`;
  } else {
    wEl.className = 'wait-badge normal'; wEl.textContent = '號碼已完成服務';
  }
}

function renderStatus() {
  const svc = currentStatusSvc;
  // 更新 tab 樣式
  ['A','B'].forEach(s => {
    const el = document.getElementById('status-tab-' + s);
    el.className = 'svc-tab' + (s === svc ? ' active-' + s : '');
  });

  const q = state[svc].queue;
  const cur = state[svc].current;
  const mins = cfg.services[svc].minutes;
  const numStr = cur > 0 ? fmt(svc, cur) : '—';
  const el = document.getElementById('status-cur');
  el.textContent = numStr;
  el.className = 'big-num color-' + svc;
  document.getElementById('status-cur-label').textContent = cur > 0 ? \`請 \${numStr} 號前往\` : '等待服務';
  document.getElementById('status-waiting').textContent = q.length;
  document.getElementById('status-est').textContent = q.length > 0 ? Math.round(q.length * mins) : '—';

  // 等候條
  const maxQ = 20;
  const pct = Math.min(100, Math.round(q.length / maxQ * 100));
  document.getElementById('status-bar').style.width = pct + '%';
  document.getElementById('status-bar').className = 'wait-bar-fill ' + svc;
  document.getElementById('status-bar-label').textContent = q.length + ' 人';

  // chips
  const chips = document.getElementById('status-chips');
  if (q.length === 0) { chips.innerHTML = '<span class="empty">目前無人候位</span>'; return; }
  chips.innerHTML = q.map((entry, i) => {
    const isMine = myTicket && myTicket.svc === svc && myTicket.num === entry.num;
    let cls = 'chip';
    if (i === 0) cls += ' cur-' + svc;
    if (isMine) cls += ' mine';
    return \`<span class="\${cls}">\${fmt(svc, entry.num)}\${isMine ? ' (我)' : ''}</span>\`;
  }).join('');
}

function renderStaff() {
  const svc = currentStaffSvc;
  ['A','B'].forEach(s => {
    const el = document.getElementById('staff-tab-' + s);
    el.className = 'svc-tab' + (s === svc ? ' active-' + s : '');
  });
  document.getElementById('staff-take-btn').textContent = \`登記 \${cfg.services[svc].name} 取號\`;

  const cur = state[svc].current;
  document.getElementById('staff-cur').textContent = cur > 0 ? fmt(svc, cur) : '—';
  document.getElementById('staff-cur').className = 'color-' + svc;
  document.getElementById('staff-served').textContent = state[svc].servedToday;
  document.getElementById('staff-waiting').textContent = state[svc].queue.length;
  document.getElementById('staff-last').textContent = state[svc].lastIssued > 0 ? fmt(svc, state[svc].lastIssued) : '—';

  const list = document.getElementById('staff-list');
  const q = state[svc].queue;
  if (q.length === 0) { list.innerHTML = '<span class="empty">目前無人候位</span>'; return; }
  list.innerHTML = q.map((entry, i) => {
    const est = Math.round((i + 1) * cfg.services[svc].minutes);
    return \`<div class="staff-entry">
      <div class="staff-num color-\${svc}">\${fmt(svc, entry.num)}</div>
      <div class="staff-info">
        <div class="staff-name">\${entry.name}</div>
        <div class="staff-meta">\${entry.phone !== '—' ? entry.phone + '｜' : ''}\${i === 0 ? '下一位' : '約 ' + est + ' 分鐘'}</div>
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
  el.innerHTML = notifyLog.slice(0,12).map(l =>
    \`<div class="log-item">
      <div class="log-dot \${l.svc}"></div>
      <div class="log-text">[\${cfg.services[l.svc]?.name || l.svc}] \${l.msg}</div>
      <div class="log-time">\${l.time}</div>
    </div>\`
  ).join('');
}

// ── 初始化 ────────────────────────────────────────
const LIFF_ID = '2006903949-Sbmw12xl';

async function initLiff() {
  try {
    await liff.init({ liffId: LIFF_ID });
    if (liff.isLoggedIn()) {
      const profile = await liff.getProfile();
      lineUserId = profile.userId;
      // 註冊到後端
      await fetch(BACKEND_URL + '/api/register', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ userId: lineUserId, name: profile.displayName })
      });
      // 預填姓名
      const nameEl = document.getElementById('inp-name');
      if (nameEl && !nameEl.value) nameEl.value = profile.displayName;
    } else {
      // 在 LINE 外部開啟時自動登入
      if (!liff.isInClient()) liff.login();
    }
  } catch(e) {
    console.warn('LIFF 初始化失敗（可能在 LINE 外部）:', e.message);
  }
}

loadAll();
loadSettingsUI();
render();
initLiff();

// 如果已有票，顯示票
if (myTicket) {
  document.getElementById('form-tarot').style.display = 'none';
  document.getElementById('form-staffonly').style.display = 'none';
  document.getElementById('my-ticket-view').style.display = 'block';
}

setInterval(() => { loadAll(); render(); }, 4000);
</script>
</body>
</html>
`);
});

app.post('/api/register', async (req, res) => {
  const { userId, name } = req.body;
  if (!userId) return res.status(400).json({ error: '缺少 userId' });
  userMap[userId] = { userId, name };
  res.json({ success: true });
});

app.post('/api/line-notify', async (req, res) => {
  const { userId, name, message } = req.body;
  if (!userId || !message) return res.status(400).json({ error: '缺少必要參數' });
  try {
    await axios.post('https://api.line.me/v2/bot/message/push', {
      to: userId,
      messages: [{ type: 'text', text: message }]
    }, {
      headers: { Authorization: `Bearer ${LINE_TOKEN}` }
    });
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/webhook', (req, res) => res.sendStatus(200));
app.get('/', (req, res) => res.send('排隊系統後端運作中'));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
