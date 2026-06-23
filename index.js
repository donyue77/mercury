const express = require('express');
const cors = require('cors');
const axios = require('axios');

const app = express();
app.use(cors());
app.use(express.json());

const LINE_TOKEN = process.env.LINE_TOKEN;

// 兩種對照表
const phoneToUserId = {};  // 手機號碼 → LINE userId（心願瓶用）
const userIdMap = {};      // LINE userId → 資料（塔羅牌用）

// 客人傳手機號碼給官方帳號時自動綁定
app.post('/webhook', async (req, res) => {
  res.sendStatus(200);
  const events = req.body.events || [];
  for (const event of events) {
    if (event.type === 'message' && event.message.type === 'text') {
      const userId = event.source.userId;
      const text = event.message.text.trim();
      if (/^09\d{8}$/.test(text)) {
        phoneToUserId[text] = userId;
        await axios.post('https://api.line.me/v2/bot/message/reply', {
          replyToken: event.replyToken,
          messages: [{ type: 'text', text: `✅ 手機號碼 ${text} 綁定成功！結帳後工作人員會幫您登記候位，輪到您時我們會主動通知您。` }]
        }, { headers: { Authorization: `Bearer ${LINE_TOKEN}` } });
      }
    }
  }
});

// LIFF 登入後註冊（塔羅牌用）
app.post('/api/register', async (req, res) => {
  const { userId, name } = req.body;
  if (!userId) return res.status(400).json({ error: '缺少 userId' });
  userIdMap[userId] = { userId, name };
  res.json({ success: true });
});

// 傳送通知（支援 userId 或 phone 兩種方式）
app.post('/api/line-notify', async (req, res) => {
  const { userId, phone, name, message } = req.body;
  if (!message) return res.status(400).json({ error: '缺少 message' });

  // 優先用 userId，其次用 phone 查找
  const targetId = userId || phoneToUserId[phone];
  if (!targetId) {
    return res.status(404).json({ error: '找不到對應的 LINE 帳號，請確認客人已綁定手機號碼' });
  }

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

app.get('/queue', (req, res) => {
  res.redirect('https://liff.line.me/2006903949-Sbmw12xl');
});

app.get('/', (req, res) => res.send('排隊系統後端運作中'));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
