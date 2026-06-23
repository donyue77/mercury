const express = require('express');
const cors = require('cors');
const axios = require('axios');

const app = express();
app.use(cors());
app.use(express.json());

const LINE_TOKEN = process.env.LINE_TOKEN;

// 用手機號碼查找 LINE userId（需要客人先傳訊息給官方帳號）
async function getUserIdByPhone(phone) {
  // 這裡用一個簡單的記憶體對照表
  // 實際上客人傳訊息時會自動登記
  return userMap[phone] || null;
}

// 記憶體中暫存 phone -> userId 的對應
const userMap = {};

// 客人傳訊息給官方帳號時，自動記錄 userId
app.post('/webhook', async (req, res) => {
  res.sendStatus(200);
  const events = req.body.events || [];
  for (const event of events) {
    if (event.type === 'message' && event.message.type === 'text') {
      const userId = event.source.userId;
      const text = event.message.text.trim();
      // 客人傳手機號碼給官方帳號，系統記錄對應關係
      if (/^09\d{8}$/.test(text)) {
        userMap[text] = userId;
        // 回覆確認訊息
        await axios.post('https://api.line.me/v2/bot/message/reply', {
          replyToken: event.replyToken,
          messages: [{ type: 'text', text: `✅ 您的 LINE 已綁定成功！取號後我們會主動通知您。` }]
        }, {
          headers: { Authorization: `Bearer ${LINE_TOKEN}` }
        });
      }
    }
  }
});

// 傳送通知給客人
app.post('/api/line-notify', async (req, res) => {
  const { phone, name, message } = req.body;
  if (!phone || !message) {
    return res.status(400).json({ error: '缺少必要參數' });
  }

  const userId = userMap[phone];
  if (!userId) {
    return res.status(404).json({ error: '找不到此手機號碼對應的 LINE 帳號，請確認客人已綁定' });
  }

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

// 健康檢查
app.get('/', (req, res) => res.send('排隊系統後端運作中'));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
