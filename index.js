const express = require('express');
const cors = require('cors');
const axios = require('axios');

const app = express();
app.use(cors());
app.use(express.json());

const LINE_TOKEN = process.env.LINE_TOKEN;

const userMap = {};

app.post('/api/register', async (req, res) => {
  const { userId, name } = req.body;
  if (!userId) return res.status(400).json({ error: '缺少 userId' });
  userMap[userId] = { userId, name };
  res.json({ success: true });
});

app.post('/api/line-notify', async (req, res) => {
  const { userId, name, message } = req.body;
  if (!userId || !message) {
    return res.status(400).json({ error: '缺少必要參數' });
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

app.post('/webhook', (req, res) => res.sendStatus(200));

app.get('/', (req, res) => res.send('排隊系統後端運作中'));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
