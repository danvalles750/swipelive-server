const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3001;

// { socketId: { peerId, ws, busy } }
const pool = new Map();
let nextSocketId = 1;

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' });
  res.end('SwipeLive ok — online: ' + pool.size);
});

const wss = new WebSocketServer({ server });

wss.on('connection', ws => {
  const socketId = String(nextSocketId++);
  let myPeerId = null;

  function send(obj) {
    if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
  }

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {

      case 'join': {
        myPeerId = msg.peerId;
        pool.set(socketId, { peerId: myPeerId, ws, busy: false });
        console.log(`+ join [${socketId}] ${myPeerId} | pool=${pool.size}`);
        broadcastCount();
        send({ type: 'joined', socketId });
        break;
      }

      // Marca como ocupado quando conectou com alguém
      case 'busy': {
        const me = pool.get(socketId);
        if (me) me.busy = true;
        break;
      }

      // Marca como livre quando desconectou
      case 'free': {
        const me = pool.get(socketId);
        if (me) me.busy = false;
        break;
      }

      case 'next': {
        // Só candidatos que estão LIVRES (não ocupados)
        const candidates = [...pool.values()].filter(u =>
          u.peerId !== myPeerId && !u.busy
        );

        if (candidates.length === 0) {
          send({ type: 'no_one' });
          break;
        }

        const pick = candidates[Math.floor(Math.random() * candidates.length)];
        console.log(`  next [${socketId}] → ${pick.peerId}`);
        send({ type: 'peer', peerId: pick.peerId });
        break;
      }

      case 'ping': {
        send({ type: 'pong' });
        break;
      }
    }
  });

  ws.on('close', () => {
    pool.delete(socketId);
    console.log(`- leave [${socketId}] ${myPeerId} | pool=${pool.size}`);
    broadcastCount();
  });

  ws.on('error', () => {
    pool.delete(socketId);
    broadcastCount();
  });
});

function broadcastCount() {
  const msg = JSON.stringify({ type: 'count', count: pool.size });
  for (const { ws } of pool.values()) {
    if (ws.readyState === ws.OPEN) ws.send(msg);
  }
}

server.listen(PORT, () => console.log('SwipeLive rodando na porta ' + PORT));
