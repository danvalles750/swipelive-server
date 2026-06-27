const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3001;

const pool = new Map();
let nextSocketId = 1;

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' });
  res.end('SwipeLive server ok — users online: ' + pool.size);
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
        pool.set(socketId, { peerId: myPeerId, ws, connectedAt: Date.now() });
        broadcastCount();
        send({ type: 'joined', socketId });
        break;
      }
      case 'next': {
        const exclude = msg.exclude || [];
        exclude.push(myPeerId);
        const candidates = [...pool.values()].filter(u => !exclude.includes(u.peerId));
        if (candidates.length === 0) { send({ type: 'no_one' }); break; }
        const pick = candidates[Math.floor(Math.random() * candidates.length)];
        send({ type: 'peer', peerId: pick.peerId });
        break;
      }
      case 'ping': { send({ type: 'pong' }); break; }
    }
  });

  ws.on('close', () => { pool.delete(socketId); broadcastCount(); });
  ws.on('error', () => { pool.delete(socketId); broadcastCount(); });
});

function broadcastCount() {
  const msg = JSON.stringify({ type: 'count', count: pool.size });
  for (const { ws } of pool.values()) {
    if (ws.readyState === ws.OPEN) ws.send(msg);
  }
}

server.listen(PORT, () => console.log('SwipeLive rodando na porta ' + PORT));
