from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import asyncio
from core.node import Node

app = FastAPI()
node: Optional[Node] = None

class JoinModel(BaseModel):
    host: str
    rest_port: int
    tcp_port: int
    peers: List[str]

class MessageModel(BaseModel):
    to: str
    text: str

class SnapshotModel(BaseModel):
    snapshot_id: str

class DelayModel(BaseModel):
    ms: int

class ReviveModel(BaseModel):
    host: str
    port: int

@app.post('/join')
async def join(body: JoinModel):
    global node
    if node is not None:
        raise HTTPException(status_code=400, detail="Node already initialized")

    node_id = f"{body.host}:{body.tcp_port}"
    tcp_peers = [p for p in body.peers]

    node = Node(node_id, tcp_peers, host=body.host, tcp_port=body.tcp_port)
    asyncio.create_task(node.start_server(body.host, body.tcp_port))
    return {"status": "joined", "node_id": node_id, "peers": tcp_peers}

@app.post('/connect')
async def connect_peers():
    if not node:
        raise HTTPException(status_code=400, detail="Node not initialized")
    for p in node.peers:
        await node.connect_to_peer(p)
    return {'status': 'peers_connected', 'peers': node.peers}

@app.post('/kill')
async def kill():
    if node:
        await node.kill()
    return {'status': 'killed'}

@app.post('/leave')
async def leave():
    if not node:
        return {'status': 'already_left'}
    await node.close_connections()
    node.state.clear()
    node.snapshot_manager.clear()
    node.alive = False
    return {'status': 'left'}

@app.post('/revive')
async def revive_node():
    if not node:
        return {"error": "Node not initialized"}

    await node.revive()
    return {'status': 'revived', 'node_id': node.node_id}

@app.post('/send')
async def send_msg(msg: MessageModel):
    if not node:
        return {'error': 'Node not initialized'}
    lamport_ts = node.clock.tick()
    message = {'type': 'CHAT', 'lamport_ts': lamport_ts, 'from': node.node_id, 'text': msg.text}
    await node.send_msg(msg.to, message)
    return {'status': 'sent', 'to': msg.to, 'text': msg.text}


@app.post('/snapshot')
async def start_snapshot(snap: SnapshotModel):
    if not node:
        return {'error': 'Node not initialized'}
    await node.snapshot_manager.start(snap.snapshot_id)
    return {'status': 'snapshot_started', 'snapshot_id': snap.snapshot_id}


@app.get("/markers")
async def get_markers():
    if not node:
        return {"error": "Node not initialized"}
    return {
        sid: {
            "markers": {ch: ("yes" if v else "no") for ch, v in snap.markers.items()},
            "buffers": {ch: len(msgs) for ch, msgs in snap.buffers.items()},
            "complete": snap.complete
        } for sid, snap in node.snapshot_manager.active.items()
    }

@app.get('/state')
async def get_state():
    if not node:
        return {'error': 'Node not initialized'}
    return node.state.snapshot_copy()

@app.post('/setDelay')
async def set_delay(delay: DelayModel):
    if not node:
        return {'error': 'Node not initialized'}
    node.set_delay(delay.ms / 1000)
    return {'status': 'delay_set', 'ms': delay.ms}
