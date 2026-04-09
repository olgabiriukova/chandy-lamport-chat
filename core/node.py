import asyncio
import json
from typing import Dict
import os
from core.lamport import LamportClock
from core.state import LocalState
from core.snapshot import SnapshotManager
from utils.logger import make_logger, async_logger

class Node:
    def __init__(self, node_id, peers, host=None, tcp_port=None):
        self.node_id = str(node_id)
        self.peers = [self.normalize_peer_id(p) for p in peers]
        self.all_nodes = set(self.peers + [self.node_id])
        self.clock = LamportClock(self.node_id)
        self.state = LocalState(self.node_id)
        self.snapshot_manager = SnapshotManager(self)
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, f"{self.node_id.replace(':', '_')}.log")
        self.log = make_logger(self.node_id, self.clock, log_file)
        self.alog = async_logger(self.node_id, self.clock, log_file)
        self.log(f"Node initialized. Peers: {self.peers}")
        self.conn: Dict[str, asyncio.StreamWriter] = {}
        self.delay_sec = 0.0
        self.cs_lock = asyncio.Lock()
        self.alive = True
        self.host = host
        self.tcp_port = tcp_port

    def normalize_peer_id(self, peer_id):
        if isinstance(peer_id, list):
            return ":".join(map(str, peer_id))
        return str(peer_id)

    async def connect_to_peer(self, peer_id):
        peer_id = self.normalize_peer_id(peer_id)
        if peer_id == self.node_id or peer_id in self.conn:
            return
        host, port = peer_id.split(":")
        try:
            reader, writer = await asyncio.open_connection(host, int(port))
            writer.write((json.dumps({"node_id": self.node_id}) + "\n").encode())
            await writer.drain()
            self.conn[peer_id] = writer
            self.state.add_channel(peer_id)
            self.all_nodes.add(peer_id)
            await self.alog(f"Connected to peer {peer_id}")
            asyncio.create_task(self._read_from_peer(peer_id, reader))
            await self.broadcast_topology()
        except Exception as e:
            await self.alog(f"Failed to connect to {peer_id}: {e}")

    async def _read_from_peer(self, peer_id, reader: asyncio.StreamReader):
        peer_id = self.normalize_peer_id(peer_id)
        while self.alive:
            try:
                data = await reader.readline()
                if not data:
                    break
                msg = json.loads(data.decode())
                await self.handle_incoming(peer_id, msg)
            except Exception as e:
                await self.alog(f"Error reading from {peer_id}: {e}")
                break
        self.conn.pop(peer_id, None)
        self.state.remove_channel(peer_id)
        self.all_nodes.discard(peer_id)
        await self.alog(f"Connection closed from {peer_id}")
        await self.broadcast_topology()

    async def _handle_client(self, reader, writer):
        peer_addr = writer.get_extra_info('peername')
        temp_id = f"{peer_addr[0]}:{peer_addr[1]}"
        try:
            data = await reader.readline()
            peer_info = json.loads(data.decode())
            peer_id = self.normalize_peer_id(peer_info.get("node_id", temp_id))
        except Exception as e:
            await self.alog(f"Handshake failed from {temp_id}: {e}")
            writer.close()
            await writer.wait_closed()
            return
        if peer_id in self.conn:
            await self.alog(f"Already connected to {peer_id}, keeping existing connection")
        else:
            self.conn[peer_id] = writer
            self.state.add_channel(peer_id)
            self.all_nodes.add(peer_id)
            await self.alog(f"New connection from {peer_id}")
            await self.broadcast_topology()

        await self._read_from_peer(peer_id, reader)

    async def handle_incoming(self, peer_id, msg):
        peer_id = self.normalize_peer_id(peer_id)
        t = msg.get("type")
        if t == "CHAT":
            self.clock.update_on_receive(msg["lamport_ts"])
            should_buffer = False
            for snapshot_id, snapshot in self.snapshot_manager.active.items():
                if not snapshot.complete:
                    if not snapshot.markers.get(peer_id, False):
                        self.snapshot_manager.record_msg(snapshot_id, peer_id, msg)
                        should_buffer = True
            if not should_buffer:
                self.state.add_msg(msg["lamport_ts"], peer_id, msg["text"])
                await self.alog(f"Received CHAT from {peer_id}: {msg['text']}")
            else:
                await self.alog(f"Buffered CHAT from {peer_id}: {msg['text']}")
        elif t == "MARKER":
            await self.snapshot_manager.handle_marker(msg["snapshot_id"], peer_id)
        elif t == "TOPOLOGY":
            nodes = msg.get("nodes", [])
            new_nodes = set(nodes) - self.all_nodes
            self.all_nodes.update(nodes)
            await self.alog(f"Topology updated: {self.all_nodes}")
            for new_node in new_nodes:
                await self.connect_to_peer(new_node)
        else:
            await self.alog(f"Unknown message type from {peer_id}: {msg}")

    async def send_msg(self, peer_id, msg):
        if not self.alive:
            await self.alog("CANNOT SEND: NODE IS DEAD")
            return False
        peer_id = self.normalize_peer_id(peer_id)
        writer = self.conn.get(peer_id)
        if not writer:
            await self.alog(f"NO CONNECTION to {peer_id}, cannot send")
            return False
        self.clock.tick()
        if self.delay_sec > 0:
            await asyncio.sleep(self.delay_sec)
        try:
            writer.write((json.dumps(msg) + "\n").encode())
            await writer.drain()
            await self.alog(f"Sent to {peer_id}: {msg}")
            return True
        except Exception as e:
            await self.alog(f"Send failed to {peer_id}: {e}")
            return False

    def set_delay(self, sec: float):
        self.delay_sec = sec

    async def send_marker(self, peer_id, snapshot_id):
        peer_id = self.normalize_peer_id(peer_id)
        await self.send_msg(peer_id, {"type": "MARKER", "snapshot_id": snapshot_id})

    async def start_server(self, host, port):
        server = await asyncio.start_server(self._handle_client, host, port)
        self.log(f"TCP server started at {host}:{port}")
        async with server:
            await server.serve_forever()

    async def broadcast_topology(self):
        msg = {"type": "TOPOLOGY", "nodes": list(self.all_nodes)}
        for peer_id in list(self.conn.keys()):
            await self.send_msg(peer_id, msg)

    def snapshot_complete(self, snapshot_id):
        snapshot = self.snapshot_manager.active[snapshot_id]
        for ch, msgs in snapshot.buffers.items():
            for msg in msgs:
                if msg.get("type") == "CHAT":
                    self.state.add_msg(msg["lamport_ts"], msg["from"], msg.get("text", ""))
        self.log(f"Snapshot {snapshot_id} complete {snapshot.local_state}")
        self.snapshot_manager.save_to_disk(snapshot_id)

    async def kill(self):
        self.alive = False
        await self.alog("NODE CRASHED: stopping all processing immediately")
        await self.close_connections()
        self.state.clear()
        self.state.incoming_ch.clear()

    async def close_connections(self):
        for writer in list(self.conn.values()):
            try:
                writer.close()
                await writer.wait_closed()
            except:
                pass
        self.conn.clear()
        self.all_nodes = {self.node_id}
        await self.broadcast_topology()

    async def revive(self):
        if self.alive:
            await self.alog("Node already alive")
            return
        self.alive = True
        self.state = LocalState(self.node_id)
        self.snapshot_manager = SnapshotManager(self)
        asyncio.create_task(self.start_server(self.host, self.tcp_port))
        for p in self.peers:
            await self.connect_to_peer(p)
        for node_id in self.all_nodes:
            await self.connect_to_peer(node_id)
        await self.alog("NODE REVIVED")
        await self.broadcast_topology()