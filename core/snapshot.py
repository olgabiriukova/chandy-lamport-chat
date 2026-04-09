from typing import Dict, List, Any
import asyncio
import os
import json

class SnapshotState:
    def __init__(self, snapshot_id: str, peers: List[str]):
        self.id = snapshot_id
        self.local_state: Dict[str, Any] = None
        self.markers: Dict[str, bool] = {ch: False for ch in peers}
        self.buffers: Dict[str, List[Any]] = {ch: [] for ch in peers}
        self.complete: bool = False
        self.active = {}


class SnapshotManager:
    def __init__(self, node):
        self.node = node
        self.active: Dict[str, SnapshotState] = {}
        self.snapshots_dir = "snapshots"
        os.makedirs(self.snapshots_dir, exist_ok=True)
    
    async def start(self, snapshot_id: str):
        incoming_ch = list(self.node.state.incoming_ch)
        state = SnapshotState(snapshot_id, incoming_ch)
        state.local_state = self.node.state.snapshot_copy()
        self.active[snapshot_id] = state
        for peer_id in self.node.peers:
            if not isinstance(peer_id, str):
                peer_id = str(peer_id)
            await self.node.send_marker(peer_id, snapshot_id)
        if not incoming_ch:
            state.complete = True
            self.node.snapshot_complete(snapshot_id)
    
    async def handle_marker(self, snapshot_id: str, incoming_ch: str):
        if not isinstance(incoming_ch, str):
            incoming_ch = str(incoming_ch)
        state = self.active.get(snapshot_id)
        if state is None:
            channels = list(self.node.state.incoming_ch)
            state = SnapshotState(snapshot_id, channels)
            state.local_state = self.node.state.snapshot_copy()
            self.active[snapshot_id] = state
            for peer_id in self.node.peers:
                if not isinstance(peer_id, str):
                    peer_id = str(peer_id)
                await self.node.send_marker(peer_id, snapshot_id)
        if incoming_ch in state.markers:
            state.markers[incoming_ch] = True
        else:
            state.markers[incoming_ch] = True
            state.buffers[incoming_ch] = []
        current_channels = list(self.node.state.incoming_ch)
        all_markers_received = all(state.markers.get(ch, False) for ch in current_channels)
        
        if all_markers_received and not state.complete:
            state.complete = True
            self.node.snapshot_complete(snapshot_id)
    
    def record_msg(self, snapshot_id: str, incoming_ch: str, msg: Dict):
        state = self.active.get(snapshot_id)
        if state is None:
            return
        if not isinstance(incoming_ch, str):
            incoming_ch = str(incoming_ch)
        if msg.get("type") == "MARKER" and msg.get("snapshot_id") == snapshot_id:
            return
        if not state.markers.get(incoming_ch, False):
            if incoming_ch not in state.buffers:
                state.buffers[incoming_ch] = []
            state.buffers[incoming_ch].append(msg)
        
    def clear(self):
        self.active.clear()

    def save_to_disk(self, snapshot_id: str):
        state = self.active.get(snapshot_id)
        if not state:
            return
        filename = os.path.join(self.snapshots_dir, f"snapshot_{snapshot_id}.json")
        data = {
            "snapshot_id": snapshot_id,
            "local_state": state.local_state,
            "markers": state.markers,
            "buffers": {ch: msgs for ch, msgs in state.buffers.items()},
            "complete": state.complete
        }
        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"Error saving snapshot {snapshot_id} to disk: {e}")