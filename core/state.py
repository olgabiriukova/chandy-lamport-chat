from typing import List, Tuple, Dict, Any
import copy

class LocalState:
    def __init__(self, node_id):
        self.node_id = node_id
        self.history: List[Tuple[int, str, str]] = []
        self.incoming_ch: List[str] = []

    def add_msg(self, lamport_ts, from_id, text):
        self.history.append((lamport_ts, from_id, text))

    def snapshot_copy(self):
        return {
            'node_id': self.node_id,
            'history': copy.deepcopy(self.history),
            'incoming_channels': list(self.incoming_ch),
        }

    def add_channel(self, ch_id):
        if ch_id not in self.incoming_ch:
            self.incoming_ch.append(ch_id)

    def remove_channel(self, ch_id):
        if ch_id in self.incoming_ch:
            self.incoming_ch.remove(ch_id)

    def clear(self):
        self.history.clear()

