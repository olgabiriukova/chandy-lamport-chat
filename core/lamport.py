

class LamportClock:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.val = 0
    
    def tick(self):
        self.val+=1
        return self.val
    
    def update_on_receive(self, remote_ts):
        self.val = max(self.val, remote_ts)+1
        return self.val
    
    def read(self):
        return self.val