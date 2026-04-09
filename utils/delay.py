import asyncio

_curr_delay_ms = 0

def set_delay(ms):
    global _curr_delay_ms
    _curr_delay_ms = ms

def get_delay():
    return _curr_delay_ms

async def delay_msg():
    if _curr_delay_ms > 0:
        await asyncio.sleep(_curr_delay_ms/1000)

