import aiohttp
import asyncio
import sys

HOST = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
PORT = sys.argv[2] if len(sys.argv) > 2 else "8000"
BASE_URL = f"http://{HOST}:{PORT}"

async def post(path, json=None):
    async with aiohttp.ClientSession() as session:
        async with session.post(BASE_URL + path, json=json) as resp:
            if resp.content_type == 'application/json':
                return await resp.json()
            else:
                text = await resp.text()
                return {"error": f"Server returned {resp.status}: {text}"}

async def get(path):
    async with aiohttp.ClientSession() as session:
        async with session.get(BASE_URL + path) as resp:
            return await resp.json()

async def handle_connect(args):
    res = await post("/connect")
    print(res)

async def handle_join(args):
    if len(args) < 3:
        print("Usage: join host:rest_port tcp_port peer1 peer2 ...")
        return
    host, rest_port = args[0].split(":")
    tcp_port = int(args[1])
    peers = args[2:]
    payload = {
        "host": host,
        "rest_port": int(rest_port),
        "tcp_port": tcp_port,
        "peers": peers
    }
    res = await post("/join", payload)
    print(res)

async def handle_send(args):
    if len(args) < 2:
        print("Usage: send target text")
        return
    to = args[0]
    text = " ".join(args[1:])
    print(await post("/send", {"to": to, "text": text}))

async def handle_snapshot(args):
    if len(args) < 1:
        print("Usage: snapshot id")
        return
    snap_id = args[0]
    print(await post("/snapshot", {"snapshot_id": snap_id}))

async def handle_state(args):
    print(await get("/state"))

async def handle_delay(args):
    if len(args) < 1:
        print("Usage: delay ms")
        return
    print(await post("/setDelay", {"ms": int(args[0])}))

async def handle_leave(args):
    print(await post("/leave"))

async def handle_kill(args):
    print(await post("/kill"))

async def handle_revive(args):
    print(await post("/revive"))

async def handle_markers(args):
    res = await get("/markers")
    for sid, snap in res.items():
        print(f"Snapshot {sid}:")
        print("  Markers:")
        for ch, val in snap["markers"].items():
            print(f"    {ch}: {val}")
        print("  Buffers:")
        for ch, msgs in snap["buffers"].items():
            count = len(msgs) if isinstance(msgs, list) else msgs
            print(f"    {ch}: {count} message(s) buffered")
        print(f"  Complete: {snap['complete']}\n")

COMMANDS = {
    "connect": handle_connect,
    "join": handle_join,
    "send": handle_send,
    "snapshot": handle_snapshot,
    "state": handle_state,
    "delay": handle_delay,
    "leave": handle_leave,
    "kill": handle_kill,
    "revive": handle_revive,
    "markers": handle_markers,
}

async def main():
    print("CLI, Type 'help' for commands.")
    while True:
        try:
            line = input(">> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting CLI.")
            break
        if not line:
            continue
        parts = line.split()
        cmd, args = parts[0], parts[1:]
        if cmd == "help":
            print("Commands: " + ", ".join(COMMANDS.keys()))
            continue
        handler = COMMANDS.get(cmd)
        if handler:
            await handler(args)
        else:
            print(f"Unknown command '{cmd}'. Type 'help' for list.")

if __name__ == "__main__":
    asyncio.run(main())
