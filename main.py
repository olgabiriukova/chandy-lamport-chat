import asyncio
import uvicorn
from rest.server import app
from utils.delay import set_delay
import sys

async def main():
    set_delay(500)
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level="info")
    server=uvicorn.Server(config)
    await server.serve()

if __name__=='__main__':
    asyncio.run(main())

