#!/usr/bin/env python3
"""Debug script to test metrics endpoint."""

import asyncio
import asyncpg
import aiohttp
from aiohttp import web
import logging

logging.basicConfig(level=logging.DEBUG)

async def simple_metrics_handler(request):
    """Simple metrics handler to test charset issue."""
    try:
        content = """# HELP ats_test_metric Test metric
# TYPE ats_test_metric gauge
ats_test_metric 42
"""
        return web.Response(
            text=content,
            content_type='text/plain; version=0.0.4',
            charset='utf-8'
        )
    except Exception as e:
        print(f"Error: {e}")
        return web.Response(
            text=f"Error: {str(e)}",
            status=500
        )

async def main():
    app = web.Application()
    app.router.add_get('/test', simple_metrics_handler)

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, '0.0.0.0', 8081)
    await site.start()

    print("Test server running on http://localhost:8081/test")

    # Keep server running
    while True:
        await asyncio.sleep(1)

if __name__ == '__main__':
    asyncio.run(main())