import unittest
from sonarclient import SonarClient
import aiohttp
import asyncio


class TestClientMethods(unittest.TestCase):

    def test_put_get_schemas(self):
        client = SonarClient(endpoint='http://localhost:9191/api',island='default')
        loop = asyncio.get_event_loop()
        schemas = loop.run_until_complete(client.get_schemas())
        loop.run_until_complete(client.close())
        loop.close()
        assert "core/schema" in schemas
        print(schemas)

if __name__ == '__main__':
    unittest.main()
