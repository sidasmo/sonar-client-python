import unittest
from sonarclient import SonarClient
import sys
import asyncio


class TestClientMethods(unittest.TestCase):
    ENDPOINT = 'http://localhost:9191/api'
    ISLAND = 'default'

    def test_put_get_schemas(self):
        client = SonarClient(self.ENDPOINT, self.ISLAND)
        loop = asyncio.get_event_loop()
        schemas = loop.run_until_complete(client.get_schemas())
        loop.run_until_complete(client.close())
        loop.close()
        assert "core/schema" in schemas


if __name__ == '__main__':
    if len(sys.argv) > 1:
        TestClientMethods.ENDPOINT = sys.argv.pop()
        TestClientMethods.ISLAND = sys.argv.pop()
    unittest.main()
