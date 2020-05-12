import unittest
from sonarclient import SonarClient
import sys
import asyncio


class TestClientMethods(unittest.TestCase):
    ENDPOINT = 'http://localhost:9191/api'
    ISLAND = 'default'

    def test_get_schema_and_schemas(self):
        client = SonarClient(self.ENDPOINT, self.ISLAND)
        loop = asyncio.get_event_loop()
        schemas = loop.run_until_complete(client.get_schemas())
        schema = loop.run_until_complete(client.get_schema('core/schema'))
        loop.run_until_complete(client.close())
        loop.close()
        assert "core/schema" in schemas
        assert "core/schema" in schema
        assert len(schema) <= 150


if __name__ == '__main__':
    if len(sys.argv) > 1:
        print(sys.argv)
        TestClientMethods.ISLAND = sys.argv.pop()
        TestClientMethods.ENDPOINT = sys.argv.pop()
    unittest.main()
