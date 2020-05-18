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
        schema = loop.run_until_complete(client.get_schema('core/source'))
        loop.run_until_complete(client.close())
        # loop.close()
        assert "core/source" in schemas
        assert "properties" in schema
        assert len(schema) <= 300

    def test_put_and_del_record(self):
        client = SonarClient(self.ENDPOINT, self.ISLAND)
        loop = asyncio.get_event_loop()
        record = { 'schema': 'doc', 'id': 'foo', 'value': { 'title': 'hello world' } }
        loop.run_until_complete(client.put(record))
        loop.run_until_complete(client.delete(record))
        loop.run_until_complete(client.close())
        loop.close()
        


if __name__ == '__main__':
    if len(sys.argv) > 1:
        TestClientMethods.ISLAND = sys.argv.pop()
        TestClientMethods.ENDPOINT = sys.argv.pop()
    unittest.main()
