import unittest
from sonarclient import SonarClient
import sys
import asyncio
import tempfile2
import subprocess
import os


class TestClientMethods(unittest.TestCase):
    ENDPOINT = 'http://localhost:9191/api'
    ISLAND = 'default'

    async def start_sonar_server(self, path_to_sonar, tmpdir):
        try:
            wd = os.getcwd()
            os.chdir(path_to_sonar)
            process = await asyncio.create_subprocess_shell(
                "./sonar start -s" + tmpdir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
            os.chdir(wd)
        except Exception:
            print('error')
            return False
        print(process)
        return True

    def test_put_and_del_record(self):

        with tempfile2.TemporaryDirectory() as tmpdir:
            self.start_sonar_server("../sonar", tmpdir)
            loop = asyncio.get_event_loop()
            try:
                client = SonarClient()
                #loop.run_until_complete(self.start_sonar_server("../sonar", tmpdir))
                collection = loop.run_until_complete(
                    client.create_collection('2ndcollection'))
                print("COLLECTION: ", collection)
                record = {'schema': 'doc', 'id': 'foo',
                          'value': {'title': 'hello world'}}
                res = loop.run_until_complete(collection.put(record))
                print(res)
                id = res.get('id')
                results = loop.run_until_complete(collection.query(
                    'records', {'id': id}, {'waitForSync': True}))
                assert len(results) == 1
                assert results[0].get('id') == id
                assert results[0].get('value').get('title') == 'hello world'
            finally:
                loop.run_until_complete(client.close())
                loop.close()
                #subprocess.call('killall node', shell=True)


   
    # def test_get_schema_and_schemas(self):
    #     client = SonarClient()
    #     loop = asyncio.get_event_loop()
    #     client.create_collection('Firstcoll')
    #     schemas = loop.run_until_complete(client.get_schemas())
    #     schema = loop.run_until_complete(client.get_schema('core/source'))
    #     loop.run_until_complete(client.close())
    #     # loop.close()c
    #     assert "core/source" in schemas
    #     assert "properties" in schema
    #     assert len(schema) <= 300


if __name__ == '__main__':
    if len(sys.argv) > 1:
        TestClientMethods.ISLAND = sys.argv.pop()
        TestClientMethods.ENDPOINT = sys.argv.pop()
    unittest.main()
