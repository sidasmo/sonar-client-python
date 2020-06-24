from sonarclient import SonarClient
import pytest
import asyncio
import tempfile2
import subprocess
import os


@pytest.mark.asyncio
async def test_put_and_del_record(event_loop):
    with tempfile2.TemporaryDirectory() as tmpdir:
        try:
            await start_sonar_server("../sonar", tmpdir, loop=event_loop)
        except Exception:
            print("error")
    client = SonarClient()
    # loop.run_until_complete(self.start_sonar_server("../sonar", tmpdir))
    collection = await client.create_collection('2ndcollection')
    print("COLLECTION: ", collection)
    record = {
        'schema': 'doc',
        'id': 'foo',
        'value': {'title': 'hello world'}}
    res = await collection.put(record)
    print(res)
    id = res.get('id')
    results = await collection.query(
        'records', {'id': id}, {'waitForSync': True})
    assert len(results) == 1
    assert results[0].get('id') == id
    assert results[0].get('value').get('title') == 'hello world'


@pytest.mark.asyncio
async def start_sonar_server(path_to_sonar, tmpdir, loop):
    try:
        wd = os.getcwd()
        os.chdir(path_to_sonar)
        process = await asyncio.create_subprocess_shell(
            "./sonar start -s" + tmpdir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
            )
        os.chdir(wd)
        print("PROZESS", process)
    except Exception:
        print('error')
        return False
    await True
