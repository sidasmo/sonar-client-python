from sonarclient import SonarClient
import pytest
import asyncio
import tempfile2
import subprocess
import os
from async_generator import yield_, async_generator


@pytest.mark.asyncio
async def test_put_and_del_record(event_loop):
    client = SonarClient()
    collection = await client.create_collection('testndcollection')
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
    await client.close()
    assert len(results) == 1
    assert results[0].get('id') == id
    assert results[0].get('value').get('title') == 'hello world'


@async_generator
@pytest.fixture(scope='module')
async def start_sonar_server(path_to_sonar, tmpdir, event_loop):
    with tempfile2.TemporaryDirectory() as tmpdir:
        try:
            wd = os.getcwd()
            os.chdir(path_to_sonar)
            process = await asyncio.create_subprocess_shell(
                "DEBUG=sonar-server ./sonar start -s", #+ tmpdir,
                stdout=subprocess.STDOUT,
                stderr=subprocess.PIPE
                )
            os.chdir(wd)
            print("PROZESS", process)
        except Exception:
            print('error')
        await yield_(process)


