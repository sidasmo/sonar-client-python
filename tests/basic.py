from sonarclient import SonarClient
import pytest
import asyncio
import tempfile2
import subprocess
import os
from async_generator import yield_, async_generator


@pytest.mark.asyncio
async def test_put_and_del_record(start_sonar_server, event_loop):
    client = SonarClient()
    collection = await client.create_collection('testndcollection')
    record = {
        'schema': 'doc',
        'id': 'foo',
        'value': {'title': 'hello world'}}
    res = await collection.put(record)
    id = res.get('id')
    results = await collection.query(
        'records', {'id': id}, {'waitForSync': 'true'})
    assert len(results) == 1
    assert results[0].get('id') == id
    assert results[0].get('value').get('title') == 'hello world'
    await client.close()

@pytest.mark.asyncio
async def test_get_and_delete_record(start_sonar_server,event_loop):
    client = SonarClient()
    collection = await client.create_collection('foocollection')
    record = {
        'schema': 'doc',
        'id': 'foo',
        'value': {'title': 'hello world'}}
    res = await collection.put(record)
    id = res['id']
    records = await collection.get({'id': id}, {'waitForSync': 'true'})
    print("RECORD:" ,records)
    assert len(records) == 1
    deletemsg= await collection.delete(record)
    print('DELETE', deletemsg)
    nu_records = await collection.get({'id': id}, {'waitForSync': 'true'})
    # TODO: deletion is not implemented yet on server-side
    #assert len(nu_records) == 1
    await client.close()


@pytest.fixture
async def start_sonar_server(scope='Session'):
    sonar_location = '../sonar'
    with tempfile2.TemporaryDirectory() as tmpdir:
        try:
            wd = os.getcwd()
            os.chdir(sonar_location)
            process = await asyncio.create_subprocess_shell(
                "./sonar start -s" + tmpdir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            ).wait()
          
            out, err = process.communicate(timeout=30)
            os.chdir(wd)
            return await yield_(process)

        except Exception:
            print('error')
