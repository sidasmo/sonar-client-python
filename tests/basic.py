from sonarclient import SonarClient
import pytest
import asyncio
import tempfile2
import subprocess
import os
from xprocess import ProcessStarter

@pytest.mark.asyncio
async def test_put_and_query_record(client):
    collection = await client.create_collection('collection')
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

@pytest.fixture()
async def client(event_loop):
    client = SonarClient()
    yield client
    await client.close()

@pytest.fixture(autouse=True)
def start_sonar_server(xprocess):
    class Starter(ProcessStarter):
        pattern = "listening on http://localhost:9191"
        args = ['node', '/home/osuiowq/Projekte/sonar/sonar-server/launch.js', '--dev', '-s' '/tmp']   
    xprocess.ensure("sonarServer", Starter)
    yield 
    xprocess.getinfo("sonarServer").terminate()        

# TODO: deletion on serverside may not working at the moment
@pytest.mark.asyncio
async def test_get_and_delete_record(client):
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
    print('NU ',nu_records)
    # TODO: deletion is not implemented yet on server-side
    assert len(nu_records) == 0

@pytest.mark.asyncio
async def test_fs_with_strings(client):
    collection = await client.create_collection('test')
    await collection.fs.write_file('/test/hello','world')
    result = await collection.fs.read_file('/test/hello')
    print("result:", result)
    assert result.decode('utf-8') == "world"

@pytest.mark.asyncio
async def test_fs_with_buffers(client):
    collection = await client.create_collection('test')
    buf = b'hello'
    await collection.fs.write_file('/test/bin', buf)
    res = await collection.fs.read_file('/test/bin')
    print(res) 
    assert type(res) == bytes