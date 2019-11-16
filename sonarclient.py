import aiohttp
import asyncio
import json


class SonarClient:

    def __init__(self, base_url, island):
        self.base_url = base_url
        self.island = island
        self.session = aiohttp.ClientSession()

    async def info(self):
        return await self._request({
            'method': 'GET',
            'path': ['_info']
        })

    async def create_island(self, name):
        return await self._request({
            'method': 'PUT',
            'path': ['_create', name],
            'data': {}
        })

    async def get_schema(self, schemaName):
        schemaName = schemaName.replace('/', '-')
        return await self._request({
            'path': [self.island, 'schema', schemaName]
        })

    async def put_schema(self, schemaName, schema):
        return await self._request({
            'method': 'PUT',
            'path': [self.island, 'schema', schemaName],
            'data': json.dumps(schema)
        })

    async def put(self, record):
        schema, value = record
        path = [self.island, 'db', schema]
        method = 'POST'
        return await self._request({
            "path": path,
            'method': method,
            'data': value})

    async def _request(self, opts):
        url = opts.get('url')
        if url is None:
            path = opts.get('path')
            if type(path) is list:
                path = '/'.join(path)
            url = self.base_url + '/' + path
        aio_opts = {
            'method': opts.get('method') or 'GET',
            'url': url,
            'headers': {'Content-Type': 'application/json'},
            'data': opts.get('data') or {},
            'responseType': opts.get('responseType')}
        if aio_opts.get('method').lower() == 'put':
            async with self.session.put(url, data=json.dumps(aio_opts)) as resp:
                return await resp.text()
        elif aio_opts.get('method').lower() == 'get':
            async with self.session.get(url) as resp:
                return await resp.text()
        elif aio_opts.get('method').lower() == 'post':
            async with self.session.post(url, data=json.dumps(aio_opts)) as resp:
                return await resp.text()


async def main():
    print("short example: create a island and get a key:")
    client = SonarClient('http://localhost:9191/api', 'default')
    resp = await client.create_island('example_island')
    key = json.loads(resp).get('key')
    print(key)
    resp = await client.info()
    print('CLIENTINFO:' + resp)
    from exampleschema import get_doc, get_telegram_media_schema
    schema = get_telegram_media_schema()
    resp = await client.put_schema("example_schema", schema)
    schema = json.loads(resp)
    schema_name = schema.get('schema')
    print(schema_name)
    record = schema_name, get_doc()
    resp = await client.put(record)
    print(resp)
    await client.session.close()
asyncio.run(main())
