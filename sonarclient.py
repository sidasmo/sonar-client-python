import aiohttp
import asyncio
import re
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
        schemaName = schemaName.re('/', '-')
        return await self._request({
            'path': [self.island, 'schema', schemaName]
        })

    async def put_schema(self, schemaName, schema):
        return await self._request({
            'method': 'PUT',
            'path': [self.island, 'schema', schemaName],
            'data': schema
        })

    async def put(self, record):
        schema, id, value = record
        path = [self.island, 'db', schema]
        method = 'POST'
        if (id):
            method = 'PUT'
            path.append(id)
        return await self._request({"path": path, 'method': method, 'data': value})

    async def _request(self, opts):
        url = opts.get('url')
        if url is None:
            url = self.base_url + '/' + \
                opts.get('path')[0]+'/' + opts.get('path')[1]
        aio_opts = {
            'method': opts['method'] or 'GET',
            'url': url,
            'headers': {'Content-Type': 'application/json'},
            'data': opts['data'] or {},
            'responseType': opts.get('responseType')}
        if aio_opts.get('method').lower() == 'put':
            async with self.session.put(url) as resp:
                return await resp.text()


async def main():
    print("short example: create a island and get a key:")
    client = SonarClient('http://localhost:9191/api', 'default')
    resp = await client.create_island('telegram')
    key = json.loads(resp).get('key')
    print(key)
    await client.put('{Hello World}')
    await client.session.close()
asyncio.run(main())
