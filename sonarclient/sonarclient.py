import aiohttp
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
            'ath': ['_create', name],
            'data': {}
        })

    async def get_schema(self, schemaName):
        schemaName = expand_schema(schemaName)
        return await self._request({
            'path': [self.island, 'schema', schemaName]
        })

    async def put_schema(self, schemaName, schema):
        schemaName = expand_schema(schemaName)
        return await self._request({
            'method': 'PUT',
            'path': [self.island, 'schema', schemaName],
            'data': schema
        })

    async def put(self, record):
        schema = record.get('schema')
        schema = expand_schema(schema)
        value = record.get('value')
        id = record.get('id')
        path = [self.island, 'db', schema]
        method = 'POST'
        if id:
            method = 'PUT'
            path.append(id)
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

        async with self.session.request(
            opts.get('method') or 'GET',
            url,
            headers={'Content-Type': 'application/json'},
            json=opts.get('data') or {},
        ) as resp:
            return await resp.text()


def expand_schema (schema):
    if "/" in schema:
        return schema
    else:
        return "_/" + schema
