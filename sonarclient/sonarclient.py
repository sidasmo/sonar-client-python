import aiohttp
import binascii
from os import urandom



class SonarClient:

    def __init__(self, endpoint, island, opts=dict()):
        self.endpoint = endpoint
        self.island = island
        self.session = aiohttp.ClientSession()
        # TODO: check if id is generated correctly
        self.id = opts.get('id', binascii.hexlify(urandom(16)).decode())
        self.name = opts.get('name')

        self._cacheid = None
        if opts.get('cache') != False:
            self._cacheid = 'client:' + self.id + ':' + self.island 

    async def close(self):
        await self.session.close()

    async def _info(self):
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

    async def focusIsland(self,name):
        if not name and self.island:
            name = self.island
        if not name:
                raise Exception("Missing island name")
       #todo
        
    async def get_schema(self, schemaName):
        # schemaName = expand_schema(schemaName)
        return await self._request({
            'path': [self.island, 'schema'],
            'params': {'name': schemaName}
        })

    async def get_schemas(self):
        return await self._request({
            'path': [self.island, 'schema']
        })

    async def put_schema(self, schemaName, schema):
        schema['name'] = schemaName
        return await self._request({
            'method': 'POST',
            'path': [self.island, 'schema'],
            'data': schema
        })

    async def put(self, record):
        schema = record.get('schema')
        schema = expand_schema(schema)
        path = [self.island, 'db']
        method = 'PUT'
        return await self._request({
            'path': path,
            'method': method,
            'data': record})

    async def get(self, schema, id, opts):
        return await self.query('records',{'schema': schema, 'id': id}, opts)

    async def delete(self, record):
        path = [self.island, 'db', record['id']]
        return await self._request({
            'path': path,
            'method': 'DELETE',
            'params': {'schema': record['schema']}
        })

    async def query(self, name, args, opts=dict()):
        if self._cacheid:
            opts['cacheid'] = self._cacheid
        
        records = await self._request({
            'path': [self.island, '_query', name],
            'method': 'POST',
            'data': args,
            'params': opts
        })

        # TODO: is cache already implemented? assign in constructor
        # if (self._cacheid):
        #     return self._cache.batch(records)
        
        return records

    async def _request(self, opts):
        url = opts.get('url')
        if url is None:
            path = opts.get('path')
            if type(path) is list:
                path = '/'.join(path)
            url = self.endpoint + '/' + path

        async with self.session.request(
            opts.get('method') or 'GET',
            url,
            headers={'Content-Type': 'application/json'},
            json=opts.get('data') or {},
            params=opts.get('params')
        ) as resp:
            return await resp.text()


def expand_schema(schema):
    if "/" in schema:
        return schema
    else:
        return "_/" + schema
