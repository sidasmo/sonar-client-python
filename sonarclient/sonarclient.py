import aiohttp
import binascii
import json
from os import urandom
from .collection import Collection
from .constants import DEFAULT_ENDPOINT


class SonarClient:

    def __init__(self, opts=dict()):
        self.endpoint = opts.get('endpoint') or DEFAULT_ENDPOINT
        if self.endpoint.endswith('/'):
            self.endpoint = self.endpoint[:-1]
        self._collections = {}
        # TODO: check if id is generated correctly
        self._id = opts.get('id', binascii.hexlify(urandom(16)).decode())

        self.session = aiohttp.ClientSession()
        self.name = opts.get('name')
        # TODO: implement commands

    async def close(self):
        await self.session.close()

    async def list_collections(self):
        _info = await self._info()
        return _info.collections

    async def _info(self):
        return await self._request({
            'method': 'GET',
            'path': ['_info']
        })

    async def create_collection(self, name, opts={}):
        print("CREATE_COLLECTION CALLED")
        res = await self._request({
            'method': 'PUT',
            'path': ['_create', name],
            'data': opts
        })
        print("RESULT OF CREATE_COLLECTION: ", res)
        collection = await self.open_collection(name)
        print("RESULT OF OPEN_COLLECTION: ", collection)
        return collection

    async def update_collection(self, name, info):
        return self._request({
            'method': 'PATCH',
            'path': name,
            'data': info
        })

    async def open_collection(self, key_or_name):
        print("OPEN_COLLECTION CALLED")
        if self._collections.get(key_or_name):
            print("COLLECTION FOUND IN CLIEN._COLLECTIONS")
            return self._collections.get(key_or_name)
        collection = Collection(self, key_or_name)
        # TODO: check if this throws if the collection does not exist
        await collection.open()
        self._collections[collection.name] = collection
        # TODO: maybe need to check that key exists (could be None?)
        self._collections[collection.key] = collection
        return collection

    async def _request(self, opts):
        url = opts.get('url')
        endpoint = opts.get('endpoint') or self.endpoint
        if url is None:
            path = opts.get('path')
            if type(path) is list:
                path = '/'.join(path)
            url = endpoint + path
            print("URL: ", url)

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

    # async def get_schema(self, schemaName):
    #     # schemaName = expand_schema(schemaName)
    #     return await self._request({
    #         'path': [self.island, 'schema'],
    #         'params': {'name': schemaName}
    #     })

    # async def get_schemas(self):
    #     return await self._request({
    #         'path': [self.island, 'schema']
    #     })

    # async def put_schema(self, schemaName, schema):
    #     schema['name'] = schemaName
    #     return await self._request({
    #         'method': 'POST',
    #         'path': [self.island, 'schema'],
    #         'data': schema
    #     })

    # async def put(self, record):
    #     schema = record.get('schema')
    #     schema = expand_schema(schema)
    #     path = [self.island, 'db']
    #     method = 'PUT'
    #     return await self._request({
    #         'path': path,
    #         'method': method,
    #         'data': record})

    # async def get(self, schema, id, opts):
    #     return await self.query('records', {'schema': schema, 'id': id}, opts)

    # async def delete(self, record):
    #     path = [self.island, 'db', record['id']]
    #     return await self._request({
    #         'path': path,
    #         'method': 'DELETE',
    #         'params': {'schema': record['schema']}
    #     })

    # async def query(self, name, args, opts=dict()):
    #     if self._cacheid:
    #         opts['cacheid'] = self._cacheid

    #     records = await self._request({
    #         'path': [self.island, '_query', name],
    #         'method': 'POST',
    #         'data': args,
    #         'params': opts
    #     })

    #     # TODO: is cache already implemented? assign in constructor
    #     # if (self._cacheid):
    #     #     return self._cache.batch(records)

    #     return records