import aiohttp
import binascii
import ujson
import re
from os import urandom
from urllib.parse import urlencode
from .collection import Collection
from .constants import DEFAULT_ENDPOINT
import json


class SonarClient:

    def __init__(self, opts=dict()):
        self.endpoint = opts.get('endpoint') or DEFAULT_ENDPOINT
        if self.endpoint.endswith('/'):
            self.endpoint = self.endpoint[:-1]
        self._collections = {}
        # TODO: check if id is generated correctly
        self._id = opts.get('id', binascii.hexlify(urandom(16)).decode())

        self.session = aiohttp.ClientSession(json_serialize=ujson.dumps)
        self.name = opts.get('name')
        # TODO: implement commands

    async def close(self):
        await self.session.close()

    async def list_collections(self):
        _info = await self._info()
        return _info.collections

    async def _info(self):
        info = await self.fetch('/_info')
        return info['collections']

    async def create_collection(self, name, opts={}):
        await self.fetch('/_create/' + name, {
            'method': 'PUT',
            'body': opts
        })
        collection = await self.open_collection(name)
        return collection

    async def update_collection(self, name, info):
        return self.fetch(name, {
            'method': 'PATCH',
            'body': info
        })

    async def open_collection(self, key_or_name):
        if self._collections.get(key_or_name):
            return self._collections.get(key_or_name)
        collection = Collection(self, key_or_name)
        # TODO: check if this throws if the collection does not exist
        await collection.open()
        self._collections[collection.name] = collection
        # TODO: maybe need to check that key exists (could be None?)
        self._collections[collection.key] = collection
        return collection

    async def fetch(self, url, opts={}):
        if not re.match(r'https?://', url):
            if '://' in url:
                raise Exception(
                    'Only http: and https: protocols are supported.')
            if url[0] != '/':
                url = '/' + url
            if opts.get('endpoint'):
                url = opts['endpoint'] + url
            else:
                url = self.endpoint + url

        if not opts.get('headers'):
            opts['headers'] = {}
        if not opts.get('requestType'):
            try:
                opts.get('body').decode()
                opts['requestType'] = 'buffer'
            except (UnicodeDecodeError, AttributeError):
                opts['requestType'] = 'json'

        if opts.get('params'):
            searchParams = urlencode(opts['params'])
            url += '?' + searchParams

        if opts.get('requestType') == 'json':
            opts['headers']['content-type'] = 'application/json'
        if opts.get('requestType') == 'buffer':
            opts['headers']['content-type'] = 'application/octet-stream'


        async with self.session.request(
            opts.get('method') or 'GET',
            url,
            headers=opts.get('headers') or {
                'content-type': 'application/json'},
            json=opts.get('body') or {},
            params=opts.get('params') or {}
        ) as resp:
            if resp.status != 200:
                try:
                    message = (await resp.json())['error']
                except Exception:
                    message = await resp.text()
            if opts.get('responseType') == 'stream':
                return await resp.body
            if opts.get('responseType') == 'buffer':
                if resp.content:
                    return await resp.content.read(10)
                else:
                    return await resp.content.read(10)
            try:
                return await resp.json()
            except Exception:
                print("NO JSON")
            return await resp.text()

# def expand_schema(schema):
#     if "/" in schema:
#         return schema
#     else:
#         return "_/" + schema

    # async def get_schema(self, schemaName):
    #     # schemaName = expand_schema(schemaName)
    #     return await self.fetch({
    #         'path': [self.island, 'schema'],
    #         'params': {'name': schemaName}
    #     })

    # async def get_schemas(self):
    #     return await self.fetch({
    #         'path': [self.island, 'schema']
    #     })

    # async def put_schema(self, schemaName, schema):
    #     schema['name'] = schemaName
    #     return await self.fetch({
    #         'method': 'POST',
    #         'path': [self.island, 'schema'],
    #         'data': schema
    #     })

    # async def put(self, record):
    #     schema = record.get('schema')
    #     schema = expand_schema(schema)
    #     path = [self.island, 'db']
    #     method = 'PUT'
    #     return await self.fetch({
    #         'path': path,
    #         'method': method,
    #         'data': record})

    # async def get(self, schema, id, opts):
    #     return await self.query('records', {'schema': schema, 'id': id},
    #  opts)

    # async def delete(self, record):
    #     path = [self.island, 'db', record['id']]
    #     return await self.fetch({
    #         'path': path,
    #         'method': 'DELETE',
    #         'params': {'schema': record['schema']}
    #     })

    # async def query(self, name, args, opts=dict()):
    #     if self._cacheid:
    #         opts['cacheid'] = self._cacheid

    #     records = await self.fetch({
    #         'path': [self.island, '_query', name],
    #         'method': 'POST',
    #         'data': args,
    #         'params': opts
    #     })

    #     # TODO: is cache already implemented? assign in constructor
    #     # if (self._cacheid):
    #     #     return self._cache.batch(records)

    #     return records
