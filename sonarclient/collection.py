from .schema import Schema
from .record_cache import RecordCache
from .fs import Fs
from .resources import Resources
import json


class Collection:

    def __init__(self, client, name):
        self.endpoint = client.endpoint + '/' + name
        self._client = client
        self._info = dict()
        self._name = name
        self._cache = RecordCache()

        self.schema = Schema()
        self.fs = Fs(self)
        self.resources = Resources(self)

    @property
    def name(self):
        if self._info:
            return self._info['name']
        return self._name

    @property
    def key(self):
        return self._info and self._info['key']

    @property
    def info(self):
        return self._info

    async def open(self):
        print("OPENING COLLECTION")
        info = await self.fetch('/')
        print("RESULT FROM FETCH /: INFO: ", info)
        print("TYPE OF INFO: ", type(info))
        info = json.loads(info)
        self._info = info
        schemas = await self.fetch('/schema')
        schemas = json.loads(schemas)
        print("RESULT OF FETCH /SCHEMA: ", schemas)
        self.schema.add(schemas)

    async def add_feed(self, key, info=dict()):
        return await self.fetch('/source/' + key, {
            'method': 'PUT',
            'body': info
        })

    async def query(self, name, args, opts):
        # TODO: implement cacheid stuff
        # if self._cacheid:
        #     opts['cacheid'] = self._cacheid

        records = await self.fetch('/query/' + name, {
            'method': 'POST',
            'body': args,
            'params': opts
        })

        # TODO: implement cacheid stuff
        # if self._cacheid:
        #     return self._cache.batch(records)
       
        return records

    async def put(self, record):
        return await self.fetch('/db', {
            'method': 'PUT',
            'body': record
        })

    async def get(self, req, opts):
        # TODO: see if this works as it is still commented out in collection.js
        # if self._cache.has(req):
        #     return self._cache.get(req)
        return await self.query('records', req, opts)

    async def delete(self, record):
        return await self.fetch('/db' + record.get('id'), {
            'method': 'DELETE',
            'params': {'schema': record.get('schema')}
        })

    async def put_schema(self, schema):
        return await self.fetch('/schema', {
            'method': 'POST',
            'body': schema
        })

    async def sync(self):
        return await self.fetch('/sync')

    async def fetch(self, path, opts=dict()):
        if not opts.get('endpoint'):
            opts['endpoint'] = self.endpoint
        opts['path'] = path
        return await self._client._request(opts)
