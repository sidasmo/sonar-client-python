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
            if type(self._info) is not str:
                return self._info.get("name")
        return self._name

    @property
    def key(self):
        if type(self._info) is not str:
            return self._info and self._info['key']
        return self._info

    @property
    def info(self):
        return self._info

    async def open(self):
        info = await self.fetch('/')
        self._info = info
        schemas = await self.fetch('/schema')
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
        return await self.fetch('/db/' + record.get('id'), {
            'method': 'DELETE',
            'params': {'schema': record.get('schema')},
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
        print('PATH:', path, 'OPTS: ', opts)
        return await self._client.fetch(path, opts)
