from schema import Schema
from record-cache import RecordCache
from fs import Fs
from resources import Resources


class Collection:

    def __init__(self, client, name):
        self.endpoint = client.endpoint + '/' + name
        self._client = client
        self._info = dict()
        self._name = name
        self._cache = new RecordCache

        self.schema = new Schema()
        self.fs = new Fs(self)
        self.resources = new Resources(self)

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
        self._info = info
        schemas = await self.fetch('/schema')
        self.schema.add(schemas)

    async def add_feed(self, key, info=dict()):
        return self.fetch('/source/' + key, {
            'method': 'PUT',
            'body': info
        })

    async def query(self, name, args, opts):
        # Check if we run into problems here if _cacheid doesn't exist yet
        if self._cacheid:
            opts['cacheid'] = self._cacheid

        records = await self.fetch('/query/' + name, {
            'method': 'POST',
            'body': args,
            'params': opts
        })

        if self._cacheid:
            return self._cache.batch(records)
       
        return records

    async def put(self, record):
        return self.fetch('/db', {
            'method': 'PUT',
            'body': record
        })

    async def get(self, req, opts):
        # TODO: see if this works as it is still commented out in collection.js
        # if self._cache.has(req):
        #     return self._cache.get(req)
        return self.query('records', req, opts)

    async def del(self, record):
        return self.fetch('/db' + record.get('id'), {
            'method': 'DELETE',
            'params': {'schema': record.get('schema')}
        })

    async def put_schema(self, schema):
        return self.fetch('/schema', {
            'method': 'POST',
            'body': schema
        })

    async def sync(self):
        return self.fetch('/sync')

    async def fetch(self, path, opts=dict()):
        if not opts.get('endpoint'):
            opts['endpoint'] = self.endpoint
        return self._client._request(path, opts)
