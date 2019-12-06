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
        schema = record.get('schema')
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
        # if aio_opts.get('method').lower() == 'put':
        #     async with self.session.put(url, json=data as resp:
        #         return await resp.text()
        # elif aio_opts.get('method').lower() == 'get':
        #     async with self.session.get(url) as resp:
        #         return await resp.text()
        # elif aio_opts.get('method').lower() == 'post':
        #     async with self.session.post(url, json=data) as resp:
        #         return await resp.text()


# async def main():
#     print("short example: create a island and get a key:")
#     client = SonarClient('http://localhost:9191/api', 'default')
#     resp = await client.create_island('example_island')
#     key = json.loads(resp).get('key')
#     print(key)
#     from exampleschema import get_doc, get_telegram_media_schema
#     schema = get_telegram_media_schema()
#     resp = await client.put_schema("example_schema", schema)
#     schema = json.loads(resp)
#     schema_name = schema.get('schema')
#     print(schema_name)
#     record_id = None
#     print("put record to sonar with example_schema, get record_id: ")
#     record = schema_name, record_id, get_doc()
#     record_id = json.loads(await client.put(record))
#     print(record_id.get('id'))
#     await client.session.close()
# asyncio.run(main())
