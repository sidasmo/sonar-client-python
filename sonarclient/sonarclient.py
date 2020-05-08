import aiohttp


class SonarClient:

    def __init__(self, endpoint, island):
        self.endpoint = endpoint
        self.island = island
        self.session = aiohttp.ClientSession()

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
        schemaName = expand_schema(schemaName)
        return await self._request({
            'path': [self.island, 'schema', schemaName]
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
        value = record.get('value')
        id = record.get('id')
        path = [self.island, 'db']
        method = 'PUT'
        # if id:
        #     method = 'PUT'
        #     path.append(id)
        return await self._request({
            "path": path,
            'method': method,
            'data': record})

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
        ) as resp:
            return await resp.text()


def expand_schema(schema):
    if "/" in schema:
        return schema
    else:
        return "_/" + schema
