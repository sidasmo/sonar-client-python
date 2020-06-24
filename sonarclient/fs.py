from .constants import HYPERDRIVE_SCHEME
import json
import parse_dat_url


class Fs:

    def __init__(self, collection):
        self.endpoint = collection.endpoint + '/fs'
        self.collection = collection

    async def fetch(self, path, opts=dict()):
        path = self.resolve_url(path)
        return self.collection.fetch(path, opts)

    def resolve_url(self, path):
        # Support hyper:// URLs
        if path.startswith(HYPERDRIVE_SCHEME):
            url = parse_dat_url(path)
            path = url['host'] + url['path']        

        # Support no other absolute URLs for now
        elif path.find('://') != -1:
            raise Exception('Invalid path: Only hyper://' +
                            ' URLs or paths are supported')

        # Assume it's a path to a file in the collection fs,
        # will fail if not found
        if not path.startswith('/'):
            path = '/' + path

        return self.endpoint + path

    async def list_drives(self):
        return self.collection.fetch('/fs-info')

    async def readdir(self, path, opts=dict()):
        this = self
        path = path or '/'
        if path == '/':
            drives = await self.list_drives()
            return list(map(lambda drive: {
                'name': drive['alias'],
                'link': '',
                'resource': None,
                'length': None,
                'directory': True},
                drives))

        if (len(path) > 2 and path[0] == '/'):
            path = path[1:]
        alias = path.split('/')[0]

        files = await self.fetch(path)

        if (files and len(files)):
            files = list(map(lambda file: file.update({
                'link': make_link(file),
                'resource': get_resource(file)}),
                files))

        def make_link(file):
            return this.endpoint+"/" + this.collection['name'] + "/fs/" + alias + "\
                /" + file['path']

        def get_resource(file):
            if not file or not file.get('metadata') or not file.get('metadata').get('sonar.id'):
                return None
            return file['metadata']

    async def write_file(self, path, file, opts=dict()):
        requestType = opts.get('requestType') or 'buffer'
        params = dict()
        if opts.get('metadata'):
            params['metadata'] = json.dumps(opts['metadata'])

        return self.fetch(path, {
            'method': 'PUT',
            'body': file,
            'params': params,
            'responseType': 'text',
            'requestType': requestType,
            # 'binary': True,
            'onUploadProgress': opts.get('onUploadProgress')
        })

    async def read_file(self, path, opts=dict()):
        opts['responseType'] = opts.get('responseType') or 'buffer'
        opts['requestType'] = opts.get('requestType') or 'buffer'
        return self.fetch(path, opts)

    async def create_read_stream(self, path, opts=dict()):
        opts['responseType'] = 'stream'
        return self.read_file(path, opts)

    async def stat_file(self, path):
        return self.fetch(path)
