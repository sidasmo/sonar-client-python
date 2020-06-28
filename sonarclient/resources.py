import sys
from .constants import HYPERDRIVE_SCHEME, METADATA_ID, SCHEMA_RESOURCE


class Resources:
    def __init__(self, collection):
        self.collection = collection

    async def _local_drive_key(self):
        if self._local_drive:
            return self._localdrive
        else:
            drives = await self.collection.fs.list_drives()
            writable_drives = list(filter(lambda f: f.writable, drives))
            if not writable_drives:
                raise Exception("No writable drive")
            else:
                self._local = writable_drives[0].key
                return self._local_drive

    async def write_file(self, record, file, opts={}):
        url = get_content_url(record)
        if not url:
            raise Exception("record has no file url")
        if not opts.metadata:
            opts.metadata = {}
        opts.metadata[METADATA_ID] = record.id
        return self.collection.fs.write_file(url, file, opts)

    async def read_file(self, record, opts={}):
        url = get_content_url(record)
        if not url:
            raise Exception("record has no file url")
        return self.collection.fs.read_file(url, opts)

    async def create(self, value, opts={}):
        filename, prefix = value
        filepath = ""
        if not filename:
            raise Exception("filename is required")
        if "/" in filename:
            raise Exception("invalid filename")
        if opts.scoped:
            raise Exception("scoped option is not supported")
        if prefix:
            filepath = [prefix, filepath].join("/")
        else:
            filepath = filename
        drive_key = await self._local_drive_key()
        content_url = create_hyperdrive_url(drive_key, filepath)

        try:
            existing = await self.collection.fs.stat_file(content_url)
        except Exception:
            print("Error: ", sys.exc_info()[0])

        if existing:
            id = existing.metadata[METADATA_ID]
            if not id:
                if not opts.force:
                    raise Exception("file exists and has no resource attached.\
                                    set fore to overwrite.")
                else:
                    # TODO: Preserve fields from an old resource?
                    # old_resource = await this.get(
                    # { id: existing.metadata[METADATA_ID] })
                    if not opts.update:
                        raise Exception("file exists, with resource ${id}.\
                                        set update to overwrite.")

        id = id or opts.id

        res = await self.collection.put({
            "schema": SCHEMA_RESOURCE,
            "id": id,
            "value": {
                *value,
                content_url,
                filename
            }
        })

        records = await self.collection.get({
            "id": res.id,
            "schema": SCHEMA_RESOURCE},
            {"waitForSync": True})
        if not records:
            raise Exception("Error loading created resource")
        return records[0]

    def resolve_file_url(self, record):
        url = get_content_url(record)
        return self.collection.fs.resolve_url(url)


def create_hyperdrive_url(key, path):
    return HYPERDRIVE_SCHEME + key + path


def get_content_url(record):
    if record.schema != SCHEMA_RESOURCE:
        return None
    if not record.value.contentUrl:
        return None
    return record.value.contentUrl
