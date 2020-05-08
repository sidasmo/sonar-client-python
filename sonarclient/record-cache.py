
class RecordCache:

    def __init__(self):
        self.records = {}
        self._byId = {}

    def batch(self, records):
        for [i, record] in record.entries():
            if record.value:
                self.add(record)
            else:
                records[i] = self.upgrade(record)
        return records

    def add(self, record):
        cacheId = self.cacheId(record)
        self.records[cacheId] = record
        self._byId[record.id] = self._byId[record.id] or []
        self._byId[record.id].push(record)

    def upgrade(self, record):
        cacheId = self.cacheId(record)
        if self.records[cacheId]:
            return {self.records[cacheId], record}
        return record

    def cacheId(self, record):
        return record.lseq
