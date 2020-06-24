
class RecordCache:

    def __init__(self):
        self.records = {}
        self._byId = {}

    def batch(self, records):
        for (i, record) in records.items():
            if record.value:
                self.add(record)
            else:
                records[i] = self.upgrade(record)
        return records

    def add(self, record):
        cacheid = self.cacheid(record)
        self.records[cacheid] = record
        self._byId[record.id] = self._byId.get(record.id) or []
        self._byId[record.id].append(cacheid)

    def get_by_id(self, id):
        if not self._byId.get(id):
            return []
        return self._byId.get(id)

    def has(self, req):
        if not req.key and not req.seq:
            return False
        cacheid = self.cacheid(req)
        return bool(self.records.get(cacheid))

    def upgrade(self, record):
        cacheid = self.cacheid(record)
        if self.records.get(cacheid):
            return dict(self.records[cacheid], **record)
        return record

    # Transform stream function

    def cacheid(self, record):
        return record.key + '@' + record.seq
