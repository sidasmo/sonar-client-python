class Schema:
    def __init__(self):
        self._schemas = dict()

    def add(self, schemas):
        self._schemas.update(schemas)

    def get_type(self, name):
        return self._schemas[name]

    def list_types(self):
        return self._schemas
