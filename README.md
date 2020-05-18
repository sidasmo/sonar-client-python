# sonar-client-python
## Work in progress
sonar-client-python provides a python api to speak with sonar

## Tests 
you can run tests with:
```python tests/basic.py``` 
with default endpoint: 'http://localhost:9191/api' and Island: 'default'
or you can run the tests with specified Endpoint and Island:
```python tests/basic.py ENDPOINT ISLAND```

## Functions to implement

- [ ] Close
- [ ] isWritable
- [ ] info
- [ ] createIsland
- [ ] focusIsland
- [ ] _getIslandInfo
- [ ] getCurrentIsland
- [X] getSchemas
- [X] getSchema
- [ ] putSchema
- [ ] putSource
- [ ] _localdriveKey
- [ ] writeResourceFile
- [ ]  readResourceFile
- [ ] createResource
- [ ] parseHyperdriveUrl
- [ ] resourceHttpUrl
- [ ] get
- [X] put
- [X] del(ete)
- [ ] sync
- [ ] query
- [ ] search
- [ ] updateIsland
- [ ] getDrives
- [ ] readdir
- [ ] makeLink
- [ ] getResource
- [ ] writeFile
- [ ] readFile
- [ ] statFile
- [ ] pullSubscription
- [ ] ackSubscription
- [ ] _url
- [ ] fileUrl
- [X] _request
- [ ] initCommandClient
- [ ] callCommand
- [ ] createQueryStream
- [ ] createSubscriptionStream
