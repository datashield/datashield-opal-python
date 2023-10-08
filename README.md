# datashield-opal-python

DataSHIELD Client Interface implementation for OBiBa/Opal

## Installation

```
pip install datashield-opal
```

## Usage

```
from datashield import DSSession, DSLoginBuilder, DSError

url = 'https://opal-demo.obiba.org'
builder = DSLoginBuilder().add('server1', url, 'dsuser', 'P@ssw0rd').add('server2', url, 'dsuser', 'P@ssw0rd')
logins = builder.build()

session = DSSession(logins)
session.open()

# do some DataSHIELD analysis stuff

session.close()
```