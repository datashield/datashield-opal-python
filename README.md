# DataSHIELD Opal Python

[![GitHub Actions](https://github.com/datashield/datashield-opal-python/actions/workflows/ci.yml/badge.svg)](https://github.com/datashield/datashield-opal-python/actions)
[![PyPI version](https://img.shields.io/pypi/v/datashield-opal.svg)](https://pypi.org/project/datashield-opal/)

[DataSHIELD Python API](https://github.com/datashield/datashield-python) implementation for [OBiBa/Opal](https://www.obiba.org/pages/products/opal/).

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

# do some DataSHIELD analysis stuff, see examples folder

session.close()
```