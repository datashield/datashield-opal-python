"""
DataSHIELD Interface implementation for Opal.
"""

from argparse import Namespace
from obiba_opal.core import OpalClient, UriBuilder
from datashield.interface import DSDriver, DSConnection, DSResult

class OpalConnection(DSConnection):

  def __init__(self, name: str, loginInfo: OpalClient.LoginInfo, profile: str = 'default', restore: str = None):
    self.name = name
    self.client = OpalClient.build(loginInfo)
    self.subject = None
    self.profile = profile
    self.restore = restore
    self.session = None
    self.verbose = False

  #
  # Content listing
  #

  def list_tables(self) -> list:
    response = self._get('/datasources').fail_on_error().send()
    datasources = response.from_json()
    names = list()
    for ds in datasources:
      if 'table' in ds:
        for table in ds['table']:
          names.append(ds['name'] + '.' + table)
    return names

  def has_table(self, name: str) -> bool:
    parts = name.split('.')
    response = self._get(UriBuilder(['datasource', parts[0], 'table', parts[1]]).build()).send()
    return response.code == 200
  
  def list_resources(self) -> list:
    response = self._get('/projects').fail_on_error().send()
    projects = response.from_json()
    names = list()
    for project in projects:
      response = self._get(UriBuilder(['project', project['name'], 'resources']).build()).fail_on_error().send()
      resources = response.from_json()
      for resource in resources:
          names.append(project['name'] + '.' + resource['name'])
    return names

  def has_resource(self, name: str) -> bool:
    parts = name.split('.')
    response = self._get(UriBuilder(['project', parts[0], 'resource', parts[1]]).build()).send()
    return response.code == 200

  #
  # Assign
  #

  def assign_table(self, symbol: str, table: str, variables: list = None, 
                    missings: bool = False, identifiers: str = None, 
                    id_name: str = None, asynchronous: bool = True) -> DSResult:
    builder = UriBuilder(['datashield', 'session', self._get_session_id(), 'symbol', symbol, 'table', table]).query('missings', missings).query('async', asynchronous)
    if variables is not None:
      builder.query('variables','name.any("%s")' % '","'.join(variables))
    if identifiers is not None:
      builder.query('identifiers', identifiers)
    if id_name is not None:
      builder.query('id', id_name)
    response = self._put(builder.build()).fail_on_error().send()
    return OpalResult(self, rid = response.content) if asynchronous else OpalResult(self, result = response.from_json())

  def assign_resource(self, symbol: str, resource: str, asynchronous: bool = True) -> DSResult:
    builder = UriBuilder(['datashield', 'session', self._get_session_id(), 'symbol', symbol, 'resource', resource]).query('async', asynchronous)
    response = self._put(builder.build()).fail_on_error().send()
    return OpalResult(self, rid = response.content) if asynchronous else OpalResult(self, result = response.from_json())

  def assign_expr(self, symbol: str, expr: str, asynchronous: bool = True) -> DSResult:
    builder = UriBuilder(['datashield', 'session', self._get_session_id(), 'symbol', symbol]).query('async', asynchronous)
    response = self._put(builder.build()).content_type_rscript().content(expr).fail_on_error().send()
    return OpalResult(self, rid = response.content) if asynchronous else OpalResult(self, result = response.from_json())

  #
  # Aggregate
  #

  def aggregate(self, expr: str, asynchronous: bool = True) -> DSResult:
    builder = UriBuilder(['datashield', 'session', self._get_session_id(), 'aggregate']).query('async', asynchronous)
    response = self._post(builder.build()).content_type_rscript().content(expr).fail_on_error().send()
    return OpalResult(self, rid = response.content) if asynchronous else OpalResult(self, result = response.from_json())

  #
  # Symbols
  #

  def list_symbols(self) -> list:
    builder = UriBuilder(['datashield', 'session', self._get_session_id(), 'symbols'])
    response = self._get(builder.build()).fail_on_error().send()
    rval = response.from_json()
    if type(rval) == str:
      rval = [rval]
    return rval

  def rm_symbol(self, name: str) -> None:
    builder = UriBuilder(['datashield', 'session', self._get_session_id(), 'symbol', name])
    self._delete(builder.build()).send()

  #
  # DataSHIELD config
  #

  def list_profiles(self) -> list:
    builder = UriBuilder(['datashield', 'profiles'])
    response = self._get(builder.build()).send()
    profiles = response.from_json()
    names = [x['name'] for x in profiles if x['enabled']]
    return {
      'available': names,
      'current': self.profile
    }
  
  def list_methods(self, type: str = 'aggregate') -> list:
    builder = UriBuilder(['datashield', 'env', type, 'methods']).query('profile', self.profile)
    response = self._get(builder.build()).send()
    methods = response.from_json()
    def format(x):
      item = {
        'name': x['name']
      }
      if 'DataShield.RFunctionDataShieldMethodDto.method' in x:
        method = x['DataShield.RFunctionDataShieldMethodDto.method']
        item['class'] = 'func' if 'func' in method else 'script'
        item['value'] = method['func'] if 'func' in method else method['script']
        item['pkg'] = method['rPackage'] if 'rPackage' in method else None
        item['version'] = method['version'] if 'version' in method else None
      return item
    
    methods = [format(x) for x in methods]
    return methods
  
  def list_packages(self) -> list:
    aggregate = self.list_methods(type = 'aggregate')
    assign = self.list_methods(type = 'assign')

    def format_method(x):
      return '%s:%s' % (x['pkg'], x['version'])

    aggregate = [format_method(x) for x in aggregate if 'pkg' in x]
    assign = [format_method(x) for x in assign if 'pkg' in x]

    # unique values
    pkgs = list(set(aggregate + assign))

    def format_pkg(x):
      parts = x.split(':')
      return {
        'pkg': parts[0],
        'version': parts[1]
      }
    
    return [format_pkg(x) for x in pkgs]

  #
  # Workspaces
  #

  def list_workspaces(self) -> list:
    builder = UriBuilder(['service', 'r', 'workspaces']).query('context', 'DataSHIELD').query('user', self._get_subject()['principal'])
    response = self._get(builder.build()).send()
    return response.from_json()
    
  def save_workspace(self, name: str) -> list:
    builder = UriBuilder(['datashield', 'session', self._get_session_id(), 'workspaces']).query('save', name)
    self._post(builder.build()).send()
    
  def restore_workspace(self, name: str) -> list:
    builder = UriBuilder(['datashield', 'session', self._get_session_id(), 'workspace', name])
    self._put(builder.build()).send()
  
  def rm_workspace(self, name: str) -> list:
    builder = UriBuilder(['service', 'r', 'workspaces']).query('context', 'DataSHIELD').query('user', self._get_subject()['principal']).query('name', name)
    self._delete(builder.build()).send()
    
  #
  # Utils
  #

  def is_async(self) -> dict:
    return {
      'aggregate': True,
      'assign_table': True, 
      'assign_resource': True,
      'assign_expr': True
    }

  def keep_alive(self) -> bool:
    try:
      self.list_symbols()
    except Exception as e:
            pass

  def disconnect(self) -> None:
    """
    Close DataSHIELD session, and then Opal session.
    """
    if self.session is not None:
      builder = UriBuilder(['datashield', 'session', self._get_session_id()])
      self._delete(builder.build()).send()
      self.session = None
    self.client.close()

  #
  # Private methods
  #

  def _get_subject(self):
    if self.subject is None:
      builder = UriBuilder(['system', 'subject-profile', '_current'])
      response = self._get(builder.build()).fail_on_error().send()
      self.subject = response.from_json()
    return self.subject

  def _get_session_id(self) -> str:
    return self._get_session()['id']
    
  def _get_session(self):
    if self.session is None:
      builder = UriBuilder(['datashield', 'sessions'])
      if self.profile is not None:
        builder.query('profile', self.profile)
      if self.restore is not None:
        builder.query('restore', self.restore)
      response = self._post(builder.build()).send()
      if response.code == 201:
        self.session = response.from_json()
      else:
        raise ValueError('DataSHIELD session creation failed: ' + response.code)
    return self.session

  def _get(self, ws):
    request = self.client.new_request()
    if self.verbose:
      request.verbose()
    return request.accept_json().get().resource(ws)

  def _post(self, ws):
    request = self.client.new_request()
    if self.verbose:
      request.verbose()
    return request.accept_json().post().resource(ws)
  
  def _put(self, ws):
    request = self.client.new_request()
    if self.verbose:
      request.verbose()
    return request.accept_json().put().resource(ws)

  def _delete(self, ws):
    request = self.client.new_request()
    if self.verbose:
      request.verbose()
    return request.accept_json().delete().resource(ws)

class OpalDriver(DSDriver):
    
  @classmethod
  def new_connection(cls, name: str, args: dict, profile: str = 'default', restore: str = None) -> DSConnection:
    namedArgs = Namespace(opal = args['url'], user = args['user'], password = args['password'], token = args['token'], otp = None)
    loginInfo = OpalClient.LoginInfo.parse(namedArgs)
    return OpalConnection(name, loginInfo, profile, restore)

class OpalResult(DSResult):

  def __init__(self, conn: OpalConnection, rid: str = None, result: any = None):
    self.conn = conn
    self.rid = rid
    self.result = result

  def is_completed(self) -> bool:
    if self.rid is None:
      return True
    else:
      # check if R command is completed
      builder = UriBuilder(['datashield', 'session', self.conn._get_session_id(), 'command', self.rid]).query('wait', False)
      response = self.conn._get(builder.build()).send()
      cmd = response.from_json()
      return (cmd['status'] == 'COMPLETED' or cmd['status'] == 'FAILED') if 'status' in cmd else False
    
  def fetch(self) -> any:
    if self.rid is None:
      print(self.result)
      return self.result
    else:
      # get the result of R command by its id
      builder = UriBuilder(['datashield', 'session', self.conn._get_session_id(), 'command', self.rid]).query('wait', True)
      response = self.conn._get(builder.build()).send()
      cmd = response.from_json()
      if 'status' in cmd and cmd['status'] == 'ERROR':
        msg = cmd['error'] if 'error' in cmd else '<no message>'
        raise ValueError('Command %s failed on %s: %s' % (self.rid, self.conn.name, msg))
      builder = UriBuilder(['datashield', 'session', self.conn._get_session_id(), 'command', self.rid, 'result'])
      response = self.conn._get(builder.build()).send()
      return response.from_json()