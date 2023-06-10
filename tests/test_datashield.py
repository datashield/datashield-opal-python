from datashield.interface import DSDriver, DSError
from datashield import DSLoginBuilder
import time


class TestClass:

    @classmethod
    def setup_class(cls):
        driver = DSDriver.load_class('datashield_opal.OpalDriver')
        #url = 'http://localhost:8080'
        url = 'https://opal-demo.obiba.org'
        builder = DSLoginBuilder().add('server1', url, 'dsuser', 'P@ssw0rd')
        logins = builder.build()
        conn = driver.new_connection(logins[0]['name'], logins[0], profile = logins[0]['profile'])
        setattr(cls, 'conn', conn)

    @classmethod
    def teardown_class(cls):
        cls.conn.disconnect()    

    def test_driver(self):
        conn = self.conn
        assert conn.name == 'server1'

    def test_workspaces(self):
        conn = self.conn
        workspaces = conn.list_workspaces()
        assert type(workspaces) == list

    def test_profiles(self):
        conn = self.conn
        profiles = conn.list_profiles()
        assert type(profiles) == dict
        assert 'available' in profiles
        assert 'current' in profiles
        assert type(profiles['available']) == list
        assert profiles['current'] == 'default'

    def test_methods(self):
        conn = self.conn
        methods = conn.list_methods(type='assign')
        assert type(methods) == list
        assert 'name' in methods[0]
        assert 'class' in methods[0]
        assert 'value' in methods[0]
        assert 'pkg' in methods[0]
        assert 'version' in methods[0]
        names = [x['name'] for x in methods]
        assert 'abs' in names
        assert 'vectorDS' in names

        methods = conn.list_methods(type='aggregate')
        assert type(methods) == list
        assert 'name' in methods[0]
        assert 'class' in methods[0]
        assert 'value' in methods[0]
        assert 'pkg' in methods[0]
        assert 'version' in methods[0]
        names = [x['name'] for x in methods]
        assert 'meanDS' in names

    def test_packages(self):
        conn = self.conn
        pkgs = conn.list_packages()
        assert 'pkg' in pkgs[0]
        assert 'version' in pkgs[0]
        names = [x['pkg'] for x in pkgs]
        assert 'resourcer' in names
        assert 'dsBase' in names

    def test_tables(self):
        conn = self.conn
        tables = conn.list_tables()
        assert type(tables) == list
        assert 'CNSIM.CNSIM1' in tables
        assert conn.has_table('CNSIM.CNSIM1') == True

    def test_resources(self):
        conn = self.conn
        resources = conn.list_resources()
        assert type(resources) == list
        assert 'RSRC.CNSIM1' in resources
        assert conn.has_resource('RSRC.CNSIM1') == True

    def test_assign_expr(self):
        conn = self.conn
        res = conn.assign_expr('x', 'c(1, 2, 3)', asynchronous = False)
        assert res.is_completed() == True
        assert res.fetch() is None
        symbols = conn.list_symbols()
        assert type(symbols) == list
        assert len(symbols) == 1
        assert 'x' in symbols
        
        res = conn.assign_expr('y', 'c(1, 2, 3)', asynchronous = True)
        self._do_wait(res)
        assert res.fetch() is None
        symbols = conn.list_symbols()
        assert type(symbols) == list
        assert len(symbols) == 2
        assert 'y' in symbols

        conn.rm_symbol('x')
        conn.rm_symbol('y')
        symbols = conn.list_symbols()
        assert type(symbols) == list
        assert len(symbols) == 0
        
    def test_assign_table(self):
        conn = self.conn
        try:
            res = conn.assign_table('x', 'CNSIM.CNSIM1', asynchronous = False)
            assert res.is_completed() == True
            assert res.fetch() is None
            symbols = conn.list_symbols()
            assert type(symbols) == list
            assert len(symbols) == 1
            assert 'x' in symbols

            res = conn.assign_table('y', 'CNSIM.CNSIM1', asynchronous = True)
            self._do_wait(res)
            assert res.fetch() is None
            symbols = conn.list_symbols()
            assert type(symbols) == list
            assert len(symbols) == 2
            assert 'y' in symbols

            conn.rm_symbol('x')
            conn.rm_symbol('y')
            symbols = conn.list_symbols()
            assert type(symbols) == list
            assert len(symbols) == 0
        except DSError as e:
            print(e.get_error())
            assert False

    def test_assign_resource(self):
        conn = self.conn
        try:
            res = conn.assign_resource('x', 'RSRC.CNSIM1', asynchronous = False)
            assert res.is_completed() == True
            assert res.fetch() is None
            symbols = conn.list_symbols()
            assert type(symbols) == list
            assert len(symbols) == 1
            assert 'x' in symbols

            res = conn.assign_resource('y', 'RSRC.CNSIM1', asynchronous = True)
            self._do_wait(res)
            assert res.fetch() is None
            symbols = conn.list_symbols()
            assert type(symbols) == list
            assert len(symbols) == 2
            assert 'y' in symbols

            conn.rm_symbol('x')
            conn.rm_symbol('y')
            symbols = conn.list_symbols()
            assert type(symbols) == list
            assert len(symbols) == 0

        except DSError as e:
            print(e.get_error())
            assert False
        
    def test_aggregate(self):
        conn = self.conn
        try:
            conn.assign_table('x', 'CNSIM.CNSIM1', asynchronous = False)
            
            # {
            #    'EstimatedMean': 6.1241,
            #    'Nmissing': 341,
            #    'Nvalid': 1822,
            #    'Ntotal': 2163,
            #    'ValidityMessage': 'VALID ANALYSIS'
            # }
            res = conn.aggregate('meanDS(x$LAB_GLUC)', asynchronous = False)
            mean = res.fetch()
            assert type(mean) == dict
            assert 'EstimatedMean' in mean
            assert 'ValidityMessage' in mean

            res = conn.aggregate('meanDS(x$LAB_GLUC)', asynchronous = True)
            self._do_wait(res)
            mean = res.fetch()
            assert type(mean) == dict
            assert 'EstimatedMean' in mean
            assert 'ValidityMessage' in mean

        except DSError as e:
            print(e.get_error())
            assert False

    def test_aggregate_function_not_allowed(self):
        conn = self.conn
        try:
            res = conn.aggregate('myfunc(x$LAB_GLUC)', asynchronous = False)
            assert False
        except DSError as e:
            print(e.get_error())
            assert True
        
        try:
            res = conn.aggregate('myfunc(x$LAB_GLUC)', asynchronous = True)
            self._do_wait(res)
            value = res.fetch()
            print(value)
            assert False
        except DSError as e:
            print(e.get_error())
            assert True


    def _do_wait(self, res, secs = 10):
        count = 0
        while not res.is_completed():
            time.sleep(0.1)
            count = count + 1
            if secs < 0.1 * count:
                raise Exception(f'Result completion timeout: {0.1 * count}s')
