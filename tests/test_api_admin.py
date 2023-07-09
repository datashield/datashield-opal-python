from datashield import DSSession, DSLoginBuilder

class TestClass:

    @classmethod
    def setup_class(cls):
        #url = 'http://localhost:8080'
        url = 'https://opal-demo.obiba.org'
        builder = DSLoginBuilder().add('server1', url, 'dsuser', 'P@ssw0rd').add('server2', url, 'dsuser', 'P@ssw0rd')
        logins = builder.build()
        session = DSSession(logins)
        session.open()
        setattr(cls, 'session', session)

    @classmethod
    def teardown_class(cls):
        cls.session.close()
    
    def test_profiles(self):
        rval = self.session.profiles()
        assert 'server1' in rval
        assert rval['server1']['current'] == 'default'
        assert len(rval['server1']['available']) > 0

    def test_packages(self):
        rval = self.session.packages()
        print(rval)
        assert True
    
    def test_methods(self):
        rval = self.session.methods()
        print(rval)
        assert True

    def test_tables(self):
        rval = self.session.tables()
        print(rval)
        assert True

    def test_resources(self):
        rval = self.session.resources()
        print(rval)
        assert True

    def test_workspaces(self):
        rval = self.session.workspaces()
        print(rval)
        assert True
