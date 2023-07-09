from datashield import DSSession, DSLoginBuilder

class TestClass:

    def setup_method(self, method):
        #url = 'http://localhost:8080'
        url = 'https://opal-demo.obiba.org'
        builder = DSLoginBuilder().add('server1', url, 'dsuser', 'P@ssw0rd').add('server2', url, 'dsuser', 'P@ssw0rd')
        logins = builder.build()
        self.session = DSSession(logins)
        self.session.open()

    def teardown_method(self, method):
        self.session.close()
    
    def test_assign_tables(self):
        self.session.assign_table('df', tables = { 'server1': 'CNSIM.CNSIM1', 'server2': 'CNSIM.CNSIM2' }, asynchronous = False)
        rval = self.session.ls()
        assert len(rval) == 2
        assert 'server1' in rval
        assert rval['server1'] == ['df']
        assert 'server2' in rval
        assert rval['server2'] == ['df']
    
    def test_assign_table(self):
        self.session.assign_table('df', table = 'CNSIM.CNSIM1', asynchronous = False)
        rval = self.session.ls()
        assert len(rval) == 2
        assert 'server1' in rval
        assert rval['server1'] == ['df']
        assert 'server2' in rval
        assert rval['server2'] == ['df']

    def test_assign_resources(self):
        self.session.assign_resource('client', resources = { 'server1': 'RSRC.CNSIM1', 'server2': 'RSRC.CNSIM2' }, asynchronous = False)
        rval = self.session.ls()
        assert len(rval) == 2
        assert 'server1' in rval
        assert rval['server1'] == ['client']
        assert 'server2' in rval
        assert rval['server2'] == ['client']

        self.session.assign_expr('df', 'as.resource.data.frame(client, strict = TRUE)', asynchronous = False)
        rval = self.session.ls()
        assert len(rval) == 2
        assert 'server1' in rval
        assert len(rval['server1']) == 2
        assert 'client' in rval['server1']
        assert 'df' in rval['server1']
    
    def test_assign_resource(self):
        self.session.assign_resource('client', resource = 'RSRC.CNSIM1', asynchronous = False)
        rval = self.session.ls()
        assert len(rval) == 2
        assert 'server1' in rval
        assert rval['server1'] == ['client']
        assert 'server2' in rval
        assert rval['server2'] == ['client']
        