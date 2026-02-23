from datashield import DSError, DSLoginBuilder, DSSession
import pytest
import time


class TestClass:
    @classmethod
    def setup_class(cls):
        # url = 'http://localhost:8080'
        url = "https://opal-demo.obiba.org"
        builder = DSLoginBuilder().add("server1", url, "dsuser", "P@ssw0rd")
        logins = builder.build()
        session = DSSession(logins)
        session.open()
        print(session.has_errors())
        print(session.has_connections())
        cls.session = session
        cls.conn = session.conns[0]

    @classmethod
    def teardown_class(cls):
        cls.session.close()

    @pytest.mark.integration
    def test_driver(self):
        conn = self.conn
        assert conn.name == "server1"

    @pytest.mark.integration
    def test_workspaces(self):
        conn = self.conn
        workspaces = conn.list_workspaces()
        assert type(workspaces) is list

    @pytest.mark.integration
    def test_profiles(self):
        conn = self.conn
        profiles = conn.list_profiles()
        assert type(profiles) is dict
        assert "available" in profiles
        assert "current" in profiles
        assert type(profiles["available"]) is list
        assert profiles["current"] == "default"

    @pytest.mark.integration
    def test_methods(self):
        conn = self.conn
        methods = conn.list_methods(type="assign")
        assert type(methods) is list
        assert "name" in methods[0]
        assert "class" in methods[0]
        assert "value" in methods[0]
        assert "pkg" in methods[0]
        assert "version" in methods[0]
        names = [x["name"] for x in methods]
        assert "abs" in names
        assert "vectorDS" in names

        methods = conn.list_methods(type="aggregate")
        assert type(methods) is list
        assert "name" in methods[0]
        assert "class" in methods[0]
        assert "value" in methods[0]
        assert "pkg" in methods[0]
        assert "version" in methods[0]
        names = [x["name"] for x in methods]
        assert "meanDS" in names

    @pytest.mark.integration
    def test_packages(self):
        conn = self.conn
        pkgs = conn.list_packages()
        assert "pkg" in pkgs[0]
        assert "version" in pkgs[0]
        names = [x["pkg"] for x in pkgs]
        print(pkgs)
        assert "resourcer" in names
        assert "dsBase" in names

    @pytest.mark.integration
    def test_tables(self):
        conn = self.conn
        tables = conn.list_tables()
        assert type(tables) is list
        assert "CNSIM.CNSIM1" in tables
        assert conn.has_table("CNSIM.CNSIM1")

    @pytest.mark.integration
    def test_table_variables(self):
        conn = self.conn
        variables = conn.list_table_variables("CNSIM.CNSIM1")
        assert type(variables) is list
        assert "LAB_TSC" in [v.get("name") for v in variables]

    @pytest.mark.integration
    def test_resources(self):
        conn = self.conn
        resources = conn.list_resources()
        assert type(resources) is list
        assert "RSRC.CNSIM1" in resources
        assert conn.has_resource("RSRC.CNSIM1")

    @pytest.mark.integration
    def test_assign_expr(self):
        conn = self.conn
        res = conn.assign_expr("x", "c(1, 2, 3)", asynchronous=False)
        assert res.is_completed()
        assert res.fetch() is None
        symbols = conn.list_symbols()
        assert type(symbols) is list
        assert len(symbols) == 1
        assert "x" in symbols

        res = conn.assign_expr("y", "c(1, 2, 3)", asynchronous=True)
        self._do_wait(res)
        assert res.fetch() is None
        symbols = conn.list_symbols()
        assert type(symbols) is list
        assert len(symbols) == 2
        assert "y" in symbols

        conn.rm_symbol("x")
        conn.rm_symbol("y")
        symbols = conn.list_symbols()
        assert type(symbols) is list
        assert len(symbols) == 0

    @pytest.mark.integration
    def test_assign_table(self):
        conn = self.conn
        try:
            res = conn.assign_table("x", "CNSIM.CNSIM1", asynchronous=False)
            assert res.is_completed()
            assert res.fetch() is None
            symbols = conn.list_symbols()
            assert type(symbols) is list
            assert len(symbols) == 1
            assert "x" in symbols

            res = conn.assign_table("y", "CNSIM.CNSIM1", asynchronous=True)
            self._do_wait(res)
            assert res.fetch() is None
            symbols = conn.list_symbols()
            assert type(symbols) is list
            assert len(symbols) == 2
            assert "y" in symbols

            conn.rm_symbol("x")
            conn.rm_symbol("y")
            symbols = conn.list_symbols()
            assert type(symbols) is list
            assert len(symbols) == 0
        except DSError as e:
            print(e.get_error())
            raise ValueError("Assign table test failed") from e

    @pytest.mark.integration
    def test_assign_resource(self):
        conn = self.conn
        try:
            res = conn.assign_resource("x", "RSRC.CNSIM1", asynchronous=False)
            assert res.is_completed()
            assert res.fetch() is None
            symbols = conn.list_symbols()
            assert type(symbols) is list
            assert len(symbols) == 1
            assert "x" in symbols

            res = conn.assign_resource("y", "RSRC.CNSIM1", asynchronous=True)
            self._do_wait(res)
            assert res.fetch() is None
            symbols = conn.list_symbols()
            assert type(symbols) is list
            assert len(symbols) == 2
            assert "y" in symbols

            conn.rm_symbol("x")
            conn.rm_symbol("y")
            symbols = conn.list_symbols()
            assert type(symbols) is list
            assert len(symbols) == 0

        except DSError as e:
            print(e.get_error())
            raise ValueError("Assign resource test failed") from e

    @pytest.mark.integration
    def test_aggregate(self):
        conn = self.conn
        try:
            conn.assign_table("x", "CNSIM.CNSIM1", asynchronous=False)

            # {
            #    'EstimatedMean': 6.1241,
            #    'Nmissing': 341,
            #    'Nvalid': 1822,
            #    'Ntotal': 2163,
            #    'ValidityMessage': 'VALID ANALYSIS'
            # }
            res = conn.aggregate("meanDS(x$LAB_GLUC)", asynchronous=False)
            mean = res.fetch()
            assert type(mean) is dict
            assert "EstimatedMean" in mean
            assert "ValidityMessage" in mean

            res = conn.aggregate("meanDS(x$LAB_GLUC)", asynchronous=True)
            self._do_wait(res)
            mean = res.fetch()
            assert type(mean) is dict
            assert "EstimatedMean" in mean
            assert "ValidityMessage" in mean

        except DSError as e:
            print(e.get_error())
            raise ValueError("Aggregate test failed") from e

    @pytest.mark.integration
    def test_aggregate_function_not_allowed(self):
        conn = self.conn
        with pytest.raises(DSError) as exc_info:
            res = conn.aggregate("myfunc(x$LAB_GLUC)", asynchronous=False)
        print(exc_info.value.get_error())

        with pytest.raises(DSError) as exc_info:
            res = conn.aggregate("myfunc(x$LAB_GLUC)", asynchronous=True)
            self._do_wait(res)
            value = res.fetch()
            print(value)
        print(exc_info.value.get_error())

    def _do_wait(self, res, secs=10):
        count = 0
        while not res.is_completed():
            time.sleep(0.1)
            count = count + 1
            if secs < 0.1 * count:
                raise Exception(f"Result completion timeout: {0.1 * count}s")
