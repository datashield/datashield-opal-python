from datashield.interface import DSDriver
from datashield.utils import DSLoginBuilder

def test_driver():
    driver = DSDriver.load_class('datashield_opal.impl.OpalDriver')
    builder = DSLoginBuilder().add('server1', 'https://opal-demo.obba.org', 'dsuser', 'P@ssw0rd')
    logins = builder.build()
    conn = driver.new_connection(logins[0]['name'], logins[0], profile = logins[0]['profile'])
    assert conn.name == 'server1'