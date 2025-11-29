import instock.lib.database as mdb
print(mdb.executeSqlFetch("SELECT * FROM cn_stock_selection WHERE date = '2025-11-28' AND code = '300102'"))