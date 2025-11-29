import instock.lib.database as mdb
print(mdb.executeSqlCount("SELECT COUNT(*) FROM cn_stock_selection WHERE date = '2025-11-28'"))