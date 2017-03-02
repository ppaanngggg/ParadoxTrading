from ParadoxTrading.Utils import Fetch

inst = Fetch.fetchInstrument(
    _tradingday='20170123',
    _product='rb',
)
print(inst)
inst = Fetch.fetchInstrument(
    _tradingday='20170123',
    _product='rb',
    _sub_dominant=True
)
print(inst)
inst = Fetch.fetchInstrument(
    _tradingday='20170123',
    _product='rb',
    _product_index=True
)
print(inst)

data = Fetch.fetchIntraDayData(
    _tradingday='20170203',
    _product='rb',
    _product_index=True
)

# data = Fetch.fetchIntraDayData(
#     _tradingday='20170123',
#     _product='rb',
#     _product_index=True,
#     _data_type=Fetch.pgsql_min_dbname,
#     _index='BarTime'
# )
