from ParadoxTrading.Fetch import FetchGuoJinTick
from ParadoxTrading.Utils import DataStruct
from datetime import datetime

fetcher = FetchGuoJinTick()
# adjust it by yourself
# fetcher.psql_host = '192.168.4.102'
# fetcher.psql_user = 'ubuntu'
# fetcher.mongo_host = '192.168.4.102'

# this is a powerful DataStruct
data:DataStruct = fetcher.fetchData('20160506', 'rb1609')

# print table like data
print(data)

# get len of data
print(len(data))

# get all column names of data
print(data.getColumnNames())
# you can skip name of index
print(data.getColumnNames(_include_index_name=False))
# and this is name of index
print(data.index_name)

# get values of index
print(data.index())
# get values of one column
print(data['askprice'])
print(data.getColumn('askprice'))

# you can get rows by number, [start, end)
print(data.iloc[:10])
# get rows by index value, [start, end) ! not same as pd.DataFrame
print(data.loc[:data.index()[10]])
# when slice one row,
print(data[data.index()[10]])
# return None if not exists
print(data[datetime(2016, 5, 5, 20)])

# merge two datastruct, and merge will keep sort of index
a = data.iloc[:5]
b = data.iloc[5:10]
print(a)
print(b)
a.merge(b)
print(a)

# turn datastruct to list of row
v,k = a.toRows(['askprice', 'askvolume'])
print(v)
print(k)
# turn datastruct to list of dict
print(a.toDicts())

# iter each line in data
for d in data:
    print(d)
