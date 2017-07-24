from ParadoxTrading.Fetch import FetchExchangeMarketIndex
from ParadoxTrading.Utils import DataStruct

fetcher = FetchExchangeMarketIndex()
# adjust it by yourself
# fetcher.psql_host = '192.168.4.103'
# fetcher.psql_user = 'ubuntu'
# fetcher.mongo_host = '192.168.4.103'

# this is a powerful DataStruct
data: DataStruct = fetcher.fetchDayData('20100101', '20170101', 'rb')

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
print(data['closeprice'])
print(data.getColumn('closeprice'))

# you can get rows by number, [start, end)
print(data.iloc[:10])
# get rows by index value, [start, end) ! not same as pd.DataFrame
print(data.loc[:data.index()[10]])
# when slice one row,
print(data.loc[data.index()[10]])
# return None if not exists
print(data.loc['20180101'])

# merge two datastruct, and merge will keep sort of index
a = data.iloc[:5]
b = data.iloc[5:10]
print(a)
print(b)
a.merge(b)
print(a)

# turn datastruct to list of row
v, k = a.toRows(['highprice', 'lowprice'])
print(v)
print(k)
# turn datastruct to list of dict
print(a.toDicts())

# iter each line in data
for d in a:
    print(d)

# change index
print(a.changeIndex('closeprice'))

# clone a new datastruct
print(a.clone())
# clone a new datastruct and select some columns
print(a.clone(['closeprice', 'volume']))

print(a)
a.toHDF5('hdf_sample.hdf5')
print(DataStruct.fromHDF5('hdf_sample.hdf5'))

df = a.toPandas()
print(df)

print(DataStruct.fromPandas(df))