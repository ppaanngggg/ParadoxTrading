import gzip
import pickle
import typing

import h5py
import psycopg2
import pymongo
from ParadoxTrading.Utils.DataStruct import DataStruct
from pymongo import MongoClient
from redis import StrictRedis


class Fetch:

    mongo_host = 'localhost'
    mongo_prod_db = 'FutureProd'
    mongo_inst_db = 'FutureInst'

    pgsql_host = 'localhost'
    pgsql_dbname = 'FutureData'
    pgsql_user = 'pang'
    pgsql_password = ''

    cache_path = 'cache.hdf5'

    def productList() -> list:
        client = MongoClient(host=Fetch.mongo_host)
        db = client[Fetch.mongo_prod_db]
        ret = db.collection_names()
        client.close()

        return ret

    def productIsTrade(_product: str, _tradingday: str) -> bool:
        """
        check whether product is traded on tradingday

        Args:
            _product (str): which product.
            _tradingday (str): which day.

        Returns:
            bool: True for traded, False for not
        """
        client = MongoClient(host=Fetch.mongo_host)
        db = client[Fetch.mongo_prod_db]
        coll = db[_product]
        count = coll.count({'TradingDay': _tradingday})
        client.close()

        return count > 0

    def productLastTradingDay(_product: str, _tradingday: str) -> str:
        """
        get the first day less then _tradingday of _product

        Args:
            _product (str): which product.
            _tradingday (str): which day.

        Returns:
            str: if None, it means nothing found
        """
        client = MongoClient(host=Fetch.mongo_host)
        db = client[Fetch.mongo_prod_db]
        coll = db[_product]
        d = coll.find_one(
            {'TradingDay': {'$lt': _tradingday}},
            sort=[('TradingDay', pymongo.DESCENDING)]
        )
        client.close()

        return d['TradingDay'] if d is not None else None

    def productNextTradingDay(_product: str, _tradingday: str) -> str:
        """
        get the first day greater then _tradingday of _product

        Args:
            _product (str): which product.
            _tradingday (str): which day.

        Returns:
            str: if None, it means nothing found
        """
        client = MongoClient(host=Fetch.mongo_host)
        db = client[Fetch.mongo_prod_db]
        coll = db[_product]
        d = coll.find_one(
            {'TradingDay': {'$gt': _tradingday}},
            sort=[('TradingDay', pymongo.ASCENDING)]
        )
        client.close()

        return d['TradingDay'] if d is not None else None

    def instrumentIsTrade(_instrument: str, _tradingday: str) -> bool:
        """
        check whether instrument is traded on tradingday

        Args:
            _instrument (str): which instrument.
            _tradingday (str): which day.

        Returns:
            bool: True for traded, False for not
        """
        client = MongoClient(host=Fetch.mongo_host)
        db = client[Fetch.mongo_inst_db]
        coll = db[_instrument]
        count = coll.count({'TradingDay': _tradingday})
        client.close()

        return count > 0

    def instrumentLastTradingDay(_instrument: str, _tradingday: str) -> str:
        """
        get the first day less then _tradingday of _instrument

        Args:
            _instrument (str): which instrument.
            _tradingday (str): which day.

        Returns:
            str: if None, it means nothing found
        """
        client = MongoClient(host=Fetch.mongo_host)
        db = client[Fetch.mongo_inst_db]
        coll = db[_instrument]
        d = coll.find_one(
            {'TradingDay': {'$lt': _tradingday}},
            sort=[('TradingDay', pymongo.DESCENDING)]
        )
        client.close()

        return d['TradingDay'] if d is not None else None

    def instrumentNextTradingDay(_instrument: str, _tradingday: str) -> str:
        """
        get the first day greater then _tradingday of _instrument

        Args:
            _instrument (str): which instrument.
            _tradingday (str): which day.

        Returns:
            str: if None, it means nothing found
        """
        client = MongoClient(host=Fetch.mongo_host)
        db = client[Fetch.mongo_inst_db]
        coll = db[_instrument]
        d = coll.find_one(
            {'TradingDay': {'$gt': _tradingday}},
            sort=[('TradingDay', pymongo.ASCENDING)]
        )
        client.close()

        return d['TradingDay'] if d is not None else None

    def fetchTradeInstrument(_product: str, _tradingday: str) -> list:
        """
        fetch all traded insts of one product on tradingday

        Args:
            _product (str): which product.
            _tradingday (str): which day.

        Returns:
            list: list of str. if len() == 0, then no traded inst
        """
        client = MongoClient(host=Fetch.mongo_host)
        db = client[Fetch.mongo_prod_db]
        coll = db[_product]
        data = coll.find_one({'TradingDay': _tradingday})
        ret = []
        if data is not None:
            ret = data['InstrumentList']
        client.close()

        return ret

    def fetchDominant(_product: str, _tradingday: str) -> str:
        """
        fetch dominant instrument of one product on tradingday

        Args:
            _product (str): which product.
            _tradingday (str): which day.

        Returns:
            str: dominant instrument. if None, then no traded inst
        """
        client = MongoClient(host=Fetch.mongo_host)
        db = client[Fetch.mongo_prod_db]
        coll = db[_product]
        data = coll.find_one({'TradingDay': _tradingday})
        ret = None
        if data is not None:
            ret = data['Dominant']
        client.close()

        return ret

    def fetchSubDominant(_product: str, _tradingday: str) -> str:
        """
        fetch sub dominant instrument of one product on tradingday

        Args:
            _product (str): which product.
            _tradingday (str): which day.

        Returns:
            str: sub dominant instrument. if None, then no traded inst
        """
        client = MongoClient(host=Fetch.mongo_host)
        db = client[Fetch.mongo_prod_db]
        coll = db[_product]
        data = coll.find_one({'TradingDay': _tradingday})
        ret = None
        if data is not None:
            ret = data['SubDominant']
        client.close()

        return ret

    def cache2DataStruct(_inst: str, _tradingday: str, _index: str) -> DataStruct:
        f = h5py.File(Fetch.cache_path, 'a')
        try:
            grp = f[_inst + '/' + _tradingday]
        except:
            return None

        datastruct = DataStruct(list(grp.keys()), _index.lower())
        for k in grp.keys():
            dataset = grp[k]
            datastruct.data[k] = dataset[:].tolist()
            if 'timestamp' in dataset.attrs['type']:
                datastruct.float2datetime(k)

        f.close()
        return datastruct

    def DataStruct2cache(
        _inst: str, _tradingday: str,
        _columns: typing.List[str], _types: typing.List[str],
        _datastruct: DataStruct
    ):
        f = h5py.File(Fetch.cache_path, 'a')

        for c, t in zip(_columns, _types):
            if 'int' in t:
                dtype = 'int32'
            elif 'char' in t:
                dtype = h5py.special_dtype(vlen=str)
            else:
                dtype = 'float64'

            if 'timestamp' in t:
                _datastruct.datetime2float(c)

            dataset = f.create_dataset(
                _inst + '/' + _tradingday + '/' + c,
                (len(_datastruct),),
                dtype=dtype,
            )
            dataset[:] = _datastruct.data[c]
            dataset.attrs['type'] = t

            if 'timestamp' in t:
                _datastruct.float2datetime(c)

        f.close()

    def fetchIntraDayData(
            _product: str, _tradingday: str,
            _instrument: str=None, _sub_dominant: bool=False,
            _index: str='HappenTime'
    ) -> DataStruct:
        """
        fetch each tick data of product(dominant) or instrument from begin date to end date

        Args:
            _product (str): if None using _instrument, else using dominant inst of _product
            _tradingday (str):
            _instrument (str): if _product is None, then using _instrument
            _sub_dominant (bool): if True and _product is not None, using sub dominant of _product

        Returns:
            DataStruct:
        """
        # set inst to real instrument name
        inst = _instrument
        if _product is not None:
            if not _sub_dominant:
                inst = Fetch.fetchDominant(_product, _tradingday)
            else:
                inst = Fetch.fetchSubDominant(_product, _tradingday)

        # check instrument valid
        if inst is None or not Fetch.instrumentIsTrade(inst, _tradingday):
            return None

        # if found in cache, then return
        ret = Fetch.cache2DataStruct(inst, _tradingday, _index)
        if ret is not None:
            return ret

        # fetch from database
        con = psycopg2.connect(
            dbname=Fetch.pgsql_dbname,
            host=Fetch.pgsql_host,
            user=Fetch.pgsql_user,
            password=Fetch.pgsql_password,
        )
        cur = con.cursor()

        # get all column names
        cur.execute(
            "select column_name, data_type from information_schema.columns " +
            "where table_name='" + inst + "'"
        )
        columns = []
        types = []
        for d in cur.fetchall():
            columns.append(d[0])
            types.append(d[1])

        # get all ticks
        cur.execute(
            "SELECT * FROM " + inst +
            " WHERE TradingDay='" + _tradingday +
            "' ORDER BY HappenTime"
        )
        datas = list(cur.fetchall())
        con.close()

        # turn into datastruct
        datastruct = DataStruct(columns, _index.lower(), datas)

        if len(datastruct):
            Fetch.DataStruct2cache(
                inst, _tradingday, columns, types, datastruct)
            return datastruct
        return None

if __name__ == '__main__':
    ret = Fetch.productList()
    assert type(ret) == list
    print('Fetch.productList', len(ret))

    ret = Fetch.productIsTrade('rb', '20170123')
    assert ret
    print('Fetch.productIsTrade', ret)

    ret = Fetch.productLastTradingDay('rb', '20170123')
    assert type(ret) == str
    print('Fetch.productLastTradingDay', ret)

    ret = Fetch.productNextTradingDay('rb', '20170123')
    assert type(ret) == str
    print('Fetch.productNextTradingDay', ret)

    ret = Fetch.fetchTradeInstrument('rb', '20170123')
    assert type(ret) == list
    print('Fetch.fetchTradeInstrument', len(ret))

    ret = Fetch.fetchDominant('rb', '20170123')
    assert type(ret) == str
    print('Fetch.fetchDominant', ret)

    ret = Fetch.fetchSubDominant('rb', '20170123')
    assert type(ret) == str
    print('Fetch.fetchSubDominant', ret)
