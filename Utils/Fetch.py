import psycopg2
from redis import StrictRedis
from pymongo import MongoClient
import pymongo
import pandas as pd
import pickle
import gzip


class Fetch:

    mongo_host = 'localhost'

    pgsql_host = 'localhost'
    pgsql_user = 'pang'

    redis_host = 'localhost'

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
        db = client.FutureInst
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
        db = client.FutureInst
        coll = db[_product]
        d = coll.find_one(
            {'TradingDay': {'$lt': _tradingday}},
            sort=[('TradingDay', pymongo.DESCENDING)], limit=1
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
        db = client.FutureInst
        coll = db[_product]
        d = coll.find_one(
            {'TradingDay': {'$gt': _tradingday}},
            sort=[('TradingDay', pymongo.ASCENDING)], limit=1
        )
        client.close()

        return d['TradingDay'] if d is not None else None

    def fetchAllTradeInst(_product: str, _tradingday: str) -> list:
        """
        fetch all traded insts of one product on tradingday

        Args:
            _product (str): which product.
            _tradingday (str): which day.

        Returns:
            list: list of str. if len() == 0, then no traded inst
        """
        client = MongoClient(host=Fetch.mongo_host)
        db = client.FutureInst
        coll = db[_product]
        data = coll.find_one({'TradingDay': _tradingday})
        ret = []
        if data is not None:
            ret = sorted(list(data['InstrumentList'].keys()))
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
        db = client.FutureInst
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
        db = client.FutureInst
        coll = db[_product]
        data = coll.find_one({'TradingDay': _tradingday})
        ret = None
        if data is not None:
            ret = data['SubDominant']
        client.close()

        return ret

    def fetchIntraDayData(
            _product: str, _tradingday: str,
            _instrument=None, _sub_dominant=False,
            _index='HappenTime') -> pd.DataFrame:
        """
        fetch each tick data of product(dominant) or instrument from begin date to end date

        Args:
            _product (str): if None using _instrument, else using dominant inst of _product
            _tradingday (str):
            _instrument (str): if _product is None, then using _instrument
            _sub_dominant (bool): if True and _product is not None, using sub dominant of _product

        Returns:
            pd.DataFrame:
        """
        # set inst to real instrument name
        inst = _instrument
        if _product is not None:
            if not _sub_dominant:
                inst = Fetch.fetchDominant(_product, _tradingday)
            else:
                inst = Fetch.fetchSubDominant(_product, _tradingday)
        if inst is None:
            return None

        # check whether cached in redis
        key = inst + '-' + _tradingday
        r = StrictRedis(host=Fetch.redis_host)
        redis_data = r.get(key)
        if redis_data is not None:
            return pickle.loads(gzip.decompress(redis_data))

        con = psycopg2.connect(
            database='FutureData',
            host=Fetch.pgsql_host,
            user=Fetch.pgsql_user,
        )
        cur = con.cursor()
        try:
            # get all column names
            cur.execute(
                "select column_name from information_schema.columns " +
                "where table_name='" + inst + "'"
            )
            columns = []
            for d in cur.fetchall():
                columns.append(d[0])
            # get all ticks
            cur.execute(
                "SELECT * FROM " + inst +
                " WHERE TradingDay='" + _tradingday +
                "' ORDER BY HappenTime"
            )
            datas = list(cur.fetchall())
            con.close()
            # turn into dataframe
            dataframe = pd.DataFrame(
                datas, columns=columns).set_index(_index.lower())
            # cache into redis
            r = StrictRedis(host=Fetch.redis_host)
            r.set(key, gzip.compress(pickle.dumps(dataframe)))

            return dataframe
        except psycopg2.DatabaseError as e:
            print('Error:', e)
            if con:
                con.rollback()
            con.close()

    # def fetchInterDayData():
    #     pass

if __name__ == '__main__':
    import time
    # start_time = time.time()
    # Fetch.fetchIntraDayData('rb', '20170104')
    # print(time.time() - start_time)

    print(Fetch.productNextTradingDay('rb', '20161230'))
