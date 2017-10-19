from ParadoxTrading.Chart import Wizard
from ParadoxTrading.Fetch import FetchExchangeMarketIndex
from ParadoxTrading.Indicator import SAR

fetcher = FetchExchangeMarketIndex()
fetcher.psql_host = '192.168.4.103'
fetcher.psql_user = 'ubuntu'
fetcher.mongo_host = '192.168.4.103'

market = fetcher.fetchDayData('20100701', '20170101', 'rb')
sar = SAR().addMany(market).getAllData()

wizard = Wizard()
price_view = wizard.addView('price', _adaptive=True)
price_view.addCandle(
    'market', market.index(),
    market.toRows(['openprice', 'highprice', 'lowprice', 'closeprice'])[0]
)
price_view.addScatter('sar', sar.index(), sar['sar'])
wizard.show()
