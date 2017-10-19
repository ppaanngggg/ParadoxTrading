from ParadoxTrading.Chart import Wizard
from ParadoxTrading.Fetch import FetchExchangeMarketIndex
from ParadoxTrading.Indicator import ZigZag

fetcher = FetchExchangeMarketIndex()
fetcher.psql_host = '192.168.4.103'
fetcher.psql_user = 'ubuntu'
fetcher.mongo_host = '192.168.4.103'

market = fetcher.fetchDayData('20100701', '20170101', 'rb')
zigzag = ZigZag(0.1).addMany(market).getAllData()

wizard = Wizard()
price_view = wizard.addView('price', _adaptive=True)
price_view.addLine('market', market.index(), market['closeprice'])
price_view.addLine('zigzag', zigzag.index(), zigzag['zigzag'])
wizard.show()
