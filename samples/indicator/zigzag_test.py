from ParadoxTrading.Chart import Wizard
from ParadoxTrading.Fetch.ChineseFutures import FetchDominantIndex
from ParadoxTrading.Indicator import ZigZag

fetcher = FetchDominantIndex()

market = fetcher.fetchDayData('20100701', '20170101', 'rb')
zigzag = ZigZag(0.1).addMany(market).getAllData()

wizard = Wizard()
price_view = wizard.addView('price', _adaptive=True)
price_view.addLine('market', market.index(), market['closeprice'])
price_view.addLine('zigzag', zigzag.index(), zigzag['zigzag'])
wizard.show()
