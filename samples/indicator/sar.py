from ParadoxTrading.Chart import Wizard
from ParadoxTrading.Fetch.ChineseFutures import FetchDominantIndex
from ParadoxTrading.Indicator import SAR

fetcher = FetchDominantIndex()

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
