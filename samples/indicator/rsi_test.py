from ParadoxTrading.Chart import Wizard
from ParadoxTrading.Fetch.ChineseFutures import FetchDominantIndex
from ParadoxTrading.Indicator import RSI

fetcher = FetchDominantIndex()

market = fetcher.fetchDayData('20100701', '20170101', 'rb')
rsi = RSI(14).addMany(market).getAllData()

wizard = Wizard()

price_view = wizard.addView('price', _view_stretch=3)
price_view.addLine('market', market.index(), market['closeprice'])

sub_view = wizard.addView('sub')
sub_view.addLine('rsi', rsi.index(), rsi['rsi'])

wizard.show()
