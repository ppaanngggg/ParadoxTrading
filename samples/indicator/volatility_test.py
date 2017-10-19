from ParadoxTrading.Chart import Wizard
from ParadoxTrading.Fetch.ChineseFutures import FetchDominantIndex
from ParadoxTrading.Indicator import Volatility

fetcher = FetchDominantIndex()

market = fetcher.fetchDayData('20100701', '20170101', 'rb')
vol_1 = Volatility(30, _smooth=1).addMany(market).getAllData()
vol_12 = Volatility(30, _smooth=12).addMany(market).getAllData()

wizard = Wizard()

price_view = wizard.addView('price', _view_stretch=3)
price_view.addLine('market', market.index(), market['closeprice'])

sub_view = wizard.addView('sub')
sub_view.addLine('vol_1', vol_1.index(), vol_1['volatility'])
sub_view.addLine('vol_12', vol_12.index(), vol_12['volatility'])

wizard.show()
