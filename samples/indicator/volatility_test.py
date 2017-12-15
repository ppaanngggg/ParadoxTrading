from ParadoxTrading.Chart import Wizard
from ParadoxTrading.Fetch.ChineseFutures import FetchDominantIndex
from ParadoxTrading.Indicator import FastVolatility, Volatility
import time

fetcher = FetchDominantIndex()

market = fetcher.fetchDayData('20100701', '20170101', 'cu')
print(time.clock())
vol_1 = Volatility(30, _smooth=1).addMany(market).getAllData()
vol_12 = Volatility(30, _smooth=12).addMany(market).getAllData()
print(time.clock())
fast_vol_1 = FastVolatility(30, _smooth=1).addMany(market).getAllData()
fast_vol_12 = FastVolatility(30, _smooth=12).addMany(market).getAllData()
print(time.clock())

wizard = Wizard()

price_view = wizard.addView('price')
price_view.addLine('market', market.index(), market['closeprice'])

sub_view = wizard.addView('std')
sub_view.addLine('vol_1', vol_1.index(), vol_1['volatility'])
sub_view.addLine('vol_12', vol_12.index(), vol_12['volatility'])
sub_view.addLine('fast_vol_1', fast_vol_1.index(), fast_vol_1['volatility'])
sub_view.addLine('fast_vol_12', fast_vol_12.index(), fast_vol_12['volatility'])

wizard.show()
