import numpy as np
from ParadoxTrading.Chart import Wizard
from ParadoxTrading.Fetch.ChineseFutures import FetchDominantIndex
from ParadoxTrading.Indicator import FastVolatility, GARCH, LogReturn
from arch import arch_model

fetcher = FetchDominantIndex()

market = fetcher.fetchDayData('20100701', '20171201', 'cu')

returns = LogReturn().addMany(market).getAllData()
return_arr = np.array(returns['logreturn'])

am = arch_model(return_arr, mean='Zero')
res = am.fit(disp='off', show_warning=False)

garch = GARCH(_smooth_period=7).addMany(market).getAllData()
fast_vol_12 = FastVolatility(
    30, _smooth=12).addMany(market).getAllData()

wizard = Wizard()

price_view = wizard.addView('price')
price_view.addLine(
    'market', market.index(), market['closeprice']
)

sub_view = wizard.addView('std')
sub_view.addLine(
    'fast_vol_12', fast_vol_12.index(), fast_vol_12['volatility']
)
sub_view.addLine(
    'garch', garch.index(), garch['predict']
)
# sub_view.addLine(
#     'total garch', returns.index(), res.conditional_volatility
# )

wizard.show()
