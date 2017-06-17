from ParadoxTrading.Chart import Wizard
from ParadoxTrading.Fetch import FetchExchangeMarketIndex

fetcher = FetchExchangeMarketIndex()

data = fetcher.fetchDayData('20160101', '20170401', 'rb')

buy_time_list = [data.index()[i] for i in range(0, len(data), 5)]
buy_price_list = [data['closeprice'][i] for i in range(0, len(data), 5)]

# 1. create a wizard for chart
wizard = Wizard()

# 2. create a view, _view_stretch will affect the rate of this view
#   if _adaptive, the y will adapti itself
price_view = wizard.addView('price', _view_stretch=3, _adaptive=True)
# 2-1. add candle example
price_view.addCandle(
    'K', data.index(),
    data.toRows(
        ['openprice', 'highprice', 'lowprice', 'closeprice']
    )[0], )
# 2-2. add line example, you can set not showing value
price_view.addLine('closeprice', data.index(), data['closeprice'], _show_value=False)
# 2-3. add scatter, you can set color
price_view.addScatter('buy', buy_time_list, buy_price_list, _color='orange', _show_value=False)

# 3. add another view
view_volume = wizard.addView('volume')
# 3-1. add bar example
view_volume.addBar('volume', data.index(), data['volume'])

# finally, show the chart
wizard.show()
