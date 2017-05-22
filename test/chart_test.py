from ParadoxTrading.Chart import Wizard
from ParadoxTrading.Fetch import FetchSHFEDayIndex

fetcher = FetchSHFEDayIndex()

data = fetcher.fetchDayData('20160101', '20170401', 'rb')
print(data)

buy_time_list = [data.index()[i] for i in range(0, len(data), 5)]
buy_price_list = [data['closeprice'][i] for i in range(0, len(data), 5)]

wizard = Wizard()

price_view = wizard.addView('price', 3, _adaptive=True)
price_view.addCandle(
    'K', data.index(),
    data.toRows(
        ['openprice', 'highprice', 'lowprice', 'closeprice']
    )[0], )
price_view.addLine('closeprice', data.index(), data['closeprice'])
price_view.addScatter('buy', buy_time_list, buy_price_list, _color='orange', _show_value=False)
view_volume = wizard.addView('volume')
view_volume.addBar('volume', data.index(), data['volume'])

wizard.show()
