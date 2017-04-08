from ParadoxTrading.Chart import Wizard
from ParadoxTrading.Performance import dailyReturn, sharpRatio

ret = dailyReturn('range_break', 'range_break', _init_fund=10000)
print(ret)
print('SharpRatio: {}'.format(sharpRatio(ret)))

wizard = Wizard('range_break_fund')

wizard.addView('fund')
wizard.addLine('fund', ret.index(), ret['fund'], 'fund change')

wizard.show()
