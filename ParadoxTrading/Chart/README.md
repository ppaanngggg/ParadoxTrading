# ParadoxTrading.Chart

This module is a chart tool to show time series data based on PyQt5

## Current support series

1. line
2. bar
3. scatter
4. candle

## quick start

```python
from ParadoxTrading.Chart import Wizard

wizard = Wizard()
view = wizard.addView('demo')
view.addLine('demo line', [1, 2], [1, 2])
wizard.show()
```

> use `arrow` key to adjust view
