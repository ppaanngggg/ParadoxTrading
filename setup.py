from distutils.core import setup

setup(
    name='ParadoxTrading',
    version='0.0.2',
    author='hantian.pang',
    packages=[
        'ParadoxTrading',
        'ParadoxTrading/Chart',
        'ParadoxTrading/Database',
        'ParadoxTrading/Database/ChineseFutures',
        'ParadoxTrading/Engine',
        'ParadoxTrading/EngineExt',
        'ParadoxTrading/EngineExt/Futures',
        'ParadoxTrading/EngineExt/Futures/Trend',
        'ParadoxTrading/EngineExt/Futures/Arbitrage',
        'ParadoxTrading/Fetch',
        'ParadoxTrading/Indicator',
        'ParadoxTrading/Indicator/Bar',
        'ParadoxTrading/Indicator/General',
        'ParadoxTrading/Indicator/Stop',
        'ParadoxTrading/Performance',
        'ParadoxTrading/Utils',
        'ParadoxTrading/Utils/CTP',
    ],
    install_requires=[
        'numpy', 'tabulate',
        'psycopg2', 'pymongo',
        'PyQt5', 'PyQtChart',
        'arrow', 'pandas',
        'diskcache',
        'schedule',
        'requests', 'beautifulsoup4', 'urllib3', 'lxml',
    ],
    extras_require={
        'CTP': ['PyCTP'],
        'TSA': ['TorchTSA'],
    },
)
