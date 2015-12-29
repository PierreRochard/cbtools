import sys

import numpy as np
import pandas as pd

sys.path.append('..')
from cbtools.models import Fills, session, engine


pd.set_option('display.float_format', lambda x: '%.8f' % x)
df = pd.read_sql_table('fills', engine, schema='cbtools')
df['product'] = df['size'] * df['price']
grouped = df.groupby('order_id').sum()
grouped['wa_price'] = grouped['product'] / grouped['size']
del grouped['trade_id']
del grouped['price']
del grouped['settled']
del grouped['product']
grouped.to_csv('fills_report.csv')
