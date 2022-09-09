'''This package includes material copied, edited, and repurposed from a Python
notebook file released under the Apache 2.0 open source license. 

NOTICE:
The contents of this package include material that has been edited from its
original form. The original distribution of this package's source material 
is available at the following URL:

https://www.kaggle.com/code/inversion/amex-competition-metric-python


Copyright 2022 Nicholas Brower

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''

import numpy as np
import pandas as pd
from pathlib import Path

input_path = Path('/kaggle/input/amex-default-prediction/')

'''Amex Metric

This is a python version of the metric for the Amex competition. Additional 
details can be found on the competition Evaluation page.
'''

def amex_metric(y_true: pd.DataFrame, y_pred: pd.DataFrame) -> float:

    def top_four_percent_captured(y_true: pd.DataFrame, y_pred: pd.DataFrame) -> float:
        df = (pd.concat([y_true, y_pred], axis='columns')
              .sort_values('prediction', ascending=False))
        df['weight'] = df['target'].apply(lambda x: 20 if x==0 else 1)
        four_pct_cutoff = int(0.04 * df['weight'].sum())
        df['weight_cumsum'] = df['weight'].cumsum()
        df_cutoff = df.loc[df['weight_cumsum'] <= four_pct_cutoff]
        return (df_cutoff['target'] == 1).sum() / (df['target'] == 1).sum()
        
    def weighted_gini(y_true: pd.DataFrame, y_pred: pd.DataFrame) -> float:
        df = (pd.concat([y_true, y_pred], axis='columns')
              .sort_values('prediction', ascending=False))
        df['weight'] = df['target'].apply(lambda x: 20 if x==0 else 1)
        df['random'] = (df['weight'] / df['weight'].sum()).cumsum()
        total_pos = (df['target'] * df['weight']).sum()
        df['cum_pos_found'] = (df['target'] * df['weight']).cumsum()
        df['lorentz'] = df['cum_pos_found'] / total_pos
        df['gini'] = (df['lorentz'] - df['random']) * df['weight']
        return df['gini'].sum()

    def normalized_weighted_gini(y_true: pd.DataFrame, y_pred: pd.DataFrame) -> float:
        y_true_pred = y_true.rename(columns={'target': 'prediction'})
        return weighted_gini(y_true, y_pred) / weighted_gini(y_true, y_true_pred)

    g = normalized_weighted_gini(y_true, y_pred)
    d = top_four_percent_captured(y_true, y_pred)

    return 0.5 * (g + d)

'''Simple Benchmark

We can create a simple benchark using the average of the feature P_2 for each customer.


train_data = pd.read_csv(
    input_path / 'train_data.csv',
    index_col='customer_ID',
    usecols=['customer_ID', 'P_2'])

train_labels = pd.read_csv(input_path / 'train_labels.csv', index_col='customer_ID')

ave_p2 = (train_data
          .groupby('customer_ID')
          .mean()
          .rename(columns={'P_2': 'prediction'}))

# Scale the mean P_2 by the max value and take the compliment
ave_p2['prediction'] = 1.0 - (ave_p2['prediction'] / ave_p2['prediction'].max())


print(amex_metric(train_labels, ave_p2)) # 0.572773

#0.5727729219880231

'''
SIMPLE_BENCHMARK_P_2 = 0.5727729219880231
