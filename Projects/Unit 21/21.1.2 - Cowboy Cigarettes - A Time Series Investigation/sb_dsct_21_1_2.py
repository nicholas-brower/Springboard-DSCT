from typing import Callable, Union, Any
from operator import attrgetter
from dataclasses import dataclass, field
from datetime import datetime
from functools import partial
from itertools import product, combinations
from textwrap import fill
import warnings
import numpy as np
from numpy.typing import ArrayLike
from numpy.lib.stride_tricks import sliding_window_view
import pandas as pd
import statsmodels.api as sm
from statsmodels.tsa.stattools import acf, pacf, kpss
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX, SARIMAXResults
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import ExpSineSquared
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split as ttt
from sklearn.preprocessing import MinMaxScaler, StandardScaler
import matplotlib.pyplot as plt
from matplotlib.axes import Axes
from matplotlib import cm
from matplotlib.figure import Figure
from matplotlib.patches import Rectangle
from matplotlib.colors import LinearSegmentedColormap as LSCmap
import pmdarima

#constants
AXES_STYLE = {
    'ax':{'facecolor':(0.95, 0.95, 0.95)}, 
    'grid':{
        'x': {'color':(0.97, 0.97, 0.97), 'zorder': 0, 'lw':3}, 
        'y': {'color':(0.7, 0.7, 0.7)}, 'zorder': 0, 'lw':3}
}
GRAY_L1 = LSCmap.from_list('gs_30_10', [[0.7]*3, [0.9]*3], N=500)
HORIZ_GRAD = np.vstack([np.linspace(0, 1, 256), np.linspace(0, 1, 256)])
VERT_GRAD = np.rot90(HORIZ_GRAD)
Subplots = tuple[Figure, Union[Axes, ArrayLike]]

#functions
def month_int(year: int, month: int) -> int:
    '''Return an integer x for given year and month integers, where x
    indicates that the year-month combination is the xth month since
    the start of year 0 AD.
    '''
    return 12 * year + month
def apply_plot_style(ax: Axes=None) -> None:
    '''Apply grid, tick, and spine parameters to a given Axes object.
    '''
    ax = plt.gca() if ax is None else ax
    ax.grid(axis='x', **AXES_STYLE['grid']['x'])
    ax.grid(axis='y', **AXES_STYLE['grid']['y'])
    ax.tick_params(
        axis='both', which='both', length=0, colors=[0.3, 0.3, 0.3], pad=10
    )
    ax.spines[:].set_color([0] * 4)
    ax.set_axisbelow(True)
def styled_subplots(
        n_rows: int, n_cols: int, figsize: tuple[int],
        n_elements: int=0) -> Subplots:
    '''Create subplots for a given figure size and given numbers of rows and
    columns. Apply style conventions to each subplot as in apply_plot_style.
    '''
    fig, axs = plt.subplots(n_rows, n_cols, figsize=figsize)
    if type(axs).__name__ == 'AxesSubplot':
        apply_plot_style(axs)
    else:
        for ax in axs:
            apply_plot_style(ax)
    if n_elements:
        num_empty = n_rows * n_cols - n_elements
        for n in range(1, num_empty + 1):
            axs.flat[-1 * n].set_axis_off()
    return fig, axs
def tt_split(a: ArrayLike, test_size: float=0.2) -> tuple[ArrayLike]:
    '''Split a given array into train and test subsets for a given test_size as
    a float between zero and 1. Return a tuple of arrays a[:i], a[i:] where i is
    the nearest nonzero integer to len(a) - test_size * len(a).
    '''
    i = min([round((1 - test_size) * len(a)), len(a) - 1])
    if type(a) in [pd.Series, pd.DataFrame]:
        return a.iloc[:i], a.iloc[i:]
    return a[:i], a[i:]
def cont_window_agg(arr: ArrayLike, window_size: int, func: Callable) -> ArrayLike:
    '''For a given input array, return an array comprised of the results of
    calling a given Callable on each subset of the array returned by a sliding
    window iterator. Windows are centered on each index of the original array
    and truncated to allow continuous results over the entire length of the 
    original array.
    '''
    out = []
    for step in range(len(arr)):
        j = int(min([len(arr), step + np.ceil(window_size/2)]))
        i = int(max([0, step - window_size//2]))
        out.append(func(arr[i:j]))
    return np.array(out)
def dt_discontinuities(df: pd.DataFrame, col: str, freq: str) -> list:
    '''For given a DataFrame, datetime column, and frequency, return a list of
    datetime values for each discontinuity in a timeseries.
    '''
    discont = [
        dt for dt in pd.date_range(df[col].min(), df[col].max(), freq=freq)
        if dt not in df[col].unique()
    ]
def is_continuous(df: pd.DataFrame, col: str, freq: str) -> bool:
    '''Return a boolean describing whether data is continuous over a given
    frequency and corresponding datetime column.
    '''
    return not bool(dt_discontinuities(df, col, freq))
def warnings_as_dicts(w: list, num_operations: int=10) -> list[dict[str]]:
    '''Store warnings as a list of dictionaries.
    '''
    w_d_list = []
    for _ in range(num_operations):
        try:
            w_d_list.append({
                'file': w[_].filename[
                    w[_].filename.index('packages') + len('packages') + 1:
                ],
                'category': w[_].category.__name__, 'message': f'{w[_].message}'
            })
        except:
            break
        return w_d_list
def print_warn(w_d_list: list[dict], indent: int=4) -> None:
    '''Return simplified strings from a given list of dictionaries as generated
    by warnings_as_dicts.
    '''
    indent = indent * ' '
    for w in w_d_list:
        print(f'{w["category"]} ({w["file"]})\n{indent}' 
              + f'\n{indent}'.join(w['message'].split('\n')))
def kpss_result(a: ArrayLike, **kwargs) -> list[object]:
    '''A convenience function to print warning messages often generated when
    calling the statsmodels implementation of the Kwiatkowski Phillips Schmidt
    Shin test for stationarity.
    '''
    with warnings.catch_warnings(record=True) as w:
        a_kpss_stat, a_kpss_p_value, a_kpss_lags, a_kpss_crit = kpss(a, **kwargs)
        kpss_a = [a_kpss_stat, a_kpss_p_value, a_kpss_lags, a_kpss_crit]
        w_ = w
    if w_:
        w = warnings_as_dicts(w_)
        print_warn(w)
        for comparison in ['smaller<', 'greater>']:
            if comparison[:-1] in w[0]['message']:
                kpss_a.append(comparison[-1])
    return kpss_a
def kpss_as_str(kpss_list: list[object]) -> str:
    '''Render the result tuple of statsmodels kpss test as a string formatted
    as a table.
    '''
    p_qual = f'{"".join([f" {c}" for c in ["<", ">"] if kpss_list[-1]==c])}'
    kpss_names = ['test statistic', f'p-value{p_qual}', 'lags', 'critical values']
    s = 'KPSS test results:\n-\n'
    kpss_results = zip(kpss_names, kpss_list)
    for name, stat in kpss_results:
        if name == 'critical values':
            s = s + f'{name:<20}\n'
            txt = '\n'.join(
                4*' ' + f'{float(k.strip("%")):>4.1f} %'.ljust(16) 
                + f'{v:>10.4f}' for k, v in stat.items()
            )
        else:
            txt = f'{name:<20}{stat:>10.4f}{(name.endswith(p_qual))*"*"}\n'            
        s = s + txt
    return s
def int_as_subscript(integer: int) -> str:
    '''Return the string representation of an integer using the Unicode
    Superscripts and Subscripts block, characters U+2080 to U+2089.
    '''
    subscripts = dict(zip('0123456789', '₀₁₂₃₄₅₆₇₈₉'))
    return ''.join(map(subscripts.get, str(integer)))
def sm_acf_conf(acf_val: ArrayLike, acf_conf: ArrayLike) -> tuple[ArrayLike]:
    conf_lo, conf_hi = acf_conf[:, 0] - acf_val, acf_conf[:, 1] - acf_val
    significant = np.invert((acf_val >= conf_lo) * (acf_val <= conf_hi))
    return conf_lo, conf_hi, significant
def acf_df(a: ArrayLike, nlags: int=0, lags: list[int]=[]) -> pd.Series:
    if not lags:
        if not nlags:
            nlags = 24
        lags = np.arange(nlags + 1)
    a_acf, a_pacf = [
        func(a, nlags=min([len(a), max(lags) + 1]), alpha=0.05)
        for func in (acf, pacf)
    ]
    (acf_val, acf_conf), (pacf_val, pacf_conf) = a_acf[:2], a_pacf[:2]
    acf_plot_lo, acf_plot_hi, acf_sig = sm_acf_conf(acf_val, acf_conf)
    pacf_plot_lo, pacf_plot_hi, pacf_sig = sm_acf_conf(pacf_val, pacf_conf)
    columns = [
        'lag', 'acf', 'acf_conf_lo', 'acf_conf_hi', 'acf_plot_lo', 
        'acf_plot_hi', 'acf_sig', 'pacf', 'pacf_conf_lo', 'pacf_conf_hi', 
        'pacf_plot_lo', 'pacf_plot_hi', 'pacf_sig'
    ]
    values = [
        np.arange(len(acf_val)), acf_val, acf_conf[:, 0], acf_conf[:, 1],
        acf_plot_lo, acf_plot_hi, acf_sig, pacf_val, pacf_conf[:, 0], 
        pacf_conf[:, 1], pacf_plot_lo, pacf_plot_hi, pacf_sig
    ]
    acf_df_ = pd.DataFrame(dict(zip(columns, values)))
    acf_df_.loc[:, 'acf_marker'] = acf_df_.loc[:, 'acf_sig'].map(
        {True:'s', False:'x'}
    )
    acf_df_.loc[:, 'pacf_marker'] = acf_df_.loc[:, 'pacf_sig'].map(
        {True:'s', False:'x'}
    )
    acf_df_.set_index('lag', inplace=True)
    return acf_df_.loc[lags]
def acf_plots(a: ArrayLike, nlags: int=0, lags: list[int]=[], 
              suptitle: str='') -> None:
    df = acf_df(a, nlags, lags)
    fig, axs = styled_subplots(2, 1, figsize=(12, 8))
    for ax, func in zip(axs, ['acf', 'pacf']):
        for lag, height in zip(df.index, df[func]):
            _tloc = ((height >= 0) * 2 - 1) * 0.07
            va = {True: 'baseline', False: 'top'}.get(height>=0)
            c = [(0.7, 0.7, 0.7), (0.3, 0.6, 0.3)][
                df.loc[lag][f'{func}_sig'].astype(int)
            ]
            ax.plot([lag, lag], [0, height], color=c, lw=2)
            '''ax.scatter(
                x=lag, y=height, marker=df.loc[lag][f'{func}_marker'], 
                color='black'
            )'''
            ax.text(
                x=lag, y=height + _tloc, s=lag, ha='center', 
                va=va, fontsize=9, color=np.array(c)/2
            )
        ax.fill_between(df.index, df[f'{func}_plot_lo'], 
                        df[f'{func}_plot_hi'], alpha=0.1, color='black')
        title = func.startswith('p') * 'Partial ' + 'autocorrelation function'
        ax.set_title(title[0].upper() + title[1:].lower(),
                     loc='left', x=0, pad=20)
        ax.axhline(0, lw=2, color='black')
        ax.set_xticks(range(0, df.index[-1], 2), [])
    if suptitle:
        ha = fig.subplotpars.left
        fig.suptitle(suptitle, horizontalalignment='left', x=ha, fontsize=14)

#classes
@dataclass
class SARIMAXParams:
    '''A dataclass to facilitate organization of SARIMAX timeseries
    model parameters in an implementation-agnostic format.
    '''
    params: list[tuple[int]]
    order: tuple[int] = field(init=False)
    seasonal_order: tuple[int] = field(init=False)
    p: int = field(init=False); d: int = field(init=False)
    q: int = field(init=False); P: int = field(init=False)
    D: int = field(init=False); Q: int = field(init=False)
    s: int = field(init=False); is_seasonal: bool = field(init=False)
    model_type: str = field(init=False)
    _kwargs: dict = field(default_factory=dict)
    
    def __post_init__(self):
        if len(list(filter(None, self.params)))==1:
            self.order, self.seasonal_order = self.params[0], None
            self.p, self.d, self.q = self.order
            self.P, self.D, self.Q, self.s = [None] * 4
            self.model_type, seasonal_str = '', ''
            self.is_seasonal = False
        else:
            self.order, self.seasonal_order = self.params
            self.p, self.d, self.q = self.order
            self.P, self.D, self.Q, self.s = self.seasonal_order
            self._kwargs['seasonal_order'] = self.seasonal_order
            self.model_type = 'S'
            seasonal_str = f'x{self.seasonal_order[:-1]}{int_as_subscript(self.s)}'
            self.is_seasonal = True
        self._kwargs['order'] = self.order
        self.model_type = f'{self.model_type}ARIMA{self.order}{seasonal_str}'
    
@dataclass
class TimeSeriesModel:
    '''A dataclass to facilitate comparison of time series modelling
    options by cross-validation results and information criteria.
    '''
    time_series: ArrayLike
    params: SARIMAXParams
    model_type: str = field(init=False)
    model: Callable = field(init=False)
    results: SARIMAXResults = field(init=False)
    arr_name: str = field(default_factory=str)
    mse: float = field(init=False)
    mse_per_var: float = field(init=False)
    mse_per_mean: float = field(init=False)
    aic: float = field(init=False)
    bic: float = field(init=False)
    
    def __post_init__(self):
        self.model_type = self.params.model_type
        self.model = {False: ARIMA, True: SARIMAX}[self.params.is_seasonal]
        with warnings.catch_warnings(record=True) as w:
            try:
                self.model = self.model(
                    self.time_series, **self.params._kwargs
                )
                self.results = self.model.fit()
                self.aic, self.bic = self.results.aic, self.results.bic
                train, test = tt_split(self.time_series)
                predictions = self.results.forecast(len(test))
                self.mse = mean_squared_error(test, predictions)
                self.mse_per_var = self.mse / np.var(test)
                self.mse_per_mean = self.mse / np.mean(test)
            except np.linalg.LinAlgError:
                self.model, self.results, self.aic, self.bic, self.mse = (
                    5 * [None]
                )
                self.mse_per_var, self.mse_per_mean = None, None
                
@dataclass
class SARIMAXCrossValResults:
    '''A dataclass to organize cross validation results for a given time
    series and a given list of fitted SARIMAX models.
    '''
    models: list[TimeSeriesModel]
    time_series: ArrayLike
    by_mse: list[TimeSeriesModel] = field(init=False)
    by_aic: list[TimeSeriesModel] = field(init=False)
    by_bic: list[TimeSeriesModel] = field(init=False)
    
    def __post_init__(self):
        self.models = [m for m in self.models  if m.model is not None]
        self.by_mse = sorted(self.models, key=attrgetter('mse'))
        self.by_aic = sorted(self.models, key=attrgetter('aic'))
        self.by_bic = sorted(self.models, key=attrgetter('bic'))
    
    def best(self, by: str='mse') -> TimeSeriesModel:
        '''Return the best model as indicated by a given performance metric.
        '''
        agg = {'mse': min, 'aic': min, 'bic': min}
        return agg[by](self.models, key=attrgetter(by))
    
    def report(self, title: str='') -> str:
        '''Return a string summarizing the results of the cv process that
        returned this SARIMAXCrossValResults instance.
        '''
        title = bool(title) * f'{title}\n{70*"-"}\n'
        m_typ = '' + self.models[0].params.is_seasonal * 'S' + 'ARIMA'
        return (
            f'{title}{m_typ} model cv results:\n{"Models tested:":<24}'
            + f'{len(self.models)}\n{"Best model:":<24}'
            + f'{self.by_mse[0].model_type}\n{"Metric":<24}'
            + f'Mean squared error (MSE)\n{"MSE:":<24}{self.by_mse[0].mse}\n'
            + f'{"MSE / s²:":<24}{self.by_mse[0].mse_per_var}\n'
            + f'{"MSE / X̄(test):":<25}{self.by_mse[0].mse_per_mean}'
        )
