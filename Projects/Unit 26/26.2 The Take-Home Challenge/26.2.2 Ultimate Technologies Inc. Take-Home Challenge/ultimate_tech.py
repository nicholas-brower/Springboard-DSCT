'''Gathered from existing boilerplate or written for this assignment by
Nicholas Brower.
'''


# Import modules and packages from standard Python library.
from calendar import day_abbr, day_name, firstweekday, month_name, monthrange
from collections import namedtuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import partial, reduce
from itertools import chain, pairwise
import json
from operator import methodcaller, attrgetter
import re
import textwrap
from typing import Any, Callable, Iterable, Sequence, Union 
import warnings

# Import third-party modules and packages.
import numpy as np
from numpy.typing import ArrayLike, DTypeLike
import pandas as pd
from pandas.tseries.offsets import DateOffset
from matplotlib import cm as colormap
import matplotlib.pyplot as plt
from matplotlib.axes import Axes, Subplot
from matplotlib.figure import Figure
import matplotlib.patches as patches
import scipy.stats
import seaborn as sns
import statsmodels.api as sm
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf

# Import scikitlearn processing and testing options
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.cluster import DBSCAN, KMeans
from sklearn.decomposition import (
    FactorAnalysis, FastICA, KernelPCA, PCA, SparsePCA, TruncatedSVD
)
from sklearn.compose import ColumnTransformer
from sklearn.impute import KNNImputer, SimpleImputer
from sklearn.model_selection import (
    GridSearchCV, GroupKFold, KFold, RandomizedSearchCV, StratifiedKFold, 
    cross_validate, cross_val_score, train_test_split 
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import (
    FunctionTransformer, PowerTransformer, QuantileTransformer, MinMaxScaler,
    RobustScaler, StandardScaler
)

# Import scikitlearn metrics
from sklearn.metrics import (
    accuracy_score, auc, balanced_accuracy_score, f1_score,
    mean_absolute_percentage_error as mape, mean_absolute_error as mae, 
    mean_squared_error as mse, median_absolute_error as median_abs_error,
    precision_score, r2_score, recall_score, classification_report
)
rmse = partial(mse, squared=False)

# Import scikitlearn classifiers
from sklearn.tree import DecisionTreeClassifier
from sklearn.discriminant_analysis import QuadraticDiscriminantAnalysis
from sklearn.ensemble import (
    AdaBoostClassifier, HistGradientBoostingClassifier, RandomForestClassifier,
)
from sklearn.gaussian_process import GaussianProcessClassifier
from sklearn.gaussian_process.kernels import RBF
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import ComplementNB, GaussianNB, MultinomialNB
from sklearn.neural_network import MLPClassifier
from sklearn.svm import LinearSVC, SVC
from sklearn.linear_model import RidgeClassifier, SGDClassifier

# Import scikitlearn regressors
from sklearn.ensemble import (
    AdaBoostRegressor, HistGradientBoostingRegressor
)
from sklearn.linear_model import Lasso, LinearRegression, Ridge
from sklearn.neighbors import KNeighborsRegressor
from sklearn.svm import LinearSVR, SVR

# Establish constants.
ARIMA, SARIMAX = sm.tsa.ARIMA, sm.tsa.SARIMAX
DATA = 'Data/'
RAW_DATA = f'{DATA}/Raw/'
HR = 79 * '-'
BR = '\n'
TAB = '    '
WEEKDAYS = list(day_name)
WEEKDAYS_ABBR = list(day_abbr)
WEEKDAY_MAP = dict(enumerate(WEEKDAYS))
WEEKDAY_ABBR_MAP = dict(enumerate(WEEKDAYS_ABBR))
MONTH_MAP = {i: month_name for i, month_name in enumerate(month_name) if i}

# Define functions.

def drop_by_num_null(
        data: pd.DataFrame, threshold: int=2, verbose: bool=False, 
        inplace: bool=False) -> Union[None, pd.DataFrame]:
    '''Drop records where the number of null fields is greater than or 
    equal to a given threshold.
    '''
    df = data.copy()
    n_or_more_missing = df.loc[df.isna().sum(axis=1) >= threshold].index
    if verbose:
        num_dropped = len(missing_2_or_more_fields)
        print(
            f'{num_dropped:,.0f} records dropped '
            + f'({100*num_dropped/len(ultimate):.2f} %)'
        )
        del num_dropped
    if inplace:
        data = df
    else:
        return df
        
def composite_function(*funcs: Callable) -> Callable:
    '''Return a composite function composed from an ordered sequence of 
    functions.
    '''
    return (
        lambda initial: reduce(
            lambda state, func: func(state), funcs, initial
        )
    )
    
def series_color_array(data: pd.Series, color: Union[ArrayLike, Sequence]):
    '''Return an array of shape (len(data), len(color)) comprised of the
    array passed to color repeated len(data) times.
    '''
    return np.full((len(data), len(color)), np.array(color))

def series_color(
        data: pd.Series, color: Union[Sequence, ArrayLike], 
        func: Union[Callable, str], *args, **kwargs) -> ArrayLike:
    if isinstance(func, str):
        func = methodcaller(func, *args, **kwargs)
    else:
        func = partial(func, *args, **kwargs)
    color = np.array(color)
    colors = series_color_array(data, color)
    return func(
        np.tile(data.values.reshape(-1, 1), reps=color.shape[-1]),
        color
    )

def first_where(
        iterable: Iterable, condition: Union[Callable, None]=None, 
        default: Union[Any, None]=None
) -> Any:
    '''Return the first element in an iterable for which 
    condition(element) returns True.
    
    Parameters:
    ====================================================================
    iterable: Iterable
        An iterable object compatible with the built-in filter function 
        and the Callable passed to the condition parameter of this
        function.
    condition: Union[Callable, None], default=None
        A Callable passed to the first positional argument of the
        built-in filter function.
    default: Union[Any, None], default=None
        A default value passed to the last positional argument of the 
        built-in next function. This value is returned if not any
        condition(element) is True.
    ====================================================================
    
    Returns: Union[Any, None]
    The first element for which condition(element) is True or the
    value specified in the default argument.
    '''
    return next(filter(condition, iterable), default)

def is_none(obj: Any) -> bool:
    '''Return the evaluated boolean expression 'obj is None'.
    '''
    return obj is None

def is_not_none(obj: Any) -> bool:
    '''Return the evaluated boolean expression 'obj is not None'.
    '''
    return obj is not None

def is_null(obj: Any) -> Any:
    '''Return the result of passing obj to the Pandas isnull function.
    '''
    py_type = firstwhere([list, set, tuple], partial(isinstance, obj), False)
    if py_type:
        return py_type(map(pd.isnull, obj))
    return ps.isnull(obj)

def not_null(obj: Any) -> Any:
    '''Return the result of passing obj to the Pandas notnull function.
    '''
    py_type = firstwhere([list, set, tuple], partial(isinstance, obj), False)
    if py_type:
        return py_type(map(pd.notnull, obj))
    return pd.notnull(obj)

def first_not_none(iterable, default: Any=False) -> Any:
    '''Return the first element of an iterable that is not None, or a
    default if all elements are None.
    
    Parameters:
    ====================================================================
    iterable: Iterable
        An iterable object compatabile with the built-in filter function
        and the locally-defined is_not_none function.
    default: Any, default=False
        Returned if all elements of iterable are None.
    ====================================================================
    Returns: Any
    '''
    return first_where(iterable, condition=is_not_none, default=default)

def indented(
        text: str, level: int=1, lw: Union[int, None]=79, 
        as_spaces: bool=True, initial: Union[int, None]=None, 
        subsequent: Union[int, None]=None, spaces_per_tab: int=4
) -> str:
    '''Return an indented, text-wrapped string.
    
    Parameters
    ====================================================================
    text: str
        A string containing text to be processed.
    level: int, default=1
        An integer indicating the desired indentation level.
    lw: Union[int, None], default=79
        An integer specifying the desired line width or None. If None,
        the returned text is not wrapped
    
    Returns: str
    '''
    _i, _s = [initial, level], [subsequent, level]
    _i, _s = map(
        partial(first_not_none, default=level), [_i, _s]
    )
    _tab = spaces_per_tab * ' ' if as_spaces else '\t'
    _i, _s = _i * _tab, _s * _tab
    if lw is None:
        return _i + re.sub(r'(\n|\r)', fr'\1{_s}', text)
    return '\n'.join([
        '\n'.join(
            textwrap.wrap(
                text_part, width=lw, initial_indent=_i, subsequent_indent=_s,
                drop_whitespace=False
            )
        ) 
        for text_part in re.split('\n', re.sub(r'\r', r'\n', text))
    ])
def hr(
        characters: str='-', lw: int=79, br: bool=False, indent_level: int=0,
        indent_str: str=TAB, newline_character: str='\n'
) -> str:
    '''Return a string depicting a horizontal rule comprised of a given 
    a character or characters repeated such that the resultant length
    of the string is equal to a given line width.
    
    Parameters
    characters: str, default='-'
        The character(s) to repeat.
    lw: int, default=79
        An integer specifying the desired length of the string.
    br: bool, default=False
        If true, return a horizontal rule followed by the newline
        character '\n'.
    indent_level: int, default=0
        Specifies the indent level at which the horizontal rule begins.
    indent_str: str, default='    '
        A string specifying the indent type. Default is 4 spaces.
    newline_character: str, default='\n'
        A string containing the newline character used when br is True.
        
    Returns: str
    '''
    indent = indent_level * indent_str
    lw = lw - len(indent)
    if lw <= 0:
        return indent + int(br) * newline_character
    rule = ((lw//len(characters) + 1) * characters)[:lw]
    return indent + rule + int(br) * newline_character

def month_length_from_datetime(dt: datetime) -> int:
    '''Return the length of a month in days from a given datetime.
    '''
    return monthrange(dt.year, dt.month)[1]
def datetime_less_parts(
        dt: datetime, keep: list[str]=['year', 'month', 'day']) -> datetime:
    date = dt.date()
    return datetime(
        **{
            attr: getattr(date, attr) if attr in keep else 0
            for attr in ['year', 'month', 'day']
        }, **{
            attr: getattr(dt, attr) if attr in keep else 0
            for attr in ['hour', 'minute', 'second']
        }, **{
            attr: getattr(dt, attr) if attr in keep else 0
            for attr in ['microsecond', 'nanosecond'] if attr in keep
        }
        
    )
def datetime_adjust_resolution(dt: datetime, resolution: str) -> datetime:
    parts = [
        'year', 'month', 'day', 'hour', 'minute', 'second', 'microsecond', 
        'nanosecond'
    ]
    defaults = [1970, 1, 1, 0, 0, 0, 0]
    date = dt.date()
    dt_parts = [
        *[getattr(date, _) for _ in parts[:3]], 
        *[getattr(dt, _) for _ in parts[3:]]
    ]
    res = parts.index(resolution) + 1
    return datetime(**dict(zip(parts, [*dt_parts[:res], *defaults[res:]])))
    
    
def datetime_floor(
        dt: datetime, resolution: DateOffset, 
        reference:Union[datetime, None]=None) -> datetime:
    '''Return a datetime rounded down to the nearest multiple of a given 
    resolution.
    '''
    _date = dt.date()
    if reference is None:
        reference = datetime(
            **{_: getattr(_date, _) for _ in ['year', 'month', 'day']},
            **{_: 0 for _ in ['hour', 'minute', 'second']}
        )   
    dt_range = pd.date_range(
        reference, reference + DateOffset(days=1), freq=resolution
    )
    return dt_range[(dt_range < dt)].max()

def outliers(
        data: pd.Series, 
        reference: {'mean', 'median', 'quantile', 'quantiles'}='quantile',
        threshold_type: {'fixed', 'multiple'}='multiple', threshold: float=1.5,
        threshold_multiple: {'std', 'var', 'iqr'}='iqr', 
        return_type: Union[bool, str, 'extent', 'ratio', list]=str,
        ) -> Union[pd.Series, pd.DataFrame]:
    '''Return a series describing values in a given series in terms of
    their relationship to the distribution of all values in the Series.
    
    Parameters
    data: pd.Series
        A Pandas Series containing numeric data.
    reference: (
            {'mean', 'median', quantile', 'quantiles'}, 
            default='quantile'
    )
        A string in the set depicted above. If 'mean' or 'median',
        outliers are defined as values whose absolute difference from 
        the given aggregate of the provided data is greater than a 
        threshold. If 'quantile' or 'quantiles', low and high outliers 
        are classified as those less than the 25th percentile minus a 
        threshold or greater than the 75th percentile plus a threshold.
    threshold_type: {'fixed', 'multiple'}, default='multiple'
        A string in the set depicted above. If fixed, outliers are 
        defined as any values in data that exist outside of the open 
        interval bound by the reference value(s) ± the value passed to 
        the threshold parameter. If 'multiple', a 'fixed' threshold
        magnitude is calculated using the statistic name passed to the 
        threshold_multiple parameter, such that
        magnitude = threshold * (statistic of data).
    threshold: float, default=1.5
        A float indicating a threshold as described in threshold_type.
    threshold_multiple: {'std', 'var' 'iqr'}, default='iqr'
        A string of the set depicted above, indicating the type of 
        calculations used to establish the thresholds beyond which 
        values in data are classified as outliers. Only relevant when 
        'multiple' is passed as argument to the threshold_type
        parameter.  If threshold_multiple is 'std', and reference is
        'mean', outliers are defined as any values in data whose 
        absolute difference from the mean is greater than x times the 
        standard deviation of data, where x equals the value passed to 
        threshold.
    return_type: (
            Union[{bool, str, 'extent', 'ratio'}, list], default=str
        )
        One of the types or strings depicted in the set above or a list
        thereof. Determines the datatype of the returned Series or 
        datatypes of the returned DataFrame.
        
        bool
            Return a boolean Series indicating whether each value is an
            outlier.
        str
            Return a Series of strings indicating the type of outlier of
            each value. A value in data deemed an outlier is described 
            as 'high' or 'low' at its index in the returned Series. The 
            returned series is null at the index of all non-outliers in
            data.
        extent
            Return a Series of floating point values indicating the
            difference between the value of each outlier and the 
            threshold it exceeded to warrant its classification. The
            returned Series is null at the index of any value in data
            not classified as an outlier.
        ratio
            Return a Series of floating point values describing the
            difference between the value of each outlier and the 
            threshold it exceeded to warrant its classification in terms 
            of the reference value used to set the threshold. When 
            outliers are defined relative to the mean using a threshold 
            set as a multiple of the standard deviation, the returned 
            Series indicates the distance of each outlier from the mean 
            as a multiple of the standard deviation. (A value of -2 
            indicates a low outlier with a value equal to the mean of 
            data minus 2 times the standard deviation of data)
    
    Returns: pd.Series | pd.DataFrame
        A Series or DataFrame as described in return_type.
    '''
    if 'quantile' in reference:
        references = data.quantile([0.25, 0.75])
    else:
        references = data.agg(2 * [reference])
    if threshold_type == 'fixed':
        thresh_ratio_unit, thresh_magnitude = threshold, threshold
        thresholds = references + (np.array([-1, 1]) * threshold)
        ratio_unit_name = f'scalar: {threshold})'
        ratio_base_multiple = 1
    else:
        if threshold_multiple == 'iqr':
            thresh_ratio_unit = np.subtract(*data.quantile([0.75, 0.25]))            
        else:
            thresh_ratio_unit = methodcaller(threshold_multiple)(data)
        thresh_magnitude = threshold * thresh_ratio_unit
        ratio_base_multiple = threshold
        ratio_unit_name = threshold_multiple
        thresholds = references + (np.array([-1, 1]) * thresh_magnitude)
        
    low_thresh, hi_thresh = thresholds
    
    def _is_outlier(value: float) -> bool:
        '''Return a boolean indicating whether a given value is either
        greater than a high threshold or less than a low threshold.
        '''
        return value < low_thresh or value > hi_thresh
    def _outlier_type(
            value: float, outlier_extent: Union[float, None]=None) -> str:
        '''Return a string indicating whether an outlier is high or low.
        '''
        if outlier_extent is not None:
            return {-1: 'low', 0: np.nan, 1: 'high'}[np.sign(outlier_extent)]
        elif value < low_thresh:
            return 'low'
        elif value > hi_thresh:
            return 'high'
    def _outlier_extent(
            value: float, outlier_type: Union[str, None]=None) -> float:
        '''Return the amount by which a given value exceeds the
        threshold by which it was classified as an outlier. If the value 
        is not an outlier or the value is null, return None.
        '''
        if pd.notna(outlier_type):
            if outlier_type=='low':
                return -1 * (low_thresh - value)
            elif outlier_type=='high':
                return value - hi_thresh
        elif pd.notna(value):
            if value < low_thresh:
                return -1 * (low_thresh - value)
            elif value > hi_thresh:
                return value - hi_thresh
    def _outlier_ratio(
            value: float, outlier_extent: Union[float, None]=None) -> str:
        '''Return the multiple of a base quantity by which an outlier
        exceeds a threshold.
        '''
        if pd.isna(outlier_extent):
            outlier_extent = _outlier_extent(value)
        if pd.notna(outlier_extent):
            return (
                np.sign(outlier_extent)
                * (abs(outlier_extent/thresh_ratio_unit) + ratio_base_multiple)
            )

    
    _out_funcs = {
        bool: _is_outlier, str: _outlier_type, 'extent': _outlier_extent,
        'ratio': _outlier_ratio
    }
    _func_names = {
        f'_{name}': name 
        for name in [
            'is_outlier', 'outlier_type', 'outlier_extent', 'outlier_ratio'
        ]
    }
    _col_names = dict(zip(_out_funcs.keys(), _func_names.values()))
    
    if isinstance(return_type, list):
        funcs = [_out_funcs[rt] for rt in return_type]
        outliers = data.copy().apply(funcs).rename(columns=_func_names)
        outliers.attrs['name'] = (
            f'{data.name}{hasattr(data, "name")*"_"}outliers)'
        )
        outliers.rename(
            columns={'outlier_ratio': f'outlier_ratio_{ratio_unit_name}'},
            inplace=True
        )
        return outliers
    else:
        func, name = _out_funcs[return_type], _func_names[return_type]
        if return_type == 'ratio':
            name = f'outlier_ratio_{ratio_unit_name}'
        return pd.Series(
            data=data.apply(func).values, index=data.index,
            name=(f'{data.name}{any(data.name) * "_"}{name}')
        )

def quantile_boundaries(
            data: pd.DataFrame, 
            quantiles: ArrayLike=[0.25, 0.5, 0.75] ) -> pd.DataFrame:
    '''Return a DataFrame comprised of the minimum, each quantile value 
    in a given array, and the maximum of each column in data.
    '''
    names = [
        '_min', 
        *[f'{i:.0f}th_ptile' for i in [100*_ for _ in quantiles]], 
        '_max'
    ]
    aggs = [
        'min', *[methodcaller('quantile', _) for _ in quantiles], 'max'
    ]
    results = pd.DataFrame(index=names, columns=data.columns)
    for name, agg in zip(names, aggs):
        results.loc[name] = data.agg(agg)
    return results
def quantile_groups(
            data: pd.DataFrame, 
            quantiles: ArrayLike=[0.25, 0.5, 0.75]) -> pd.DataFrame:
    '''Return a DataFrame of integers indicating the quantile group of 
    each value in each column of data.
    '''
    q_boundaries = quantile_boundaries(data, quantiles)
    boundary_pairs = {
        col: list(pairwise(q_boundaries.loc[:, col].values))
        for col in q_boundaries.columns
    }
    for col, pairs in boundary_pairs.items():
        for pair, count in {pair: pairs.count(pair) for pair in pairs}.items():
            for i in range(count-1):
                pairs.remove(pair)
        for col, pairs in boundary_pairs.items():
            assert pairs[0][0] == q_boundaries.loc['_min', col]
    include = {col: [] for col in boundary_pairs.keys()}
    for col, pairs in boundary_pairs.items():
        last = 'left'
        for pair in pairs:
            if pair[0] == pair[1]:
                include[col].append('both')
                last = 'both'
            elif last == 'both':
                if pairs.index(pair)==len(pairs)-1:
                    include[col].append('right')
                else:
                    include[col].append('neither')
                    last = 'neither'
            elif pairs.index(pair)==len(pairs)-1:
                include[col].append('both')
            else:
                include[col].append('left')
    columns = [f'{col}_q' for col in data.columns]
    results = pd.DataFrame(index=data.index, columns=columns, dtype=int)
    for col, pairs in boundary_pairs.items():
        inclusive = include[col]
        assert len(pairs)==len(inclusive)
        for i, ((low, hi), inc) in enumerate(zip(pairs, inclusive), 1):
            results.loc[data.index[data[col].between(low, hi, inclusive=inc)], f'{col}_q'] = i
    return results

def kfcv_scores(
        X, y, scorers: list[Callable], model: BaseEstimator,
        names: Union[list[str], None]=None, *args, **kwargs) -> dict:
    scores = {_: [] for _ in scorers}
    kf = KFold(*args, **kwargs)
    for train, test in kf.split(X):
        X_train, X_test = X.iloc[train], X.iloc[test]
        y_train, y_test = y.iloc[train], y.iloc[test]
        model.fit(X_train, y_train)
        y_p = model.predict(X_test)
        for func, score_list in scores.items():
            score_list.append(func(y_test, y_p))
    scores = {k: np.mean(v) for k, v in scores.items()}
    if names is not None:
        return dict(zip(names, scores.values()))
    return scores
    
    
def is_weekend(dt: datetime) -> bool:
    '''Return a boolean indicating whether the day of the week of a 
    given datetime is Saturday or Sunday.
    '''
    return datetime.weekday(dt) >= 5

def is_weekday(dt: datetime) -> bool:
    '''Return a boolean indicating whether the day of the week of a 
    given datetime is not Saturday or Sunday.
    '''
    return datetime.weekday(dt) < 5
    
def clock_degrees(
            hour: Union[int, None]=None, minute: Union[int, None]=None,
            second: Union[float, int, None]=None,
            time: Union[datetime, None]=None) -> float:
    '''Return the location on a circle in degrees for the time attribute
    of a given datetime instance or a sequence of time components.
    '''
    if time is not None:
        hour, minute, second = (
            time.hour, time.minute,
            time.second + time.nanosecond
        )
    hour = hour%12
    minute, second = minute or 0, second or 0
    time = hour + minute/60 + second/3600
    time = time/12 * 360
    return time
    
def clock_radians(
            hour: Union[int, None]=None, minute: Union[int, None]=None,
            second: Union[float, int, None]=None,
            time: Union[datetime, None]=None) -> float:
    '''Return the location on a circle in radians for the time attribute
    of a given datetime instance or a sequence of time components.
    '''
    if time is not None:
        hour, minute, second = (
            time.hour, time.minute,
            time.second + time.nanosecond
        )
    hour = hour%12
    minute, second = minute or 0, second or 0
    time = hour + minute/60 + second/3600
    time = time/12 * 360
    return np.deg2rad(time)

def add_clock_subplot(
        fig: Figure, rect: Union[tuple, None]=None, 
        fontsize: Union[int, None]=None) -> Axes:
    '''Add a polar subplot to a given Figure instance with markings and 
    text representing an analog clock.
    '''
    if rect is None:
        figwidth, figheight = fig.get_figwidth(), fig.get_figheight()
        dims = np.array([figheight, figwidth])
        dims = min(dims/dims.max())
        rect = (1, 0, dims, dims)
        del figwidth, figheight, dims
    if fontsize is None:
        fontsize = fig.get_figheight() * 4
    clock = fig.add_axes(
        rect, projection='polar', aspect='equal', theta_direction=-1, 
        theta_offset=(np.pi/2)
    )
    clock.grid(visible=False)
    clock.set_yticks([], []); clock.set_xticks([], [])
    clock.set_ylim((0, 1))
    clock_hours = [12, *range(1, 12)]
    for hour in clock_hours:
        clock.plot(
            2*[clock_radians(hour)], [0.95, 0.9], color=(0, 0, 0), lw=0.8
        )
        clock.text(
            x=clock_radians(hour), y=0.7, s=f'{hour:.0f}', va='center', 
            ha='center', fontsize=fontsize
        )
        for minute in np.linspace(0, 60, 5, endpoint=False):
            clock.plot(
                2*[clock_radians(hour=hour, minute=minute)], 
                [0.925, 0.9125], color=(0, 0, 0, 0.5), lw=0.5
            )
    clock.bar(0, width=np.deg2rad(360), height=0.025, color='black')
    return clock


    
def plt_gca_set_title(*args, **kwargs) -> None:
    '''Set the plot title on a current figure's axes object.
    '''
    if 'ax' in kwargs:
        ax = kwargs.pop('ax')
    else:
        ax = plt.gca()
    ax.set_title(*args, **kwargs)


set_plot_title = partial(plt_gca_set_title, pad=10, x=0, loc='left')

kfcv = partial(
    kfcv_scores, scorers=[rmse, r2_score, mape], names=[
        'RMSE', 'R2', "Mean absolute % error"
    ]
)

class DataFrameColumnTransformer(BaseEstimator):
    def __init__(
            self, column: str, func: Union[Callable, None, str], args: list=[], 
            kwargs: dict={}):
        self.func = (
            methodcaller(func, *args, **kwargs) if isinstance(func, str)
            else lambda X: func(X, *args, **kwargs)
        )
        self.column = column
        self.args = args
        self.kwargs = kwargs
    def fit(self, X: ArrayLike, y: Union[ArrayLike, None]=None):
        return self
    def transform(
            self, X: ArrayLike, y: Union[ArrayLike, None]=None) -> ArrayLike:
        X[self.column] = self.func(X[self.column])
        return X
    def fit_transform(
            self, X: ArrayLike, y: Union[ArrayLike, None]=None) -> ArrayLike:
        self.fit(X, y)
        return self.transform(X, y)
class AddDerivedFeatureTransformer(BaseEstimator):
    def __init__(
            self, input_feature: str, derived_feature_name: str, 
            func: Callable, output_dtype: Union[str, DTypeLike, None]=None, 
            retain_derived_feature_values: bool=False, 
            drop_input_feature: bool=False):
        self.input_feature = input_feature
        self.derived_feature_name = derived_feature_name
        self.retain_derived_feature_values = retain_derived_feature_values
        self.func = func
        self.output_dtype = output_dtype
        self.drop_input_feature = drop_input_feature
    def fit(self, X: ArrayLike, y: Union[ArrayLike, None]=None):
        if self.retain_derived_feature_values:
            self.derived_feature_values = self.func(
                X[self.input_feature].copy()
            )
        else:
            self.derived_feature_values = None
        return self
    def transform(
            self, X: ArrayLike, y: Union[ArrayLike, None]=None) -> ArrayLike:
        if self.derived_feature_values is not None:
            X[self.derived_feature_name] = self.derived_feature_values
        else:
            X[self.derived_feature_name] = self.func(X[self.input_feature])
            if self.output_dtype is not None:
                X[self.derived_feature_name] = (
                    X[self.derived_feature_name].astype(self.output_dtype)
                )
        if self.drop_input_feature:
            X.drop(columns=[self.input_feature], inplace=True)
        return X
    def fit_transform(
            self, X: ArrayLike, y: Union[ArrayLike, None]=None) -> ArrayLike:
        self.fit(X, y)
        return self.transform(X, y)
class AddDummiesTransformer(BaseEstimator):
    def __init__(
            self, input_feature: str, values: Union[list, dict, None]=None,
            names: Union[list, None]=None, drop_one: bool=False, 
            drop_input_feature: bool=False):
        self.input_feature = input_feature
        if isinstance(values, dict):
            self.values = [*values.keys()]
            self.names = [*values.values()]
        else:
            self.values = values
            self.names = names
        self.drop_one = drop_one
        self.drop_input_feature = drop_input_feature
    def fit(self, X: ArrayLike, y: Union[ArrayLike, None]=None):
        if self.values is None:
            self.values = X[self.input_feature].unique()
            if self.drop_one:
                if pd.Series(self.values).isna().any():
                    self.values = pd.Series(self.values).dropna().values
                else:
                    self.values = self.values[:-1]
        if self.names is None:
            if pd.api.types.is_numeric_dtype(X[self.input_feature]):
                self.names = [f'{self.input_feature}_{v}' for v in self.values]
            elif pd.api.types.is_bool_dtype(X[self.input_feature]):
                self.names = [
                    f'{self.input_feature}_{int(v):.0f}'
                    if not pd.isna(v) else f'{self.input_feature}_is_null'
                    for v in self.values
                ]
            else:
                self.names = self.values
        return self
    def transform(
            self, X: ArrayLike, y: Union[ArrayLike, None]=None) -> ArrayLike:
        for value, name in zip(self.values, self.names):
            if not pd.isna(value):
                X[name] = (X[self.input_feature] == value).astype(int)
            else:
                X[name] = X[self.input_feature].isna().astype(int)
        if self.drop_input_feature:
            return X.drop(columns=[self.input_feature])
        return X
    def fit_transform(
            self, X: ArrayLike, y: Union[ArrayLike, None]=None) -> ArrayLike:
        self.fit(X, y)
        return self.transform(X, y)
class DropNullRecordsTransformer(BaseEstimator):
    def __init__(
            self, method: Union[str, int]='any',
            threshold: Union[int, None]=None,
            retain_dropped_index: bool=False):
        method = 'threshold' if threshold is not None else method
        self.method = method
        self.threshold = threshold
        self.retain_dropped_index = retain_dropped_index
    def fit(self, X: ArrayLike, y: Union[ArrayLike, None]=None) -> ArrayLike:
        if self.retain_dropped_index:
            if self.method == 'any':
                self.mask = X.isna().any(axis=1).copy()
            if self.method == 'all':
                self.mask = X.isna().all(axis=1).copy()
            else:
                self.mask = (X.isna().sum(axis=1)>=threshold).copy()
        else:
            self.mask = None
        return self
    def transform(
            self, X: ArrayLike, y: Union[ArrayLike, None]=None) -> ArrayLike:
        if self.mask is not None:
            X = X[~self.mask]
        else:
            if self.threshold is None:
                X.dropna(how=self.method, inplace=True)
            else:
                X = X[~(X.isna().sum(axis=1) >= self.threshold)]
        return X
    def fit_transform(
            self, X: ArrayLike, y: Union[ArrayLike, None]=None) -> ArrayLike:
        self.fit(X, y)
        return self.transform(X, y)
class SelectiveKNNImputer(BaseEstimator):
    '''A convenience class to allow use of SciKitLearn's KNNImputer to 
    impute null values in a subset of features of a given DataFrame.
    '''
    def __init__(
            self, train_features: ArrayLike=[], impute_features: ArrayLike=[],
            exclude_features: list=[], knn_imputer_args: list=[], 
            knn_imputer_kwargs: dict={}):
        self.train_features = train_features
        self.impute_features = impute_features
        self.exclude_features = exclude_features
        self.knn_imputer_args = knn_imputer_args
        self.knn_imputer_kwargs = knn_imputer_kwargs
        self.imputer = KNNImputer(*knn_imputer_args, **knn_imputer_kwargs)
    def fit(self, X: ArrayLike, y: Union[ArrayLike, None]=None) -> ArrayLike:
        if not any(self.train_features):
            self.train_features = [
                col for col in X.columns if col not in self.exclude_features
            ]
        if not any(self.impute_features):
            self.impute_features = [
                col for col in X.columns if col not in self.exclude_features
            ]
        self.imputer.fit(X[self.train_features])
        return self
    def transform(
            self, X: ArrayLike, y: Union[ArrayLike, None]=None) -> ArrayLike:
        imputed = X[self.train_features].copy()
        imputed[self.train_features] = (
            self.imputer.transform(imputed[self.train_features])
        )
        X[self.impute_features] = imputed[self.impute_features].copy()
        del imputed
        return X
    def fit_transform(
            self, X: ArrayLike, y: Union[ArrayLike, None]=None) -> ArrayLike:
        self.fit(X, y)
        return self.transform(X, y)
        
        
@dataclass
class KFCVClassificationResults:
    name: str
    class_results: dict[dict[str, float]]
    accuracy: float
    macro_avg: dict
    weighted_avg: dict
    
    def report_str(
            self, score_digits: int=4, support_digits: int=0,
            rjust: int=18
    )->str:
        lbl_fmt = max([max(map(len, self.class_results.keys())) + 4, 15])
        line_len = lbl_fmt + 4 * rjust
        lbl_fmt = f'<{lbl_fmt}'
        digits = 3* [f'.{score_digits}f'] + [f',.{support_digits}f']
        score_names =  ['precision', 'recall', 'f1-score', 'support']
        str_fmts = [f'>{rjust}{digit}' for digit in digits]
        report = f'{"":{lbl_fmt}}' + ''.join(
            f'{score_name.replace("-", " "):>{rjust}}' for score_name in
            score_names
        ) + f'{BR}{hr("=", lw=line_len, br=False)}'
        for y_class in self.class_results:
            report = report + f'{BR}{y_class:{lbl_fmt}}' + ''.join(
                f'{self.class_results[y_class][score_name]:{fmt}}'
                for score_name, fmt in zip(score_names, str_fmts)
            )
        report = report + (
            f'{BR}-{BR}{"accuracy":{lbl_fmt}}' + 
            2 * f'{"":{rjust}}' + f'{self.accuracy:{str_fmts[0]}}'
        )
        for avg_name in ['macro_avg', 'weighted_avg']:
            report = (
                report + f'{BR}{avg_name.replace("_", " "):{lbl_fmt}}' 
                + ''.join(
                    f'{getattr(self, avg_name)[score_name]:{fmt}}'
                    for score_name, fmt in zip(score_names, str_fmts)
                )
            )
        del lbl_fmt, score_names, str_fmts
        return f'{report}{BR}{hr("-", lw=line_len)}-'
def kf_classification_report(
        X: ArrayLike, y: ArrayLike, model: BaseEstimator,
        stratify: bool=True, kfold_args: list=[], kfold_kwargs: dict={},
        name: str=''
) -> dict:
    if not name and hasattr(y, 'name'):
        name = y.name
    kf = {False: KFold, True: StratifiedKFold}[stratify]
    kf = kf(*kfold_args, **kfold_kwargs)
    y_classes = sorted([str(_) for _ in pd.unique(y)])
    
    report = {
        category: {
            metric: [] 
            for metric in [
                'precision', 'recall', 'f1-score', 'support'
            ]
        } for category in [
            *y_classes, 'macro avg', 'weighted avg'
        ]
    }
    
    report['accuracy'] = []
    for train, test in kf.split(X, y):
        train, test = y.index[train], y.index[test]
        X_train, X_test = X.loc[train], X.loc[test]
        y_train, y_test = y.loc[train], y.loc[test]
        model.fit(X_train, y_train)
        p = pd.Series(model.predict(X_test), X_test.index)
        cl_report = classification_report(y_test, p, output_dict=True)
        
        for category, details in cl_report.items():
            if category=='accuracy':
                report['accuracy'].append(details)
                continue
            for metric, value in details.items():
                report[category][metric].append(value)
        del X_train, X_test, y_train, y_test, p, cl_report
    for k in [*y_classes, 'macro avg', 'weighted avg']:
        for metric in report[k].keys():
            report[k][metric] = np.mean(report[k][metric])
    report['accuracy'] = np.mean(report['accuracy'])
    report = KFCVClassificationResults(
        name, {y_class: report[y_class] for y_class in y_classes},
        report['accuracy'], report['macro avg'], report['weighted avg']
    )
    del y_classes, kf
    return report
    