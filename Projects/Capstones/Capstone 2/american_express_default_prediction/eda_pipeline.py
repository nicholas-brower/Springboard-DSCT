'''Dataclasses, constants, and functions to facilitate exploratory data
analysis in the context of a data science project.

Requires project_palette and type_maps packages.
'''

from typing import Callable, Type
from numbers import Number
from collections.abc import Sequence
from dataclasses import dataclass, field, InitVar
from itertools import chain, cycle
from os import PathLike, path
import numpy as np
from numpy.typing import ArrayLike, DTypeLike
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as plt_colors
import matplotlib.lines as plt_lines
from matplotlib.patches import Patch, Rectangle
from matplotlib.axes import Axes
import matplotlib.scale as plt_scale
from matplotlib.figure import Figure
import seaborn as sns
from project_palette import *
from type_maps import  TYPE_MAP, TYPE_MAP_KEYS

COLOR_CYCLE = cycle(PALETTE)
DARK_PALETTE = []
for rgb in PALETTE_RGB:
    if all([v > 0.5 for v in rgb]):
        while all([v > 0.4 for v in rgb]):
            rgb = np.array(rgb)/1.1
        DARK_PALETTE.append(np.array(rgb)/2)
    else:
        DARK_PALETTE.append(np.array(rgb))
DARK_COLOR_CYCLE = cycle(DARK_PALETTE)
DESAT_PALETTE = [plt_colors.rgb_to_hsv(color) for color in PALETTE]
DESAT_PALETTE = [(h, s/3, v/1.5) for h, s, v in DESAT_PALETTE]
DESAT_PALETTE = [plt_colors.hsv_to_rgb(color) for color in DESAT_PALETTE]
DESAT_CYCLE = cycle(DESAT_PALETTE)


@dataclass
class Line2dProps:
    color: ArrayLike 
    alpha: float = field(default_factory=None)
    lw: float = field(default_factory=None) #linewidth
    ls: str = field(default_factory=None) #linestyle
    marker: str = field(default_factory=str)
    markersize: float = field(default_factory=None)
    zorder: float = field(default_factory=None)
    
    def props(self):
        return{
        key: val for key, val in self.asdict().items()
        if val is not None
    }

@dataclass
class PatchProps:
    facecolor: ArrayLike
    alpha: float = field(init=False)
    lw: float = field(init=False)
    edgecolor: ArrayLike = field(init=False)
    zorder: float
    
    def props(self):
        return{
        key: val for key, val in self.asdict().items()
        if val is not None
    }

@dataclass
class BoxplotProps:
    boxprops: dict = field(default_factory=None)
    medianprops: dict = field(default_factory=None)
    whiskerprops: dict = field(default_factory=None)
    capprops: dict = field(default_factory=None)
    flierprops: dict = field(default_factory=None)
    

@dataclass
class PlotElement:
    label: str = field(default_factory='')
    color: ArrayLike = field(default_factory=None)
    cmap: plt_colors.Colormap = field(default_factory=None)
    line: Line2dProps = field(default_factory=None)
    patch: PatchProps = field(default_factory=None)
    boxplot_props: BoxplotProps = field(default_factory=None)

@dataclass
class TypeMap:
    type_str: InitVar[str]
    env_str: InitVar[str] = ''
    py: Type = field(init=False)
    np: DTypeLike = field(init=False)
    sql: str = field(init=False)
    
    def __post_init__(self):
        if self.env_str:
            key = TYPE_MAP_KEYS['environment'][self.env_str][self.type_str]
        else:
            key = TYPE_MAP_KEYS['type'][self.type_str]
        self.py_type, self.np_type, self.sql_type = TYPE_MAP[key]

@dataclass
class DataFeature:
    name: str
    dtype: DTypeLike = field(default_factory=None)
    source: str = field(default_factory=str)
    col: str = field(default_factory=str)
    f_type: str = field(default_factory=str)
    category: str = field(default_factory=str)
    summary: dict = field(default_factory=dict)
    flgas: list[str] = field(default_factory=list)
    
    def __post_init__(self):
        if not self.col:
            self.col = self.name

@dataclass
class CategoricalSubfeature(DataFeature):
    source_feature: DataFeature = field(default_factory=None)
    value: object = field(default_factory=object)
    key: int = field(default_factory=int)
        
@dataclass
class CategoricalFeature(DataFeature):
    subfeatures: list[DataFeature] = field(default_factory=list)
    subfeature_keys: dict[int:object] = field(default_factory=dict)
    values: list = field(default_factory=list)
    
    def __post_init__(self):
        if self.values and not any([self.subfeatures, self.subfeature_keys]):
            self.values = sorted(self.values, key=lambda item: [
                len(f'{item}'), f'{item}'
            ]
        )
            self.subfeature_keys = dict(enumerate(self.values))
        if self.subfeature_keys and not self.subfeatures:
            self.subfeatures = [
                CategoricalSubfeature(
                    name=f'{self.name}_{key}',
                    value=value,
                    key=key,
                    source_feature=self
                )
                for key, value in self.subfeature_keys.items()
            ]
        if self.subfeature_keys and not self.values:
            self.values = list(self.subfeature_keys.values())

    
@dataclass
class TargetFeatureClass:
    name: str
    feature: DataFeature
    key: int
    value: object
    col: str = field(default_factory=str)
    plot_attrs: PlotElement = field(init=False)
    color: ArrayLike = field(init=False)
    cmap: plt_colors.Colormap = field(init=False)
    line: Line2dProps = field(init=False)
    patch: PatchProps = field(init=False)
    boxplot_props: BoxplotProps = field(init=False)
    label: str = field(init=False)
    
    def __post_init__(self):
        self.plot_attrs = TARGET_PALETTE[self.key]
        self.color = self.plot_attrs.color
        self.cmap = self.plot_atrrs.cmap
        self.line = self.plot_atrrs.line
        self.patch = self.plot_atrrs.patch
        self.boxplot_props = self.plot_atrrs.boxplot_props
        self.label = self.plotattrs.label
        if not col:
            self.col = self.feature.name

    def bool_mask(self, data: pd.DataFrame) -> pd.Series:
        return data[self.feature.name]==self.value
    def filter(self, data: pd.DataFrame) -> pd.DataFrame:
        return data[self.bool_mask(data)]

@dataclass
class TargetFeature(DataFeature):
    classes: list[TargetFeatureClass] = field(default_factory=list)
    
BOXPLOT_DEFAULT = BoxplotProps(
    medianprops = {'lw': 4, 'zorder':10, 'alpha':1},
    boxprops = {'lw':4, 'zorder':3},
    capprops = {'lw': 3, 'zorder': 2, 'alpha': 0.5},
    whiskerprops = {'lw':3, 'alpha': 0.2, 'zorder': 0},
    flierprops = None
)

def arrange_subplots(
        items: Sequence, num_cols: int, 
        figsize: tuple[float]) -> tuple[Figure, tuple[object, Axes]]:
    '''Return a tuple of objects to facilitate plotting a sequence of items
    on a grid of subplots. Render unused plots invisible.
    
    Arguments
    items
        A Sequence object for which each item will be paired with an Axes
        object in a subplot grid.
    num_cols
        An integer specifying the number of columns in each row of subplots.
        If num_cols < len(items), the number of rows created will be
        len(items) // num_cols + 1.
    figsize
        A tuple of numbers passed to the matplotlib.pyplot.subplots argument
        of the same name.

    Returns
        A tuple (fig, rows). fig is a matplotlib.figure.Figure object. rows is
        a zip object pairing each item in items with a subplot Axes object.
    '''
    fig, ax_arr = plt.subplots(
        len(items) // num_cols + bool(len(items)%num_cols)*1,
        num_cols, figsize=figsize
    )
    ax_arr = ax_arr.flat
    extra_plots = (num_cols - len(items)%num_cols)%num_cols
    if extra_plots:
        for _ in range(1, 1 + extra_plots):
            ax_arr[_ * -1].set_axis_off()
    rows = zip(items, ax_arr)
    return fig, rows
def remove_ticks(ax: Axes, ticks: bool=True, ticklabels:bool=True, 
        labels: bool=False) -> None:
    '''Remove ticks, tick labels, and/or axes labels from an axes object.
    
    Arguments
    ax
        An Axes obejct for which elements will be removed.
    ticks
        A boolean specifying whether ticks will be removed.
        Default = True
    ticklabels
        A boolean specifying whether tick labels will be removed.
        Default = True
    labels
        A boolean specifing whether axis labels will be removed.
        Default = False
    
    Returns
    None
    '''
    if ticks:
        ax.set_xticks([])
        ax.set_yticks([])
    if ticklabels:
        ax.set_xticklabels([])
        ax.set_yticklabels([])
        ax.tick_params(axis='both', which='both', width=0, size=0, length=0)
    if labels:
        ax.set_ylabel('')
        ax.set_xlabel('')
def get_desaturated(
        rgb_color: Sequence[float], proportion: Number=None,
        absolute: Number=None) -> ArrayLike:
    '''Return a desaturated form of an rgb color.
    
    Arguments
    rgb_color
        A sequence representing an RGB oclor
    proportion
        A Number between 0 and 1. The return saturation is the original
        saturation multiplied by proportion when proportion is not None.
        Default = None
    absolute
        A Number between 0 and 1. When absolute is not None, the saturation of
        the output color is set to the value passed to absolute. Overrides the
        proportion argument.
        Default = None
    
    Returns
    An ArrayLike RGB color, desaturated as specified above.
    '''
    if not all([val <= 1 for val in rgb_color]):
        rgb_color = np.array(rgb_color) / 255
    hsv_color = plt_colors.rgb_to_hsv(rgb_color)
    h, s, v = [hsv_color[_] for _ in range(3)]
    if absolute is not None:
        s = absolute
    else:
        s = s * proportion
    return plt_color.hsv_to_rgb(np.array([h, s, v]))
def plot_subset(
        df: pd.DataFrame, feature: DataFeature=None, col:str='',
        group_by: str='', aggregate: Callable=np.mean,
        ax: Axes=None) -> tuple[Axes, ArrayLike]:
    '''Return a tuple comprised of an Axes object ax and an ArrayLike sequence
    data. Though they are default arguments, one of either feature or col must
    be specified when calling plot_subset.
    
    Arguments:
    df
        A pandas DataFrame containing the columns specified in other
        arguments.
        Default = None
    feature
        A DataFeature object providing the name of a column in df through its
        col attribute. If feature is None, a string must be passed to col.
        Default = None
    col
        A string providing the name of a column in df. If col is None, col is
        defined as feature.col
        Default = ''
    group_by
        A string providing the name of a column on which df will be grouped
        and aggregated using the aggregate function specified by aggregate.
        Default = ''
    aggregate
        A callable function compatible with the pandas DataFrame.agg() method.
        Default = np.mean
    ax
        An Axes object on which data will be plotted. If ax is None, an Axes
        object is created using matplotlib.pyplot.gca.
        Default = None
    Returns
    A tuple (ax, data) comprised of an Axes object and an ArrayLike object.
      
    '''
    if not ax:
        ax = plt.gca()
    if feature:
        col = feature.col
    if group_by:
        data = df[[group_by, col]].dropna().copy()
        data = data.groupby(group_by)[col].agg(aggregate).to_numpy()
    else:
        data = df[col].dropna().to_numpy()
    return ax, data
def apply_boxplot_colors(
        boxplot: dict, colors: Sequence, resize_caps: bool=True,
        whisker_colors: Sequence=None, cap_colors: Sequence=None,
        median_colors: Sequence=None,
        desaturate_fills: Number=None, desaturate_lines:Number=None) -> None:
    '''Apply colors to boxplot elements given a boxplot object and Sequences
    of RGB color arrays.
    
    Arguments:
    boxplot
        A dictionary of the same structure as the return object of a call to 
        matplotlib.pyplot.boxplot()
    colors
        A sequence of RGB colors
    whisker_colors, cap_colors, median_colors:
        Sequences of RGB colors to apply to each boxplot element. If None, the
        colors Sequence is used for all of these elements.
        Default = None
    desaturate_fills:
        A Number passed to the proportion argument of get_desaturated. If
        no value is specified, fills = colors.
        Default = None
    desaturate_lines:
        A Number passed to the proportion argument of get_desaturated. Applies
        for all line elements in a boxplot in the same manner as 
        desaturate_fills.
        Default = None
     
    Returns
    None
    
    '''
    if not whisker_colors:
        whisker_colors = colors
    if not cap_colors:
        cap_colors = colors
    if desaturate_fills:
        fills = [
            get_desaturated(color, proportion=desaturate_fills)
            for color in colors
        ]
    else:
        fills = colors
    line_colors = colors
    if desaturate_lines:
        whisker_colors, cap_colors, line_colors = [
            [
                get_desaturated(color, proportion=desaturate_lines)
                for color in color_list
            ] for color_list in [whisker_colors, cap_colors, line_colors]
        ]
    for patch, fill, line_color in zip(boxplot['boxes'], fills, line_colors):
        patch.set_facecolor(fill)
        patch.set_edgecolor(line_color)
    for whisker, whisker_color in zip(boxplot['whiskers'], whisker_colors):
        whisker.set_color(whisker_color)
    for cap, cap_color in zip(boxplot['caps'], cap_colors):
        if resize_caps:
            cap.set_xdata(cap.get_xdata() + [-0.225, 0.225])
        cap.set_color(cap_color)
    if median_colors:
        for median, color in zip(boxplot['medians'], median_colors):
            median.set_color(median_color)
def subplot_rows(
        num_cols: int, plot_function: Callable, 
        features: list[DataFeature]=[], cols: list[str]=[],
        figsize: tuple[float] = (13, 1), save_fig: bool=False,
        save_path: PathLike = '', save_name = '', show: bool=True,
        **plot_func_kwargs) -> None:
    '''For a given list of features and num_cols number of columns, generate
    subplots by row for each feature in features, using a given plot function
    and keyword arguments.
    
    Arguments
    num_cols
        An integer specifying the number of columns per row.
    plot_function
        A Callable capable of producing a plot on a given Axes object given
        col and ax arguments.
    features
        A list of DataFeatures to plot. Though they are both default
        arguments, at least one of the arguments features and cols must be 
        specified when calling subplot_rows.
        Default = None
    cols
        A list of strings indicating the names of columns to plot
    figsize
        A tuple of numbers passed to the matplotlib.pyplot.subplots argument
        of the same name. Specifies the dimensions of each row.
        Default = (13, 1)
    save_fig
        A boolean specifying whether to save each row of subplots.
        Default = False
    save_dir
        The relative path to the directory in which the figure will be saved.
        Default = ''
    save_name
        The name stem applied as a prefix to each row's filename.
        Default = ''
    show
        A boolean specifying whether the plot will be shown.
        Default = True
    **plot_func_kwargs
        Keyword arguments passed to plot_function.
     
    Returns
    None
    '''
    if features:
        cols = [feature.col for feature in features]
    for i in range(0, len(cols), num_cols):
        row_features = cols[i:i + num_cols]
        fig, row = arrange_subplots(
            items=row_features, figsize=figsize, num_cols=num_cols
        )
        for col, ax in row:
            plot_function(col=col, ax=ax, **plot_func_kwargs)
        if save_fig:
            with open(f'{save_dir}{save_name}_row_{i}.png', 'w') as png:
                fig.savefig(png)
        if show:
            plt.show()
def scatter_one_by_group(
        y_feature: DataFeature, x_features: list[DataFeature],
        data: pd.DataFrame, target_feature: TargetFeature=None,
        target_classes: list[TargetFeatureClass]=None) -> None:
    '''Plot scatterplots, with y_feature on the y axis and x_feature on the
    x axis for each x_feature in x_features.
    
    Arguments:
    y_feature
        A DataFeature with y data
    x_features
        A list of DatFeatures with x data
    data
        A DataFrame object containing the columns in y_feature and x_features.
    target_feature
        A TargetFeature object. Though they are both default arguments, one of
        the arguments target_feature and target_classes must be specified when
        calling scatter_one_by_group.
        Default = None
    target_classes
        A list comprised of one TargetFeatureClass instance per possible value
        of the target feature. Used to determine plot attributes.
        Default = None
    
    Returns
    None
    '''
    if not target_classes:
        target_classes = target_feature.classes
    fig, ax_ = plt.subplots(1, len(group), figsize =(12, 4))
    ax_ = ax_.flat
    y = y_feature.col
    for x_feature, ax in zip(ax_.flat, x_features):
        x = x_feature.col
        for target in target_classes:
            ax.scatter(
                x=target.filter(data)[x], y=target.filter(data)[y],
                color = target.color
            )
            remove_ticks(ax)
            ax.set_title(x_feature.name)
    ax_[0].set_ylabel(y_feature.name)
    plt.show();

def scatter_groups(
        y_features: list[DataFeature], x_features: list[DataFeature],
        target_feature: TargetFeature=None,
        target_classes: list[TargetFeatureClass]=None) -> None:
    '''Generate a grid of scatterplots, comparing each x_feature against each
    y_feature, encoding target classifications by color.
    
    Arguments
    y_features
        A list of DataFeatures. Each DataFeature in y_features is passed
        to the y_feature argument of scatter_one_by_group.
    x_features
        A list of DataFeatures. This list is passed to the x_features
        argument for each call to scatter_one_by_group.
    target_feature
        A TargetFeature object passed to the target_feature argument of
        scatter_one_by_group.
        Default = None
    target_classes
        A list of TargetFeatureClass objects passed to the target_classes
        argument of scatter_one_by_group
        Default = None
    
    Returns
    None
    '''
    if not target_classes:
        target_classes = target_feature.classes
    for y_feature in y_features:
        scatter_one_by_group(y_feature, x_features, target_classes)

def df_eda_boxplot(
        df: pd.DataFrame, feature: DataFeature=None, col: str='',
        target_feature: TargetFeature=None,
        target_classes: list[TargetFeatureClass]=None, 
        ax: Axes=None) -> None:
    '''For a given feature, generate one boxplot per target feature class.
    Format plot for presentation within a dense subplot grid.
    
    Arguments:
    df
        A DataFrame containing the column to plot.
    feature
        A DataFeature object to plot. Either feature or col must be specified
        when calling eda_boxplot.
        Default = ''
    col
        A string specifyng a column in df.
        Default = ''
    target_feature
        A TargetFeature object used to split col into groups.
        Default = None
    target_classes
        A list of TargetFeatureClass objects used to split col into groups.
        Default = None
    
    Returns
    None
    '''
    '''
    if not ax:
        ax = plt.gca()
    bxp = BOXPLOT_DEFAULT
    data = df[target]
    boxplot = ax.boxplot(
        
        sym='', 
        widths=0.9,
        patch_artist=True,
        boxprops=bxp.boxprops,
        medianprops=bxp.medianprops, 
        capprops=bxp.capprops,
        whiskerprops=bxp.whiskerprops
    )
    for patch, target_class in zip(boxplot['boxes'], target_classes):
        patch.set_facecolor(target_class.color)
        patch.set_edgecolor(target_class.color)
    for cap, target_class in zip(boxplot['caps'], target_classes):
        cap.set_xdata(cap.get_xdata() + [-0.225, 0.225])
        cap.set_color(target_class.color)
    remove_ticks(ax)
    '''
    return
def compare_boxplots(
        data: Sequence[ArrayLike], colors: Sequence, ax: Axes=None, 
        labels: Sequence[str]=[], 
        boxplot_props: BoxplotProps=BOXPLOT_DEFAULT) -> None:
    bxp = boxplot_props
    if not ax:
        ax = plt.gca()
    if not labels:
        labels= ['' for _ in range(len(data))]
    boxplot = ax.boxplot(
        x=data, sym='', patch_artist=True, widths=0.9, boxprops=bxp.boxprops, 
        medianprops=bxp.medianprops, capprops=bxp.capprops, 
        whiskerprops=bxp.whiskerprops, labels=labels
    )
    apply_boxplot_colors(boxplot, colors)
def compare_distributions(
        df_a: pd.DataFrame, df_b: pd.DataFrame,
        ax: Axes=None, feature: DataFeature=None, col: str= '', 
        density: bool=True, log: bool=False, y_offset: float=0.2, 
        palette: list=PALETTE_HEX) -> None:
    '''Compare the distribution of values for a feature between provided
    train and test datasets using a histogram.
    '''
    c1, c2 = [palette[1], palette[3]]
    if ax is None:
        ax = plt.gca()
    if feature:
        col = feature.col
    xlim = [min([df_a[col].min(), df_b[col].min()]), max([df_a[col].max(), df_b[col].max()])]
    if len(df_a[col].dropna().unique()) < 100:
        bins = min(len(df_a[col].dropna().unique()), 12)
    else:
        bins = 100
    ax.hist(df_a[col], color=c1, alpha=0.5, zorder=2, bins=bins, density=density, log=log)
    ax.hist(df_b[col], color=c2, alpha=1, zorder=0, bins=bins, density=density, log=log)
    ax.set_title(col)
    if xlim[0] != xlim[1]:
        ax.set_xlim(xlim)
    remove_ticks(ax)
def compare_weighted_dist(
        df_a: pd.DataFrame, df_b: pd.DataFrame, id_col: str, agg: Callable,
        ax: Axes=None, feature: DataFeature=None, col: str= '', density: bool=True,
        log: bool=False, y_offset: float=0.2, palette: list=PALETTE_HEX) -> None:
    '''Compare the distribution of values for a feature between provided
    train and test datasets using a histogram, aggregating value by an
    identifier column using an aggregate function.
    '''
    c1, c2 = [palette[1], palette[3]]
    if ax is None:
        ax = plt.gca()
    if feature:
        col = feature.col
    data_a, data_b = [df.groupby(id_col)[col].agg(agg) for df in [df_a, df_b]]
    xlim = [min([data_a.min(), data_b.min()]), max([data_a.max(), data_b.max()])]
    if len(data_a.dropna().unique()) < 100:
        bins = min(len(data_a.dropna().unique()) + 1, 12)
    else:
        bins = 50
    ax.hist(
        data_a, color=c1, alpha=0.5, zorder=2, bins=bins, 
        density=density, log=log
    )
    ylim = ax.get_ylim()
    height = y_offset * (ylim[1] - ylim[0])
    n, bins, patches = ax.hist(
        data_b, color=c2, alpha=1, zorder=0, bins=bins, density=density, 
        log=log
    )
    ax.set_title(col)
    if xlim[0] != xlim[1]:
        ax.set_xlim(xlim)
    remove_ticks(ax)
def quick_hist(
        df: pd.DataFrame, ax: Axes=None, feature: DataFeature=None,
        col: str='', density: bool=True, y_scale: str= '', log: bool=False,
        color: object=None, aggregate: Callable=np.mean, group_by: str='',
        bin_range: list[float]=[0.5, 99.5], ticks_visible: bool=False, 
        add_title: bool=True, remove_axes: bool=False, bin_calc: str='doane',
        color_cycle: cycle=DESAT_CYCLE) -> None:
    '''A quick histogram
    '''
    ax, data = plot_subset(
        df=df, ax=ax, feature=feature, col=col, aggregate=aggregate,
        group_by=group_by
    )
    bin_range = np.percentile(data, bin_range)
    color = next(color_cycle)
    ax.hist(
        data, 
        bins=np.histogram_bin_edges(
            data, bins=bin_calc, range=bin_range
        ),
        density=density, color=color, log=log
    )
    if y_scale:
        ax.set_yscale(y_scale)
    if not ticks_visible:
        remove_ticks(ax)
    if remove_axes:
        ax.axis('off')
    if add_title:
        ax.set_title(col)
def percentile_hist(
        df: pd.DataFrame,
        feature: DataFeature=None,
        col: str='',
        percentile_bins: list[tuple[float, float, int]]=[],
        group_by: str='',
        density: bool=True,
        aggregate: Callable=np.mean,
        ax: Axes=None,
        add_title: bool=True,
        y_scale: str='',
        ticks_visible: bool=False,
        palette: list=PALETTE_HEX) -> None:
    '''Create a histogram with varying bin width for a given feature.
    
    Arguments:
    df
        The source DataFrame.
    feature
        A compatible DataFeature. Though they are both default arguments, at
        least one of feature and col must be specified when calling
        percentile_hist.
        Default = None
    col
        A string indicating the column in df to plot.
        Default = ''
    percentile_bins
        A list of tuples (start, stop, steps) compatible with the positional
        arguments of numpy's linspace function. Start and stop indicate
        percentiles between which a percentile is calculated for every step
        in the evenly spaced steps between start and stop. start and stop must 
        be compatible with the q argument of numpy's percentile function. The
        tuples of percentile bins are passed as positional arguments to
        linspace, the return array of which is passed to percentile.
    group_by
        A string indicating the column to group df by. If group_by is not the
        empty string '', df is aggregated by the group_by column using the
        aggregate function before being passed to hist.
    density
        Equivalent to the density argument in numpy's histogram and
        matplotlib.pyplot's hist functions. 
        Default = True
    aggregate
        A callable capable of returning an array from an array.
        Default = numpy.mean
    ax
        A matplotlib.axes.Axes object.
        Default = None
    add_title
        A boolean. When true, add a title to the plot
        Default = True
    y_scale
        A string compatbile with ax.set_yscale. If nothing is passed, the
        default y scale is preserved.
        Default = ''
    ticks_visible:
        A boolean indicating whether tick marks are visible in the resultant
        plot.
        Default = False
    palette
        A list of matplotlib.pyplot compatible colors. Default value is
        supplied by a constant imported per project.
        Default = PALLETTE_HEX
        
    '''
    ax, data = plot_subset(
        df=df, feature=feature, col=col, group_by=group_by,
        aggregate=aggregate, ax=ax
    )
    if not percentile_bins:
        percentile_bins = [
            (2, 10, 2), (10, 25, 10), (25, 75, 100), (75, 90, 10), (90, 98, 2)
        ]
    percentile_bins = np.hstack([
        np.linspace(start, stop, steps, endpoint=False)
        for start, stop, steps in percentile_bins
    ])
    col_bins = np.percentile(data, percentile_bins)
    x_range = [col_bins[0], col_bins[-1]]
    ax.hist(data, bins=col_bins, density=density, color=palette[0], range=x_range)
    if add_title:
        ax.set_title(col)
    if y_scale:
        ax.set_yscale(y_scale)
    if not ticks_visible:
        remove_ticks(ax)
