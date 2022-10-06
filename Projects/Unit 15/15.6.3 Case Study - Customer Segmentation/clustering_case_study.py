import calendar
from dataclasses import dataclass, field
from collections.abc import Sequence, ItemsView
from textwrap import fill
from typing import Union
from numbers import Number
import bisect
import numpy as np
from numpy.typing import ArrayLike
import pandas as pd
from mpl_toolkits import mplot3d
import matplotlib.pyplot as plt
import matplotlib.cm as cm
from matplotlib.axes import Axes
from matplotlib.colors \
    import Colormap, to_rgb, LinearSegmentedColormap, rgb2hex
import seaborn as sns
from country_abbrev import ctry_to_iso_3166_1_a3
from scipy.spatial.distance import cityblock, euclidean
from scipy.cluster.hierarchy import fcluster, linkage, dendrogram, \
    set_link_color_palette
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.base import BaseEstimator
from sklearn.decomposition import PCA
from sklearn.cluster import AffinityPropagation, AgglomerativeClustering, \
    DBSCAN, KMeans, SpectralClustering
from sklearn.pipeline import Pipeline
from sklearn.metrics import silhouette_samples, silhouette_score

# Constants

VARIETALS = [
    'Pinot Grigio', 'Chardonnay', 'Espumante', 'Champagne', 'Prosecco', 
    'Cabernet Sauvignon', 'Merlot', 'Pinot Noir', 'Malbec'
]
W_COLORS = [to_rgb(_) for _ in 
            ['#ECE9CA', '#D9D49A', '#F1DD46', '#E7B54B', '#EDBF9C', 
             '#911F45', '#682036', '#6A0E27', '#451B29']]
PALETTE = dict(zip(VARIETALS, W_COLORS))
CMAP = LinearSegmentedColormap.from_list(
    'w', list(PALETTE.values()), 500
)
GRADS = [
    LinearSegmentedColormap.from_list(
        f'w_{i}', [np.hstack([g, [_]]) for _ in [0.7, 1]], N=500
    ) for i, g in enumerate(W_COLORS)
]
CAT_CMAP = [
    [0.45, 0.14, 0.29], [0.78, 0.72, 0.71], [0.05, 0.49, 0.89], 
    [0.95, 0.87, 0.27], [0.0, 0.74, 0.58], [0.93, 0.75, 0.61], 
    [0.2, 0.2, 0.6], [0.33, 0.87, 0.47], [0.56, 0.44, 0.41], 
    [0.91, 0.71, 0.29], [0.67, 0.57, 0.42], [0.89, 0.85, 0.54], 
    [0.64, 0.18, 0.26], [0.77, 0.95, 0.55]
]
CAT_HEX = [rgb2hex(CAT_CMAP[i]) for i in [0, 3, 4, 8, 5, 9, 1, -1]]
set_link_color_palette(CAT_HEX)

# Functions

def hr(divider: str='=', width:int=79, newline:bool=True) -> str:
    '''Return a horizontal rule. Functionally similar to html's <hr>.
    '''
    return (divider * (width//len(divider) + 1))[:width] + int(newline) * '\n'
def est_pop_std(numeric_array: Sequence[Number]) -> float:
    return np.std(numeric_array, ddof=1)
def clear_empty_subplots(data: Sequence, axs: Sequence[Axes]) -> None:
    '''Disable each empty axes for a subplot grid.
    '''
    empty_axs = len(axs) - len(data)
    if empty_axs:
        for r_ind in range(1, empty_axs + 1):
            axs[-1 * r_ind].set_axis_off()
def arr_as_txt(
        arr: Sequence, i_ind: int=0, s_ind: int=0, w: int=70) -> str:
    '''Return an array formatted as a string for readability.
    '''
    a = ', '.join([f'{_}' for _ in arr])
    i_ind, s_ind = i_ind * ' ', s_ind * ' '
    return fill(a, width=w, initial_indent=i_ind, subsequent_indent=s_ind)
def interpret_silh_score(silh_score: float) -> str:
    '''Return a qualitative assessment of the structure identified by
    a clustering operation given its clusters' average silhouette score.
    '''
    i = bisect.bisect([0.26, 0.51, 0.71], silh_score)
    return ['insubstantial', 'weak', 'reasonable', 'strong'][i]
def varietal_legend(palette: dict = PALETTE) -> None:
    fig, ax = plt.subplots(figsize=(0.5, 3))
    for i, (wine, color) in enumerate(palette.items()):
        ax.barh(y=i, width=1, height=0.7, color=color, edgecolor=np.array(color)/1.3)
    ax.set_yticks(range(len(palette)), palette.keys());
    ax.grid(visible=False); ax.set_xticks([]), ax.spines[:].set_color([0]*4)
    ax.tick_params(axis='both', which='both', length=0)
    ax.yaxis.tick_right(); ax.set_xlim([0.2, 0.4])
    plt.show();
def clustering_scatterplot(
        pc_1: ArrayLike, pc_2: ArrayLike, labels: ArrayLike,
        n: int, cmap: object=None, palette: dict=None, ax:Axes=None,
        xlabel: str='PC 1', ylabel: str='PC2', title: str='') -> None:
    if ax is None:
        fig, ax = plt.subplots(figsize=(4, 4))
    ax.grid(visible=True, zorder=-2); ax.spines[:].set_edgecolor([0.5, 0.5, 0.5, 0])
    ax.tick_params(axis='both', which='both', length=0)
    if palette is not None:
        color = [palette[label] for label in labels]
    else:
        if cmap is None:
            palette = CAT_CMAP
            color = [palette[label] for label in labels]
        else:
            color = [cmap(label) for label in labels]
    ax.set_facecolor([0.93, 0.93, 0.93])
    ax.scatter(x=pc_1, y=pc_2, zorder=100, color=color)
    ax.set_xlabel(xlabel, labelpad=12); ax.set_ylabel(ylabel, labelpad=12)
    ax.set_yticks(ax.get_yticks(), labels=['' for _ in ax.get_yticks()])
    ax.set_xticks(ax.get_xticks(), labels=['' for _ in ax.get_xticks()])
    if not title:
        title = f'{n} Clusters'
    ax.set_title(title, pad=12)
    plt.tight_layout()
def linkage_matrix(clusterer: BaseEstimator) -> ArrayLike:
    '''Return a linkage matrix for a given agglomerative clusterer.
    '''
    counts = np.zeros(clusterer.children_.shape[0])
    num_labels = len(clusterer.labels_)
    for i, merge in enumerate(clusterer.children_):
        j = 0
        for child_idx in merge:
            if child_idx < num_labels:
                j += 1
            else:
                j += counts[child_idx - num_labels]
        counts[i] = j
    return np.column_stack(
        [clusterer.children_, clusterer.distances_, counts]
    ).astype(float)
def dendrogram_from_linkage(
        linkage_matrix: ArrayLike, ax: Axes=None, **dendrogram_kwargs) -> None:
    '''Plot a dendrogram from given clusterer on a given Axes.
    '''
    if ax is None:
        ax = plt.gca()
    dendrogram(linkage_matrix, ax=ax, **dendrogram_kwargs)
def dendrogram_from_clusterer(
        clusterer: BaseEstimator, ax: Axes=None, **dendrogram_kwargs) -> None:
    dendrogram_from_linkage(
        linkage_matrix(clusterer), ax=ax, **dendrogram_kwargs
    )

# Classes

@dataclass
class KMeansOption:
    '''A dataclass to streamline K Means clustering operations and provide
    framework for models of a KMeansGroup instance. See KMeansGroup.
    '''
    k: int
    X: ArrayLike
    _kwargs_: dict = field(default_factory=dict)
    labels: ArrayLike = field(init=False)
    clusterer: BaseEstimator = field(init=False)
    inertia: float = field(init=False)
    silhouette_score: float = field(init=False)
    silhouette_samples: ArrayLike = field(init=False)
    info: tuple[int, float, float] = field(init=False)
    scores: tuple[float, float] = field(init=False)
    sil_samples: ArrayLike = field(init=False)
    
    def __post_init__(self):
        self.X = self.X.copy()
        self.clusterer = KMeans(
            n_clusters=self.k, random_state=123, *self._kwargs_
        )
        self.labels = self.clusterer.fit_predict(self.X)
        self.inertia = self.clusterer.inertia_
        self.silhouette_score = silhouette_score(self.X, self.labels)
        self.info = (self.k, self.inertia, self.silhouette_score)
        self.scores = (self.inertia, self.silhouette_score)
        self.silhouette_samples = silhouette_samples(self.X, self.labels)
        self.sil_samples = self.silhouette_samples

@dataclass
class KMeansGroup:
    '''A dataclass to facilitate analysis and evaluation of K Means clustering
    results for given data X clustered at each of a given range of K values.
    '''
    X: ArrayLike
    k_list: list[int] = field(default_factory=list)
    models: dict[int, KMeansOption] = field(default_factory=dict)
    
    def __post_init__(self):
        self.X = self.X.copy()
        if not self.models:
            self.models = {k_: KMeansOption(k_, self.X) for k_ in self.k_list}
        
    def describe() -> str:
        header = f'{f"k":<5}{f"inertia":>14}{f"silhouette":>14}\n{"":-<35}\n'
        table = '\n'.join(
            f'{k:<5}{inertia:>14.3f}{score:>14.3f}' for k, inertia, score 
            in model.info for model in self.models.values()
        )
        return header + table
    
    def sil_subplot_rows(self, ncols: int, cmap: Colormap=None) -> None:
        '''Generate a grid of silhouette plots for each of the values in
        self.k_list. Adapted from code presented at:
            URL(https://scikit-learn.org/stable/auto_examples/cluster
                /plot_kmeans_silhouette_analysis.html)
        '''
        cmap = cm.terrain if cmap is None else cmap
        nrows = len(self.k_list) // ncols + bool(len(self.k_list) % ncols)
        fig, axs = plt.subplots(nrows, ncols, figsize=(12, nrows*4))
        axs = axs.flat
        clear_empty_subplots(self.k_list, axs)
        ax_i, ax_j = 0, range(0, nrows * ncols + 1, ncols)
        for (k, kmm), ax in zip(self.models.items(), axs):
            x_lower = 10
            ax.tick_params(axis='both', which='both', length=0, pad=8)
            ax.set_title(f"K = {k}", pad=8)
            ax.set_facecolor([0.93, 0.93, 0.93])
            ax.grid(color=[0.83, 0.83, 0.83], zorder=-1, alpha=0.5)
            for i in range(k):
                ith_cluster_sil_values = kmm.sil_samples[kmm.labels == i]
                ith_cluster_sil_values.sort()
                size_cluster_i = ith_cluster_sil_values.shape[0]
                x_upper = x_lower + size_cluster_i
                color = cmap(float(i) / k)
                ax.fill_between(
                    np.arange(x_lower, x_upper), 0, ith_cluster_sil_values,
                    facecolor=color, edgecolor=color, alpha=0.75, label=f'{i}',
                    zorder=2
                )
                x_lower = x_upper + 10
            if ax_i in ax_j:
                ax.set_ylabel('Silhouette coefficient value'); ax.set_xlabel('')
                ax.set_yticks(np.arange(-0.1, 0.7, 0.1))
            else:
                ax.set_ylabel(''); ax.set_xlabel(''); 
                ax.set_yticks(np.arange(-0.1, 0.7, 0.1), [''] * 8)
            # The horizontal line for average silhouette score of all the values
            ax.axhline(y=kmm.silhouette_score, color="red", linestyle="--", zorder=4)
            ax.set_ylim([-0.2, 0.6])
            ax.set_xticks([]); ax.spines[:].set_color((1, 1, 1, 0));
            ax_i += 1
        fig.suptitle('Clustering Silhouette Analysis by K Value')
        fig.tight_layout()
        plt.show()
@dataclass
class ClusteringOption:
    '''A dataclass to facilitate comparison of clustering outcomes between
    distinct clustering methods.
    '''
    x: ArrayLike
    x_id: ArrayLike
    id_col: str
    data: pd.DataFrame   
    clusterer: BaseEstimator
    labels: ArrayLike = field(init=False)
    clustering_details: pd.DataFrame = field(init=False)
    avg_silhouette_score: float = field(init=False)

    def __post_init__(self):
        self.labels = self.clusterer.fit_predict(X=self.x)
        if len(set(self.labels)) < 2:
            self.avg_silhouette_score = np.nan
        else:
            self.avg_silhouette_score = silhouette_score(X=self.x, labels=self.labels)
        self.clustering_details = (
            pd.DataFrame({self.id_col: self.x_id, 'cluster_id': self.labels}).merge(
                self.data, how='left', left_on=self.id_col, right_on=self.id_col
            )
        )

@dataclass
class ClusteringOptionGroup:
    x: ArrayLike
    x_id: ArrayLike
    id_col: str
    data: pd.DataFrame
    clusterers: list[BaseEstimator]
    names: list[str]
    options: dict = field(default_factory=dict)
    op_items: ItemsView = field(init=False)
    
    def __post_init__(self):
        for name, clusterer in zip(self.names, self.clusterers):
            self.options[name] = ClusteringOption(
                self.x, self.x_id, self.id_col, self.data, clusterer
            )
        self.op_items = self.options.items()