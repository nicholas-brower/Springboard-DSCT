import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from itertools import chain


# constants

PALETTE_HEX = '#00175A #006FCF #A6CDAB #FDB92D #D5C37C #B9BFD1 #736E66'.split()
PALETTE_RGB = [
    (0, 23, 90), (0, 111, 207), (166, 205, 171), (253, 185, 45), 
    (213, 195, 124), (185, 191, 209), (115, 110, 102)
]
PALETTE_RGB = [np.array(_)/255 for _ in PALETTE_RGB]
PALETTE = PALETTE_RGB
PALETTE_RGBA = [list(_) + [1] for _ in PALETTE_RGB]


#functions

def show_palette(palette: list[str], figsize: tuple[int] = (12, 0.35)) -> None:
    '''Plot colors of a given palette object
    '''
    plt.figure(figsize=figsize)
    for i in range(len(palette)):
        color = palette[i]
        plt.bar(x=i, height=1, width=1, color=color)
    plt.xlim(-0.5, len(palette) -0.5)
    plt.ylim(0, figsize[-1]); plt.xticks([]); plt.yticks([])
    plt.show();
def show_cmaps(cmaps: list[mcolors.Colormap]) -> None: 
    fig, ax = plt.subplots(len(cmaps), 1, figsize=(12, 1))
    gradient = np.linspace(0, 1, 256)
    gradient = np.vstack((gradient, gradient))
    for ax_, cmap in zip(ax.flat, cmaps):
        ax_.imshow(gradient, aspect='auto', cmap=cmap)
        ax_.set_xticks([]), ax_.set_yticks([]);
    plt.subplots_adjust(wspace=None, hspace=None)
def show_project_colors(
        palettes: list[list], cmaps: list[mcolors.Colormap], 
        barsize: tuple[int] = (12, 0.35)) -> None:
    num_items = len(cmaps) + len(palettes)
    figsize = (barsize[0], barsize[1] * num_items)
    fig, ax = plt.subplots(len(palettes) + len(cmaps), 1, figsize=figsize)
    for i, palette in enumerate(palettes):
        for j in range(len(palette)):
            ax[i].bar(x=j, height=1, width=1, color=palette[j])
        ax[i].set_xlim(-0.5, len(palette) - 0.5); ax[i].set_ylim(0, barsize[-1])
        ax[i].set_xticks([]); ax[i].set_yticks([])
    gradient = np.linspace(0, 1, 256)
    gradient = np.vstack((gradient, gradient))
    for i, cmap in enumerate(cmaps, len(palettes)):
        ax[i].imshow(gradient, aspect='auto', cmap=cmap)
        ax[i].set_xticks([]); ax[i].set_yticks([])
    plt.show();
    



#colormaps


cmap0, cmap1 = [
    mcolors.LinearSegmentedColormap.from_list(map_name, cmap, N=500)
    for map_name, cmap in zip(
        ('cmap0', 'cmap1'),
        [[[0.0, 0.43529411764705883, 0.8117647058823529, 0.0], 
          [0.0, 0.43529411764705883, 0.8117647058823529, 0.15],
          [0.0, 0.43529411764705883, 0.8117647058823529, 0.25],
          [0.0, 0.43529411764705883, 0.8117647058823529, 0.35],
          [0.0, 0.43529411764705883, 0.8117647058823529, 0.35],
          [0.0, 0.43529411764705883, 0.8117647058823529, 0.4],
          [0.0, 0.43529411764705883, 0.8117647058823529, 0.45],
          [0.0, 0, 1, 0.5]],
         [[0.9921568627450981, 0.7254901960784313, 0.17647058823529413, 0.0], 
          [0.9921568627450981, 0.7254901960784313, 0.17647058823529413, 0.15], 
          [0.9921568627450981, 0.7254901960784313, 0.17647058823529413, 0.25],
          [0.9921568627450981, 0.7254901960784313, 0.17647058823529413, 0.35],
          [0.9921568627450981, 0.7254901960784313, 0.17647058823529413, 0.4],
          [0.9921568627450981, 0.7254901960784313, 0.17647058823529413, 0.45],
          [1, 1, 0.17647058823529413, 0.5]]
        ]
    )
]

TARGET_PALETTE = {
    0: {
        'label': 'no default', 
        'color': PALETTE_RGB[1], 
        'cmap': cmap0,
        'value': 0
    },
    1: {
        'label': 'default',
        'color': PALETTE_RGB[3],
        'cmap': cmap1,
        'value': 1
    }
}



#print('palettes and colormaps:')
show_project_colors([PALETTE_HEX], [cmap0, cmap1])