import importlib.metadata

from .map_widget import (
    Map,
    MapContent,
    Bookmark,
    Bookmarks,
    Legend,
    LayerList,
    LayerVisibility,
    TimeSlider,
    BasemapManager,
)
from .scene_widget import Scene, SceneContent, Environment
from .group_layer import GroupLayer, SubtypeGroupLayer, SubtypeGroupTable, BaseGroup
from .smart_mapping import SmartMappingManager
from .offline_mapping import OfflineMapAreaManager
from . import popups, renderers, symbols, forms
from ._basemap_styles_service import (
    BasemapStylesService,
    BasemapStyle,
    BasemapStylesLanguage,
    BasemapStylesPlace,
    BasemapStylesWorldview,
)
from ._utils import _HelperMethods, refreshable_property

__all__ = [
    "Map",
    "Scene",
    "popups",
    "renderers",
    "symbols",
    "forms",
    "OfflineMapAreaManager",
    "BasemapStylesService",
    "BasemapStyle",
    "BasemapStylesLanguage",
    "BasemapStylesPlace",
    "BasemapStylesWorldview",
]
try:
    __version__ = importlib.metadata.version("arcgis-mapping")
except importlib.metadata.PackageNotFoundError:
    __version__ = "unknown"
