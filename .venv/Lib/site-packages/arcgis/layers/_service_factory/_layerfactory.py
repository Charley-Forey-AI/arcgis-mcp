"""
Generates Layer Types from the given inputs.

"""

from __future__ import annotations
from __future__ import absolute_import
import os
from arcgis.auth.tools import LazyLoader

from urllib.parse import urlparse
from arcgis.gis import GIS
from arcgis.features.layer import (
    FeatureLayer,
    FeatureLayerCollection,
    OrientedImageryLayer,
    Table,
)
from arcgis.geocoding import Geocoder
from arcgis.geoprocessing._tool import Toolbox
from arcgis.geoprocessing import import_toolbox as _import_toolbox
from arcgis._impl.tools import _GeometryService as GeometryService
from arcgis.network import NetworkDataset
from arcgis.gis import Layer
from arcgis.layers import VectorTileLayer
from arcgis.layers import MapImageLayer, MapServiceLayer
from arcgis.raster import ImageryLayer
from arcgis.schematics import SchematicLayers
from arcgis.layers._scenelyrs import SceneLayer
from arcgis.graph._service import KnowledgeGraph
from ...gis._impl._con import Connection
from ...gis.server._service._geodataservice import GeoData
import requests
from types import LambdaType
from arcgis.gis._impl._util import _get_item_url

_arcgis = LazyLoader("arcgis")


###########################################################################
class _DataServiceUrlFactory(type):
    """
    A factory that handles URLs that endwith /data.  This would normally be Item
    urls where the Item type is not known coming from the webmap.

    ==================     ====================================================================
    **Parameter**           **Description**
    ------------------     --------------------------------------------------------------------
    url                    Required string, specify the url ending in /data
    ------------------     --------------------------------------------------------------------
    gis                    Optional :class:`~arcgis.gis.GIS`  object. If not specified, the active GIS connection is
                           used.
    ==================     ====================================================================

    """

    def __call__(cls, url, gis=None):
        if hasattr(url, "url") and getattr(url, "url"):
            #  item object is given for some reason
            url = url.url
        if url.lower().endswith("/data"):
            item_url: str = url.replace("/data", "")
            resp: requests.Response = gis.session.get(
                item_url,
                params={
                    "f": "json",
                },
            )
            data: dict = resp.json()
            if data["type"] in ["KML", "KML Collection"]:
                from .._ogc import KMLLayer

                return KMLLayer(url=url, gis=gis)
            elif data["type"] in ["GeoJSON", "GeoJson"]:
                from .._ogc import GeoJSONLayer

                return GeoJSONLayer(url=url, gis=gis)
            elif data["type"] == "CSV":
                from .._ogc import CSVLayer

                return CSVLayer(url=url, gis=gis)
        else:
            raise ValueError("Invalid URL. The URL for this factory must end in /data")


###########################################################################
class DataServiceLayer(Layer, metaclass=_DataServiceUrlFactory):
    """
    The ``SceneLayer`` class represents a Web scene layer.

    .. note::
        Web scene layers are cached web layers that are optimized for displaying a large amount of 2D and 3D features.

    .. note::
        Web scene layers can be used to represent 3D points, point clouds, 3D objects and
        integrated mesh layers.

    ==================     ====================================================================
    **Parameter**           **Description**
    ------------------     --------------------------------------------------------------------
    url                    Required string, specify the url ending in /SceneServer/
    ------------------     --------------------------------------------------------------------
    gis                    Optional :class:`~arcgis.gis.GIS`  object. If not specified, the active GIS connection is
                           used.
    ==================     ====================================================================

    .. code-block:: python

        # USAGE EXAMPLE 1: Instantiating a SceneLayer object

        from arcgis.layers import SceneLayer
        s_layer = SceneLayer(url='https://your_portal.com/arcgis/rest/services/service_name/SceneServer/')

        type(s_layer)
        >> arcgis.layers.PointCloudLayer

        print(s_layer.properties.layers[0].name)
        >> 'your layer name'
    """

    def __init__(self, url: str, gis=None):
        """
        Constructs a SceneLayer given a web scene layer URL
        """
        super().__init__(url=url, gis=gis)


###########################################################################
class _FeatureServiceLayerFactory(type):
    """
    Factory that generates the Scene Layers

    ==================     ====================================================================
    **Parameter**           **Description**
    ------------------     --------------------------------------------------------------------
    url                    Required string, specify the url ending in /FeatureServer/
    ------------------     --------------------------------------------------------------------
    gis                    Optional :class:`~arcgis.gis.GIS`  object. If not specified, the active GIS connection is
                           used.
    ==================     ====================================================================

    .. code-block:: python

        # USAGE EXAMPLE 1: Instantiating a SceneLayer object

        from arcgis.layers import SceneLayer
        s_layer = FeatureServiceLayer(url='https://your_portal.com/arcgis/rest/services/service_name/FeatureServer')

        type(s_layer)
        >> arcgis.layers.PointCloudLayer

        print(s_layer.properties.layers[0].name)
        >> 'your layer name'
    """

    def __call__(cls, url, gis=None):
        lyr = Layer(url=url, gis=gis)
        props = lyr.properties
        if props["type"] in ["Feature Layer", "Catalog Layer"]:
            return FeatureLayer(url, gis=gis)
        elif props["type"] == "Oriented Imagery Layer":
            return OrientedImageryLayer(url, gis=gis)
        elif props["type"] == "Table":
            return Table(url, gis=gis)

        return lyr


###########################################################################
class FeatureServiceLayer(Layer, metaclass=_FeatureServiceLayerFactory):
    """
    The ``SceneLayer`` class represents a Web scene layer.

    .. note::
        Web scene layers are cached web layers that are optimized for displaying a large amount of 2D and 3D features.

    .. note::
        Web scene layers can be used to represent 3D points, point clouds, 3D objects and
        integrated mesh layers.

    ==================     ====================================================================
    **Parameter**           **Description**
    ------------------     --------------------------------------------------------------------
    url                    Required string, specify the url ending in /SceneServer/
    ------------------     --------------------------------------------------------------------
    gis                    Optional :class:`~arcgis.gis.GIS`  object. If not specified, the active GIS connection is
                           used.
    ==================     ====================================================================

    .. code-block:: python

        # USAGE EXAMPLE 1: Instantiating a SceneLayer object

        from arcgis.layers import SceneLayer
        s_layer = SceneLayer(url='https://your_portal.com/arcgis/rest/services/service_name/SceneServer/')

        type(s_layer)
        >> arcgis.layers.PointCloudLayer

        print(s_layer.properties.layers[0].name)
        >> 'your layer name'
    """

    def __init__(self, url: str, gis=None):
        """
        Constructs a SceneLayer given a web scene layer URL
        """
        super(SceneLayer, self).__init__(url, gis)


class ServiceFactory(type):
    """
    Generates a layer object from a given set of
    JSON (dictionary or iterable) or url.
    """

    @staticmethod
    def _item_properties(itemid: str, gis: "GIS") -> tuple[dict, str]:
        url: str = f"{gis._portal.resturl}content/items/{itemid}"
        return gis.session.get(url, params={"f": "json"}).json(), url

    @staticmethod
    def _get_url_for_item(item_url: str, item_props: dict):
        if item_props["type"] in [
            "KML",
            "KML Collection",
            "CSV",
            "GeoJSON",
            "GeoJson",
        ] and not item_url.endswith("/data"):
            item_url = f"{item_url}/data"
        return item_url

    @staticmethod
    def _get_url_from_item(item: _arcgis.gis.Item, gis: _arcgis.gis.GIS) -> str:
        props: dict
        item_url: str
        props, item_url = ServiceFactory._item_properties(item.id, gis=gis)
        return ServiceFactory._get_url_for_item(item_url, props)

    @staticmethod
    def _layer_type_from_url(url: str):
        """Returns the layer type (or tuple[str, function], for some edge-cases)"""
        parsed_url = urlparse(url)
        base_name = os.path.basename(parsed_url.path)
        has_layer = False

        if "sceneserver/layers" in parsed_url.path.lower():
            # special case for scene layers
            base_name = "sceneserver"
            has_layer = True
        elif base_name.isdigit():
            # special case for services with a layer index
            # use the part before the index as the base name
            base_name = os.path.basename(os.path.dirname(parsed_url.path))
            has_layer = True

        base_name_lower = base_name.lower()

        layer_mapping = {
            "data": DataServiceLayer,
            "featureserver": (
                FeatureServiceLayer if has_layer else FeatureLayerCollection
            ),
            "geocodeserver": Geocoder,
            "geodataserver": GeoData,
            "geometryserver": GeometryService,
            "gpserver": ("GeoprocessingToolbox", _import_toolbox),
            "imageserver": ImageryLayer,
            "knowledgegraphserver": KnowledgeGraph,
            "mapserver": MapServiceLayer if has_layer else MapImageLayer,
            "naserver": NetworkDataset,
            "sceneserver": SceneLayer,
            "schematicsserver": SchematicLayers,
            "vectortileserver": VectorTileLayer,
        }

        if base_name_lower in layer_mapping:
            return layer_mapping[base_name_lower]

        if base_name_lower == "ogcfeatureserver":
            from .._ogc import OGCFeatureService

            return OGCFeatureService
        if base_name_lower.endswith(".geojson"):
            from .._ogc import GeoJSONLayer

            return GeoJSONLayer
        if base_name_lower.endswith(".csv"):
            from .._ogc import CSVLayer

            return CSVLayer
        if base_name_lower.endswith(".kml") or base_name_lower.endswith(".kmz"):
            from .._ogc import KMLLayer

            return KMLLayer
        if base_name_lower.startswith("wmts"):
            from .._ogc import WMTSLayer

            return WMTSLayer
        if base_name_lower == "wmsserver" or url.lower().find("service=wms") > -1:
            from .._ogc import WMSLayer

            return WMSLayer

        # GlobeServer and MobileServer use generic Layer
        # Fall back to Layer for all other services
        return Layer

    @staticmethod
    def _get_layer_instance(layer_type, url, server, connection=None, parent_url=None):
        """
        Handles nuanced differences in initializer signature
        between layer types and returns an instance of the Layer from type

        Most Layers can be initialized using the `url` and `gis` parameters,
        but some require a `connection` parameter, or `url_or_item`
        """
        if layer_type == GeoData:
            return layer_type(url=url, connection=connection)
        if layer_type == _arcgis.layers._ogc._wms.WMSLayer:
            from urllib.parse import urlparse, parse_qs

            parameters: dict = parse_qs(urlparse(url).query)
            version: str = "1.3.0"
            if "version" in parameters:
                version: str = parameters["version"][0]
            return layer_type(url=url, gis=server, version=version)
        if isinstance(layer_type, _arcgis.layers._ogc._csv.CSVLayer):
            return layer_type(url_or_item=url, gis=server)
        if isinstance(layer_type, tuple):
            type_hint, _func = layer_type
            if not isinstance(_func, LambdaType):
                raise ValueError(
                    f"Instance function must be a function to instantiate {type_hint}"
                )
            return _func(url, server)
        elif layer_type == _arcgis.geocoding._functions.Geocoder:
            return layer_type(location=url, gis=server)
        elif (
            layer_type == _arcgis.layers.SceneLayer
            or layer_type == _arcgis.layers.VectorTileLayer
        ):
            # This is a special case where we pass in the parent url because serviceItemId is not part of the layer properties
            return layer_type(url=url, gis=server, parent_url=parent_url)
        return layer_type(url=url, gis=server)

    def __call__(
        cls,
        url_or_item: _arcgis.gis.Item | str = None,
        server=None,
        initialize=False,
        parent_url=None,
    ):
        """generates the proper type of layer from a given url"""
        from ...gis.server import ServicesDirectory

        url: str
        server = server or _arcgis.env.active_gis
        if isinstance(url_or_item, _arcgis.gis.Item):
            url = _get_item_url(url_or_item)
            if url in [None, ""]:
                url = cls._get_url_from_item(url_or_item, gis=server)
        elif isinstance(url_or_item, str):
            url = url_or_item
        else:
            raise ValueError("A URL to the service or an arcgis.Item is required.")

        layer_type = cls._layer_type_from_url(url)
        # GeoData is a legacy edge case that needs a Connection instead of GIS
        # This workflow is deprecated and will be removed in a future release
        if layer_type == GeoData:
            if isinstance(server, Connection) or hasattr(server, "token"):
                connection = server
            elif isinstance(server, (ServicesDirectory)):
                connection = server._con
            elif isinstance(server, GIS):
                ...
            else:
                parsed_url = urlparse(url)
                try:
                    site_url = "{scheme}://{nl}/{wa}".format(
                        scheme=parsed_url.scheme,
                        nl=parsed_url.netloc,
                        wa=parsed_url.path[1:].split("/")[0],
                    )
                    connection = Connection(baseurl=site_url)  # anonymous connection
                    server = ServicesDirectory(url=site_url)
                except:
                    site_url = "https://{nl}/rest/services".format(
                        scheme=parsed_url.scheme, nl=parsed_url.netloc
                    )
                    connection = Connection(
                        baseurl=site_url,
                        all_ssl=parsed_url.scheme == "https",
                    )  # anonymous connection
                    server = ServicesDirectory(url=site_url)
            return cls._get_layer_instance(layer_type, url, server, connection)
        return cls._get_layer_instance(layer_type, url, server, parent_url=parent_url)


###########################################################################
class Service(object, metaclass=ServiceFactory):
    """
    The *Service* class allows users to pass a *url* string or an
    :class:`~arcgis.gis.Item`, along with an optional :class:`~arcgis.gis.GIS`
    connection or specific :class:`~arcgis.gis.server.Server` object to return
    an instance of the specific ArcGIS API for Python object the service
    represents.

    ===================     ====================================================
    **Parameter**           **Description**
    -------------------     ----------------------------------------------------
    url_or_item             Required String. Internet endpoint for the service
                            to initialize as a Python object.
    -------------------     ----------------------------------------------------
    server                  Optional :class:`~arcgis.gis.server.Server` or
                            :class:`~arcgis.gis.GIS` object.
    ===================     ====================================================

    :returns:
        An object representing the service type of the input value.

    .. code-block::

        # Usage Example: Directly from a url
        >>> from arcgis.gis import GIS
        >>> from arcgis.layers import Service

        >>> gis = GIS(profile="your_online_profile")

        >>> fs_url = "https://services7.arcgis.com/<org_id>/arcgis/rest/services/ancient_places/FeatureServer"

        >>> flc = Service(
                    url_or_item=fs_url
                )
        >>> flc
        <FeatureLayerCollection url:"https://services7.arcgis.com/<org_id>/arcgis/rest/services/ancient_places/FeatureServer">

        >>> type(flc)
        arcgis.features.layer.FeatureLayerCollection

        # Usage Example #2: From an item
        >>> org_item = gis.content.get("_item_id_")

        >>> org_item.type
        Vector Tile Service

        >>> vts = Service(
                    url_or_item=org_item
                  )
        >>> vts
        <VectorTileLayer url:"https://tiles.arcgis.com/tiles/<org_id>/arcgis/rest/services/Custom_Basemap_SXT/VectorTileServer">
    """

    def __init__(
        self,
        url_or_item: _arcgis.gis.Item | str | None = None,
        server=None,
        initialize=False,
        parent_url=None,
    ) -> None: ...
