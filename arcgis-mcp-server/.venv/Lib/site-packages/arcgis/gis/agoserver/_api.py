from __future__ import annotations
from arcgis.auth.tools import LazyLoader
import os
import functools
import urllib.parse
from arcgis.gis import GIS

logging = LazyLoader("logging")
_isd = LazyLoader("arcgis._impl.common._isd")
layer = LazyLoader("arcgis.features.layer")
geocoding = LazyLoader("arcgis.geocoding")
gptool = LazyLoader("arcgis.geoprocessing._tool")
geommodule = LazyLoader("arcgis._impl.tools")
network_dataset = LazyLoader("arcgis.network")
_gis = LazyLoader("arcgis.gis")
mapping = LazyLoader("arcgis.layers")
raster = LazyLoader("arcgis.raster")
schematics = LazyLoader("arcgis.schematics")

_log = logging.getLogger()


@functools.lru_cache(maxsize=250)
def _create_service(url: str, layer_type: str, gis: GIS, name: str = None):
    has_layer = False
    digit = None
    if name:
        url = url.replace(name, urllib.parse.quote(name))
    if os.path.basename(url).isdigit():
        digit = os.path.basename(url)
        url = os.path.dirname(url)
        has_layer = True
    if layer_type.lower() == "mapserver":
        if has_layer:
            return mapping.MapServiceLayer(url=f"{url}/{digit}", gis=gis)
        else:
            return mapping.MapImageLayer(url=url, gis=gis)
    elif layer_type.lower() == "featureserver":
        if has_layer:
            return layer.FeatureLayer(url=url, gis=gis)
        else:
            return layer.FeatureLayerCollection(url=url, gis=gis)
    elif layer_type.lower() == "imageserver":
        return raster.ImageryLayer(url=url, gis=gis)
    elif layer_type.lower() == "gpserver":
        return gptool.Toolbox(url=url, gis=gis)
    elif layer_type.lower() == "geometryserver":
        return geommodule.GeometryService(url=url, gis=gis)
    elif layer_type.lower() == "geocodeserver":
        return geocoding.Geocoder(location=url, gis=gis)
    elif layer_type.lower() == "naserver":
        return network_dataset.NetworkDataset(url=url, gis=gis)
    elif layer_type.lower() == "vectortileserver":
        return mapping.VectorTileLayer(url=url, gis=gis)
    elif layer_type.lower() == "sceneserver":
        return mapping._scenelyrs._lyrs.SceneLayer(url=url, gis=gis)
    else:
        return _gis.Layer(url=url, gis=gis)

    return None


###########################################################################
class AGOLServicesDirectory:
    """
    Provides access to the hosted services for specific servers of the ArcGIS
    Online Organization. Objects of this class are not meant to be initialized
    directly, but instead a list of directories is returned when accessing the
    :attr:`~arcgis.gis.GIS.hosting_servers` property on a GIS object initialized
    with ArcGIS Online credentials:

    .. code-block:: python

        # Usage Example: Get ArcGIS Online organization Service Directories
        >>> from arcgis.gis import GIS
        >>> gis = GIS(profile="your_online_profile")

        >>> gis.hosting_servers

        [< AGOLServicesDirectory @ https://servicesX.arcgis.com/<org_id>/arcgis/rest/services >,
         < AGOLServicesDirectory @ https://tiles.arcgis.com/tiles/<org_id>/arcgis/rest/services >]

    ==================     ====================================================================
    **Parameter**           **Description**
    ------------------     --------------------------------------------------------------------
    url                    Required String. The url string to the ArcGIS Online Server
    ------------------     --------------------------------------------------------------------
    gis                    Required :class:`~arcgis.gis.GIS` object initialized from
                           ArcGIS Online credentials.
    ==================     ====================================================================

    """

    _gis = None
    _url = None
    _properties = None

    # ---------------------------------------------------------------------
    def __init__(self, url: str, gis: GIS):
        """initializer"""
        self._gis = gis
        self._url = url

    # ---------------------------------------------------------------------
    def __str__(self):
        return f"< AGOLServicesDirectory @ {self._url} >"

    # ---------------------------------------------------------------------
    def __repr__(self):
        return f"< AGOLServicesDirectory @ {self._url} >"

    # ---------------------------------------------------------------------
    @functools.lru_cache(maxsize=255)
    def _org_id(self, gis: GIS) -> str:
        return gis.properties.id

    # ---------------------------------------------------------------------
    @property
    def properties(self) -> _isd.InsensitiveDict:
        """
        Returns the server's version property and a list of services
        in the organization on that server.

        .. note::
            Return times for this property will vary based on number of services
            hosted by the organization.

        :returns:
            A dictionary-like InsensitiveDict object containing the
            version and a list of services hosted on the particular server.
        """
        resp = self._gis._con.get(self._url, {"f": "json"})
        return _isd.InsensitiveDict(resp)

    # ---------------------------------------------------------------------
    @property
    def folders(self) -> list:
        """
        Returns an empty list.

        :returns: List
        """
        return []

    # ---------------------------------------------------------------------
    @property
    def services(self) -> list:
        """
        Returns a list of layer objects hosted on the ArcGIS Online server.

        .. code-block:: python

            # Usage Example: Accessing services through the properties of a Tile Server
            >>> from arcgis.gis import GIS
            >>> gis = GIS(profile="your_online_admin_profile")

            >>> gis.hosting_servers

            [< AGOLServicesDirectory @ https://services7.arcgis.com/<org_id>/arcgis/rest/services >,
             < AGOLServicesDirectory @ https://tiles.arcgis.com/tiles/<org_id>/arcgis/rest/services >]

            >>> tile_sd = gis.hosting_servers[1]
            >>> tiles_sd.services

            [<PointCloudLayer url:"https://tiles.arcgis.com/tiles/org_id/arcgis/rest/services/scene_service1/SceneServer">,
            <VectorTileLayer url:"https://tiles.arcgis.com/tiles/org_id/arcgis/rest/services/vector_service1/VectorTileServer">,
            ...
            <MapImageLayer url:"https://tiles.arcgis.com/tiles/org_id/arcgis/rest/services/map_service1/MapServer">]





        """
        services = []

        if "services" in self.properties:
            for s in self.properties["services"]:
                try:
                    services.append(
                        _create_service(
                            url=s["url"],
                            layer_type=s["type"],
                            gis=self._gis,
                            name=s.get("name", None),
                        )
                    )
                except Exception as e:
                    msg = "URL: %s is throwing error:  %s" % (s["url"], e)
                    _log.warning(msg)
                    _log.warning("Could not load service: %s" % s["url"])
        return services
