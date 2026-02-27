from __future__ import annotations
import logging
import urllib.parse
from typing import Union, List
from functools import lru_cache

from cachetools import cached, TTLCache
from arcgis.auth.tools import LazyLoader
from arcgis._impl.common._isd import InsensitiveDict
from arcgis.gis import GIS

_lyrs = LazyLoader("arcgis.layers")
_featuremgr = LazyLoader("arcgis.features.managers")
_imagemgr = LazyLoader("arcgis.raster._layer")

_log = logging.getLogger()


###########################################################################
class AGOLServerManager:
    """
    Represents a Single ArcGIS Online server for an organization. This class
    is not meant to be initialized directly, but instances of this class are
    returned by these attributes of the
    :class:`~arcgis.gis.agoserver.AGOLServersManager`:

    * the :meth:`~arcgis.gis.agoserver.AGOLServersManager.list` method
    * the :attr:`~arcgis.gis.agoserver.AGOLServersManager.feature_server` property
    * the :attr:`~arcgis.gis.agoserver.AGOLServersManager.tile_server` property

    ==================     ====================================================================
    **Parameter**           **Description**
    ------------------     --------------------------------------------------------------------
    url                    Required String. The url string to the ArcGIS Online Server
    ------------------     --------------------------------------------------------------------
    gis                    Required GIS. The connection to ArcGIS Online.
    ==================     ====================================================================

    .. code-block:: python

        # Usage Example: Accessing an AGOLServerManager object
        >>> from arcgis.gis import GIS
        >>> gis = GIS(profile="your_online_admin_manager")

        >>> online_servers_mgr = gis.admin.servers
        >>> hosted_feature_svc_mgr = online_servers_mgr.feature_server[0]
        >>> hosted_feature_svc_mgr

        < AGOLServerManager @ https://services7.arcgis.com/<org_id>/ArcGIS/admin/services >
    """

    _gis = None
    _url = None
    _properties = None

    def __init__(self, url: str, gis: GIS):
        """initializer"""
        self._url = url
        self._gis = gis

    # ---------------------------------------------------------------------
    def __str__(self):
        return f"< AGOLServerManager @ {self._url} >"

    # ---------------------------------------------------------------------
    def __repr__(self):
        return f"< AGOLServerManager @ {self._url} >"

    @property
    @lru_cache(maxsize=100)
    def is_tile_server(self) -> bool:
        """
        Returns if the server if hosting tiles or not

        :returns: bool
        """
        return self._url.lower().find("/tiles/") > -1

    @property
    @cached(cache=TTLCache(maxsize=10, ttl=25))
    def properties(self) -> InsensitiveDict:
        """
        Returns a dictionary with a *servers* key that contains information,
        such as the type, name, and status of the services on the server.

        .. note::
            This call is cached for 25 seconds.

        .. code-block:: python

            # Usage Example:
            >>> from arcgis.gis import GIS
            >>> gis = GIS(profile="your_online_admin_profile")

            >>> online_servers_mgr = gis.admin.servers
            >>> feature_server_mgr = online_servers_mgr.feature_server[0]
            >>> service_info = feature_server_mgr.properties["services"][0]
            >>> service_info

            {
              'adminServiceInfo': {
                     'name': 'service_name',
                     'type': 'FeatureServer',
                     'status': 'Started',
                     'maxRecordCount': 1000
                     },
               'serviceDescription': ''
            }

        :return: Dict
        """
        resp = self._gis._con.get(self._url, {"f": "json"})
        return InsensitiveDict(resp)

    @lru_cache(maxsize=50)
    def get(self, name: str) -> Union[
        _lyrs.VectorTileLayerManager,
        _imagemgr.ImageryLayerCacheManager,
        _lyrs.SceneLayerManager,
        _featuremgr.FeatureLayerCollectionManager,
        _lyrs.MapImageLayerManager,
    ]:
        """
        Returns a single service manager.

        ==================     ====================================================================
        **Parameter**           **Description**
        ------------------     --------------------------------------------------------------------
        name                   Required String. The name of the service.
        ==================     ====================================================================

        :returns: Union[:class:`~arcgis.layers.VectorTileLayer`,
                        :class:`~arcgis.raster.ImageryLayerCacheManager`,
                        :class:`~arcgis.layers.SceneLayerManager`,
                        :class:`~arcgis.features.managers.FeatureLayerCollectionManager`,
                        :class:`~arcgis.layers.MapImageLayerManager`]
        """

        if self.is_tile_server == False:
            for service in self.services:
                if service.properties.adminServiceInfo.name.lower() == name.lower():
                    return service
        else:
            for service in self.services:
                if service.properties.name.lower() == name.lower():
                    return service

        return

    def status(self, name: str) -> str:
        """
        Returns the status of a given service by name.

        ==================     ====================================================================
        **Parameter**           **Description**
        ------------------     --------------------------------------------------------------------
        name                   Required String. The name of the service.
        ==================     ====================================================================

        :returns: string
        """
        found = False
        if self.is_tile_server == False:
            if "services" in self.properties:
                for service in self.properties["services"]:
                    if "adminServiceInfo" in service:
                        service = service["adminServiceInfo"]
                    if service["name"].lower() == name.lower():
                        found = True
                        return service["status"]
        else:
            for service in self.services:
                if service.properties.name.lower() == name.lower():
                    found = True
                    if hasattr(service, "status"):
                        return service.status
                    elif "status" in service.properties:
                        return service.properties.status
                    else:
                        return "UNKNOWN"
        if found == False:
            raise Exception("Service not found.")

    @property
    def services(self) -> list:
        """Returns the Administrative Endpoints

        :returns: list
        """
        services = []
        properties = self.properties
        if "services" in properties:
            for service in properties["services"]:
                if "adminServiceInfo" in service:
                    service = service["adminServiceInfo"]
                name = urllib.parse.quote(service["name"])
                if self.is_tile_server:
                    url = f"{self._url}/{name}/{service['type']}"
                else:
                    url = f"{self._url}/{name}.{service['type']}"
                service_type = service["type"].lower()
                if service_type == "mapserver":
                    services.append(_lyrs.MapImageLayerManager(url=url, gis=self._gis))
                elif service_type == "featureserver":
                    services.append(
                        _featuremgr.FeatureLayerCollectionManager(
                            url=url, gis=self._gis
                        )
                    )

                elif service_type.find("vector") > -1:
                    services.append(
                        _lyrs.VectorTileLayerManager(url=url, gis=self._gis)
                    )
                elif service_type == "sceneserver":
                    services.append(_lyrs.SceneLayerManager(url=url, gis=self._gis))
                elif service_type == "imageserver":
                    services.append(
                        _imagemgr.ImageryLayerCacheManager(url, gis=self._gis)
                    )
                else:
                    _log.warning(f"No manager found for service: {url}.")
        return services


###########################################################################
class AGOLServersManager:
    """
    This class allows users to work with hosted tile and feature services on
    ArcGIS Online. This class is not meant to be initialized directly, but
    accessed using the :attr:`~arcgis.gis.admin.AGOLAdminManager.servers`
    property on an :class:`~arcgis.gis.admin.AGOLAdminManager` object:

    .. code-block:: python

        # Usage Example: Initialize the ArcGIS Online Servers manager
        >>> from arcgis.gis import GIS
        >>> gis = GIS(profile="your_online_admin_profile")

        >>> online_admin = gis.admin
        >>> online_admin

        < AGOLAdminManager @ https://example.online.com/sharing/rest/ >

        >>> online_servers = online_admin.servers
        >>> online_servers

        <arcgis.gis.agoserver._admin.AGOLServersManager object at <mem_addr>>


    ==================     ====================================================================
    **Parameter**           **Description**
    ------------------     --------------------------------------------------------------------
    gis                    Required GIS. The connection to ArcGIS Online.
    ==================     ====================================================================


    """

    _gis = None

    def __init__(self, gis: GIS):
        """initializer"""
        if gis._portal.is_arcgisonline == False:
            raise ValueError("Invalid GIS")
        self._gis = gis

    @property
    def properties(self) -> InsensitiveDict:
        """
        Returns the properties of the hosted tile and feature servers for
        the organization.

        .. code-block:: python

            # Usage Example: Online Servers Manager properties
            >>> from arcgis.gis import GIS
            >>> gis = GIS(profile="your_online_admin_profile")

            >>> online_servers_mgr = gis.admin.servers
            >>> online_servers_mgr.properties

            {'tile': ['https://tiles.arcgis.com/tiles/<org_id>/arcgis/rest/admin/services'],
            'feature': ['https://services7.arcgis.com/<org_id>/ArcGIS/admin/services']}

        :returns:
            An dictionary-like object (InsensitiveDict) providing the admin
            URL endpoints for the online organization's hosted feature and
            tile services.
        """
        return InsensitiveDict(self._urls(gis=self._gis))

    @lru_cache(maxsize=254)
    def _urls(self, gis: GIS) -> dict:
        """returns the parsed urls"""
        info = gis._registered_servers()
        tile_urls = set(info["urls"].get("tiles", {}).get("https", []))
        feature_urls = set(info["urls"].get("features", {}).get("https", []))
        tile_urls = set(info["urls"].get("tiles", {}).get("https", []))
        pid = gis.properties.id
        tile_urls = [
            f"https://{url}/tiles/{pid}/arcgis/rest/admin/services"
            for url in tile_urls
            if url not in feature_urls
        ]
        feature_urls = [
            f"https://{url}/{pid}/ArcGIS/admin/services" for url in feature_urls
        ]
        return {"tile": tile_urls, "feature": feature_urls}

    @property
    def tile_server(self) -> List[AGOLServerManager]:
        """
        Returns a list of :class:`~arcgis.gis.agoserver.AGOLServerManager`
        objects for the hosted tile services in the organization.
        """

        return [
            AGOLServerManager(url, gis=self._gis)
            for url in self._urls(self._gis)["tile"]
        ]

    @property
    def feature_server(self) -> List[AGOLServerManager]:
        """
        Returns a list of :class:`~arcgis.gis.agoserver.AGOLServerManager`
        objects for the hosted feature services in the organization.
        """
        return [
            AGOLServerManager(url, gis=self._gis)
            for url in self._urls(self._gis)["feature"]
        ]

    @lru_cache(maxsize=254)
    def list(self) -> List[AGOLServerManager]:
        """
        Returns a list of all server managers for the organization.

        :returns: List[:class:`~arcgis.gis.agoserver.AGOLServerManager`]
        """
        return self.tile_server + self.feature_server
