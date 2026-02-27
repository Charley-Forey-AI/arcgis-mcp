from __future__ import absolute_import, annotations

import re
import uuid
import urllib.parse
from typing import Any
from functools import lru_cache
from arcgis.gis import Item
from arcgis.geoprocessing import import_toolbox
import requests
from arcgis.auth.tools import LazyLoader
from arcgis.gis._impl._util import _get_item_url
from arcgis._impl.common._utils import _validate_url

collections = LazyLoader("collections")
json = LazyLoader("json")
os = LazyLoader("os")
pathlib = LazyLoader("pathlib")
tempfile = LazyLoader("tempfile")
time = LazyLoader("time")
datetime = LazyLoader("datetime")
arcgis = LazyLoader("arcgis")
_gis = LazyLoader("arcgis.gis")
_geometry = LazyLoader("arcgis.geometry")
_layers = LazyLoader("arcgis.layers")
from arcgis.gis.server.admin._services import Service as _AdminService


###########################################################################
class EnterpriseVectorTileLayerManager(arcgis.gis._GISResource):
    """
    The ``EnterpriseVectorTileLayerManager`` class allows administration (if access permits) of ArcGIS Enterprise hosted vector tile layers.
    A Hosted Vector Tile Service is published through a Feature Layer and these methods can only be
    applied to such Vector Tile Services.
    A :class:`~arcgis.layers.VectorTileLayer` offers access to layer content.

    .. note:: Url must be admin url such as: ``https://services.myserver.com/arcgis/server/admin/services/serviceName.VectorTileServer/``
    """

    _gptbx = None

    def __init__(self, url, gis=None, vect_tile_lyr=None):
        if url.split("/")[-1].isdigit():
            url = url.replace(f"/{url.split('/')[-1]}", "")
        if gis.version <= [8, 4]:
            raise Warning("Manager not available. Update version of Enterprise")
        super(EnterpriseVectorTileLayerManager, self).__init__(url, gis)
        self._vtl = vect_tile_lyr
        self._is_hosted = self.properties["portalProperties"]["isHosted"]

    @lru_cache(maxsize=255)
    def _admin_service(self) -> _AdminService:
        """returns the server admin endpoints for the manager"""
        return _AdminService(self.url, self._gis)

    # ----------------------------------------------------------------------
    def edit(self, service_dictionary):
        """
        This operation edits the properties of a service. To edit a service,
        you need to submit the complete JSON representation of the service,
        which includes the updates to the service properties.
        Editing a service can cause the service to be restarted with updated properties.

        The JSON representation of a service contains the following four sections:

        * Service description properties—Common properties that are shared by all services. These properties typically identify a specific service.
        * Service framework properties—Properties targeted toward the framework that hosts the GIS service. They define the life cycle and load balancing of the service.
        * Service type properties—Properties targeted toward the core service type as seen by the server administrator. Since these properties are associated with a server object, they vary across the service types.
        * Extension properties—Represent the extensions that are enabled on the service.

        .. note::
            The JSON is submitted to the operation URL as a value of the
            parameter service. You can leave out the serviceName and type parameters
            in the JSON representation. Any other properties that are left out are not persisted by the server.

        .. note::
            If the service is currently running you need to stop the service before editing it. This can be done
            by calling the stop method on the service object. Once the service is stopped, you can edit the service
            and then start it again by calling the start method on the service object.

        ===================     ====================================================================
        **Parameter**            **Description**
        -------------------     --------------------------------------------------------------------
        service_dictionary      Required dict. The JSON representation of the service and the
                                properties that have been updated or added.

                                Example:

                                    |    {
                                    |        "serviceName": "RI_Fed2019_WM",
                                    |        "type": "VectorTileServer",
                                    |        "description": "",
                                    |        "capabilities": "TilesOnly,Tilemap",
                                    |        "extensions": [],
                                    |        "frameworkProperties": {},
                                    |        "datasets": []
                                    |        }
        ===================     ====================================================================


        :return: boolean
        """
        vtl_service = self._admin_service()
        return vtl_service.edit(service_dictionary)

    # ----------------------------------------------------------------------
    def start(self):
        """This operation starts a service and loads the service's configuration."""
        vtl_service = self._admin_service()
        return vtl_service.start()

    # ----------------------------------------------------------------------
    def stop(self):
        """
        This operation stops all instances of a service. Once a service is
        stopped, it cannot process any incoming requests. Performing this
        operation will stop the respective servers, terminating all pods
        that run this service.
        """
        vtl_service = self._admin_service()
        return vtl_service.stop()

    # ----------------------------------------------------------------------
    def change_provider(self, provider: str):
        """
        The changeProvider operation updates an individual service to use
        either a dedicated or a shared instance type. When a qualified service
        is published, the service is automatically set to use shared instances.

        When using this operation, services may populate other provider types
        as values for the provider parameter, such as ArcObjects and SDS.
        While these are valid provider types, this operation does not support
        changing the provider of such services to either ArcObjects11 or DMaps.
        Services with ArcObjects or SDS as their provider cannot change their instance type.

        ======================      =======================================================
        **Parameter**                **Description**
        ----------------------      -------------------------------------------------------
        provider                    Optional String. Specifies the service instance as either
                                    a shared ("DMaps") or dedicated ("ArcObjects11") instance
                                    type. These values are case sensitive.
        ======================      =======================================================

        :return: Boolean

        """
        if provider in ["ArcObjects11", "DMaps"]:
            vtl_service = self._admin_service()
            return vtl_service.change_provider(provider)
        return False

    # ----------------------------------------------------------------------
    def delete(self):
        """
        This operation deletes an individual service, stopping the service
        and removing all associated resources and configurations.
        """
        vtl_service = self._admin_service()
        return vtl_service.delete()

    # ----------------------------------------------------------------------
    @property
    def _tbx(self):
        """gets the toolbox"""
        if self._gptbx is None:
            self._gptbx = import_toolbox(
                url_or_item=self._gis.hosting_servers[0].url
                + "/System/CachingControllers/GPServer",
                gis=self._gis,
            )
            self._gptbx._is_fa = True
        return self._gptbx

    # ----------------------------------------------------------------------
    def rebuild_cache(self, min_scale=None, max_scale=None):
        """
        The rebuild_cache operation updates the vector tile layer cache to reflect
        any changes made.
        The results of the operation is the url to the vector tile service once it is
        done rebuilding.

        ======================      =======================================================
        **Parameter**                **Description**
        ----------------------      -------------------------------------------------------
        min_scale                   Optional Float. Represents the minimum scale of the tiles.
                                    If nothing is provided, default value is used.
        ----------------------      -------------------------------------------------------
        max_scale                   Optional Float. Represents the maximum scale of the tiles.
                                    If nothing is provided, default value is used.
        ======================      =======================================================
        """
        return self._tbx.manage_vector_tile_cache(
            service_name=self.properties.serviceName,
            service_folder="Hosted",
            min_scale=min_scale,
            max_scale=max_scale,
        )


###########################################################################
class VectorTileLayerManager(arcgis.gis._GISResource):
    """
    The ``VectorTileLayerManager`` class allows administration (if access permits) of ArcGIS Online Hosted Vector Tile Layers.
    A Hosted Vector Tile Service is published through a Feature Layer and these methods can only be
    applied to such Vector Tile Services.
    A :class:`~arcgis.layers.VectorTileLayer` offers access to layer content.

    .. note::
        Url must be admin url such as: ``https://services.myserver.com/arcgis/rest/admin/services/serviceName/VectorTileServer/``
    """

    def __init__(self, url, gis=None, vect_tile_lyr=None):
        if url.split("/")[-1].isdigit():
            url = url.replace(f"/{url.split('/')[-1]}", "")
        super(VectorTileLayerManager, self).__init__(url, gis)
        self._vtl = vect_tile_lyr
        self._source_type = (
            self.properties["sourceType"]
            if "sourceType" in self.properties
            else self.properties["sourceServiceType"]
        )
        self._session = gis.session

    # ----------------------------------------------------------------------
    def edit_tile_service(
        self,
        source_item_id: str | None = None,
        export_tiles_allowed: bool | None = None,
        min_scale: float | None = None,
        max_scale: float | None = None,
        max_export_tile_count: int | None = None,
        layers: list[dict] | None = None,
        cache_max_age: int | None = None,
        max_zoom: int | None = None,
    ) -> dict:
        """
        The edit operation enables editing many parameters in the service definition as well as
        the source_item_id which can be found by looking at the Vector Tile Layer's related items.

        ======================      =======================================================
        **Parameter**                **Description**
        ----------------------      -------------------------------------------------------
        source_item_id              Optional String. The Source Item ID is the GeoWarehouse
                                    Item ID of the tile service.
        ----------------------      -------------------------------------------------------
        export_tiles_allowed        Optional boolean. ``exports_tiles_allowed`` sets
                                    the value to let users export tiles
        ----------------------      -------------------------------------------------------
        min_scale                   Optional float. Sets the services minimum scale for
                                    caching. At the moment this parameter can only be set if
                                    the Vector Tile Layer was published through a service directory.
        ----------------------      -------------------------------------------------------
        max_scale                   Optional float. Sets the services maximum scale for
                                    caching. At the moment this parameter can only be set if
                                    the Vector Tile Layer was published through a service directory.
        ----------------------      -------------------------------------------------------
        max_export_tile_count       Optional int. ``max_export_tile_count`` sets the
                                    maximum amount of tiles to be exported from a single
                                    call.
        ----------------------      -------------------------------------------------------
        layers                      Optional list of dictionaries. Each dict representing a layer.

                                    Syntax Example:

                                        | layers = [{
                                        |        "name": "Layer Name",
                                        |        "id": 1159321,
                                        |        "layerId": 0,
                                        |        "tableName": "tableName",
                                        |        "type": "Feature Layer",
                                        |        "xssTrustedFields": ""
                                        |    }]
        ----------------------      -------------------------------------------------------
        cache_max_age               Optional int. The maximum cache age. At the moment this
                                    parameter can only be set if the Vector Tile Layer was
                                    published through a feature service.
        ----------------------      -------------------------------------------------------
        max_zoom                    Optional int. The maximum zoom level. At the moment this
                                    parameter can only be set if the Vector Tile Layer was
                                    published through a feature service.
        ======================      =======================================================

        .. code-block:: python

            # USAGE EXAMPLE

            >>> from arcgis.layers import VectorTileLayer
            >>> from arcgis.gis import GIS

            # connect to your GIS and get the tile layer item
            >>> gis = GIS(url, username, password)

            >>> vector_layer_item = gis.content.get('abcd_item-id')
            >>> source_item_id = vector_tile_item.related_items(rel_type="Service2Data", direction="forward")[0]["id"]
            >>> vector_tile_layer = VectorTileLayer.fromitem(vector_layer_item)
            >>> vtl_manager = vector_tile_layer.manager
            >>> vtl_manager.edit_tile_service(
                                            min_scale = 50,
                                            max_scale = 100,
                                            source_item_id = source_item_id,
                                            export_tiles_allowed = True,
                                            max_Export_Tile_Count = 10000
                                            )
        """
        # Parameters depend on how the vector tile layer was published.
        feature_service_pub = True if self._source_type == "FeatureServer" else False

        params = {
            "f": "json",
            "serviceDefinition": {},
        }
        if max_export_tile_count:
            params["serviceDefinition"]["maxExportTilesCount"] = max_export_tile_count
        if export_tiles_allowed and export_tiles_allowed in [True, False]:
            params["serviceDefinition"]["exportTilesAllowed"] = export_tiles_allowed
        if source_item_id:
            params["sourceItemId"] = source_item_id
        if layers:
            params["serviceDefinition"]["layerProperties"] = {"layers": layers}

        # These parameters depend on publish source.
        if min_scale and feature_service_pub is False:
            params["serviceDefinition"]["minScale"] = min_scale
        if max_scale and feature_service_pub is False:
            params["serviceDefinition"]["maxScale"] = max_scale
        if cache_max_age and feature_service_pub is True:
            params["serviceDefinition"]["cacheMaxAge"] = cache_max_age
        if max_zoom and feature_service_pub is False:
            params["serviceDefinition"]["maxZoom"] = max_zoom

        # endpoint and post call
        url = self._url + "/edit"
        resp: requests.Response = self._session.post(url=url, data=params)
        resp.raise_for_status()
        return resp.json()

    # ----------------------------------------------------------------------
    def update_tiles(self, merge_bundle: bool = False) -> dict:
        """
        The update_tiles operation supports updating the cooking extent and
        cache levels in a Hosted Vector Tile Service. The results of the
        operation is a response indicating success and a url
        to the Job Statistics page, or failure.

        It is recommended to use the `rebuild_cache` method when your layer has been
        published through a Feature Layer since edits require regeneration of the tiles.

        ===============     ====================================================
        **Parameter**        **Description**
        ---------------     ----------------------------------------------------
        merge_bundle        Optional bool. Default is False. This parameter will
                            only be set if the Vector Tile Layer has been published
                            through a service directory.
        ===============     ====================================================

        :returns:
           Dictionary. If the product is not ArcGIS Online tile service, the
           result will be None.

        .. code-block:: python

            # USAGE EXAMPLE

            >>> from arcgis.layers import VectorTileLayer
            >>> from arcgis.gis import GIS

            # connect to your GIS and get the web map item
            >>> gis = GIS(url, username, password)
            >>> vector_layer_item = gis.content.get('abcd_item-id')
            >>> vector_tile_layer = VectorTileLayer.fromitem(vector_layer_item)
            >>> vtl_manager = vector_tile_layer.manager
            >>> update_tiles = vtl_manager.update_tiles()
            >>> type(update_tiles)
            <Dictionary>
        """
        # Parameters depend on how the vector tile layer was published.
        feature_service_pub = True if self._source_type == "FeatureServer" else False

        params = {"f": "json"}
        if feature_service_pub:
            url = "%s/updateTiles" % self._url
        else:
            url = "%s/update" % self._url
            params["mergeBundles"] = merge_bundle
        resp: requests.Response = self._session.post(url=url, data=params)
        resp.raise_for_status()
        return resp.json()

    # ----------------------------------------------------------------------
    def refresh(self):
        """
        The refresh operation clears and refreshes the service cache.
        """
        url = self._url + "/refresh"
        params = {"f": "json"}
        resp: requests.Response = self._session.post(url=url, data=params)
        resp.raise_for_status()
        return resp.json()

    # ----------------------------------------------------------------------
    def rebuild_cache(self):
        """
        The rebuild_cache operation update the vector tile layer cache to reflect
        any changes made to the feature layer used to publish this vector tile layer.
        The results of the operation is a response indicating success, which
        redirects you to the Job Statistics page, or failure.
        """
        url = self._url + "/rebuildCache"
        resp: requests.Response = self._session.get(url=url)
        resp.raise_for_status()
        return resp.json()

    # ----------------------------------------------------------------------
    def status(self) -> dict:
        """
        The status operation returns a dictionary indicating
        whether a service is started (available) or stopped.
        """
        url = self._url + "/status"
        resp: requests.Response = self._session.get(url=url)
        resp.raise_for_status()
        return resp.json()

    # ----------------------------------------------------------------------
    def jobs(self) -> dict:
        """
        The tile service job summary (jobs) resource represents a
        summary of all jobs associated with a vector tile service.
        Each job contains a jobid that corresponds to the specific
        jobid run and redirects you to the Job Statistics page.

        """
        url = self._url + "/jobs"
        resp: requests.Response = self._session.get(url=url)
        resp.raise_for_status()
        return resp.json()

    # ----------------------------------------------------------------------
    def job_statistics(self, job_id: str) -> dict:
        """
        The tile service job summary (jobs) resource represents a
        summary of all jobs associated with a vector tile service.
        Each job contains a jobid that corresponds to the specific
        jobid run and redirects you to the Job Statistics page.

        """
        url = self._url + "/jobs/{job_id}".format(job_id=job_id)
        params = {"f": "json"}
        resp: requests.Response = self._session.post(url=url, data=params)
        resp.raise_for_status()
        return resp.json()

    # ----------------------------------------------------------------------
    def delete_job(self, job_id: str) -> dict:
        """
        This operation deletes the specified asynchronous job being run by
        the geoprocessing service. If the current status of the job is
        SUBMITTED or EXECUTING, it will cancel the job. Regardless of status,
        it will remove all information about the job from the system. To cancel a
        job in progress without removing information, use the Cancel Job operation.
        """
        url = self._url + "jobs/{job_id}/delete".format(job_id=job_id)
        params = {"f": "json"}
        resp: requests.Response = self._session.post(url=url, data=params)
        resp.raise_for_status()
        return resp.json()

    # ----------------------------------------------------------------------
    def cancel_job(self, job_id: str) -> dict:
        """
        The cancel operation supports cancelling a job while update
        tiles is running from a hosted feature service. The result of this
        operation is a response indicating success or failure with error
        code and description.
        """
        url = self._url + "jobs/{job_id}/cancel".format(job_id=job_id)
        params = {"f": "json"}
        resp: requests.Response = self._session.post(url=url, data=params)
        resp.raise_for_status()
        return resp.json()

    # ----------------------------------------------------------------------
    def rerun_job(self, code, job_id: str) -> dict:
        """
        The ``rerun_job`` operation supports re-running a canceled job from a
        hosted map service. The result of this operation is a response
        indicating success or failure with error code and description.

        ===============     ====================================================
        **Parameter**        **Description**
        ---------------     ----------------------------------------------------
        code                required string, parameter used to re-run a given
                            jobs with a specific error
                            code: ``ALL | ERROR | CANCELED``
        ---------------     ----------------------------------------------------
        job_id              required string, job to reprocess
        ===============     ====================================================

        :returns:
           A boolean or dictionary
        """
        url = self._url + "/jobs/%s/rerun" % job_id
        params = {"f": "json", "rerun": code}
        resp: requests.Response = self._session.post(url=url, data=params)
        resp.raise_for_status()
        return resp.json()

    ######################### These Methods Only Apply to VTL Service from a Service Directory #################################
    def swap(self, target_service_name):
        """
        The swap operation replaces the current service cache with an existing one.

        .. note::
            The ``swap`` operation is for ArcGIS Online only and can only be used for a Vector
            Tile Layer published from a service directory.

        ====================        ====================================================
        **Parameter**                **Description**
        --------------------        ----------------------------------------------------
        target_service_name         Required string. Name of service you want to swap with.
        ====================        ====================================================

        :return: Dictionary indicating success or error
        """
        if self._source_type != "FeatureServer":
            url = self._url + "/swap"
            params = {"f": "json", "targetServiceName": target_service_name}
            resp: requests.Response = self._session.post(url=url, data=params)
            resp.raise_for_status()
            return resp.json()
        return None

    # ----------------------------------------------------------------------
    def delete_tiles(self):
        """
        The ``delete_tiles`` method deletes tiles from the current cache.

        .. note::
            The ``delete_tiles`` operation is for ArcGIS Online only and can only
            be used for a Vector Tile Layer published from a service directory.

        :return:
           A dictionary

        .. code-block:: python

            # USAGE EXAMPLE

            >>> from arcgis.layers import VectorTileLayer
            >>> from arcgis.gis import GIS

            # connect to your GIS
            >>> gis = GIS(url, username, password)

            >>> vector_layer_item = gis.content.get('abcd_item-id')
            >>> vector_tile_layer = VectorTileLayer.fromitem(vector_layer_item)
            >>> vtl_manager = vector_tile_layer.manager
            >>> deleted_tiles = vtl_manager.delete_tiles()
            >>> type(deleted_tiles)
        """
        if self._source_type != "FeatureServer":
            params = {
                "f": "json",
            }
            url = self._url + "/delete"
            resp: requests.Response = self._session.post(url=url, data=params)
            resp.raise_for_status()
            return resp.json()
        return None


###########################################################################
class VectorTileLayer(arcgis.gis.Layer):
    """
    A Vector Tile Layer is a type of data layer used to access and display
    tiled data and its corresponding styles. This is stored as an item in ArcGIS
    and is used to access a vector tile service. Layer data include its
    name, description, and any overriding style definition.
    """

    def __init__(self, url, gis, parent_url=None):
        super(VectorTileLayer, self).__init__(url, gis)
        if gis is None:
            raise ValueError("GIS object must be provided")
        self._session = gis.session
        self._parent_url = parent_url

    # ----------------------------------------------------------------------
    @classmethod
    def fromitem(cls, item) -> VectorTileLayer:
        if not item.type == "Vector Tile Service":
            raise TypeError(
                "Item must be a type of Vector Tile Service, not " + item.type
            )
        if item._gis._use_private_url_only:
            url: str = _get_item_url(item=item)
        else:
            url: str = _validate_url(item.url, item._gis)
        return cls(url, item._gis)

    # ----------------------------------------------------------------------
    @property
    def styles(self) -> dict:
        """
        The styles property returns styles for vector tiles in Mapbox GL
        Style specification version 8. The response for this styles resource
        includes the sprite and glyphs properties, with a relative path
        to the Vector Tile Sprite and Vector Tile Font resources.
        It also includes the version property,
        which represents the version of the style specification.
        """
        url = "{url}/resources/styles".format(url=self._url)
        resp: requests.Response = self._session.get(url=url)
        resp.raise_for_status()
        return resp.json()

    # ----------------------------------------------------------------------
    @property
    def tile_map(self) -> dict:
        """
        The tile_map property describes a quadtree of tiles and can be used to
        avoid requesting tiles that don't exist in the server. Each node
        of the tree has an associated tile. The root node (lod 0) covers
        the entire extent of the data. Children are identified by their position
        with NW, NE, SW, and SE. Tiles are identified by lod/h/v, where h and v
        are indexes on a 2^lod by 2^lod grid . These values are derived from the
        position in the tree. The tree has a variable depth. A node doesn't have
        children if the complexity of the data in the associated tile is below
        a threshold. This threshold is based on a combination of number of
        features, attributes, and vertices.

        """
        url = "{url}/tilemap".format(url=self._url)
        resp: requests.Response = self._session.get(url=url)
        resp.raise_for_status()
        return resp.json()

    # ----------------------------------------------------------------------
    @property
    def manager(self) -> VectorTileLayerManager:
        """
        The ``manager`` property returns an instance of :class:`~arcgis.layers.VectorTileLayerManager` class or
        :class:`~arcgis.layers.EnterpriseVectorTileLayerManager` class
        which provides methods and properties for administering this service.
        """
        if self._gis._portal.is_arcgisonline:
            rd = {"/rest/services/": "/rest/admin/services/"}
            adminURL = self._str_replace(self._url, rd)
            if adminURL.split("/")[-1].isdigit():
                adminURL = adminURL.replace(f'/{adminURL.split("/")[-1]}', "")
            self._admin = VectorTileLayerManager(adminURL, self._gis, self)
        else:
            rd = {
                "/rest/": "/admin/",
                "/VectorTileServer": ".VectorTileServer",
            }
            adminURL = self._str_replace(self._url, rd)
            if adminURL.split("/")[-1].isdigit():
                adminURL = adminURL.replace(f'/{adminURL.split("/")[-1]}', "")
            self._admin = EnterpriseVectorTileLayerManager(adminURL, self._gis, self)
        return self._admin

    # ----------------------------------------------------------------------
    @property
    def info(self) -> list:
        """
        The ``info`` property retrieves the relative paths to a list of resource files.

        :return:
           A list of relative paths
        """
        url = "{url}/resources/info".format(url=self._url)
        resp: requests.Response = self._session.get(url=url)
        resp.raise_for_status()
        res = resp.json()
        return res["resourceInfo"]

    # ----------------------------------------------------------------------
    def tile_fonts(self, fontstack: str, stack_range: str) -> str:
        """
        The ``tile_fonts`` method retrieves glyphs in
        `protocol buffer format. <https://developers.google.com/protocol-buffers/>`_

        ============================    ===================================================================================================================
        **Parameter**                    **Description**
        ----------------------------    -------------------------------------------------------------------------------------------------------------------
        fontstack                       Required string.

                                        .. note::
                                            The template url for this font resource is represented in the
                                            `Vector Tile Style <https://developers.arcgis.com/rest/services-reference/enterprise/vector-tile-style.htm>`_
                                            resource.
        ----------------------------    -------------------------------------------------------------------------------------------------------------------
        stack_range                     Required string that depict a range. Ex: "0-255"
        ============================    ===================================================================================================================

        :return:
            Glyphs in PBF format
        """
        url = "{url}/resources/fonts/{fontstack}/{stack_range}.pbf".format(
            url=self._url, fontstack=fontstack, stack_range=stack_range
        )
        resp: requests.Response = self._session.get(url=url)
        resp.raise_for_status()
        return resp.text

    # ----------------------------------------------------------------------
    def vector_tile(self, level: int, row: int, column: int) -> str:
        """
        The ``vector_tile`` method represents a single vector tile for the map.

        .. note::
            The bytes for the tile at the specified level, row and column are
            returned in PBF format. If a tile is not found, an error is returned.

        ============================    ================================================
        **Parameter**                    **Description**
        ----------------------------    ------------------------------------------------
        level                           Required string. A level number as a string.
        ----------------------------    ------------------------------------------------
        row                             Required string. Number of the row that the tile
                                        belongs to.
        ----------------------------    ------------------------------------------------
        column                          Required string. Number of the column that tile
                                        belongs to.
        ============================    ================================================

        :returns:
            Bytes in PBF format
        """
        url = "{url}/tile/{level}/{row}/{column}.pbf".format(
            url=self._url, level=level, row=row, column=column
        )
        resp: requests.Response = self._session.get(url=url)
        resp.raise_for_status()
        return resp.text

    # ----------------------------------------------------------------------
    def tile_sprite(self, out_format: str = "sprite.json") -> dict:
        """
        The ``tile_sprite`` resource retrieves sprite images and metadata.

        ============================    ================================================
        **Parameter**                    **Description**
        ----------------------------    ------------------------------------------------
        out_format                      Optional string. Default is "sprite.json".

                                        Values: ``sprite.json`` | ``sprite.png`` | ``sprite@2x.png``
        ============================    ================================================

        :return:
            Sprite image and metadata.
        """
        url = "{url}/resources/sprites/{f}".format(url=self._url, f=out_format)
        resp: requests.Response = self._session.get(url=url)
        resp.raise_for_status()
        return resp.json()

    # ----------------------------------------------------------------------
    def export_tiles(
        self,
        levels: str | None = None,
        export_extent: dict[str, Any] | None = None,
        polygon: dict[str, Any] | _geometry.Polygon | None = None,
        create_item: bool = False,
    ) -> str | Item:
        """
        Export vector tile layer

        =====================       =======================================================
        **Parameter**                **Description**
        ---------------------       -------------------------------------------------------
        levels                      Optional string.Specifies the tiled service levels to export.
                                    The values should correspond to Level IDs. The values
                                    can be comma-separated values or a range of values.
                                    Ensure that the tiles are present at each specified level.

                                    .. code-block:: python

                                        # Example:

                                        # Comma-separated values
                                        >>> levels=1,2,3,4,5,6,7,8,9

                                        //Range values
                                        >>> levels=1-4, 7-9
        ---------------------       -------------------------------------------------------
        export_extent               Optional dictionary of the extent (bounding box) of the vector
                                    tile package to be exported.
                                    The extent should be within the specified spatial reference.
                                    The default value is the full extent of the tiled map service.

                                    .. code-block:: python

                                        # Example:

                                        >>> export_extent = {
                                                             "xmin": -109.55, "ymin" : 25.76,
                                                             "xmax": -86.39, "ymax" : 49.94,
                                                             "spatialReference": {"wkid": 4326}
                                                            }
        ---------------------       -------------------------------------------------------
        polygon                     Optional dictionary.
                                    Introduced at 10.7. A JSON representation of a polygon,
                                    containing an array of rings and a spatialReference.

                                    .. code-block:: python

                                        # Example:

                                        polygon = {
                                                   "rings": [
                                                             [[6453,16815],[10653,16423],
                                                             [14549,5204],[-7003,6939],
                                                             [6453,16815]],[[914,7992],
                                                             [3140,11429],[1510,10525],
                                                             [914,7992]]
                                                            ],
                                                   "spatialReference": {"wkid": 54004}
                                                  }
        ---------------------       -------------------------------------------------------
        create_item                 Optional boolean. Indicated whether an item will be created
                                    from the export (True) or a path to a downloaded file (False).
                                    Default is False. ArcGIS Online Only.
        =====================       =======================================================

        :returns:
            A list of exported item dictionaries or a single path
        """
        if not self.properties.exportTilesAllowed:
            raise arcgis.gis.Error(
                "Export Tiles operation is not allowed for this service. Enable offline mode."
            )

        params = {
            "f": "json",
            "exportBy": "levelId",
            "storageFormatType": "Compact",
            "tilePackage": False,
            "optimizeTilesForSize": False,
        }
        params["levels"] = levels if levels else None

        if isinstance(export_extent, dict):
            compare_sr = lambda sr1, sr2: any(
                val in sr2.values() for val in sr1.values()
            ) or any(val in sr1.values() for val in sr2.values())

            if compare_sr(
                export_extent["spatialReference"],
                self.properties["fullExtent"]["spatialReference"],
            ):
                export_extent: str = json.dumps(export_extent)
            else:
                raise ValueError(
                    "The export_extent must be in the same spatial reference as the source."
                )
        elif export_extent is None:
            export_extent = json.dumps(dict(self.properties["fullExtent"]))
        params["exportExtent"] = export_extent
        # parameter introduced at 10.7
        if polygon and self.gis.version >= [7, 1]:
            if isinstance(polygon, dict):
                polygon: str = json.dumps(polygon)
            params["polygon"] = polygon
        if create_item is True:
            params["createItem"] = "on"
        url = "{url}/exportTiles".format(url=self._url)

        # a job is returned from the get
        resp: requests.Response = self._session.get(url=url, params=params)
        resp.raise_for_status()
        exportJob = resp.json()

        # get the job information
        path = "%s/jobs/%s" % (self._url, exportJob["jobId"])

        resp_params = {"f": "json"}
        resp: requests.Response = self._session.post(url=path, data=resp_params)
        resp.raise_for_status()
        job_response = resp.json()

        if "status" in job_response or "jobStatus" in job_response:
            status = job_response.get("status") or job_response.get("jobStatus")
            i = 0
            while not status == "esriJobSucceeded":
                if i < 10:
                    i = i + 1
                time.sleep(i)
                resp: requests.Response = self._session.post(url=path, data=resp_params)
                resp.raise_for_status()
                job_response = resp.json()
                status = job_response.get("status") or job_response.get("jobStatus")
                if status in [
                    "esriJobFailed",
                    "esriJobCancelling",
                    "esriJobCancelled",
                    "esriJobTimedOut",
                ]:
                    print(str(job_response["messages"]))
                    raise Exception("Job Failed with status " + status)
        else:
            raise Exception("No job results.")

        if "results" in job_response:
            value = job_response["results"]["out_service_url"]["paramUrl"]
            result_path = path + "/" + value
            resp: requests.Response = self._session.get(
                url=result_path,
                params={
                    "f": "json",
                },
            )
            resp.raise_for_status()
            allResults = resp.json()

            if "value" in allResults:
                value = allResults["value"]
                files: list[str] = []
                resp: requests.Response = self._session.get(
                    url=value,
                    params={
                        "f": "json",
                    },
                )
                resp.raise_for_status()
                gpRes = resp.json()
                for file in gpRes["files"]:
                    fn: str = file.get("name", f"{uuid.uuid4()}.vtpk")
                    fp: str = os.path.join(tempfile.gettempdir(), fn)
                    url: str | None = file.get("url", None)
                    if url:
                        resp: requests.Response = self._session.get(url, stream=True)
                        with open(fp, "wb") as writer:
                            for chunk in resp.iter_content(chunk_size=5 * 1024 * 1024):
                                if chunk:
                                    writer.write(chunk)
                        files.append(fp)
                return files
            else:
                return None
        elif "output" in job_response:
            allResults = job_response["output"]
            if allResults["itemId"]:
                return _gis.Item(gis=self._gis, itemid=allResults["itemId"])
            else:
                files: list[str] = []

                extract_filename_lambda = lambda content_disposition: (
                    re.search(r"filename=([^;]+)", content_disposition).group(1)
                    if re.search(r"filename=([^;]+)", content_disposition)
                    else f"{uuid.uuid4().hex}.vtpk"
                )
                if self._gis._portal.is_arcgisonline:
                    for url in allResults["outputUrl"]:
                        for k, v in urllib.parse.parse_qs(
                            urllib.parse.urlparse(url).query
                        ).items():
                            if k == "response-content-disposition":
                                fn = extract_filename_lambda(v[0])
                                fp = os.path.join(tempfile.gettempdir(), fn)
                                with open(fp, "wb") as writer:
                                    resp: requests.Response = requests.get(
                                        url, stream=True
                                    )
                                    for chunk in resp.iter_content(
                                        chunk_size=5 * 1024 * 1024
                                    ):
                                        if chunk:  # filter out keep-alive new chunks
                                            writer.write(chunk)
                                files.append(fp)
                    return files
                else:
                    for url in allResults["outputUrl"]:
                        for k, v in urllib.parse.parse_qs(
                            urllib.parse.urlparse(url).query
                        ).items():
                            if k == "response-content-disposition":
                                fn = extract_filename_lambda(v[0])
                                fp = os.path.join(tempfile.gettempdir(), fn)
                                with open(fp, "wb") as writer:
                                    # Session with streaming needs to be used here.
                                    resp: requests.Response = self._session.get(
                                        url, stream=True
                                    )
                                    for chunk in resp.iter_content(
                                        chunk_size=5 * 1024 * 1024
                                    ):
                                        if chunk:  # filter out keep-alive new chunks
                                            writer.write(chunk)
                                files.append(fp)
                    return files
        else:
            raise Exception(job_response)

    # ----------------------------------------------------------------------
    def _str_replace(self, mystring, rd):
        """Replaces a value based on a key/value pair where the
        key is the text to replace and the value is the new value.

        The find/replace is case insensitive.

        """
        import re

        patternDict = {}
        for key, value in rd.items():
            pattern = re.compile(re.escape(key), re.IGNORECASE)
            patternDict[value] = pattern
        for key in patternDict:
            regex_obj = patternDict[key]
            mystring = regex_obj.sub(key, mystring)
        return mystring
