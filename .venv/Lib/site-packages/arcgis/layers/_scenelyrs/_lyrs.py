from __future__ import annotations
import json
import requests
import re
from arcgis.gis import Layer, _GISResource, Item
from arcgis.geoprocessing import import_toolbox
from arcgis.auth.tools import LazyLoader


_layers = LazyLoader("arcgis.layers")


class SceneJob(_GISResource):
    """Represents a single Scene layer job"""

    def __init__(self, url: str, gis: "GIS", manager: "SceneLayerManager") -> None:
        self.url: str = url
        self.gis = gis
        self.manager: SceneLayerManager = manager

    @property
    def properties(self) -> dict:
        """returns the job's properties"""
        return self.gis._con.get(
            self.url,
            {
                "f": json,
            },
        )


class SceneLayerManager(_GISResource):
    """
    The ``SceneLayerManager`` class allows administration (if access permits) of ArcGIS Online hosted scene layers.
    A :class:`~arcgis.layers.SceneLayerManager` offers access to map and layer content.
    """

    def __init__(self, url: str, gis=None, scene_lyr=None):
        if url.split("/")[-1].isdigit():
            url = url.replace(f"/{url.split('/')[-1]}", "")
        super(SceneLayerManager, self).__init__(url, gis)
        self._sl = scene_lyr
        # Scene Layers published from Scene Layer Package are read only.
        if "layers" in self.properties:
            self._source_type = "Feature Service"

        else:
            # No layers are present so we will not have cache
            self._source_type = "Scene Layer Package"
        self._session = gis.session

    # ----------------------------------------------------------------------
    def refresh(self) -> dict | None:
        """
        The ``refresh`` operation refreshes a service, which clears the web
        server cache for the service.
        """
        if self._source_type == "Scene Layer Package":
            url = self._url + "/refresh"
            params = {"f": "json"}
            resp: requests.Response = self._session.post(url=url, data=params)
            resp.raise_for_status()
            res = resp.json()

            super(SceneLayerManager, self)._refresh()

            return res
        return None

    # ----------------------------------------------------------------------
    def swap(self, target_service_name: str) -> dict | None:
        """
        The swap operation replaces the current service cache with an existing one.

        .. note::
            The ``swap`` operation is for ArcGIS Online only.

        ====================        ====================================================
        **Parameter**                **Description**
        --------------------        ----------------------------------------------------
        target_service_name         Required string. Name of service you want to swap with.
        ====================        ====================================================

        :returns: dictionary indicating success or error

        """
        if self._source_type == "Scene Layer Package":
            url = self._url + "/swap"
            params = {"f": "json", "targetServiceName": target_service_name}
            resp: requests.Response = self._session.post(url=url, data=params)
            resp.raise_for_status()
            return resp.json()
        return None

    # ----------------------------------------------------------------------
    def jobs(self) -> dict | None:
        """
        The tile service job summary (jobs) resource represents a
        summary of all jobs associated with a vector tile service.
        Each job contains a jobid that corresponds to the specific
        jobid run and redirects you to the Job Statistics page.

        """
        if self._source_type == "Scene Layer Package":
            url = self._url + "/jobs"
            resp: requests.Response = self._session.get(url=url)
            resp.raise_for_status()
            return resp.json()
        return None

    # ----------------------------------------------------------------------
    def cancel_job(self, job_id: str) -> dict | None:
        """
        The ``cancel_job`` operation supports cancelling a job while update
        tiles is running from a hosted feature service. The result of this
        operation is a response indicating success or failure with error
        code and description.

        ===============     ====================================================
        **Parameter**        **Description**
        ---------------     ----------------------------------------------------
        job_id              Required String. The job id to cancel.
        ===============     ====================================================

        """
        if self._source_type == "Scene Layer Package":
            url = self._url + "/jobs/%s/cancel" % job_id
            params = {"f": "json"}
            resp: requests.Response = self._session.post(url=url, data=params)
            resp.raise_for_status()
            return resp.json()
        return None

    # ----------------------------------------------------------------------
    def job_statistics(self, job_id: str) -> dict | None:
        """
        Returns the job statistics for the given jobId

        """
        if self._source_type == "Scene Layer Package":
            url = self._url + "/jobs/%s" % job_id
            params = {"f": "json"}
            resp: requests.Response = self._session.post(url=url, data=params)
            resp.raise_for_status()
            return resp.json()
        return None

    # -----------------------------------------------------------------------
    def rerun_job(self, job_id: str, code: str) -> dict | None:
        """
        The ``rerun_job`` operation supports re-running a canceled job from a
        hosted map service. The result of this operation is a response
        indicating success or failure with error code and description.

        ===============     ====================================================
        **Parameter**        **Description**
        ---------------     ----------------------------------------------------
        code                Required string, parameter used to re-run a given
                            jobs with a specific error
                            code: ``ALL | ERROR | CANCELED``
        ---------------     ----------------------------------------------------
        job_id              Required string, job to reprocess
        ===============     ====================================================

        :return:
           A boolean or dictionary
        """
        if self._source_type == "Scene Layer Package":
            url = self._url + "/jobs/%s/rerun" % job_id
            params = {"f": "json", "rerun": code}
            resp: requests.Response = self._session.post(url=url, data=params)
            resp.raise_for_status()
            return resp.json()
        return None

    # ----------------------------------------------------------------------
    def import_package(self, item: str | Item) -> dict | None:
        """
        The ``import`` method imports from an :class:`~arcgis.gis.Item` object.

        ===============     ====================================================
        **Parameter**        **Description**
        ---------------     ----------------------------------------------------
        item                Required ItemId or :class:`~arcgis.gis.Item` object. The TPK file's item id.
                            This TPK file contains to-be-extracted bundle files
                            which are then merged into an existing cache service.
        ===============     ====================================================

        :return:
            A dictionary

        """
        if self._source_type == "Scene Layer Package":
            params = {
                "f": "json",
                "sourceItemId": None,
            }
            if isinstance(item, str):
                params["sourceItemId"] = item
            elif isinstance(item, Item):
                params["sourceItemId"] = item.itemid
            else:
                raise ValueError("The `item` must be a string or Item")
            url = self._url + "/import"
            resp: requests.Response = self._session.post(url=url, data=params)
            resp.raise_for_status()
            res = resp.json()
            return res
        return None

    # ----------------------------------------------------------------------
    def update(self) -> dict | None:
        """
        The ``update`` method starts update generation for ArcGIS Online. It updates
        the underlying source dataset for the service, essentially refreshing the
        underlying package data.

        :return:
           Dictionary.
        """
        if self._gis._portal.is_arcgisonline:
            url = "%s/update" % self._url
            params = {"f": "json"}
            resp: requests.Response = self._session.post(url=url, data=params)
            resp.raise_for_status()
            return resp.json()
        return None

    # ----------------------------------------------------------------------
    def edit(self, item: str | Item) -> dict | None:
        """
        The ``edit`` method edits from an :class:`~arcgis.gis.Item` object.

        ===============     ====================================================
        **Parameter**        **Description**
        ---------------     ----------------------------------------------------
        item                Required ItemId or :class:`~arcgis.gis.Item` object. The TPK file's item id.
                            This TPK file contains to-be-extracted bundle files
                            which are then merged into an existing cache service.
        ===============     ====================================================

        :return:
            A dictionary

        """
        if self._source_type == "Scene Layer Package":
            params = {
                "f": "json",
                "sourceItemId": None,
                "serviceDefinition": {"capabilities": ["View", "Query"]},
            }
            if isinstance(item, str):
                params["sourceItemId"] = item
            elif isinstance(item, Item):
                params["sourceItemId"] = item.itemid
            else:
                raise ValueError("The `item` must be a string or Item")
            url = self._url + "/edit"
            resp: requests.Response = self._session.post(url=url, data=params)
            resp.raise_for_status()
            res = resp.json()
            return res
        return None

    # ----------------------------------------------------------------------
    def rebuild_cache(self, layers: int | list[int]) -> dict | None:
        """
        The rebuild_cache operation update the scene layer cache to reflect
        any changes made to the feature layer used to publish this scene layer.
        The results of the operation is a response indicating success, which
        redirects you to the Job Statistics page, or failure.

        =====================       ====================================================
        **Parameter**                **Description**
        ---------------------       ----------------------------------------------------
        layers                      Required int or list of int. Comma separated values indicating
                                    the id of the layers to rebuild in the cache.

                                    Ex: [0,1,2]
        =====================       ====================================================
        """
        if self._source_type == "Feature Service":
            url = self._url + "/rebuildCache"
            params = {"f": "json", "layers": layers}
            resp: requests.Response = self._session.post(url=url, data=params)
            resp.raise_for_status()
            data: dict = resp.json()
            if "error" in data or ("success" in data and data["success"] == False):
                raise Exception(data)

            if "jobId" in data:
                job_url = f"{self.url}/jobs/{data.get('jobId')}"
            elif "jobUrl" in data:
                job_url = f'{self.url}/jobs/{data.get("jobUrl")}'
            else:
                raise Exception(f"Job ID not returned: {data}")
            return SceneJob(url=job_url, gis=self._gis, manager=self)
        return None

    # ----------------------------------------------------------------------
    def update_cache(self, layers: int | list[int]) -> dict | None:
        """
        Update Cache is a "light rebuild" where attributes and geometries of
        the layers selected are updated and can be used for change tracking on
        the feature layer to only update nodes with dirty tiles.
        The results of the operation is a response indicating success, which
        redirects you to the Job Statistics page, or failure.

        =====================       ====================================================
        **Parameter**                **Description**
        ---------------------       ----------------------------------------------------
        layers                      Required int or list of int. Comma separated values indicating
                                    the id of the layers to update in the cache.

                                    Ex: [0,1,2]
        =====================       ====================================================
        """
        if self._source_type == "Feature Service":
            url = self._url + "/updateCache"
            params = {"f": "json", "layers": layers}
            resp: requests.Response = self._session.post(url=url, data=params)
            resp.raise_for_status()
            return resp.json()
        return None

    # ----------------------------------------------------------------------
    def update_attribute(self, layers: int | list[int]) -> dict | None:
        """
        Update atrribute is a "light rebuild" where attributes of
        the layers selected are updated and can be used for change tracking.
        The results of the operation is a response indicating success, which
        redirects you to the Job Statistics page, or failure.

        =====================       ====================================================
        **Parameter**                **Description**
        ---------------------       ----------------------------------------------------
        layers                      Required int or list of int. Comma separated values indicating
                                    the id of the layers to update in the cache.

                                    Ex: [0,1,2]
        =====================       ====================================================
        """
        if self._source_type == "Feature Service":
            url = self._url + "/updateAttribute"
            params = {"f": "json", "layers": layers}
            resp: requests.Response = self._session.post(url=url, data=params)
            resp.raise_for_status()
            return resp.json()
        return None


###########################################################################
class EnterpriseSceneLayerManager(_GISResource):
    """
    The ``EnterpriseSceneLayerManager`` class allows administration (if access permits) of ArcGIS Enterprise hosted scene layers.
    A :class:`~arcgis.layers.SceneLayer` offers access to layer content.

    .. note:: Url must be admin url such as: ``https://services.myserver.com/arcgis/rest/admin/services/serviceName/SceneServer/``
    """

    _gptbx = None

    def __init__(self, url: str, gis=None, scene_lyr=None):
        if url.split("/")[-1].isdigit():
            url = url.replace(f"/{url.split('/')[-1]}", "")
        super(EnterpriseSceneLayerManager, self).__init__(url, gis)
        self._sl = scene_lyr

    # ----------------------------------------------------------------------
    def edit(self, service_dictionary: dict):
        """
        To edit a service, you need to submit the complete JSON
        representation of the service, which includes the updates to the
        service properties. Editing a service causes the service to be
        restarted with updated properties.

        ===================     ====================================================================
        **Parameter**            **Description**
        -------------------     --------------------------------------------------------------------
        service_dictionary     Required dict. The service JSON as a dictionary.
        ===================     ====================================================================


        :return: boolean
        """
        sl_service = _layers.Service(self.url, self._gis)
        return sl_service.edit(service_dictionary)

    # ----------------------------------------------------------------------
    def start(self):
        """starts the specific service"""
        sl_service = _layers.Service(self.url, self._gis)
        return sl_service.start()

    # ----------------------------------------------------------------------
    def stop(self):
        """stops the specific service"""
        sl_service = _layers.Service(self.url, self._gis)
        return sl_service.stop()

    # ----------------------------------------------------------------------
    def change_provider(self, provider: str):
        """
        Allows for the switching of the service provide and how it is hosted on the ArcGIS Server instance.

        Values:

        + 'ArcObjects' means the service is running under the ArcMap runtime i.e. published from ArcMap
        + 'ArcObjects11': means the service is running under the ArcGIS Pro runtime i.e. published from ArcGIS Pro
        + 'DMaps': means the service is running in the shared instance pool (and thus running under the ArcGIS Pro provider runtime)

        :return: Boolean

        """
        sl_service = _layers.Service(self.url, self._gis)
        return sl_service.change_provider(provider)

    # ----------------------------------------------------------------------
    def delete(self):
        """deletes a service from arcgis server"""
        sl_service = _layers.Service(self.url, self._gis)
        return sl_service.delete()

    # ----------------------------------------------------------------------
    @property
    def _tbx(self):
        """gets the toolbox"""
        if self._gptbx is None:
            self._gptbx = import_toolbox(
                url_or_item=self._gis.hosting_servers[0].url
                + "/System/SceneCachingControllers/GPServer",
                gis=self._gis,
            )
            self._gptbx._is_fa = True
        return self._gptbx

    # ----------------------------------------------------------------------
    def rebuild_cache(
        self,
        layer: list[int] | None = None,
        extent: dict | None = None,
        area_of_interest: dict | None = None,
    ) -> str:
        """
        The rebuild_cache operation update the scene layer cache to reflect
        any changes made to the feature layer used to publish this scene layer.
        The results of the operation is the url to the scene service once it is
        done rebuilding.

        ===============================     ====================================================================
        **Parameter**                        **Description**
        -------------------------------     --------------------------------------------------------------------
        layer                               Optional list of integers. The list of layers to cook.
        -------------------------------     --------------------------------------------------------------------
        extent                              Optional dict. The updated extent to be used. If nothing is specified,
                                            the default extent is used.
        -------------------------------     --------------------------------------------------------------------
        area_of_interest                    Optional dict representing a feature. Specify the updated area
                                            of interest.

                                            Syntax:
                                                {
                                                    "displayFieldName": "",
                                                    "geometryType": "esriGeometryPolygon",
                                                    "spatialReference": {
                                                    "wkid": 54051,
                                                    "latestWkid": 54051
                                                    },
                                                    "fields": [
                                                    {
                                                    "name": "OID",
                                                    "type": "esriFieldTypeOID",
                                                    "alias": "OID"
                                                    },
                                                    {
                                                    "name": "updateGeom_Length",
                                                    "type": "esriFieldTypeDouble",
                                                    "alias": "updateGeom_Length"
                                                    },
                                                    {
                                                    "name": "updateGeom_Area",
                                                    "type": "esriFieldTypeDouble",
                                                    "alias": "updateGeom_Area"
                                                    }
                                                    ],
                                                    "features": [],
                                                    "exceededTransferLimit": False
                                                }
        ===============================     ====================================================================

        :return: If successful, the url to the scene service

        """
        if layer is None:
            layer = {}
        elif layer is not None:
            layer = {layer}
        if extent is None:
            extent = "DEFAULT"
        if area_of_interest is None:
            return self._tbx.manage_scene_cache(
                service_url=self._sl.url,
                num_of_caching_service_instances=2,
                layer=layer,
                update_mode="RECREATE_ALL_NODES",
                update_extent=extent,
            )
        else:
            return self._tbx.manage_scene_cache(
                service_url=self._sl.url,
                num_of_caching_service_instances=2,
                layer=layer,
                update_mode="RECREATE_ALL_NODES",
                update_extent=extent,
                area_of_interest=area_of_interest,
            )

    # ----------------------------------------------------------------------
    def update_cache(
        self,
        layer: list[int] | None = None,
        extent: dict | None = None,
        area_of_interest: dict | None = None,
    ) -> str:
        """
        Update Cache is a "light rebuild" where attributes and geometries of
        the layers selected are updated and can be used for change tracking on
        the feature layer to only update nodes with dirty tiles,.
        The results of the operation is the url to the scene service once it is
        done updating.

        ===============================     ====================================================================
        **Parameter**                        **Description**
        -------------------------------     --------------------------------------------------------------------
        layer                               Optional list of integers. The list of layers to cook.
        -------------------------------     --------------------------------------------------------------------
        extent                              Optional dict. The updated extent to be used. If nothing is specified,
                                            the default extent is used.
        -------------------------------     --------------------------------------------------------------------
        area_of_interest                    Optional dict representing a feature. Specify the updated area
                                            of interest.

                                            Syntax:
                                                {
                                                    "displayFieldName": "",
                                                    "geometryType": "esriGeometryPolygon",
                                                    "spatialReference": {
                                                    "wkid": 54051,
                                                    "latestWkid": 54051
                                                    },
                                                    "fields": [
                                                    {
                                                    "name": "OID",
                                                    "type": "esriFieldTypeOID",
                                                    "alias": "OID"
                                                    },
                                                    {
                                                    "name": "updateGeom_Length",
                                                    "type": "esriFieldTypeDouble",
                                                    "alias": "updateGeom_Length"
                                                    },
                                                    {
                                                    "name": "updateGeom_Area",
                                                    "type": "esriFieldTypeDouble",
                                                    "alias": "updateGeom_Area"
                                                    }
                                                    ],
                                                    "features": [],
                                                    "exceededTransferLimit": False
                                                }
        ===============================     ====================================================================

        :return: If successful, the url to the scene service

        """
        if layer is None:
            layer = {}
        elif layer is not None:
            layer = {layer}
        if extent is None:
            extent = "DEFAULT"
        if area_of_interest is None:
            return self._tbx.manage_scene_cache(
                service_url=self._sl.url,
                num_of_caching_service_instances=2,
                layer=layer,
                update_mode="PARTIAL_UPDATE_NODES",
                update_extent=extent,
            )
        else:
            return self._tbx.manage_scene_cache(
                service_url=self._sl.url,
                num_of_caching_service_instances=2,
                layer=layer,
                update_mode="PARTIAL_UPDATE_NODES",
                update_extent=extent,
                area_of_interest=area_of_interest,
            )

    # ----------------------------------------------------------------------
    def update_attribute(
        self,
        layer: list[int] | None = None,
        extent: dict | None = None,
        area_of_interest: dict | None = None,
    ) -> str:
        """
        Update attribute is a "light rebuild" where attributes of
        the layers selected are updated and can be used for change tracking.
        The results of the operation is the url to the scene service once it is
        done updating.

        ===============================     ====================================================================
        **Parameter**                        **Description**
        -------------------------------     --------------------------------------------------------------------
        layer                               Optional list of integers. The list of layers to cook.
        -------------------------------     --------------------------------------------------------------------
        extent                              Optional dict. The updated extent to be used. If nothing is specified,
                                            the default extent is used.
        -------------------------------     --------------------------------------------------------------------
        area_of_interest                    Optional dict representing a feature. Specify the updated area
                                            of interest.

                                            Syntax:
                                                {
                                                    "displayFieldName": "",
                                                    "geometryType": "esriGeometryPolygon",
                                                    "spatialReference": {
                                                    "wkid": 54051,
                                                    "latestWkid": 54051
                                                    },
                                                    "fields": [
                                                    {
                                                    "name": "OID",
                                                    "type": "esriFieldTypeOID",
                                                    "alias": "OID"
                                                    },
                                                    {
                                                    "name": "updateGeom_Length",
                                                    "type": "esriFieldTypeDouble",
                                                    "alias": "updateGeom_Length"
                                                    },
                                                    {
                                                    "name": "updateGeom_Area",
                                                    "type": "esriFieldTypeDouble",
                                                    "alias": "updateGeom_Area"
                                                    }
                                                    ],
                                                    "features": [],
                                                    "exceededTransferLimit": false
                                                }
        ===============================     ====================================================================

        :return: If successful, the url to the scene service
        """
        if layer is None:
            layer = {}
        elif layer is not None:
            layer = {layer}
        if extent is None:
            extent = "DEFAULT"
        if area_of_interest is None:
            return self._tbx.manage_scene_cache(
                service_url=self._sl.url,
                num_of_caching_service_instances=2,
                layer=layer,
                update_mode="PARTIAL_UPDATE_ATTRIBUTES",
                update_extent=extent,
            )
        else:
            return self._tbx.manage_scene_cache(
                service_url=self._sl.url,
                num_of_caching_service_instances=2,
                layer=layer,
                update_mode="PARTIAL_UPDATE_ATTRIBUTES",
                update_extent=extent,
                area_of_interest=area_of_interest,
            )


###########################################################################
class Object3DLayer(Layer):
    """
    The ``Object3DLayer`` represents a Web scene 3D Object layer.

    .. note::
        Web scene layers are cached web layers that are optimized for displaying a large amount of 2D and 3D features.
        See the :class:`~arcgis.layers.SceneLayer` class for more information.

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
        >> arcgis.layers.Point3DLayer

        print(s_layer.properties.layers[0].name)
        >> 'your layer name'
    """

    def __init__(self, url: str, gis=None, parent_url=None):
        """
        Constructs a SceneLayer given a web scene layer URL
        """
        super(Object3DLayer, self).__init__(url, gis)
        self._admin = None
        self._parent_url = parent_url

    @property
    def _lyr_dict(self):
        lyr_dict = {"type": "SceneLayer", "url": self.url}
        if hasattr(self, "_token") and self._token:
            lyr_dict["serviceToken"] = self._token
        if hasattr(self, "filter") and self.filter:
            lyr_dict["filter"] = self.filter
        if hasattr(self, "_time_filter") and self._time_filter:
            lyr_dict["time"] = self._time_filter
        return lyr_dict

    # ----------------------------------------------------------------------
    @property
    def _lyr_json(self):
        url = self.url
        if self._token is not None:
            url += "?token=" + self._token

        lyr_dict = {"type": "SceneLayer", "url": url}

        if self.filter is not None:
            lyr_dict["options"] = json.dumps({"definition_expression": self.filter})
        if self._time_filter is not None:
            lyr_dict["time"] = self._time_filter
        return lyr_dict

    # ----------------------------------------------------------------------
    @property
    def manager(self) -> SceneLayerManager | EnterpriseSceneLayerManager:
        """
        The ``manager`` property returns an instance of :class:`~arcgis.layers.SceneLayerManager` class
        or :class:`~arcgis.layers.EnterpriseSceneLayerManager` class
        which provides methods and properties for administering this service.
        """
        if self._admin is None:
            url = self._parent_url or self._url  # prioritize parent_url if available
            rd = (
                {"/rest/services/": "/rest/admin/services/"}
                if self._gis._portal.is_arcgisonline
                else {"/rest/": "/admin/", "/SceneServer": ".SceneServer"}
            )
            adminURL = self._str_replace(url, rd)
            if adminURL.split("/")[-1].isdigit():
                adminURL = "/".join(adminURL.split("/")[:-1])
            self._admin = (
                SceneLayerManager(adminURL, self._gis, self)
                if self._gis._portal.is_arcgisonline
                else EnterpriseSceneLayerManager(adminURL, self._gis, self)
            )
        return self._admin

    # ----------------------------------------------------------------------
    def _str_replace(self, mystring, rd):
        """Replaces a value based on a key/value pair where the
        key is the text to replace and the value is the new value.

        The find/replace is case insensitive.

        """

        patternDict = {}
        for key, value in rd.items():
            pattern = re.compile(re.escape(key), re.IGNORECASE)
            patternDict[value] = pattern
        for key in patternDict:
            regex_obj = patternDict[key]
            mystring = regex_obj.sub(key, mystring)
        return mystring


###########################################################################
class IntegratedMeshLayer(Layer):
    """
    The ``IntegratedMeshLayer`` class represents a Web scene Integrated Mesh layer.

    .. note::
        Web scene layers are cached web layers that are optimized for displaying a large amount of 2D and 3D features.
        See the :class:`~arcgis.layers.SceneLayer` class for more information.

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
        >> arcgis.layers.Point3DLayer

        print(s_layer.properties.layers[0].name)
        >> 'your layer name'
    """

    def __init__(self, url: str, gis=None, parent_url=None):
        """
        Constructs a SceneLayer given a web scene layer URL
        """
        super(IntegratedMeshLayer, self).__init__(url, gis)
        self._admin = None
        self._parent_url = parent_url

    @property
    def _lyr_dict(self):
        lyr_dict = {"type": "IntegratedMeshLayer", "url": self.url}
        if hasattr(self, "_token") and self._token:
            lyr_dict["serviceToken"] = self._token
        if hasattr(self, "filter") and self.filter:
            lyr_dict["filter"] = self.filter
        if hasattr(self, "_time_filter") and self._time_filter:
            lyr_dict["time"] = self._time_filter
        return lyr_dict

    # ----------------------------------------------------------------------
    @property
    def _lyr_json(self):
        url = self.url
        if self._token is not None:
            url += "?token=" + self._token

        lyr_dict = {"type": "IntegratedMeshLayer", "url": url}

        if self.filter is not None:
            lyr_dict["options"] = json.dumps({"definition_expression": self.filter})
        if self._time_filter is not None:
            lyr_dict["time"] = self._time_filter
        return lyr_dict

    # ----------------------------------------------------------------------
    @property
    def manager(self) -> SceneLayerManager | EnterpriseSceneLayerManager:
        """
        The ``manager`` property returns an instance of :class:`~arcgis.layers.SceneLayerManager` class
        or :class:`~arcgis.layers.EnterpriseSceneLayerManager` class
        which provides methods and properties for administering this service.
        """
        if self._admin is None:
            url = self._parent_url or self._url  # prioritize parent_url if available
            rd = (
                {"/rest/services/": "/rest/admin/services/"}
                if self._gis._portal.is_arcgisonline
                else {"/rest/": "/admin/", "/SceneServer": ".SceneServer"}
            )
            adminURL = self._str_replace(url, rd)
            if adminURL.split("/")[-1].isdigit():
                adminURL = "/".join(adminURL.split("/")[:-1])
            self._admin = (
                SceneLayerManager(adminURL, self._gis, self)
                if self._gis._portal.is_arcgisonline
                else EnterpriseSceneLayerManager(adminURL, self._gis, self)
            )
        return self._admin

    # ----------------------------------------------------------------------
    def _str_replace(self, mystring, rd):
        """Replaces a value based on a key/value pair where the
        key is the text to replace and the value is the new value.

        The find/replace is case insensitive.

        """

        patternDict = {}
        for key, value in rd.items():
            pattern = re.compile(re.escape(key), re.IGNORECASE)
            patternDict[value] = pattern
        for key in patternDict:
            regex_obj = patternDict[key]
            mystring = regex_obj.sub(key, mystring)
        return mystring


###########################################################################
class Tiles3DLayerManager(_GISResource):
    def __init__(self, url, gis=None, tiles3d_service=None):
        if url.split("/")[-1].isdigit():
            url = url.replace(f"/{url.split('/')[-1]}", "")
        super(Tiles3DLayerManager, self).__init__(url, gis)
        self._tiles3dservice = tiles3d_service
        # Scene Layers published from Scene Layer Package are read only.
        if "layers" in self.properties:
            self._source_type = (
                "Feature Service"
                if "updateEnabled" in self.properties.layers[0]
                else "3DTiles Package"
            )
        else:
            # No layers are present so we will not have cache
            self._source_type = "3DTilesPackage"


###########################################################################
class Tiles3DLayer(Layer):
    """
    The ``Tiles3DLayer`` class represents a Web scene 3D Tile Service Layer.

    .. note::
        Web scene layers are cached web layers that are optimized for displaying a large amount of 2D and 3D features.
        See the :class:`~arcgis.layers.SceneLayer` class for more information.

    ==================     ====================================================================
    **Parameter**           **Description**
    ------------------     --------------------------------------------------------------------
    url                    Required string, specify the url ending in /3DTilesServer/
    ------------------     --------------------------------------------------------------------
    gis                    Optional :class:`~arcgis.gis.GIS`  object. If not specified, the active GIS connection is
                           used.
    ==================     ====================================================================
    """

    def __init__(self, url: str, gis=None, parent_url=None):
        """
        Constructs a Tiles3D Layer given a web scene layer URL
        """
        super(Tiles3DLayer, self).__init__(url, gis)
        self._admin = None
        self._parent_url = parent_url

    @property
    def _lyr_dict(self):
        lyr_dict = {"type": "3DTiles Service", "url": self.url}
        if hasattr(self, "_token") and self._token:
            lyr_dict["serviceToken"] = self._token
        if hasattr(self, "filter") and self.filter:
            lyr_dict["filter"] = self.filter
        if hasattr(self, "_time_filter") and self._time_filter:
            lyr_dict["time"] = self._time_filter
        return lyr_dict

    # ----------------------------------------------------------------------
    @property
    def _lyr_json(self):
        url = self.url
        if self._token is not None:
            url += "?token=" + self._token

        lyr_dict = {"type": "3DTiles Service", "url": url}

        if self.filter is not None:
            lyr_dict["options"] = json.dumps({"definition_expression": self.filter})
        if self._time_filter is not None:
            lyr_dict["time"] = self._time_filter
        return lyr_dict

    # ----------------------------------------------------------------------
    @property
    def manager(self) -> Tiles3DLayerManager:
        """
        The ``manager`` property returns an instance of :class:`~arcgis.layers.Tiles3DLayerManager` class
        which provides methods and properties for administering this service.
        """
        if self._admin is None:
            if self._gis._portal.is_arcgisonline:
                rd = {"/rest/services/": "/rest/admin/services/"}
                adminURL = self._str_replace(self._url, rd)
                if adminURL.split("/")[-1].isdigit():
                    adminURL = adminURL.replace(f'/{adminURL.split("/")[-1]}', "")
            else:
                rd = {
                    "/rest/": "/admin/",
                    "/3DTilesServer": ".3DTilesServer",
                }
                adminURL = self._str_replace(self._url, rd)
                if adminURL.split("/")[-1].isdigit():
                    adminURL = "/".join(adminURL.split("/")[:-1])
            self._admin = Tiles3DLayerManager(adminURL, self._gis, self)
        return self._admin

    # ----------------------------------------------------------------------
    def _str_replace(self, mystring, rd):
        """Replaces a value based on a key/value pair where the
        key is the text to replace and the value is the new value.

        The find/replace is case insensitive.

        """

        patternDict = {}
        for key, value in rd.items():
            pattern = re.compile(re.escape(key), re.IGNORECASE)
            patternDict[value] = pattern
        for key in patternDict:
            regex_obj = patternDict[key]
            mystring = regex_obj.sub(key, mystring)
        return mystring


###########################################################################


class VoxelLayer(Layer):
    """
    The ``VoxelLayer`` class represents a Web Scene Voxel layer.

    .. note::
        Web scene layers are cached web layers that are optimized for displaying
        a large amount of 2D and 3D features. See the
        :class:`~arcgis.layers.SceneLayer` class for more information.

    ==================     =============================================================
    **Parameter**           **Description**
    ------------------     -------------------------------------------------------------
    url                    Required string, specify the url ending in ``/SceneServer/``
    ------------------     -------------------------------------------------------------
    gis                    Optional :class:`~arcgis.gis.GIS` object. If not specified,
                           the active GIS connection is used.
    ==================     =============================================================

    .. code-block:: python

        # USAGE EXAMPLE 1: Instantiating a SceneLayer object

        from arcgis.layers import SceneLayer
        s_layer = SceneLayer(url='https://your_portal.com/arcgis/rest/services/service_name/SceneServer/')

        type(s_layer)
        >> arcgis.layers.VoxelLayer

        print(s_layer.properties.layers[0].name)
        >> 'your layer name'
    """

    def __init__(self, url: str, gis=None, parent_url=None):
        """
        Constructs a SceneLayer given a web scene layer URL
        """
        super(VoxelLayer, self).__init__(url, gis)
        self._admin = None
        self._parent_url = parent_url

    @property
    def _lyr_dict(self):
        lyr_dict = {"type": "VoxelLayer", "url": self.url}
        if hasattr(self, "_token") and self._token:
            lyr_dict["serviceToken"] = self._token
        if hasattr(self, "filter") and self.filter:
            lyr_dict["filter"] = self.filter
        if hasattr(self, "_time_filter") and self._time_filter:
            lyr_dict["time"] = self._time_filter
        return lyr_dict

    # ----------------------------------------------------------------------
    @property
    def _lyr_json(self):
        url = self.url
        if self._token is not None:
            url += "?token=" + self._token

        lyr_dict = {"type": "VoxelLayer", "url": url}

        if self.filter is not None:
            lyr_dict["options"] = json.dumps({"definition_expression": self.filter})
        if self._time_filter is not None:
            lyr_dict["time"] = self._time_filter
        return lyr_dict

    # ----------------------------------------------------------------------
    @property
    def manager(self) -> SceneLayerManager | EnterpriseSceneLayerManager:
        """
        The ``manager`` property returns an instance of :class:`~arcgis.layers.SceneLayerManager` class
        or :class:`~arcgis.layers.EnterpriseSceneLayerManager` class
        which provides methods and properties for administering this service.
        """
        if self._admin is None:
            url = self._parent_url or self._url  # prioritize parent_url if available
            rd = (
                {"/rest/services/": "/rest/admin/services/"}
                if self._gis._portal.is_arcgisonline
                else {"/rest/": "/admin/", "/SceneServer": ".SceneServer"}
            )
            adminURL = self._str_replace(url, rd)
            if adminURL.split("/")[-1].isdigit():
                adminURL = "/".join(adminURL.split("/")[:-1])
            self._admin = (
                SceneLayerManager(adminURL, self._gis, self)
                if self._gis._portal.is_arcgisonline
                else EnterpriseSceneLayerManager(adminURL, self._gis, self)
            )
        return self._admin

    # ----------------------------------------------------------------------
    def _str_replace(self, mystring, rd):
        """Replaces a value based on a key/value pair where the
        key is the text to replace and the value is the new value.

        The find/replace is case insensitive.

        """

        patternDict = {}
        for key, value in rd.items():
            pattern = re.compile(re.escape(key), re.IGNORECASE)
            patternDict[value] = pattern
        for key in patternDict:
            regex_obj = patternDict[key]
            mystring = regex_obj.sub(key, mystring)
        return mystring


###########################################################################
class Point3DLayer(Layer):
    """
    The ``Point3DLayer`` class represents a Web scene 3D Point layer.

    .. note::
        Web scene layers are cached web layers that are optimized for displaying a large amount of 2D and 3D features.
        See the :class:`~arcgis.layers.SceneLayer` class for more information.

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
        >> arcgis.layers.Point3DLayer

        print(s_layer.properties.layers[0].name)
        >> 'your layer name'
    """

    def __init__(self, url: str, gis=None, parent_url=None):
        """
        Constructs a SceneLayer given a web scene layer URL
        """
        super(Point3DLayer, self).__init__(url, gis)
        self._admin = None
        self._parent_url = parent_url

    # ----------------------------------------------------------------------
    @property
    def _lyr_dict(self):
        lyr_dict = {"type": "SceneLayer", "url": self.url}
        if hasattr(self, "_token") and self._token:
            lyr_dict["serviceToken"] = self._token
        if hasattr(self, "filter") and self.filter:
            lyr_dict["filter"] = self.filter
        if hasattr(self, "_time_filter") and self._time_filter:
            lyr_dict["time"] = self._time_filter
        return lyr_dict

    # ----------------------------------------------------------------------
    @property
    def _lyr_json(self):
        url = self.url
        if self._token is not None:
            url += "?token=" + self._token

        lyr_dict = {"type": "SceneLayer", "url": url}

        if self.filter is not None:
            lyr_dict["options"] = json.dumps({"definition_expression": self.filter})
        if self._time_filter is not None:
            lyr_dict["time"] = self._time_filter
        return lyr_dict

    # ----------------------------------------------------------------------
    @property
    def manager(self) -> SceneLayerManager | EnterpriseSceneLayerManager:
        """
        The ``manager`` property returns an instance of :class:`~arcgis.layers.SceneLayerManager` class
        or :class:`~arcgis.layers.EnterpriseSceneLayerManager` class
        which provides methods and properties for administering this service.
        """
        if self._admin is None:
            url = self._parent_url or self._url  # prioritize parent_url if available
            rd = (
                {"/rest/services/": "/rest/admin/services/"}
                if self._gis._portal.is_arcgisonline
                else {"/rest/": "/admin/", "/SceneServer": ".SceneServer"}
            )
            adminURL = self._str_replace(url, rd)
            if adminURL.split("/")[-1].isdigit():
                adminURL = "/".join(adminURL.split("/")[:-1])
            self._admin = (
                SceneLayerManager(adminURL, self._gis, self)
                if self._gis._portal.is_arcgisonline
                else EnterpriseSceneLayerManager(adminURL, self._gis, self)
            )
        return self._admin

    # ----------------------------------------------------------------------
    def _str_replace(self, mystring, rd):
        """Replaces a value based on a key/value pair where the
        key is the text to replace and the value is the new value.

        The find/replace is case insensitive.

        """

        patternDict = {}
        for key, value in rd.items():
            pattern = re.compile(re.escape(key), re.IGNORECASE)
            patternDict[value] = pattern
        for key in patternDict:
            regex_obj = patternDict[key]
            mystring = regex_obj.sub(key, mystring)
        return mystring


###########################################################################
class PointCloudLayer(Layer):
    """
    The ``PointCloudLayer`` class represents a Web scene Point Cloud layer.

    .. note::
        Point Cloud layers are cached web layers that are optimized for displaying a large amount of 2D and 3D features.
        See the :class:`~arcgis.layers.SceneLayer` class for more information.

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

    def __init__(self, url: str, gis=None, parent_url=None):
        """
        Constructs a SceneLayer given a web scene layer URL
        """
        super(PointCloudLayer, self).__init__(url, gis)
        self._admin = None
        self._parent_url = parent_url

    @property
    def _lyr_dict(self):
        lyr_dict = {"type": "PointCloudLayer", "url": self.url}
        if hasattr(self, "_token") and self._token:
            lyr_dict["serviceToken"] = self._token
        if hasattr(self, "filter") and self.filter:
            lyr_dict["filter"] = self.filter
        if hasattr(self, "_time_filter") and self._time_filter:
            lyr_dict["time"] = self._time_filter
        return lyr_dict

    # ----------------------------------------------------------------------
    @property
    def _lyr_json(self):
        url = self.url
        if self._token is not None:
            url += "?token=" + self._token

        lyr_dict = {"type": "PointCloudLayer", "url": url}

        if self.filter is not None:
            lyr_dict["options"] = json.dumps({"definition_expression": self.filter})
        if self._time_filter is not None:
            lyr_dict["time"] = self._time_filter
        return lyr_dict

    # ----------------------------------------------------------------------
    @property
    def manager(self) -> SceneLayerManager | EnterpriseSceneLayerManager:
        """
        The ``manager`` property returns an instance of :class:`~arcgis.layers.SceneLayerManager` class
        or :class:`~arcgis.layers.EnterpriseSceneLayerManager` class
        which provides methods and properties for administering this service.
        """
        if self._admin is None:
            url = self._parent_url or self._url  # prioritize parent_url if available
            rd = (
                {"/rest/services/": "/rest/admin/services/"}
                if self._gis._portal.is_arcgisonline
                else {"/rest/": "/admin/", "/SceneServer": ".SceneServer"}
            )
            adminURL = self._str_replace(url, rd)
            if adminURL.split("/")[-1].isdigit():
                adminURL = "/".join(adminURL.split("/")[:-1])
            self._admin = (
                SceneLayerManager(adminURL, self._gis, self)
                if self._gis._portal.is_arcgisonline
                else EnterpriseSceneLayerManager(adminURL, self._gis, self)
            )
        return self._admin

    # ----------------------------------------------------------------------
    def _str_replace(self, mystring, rd):
        """Replaces a value based on a key/value pair where the
        key is the text to replace and the value is the new value.

        The find/replace is case insensitive.

        """

        patternDict = {}
        for key, value in rd.items():
            pattern = re.compile(re.escape(key), re.IGNORECASE)
            patternDict[value] = pattern
        for key in patternDict:
            regex_obj = patternDict[key]
            mystring = regex_obj.sub(key, mystring)
        return mystring


###########################################################################
class BuildingLayer(Layer):
    """
    The ``BuildingLayer`` class represents a Web building layer.

    .. note::
        Web scene layers are cached web layers that are optimized for displaying a large amount of 2D and 3D features.
        See the :class:`~arcgis.layers.SceneLayer` class for more information.

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
        >> arcgis.layers.BuildingLayer

        print(s_layer.properties.layers[0].name)
        >> 'your layer name'
    """

    def __init__(self, url: str, gis=None, parent_url=None):
        """
        Constructs a SceneLayer given a web scene layer URL
        """
        super(BuildingLayer, self).__init__(url, gis)
        self._admin = None
        self._parent_url = parent_url

    @property
    def _lyr_dict(self):
        lyr_dict = {"type": "BuildingSceneLayer", "url": self.url}
        if hasattr(self, "_token") and self._token:
            lyr_dict["serviceToken"] = self._token
        if hasattr(self, "filter") and self.filter:
            lyr_dict["filter"] = self.filter
        if hasattr(self, "_time_filter") and self._time_filter:
            lyr_dict["time"] = self._time_filter
        return lyr_dict

    # ----------------------------------------------------------------------
    @property
    def _lyr_json(self):
        url = self.url
        if self._token is not None:
            url += "?token=" + self._token

        lyr_dict = {"type": "BuildingSceneLayer", "url": url}

        if self.filter is not None:
            lyr_dict["options"] = json.dumps({"definition_expression": self.filter})
        if self._time_filter is not None:
            lyr_dict["time"] = self._time_filter
        return lyr_dict

    # ----------------------------------------------------------------------
    @property
    def manager(self) -> SceneLayerManager | EnterpriseSceneLayerManager:
        """
        The ``manager`` property returns an instance of :class:`~arcgis.layers.SceneLayerManager` class
        or :class:`~arcgis.layers.EnterpriseSceneLayerManager` class
        which provides methods and properties for administering this service.
        """
        if self._admin is None:
            url = self._parent_url or self._url  # prioritize parent_url if available
            rd = (
                {"/rest/services/": "/rest/admin/services/"}
                if self._gis._portal.is_arcgisonline
                else {"/rest/": "/admin/", "/SceneServer": ".SceneServer"}
            )
            adminURL = self._str_replace(url, rd)
            if adminURL.split("/")[-1].isdigit():
                adminURL = "/".join(adminURL.split("/")[:-1])
            self._admin = (
                SceneLayerManager(adminURL, self._gis, self)
                if self._gis._portal.is_arcgisonline
                else EnterpriseSceneLayerManager(adminURL, self._gis, self)
            )
        return self._admin

    # ----------------------------------------------------------------------
    def _str_replace(self, mystring, rd):
        """Replaces a value based on a key/value pair where the
        key is the text to replace and the value is the new value.

        The find/replace is case-insensitive.

        """

        patternDict = {}
        for key, value in rd.items():
            pattern = re.compile(re.escape(key), re.IGNORECASE)
            patternDict[value] = pattern
        for key in patternDict:
            regex_obj = patternDict[key]
            mystring = regex_obj.sub(key, mystring)
        return mystring


###########################################################################
class _SceneLayerFactory(type):
    """
    Factory that generates the Scene Layers

    ==================     ====================================================================
    **Parameter**           **Description**
    ------------------     --------------------------------------------------------------------
    url                    Required string, specify the url ending in /SceneServer/
    ------------------     --------------------------------------------------------------------
    gis                    Optional :class:`~arcgis.gis.GIS`  object. If not specified, the active GIS connection is
                           used.
    ------------------     --------------------------------------------------------------------
    parent_url             Optional string, specify the parent url of the service.
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

    def __call__(cls, url, gis=None, parent_url=None):
        lyr = Layer(url=url, gis=gis)
        props = lyr.properties
        layer_type_map = {
            "pointcloud": PointCloudLayer,
            "point": Point3DLayer,
            "3dobject": Object3DLayer,
            "building": BuildingLayer,
            "integratedmesh": IntegratedMeshLayer,
            "voxel": VoxelLayer,
        }
        if "sublayers" in props:
            return BuildingLayer(url=url, gis=gis, parent_url=parent_url)
        elif "layerType" in props:
            lt = props.layerType
        else:
            lt = props.layers[0].layerType
        return layer_type_map.get(str(lt).lower(), Layer)(
            url=url, gis=gis, parent_url=parent_url
        )


###########################################################################
class SceneLayer(Layer, metaclass=_SceneLayerFactory):
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

    def __init__(self, url: str, gis=None, parent_url=None):
        """
        Constructs a SceneLayer given a web scene layer URL
        """
        super(SceneLayer, self).__init__(url, gis, parent_url)
        self._parent_url = parent_url
