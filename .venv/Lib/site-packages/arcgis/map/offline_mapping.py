from __future__ import annotations
from warnings import warn
from typing import Any
import logging
from re import findall
from arcgis.auth.tools import LazyLoader

os = LazyLoader("os")
json = LazyLoader("json")
arcgismapping = LazyLoader("arcgis.map")
_gis_mod = LazyLoader("arcgis.gis")
_dt = LazyLoader("datetime")
_mixins = LazyLoader("arcgis._impl.common._mixins")
_geometry = LazyLoader("arcgis.geometry")
arcgis_layers = LazyLoader("arcgis.layers")
_log = logging.getLogger(__name__)


###########################################################################
class PackagingJob(object):
    """
    The ``PackagingJob`` class represents a Single Packaging Job.


    ================  ===============================================================
    **Parameter**      **Description**
    ----------------  ---------------------------------------------------------------
    future            Required `Future <https://docs.python.org/3/library/concurrent.futures.html>`_ object. The async object created by
                      the ``geoprocessing`` :class:`~arcgis.geoprocessing.GPTask`.
    ----------------  ---------------------------------------------------------------
    notify            Optional Boolean.  When set to ``True``, a message will inform the
                      user that the ``geoprocessing`` task has completed. The default is
                      ``False``.
    ================  ===============================================================

    """

    _future = None
    _gis = None
    _start_time = None
    _end_time = None

    # ----------------------------------------------------------------------
    def __init__(self, future, notify=False):
        """
        initializer
        """
        self._future = future
        self._start_time = _dt.datetime.now()
        if notify:
            self._future.add_done_callback(self._notify)
        self._future.add_done_callback(self._set_end_time)

    # ----------------------------------------------------------------------
    @property
    def elapse_time(self) -> _dt.timedelta | str:
        """
        Reports the total amount of time that passed while the
        :class:`~arcgis.layers.PackagingJob` ran.

        :return:
            The elapsed time

        """
        if self._end_time:
            return self._end_time - self._start_time
        else:
            return _dt.datetime.now() - self._start_time

    # ----------------------------------------------------------------------
    def _set_end_time(self, future):
        """sets the finish time"""
        self._end_time = _dt.datetime.now()

    # ----------------------------------------------------------------------
    def _notify(self, future):
        """prints finished method"""
        jobid = str(self).replace("<", "").replace(">", "")
        try:
            future.result()
            infomsg = "{jobid} finished successfully.".format(jobid=jobid)
            _log.info(infomsg)
            print(infomsg)
        except Exception as e:
            msg = str(e)
            msg = "{jobid} failed: {msg}".format(jobid=jobid, msg=msg)
            _log.info(msg)
            print(msg)

    # ----------------------------------------------------------------------
    def __str__(self):
        return "<Packaging Job>"

    # ----------------------------------------------------------------------
    def __repr__(self):
        return "<Packaging Job>"

    # ----------------------------------------------------------------------
    @property
    def status(self) -> str:
        """
        Get the GP status of the call.

        :return:
            A String
        """
        return self._future.done()

    # ----------------------------------------------------------------------
    def cancel(self) -> bool:
        """
        The ``cancel`` method attempts to cancel the job.

        .. note::
            If the call is currently being executed
            or finished running and cannot be cancelled then the method will
            return ``False``, otherwise the call will be cancelled and the method
            will return True.

        :return:
            A boolean indicating the call will be cancelled (True), or cannot be cancelled (False)
        """
        if self.done():
            return False
        if self.cancelled():
            return False
        return True

    # ----------------------------------------------------------------------
    def cancelled(self) -> bool:
        """
        The ``cancelled`` method retrieves whether the call was successfully cancelled.

        :return:
            A boolean indicating success (True), or failure (False)
        """
        return self._future.cancelled()

    # ----------------------------------------------------------------------
    def running(self) -> bool:
        """
        The ``running`` method retrieves whether the call is currently being executed and cannot be cancelled.

        :return:
            A boolean indicating success (True), or failure (False)
        """
        return self._future.running()

    # ----------------------------------------------------------------------
    def done(self) -> bool:
        """
        The ``done`` method retrieves whether the call was successfully cancelled or finished running.

        :return:
            A boolean indicating success (True), or failure (False)
        """
        return self._future.done()

    # ----------------------------------------------------------------------
    def result(self) -> Any:
        """
        The ``result`` method retrieves the value returned by the call.

        .. note::
            If the call hasn't yet completed then this method will wait.

        :return:
            An Object
        """
        if self.cancelled():
            return None
        return self._future.result()


########################################################################
class OfflineMapAreaManager(object):
    """
    The ``OfflineMapAreaManager`` is a helper class to manage offline map areas
    for a Web Map :class:`~arcgis.gis.Item`. Objects of this class should not
    be initialized directly, but rather accessed using the
    :attr:`~arcgis.map.Map.offline_areas` property on a
    :class:`~arcgis.map.Map` object.

    .. code-block:: python

        >>> from arcgis.gis import GIS
        >>> from arcgis.map import Map

        >>> gis = GIS(profile="your_Web_GIS_profile")

        >>> wm_item = gis.content.get("<web map id>")
        >>> wm_obj = Map(wm_item)

        >>> oma_mgr = wm_obj.offline_areas
        <arcgis.map.OfflineMapAreaManager at <memory_addr>>

    .. note::
        There are important concepts to understand about offline mapping before
        the properties and methods of this class will function properly. Both
        reference basemap and operational layers contained in a *Web Map* must
        be configured very specifically before they can be taken offline. See the
        documentation below for full details:

        * `ArcGIS Enterprise <https://enterprise.arcgis.com/en/portal/latest/use/take-maps-offline.htm>`_

          * `Basemap Considerations for ArcGIS Enterprise <https://enterprise.arcgis.com/en/portal/11.2/use/take-maps-offline.htm#ESRI_SECTION2_384E9B7E99EC4460810B947DE70FB2DA>`_

        * `ArcGIS Online <https://doc.arcgis.com/en/arcgis-online/manage-data/take-maps-offline.htm>`_
    """

    _pm = None
    _gis = None
    _tbx = None
    _item = None
    _portal = None
    _web_map = None

    # ----------------------------------------------------------------------
    def __init__(self, item: _gis_mod.Item, gis: _gis_mod.GIS):
        self._gis = gis
        self._portal = gis._portal
        self._item = item
        self._map = arcgismapping.Map(self._item)
        try:
            self._url = self._gis.properties.helperServices.packaging.url
            self._pm = self._gis._tools.packaging

        except Exception:
            warn("GIS does not support creating packages for offline usage")

    # ----------------------------------------------------------------------
    def _get_offline_value(self, source_dict, keys, lookup_dict=None):
        """
        Helper to fetch a value from a nested dictionary, applying a lookup dictionary if provided.
        """
        value = source_dict
        for key in keys:
            value = value.get(key, None)
            if value is None:
                return None
        return lookup_dict.get(value, value) if lookup_dict else value

    # ----------------------------------------------------------------------
    @property
    def offline_properties(self) -> dict:
        """
        This property allows users to configure the offline properties
        for a webmap.  The `offline_properties` allows for defining
        how available offline editing, basemap, and read-only layers
        behave in the web map application. For further reading about concepts
        for working with web maps offline, see
        `Configure the map to work offline <https://doc.arcgis.com/en/field-maps/latest/prepare-maps/configure-the-map.htm#ESRI_SECTION1_1822CD8DD1E74F08BC4308E03A5677F1>`_.
        Also, see the *applicationProperties* object in the
        `Web Map specification <https://developers.arcgis.com/web-map-specification/objects/applicationProperties>`_.

        ==================     ====================================================================
        **Parameter**          **Description**
        ------------------     --------------------------------------------------------------------
        values                 Required Dict.  The key/value pairs that define the offline
                               application properties.
        ==================     ====================================================================

        The dictionary supports the following keys:

        ==================     ====================================================================
        **Key**                **Values**
        ------------------     --------------------------------------------------------------------
        download               Optional string. Possible values:

                               - *None*
                               - *features*
                               - *features_and_attachments*

                               When editing layers, the edits are always sent to the server. This
                               string argument indicates which data is retrieved from the server.

                               * If argument is *None* - only the schema is written since neither
                                 features nor attachments are retrieved
                               * If argument is *features* - a full sync without downloading
                                 attachments occurs
                               * If argument is *features_and_attachments*, which is the Default -
                                 both features and attachments are retrieved
        ------------------     --------------------------------------------------------------------
        sync                   `sync` applies to editing layers only.  This string value indicates
                               how the data is synced:

                               * ``sync_features_and_attachments``  - bidirectional sync
                               * ``sync_features_upload_attachments`` - bidirectional sync for
                                 features but upload only for attachments
                               * ``upload_features_and_attachments`` - upload only for both features
                                 and attachments (initial replica is just a schema)
        ------------------     --------------------------------------------------------------------
        reference_basemap      The filename of a basemap that has been copied to a mobile device.
                               This can be used instead of the default basemap for the map to
                               reduce downloads.
        ------------------     --------------------------------------------------------------------
        get_attachments        Boolean value that indicates whether to include attachments with the
                               read-only data.
        ==================     ====================================================================

        :return: Dictionary

        .. code-block:: python

            # USAGE EXAMPLE

            >>> from arcgis.gis import GIS
            >>> from arcgis.map import Map

            >>> wm_item = gis.content.get("<web_map_id>")
            >>> wm_obj = Map(wm_item)

            >>> offline_mgr = wm_obj.offline_areas
            >>> offline_mgr.offline_properties = {"download": "features",
                                                  "sync": "sync_features_upload_attachments"}

        """
        dl_lu = {
            "features": "features",
            "featuresAndAttachments": "features_and_attachments",
            "features_and_attachments": "featuresAndAttachments",
            "none": None,
            "None": "none",
            None: "none",
            "syncFeaturesAndAttachments": "sync_features_and_attachments",
            "sync_features_and_attachments": "syncFeaturesAndAttachments",
            "syncFeaturesUploadAttachments": "sync_features_upload_attachments",
            "sync_features_upload_attachments": "syncFeaturesUploadAttachments",
            "uploadFeaturesAndAttachments": "upload_features_and_attachments",
            "upload_features_and_attachments": "uploadFeaturesAndAttachments",
        }

        # Populate values
        app_props = self._map._webmap.get("application_properties", {})
        offline_dict = app_props.get("offline", {}).dict() if app_props else {}

        values = {
            "download": self._get_offline_value(
                offline_dict, ["editableLayers", "download"], dl_lu
            ),
            "sync": self._get_offline_value(
                offline_dict, ["editableLayers", "sync"], dl_lu
            ),
            "reference_basemap": self._get_offline_value(
                offline_dict, ["offlinebasemap", "referenceBasemapName"]
            ),
            "get_attachments": self._get_offline_value(
                offline_dict, ["readonlyLayers", "downloadAttachments"]
            ),
        }

        return {k: v for k, v in values.items() if v is not None}

    # ----------------------------------------------------------------------
    @offline_properties.setter
    def offline_properties(self, values: dict[str, Any]):
        """
        See main `offline_properties` property docstring.
        """
        dl_lu = {
            "features": "features",
            "featuresAndAttachments": "features_and_attachments",
            "features_and_attachments": "featuresAndAttachments",
            "none": None,
            "None": "none",
            None: "none",
            "syncFeaturesAndAttachments": "sync_features_and_attachments",
            "sync_features_and_attachments": "syncFeaturesAndAttachments",
            "syncFeaturesUploadAttachments": "sync_features_upload_attachments",
            "sync_features_upload_attachments": "syncFeaturesUploadAttachments",
            "uploadFeaturesAndAttachments": "upload_features_and_attachments",
            "upload_features_and_attachments": "uploadFeaturesAndAttachments",
        }

        keys = {
            "download": "download",
            "sync": "sync",
            "reference_basemap": "referenceBasemapName",
            "get_attachments": "downloadAttachments",
        }

        # Initialize application properties
        if "application_properties" not in self._map._webmap:
            self._map._webmap.application_properties = {"offline": {}}

        v = self._map._webmap.application_properties
        offline = v.get("offline", {})

        # Define the offline structure dynamically
        editable_layers = {}
        readonly_layers = {}
        offline_basemap = {}

        if "download" in values:
            editable_layers["download"] = dl_lu.get(
                values["download"], values["download"]
            )
        if "sync" in values:
            editable_layers["sync"] = dl_lu.get(values["sync"], values["sync"])
        if "reference_basemap" in values:
            offline_basemap["referenceBasemapName"] = values["reference_basemap"]
        if "get_attachments" in values:
            readonly_layers["downloadAttachments"] = values["get_attachments"]

        if editable_layers:
            offline["editableLayers"] = editable_layers
        if offline_basemap:
            offline["offlinebasemap"] = offline_basemap
        if readonly_layers:
            offline["readonlyLayers"] = readonly_layers

        # Update the offline properties
        v["offline"] = offline

        update_items = {
            "clearEmptyFields": True,
            "text": json.dumps(self._map._webmap.dict()),
        }

        if self._item.update(item_properties=update_items):
            self._item._hydrated = False
            self._item._hydrate()
            self._map = arcgismapping.Map(self._item)
        else:
            raise Exception("Could not update the offline properties.")

    # ----------------------------------------------------------------------
    def _run_async(self, fn, **inputs):
        """runs the inputs asynchronously"""
        import concurrent.futures

        tp = concurrent.futures.ThreadPoolExecutor(1)
        try:
            future = tp.submit(fn=fn, **inputs)
        except:
            future = tp.submit(fn, **inputs)
        tp.shutdown(False)
        return future

    # ----------------------------------------------------------------------
    def create(
        self,
        area: str | list | dict[str, Any],
        item_properties: dict[str, Any] | None = None,
        folder: str | None = None,
        min_scale: int | None = None,
        max_scale: int | None = None,
        layers_to_ignore: list[str] | None = None,
        refresh_schedule: str = "Never",
        refresh_rates: dict[str, int] | None = None,
        enable_updates: bool = False,
        ignore_layers: list[str] | None = None,
        tile_services: list[dict[str, str]] | None = None,
        future: bool = False,
    ) -> _gis_mod.Item | PackagingJob:
        """
        This method creates offline map area items and packages for ArcGIS
        Runtime powered applications to use. The method creates two different
        types of :class:`Items <arcgis.gis.Item>`

        * ``Map Area`` items for the specified extent, bookmark, or polygon
        * ``Map Area Packages`` corresponding to the operational layer(s) and
          basemap layer(s) within the extent, bookmark or polygon area

        .. note::
            Packaging will fail if the size of the offline map area, when
            packaged, is **larger than 4 GB**.

            * If packaging fails, try using a smaller bookmark, extent or
              geometry for the *area* argument.
            * If the map contains feature layers that have attachments, you can
              exclude attachments from the offline package to decrease the
              package size.
            * If the map includes tile layers, use the *tile_services* argument
              to constrain the number of levels included in the resulting
              packages. This is typically *required* to reduce the tile package
              size for the basemap layer(s) in ArcGIS Enterprise.

        .. note::
            Only the owner of the Web Map item can create offline map areas.

        ==================     ====================================================================
        **Parameter**          **Description**
        ------------------     --------------------------------------------------------------------
        area                   Required *bookmark*, *extent*, or :class:`~arcgis.geometry.Polygon`
                               object. Specify as either:

                               + bookmark name

                                 .. code-block:: python

                                    >>> wm_item = gis.content.get("<web map id>")
                                    >>> wm_obj = Map(wm_item)

                                    >>> wm_bookmarks = wm_obj.bookmarks
                                    >>> area = wm_bookmarks[0]

                               + extent: as a list of coordinate pairs:

                                 .. code-block:: python

                                    >>> area = [['xmin', 'ymin'], ['xmax', 'ymax']]

                               + extent: as a dictionary:

                                 .. code-block:: python

                                    >>> area = {
                                                'xmin': <value>,
                                                'ymin': <value>,
                                                'xmax': <value>,
                                                'ymax': <value>,
                                                'spatialReference' : {'wkid' : <value>}
                                               }

                               + polygon: as a :class:`~arcgis.gis.Polygon` object


                               .. note::
                                    If spatial reference is not specified,
                                    it is assumed {'wkid': 4326}. Make sure this is the same as the
                                    spatial reference of the web map, otherwise the creation will fail.

        ------------------     --------------------------------------------------------------------
        item_properties        Required dictionary. See table below for the keys and values.
        ------------------     --------------------------------------------------------------------
        folder                 Optional string. Specify a folder name if you want the offline map
                               area item and the packages to be created inside a folder.

                               .. note::
                                   These items will not display when viewing the content folder in
                                   a web browser. They will display in the *Portal* tab of the
                                   Content Pane in ArcGIS Pro.
        ------------------     --------------------------------------------------------------------
        min_scale              Optional integer. Specify the minimum scale to cache tile and vector
                               tile layers. When zoomed out beyond this scale, cached layers would
                               not display.

                               .. note::
                                   The ``min_scale`` value is always larger than the ``max_scale``.
        ------------------     --------------------------------------------------------------------
        max_scale              Optional integer. Specify the maximum scale to cache tile and vector
                               tile layers. When zoomed in beyond this scale, cached layers would
                               not display.
        ------------------     --------------------------------------------------------------------
        layers_to_ignore       Optional List of layer objects to exclude when creating offline
                               packages. You can get the list of layers in a web map by calling
                               the `layers` property on the `MapContent` object.

                               .. python::
                                   layers = map.content.layers
                                   layers_to_ignore = [layers[0], layers[1]]
        ------------------     --------------------------------------------------------------------
        refresh_schedule       Optional string. Allows for the scheduling of refreshes at given
                               times.

                               The following are valid variables:

                               + ``Never`` - never refreshes the offline package (default)
                               + ``Daily`` - refreshes everyday
                               + ``Weekly`` - refreshes once a week
                               + ``Monthly`` - refreshes once a month

        ------------------     --------------------------------------------------------------------
        refresh_rates          Optional dict. This parameter allows for the customization of the
                               scheduler.  The dictionary accepts the following:

                               .. code-block:: python

                                   {
                                    "hour" : 1
                                    "minute" = 0
                                    "nthday" = 3
                                    "day_of_week" = 0
                                   }

                               - hour - a value between 0-23 (integers)
                               - minute - a value between 0-60 (integers)
                               - nthday - this is used for monthly only. Thw refresh will occur
                                 on the 'n' day of the month.
                               - day_of_week - a value between 0-6 where 0 is Sunday and 6 is
                                 Saturday.

                               .. code-block:: python

                                   # Example **Daily**: every day at 10:30 AM UTC

                                    >>> refresh_rates = {
                                                         "hour": 10,
                                                         "minute" : 30
                                                        }

                                   # Example **Weekly**: every Wednesday at 11:59 PM UTC

                                    >>> refresh_rates = {
                                                         "hour" : 23,
                                                         "minute" : 59,
                                                         "day_of_week" : 4
                                                        }
        ------------------     --------------------------------------------------------------------
        enable_updates         Optional Boolean.  Allows for the updating of the layers.
        ------------------     --------------------------------------------------------------------
        ignore_layers          Optional List.  A list of individual layers, specified with their
                               service URLs, in the map to ignore. The task generates packages for
                               all map layers by default.

                               .. note ::
                                    Deprecated, use `layers_to_ignore` instead.
        ------------------     --------------------------------------------------------------------
        tile_services          Optional List. An list of Python dictionary objects that contains
                               information about the *export tiles-enabled* services for which
                               tile packages (.tpk or .vtpk) need to be created. Each tile service
                               is specified with its *url* and desired level of details.

                               .. code-block:: python

                                   >>> tile_services = [
                                                        {
                                                         "url": "https://tiledbasemaps.arcgis.com/arcgis/rest/services/World_Imagery/MapServer",
                                                         "levels": "17,18,19"
                                                        }

                               .. note::
                                   This argument **should** be specified when using ArcGIS
                                   Enterprise items. The number of levels included greatly
                                   impacts the overall size of the resulting packages to
                                   keep them under the 2.5 GB limit.
        ------------------     --------------------------------------------------------------------
        future                 Optional boolean. If *True*, a future object will be returned and the
                               process will return control to the user before the task completes.
                               If *False*, control returns once the operation completes. The default
                               is *False*.
        ==================     ====================================================================

        Key:Value Dictionary options for argument ``item_properties``

        =================  =====================================================================
        **Key**            **Value**
        -----------------  ---------------------------------------------------------------------
        description        Optional string. Description of the item.
        -----------------  ---------------------------------------------------------------------
        title              Optional string. Name label of the item.
        -----------------  ---------------------------------------------------------------------
        tags               Optional string of comma-separated values, or a list of
                           strings for each tag.
        -----------------  ---------------------------------------------------------------------
        snippet            Optional string. Provide a short summary (limit to max 250
                           characters) of the what the item is.
        =================  =====================================================================

        :return:
            Map Area :class:`~arcgis.gis.Item`, or if *future=True*, a
            :class:`~arcgis.layers.PackagingJob` object to further query for
            results.

        .. code-block:: python

            # USAGE EXAMPLE #1: Creating offline map areas using *scale* argument

            >>> from arcgis.gis import GIS
            >>> from arcgis.map import Map

            >>> gis = GIS(profile="your_online_organization_profile")

            >>> wm_item = gis.content.get("<web_map_id>")
            >>> wm_obj = Map(wm_item)

            >>> item_prop = {"title": "Clear lake hyperspectral field campaign",
                             "snippet": "Offline package for field data collection using spectro-radiometer",
                             "tags": ["python api", "in-situ data", "field data collection"]}

            >>> aviris_layer = wm_item.content.layers[-1]

            >>> north_bed = wm_obj.bookmarks.list[-1].name
            >>> wm.offline_areas.create(area=north_bed,
                                        item_properties=item_prop,
                                        folder="clear_lake",
                                        min_scale=9000,
                                        max_scale=4500,
                                        layers_to_ignore=[aviris_layer])

            # USAGE Example #2: ArcGIS Enterprise web map specifying *tile_services*

            >>> gis = GIS(profile="your_enterprise_profile")

            >>> wm_item = gis.content.get("<item_id>")
            >>> wm_obj = Map(wm_item)

            # Enterprise: Get the url for tile services from basemap
            >>> basemap_lyrs = wm_obj.basemap.basemap["baseMapLayers"]
            >>> basemap_lyrs

                [
                 {'id': '18d9e5e151c-layer-2',
                  'title': 'Light_Gray_Export_AGOL_Group',
                  'itemId': '042f5e5aadcb8dbd910ae310b1f26d18',
                  'layerType': 'VectorTileLayer',
                  'styleUrl': 'https:/example.com/portal/sharing/servers/042f5e5aadcb8dbd910ae310b1f26d1/rest/services/World_Basemap_Export_v2/VectorTileServer/resources/styles/root.json'}
                ]

            # Get the specific Tile Layer item to see options for levels
            >>> vtl_item = gis.content.get(basemap_lyrs[0]["itemId"])
            >>> vtl_lyr = vtl_item.layers[0]
            >>> print(f"min levels: {vtl_lyr.properties['minLOD']}")
            >>> print(f"max levels: {vtl_lyr.properties['maxLOD']}")

                min levels: 0
                max levels: 16

            >>> vtl_svc_url = vtl_item.layers[0].url
            >>> vtl_svc_url
            https:/example.com/portal/sharing/servers/042f5e5aadcb8dbd910ae310b1f26d1/rest/services/World_Basemap_Export_v2/VectorTileServer

            # Get a list of bookmark names to iterate through
            >>> bookmarks = wm_obj.bookmarks.list()
            >>> bkmrk_names = [bookmark.name for bookmark in bookmarks]
            >>> bname = bkmrk_names[1]

            >>> oma = offline_mgr.create(area=bname,
                                         item_properties={"title": bname + "_OMA",
                                                          "tags": "offline_mapping,administrative boundaries,parks",
                                                          "snippet": bname + " in County",
                                                          "description": "Offline mapping area in " + bname + " for sync"},
                                         tile_services=[{"url": vtl_svc_url,
                                                         "levels": "6,7,8,9,10,11,12,13"}])
            >>> oma
            <Item title:"County_OMA" type:Map Area owner:gis_user>

            >>> # List packages created:
            >>> for oma_pkg in oma.related_items("Area2Package", "forward"):
            >>>     print(f"{oma_pkg.title:60}{oma_pkg.type}")

            <County_Layer-<id_string>                SQLite Geodatabase
            <VectorTileServe-<id_string>             Vector Tile Package

        .. note::
            This method executes silently. To view informative status messages, set the verbosity environment variable
            as shown below prior to running the method:

            .. code-block:: python

                # USAGE EXAMPLE: setting verbosity

                >>> from arcgis import env
                >>> env.verbose = True
        """
        # Check if user is the owner of the web map
        if self._item.owner != self._gis.users.me.username:
            raise Exception("Only the owner of the web map can create offline areas.")
        inputs = {
            "area": area,
            "item_properties": item_properties,
            "folder": folder,
            "min_scale": min_scale,
            "max_scale": max_scale,
            "layers_to_ignore": layers_to_ignore,
            "refresh_schedule": refresh_schedule,
            "refresh_rates": refresh_rates,
            "enable_updates": enable_updates,
            "tile_services": tile_services,
        }
        if future:
            future = self._run_async(self._create, **inputs)
            return PackagingJob(future=future)
        else:
            return self._create(**inputs)

    # ----------------------------------------------------------------------
    def _create(
        self,
        area,
        item_properties=None,
        folder=None,
        min_scale=None,
        max_scale=None,
        layers_to_ignore=None,
        refresh_schedule="Never",
        refresh_rates=None,
        enable_updates=False,
        tile_services=None,
        future=False,
    ):
        """
        See create method for docstring.
        """
        _dow_lu = {
            0: "SUN",
            1: "MON",
            2: "TUE",
            3: "WED",
            4: "THU",
            5: "FRI",
            6: "SAT",
            7: "SUN",
        }
        # region find if bookmarks or extent is specified
        _bookmark = None
        _extent = None
        if item_properties is None:
            item_properties = {}
        if isinstance(area, str):  # bookmark specified
            _bookmark = area
            area_type = "BOOKMARK"
        elif isinstance(area, (list, tuple)):  # extent specified as list
            _extent = {
                "xmin": area[0][0],
                "ymin": area[0][1],
                "xmax": area[1][0],
                "ymax": area[1][1],
                "spatialReference": {"wkid": 4326},
            }

        elif isinstance(area, dict) and "xmin" in area:  # geocoded extent provided
            _extent = area
            if _extent.get("spatialReference") is None:
                _extent["spatialReference"] = {"wkid": 4326}
        # endregion

        # region build input parameters - for CreateMapArea tool
        if folder:
            user_folders = self._gis.users.me.folders
            if user_folders:
                matching_folder_ids = [
                    f["id"] for f in user_folders if f["title"] == folder
                ]
                if matching_folder_ids:
                    folder_id = matching_folder_ids[0]
                else:  # said folder not found in user account
                    folder_id = None
            else:  # ignore the folder, output will be created in same folder as web map
                folder_id = None
        else:
            folder_id = None

        if "tags" in item_properties:
            if type(item_properties["tags"]) is list:
                tags = ",".join(item_properties["tags"])
            else:
                tags = item_properties["tags"]
        else:
            tags = None

        if refresh_schedule.lower() in ["daily", "weekly", "monthly"]:
            refresh_schedule = refresh_schedule.lower()
            if refresh_schedule == "daily":
                if refresh_rates and isinstance(refresh_rates, dict):
                    hour = 1
                    minute = 0
                    if "hour" in refresh_rates:
                        hour = refresh_rates["hour"]
                    if "minute" in refresh_rates:
                        minute = refresh_rates["minute"]
                    map_area_refresh_params = {
                        "startDate": int(
                            _dt.datetime.now(tz=_dt.timezone.utc).timestamp()
                        )
                        * 1000,
                        "type": "daily",
                        "nthDay": 1,
                        "dayOfWeek": 0,
                    }
                    refresh_schedule = "0 {m} {hour} * * ?".format(m=minute, hour=hour)
                else:
                    map_area_refresh_params = {
                        "startDate": int(
                            _dt.datetime.now(tz=_dt.timezone.utc).timestamp()
                        )
                        * 1000,
                        "type": "daily",
                        "nthDay": 1,
                        "dayOfWeek": 0,
                    }
                    refresh_schedule = "0 0 1 * * ?"
            elif refresh_schedule == "weekly":
                if refresh_rates and isinstance(refresh_rates, dict):
                    hour = 1
                    minute = 0
                    dayOfWeek = "MON"
                    if "hour" in refresh_rates:
                        hour = refresh_rates["hour"]
                    if "minute" in refresh_rates:
                        minute = refresh_rates["minute"]
                    if "day_of_week" in refresh_rates:
                        dayOfWeek = refresh_rates["day_of_week"]
                    map_area_refresh_params = {
                        "startDate": int(
                            _dt.datetime.now(tz=_dt.timezone.utc).timestamp()
                        )
                        * 1000,
                        "type": "weekly",
                        "nthDay": 1,
                        "dayOfWeek": dayOfWeek,
                    }
                    refresh_schedule = "0 {m} {hour} ? * {dow}".format(
                        m=minute, hour=hour, dow=_dow_lu[dayOfWeek]
                    )
                else:
                    map_area_refresh_params = {
                        "startDate": int(
                            _dt.datetime.now(tz=_dt.timezone.utc).timestamp()
                        )
                        * 1000,
                        "type": "weekly",
                        "nthDay": 1,
                        "dayOfWeek": 1,
                    }
                    refresh_schedule = "0 0 1 ? * MON"
            elif refresh_schedule == "monthly":
                if refresh_rates and isinstance(refresh_rates, dict):
                    hour = 1
                    minute = 0
                    nthday = 3
                    dayOfWeek = 0
                    if "hour" in refresh_rates:
                        hour = refresh_rates["hour"]
                    if "minute" in refresh_rates:
                        minute = refresh_rates["minute"]
                    if "nthday" in refresh_rates:
                        nthday = refresh_rates["nthday"]
                    if "day_of_week" in refresh_rates:
                        dayOfWeek = refresh_rates["day_of_week"]
                    map_area_refresh_params = {
                        "startDate": int(
                            _dt.datetime.now(tz=_dt.timezone.utc).timestamp()
                        )
                        * 1000,
                        "type": "monthly",
                        "nthDay": nthday,
                        "dayOfWeek": dayOfWeek,
                    }
                    refresh_schedule = "0 {m} {hour} ? * {nthday}#{dow}".format(
                        m=minute, hour=hour, nthday=nthday, dow=dayOfWeek
                    )
                else:
                    map_area_refresh_params = {
                        "startDate": int(
                            _dt.datetime.now(tz=_dt.timezone.utc).timestamp()
                        )
                        * 1000,
                        "type": "monthly",
                        "nthDay": 3,
                        "dayOfWeek": 3,
                    }
                    refresh_schedule = "0 0 14 ? * 4#3"
        else:
            refresh_schedule = None
            map_area_refresh_params = {"type": "never"}

        output_name = {
            "title": (item_properties["title"] if "title" in item_properties else None),
            "snippet": (
                item_properties["snippet"] if "snippet" in item_properties else None
            ),
            "description": (
                item_properties["description"]
                if "description" in item_properties
                else None
            ),
            "tags": tags,
            "folderId": folder_id,
            "packageRefreshSchedule": refresh_schedule,
        }

        # region call CreateMapArea tool
        pkg_tb = self._gis._tools.packaging
        if _extent:
            area = _extent
            area_type = "ENVELOPE"
        elif _bookmark:
            area = {"name": _bookmark}
            area_type = "BOOKMARK"

        if isinstance(area, str):
            area_type = "BOOKMARK"
        elif isinstance(area, _geometry.Polygon) or (
            isinstance(area, dict) and "rings" in area
        ):
            area_type = "POLYGON"
        elif isinstance(area, _geometry.Envelope) or (
            isinstance(area, dict) and "xmin" in area
        ):
            area_type = "ENVELOPE"
        elif isinstance(area, (list, tuple)):
            area_type = "ENVELOPE"
        if refresh_schedule is None:
            output_name.pop("packageRefreshSchedule")
        if folder_id is None:
            output_name.pop("folderId")
        oma_result = pkg_tb.create_map_area(
            map_item_id=self._item.id,
            area_type=area_type,
            area=area,
            output_name=output_name,
        )
        # endregion

        # Call update on Item with Refresh Information
        # import datetime
        item = _gis_mod.Item(gis=self._gis, itemid=oma_result)
        update_items = {
            "snippet": "Map with no advanced offline settings set (default is assumed to be features and attachments)",
            "title": (item_properties["title"] if "title" in item_properties else None),
            "typeKeywords": "Map, Map Area",
            "clearEmptyFields": True,
            "text": json.dumps(
                {
                    "mapAreas": {
                        "mapAreaTileScale": {
                            "minScale": min_scale,
                            "maxScale": max_scale,
                        },
                        "mapAreaRefreshParams": map_area_refresh_params,
                        "mapAreasScheduledUpdatesEnabled": enable_updates,
                    }
                }
            ),
        }
        item.update(item_properties=update_items)
        if _extent is None and area_type == "BOOKMARK":
            for bm in self._map._webmap_dict["bookmarks"]:
                if isinstance(area, dict):
                    if bm["name"].lower() == area["name"].lower():
                        _extent = bm["extent"]
                        break
                else:
                    if bm["name"].lower() == area.lower():
                        _extent = bm["extent"]
                        break
        update_items = {
            "properties": {
                "status": "processing",
                "packageRefreshSchedule": refresh_schedule,
            }
        }
        update_items["properties"].update(item.properties)
        if _extent and not "extent" in item.properties:
            update_items["properties"]["extent"] = _extent
        if area and not "area" in item.properties:
            update_items["properties"]["area"] = _extent
        item.update(item_properties=update_items)
        # End Item Update Refresh Call

        # region build input parameters - for setupMapArea tool
        # map layers to ignore parameter
        map_layers_to_ignore = []
        if isinstance(layers_to_ignore, list):
            for layer in layers_to_ignore:
                if isinstance(layer, str):
                    map_layers_to_ignore.append(layer)
                else:
                    # instance of layer class
                    map_layers_to_ignore.append(layer.url)
        elif isinstance(layers_to_ignore, str):
            map_layers_to_ignore.append(layers_to_ignore)

        # LOD parameter
        lods = []
        if min_scale or max_scale:
            # find tile and vector tile layers in map
            cached_layers = [
                self._map.content.layers[idx]
                for idx, l in enumerate(self._map._webmap.operational_layers)
                if l.layer_type in ["VectorTileLayer", "ArcGISTiledMapServiceLayer"]
            ]

            # find tile and vector tile layers in basemap set of layers
            if hasattr(self._map.basemap, "basemap"):
                if "baseMapLayers" in self._map.basemap.basemap:
                    cached_layers_bm = [
                        l
                        for l in self._map.basemap.basemap["baseMapLayers"]
                        if l["layerType"]
                        in ["VectorTileLayer", "ArcGISTiledMapServiceLayer"]
                    ]

                    # combine both the layer lists together
                    cached_layers.extend(cached_layers_bm)

            for cached_layer in cached_layers:
                if isinstance(
                    cached_layer,
                    (arcgis_layers.VectorTileLayer, arcgis_layers.MapImageLayer),
                ):
                    layer0_obj = cached_layer
                elif cached_layer.get("layerType") == "VectorTileLayer":
                    if "url" in cached_layer:
                        layer0_obj = arcgis_layers.VectorTileLayer(
                            cached_layer["url"], self._gis
                        )
                    elif "itemId" in cached_layer:
                        layer0_obj = arcgis_layers.VectorTileLayer.fromitem(
                            self._gis.content.get(cached_layer["itemId"])
                        )
                    elif "styleUrl" in cached_layer and cached_layer["title"] in [
                        "OpenStreetMap"
                    ]:
                        res = findall(
                            r"[0-9a-f]{8}(?:[0-9a-f]{4}){3}[0-9a-f]{12}",
                            cached_layer["styleUrl"],
                        )
                        if res:
                            layer0_obj = arcgis_layers.VectorTileLayer.fromitem(
                                self._gis.content.get(res[0])
                            )
                else:
                    layer0_obj = arcgis_layers.MapImageLayer(
                        cached_layer["url"], self._gis
                    )

                # region snap logic
                # Objective is to find the LoD that is close to the min scale specified. When scale falls between two
                # levels in the tiling scheme, we will pick the larger limit for min_scale and smaller limit for
                # max_scale.

                # Start by sorting the tileInfo dictionary. Then use Python's bisect_left to find the conservative tile
                # LOD that is closest to min scale. Do similar for max_scale.

                sorted_lods = sorted(
                    layer0_obj.properties.tileInfo.lods,
                    key=lambda x: x["scale"],
                )
                keys = [l["scale"] for l in sorted_lods]

                from bisect import bisect_left

                min_lod_info = sorted_lods[bisect_left(keys, min_scale)]
                max_lod_info = sorted_lods[
                    (
                        bisect_left(keys, max_scale) - 1
                        if bisect_left(keys, max_scale) > 0
                        else 0
                    )
                ]

                lod_span = [
                    str(i)
                    for i in range(min_lod_info["level"], max_lod_info["level"] + 1)
                ]
                lod_span_str = ",".join(lod_span)
                # endregion
                lods.append({"url": layer0_obj.url, "levels": lod_span_str})
            # endregion
        feature_services = None
        if enable_updates:
            if feature_services is None:
                feature_services = {}
                for l in self._map.content.layers:
                    if os.path.dirname(l["url"]) not in feature_services:
                        feature_services[os.path.dirname(l["url"])] = {
                            "url": os.path.dirname(l["url"]),
                            "layers": [int(os.path.basename(l["url"]))],
                            "createPkgDeltas": {"maxDeltaAge": 5},
                        }
                    else:
                        feature_services[os.path.dirname(l.url)]["layers"].append(
                            int(os.path.basename(l.url))
                        )
                feature_services = list(feature_services.values())
        # region call the SetupMapArea tool
        ts = tile_services if tile_services else lods
        setup_oma_result = pkg_tb.setup_map_area(
            map_area_item_id=oma_result,
            map_layers_to_ignore=map_layers_to_ignore,
            tile_services=ts,
            feature_services=feature_services,
            gis=self._gis,
            future=True,
        )
        if future:
            return setup_oma_result
        _log.info(str(setup_oma_result.result()))
        # endregion
        return _gis_mod.Item(gis=self._gis, itemid=oma_result)

    # ----------------------------------------------------------------------
    def modify_refresh_schedule(
        self,
        item: _gis_mod.Item,
        refresh_schedule: str | None = None,
        refresh_rates: dict[str, int] | None = None,
    ):
        """
        The ``modify_refresh_schedule`` method modifies an existing offline package's refresh schedule.

        ============================     ====================================================================
        **Parameter**                     **Description**
        ----------------------------     --------------------------------------------------------------------
        item                             Required :class:`~arcgis.gis.Item` object.
                                         This is the Offline Package to update the refresh schedule.
        ----------------------------     --------------------------------------------------------------------
        refresh_schedule                 Optional String.  This is the rate of refreshing.

                                         The following are valid variables:

                                         + Never - never refreshes the offline package (default)
                                         + Daily - refreshes everyday
                                         + Weekly - refreshes once a week
                                         + Monthly - refreshes once a month
        ----------------------------     --------------------------------------------------------------------
        refresh_rates                    Optional dict. This parameter allows for the customization of the
                                         scheduler. Note all time is in UTC.

                                         The dictionary accepts the following:

                                             {
                                             "hour" : 1
                                             "minute" = 0
                                             "nthday" = 3
                                             "day_of_week" = 0
                                             }

                                         - hour - a value between 0-23 (integers)
                                         - minute a value between 0-60 (integers)
                                         - nthday - this is used for monthly only. This say the refresh will occur on the 'x' day of the month.
                                         - day_of_week - a value between 0-6 where 0 is Sunday and 6 is Saturday.

                                         Example **Daily**:

                                             {
                                             "hour": 10,
                                             "minute" : 30
                                             }

                                         This means every day at 10:30 AM UTC

                                         Example **Weekly**:

                                             {
                                             "hour" : 23,
                                             "minute" : 59,
                                             "day_of_week" : 4
                                             }

                                         This means every Wednesday at 11:59 PM UTC

        ============================     ====================================================================

        :return:
            A boolean indicating success (True), or failure (False)


        .. code-block:: python

            ## Updates Offline Package Building Everyday at 10:30 AM UTC

            gis = GIS(profile='owner_profile')
            item = gis.content.get('9b93887c640a4c278765982aa2ec999c')
            oa = wm.offline_areas.modify_refresh_schedule(item.id, 'daily', {'hour' : 10, 'minute' : 30})


        """
        if isinstance(item, str):
            item = self._gis.content.get(item)
        _dow_lu = {
            0: "SUN",
            1: "MON",
            2: "TUE",
            3: "WED",
            4: "THU",
            5: "FRI",
            6: "SAT",
            7: "SUN",
        }
        hour = 1
        minute = 0
        nthday = 3
        dayOfWeek = 0
        if refresh_rates is None:
            refresh_rates = {}
        if refresh_schedule is None or str(refresh_schedule).lower() == "never":
            refresh_schedule = None
            map_area_refresh_params = {"type": "never"}
        elif refresh_schedule.lower() == "daily":
            if "hour" in refresh_rates:
                hour = refresh_rates["hour"]
            if "minute" in refresh_rates:
                minute = refresh_rates["minute"]
            map_area_refresh_params = {
                "startDate": int(_dt.datetime.now(tz=_dt.timezone.utc).timestamp())
                * 1000,
                "type": "daily",
                "nthDay": 1,
                "dayOfWeek": 0,
            }
            refresh_schedule = "0 {m} {hour} * * ?".format(m=minute, hour=hour)
        elif refresh_schedule.lower() == "weekly":
            if "hour" in refresh_rates:
                hour = refresh_rates["hour"]
            if "minute" in refresh_rates:
                minute = refresh_rates["minute"]
            if "day_of_week" in refresh_rates:
                dayOfWeek = refresh_rates["day_of_week"]
            map_area_refresh_params = {
                "startDate": int(_dt.datetime.now(tz=_dt.timezone.utc).timestamp())
                * 1000,
                "type": "weekly",
                "nthDay": 1,
                "dayOfWeek": dayOfWeek,
            }
            refresh_schedule = "0 {m} {hour} ? * {dow}".format(
                m=minute, hour=hour, dow=_dow_lu[dayOfWeek]
            )
        elif refresh_schedule.lower() == "monthly":
            if "hour" in refresh_rates:
                hour = refresh_rates["hour"]
            if "minute" in refresh_rates:
                minute = refresh_rates["minute"]
            if "nthday" in refresh_rates:
                nthday = refresh_rates["nthday"]
            if "day_of_week" in refresh_rates:
                dayOfWeek = refresh_rates["day_of_week"]
            map_area_refresh_params = {
                "startDate": int(_dt.datetime.now(tz=_dt.timezone.utc).timestamp())
                * 1000,
                "type": "monthly",
                "nthDay": nthday,
                "dayOfWeek": dayOfWeek,
            }
            refresh_schedule = "0 {m} {hour} ? * {nthday}#{dow}".format(
                m=minute, hour=hour, nthday=nthday, dow=dayOfWeek
            )
        else:
            raise ValueError(
                (
                    "Invalid refresh_schedule, value"
                    " can only be Never, Daily, Weekly or Monthly."
                )
            )
        text = item.get_data()
        text["mapAreas"]["mapAreaRefreshParams"] = map_area_refresh_params
        update_items = {"clearEmptyFields": True, "text": json.dumps(text)}
        item.update(item_properties=update_items)
        _extent = item.properties["extent"]
        update_items = {
            "properties": {
                "extent": _extent,
                "status": "complete",
                "packageRefreshSchedule": refresh_schedule,
            }
        }
        item.update(item_properties=update_items)
        try:
            self._pm.create_map_area(map_item_id=item.id, future=False)
            return True
        except:
            return False

    # ----------------------------------------------------------------------
    def list(self) -> list:
        """
        Retrieves a list of all *Map Area* items for the
        :class:`~arcgis.map.Map` object.

        .. note::
            *Map Area* items and the corresponding offline packages share a relationship
            of type *Area2Package*. You can use this relationship to get the list of
            package items cached for each *map area* item. Refer to the Python snippet
            below for the steps:

        .. code-block:: python

            # USAGE EXAMPLE: Listing Map Area Items

            >>> from arcgis.gis import GIS
            >>> from arcgis.map import Map

            >>> wm_item = gis.content.search("*", "Web Map")[0]
            >>> wm_obj = Map(wm_item)

            >>> all_map_areas = wm.offline_areas.list()
            >>> all_map_areas

            [<Item title:"Ballerup_OMA", type:Map Area owner:gis_user1>,
             <Item title:"Viborg_OMA", type:Map Area owner:gis_user1>]

            # USAGE Example: Inspecting Map Area packages

            >>> area1 = all_map_areas[0]
            >>> area1_packages = area1.related_items("Area2Package","forward")

            >>> for pkg in area1_packages:
            >>>     print(f"{pkg.title}")
            <<<     print(f"{' ' * 2}{pkg.type}")
            >>>     print(f"{' ' * 2}{pkg.homepage}")

            VectorTileServe-<value_string>
              Vector Tile Package
              https://<organziation_url>/home/item.html?id=<item_id>


            DK_lau_data-<value_string>
              SQLite Geodatabase
              https://organization_url/home/item.html?id=<item_id>

        :return:
            A List of *Map Area* :class`items <arcgis.gis.Item>` related to the
            *Web Map* item.
        """
        return self._item.related_items("Map2Area", "forward")

    # ----------------------------------------------------------------------
    def update(
        self,
        offline_map_area_items: list | None = None,
        future: bool = False,
    ) -> dict | PackagingJob | None:
        """
        The ``update`` method refreshes existing map area packages associated
        with each of the ``Map Area`` items specified. This process updates the
        packages with changes made on the source data since the last time those
        packages were created or refreshed. See `Refresh Map Area Package
        <https://developers.arcgis.com/rest/packaging/api-reference/refresh-map-area-package.htm>`_
        for more information.

        ============================     ====================================================================
        **Parameter**                     **Description**
        ----------------------------     --------------------------------------------------------------------
        offline_map_area_items           Optional list. Specify one or more Map Area
                                         :class:`items <arcgis.gis.Item>` for which the packages need to be
                                         refreshed. If not specified, this method updates all the packages
                                         associated with all the map area items of the web map.

                                         .. note::
                                             To get the list of ``Map Area`` items related to the *Map*
                                             object, call the
                                             :meth:`~arcgis.layers.OfflineMapAreaManager.list` method on
                                             the :class:`~arcgis.layers.OfflineMapAreaManager` for the
                                             *Map*.
        ----------------------------     --------------------------------------------------------------------
        future                           Optional Boolean.
        ============================     ====================================================================

        :return:
            Dictionary containing update status.

        .. note::
            This method executes silently. To view informative status messages,
            set the verbosity environment variable as shown below before running
            the code:

            .. code-block:: python

               USAGE EXAMPLE: setting verbosity

               from arcgis import env
               env.verbose = True
        """
        # find if 1 or a list of area items is provided
        if isinstance(offline_map_area_items, _gis_mod.Item):
            offline_map_area_items = [offline_map_area_items]
        elif isinstance(offline_map_area_items, str):
            offline_map_area_items = [offline_map_area_items]

        # get packages related to the offline area item
        _related_packages = []
        if not offline_map_area_items:  # none specified
            _related_oma_items = self.list()
            for (
                related_oma
            ) in _related_oma_items:  # get all offline packages for this web map
                _related_packages.extend(
                    related_oma.related_items("Area2Package", "forward")
                )

        else:
            for offline_map_area_item in offline_map_area_items:
                if isinstance(offline_map_area_item, _gis_mod.Item):
                    _related_packages.extend(
                        offline_map_area_item.related_items("Area2Package", "forward")
                    )
                elif isinstance(offline_map_area_item, str):
                    offline_map_area_item = _gis_mod.Item(
                        gis=self._gis, itemid=offline_map_area_item
                    )
                    _related_packages.extend(
                        offline_map_area_item.related_items("Area2Package", "forward")
                    )

        # update each of the packages
        if _related_packages:
            _update_list = [{"itemId": i.id} for i in _related_packages]
            job = self._pm.refresh_map_area_package(
                packages=json.dumps(_update_list), future=True, gis=self._gis
            )
            if future:
                return job
            return job.result()
        else:
            return None
