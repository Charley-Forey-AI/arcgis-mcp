from __future__ import annotations
import json
from arcgis.auth.tools import LazyLoader
import pathlib
import anywidget
from traitlets import (
    Unicode,
    Int,
    Dict,
    Bool,
    observe,
)
from typing import Optional, Any, Union
from arcgis.map._utils import _HelperMethods, refreshable_property

import warnings

from arcgis.map._basemap_styles_service import BasemapStyle, BasemapStylesService

# notebook 7 serialization warning given when json is null. This does not cause issue, suppress for users.
warnings.filterwarnings(
    "ignore", category=UserWarning, message="Message serialization failed"
)

arcgis = LazyLoader("arcgis")
basemapdef = LazyLoader("arcgis.map.definitions._basemap_definitions")
renderers = LazyLoader("arcgis.map.renderers")
rm = LazyLoader("arcgis.map.definitions._renderer_metaclass")
group = LazyLoader("arcgis.map.group_layer")
offline = LazyLoader("arcgis.map.offline_mapping")
_dt = LazyLoader("datetime")
features = LazyLoader("arcgis.features")
forms = LazyLoader("arcgis.map.forms")
geo = LazyLoader("arcgis.features.geo")
arcgis_layers = LazyLoader("arcgis.layers")
pd = LazyLoader("pandas")
popups = LazyLoader("arcgis.map.popups")
raster = LazyLoader("arcgis.raster")
realtime = LazyLoader("arcgis.realtime")
symbols = LazyLoader("arcgis.map.symbols")
smart_mapping = LazyLoader("arcgis.map.smart_mapping")
uuid = LazyLoader("uuid")
_gis_mod = LazyLoader("arcgis.gis")
_models = LazyLoader("arcgis.map.dataclasses.models")


class Map(anywidget.AnyWidget):
    _esm = pathlib.Path(__file__).parent / "static" / "map_widget.js"
    _css = pathlib.Path(__file__).parent / "static" / "map_widget.css"
    _portal_token = Unicode("").tag(sync=True)
    _auth_mode = Unicode("").tag(sync=True)
    _portal_rest_url = Unicode("").tag(sync=True)
    _proxy_rule = Dict({}).tag(sync=True)
    _username = Unicode("").tag(sync=True)
    _show_legend = Bool(False).tag(sync=True)
    _show_bookmarks = Bool(False).tag(sync=True)
    _show_layer_list = Bool(False).tag(sync=True)
    _webmap_dict = Dict({}).tag(sync=True)
    # Time Slider Portion
    _show_time_slider = Bool(False).tag(sync=True)
    _time_slider_layer = Int().tag(sync=True)
    _time_slider_full_extent = Dict({}).tag(sync=True)
    _time_slider_time_interval = Dict({}).tag(sync=True)
    # Smart Mapping
    _smart_mapping_renderer = Dict({}).tag(sync=True)
    _smart_mapping_params = Dict({}).tag(sync=True)
    # Local Raster
    _local_image_data = Dict({}).tag(sync=True)
    _raster_to_media_layer = Dict({}).tag(sync=True)  # only used by JS
    # Only for python code
    _linked_maps = []
    _helper_obj: _HelperMethods | None = None

    # CDN for Enterprise Only
    js_api_path = Unicode("").tag(sync=True)

    #####
    # Traitlets (properties) that can be set by user. These look a bit different in the documentation.
    # They act the same as if you put a property decorator over a method. They are linked to js side for widget use.
    theme = Unicode("light").tag(sync=True)
    """
    Get/Set the widget theme when displaying in a Jupyter environment.

    Values can be "light" or "dark"
    """
    _view_state = Dict(default_value={}).tag(sync=True)

    @observe("_view_state")
    def _on_view_state_change(self, change):
        # This method will be called whenever the 'view_state' trait changes
        # Check the Initial State of the webmap. If the target geometry in the viewpoint is different, update it.
        # Don't update the webmap dict since we are dealing with a traitlets change
        new_extent = (
            _models.Extent(**change["new"].get("extent"))
            if change["new"].get("extent")
            else {}
        )
        # First time code is executed there might not be an initial state, if not set to None for old extent.
        old_extent = (
            (
                self._webmap.initial_state.dict()
                .get("viewpoint", {})
                .get("targetGeometry")
            )
            if self._webmap.initial_state
            else None
        )

        # Compare the new extent from the view state with the old extent from the webmap's initial state.
        # If there is no change in extent, no update to the initial state is needed.
        if not new_extent or json.dumps(old_extent, sort_keys=True) == json.dumps(
            new_extent.dict(), sort_keys=True
        ):
            # No change in extent, do nothing
            return
        if self._webmap.initial_state:
            self._webmap.initial_state.viewpoint.target_geometry = new_extent
        else:
            self._webmap.initial_state = _models.MapInitialState(
                viewpoint=_models.MapViewpoint(target_geometry=new_extent)
            )

    @property
    def zoom(self):
        """
        Get/Set the level of zoom applied to the rendered Map.

        ===============     ====================================================================
        **Parameter**        **Description**
        ---------------     --------------------------------------------------------------------
        value               Required int.
                            .. note::
                                The higher the number, the more zoomed in you are.
        ===============     ====================================================================

        :return:
            Int value that represent the zoom level

        .. code-block:: python

            # Usage example
            map.zoom = 10

        """
        return self._view_state.get("zoom", 0)

    @zoom.setter
    def zoom(self, value):
        if not isinstance(value, int):
            raise ValueError("Zoom must be an integer.")
        # When a value gets changed that affects view state we create new object to trigger the widget
        self._view_state = {"zoom": value}

    @property
    def scale(self):
        """
        Get/Set the map scale at the center of the rendered Map. If set to X, the scale
        of the map would be 1:X.

        ===============     ====================================================================
        **Parameter**        **Description**
        ---------------     --------------------------------------------------------------------
        value               Required int.
        ===============     ====================================================================

        :return: The int value of the scale

        .. code-block:: python

            # Usage example: Sets the scale to 1:24000
            map.scale = 24000

        """
        return self._view_state.get("scale", -1)

    @scale.setter
    def scale(self, value):
        if not isinstance(value, int):
            raise ValueError("Scale must be an integer.")
        # When a value gets changed that affects view state we create new object to trigger the widget
        self._view_state = {"scale": value}

    @property
    def center(self):
        """
        Get/Set the center of the rendered Map.

        ==================     ====================================================================
        **Parameter**           **Description**
        ------------------     --------------------------------------------------------------------
        center                 A `[lat, long]` list that represents the JSON of the map
                                widget's center.
        ==================     ====================================================================

        :return: A list that represents the latitude and longitude of the map's center.

        .. code-block:: python

            # Usage example: Sets the center of the map to the given lat/long
            map.center = [34.05, -118.24]

        """
        return self._view_state.get("center", [0, 0])

    @center.setter
    def center(self, value):
        if not isinstance(value, (list, tuple)) or len(value) != 2:
            raise ValueError(
                "Center must be a list or tuple with two elements [lat, long]."
            )
        # When a value gets changed that affects view state we create new object to trigger the widget
        self._view_state = {"center": value}

    @property
    def extent(self):
        """
        Get/Set the map's extent.

        ==================     ====================================================================
        **Parameter**           **Description**
        ------------------     --------------------------------------------------------------------
        extent                 A dictionary that represents the JSON of the map widget's extent.
        ==================     ====================================================================

        :return: A dictionary representing the map's extent.

        .. code-block:: python

            # Usage example: Sets the extent of the map to the given extent
            map.extent = {
                "xmin": -124.35,
                "ymin": 32.54,
                "xmax": -114.31,
                "ymax": 41.95
                "spatialReference": { "wkid":102100 }
            }

        """
        return (
            self._view_state.get("extent")
            or self._webmap.initial_state.viewpoint.target_geometry.dict()
        )

    @extent.setter
    def extent(self, value):
        if value and "spatialReference" not in value:
            raise ValueError("Extent must have a spatial reference.")
        # When a value gets changed that affects view state we create new object to trigger the widget
        self._view_state = {"extent": value}

    @property
    def rotation(self):
        """
        Get/Set the rotation of the rendered Map.

        ===============     ====================================================================
        **Parameter**        **Description**
        ---------------     --------------------------------------------------------------------
        value               Required int. The rotation angle in degrees.
        ===============     ====================================================================

        :return: The int value of the rotation angle

        .. code-block:: python

            # Usage example: Sets the rotation angle of the map to 45 degrees
            map.rotate = 45

        """
        return self._view_state.get("rotation", 0)

    @rotation.setter
    def rotation(self, value):
        if not isinstance(value, (int, float)):
            raise ValueError("Rotation must be a number.")
        self._view_state = {"rotation": value}

    ##### end traitlets

    def __init__(
        self,
        location: str | None = None,
        *,
        item: _gis_mod.Item | str | None = None,
        gis: _gis_mod.GIS | None = None,
        **kwargs,
    ) -> None:
        """
        ==================      =====================================================================
        **Parameter**           **Description**
        ------------------      ---------------------------------------------------------------------
        location                Optional string. The address or place where the map is to be centered.
        ------------------      ---------------------------------------------------------------------
        item                    Optional item of type "Web Map" to initiate the Map with.
        ------------------      ---------------------------------------------------------------------
        gis                     Optional GIS. The GIS object to use for working with the map. If None provided,
                                the current active GIS will be used.
        ==================      =====================================================================
        """
        # Initialize
        super().__init__(**kwargs)

        if location and isinstance(location, _gis_mod.Item):
            # Fix if user doesn't specify item parameter
            item = location
            location = None

        # Set up GIS
        self._helper._setup_gis_properties(gis)

        # Set up the map that will be used
        # either existing or new instance
        self._setup_webmap_properties(item)

        # Assign the definition to helper class. This is the pydantic dataclass.
        self._helper._set_widget_definition(self._webmap)

        # Set up the widget (map that is rendered)
        # this sets extent, location, etc.
        geocoder = kwargs.pop("geocoder", None)

        # If the user gave an item and no location, we use the item's extent
        self._helper._setup_location_properties(location, geocoder)

        # Set up basemap and content managers
        self._content: MapContent = None
        self._basemap_manager: BasemapManager = None

    @property
    def _helper(self) -> _HelperMethods:
        """internal helper method class"""
        if self._helper_obj is None:
            self._helper_obj = _HelperMethods(self)
        return self._helper_obj

    @observe("_smart_mapping_renderer")
    def _renderer_changed(self, change):
        """
        Observe the smart mapping renderer and update the webmap definition.
        When changes are made on the JS side, such as all the smart mapping
        methods, they need to be communicated to the python side.

        This is asynchronous so, for example, if a user tries to save the Map
        in the same cell as their smart mapping method, the renderer will not
        be updated in time.
        """
        # Check if an error is returned from the JS side
        if "error" in change["new"]:
            raise ValueError(change["new"]["error"])

        # use the layer id in the smart mapping params and update the corresponding layer
        if self._smart_mapping_renderer:
            layer_id = self._smart_mapping_params["layerId"]
            self._update_renderer_key(layer_id, self._smart_mapping_renderer)
        else:
            raise Warning(
                "No renderer was returned from the smart mapping method. Nothing was updated."
            )

    @observe("_raster_to_media_layer")
    def _update_operational_layers(self, change):
        """
        Observe the operational layers change and update the webmap definition.
        """
        if "error" in change["new"]:
            raise ValueError(change["new"]["error"])

        if self._raster_to_media_layer:
            try:
                media_layer = _models.MediaLayer(**self._raster_to_media_layer)
                self._webmap.operational_layers.append(media_layer)
            except:
                raise Warning(
                    "Unable to create the layer properly. Saving the map may result in an error or missing layer."
                )

    def _update_renderer_key(self, target_layer_id: str, new_renderer: str):
        """
        Recursive method that finds the layer by id. Each layer has a unique id since the same layer can
        be added multiple times. This method is recursive since the layer could be in a Group Layer.
        """

        def recursive_update(layers, target_layer_id, new_renderer):
            for layer in layers:
                if (
                    not isinstance(layer, group.GroupLayer)
                    and layer.id == target_layer_id
                ):
                    if hasattr(layer, "featureCollection"):
                        # This gets updated in the layer definition of the feature collection layers
                        layer.feature_collection.layers[
                            0
                        ].layer_definition.drawing_info.renderer = new_renderer
                    else:
                        # This gets updated in the layer definition key of the layer
                        layer.layer_definition.drawing_info.renderer = new_renderer
                    return True
                # check if the layer is a group layer and if so, recursively update
                elif isinstance(layer, group.GroupLayer):
                    if recursive_update(layer.layers):
                        return True
            return False

        # Search for layer to update
        if not recursive_update(
            self._webmap.operational_layers, target_layer_id, new_renderer
        ):
            print(f"Layer with ID {target_layer_id} not found in the webmap.")

    def _setup_webmap_properties(self, item):
        """
        Set up the webmap property to be used. This can either be from an
        existing 'Web Map' item or it can be a new map. A pydantic Webmap instance
        will be created in either case and this is what we will use to make edits and save.
        """
        # Set up the webmap dictionary
        if item:
            # Existing Map was passed in
            if isinstance(item, str):
                # Get Item from itemid
                item: _gis_mod.Item = self._gis.content.get(item)
                if item is None:
                    # No item found with associated gis
                    raise ValueError("No item was found corresponding to this item id.")
            if item.type.lower() != "web map":
                # Has to be a pre-existing item of type webmap
                raise TypeError("Item must be of type Map or Web Map.")

            # Keep track of the item
            self.item: _gis_mod.Item = item
            # Set up webmap data
            data: dict = self.item.get_data()
            if "version" in data:
                del data["version"]  # webmap spec will update this

            # make sure spatial reference is set
            if "spatialReference" not in data:
                data["spatialReference"] = _models.SpatialReference(
                    latestWkid=3857, wkid=102100
                )

            # Fix the basemap data
            data = self._fix_basemap_data(data)

            # Use pydantic dataclass from webmap_spec
            self._webmap = _models.Webmap(**data)

            # For bookmarks class
            if self._webmap.bookmarks is None:
                self._webmap.bookmarks = []
        else:
            # New Map
            self.item = None
            # Set default spatial reference
            spatial_reference = _models.SpatialReference(latestWkid=3857, wkid=102100)

            ## Basemap Section
            # Default basemap to use
            default_basemap_group = (
                "defaultVectorBasemap"
                if getattr(self._gis.properties, "useVectorBasemaps", False)
                else "defaultBasemap"
            )
            if (
                self._gis._is_authenticated
                and self._gis.properties[default_basemap_group]
            ):
                # Find the org's default basemap.
                for idx, dbmap in enumerate(
                    self._gis.properties[default_basemap_group]["baseMapLayers"]
                ):
                    # Account for Enterprise basemap not having title
                    if "title" not in dbmap:
                        self._gis.properties[default_basemap_group]["baseMapLayers"][
                            idx
                        ]["title"] = str(uuid.uuid4())[0:7]
                # Create pydantic dataclass with basemap
                basemap = _models.MapBasemap(
                    **self._gis.properties[default_basemap_group]
                )
            else:
                # Create pydantic dataclass with generic default
                basemap = _models.MapBasemap(
                    baseMapLayers=basemapdef.basemap_dict["topo-vector"],
                    title="Topographic",
                )

            # New Webmap from pydantic dataclass from webmap spec generation
            self._webmap = _models.Webmap(
                operationalLayers=[],
                baseMap=basemap,
                spatialReference=spatial_reference,
                authoringApp="ArcGISPythonAPI",
                authoringAppVersion=str(arcgis.__version__),
                bookmarks=[],
                initialState={
                    "viewpoint": {"rotation": 0, "targetGeometry": {}}
                },  # gets fixed later in _setup_location_properties
            )
        self._update_source()

    def _fix_basemap_data(self, data):
        """
        Fix the basemap data to have the correct data for the webmap spec.
        """
        # This is a downfall on the webmap spec side. Basemaps created
        # in the JS API a while back do not always have a title. This is a workaround to fix that.
        if "baseMap" in data:
            data["baseMap"]["title"] = data["baseMap"].get(
                "title", uuid.uuid4().hex[:7]
            )
            if "baseMapLayers" in data["baseMap"]:
                for layer in data["baseMap"]["baseMapLayers"]:
                    if "title" not in layer:
                        layer["title"] = data["baseMap"]["title"]
        return data

    def _repr_mimebundle_(self, include=None, exclude=None, **kwargs):
        # Refresh dict incase changes made before rendering
        self._update_source()
        # Render the widget
        return super(Map, self)._repr_mimebundle_(
            include=include, exclude=exclude, **kwargs
        )

    def __add__(self, item):
        """
        Ability to add a layer to the map by using the '+' operator.
        """
        if item:
            self.content.add(item)
        else:
            return None

    def __sub__(self, index):
        """ "
        Ability to remove a layer by index using the '-' operator.
        Cannot be used to remove tables.
        """
        if isinstance(index, int):
            self.content.remove(index=index)

    def _update_source(self):
        self._webmap_dict = self._webmap.dict()

    def js_requirement(self):
        """
        Return the JS API version needed to work with the current version of the mapping module in a disconnected environment.

        :return: A string representing the JS API version.
        """
        return self._helper._js_requirement()

    @property
    def content(self) -> MapContent:
        """
        Returns a MapContent object that can be used to access the layers and tables
        in the map. This is useful for adding, updating, getting, and removing content
        from the Map.
        """
        if self._content is None:
            self._content = MapContent(self)
        return self._content

    @property
    def basemap(self) -> BasemapManager:
        """
        Returns a BasemapManager object that can be used to handle
        basemap related properties and methods on the Map.
        """
        if self._basemap_manager is None:
            self._basemap_manager = BasemapManager(self)
        return self._basemap_manager

    @property
    def bookmarks(self) -> Bookmarks:
        """
        Get an instance of the Bookmarks that can be seen in your Map. This class
        can be used to add and remove bookmarks as well as edit them. You can also
        enable and disable the bookmark widget on the rendered map in Jupyter Lab.
        """
        return Bookmarks(self)

    @property
    def legend(self) -> Legend:
        """
        Get an instance of the Legend class. You can use this class to enable or disable
        the legend widget. Best used inside of Jupyter Lab.
        """
        return Legend(self)

    @property
    def layer_list(self) -> LayerList:
        """
        Get an instance of the LayerList class. You can use this class to enable or disable
        the layer list widget. Best used inside of Jupyter Lab.
        """
        return LayerList(self)

    @property
    def time_slider(self) -> TimeSlider:
        """
        Get an instance of the TimeSlider class. You can use this class to enable or disable the
        time slider on the widget. Best used inside of Jupyter Lab
        """
        return TimeSlider(self)

    @property
    def offline_areas(self) -> offline.OfflineMapAreaManager | None:
        """
        The ``offline_areas`` property is the resource manager for offline areas cached for the ``WebMap`` object.

        :return:
            The :class:`~arcgis.map.OfflineMapAreaManager` for the ``WebMap`` object.
        """
        if self.item is None:
            return None
        else:
            return offline.OfflineMapAreaManager(self.item, self._gis)

    def save(
        self,
        item_properties: dict[str, Any],
        thumbnail: Optional[str] = None,
        metadata: Optional[str] = None,
        owner: Optional[str] = None,
        folder: Optional[str] = None,
    ) -> _gis_mod.Item:
        """
        Saves the ``Map`` object as a new Web Map Item in your :class:`~arcgis.gis.GIS`.

        .. note::
            If you started with a ``Map`` object from an existing web map item,
            calling this method will create a new item with your changes. If you want to
            update the existing ``Map`` item found in your portal with your changes, call the
            `update` method instead.

        ===============     ====================================================================
        **Parameter**        **Description**
        ---------------     --------------------------------------------------------------------
        item_properties     Required dictionary. See table below for the keys and values.
                            The three required keys are: 'title', 'tag', 'snippet'.
        ---------------     --------------------------------------------------------------------
        thumbnail           Optional string. Either a path or URL to a thumbnail image.
        ---------------     --------------------------------------------------------------------
        metadata            Optional string. Either a path or URL to the metadata.
        ---------------     --------------------------------------------------------------------
        owner               Optional string. Defaults to the logged in user.
        ---------------     --------------------------------------------------------------------
        folder              Optional string. Name of the folder into which the web map should be
                            saved.
        ===============     ====================================================================

        *Key:Value Dictionary Options for Argument item_properties*

        =================  =====================================================================
        **Key**            **Value**
        -----------------  ---------------------------------------------------------------------
        title              Required string. Name label of the item.
        -----------------  ---------------------------------------------------------------------
        tags               Required string. Tags listed as comma-separated values, or a list of strings.
                           Used for searches on items.
        -----------------  ---------------------------------------------------------------------
        snippet            Required string. Provide a short summary (limit to max 250 characters) of the what the item is.
        -----------------  ---------------------------------------------------------------------
        typeKeywords       Optional string. Provide a lists all subtypes, see URL 1 below for valid values.
        -----------------  ---------------------------------------------------------------------
        description        Optional string. Description of the item.
        -----------------  ---------------------------------------------------------------------
        extent             Optional dict. The extent of the item.
        -----------------  ---------------------------------------------------------------------
        accessInformation  Optional string. Information on the source of the content.
        -----------------  ---------------------------------------------------------------------
        licenseInfo        Optional string.  Any license information or restrictions regarding the content.
        -----------------  ---------------------------------------------------------------------
        culture            Optional string. Locale, country and language information.
        -----------------  ---------------------------------------------------------------------
        access             Optional string. Valid values are private, shared, org, or public.
        -----------------  ---------------------------------------------------------------------
        commentsEnabled    Optional boolean. Default is true, controls whether comments are allowed (true)
                           or not allowed (false).
        -----------------  ---------------------------------------------------------------------
        culture            Optional string. Language and country information.
        =================  =====================================================================

        The above are the most common item properties (metadata) that you set. To get a complete list, see
        the
        `Common Parameters <https://developers.arcgis.com/rest/users-groups-and-items/common-parameters.htm#ESRI_SECTION1_1FFBA7FE775B4BDA8D97524A6B9F7C98>`_
        page in the ArcGIS REST API documentation.

        :return:
            :class:`~arcgis.gis.Item` object corresponding to the new web map Item created.

            # save the web map
            webmap_item_properties = {'title':'Ebola incidents and facilities',
                         'snippet':'Map created using Python API showing locations of Ebola treatment centers',
                         'tags':['automation', 'ebola', 'world health', 'python'],
                         'extent': {'xmin': -122.68, 'ymin': 45.53, 'xmax': -122.45, 'ymax': 45.6, 'spatialReference': {'wkid': 4326}}}

            new_wm_item = wm.save(webmap_item_properties, thumbnail='./webmap_thumbnail.png')
        """
        # check item props are there
        return self._helper._save(item_properties, thumbnail, metadata, owner, folder)

    def update(
        self,
        item_properties: Optional[dict[str, Any]] = None,
        thumbnail: Optional[str] = None,
        metadata: Optional[str] = None,
    ) -> bool:
        """
        The ``update`` method updates the Web Map Item in your :class:`~arcgis.gis.GIS`
        with the changes you made to the ``Map`` object. In addition, you can update
        other item properties, thumbnail and metadata.

        .. note::
            If you started with a ``Map`` object from an existing web map item, calling this method will update the item
            with your changes.

            If you started out with a fresh Map object (without a web map item), calling this method will raise a
            RuntimeError exception. If you want to save the Map object into a new web map item, call the
            `save` method instead.

        For ``item_properties``, pass in arguments for the properties you want to be updated.
        All other properties will be untouched.  For example, if you want to update only the
        item's description, then only provide the description argument in ``item_properties``.

        ===============     ====================================================================
        **Parameter**        **Description**
        ---------------     --------------------------------------------------------------------
        item_properties     Optional dictionary. See table below for the keys and values.
        ---------------     --------------------------------------------------------------------
        thumbnail           Optional string. Either a path or URL to a thumbnail image.
        ---------------     --------------------------------------------------------------------
        metadata            Optional string. Either a path or URL to the metadata.
        ===============     ====================================================================

        *Key:Value Dictionary Options for Argument item_properties*

        =================  =====================================================================
        **Key**            **Value**
        -----------------  ---------------------------------------------------------------------
        typeKeywords       Optional string. Provide a lists all sub-types, see URL 1 below for valid values.
        -----------------  ---------------------------------------------------------------------
        description        Optional string. Description of the item.
        -----------------  ---------------------------------------------------------------------
        title              Optional string. Name label of the item.
        -----------------  ---------------------------------------------------------------------
        tags               Optional string. Tags listed as comma-separated values, or a list of strings.
                           Used for searches on items.
        -----------------  ---------------------------------------------------------------------
        snippet            Optional string. Provide a short summary (limit to max 250 characters) of the what the item is.
        -----------------  ---------------------------------------------------------------------
        accessInformation  Optional string. Information on the source of the content.
        -----------------  ---------------------------------------------------------------------
        licenseInfo        Optional string.  Any license information or restrictions regarding the content.
        -----------------  ---------------------------------------------------------------------
        culture            Optional string. Locale, country and language information.
        -----------------  ---------------------------------------------------------------------
        access             Optional string. Valid values are private, shared, org, or public.
        -----------------  ---------------------------------------------------------------------
        commentsEnabled    Optional boolean. Default is true, controls whether comments are allowed (true)
                           or not allowed (false).
        =================  =====================================================================

        The above are the most common item properties (metadata) that you set. To get a complete list, see
        the
        `common parameters <https://developers.arcgis.com/rest/users-groups-and-items/common-parameters.htm#ESRI_SECTION1_1FFBA7FE775B4BDA8D97524A6B9F7C98>`_
        page in the ArcGIS REST API documentation.

        :return:
           A boolean indicating success (True) or failure (False).
        """
        return self._helper._update(item_properties, thumbnail, metadata)

    def zoom_to_layer(
        self,
        item: _gis_mod.Item | dict | features.FeatureSet | features.FeatureLayer,
    ) -> None:
        """
        The ``zoom_to_layer`` method snaps the map to the extent of the provided :class:`~arcgis.gis.Item` object(s).

        Zoom to layer is modifying the rendered map's extent. It is best to run this method call
        in a separate cell from the map rendering cell to ensure the map is rendered before the extent is set.

        ==================     ====================================================================
        **Parameter**           **Description**
        ------------------     --------------------------------------------------------------------
        item                   The item at which you want to zoom your map to.
                               This can be a single extent or an :class:`~arcgis.gis.Item`, ``Layer`` , ``DataFrame`` ,
                               :class:`~arcgis.features.FeatureLayer`, :class:`~arcgis.map.group_layer.SubtypeGroupLayer`,
                               :class:`~arcgis.features.FeatureSet`, or :class:`~arcgis.features.FeatureCollection`
                               object.
        ==================     ====================================================================
        """
        self._helper._zoom_to_layer(item)

    def sync_navigation(self, map: Map | list[Map]) -> None:
        """
        The ``sync_navigation`` method synchronizes the navigation from one rendered Map to
        another rendered Map instance so panning/zooming/navigating in one will update the other.

        .. note::
            Users can sync more than two instances together by passing in a list of Map instances to
            sync. The syncing will be remembered

        ==================      ===================================================================
        **Parameter**           **Description**
        ------------------      -------------------------------------------------------------------
        map                     Either a single Map instance, or a list of ``Map``
                                instances to synchronize to.
        ==================      ===================================================================

        """
        self._helper._sync_navigation(map)

    def unsync_navigation(self, map: Optional[Map | list[Map]] = None) -> None:
        """
        The ``unsync_navigation`` method unsynchronizes connections made to other rendered Map instances
        made via the sync_navigation method.

        ==================     ===================================================================
        **Parameter**           **Description**
        ------------------     -------------------------------------------------------------------
        map                    Optional, either a single `Map` instance, or a list of
                               `Map` instances to unsynchronize. If not specified, will
                               unsynchronize all synced `Map` instances.
        ==================     ===================================================================
        """
        self._helper._unsync_navigation(map)

    def export_to_html(
        self,
        path_to_file: str,
        title: Optional[str] = None,
    ) -> bool:
        """
        The ``export_to_html`` method takes the current state of the rendered map and exports it to a
        standalone HTML file that can be viewed in any web browser.

        By default, only publicly viewable layers will be visible in any
        exported html map. Specify ``credentials_prompt=True`` to have a user
        be prompted for their credentials when opening the HTML page to view
        private content.

        .. warning::
            Follow best security practices when sharing any HTML page that
            prompts a user for a password.

        .. note::
            You cannot successfully authenticate if you open the HTML page in a
            browser locally like file://path/to/file.html. The credentials
            prompt will only properly function if served over a HTTP/HTTPS
            server.

        ==================     ====================================================================
        **Parameter**           **Description**
        ------------------     --------------------------------------------------------------------
        path_to_file           Required string. The path to save the HTML file on disk.
        ------------------     --------------------------------------------------------------------
        title                  Optional string. The HTML title tag used for the HTML file.
        ==================     ====================================================================
        """
        return self._helper._export_to_html(path_to_file, title)

    def print(
        self,
        file_format: str,
        extent: dict[str, Any],
        dpi: int = 92,
        output_dimensions: tuple[float] = (500, 500),
        scale: Optional[float] = None,
        rotation: Optional[float] = None,
        spatial_reference: Optional[dict[str, Any]] = None,
        layout_template: str = "MAP_ONLY",
        time_extent: Optional[Union[tuple[int], list[int]]] = None,
        layout_options: Optional[dict[str, Any]] = None,
    ) -> str:
        """
        The ``print`` method prints the ``Map`` object to a printable file such as a PDF, PNG32, JPG.

        .. note::

            The render and print operations happen
            server side (ArcGIS Online or Enterprise) and not on the client.

        The ``print`` method takes the state of
        the ``Map``, renders and returns either a page layout or a map without page surrounds of the specified extent
        in raster or vector format.

        ==================     ====================================================================
        **Parameter**           **Description**
        ------------------     --------------------------------------------------------------------
        file_format            Required String. Specifies the output file format. Valid types:

                               ``PNG8`` | ``PNG32`` | ``JPG`` | ``GIF`` | ``PDF`` | ``EPS``
                               | ``SVG`` | ``SVGZ``.
        ------------------     --------------------------------------------------------------------
        extent                 Required Dictionary. Specify the extent to be printed.

                               .. code-block:: python

                                   # Example Usage:

                                   >>> extent = {'spatialReference': {'latestWkid': 3857,
                                                                      'wkid': 102100},
                                                 'xmin': -15199645.40582486,
                                                 'ymin': 3395607.5273594954,
                                                 'xmax': -11354557.134968376,
                                                 'ymax': 5352395.451459487}

                               The spatial reference of the extent object is optional; when it is
                               not provided, it is assumed to be in the map's spatial reference.
                               When the aspect ratio of the map extent is different than the size
                               of the map on the output page or the ``output_dimensions``,
                               you might notice more features on the output map.
        ------------------     --------------------------------------------------------------------
        dpi                    Optional integer. Specify the print resolution of the output file. ``dpi`` stands for
                               *dots per inch*. A higher number implies better resolution and a
                               larger file size.
        ------------------     --------------------------------------------------------------------
        output_dimensions      Optional tuple. Specify the dimensions of the output file in pixels. If the
                               ``layout_template`` is not ``MAP_ONLY``, the specific layout
                               template chosen takes precedence over this parameter.
        ------------------     --------------------------------------------------------------------
        scale                  Optional float. Specify the map scale to be printed. The map scale at which you
                               want your map to be printed. This parameter is optional but
                               recommended for optimal results. The ``scale`` property is
                               especially useful when map services in the web map have
                               scale-dependent layers or reference scales set. Since the map that
                               you are viewing on the web app may be smaller than the size of the
                               output map (for example, 8.5 x 11 in. or A4 size), the scale of the
                               output map will be different and you could see differences in
                               features and/or symbols in the web application as compared with
                               the output map.

                               When scale is used, it takes precedence over the extent, but the
                               output map is drawn at the requested scale centered on the center
                               of the extent.
        ------------------     --------------------------------------------------------------------
        rotation               Optional float. Specify the number of degrees by which the map frame will be
                               rotated, measured counterclockwise from the north. To rotate
                               clockwise, use a negative value.
        ------------------     --------------------------------------------------------------------
        spatial_reference      Optional Dictionary.Specify the spatial reference in which map should be printed. When
                               not specified, the following is the order of precedence:

                               - read from the ``extent`` parameter
                               - read from the base map layer of your web map
                               - read from the ``layout_template`` chosen
        ------------------     --------------------------------------------------------------------
        layout_template        Optional String. The default value ``MAP_ONLY`` does not use any template.
        ------------------     --------------------------------------------------------------------
        time_extent            Optional List . If there is a time-aware layer and you want it
                               to be drawn at a specified time, specify this property. This order
                               list can have one or two elements. Add two elements (``startTime``
                               followed by ``endTime``) to represent a time extent, or provide
                               only one time element to represent a time instant.
                               Times are always in UTC.


                               .. code-block:: python

                                   # Example Usage to represent Tues. Jan 1, 2008 00:00:00 UTC:
                                   # to Thurs. Jan 1, 2009 00:00:00 UTC.

                                   >>> time_extent = [1199145600000, 1230768000000]
        ------------------     --------------------------------------------------------------------
        layout_options         Optional Dictionary. This defines settings for different available page layout elements
                               and is only needed when an available ``layout_template`` is chosen.
                               Page layout elements include ``title``, ``copyright text``,
                               ``scale bar``, ``author name``, and ``custom text elements``.
                               For more details, see
                               `ExportWebMap specification. <https://developers.arcgis.com/rest/services-reference/enterprise/exportwebmap-specification.htm>`_
        ==================     ====================================================================

        :return: A URL to the file which can be downloaded and printed.

        .. code-block:: python

                # USAGE EXAMPLE 1: Printing a web map to a JPG file of desired extent.

                from arcgis.map import Map
                from arcgis.gis import GIS

                # connect to your GIS and get the web map item
                gis = GIS(url, username, password)
                wm_item = gis.content.get('1234abcd_web map item id')

                # create a WebMap object from the existing web map item
                wm = Map(item=wm_item)

                # create an empty web map
                wm2 = Map()
                wm2.content.add(<desired Item or Layer object>)

                # set extent
                redlands_extent = {'spatialReference': {'latestWkid': 3857, 'wkid': 102100},
                                     'xmin': -13074746.000753032,
                                     'ymin': 4020957.451106308,
                                     'xmax': -13014666.49652086,
                                     'ymax': 4051532.26242039}

                # print
                printed_file_url = wm.print(file_format='JPG', extent=redlands_extent)
                printed_file2_url = wm2.print(file_format='PNG32', extent=redlands_extent)

                # Display the result in a notebook:
                from IPython.display import Image
                Image(printed_file_url)

                # Download file to disk
                import requests
                with requests.get(printed_file_url) as resp:
                    with open('./output_file.png', 'wb') as file_handle:
                        file_handle.write(resp.content)
        """
        return self._helper._print(
            file_format,
            extent,
            dpi,
            output_dimensions,
            scale,
            rotation,
            spatial_reference,
            layout_template,
            time_extent,
            layout_options,
        )


class Bookmarks:
    """
    The Bookmarks allows end users to quickly navigate to a particular area of interest.
    This class allows users to add, remove and edit bookmarks in their Map.

    .. note::
        This class should now be created by a user but rather called through the `bookmarks` property on
        a Map instance.
    """

    def __init__(self, map: Map) -> None:
        self._source = map

    def __str__(self) -> str:
        return "Bookmarks"

    def __repr__(self) -> str:
        return "Bookmarks()"

    def __setattr__(self, name, value):
        # Allow private attributes and existing properties
        if name.startswith("_") or hasattr(self.__class__, name):
            super().__setattr__(name, value)
        else:
            raise AttributeError(
                f"'{self.__class__.__name__}' object has no attribute '{name}'"
            )

    @property
    def enabled(self) -> bool:
        """
        Get and set whether the bookmarks widget is shown on the rendered or not. Set to True for the
        bookmarks to be visible.
        """
        return self._source._show_bookmarks

    @enabled.setter
    def enabled(self, value):
        if isinstance(value, bool):
            if value != self._source._show_bookmarks:
                # only set if it changed
                self._source._show_bookmarks = value

    @property
    def list(self) -> list[Bookmark]:
        bookmarks = []
        for idx in range(len(self._source._webmap.bookmarks)):
            bookmarks.append(Bookmark(self._source._webmap.bookmarks[idx], self))
        return bookmarks

    def add(
        self,
        name: str,
        extent: dict,
        rotation: int | None = None,
        time_extent: dict | None = None,
        index: int | None = None,
    ) -> bool:
        """
        Add a new bookmark

        =====================       ===================================================================
        **Parameter**                **Definition**
        ---------------------       -------------------------------------------------------------------
        name                        Required string. The name of the bookmark.
        ---------------------       -------------------------------------------------------------------
        extent                      Required dict. The extent of the bookmark.
        ---------------------       -------------------------------------------------------------------
        rotation                    Optional float. The rotation of the viewpoint for the bookmark.
        ---------------------       -------------------------------------------------------------------
        time_extent                 Optional dict. The time extent of the bookmark item.

                                    Example:
                                        time_extent = {
                                            "start": datetime.datetime(1996, 11, 10),
                                            "end": datetime.datetime(1996, 11, 25)
                                        }
        ---------------------       -------------------------------------------------------------------
        index                       Required integer. The index position for the bookmark. If none specified,
                                    added to the end of the list.
        =====================       ===================================================================
        """
        # Set the index
        index = index if index is not None else len(self._source._webmap.bookmarks)
        # Create pydantic dataclass extent to validate
        extent = _models.Extent(**extent)
        # Create pydantic dataclass viewpoint to validate
        viewpoint = _models.MapViewpoint(
            rotation=rotation if rotation else 0, targetGeometry=extent
        )
        if time_extent:
            # Create pydantic dataclass time extent to validate
            time_extent = _models.TimeExtent(**time_extent)

        # Create the bookmark with our properties
        bookmark = _models.Bookmark(
            name=name,
            extent=extent,
            viewpoint=viewpoint,
            timeExtent=time_extent,
        )

        # Add to the list of bookmarks in webmap
        self._source._webmap.bookmarks.insert(index, bookmark)

        # If the widget is enabled, update the webmap dict so it reflects in the widget
        self._source._update_source()
        return True

    def remove(self, index: int) -> bool:
        """
        Remove a bookmark from the list of bookmarks.

        =====================       ===================================================================
        **Parameter**                **Definition**
        ---------------------       -------------------------------------------------------------------
        index                       Required integer. The index for the bookmark in the bookmarks list that
                                    will be removed.
        =====================       ===================================================================

        """
        # remove from webmap bookmarks list
        del self._source._webmap.bookmarks[index]
        # If the widget is enabled, update the webmap dict so it reflects in the widget
        self._source._update_source()
        return True


class Bookmark:
    """
    Represent one bookmark in the webmap. This class can be used to edit a bookmark instant and can be accessed through the `list` property of the Bookmarks class.

    .. note::
        This class should now be created by a user but rather called through the `list` property on
        a Bookmarks instance.
    """

    def __init__(self, bookmark: _models.Bookmark, bm_mngr: Bookmarks) -> None:
        self._bookmark = bookmark
        self._bm_mngr: Bookmarks = bm_mngr  # used to update

    def __str__(self) -> str:
        return "Bookmark: " + self._bookmark.name

    def __repr__(self) -> str:
        return f"Bookmark(name={self._bookmark.name})"

    def __setattr__(self, name, value):
        # Allow private attributes and existing properties
        if name.startswith("_") or hasattr(self.__class__, name):
            super().__setattr__(name, value)
        else:
            raise AttributeError(
                f"'{self.__class__.__name__}' object has no attribute '{name}'"
            )

    @property
    def name(self) -> str:
        """
        Returns the name of the bookmark
        """
        return self._bookmark.name

    @property
    def extent(self) -> dict:
        """
        Returns the extent of the bookmark
        """
        extent = self._bookmark.extent
        return extent.dict() if extent else {}

    @property
    def viewpoint(self) -> dict:
        """
        Returns the current viewpoint set for the bookmark
        """
        viewpoint = self._bookmark.viewpoint
        if viewpoint is None:
            self.edit(extent=self.extent)
            # add a viewpoint and call again
            return self.viewpoint
        return viewpoint.dict() if viewpoint else {}

    def edit(
        self,
        name: str | None = None,
        extent: dict | None = None,
        rotation: int | None = None,
        time_extent: dict | None = None,
    ) -> None:
        """
        Edit the properties of a bookmark. Edit the name, extent, rotation, scale and/or time_extent.

        =====================       ===================================================================
        **Parameter**                **Definition**
        ---------------------       -------------------------------------------------------------------
        name                        Optional string. The name of the bookmark.
        ---------------------       -------------------------------------------------------------------
        extent                      Optional dict. The extent of the bookmark.
        ---------------------       -------------------------------------------------------------------
        rotation                    Optional float. The rotation of the viewpoint for the bookmark.
        ---------------------       -------------------------------------------------------------------
        time_extent                 Optional dict. The time extent of the bookmark item.

                                    Example:
                                        time_extent = {
                                            "start": datetime.datetime(1996, 11, 10),
                                            "end": datetime.datetime(1996, 11, 25)
                                        }
        =====================       ===================================================================

        """
        if name is not None:
            # Edit bookmark name
            self._bookmark.name = name
        if extent is not None:
            # Create pydantic extent
            new_extent = _models.Extent(**extent)
            # Set new extent where needed
            self._bookmark.extent = new_extent
            if self._bookmark.viewpoint is None:
                # If viewpoint is None, create one
                self._bookmark.viewpoint = _models.MapViewpoint(
                    targetGeometry=new_extent
                )
            else:
                self._bookmark.viewpoint.target_geometry = new_extent
        if rotation is not None:
            # Edit rotation
            if self._bookmark.viewpoint is None:
                # If viewpoint is None, create one
                self._bookmark.viewpoint = _models.MapViewpoint(
                    targetGeometry=self._bookmark.extent
                )
            self._bookmark.viewpoint.rotation = rotation
        if time_extent is not None:
            # Create pydantic time extent
            new_time_extent = _models.TimeExtent(**time_extent)
            # Set new time extent
            self._bookmark.time_extent = new_time_extent
        if self._bm_mngr.enabled:
            # If the widget is enabled, update the webmap dict so it reflects in the widget
            self._bm_mngr._source._update_source()


class Legend:
    """
    The Legend describes the symbols used to represent layers
    in a map. All symbols and text used in this widget are configured
    in the Renderer of the layer. The legend will only display layers and
    sub-layers that are visible in the map.

    The legend automatically updates when
    - the visibility of a layer or sub-layer changes
    - a layer is added or removed from the map
    - a layer's renderer, opacity, or title is changed
    - the legendEnabled property is changed (set to true or false on the layer)


    .. note::
        This class should now be created by a user but rather called through the `legend` property on
        a Map instance.
    """

    def __init__(self, map) -> None:
        self._source = map

    def __str__(self) -> str:
        return "Legend"

    def __repr__(self) -> str:
        return "Legend()"

    def __setattr__(self, name, value):
        # Allow private attributes and existing properties
        if name.startswith("_") or hasattr(self.__class__, name):
            super().__setattr__(name, value)
        else:
            raise AttributeError(
                f"'{self.__class__.__name__}' object has no attribute '{name}'"
            )

    @property
    def enabled(self) -> bool:
        """
        Get and set whether the legend is shown on the map/scene or not. Set to True for the
        legend to be visible.

        The Legend widget describes the symbols used to represent layers in a map.
        All symbols and text used in this widget are configured in the renderer of the layer.
        The legend will only display layers and sub-layers that are visible in the map.

        The legend automatically updates when
        - the visibility of a layer or sub-layer changes
        - a layer is added or removed from the map
        - a layer's renderer, opacity, or title is changed

        .. note::

            Known Limitations:
            - Currently, the legend widget does not support the following layer types: ElevationLayer, GraphicsLayer, IntegratedMeshLayer, KMLLayer, MapNotesLayer, OpenStreetMapLayer, VectorTileLayer, and WebTileLayer.
            - 3D symbols with more than one symbol layer are not supported.
            - DictionaryRenderer is not supported.

        """
        return self._source._show_legend

    @enabled.setter
    def enabled(self, value: bool):
        if isinstance(value, bool):
            if value != self._source._show_legend:
                # only set if it changed
                self._source._show_legend = value


class LayerList:
    """
    A class that can be used to enable the layer list widget on a rendered Map. This class is most useful when used
    inside a Jupyter Lab environment.


    .. note::
        This class should now be created by a user but rather called through the `layer_list` property on
        a Map instance.
    """

    def __init__(self, map) -> None:
        self._source = map

    def __str__(self) -> str:
        "Layer List"

    def __repr__(self) -> str:
        return "LayerList()"

    def __setattr__(self, name, value):
        # Allow private attributes and existing properties
        if name.startswith("_") or hasattr(self.__class__, name):
            super().__setattr__(name, value)
        else:
            raise AttributeError(
                f"'{self.__class__.__name__}' object has no attribute '{name}'"
            )

    @property
    def enabled(self) -> bool:
        """
        The Layer List widget allows end users to quickly see the layers that are present on your map.
        It displays a list of layers, which are defined inside the Map. Set to True for the
        layer list to be visible. This property is best used inside Jupyter Lab with a rendered Map.

        .. note::
            Any changes made on the layer list widget will only be reflected in the rendered map instance, these
            will not affect the map definition. To change layer visibility in the definition, use the `layer_visibility`
            method.
        """
        return self._source._show_layer_list

    @enabled.setter
    def enabled(self, value):
        if isinstance(value, bool):
            if value != self._source._show_layer_list:
                # only set if it changed
                self._source._show_layer_list = value


class TimeSlider:
    """
    A class that can be used to enable the time slider widget on a rendered Map. This class is most useful when used
    inside a Jupyter Lab environment.


    .. note::
        This class should now be created by a user but rather called through the `time_slider` property on
        a Map instance.
    """

    def __init__(self, map) -> None:
        self._source = map

    def __str__(self) -> str:
        "Time Slider"

    def __repr__(self) -> str:
        return "TimeSlider()"

    def __setattr__(self, name, value):
        # Allow private attributes and existing properties
        if name.startswith("_") or hasattr(self.__class__, name):
            super().__setattr__(name, value)
        else:
            raise AttributeError(
                f"'{self.__class__.__name__}' object has no attribute '{name}'"
            )

    @property
    def enabled(self) -> bool:
        """
        The TimeSlider widget simplifies visualization of temporal data in your application.

        .. note::
            The TimeSlider widget is only available for layers that have time information enabled.
        """
        return self._source._show_time_slider

    @enabled.setter
    def enabled(self, value):
        if isinstance(value, bool):
            if value != self._source._show_time_slider:
                # only set if it changed
                self._source._show_time_slider = value

    def time_extent(
        self,
        start_time: _dt.datetime | None = None,
        end_time: _dt.datetime | None = None,
        time_interval: dict | None = None,
        layer_idx: int = 0,
    ):
        """
        The ``time_extent`` is called when `enabled = True` and is the full time extent to display on the time
        slider.

        ==================      ====================================================================
        **Parameter**           **Description**
        ------------------      --------------------------------------------------------------------
        start_time              Optional ``datetime.datetime``. The lower bound of the full time extent to
                                display on the time slider.
        ------------------      --------------------------------------------------------------------
        end_time                Optional ``datetime.datetime``. The upper bound of the full time extent to
                                display on the time slider.
        ------------------      --------------------------------------------------------------------
        time_interval           Optional dict. The time interval to display on the time slider. The time
                                interval is a dictionary with two keys: `value` and `units`. The interval
                                is the number of units to display on the time slider. The units are the units
                                of the interval.
        ------------------      --------------------------------------------------------------------
        layer_idx               Optional integer. The index of the layer in the webmap that the time extent
                                should be applied to. If not specified, the time extent will be applied to
                                the first layer in the webmap.
        ==================      ====================================================================

        """
        # Check that time is in correct format
        if self.enabled is False:
            raise Exception("The time slider must be enabled in order to edit.")
        if start_time and not (isinstance(start_time, _dt.datetime)):
            raise Exception("`start_time` argument must be of type `datetime.datetime`")
        if end_time and not (isinstance(end_time, _dt.datetime)):
            raise Exception("`end_time` argument must be of type `datetime.datetime`")
        if time_interval and not (isinstance(time_interval, dict)):
            raise Exception(
                "`time_interval` argument must be of type `dict` with keys `value` and `units`"
            )
        if time_interval:
            if "value" not in time_interval or "units" not in time_interval:
                raise Exception(
                    "`time_interval` argument must be of type `dict` with keys `value` and `units`"
                )

        full_extent = {
            "start": int(start_time.timestamp()) if start_time else None,
            "end": int(end_time.timestamp()) if end_time else None,
        }

        self._source._time_slider_full_extent = full_extent
        self._source._time_slider_time_interval = time_interval
        self._source._time_slider_layer = layer_idx


class LayerVisibility:
    """
    A class that can be used to manage the visibility of layers in a map. The layer visibility class is primarily
    used to set the visibility of the layers on the map and in the legend.


    .. note::
        This class should now be created by a user but rather called through the `layer_visibility` property on
        a Map or GroupLayer instance.
    """

    def __init__(self, layer: _models.Layer) -> None:
        # The pydantic layer, this hooks it to the main webmap and tracks changes made
        self._layer: _models.Layer = layer

    def __str__(self) -> str:
        return "Layer Visibility for: " + self._layer.title

    def __repr__(self) -> str:
        return f"LayerVisibility(layer={self._layer.title})"

    def __setattr__(self, name, value):
        # Allow private attributes and existing properties
        if name.startswith("_") or hasattr(self.__class__, name):
            super().__setattr__(name, value)
        else:
            raise AttributeError(
                f"'{self.__class__.__name__}' object has no attribute '{name}'"
            )

    @property
    def visibility(self) -> bool:
        """
        Get and Set the visibility of the layer. Set to True for the layer to be
        visible and False otherwise.
        """
        return self._layer.visibility

    @visibility.setter
    def visibility(self, visible: bool):
        if isinstance(visible, bool):
            self._layer.visibility = visible

    @property
    def legend_visibility(self) -> bool:
        """
        Get and Set the visibility of the layer in the legend. Set to True for the layer to be
        visible in the legend and False otherwise.
        """
        return self._layer.show_legend

    @legend_visibility.setter
    def legend_visibility(self, visible: bool):
        if isinstance(visible, bool):
            self._layer.show_legend = visible


class BasemapManager:
    """
    The Basemap Manager allows users to manage the basemap of a webmap or webscene.
    Users can view their basemap options, set the basemap to a new one, and add layers to the basemap.

    This class is created from the `basemap` property on a Map or Scene instance.
    """

    _basemap_gallery = {}

    def __init__(self, map) -> None:
        self._source = map
        self._helper = map._helper
        self._pydantic_class = map._webmap if isinstance(map, Map) else map._webscene

    def __str__(self) -> str:
        return "BasemapManager"

    def __repr__(self) -> str:
        return "BasemapManager()"

    @property
    def layers(self):
        """
        List of layers in the basemap.
        """
        return self.basemap["baseMapLayers"]

    @property
    def basemap_styles_service(self) -> BasemapStylesService:
        """
        This property returns a :class:`~arcgis.map.BasemapStylesService` object
        providing access to the `ArcGIS Basemap Styles service <https://developers.arcgis.com/rest/basemap-styles/>`_.
        It allows users to retrieve and manage :class:`basemap style <arcgis.map.BasemapStyle>`
        objects for use in ArcGIS Notebooks and mapping applications.

        .. note::
            **Only** available in *ArcGIS Online* or *Location Platform*.
        """
        if self._source._gis._is_arcgisonline:
            # If the GIS is ArcGIS Online, use the BasemapStylesService class
            return BasemapStylesService(self._source._gis)
        else:
            raise Exception(
                "Basemap styles service is only available on ArcGIS Online or Location Platform."
            )

    @property
    def basemaps(self) -> list[str]:
        """
        List of possible basemaps to use.
        All those starting with 'arcgis' require you to be authenticated.
        """
        return self._helper.basemaps

    @property
    def basemaps3d(self) -> list[str]:
        """
        List of possible 3D basemaps to use with a Scene.
        All those starting with 'arcgis' require you to be authenticated.
        """
        # List of basemaps from the basemap def file
        return self._helper.basemaps3d

    @property
    def basemap(self) -> str | _gis_mod.Item:
        """
        Get and/or set the current basemap of the Map or Scene.
        Basemap values can be found by calling the :attr:`~arcgis.map.BasemapManager.basemaps`
        or :attr:`~arcgis.map.BasemapManager.basemap_gallery` property. An existing *web map*
        :class:`~arcgis.gis.Item` type can also be assigned, and then its basemap will be set
        as the *map* object's basemap.

        Setting the *basemap* will replace all current basemap layers with the new basemap's
        layers.

        To use the `ArcGIS Basemap Styles service <https://developers.arcgis.com/rest/basemap-styles/>`_ to
        set the basemap, pass a :class:`basemap style <arcgis.map.BasemapStyle>` to use.

        .. note::
            **Only** works on rendered map instances.

        .. code-block:: python

            # Usage Example #1: Set the basemap with the styles service

            >>> from arcgis.gis import GIS

            >>> gis = GIS(profile="your_online_profile")
            >>> new_map = gis.map("Geneva")

            >>> styles_svc = new_map.basemap.basemap_styles_service

            >>> styles_svc.styles_names

            ['ArcGIS Imagery',
             'ArcGIS Imagery Standard',
             'ArcGIS Dark Gray'
             ...
             'Open Basemaps OpenStreetMap Style']

            >>> dark_gray_style = styles_svc.get_style("ArcGIS Dark Gray")

            >>> new_map.basemap.basemap = dark_gray_style

            # Usage Example #2: Set the basemap with value from *basemaps* list

            >>> new_map.basemap.basemaps
            ['satellite'
             'hybrid',
             ...
             'osm-streets']

            >>> new_map.basemap.basemap = "satellite"
            # visible in rendered map in an ArcGIS Notebook


        .. note::
            If you set a *basemap* with a different spatial reference than a *webmap*
            or *webscene*, any operational layers on the original map will not be
            re-projected automatically.
        """
        # Current basemap in the webmap
        # Set exclude none to True to avoid returning unnecessary properties
        return self._helper.basemap

    @basemap.setter
    def basemap(self, basemap: str | _gis_mod.Item | dict):
        basemap_dict = None

        if isinstance(basemap, BasemapStyle):
            # Get the webmap dictionary from the basemap style
            basemap_dict = basemap._get_webmap_dict()["baseMap"]
        elif isinstance(basemap, str):
            # helper method returns necessary dict
            basemap_dict = self._helper._set_basemap_from_definition(basemap)
        elif isinstance(basemap, _gis_mod.Item) and (
            basemap.type.lower() == "web scene" or basemap.type.lower() == "web map"
        ):
            # helper method returns necessary dict
            basemap_dict = self._helper._set_basemap_from_map_scene_item(basemap)
        elif (
            isinstance(basemap, _gis_mod.Item)
            or isinstance(basemap, arcgis_layers.VectorTileLayer)
            or isinstance(basemap, arcgis_layers.MapServiceLayer)
            or isinstance(basemap, arcgis_layers.WMTSLayer)
            or isinstance(basemap, arcgis_layers.WMSLayer)
        ):
            # helper method sets the basemap, return when done
            self._helper._set_basemap_from_item(basemap)
            return
        else:
            raise ValueError(
                "Basemap value must be a string from the basemaps property or gallery basemaps, an existing Scene Item, Webmap Item, or a Layer of type that can be added as the basemap."
            )
        if basemap_dict is None:
            raise ValueError("The basemap could not be found.")
        # set on the pydantic class
        self._pydantic_class.base_map = (
            _models.MapBasemap(**basemap_dict)
            if isinstance(self._source, Map)
            else _models.SceneBasemap(**basemap_dict)
        )
        # Update source because possibility of spatial reference change
        self._source._update_source()

    @property
    def basemap_gallery(self) -> list[str]:
        """
        The ``basemap_gallery`` property allows for viewing of your portal's custom basemap group.
        """
        return self._helper.basemap_gallery

    def move_from_basemap(self, index: int) -> bool:
        """
        Move a layer from the basemap layers to the operational layers. The reverse process of
        `move_to_basemap`.

        =====================       ===================================================================
        **Parameter**                **Definition**
        ---------------------       -------------------------------------------------------------------
        index                       Required integer. The index of the layer found in the basemap layers that
                                    will be moved to be in the operational layers.
        =====================       ===================================================================

        .. code-block:: python

            wm = Map(item=<webmap_item_id>)
            layer = wm.basemap["baseMapLayer"][0]
            wm.move_from_basemap()
            wm.update()
        """
        self._helper._move_from_basemap(index)
        # Refresh widget
        self._source._update_source()
        return True

    @property
    def title(self):
        """
        Get/Set the basemap title.

        =====================       ===================================================================
        **Parameter**                **Definition**
        ---------------------       -------------------------------------------------------------------
        title                       Required string. The title to set for the basemap.
        =====================       ===================================================================
        """
        return self._pydantic_class.base_map.title

    @title.setter
    def title(self, title: str):
        self._helper._set_basemap_title(title)

    def remove_basemap_layer(self, index: int) -> bool:
        """
        Remove a layer from the basemap layers. You can see the current basemap layers
        by calling the `basemap` property on your map. If you want to update the title of the basemap
        you can use the `basemap_title` method.

        .. note::
            There must be at least one basemap layer present. You cannot remove all basemap layers.

        =====================       ===================================================================
        **Parameter**                **Definition**
        ---------------------       -------------------------------------------------------------------
        index                       Required integer. The index for the layer in the basemap layers that
                                    will be removed.
        =====================       ===================================================================

        """
        self._helper._remove_basemap_layer(index)
        self._source._update_source()
        return True


class MapContent:
    """
    This class allows users to manage the content of a webmap. Users can view the operational layers,
    add layers, reorder layers, and remove layers.

    This class is created from the `map_content` property on a Map instance.
    """

    layers: list = None
    tables: list = None

    def __init__(self, webmap: Map) -> None:
        self._source: Map = webmap
        self._helper = webmap._helper

    def __str__(self) -> str:
        return "Map Content"

    def __repr__(self) -> str:
        return "MapContent()"

    def clear_cache(self):
        type(self).layers.clear(self)
        type(self).tables.clear(self)

    @refreshable_property
    def layers(self) -> list:
        """
        Initialize the list of layers found in the list.
        This will return a list of class instances representing each layer.
        """
        return self._helper._layers

    @refreshable_property
    def tables(self) -> list[features.Table]:
        """
        Initialize the list of tables found in the list.
        This will return a list of Table instances representing each table.
        """
        return self._helper._tables

    @property
    def layer_info(self) -> pd.DataFrame:
        """
        Return a DataFrame with information about the layers in the map.
        """
        return self._helper._layer_info()

    @property
    def table_info(self) -> pd.DataFrame:
        """
        Return a DataFrame with information about the tables in the map.
        """
        return self._helper._table_info()

    def get_layer(self, title: str) -> arcgis.gis.Layer:
        """
        Get a layer or table from the map by title. If the layer is added more than once only the
        first instance of the layer will be returned.

        ..note::
            To find a layer by index you can use the `layer_info` method to see the layers in a table form.

        =====================       ===================================================================
        **Parameter**                **Definition**
        ---------------------       -------------------------------------------------------------------
        title                       Required string. The title of the layer or table to get.
        =====================       ===================================================================

        :returns: Layer
        """
        return self._helper._get_layer(title, False)

    def get_table(self, title: str) -> arcgis.gis.Table:
        """
        Get a table from the map by title. If the table is added more than once only the
        first instance of the table will be returned.

        ..note::
            To find a table by index you can use the `table_info` method to see the tables in a table form.

        =====================       ===================================================================
        **Parameter**                **Definition**
        ---------------------       -------------------------------------------------------------------
        title                       Required string. The title of the table to get.
        =====================       ===================================================================

        :returns: Table
        """
        return self._helper._get_layer(title, True)

    def add(
        self,
        item: (
            _gis_mod.Item
            | _gis_mod.Layer
            | features.Table
            | features.FeatureCollection
            | pd.DataFrame
            | list
            | str
        ),
        drawing_info: dict | None = None,
        popup_info: popups.PopupInfo = None,
        index: int | None = None,
        options: dict | None = None,
    ) -> None:
        """
        Add a layer or table to the map.

        .. note::
            The spatial reference of the map is derived from the basemap layer.
            If the spatial reference of the layer being added is different from the map's spatial reference, you may not see it render correctly.
            You can set the layer as the basemap to change the map's spatial reference.

        ==================      =====================================================================
        **Parameter**           **Description**
        ------------------      ---------------------------------------------------------------------
        item                    Required Portal Item, Layer, Spatial DataFrame, or Feature Collection to add to the map.

                                    * If an item is passed in: Creates a new layer instance of the
                                    appropriate layer class from an ArcGIS Online or ArcGIS Enterprise portal
                                    item.
                                    * If the item points to a feature service with multiple layers,
                                    then a GroupLayer is created.
                                    * If the item points to a service with a single
                                    layer, then it resolves to a layer of the same type of class as the service.
                                    * To add an ogc feature service, pass in an instance of the OGCFeatureService class.
                                    The first collection of the service will be added. If you want to specify the collection
                                    then you can pass in an instance of the FeatureCollection you want added.
                                    * A list of layers. This is useful when wanting to create a GroupLayer out of multiple
                                    layers. The list can contain any combination of the layer types listed above.
                                    * Raster object created from a local raster can be added. However, the raster will only render on the map and cannot
                                    be saved to the webmap. The raster will be added as a MediaLayer.

                                .. note::
                                    If the item is a WMTS Layer, you can pass a key-value pair in the options parameter to select which layer
                                    to add if the WMTS service has multiple layers. The key should be 'layer' and the value should be the layer identifier.
                                    If nothing is passed, the first layer will be added.
        ------------------      ---------------------------------------------------------------------
        drawing_info            Optional dictionary representing the drawing info to apply to the layer.
                                The keys can include any combination of: 'renderer', 'labelingInfo', 'scaleSymbols',
                                'showLabels', 'transparency'. This can only be applied when adding one layer.
                                Renderer 'type' can be "simple", "uniqueValue", "classBreaks", "heatmap", "dotDensity", etc.

                                To create a renderer see the renderer module where all the dataclasses are defined.

                                Example Structure:
                                drawing_info = {
                                    "labelingInfo": <list>,
                                    "renderer": <Renderer Dataclass>,
                                    "scaleSymbols": <bool>,
                                    "showLabels": <bool>,
                                    "transparency": <float>
                                }

                                More information on the structure of the dictionary can be found in the
                                `ArcGIS REST API documentation <https://developers.arcgis.com/web-map-specification/objects/drawingInfo/>`_.
        ------------------      ---------------------------------------------------------------------
        popup_info              Optional PopupInfo dataclass that can be created from the popups module.
                                See: :class:`~arcgis.map.popups.PopupInfo`
        ------------------      ---------------------------------------------------------------------
        index                   Optional integer. Determines at what index the layer should be added.
        ------------------      ---------------------------------------------------------------------
        options                 Optional dictionary. Pass in key, value pairs that will edit the layer properties.
                                This is useful for updating the layer properties that are not exposed through
                                the other parameters.
                                For example, you can update the layer title, opacity, or other
                                properties that are applicable for the type of layer.

                                You can pass in the service item id associated to the service url to persist renderer
                                and popup info that is available in the service item.

                                You can pass custom parameters as well by adding a key-value pair to the dictionary.
                                Example: {"custom_parameters": {"subscription-key": "your_key"}}
        ==================      =====================================================================

        .. code-block:: python

            # Usage Example 1: Add a layer from a portal item
            m = Map()
            item = gis.content.get(<item_id>)

            renderer = HeatmapRenderer(**{
                    "authoringInfo": {"fadeRatio": 0.2},
                    "type": "heatmap",
                    "blurRadius": 5.555555555555556,
                    "colorStops": [
                        {"color": [133, 193, 200, 0], "ratio": 0},
                        {"color": [144, 161, 190, 212], "ratio": 0.0925},
                        {"color": [156, 129, 132, 255], "ratio": 0.17500000000000002},
                        {"color": [167, 97, 170, 255], "ratio": 0.2575},
                        {"color": [175, 73, 128, 255], "ratio": 0.34},
                        {"color": [255, 255, 0, 255], "ratio": 1},
                    ],
                    "maxDensity": 0.611069562632112,
                    "maxPixelIntensity": 139.70497545315783,
                    "minDensity": 0,
                    "minPixelIntensity": 0,
                    "radius": 10,
            })
            m.content.add(item, drawing_info={"renderer": renderer})
        """
        # ----------------------------------------------
        # Step 1: Define default variables needed for the method
        # ----------------------------------------------
        drawing_info = drawing_info or {}
        popup_info = popup_info or {}
        options = options or {}
        if index:
            # index added to options for later use
            options["index"] = index

        # ----------------------------------------------
        # Step 2: Clean and convert item input if string
        # ----------------------------------------------
        if isinstance(item, str):
            cleaned = item.strip()
            if cleaned.lower().startswith(("http://", "https://")):
                # Assume it's a service URL
                item = arcgis_layers.Service(cleaned)
            elif len(cleaned) == 32:
                # assume it's an item id
                item = self._source._gis.content.get(cleaned)
                if item is None:
                    raise ValueError(
                        f"Item with id {cleaned} could not be found in the portal."
                    )
            else:
                raise ValueError(
                    "If passing a string, it must be a URL starting with http or https."
                )

        ### Do not change order of this section ###
        # ----------------------------------------------
        # Step 3: Clean and convert item input if dataframe, sedf, featureset, feature collection
        # ----------------------------------------------
        if isinstance(item, geo.GeoAccessor) or isinstance(item, pd.DataFrame):
            # either spatial df or sedf
            if isinstance(item, pd.DataFrame) and geo._is_geoenabled(item):
                # If dataframe passed in, make it SEDF
                item = item.spatial
            elif not isinstance(item, geo.GeoAccessor):
                raise Exception(
                    "Invalid item format. The dataframe must be geoenabled to be added as a layer to the map."
                )
            # If no renderer is given to the drawing info, check if the SEDF has a renderer
            if not drawing_info.get("renderer"):
                if hasattr(item, "renderer") and item.renderer is not None:
                    # Create the correct renderer
                    drawing_info["renderer"] = rm.FactoryWorker(
                        renderer_type=item.renderer["type"],
                        renderer=item.renderer,
                    )
            # Step 1: Turn SEDF into feature set
            item = item.to_featureset()
        # Handle feature set
        if isinstance(item, features.FeatureSet):
            # Step 2: Turn Feature Set into Feature Collection
            item = features.FeatureCollection.from_featureset(item)

        # ----------------------------------------------
        # Step 4: Handle add feature collection or service
        # ----------------------------------------------
        # Handle feature collection
        if isinstance(item, features.FeatureCollection):
            # Step 3: Add feature collection as a layer
            # This also adds to the layers property in the method
            self._helper._add_from_feature_collection(
                item, drawing_info, popup_info, options
            )
        else:
            # Create a layer, could be group or single layer
            self._helper._create_layer_from_service(
                item, drawing_info, popup_info, options
            )

        # ----------------------------------------------
        # Step 5: Clear Cache and Refresh Widget
        # ----------------------------------------------
        self.clear_cache()
        self._source._update_source()

    def update_layer(
        self,
        index: int = None,
        labeling_info: list[dict[str, Any]] | None = None,
        renderer: (
            renderer.SimpleRenderer
            | renderer.HeatmapRenderer
            | renderer.PieChartRenderer
            | renderer.ClassBreaksRenderer
            | renderer.UniqueValueRenderer
            | renderer.DotDensityRenderer
            | renderer.DictionaryRenderer
            | None
        ) = None,
        scale_symbols: bool | None = None,
        transparency: int | None = None,
        options: dict | None = None,
        form: forms.FormInfo | None = None,
    ) -> None:
        """
        This method can be used to update certain properties on a layer that is in your map.

        ==================      =====================================================================
        **Parameter**           **Description**
        ------------------      ---------------------------------------------------------------------
        index                   Optional integer specifying the index for the layer you want to update.
                                To see a list of layers use the layers property. This cannot be a group layer.
                                To update a group layer, use the `update` method in the group layer class.
                                It's better to pass the index to be more specific with which layer you
                                want to update.
        ------------------      ---------------------------------------------------------------------
        labeling_info           Optional list of dictionaries. Defines the properties used for labeling the layer.

                                Example of some properties:
                                labeling_info = [{
                                    "labelExpression": '[FAA_ID]',
                                    "maxScale": 0,
                                    "minScale": 10000,
                                    "symbol": {
                                        "color": [104, 104, 104, 255],
                                        "type": "esriTS",
                                        "verticalAlignment": "bottom",
                                        "horizontalAlignment": "center",
                                        "font": {
                                            "decoration": "none",
                                            "family": "Arial",
                                            "size": 8,
                                            "style": "normal",
                                            "weight": "bold"
                                        }
                                    }
                                }]
        ------------------      ---------------------------------------------------------------------
        renderer                Optional Renderer Dataclass. See the renderer module where all the dataclasses are defined.
        ------------------      ---------------------------------------------------------------------
        scale_symbols           Optional bool. Indicates whether symbols should stay the same size in
                                screen units as you zoom in. False means the symbols stay the same size
                                in screen units regardless of the map scale.
        ------------------      ---------------------------------------------------------------------
        transparency            Optional int. Value ranging between 0 (no transparency) to 100 (completely transparent).
        ------------------      ---------------------------------------------------------------------
        options                 Optional dictionary. Pass in key, value pairs that will edit the layer properties.
                                This is useful for updating the layer properties that are not exposed through
                                the other parameters. For example, you can update the layer title, opacity, or other
                                properties that are applicable for the type of layer.
        ------------------      ---------------------------------------------------------------------
        form                    Optional FormInfo Dataclass. See the forms module where all the dataclasses are defined.
                                You can get the current FormInfo by calling the `form` property and indicating the index
                                of the layer you want to get the form for.
                                Forms are only supported for Feature Layers, Tables, and Oriented Imagery Layers.
        ==================      =====================================================================
        """
        self._helper.update_layer(
            index,
            labeling_info,
            renderer,
            scale_symbols,
            transparency,
            options,
            form,
        )
        MapContent.layers.clear(self)

    def remove(self, index: int | None = None, is_layer: bool = True) -> bool:
        """
        Remove a layer or table from the map either by specifying the index or passing in the layer dictionary.

        ==================      =====================================================================
        **Parameter**           **Description**
        ------------------      ---------------------------------------------------------------------
        index                   Optional integer specifying the index for the layer you want to remove.
                                To see a list of layers use the layers property.
        ------------------      ---------------------------------------------------------------------
        is_layer                Optional boolean. Set to True if removing a layer and False if removing a table.
        ==================      =====================================================================
        """
        if is_layer:
            self._helper.remove_layer(index)
        else:
            self._helper.remove_table(index)
        # Refresh the widget
        self._source._update_source()
        return True

    def remove_all(self) -> bool:
        """
        Remove all layers and tables from the map.
        """
        self._helper.remove_all()
        # Refresh the widget
        self._source._update_source()
        return True

    def draw(
        self,
        shape,
        popup=None,
        symbol=None,
        attributes=None,
        title: str | None = None,
    ) -> None:
        """
        Draw a shape on the map. This will add the shape as a feature collection to your layers of the map.
        Anything can be drawn from known :class:`~arcgis.geometry.Geometry` objects.

        The shape will be drawn as a feature collection on the map. This means that the shape will be added as a layer
        to the map. The shape will be drawn with the symbol and popup info that is passed in. If no symbol is passed in,
        a default symbol will be created based on the geometry type. If no popup is passed in, a default popup will be created
        with the title of the feature collection. The attributes will be added to the feature collection as well.

        .. note::
            Ensure that the spatial reference of the shape matches the spatial reference of the map. If the spatial reference
            does not match, the shape will be added but not drawn on the map. To see the spatial reference of the map, call the
            `extent` property on the map.

        ==================      =====================================================================
        **Parameter**           **Description**
        ------------------      ---------------------------------------------------------------------
        shape                   Required Geometry object.
                                Known :class:`~arcgis.geometry.Geometry` objects:
                                Shape is one of ['circle', 'ellipse', :class:`~arcgis.geometry.Polygon`,
                                :class:`~arcgis.geometry.Polyline`,
                                :class:`~arcgis.geometry.MultiPoint`, :class:`~arcgis.geometry.Point`,
                                'rectangle', 'triangle'].
        ------------------      ---------------------------------------------------------------------
        popup                   Optional PopupInfo Dataclass. See: :class:`~arcgis.map.popups.PopupInfo`
        ------------------      ---------------------------------------------------------------------
        symbol                  Optional Symbol Dataclass. See the symbols module where all the dataclasses are defined.
        ------------------      ---------------------------------------------------------------------
        attributes              Optional dict. Specify a dict containing name value pairs of fields and field values
                                associated with the graphic.
        ------------------      ---------------------------------------------------------------------
        title                   Optional string. The title of the feature collection that will be created
                                from the geometry or feature set.
        ==================      =====================================================================

        .. code::python

            #Example Usage

            g_map = Map(location="Redlands,CA")

            single_line_address = "380 New York Street, Redlands, CA 92373"
            sr = g_map.extent['spatialReference']['latestWkid']
            esrihq = geocode(single_line_address, out_sr=sr)[0]
            popup = PopupInfo(**{
                "title" : "Esri Headquarters",
                "popup_elements" : [{"text":esrihq['address'], "type":"text"}]
                })

            g_map.content.draw(esrihq['location'], popup)

        """
        # Create the title for feature and layer
        title = title if title else "Sketch Layer"

        if isinstance(shape, features.FeatureSet):
            # Shape is a feature set already so we don't have to do as much
            geometry_type = shape.geometry_type
            fields = shape.fields

            # Create symbol
            if symbol is None:
                symbol = self._create_default_symbol(geometry_type)
            # Create a pydantic FeatureSet to validate everything
            # First make sure everything is a dict
            all_features = []
            for feature in shape.features:
                all_features.append(feature.as_dict)
            feature_set = _models.FeatureSet(
                features=all_features, geometryType=geometry_type
            )

        else:
            # Create the popup for feature and make dict for add method
            popup = (
                popup
                if popup
                else popups.PopupInfo(
                    title=title,
                    fieldInfos=[
                        {
                            "fieldName": "OBJECTID",
                            "label": "OBJECTID",
                            "isEditable": True,
                            "visible": True,
                        }
                    ],
                    showAttachments=True,
                )
            )
            # Create the attributes for feature
            attributes = attributes if attributes else {"OBJECTID": 1}
            # Get the geometry type for feature set
            if "type" not in shape:
                if "rings" in shape:
                    shape["type"] = "polygon"
                elif "paths" in shape:
                    shape["type"] = "polyline"
                elif "points" in shape:
                    shape["type"] = "multipoint"
                else:
                    shape["type"] = "point"
            switcher = {
                "polygon": "esriGeometryPolygon",
                "polyline": "esriGeometryPolyline",
                "multipoint": "esriGeometryMultipoint",
                "point": "esriGeometryPoint",
                "envelope": "esriGeometryEnvelope",
            }
            geometry_type = switcher.get(shape["type"].lower())

            # Create symbol
            if symbol is None:
                symbol = self._create_default_symbol(geometry_type)
            fields = [
                {
                    "name": "OBJECTID",
                    "type": "esriFieldTypeOID",
                    "alias": "OBJECTID",
                    "sqlType": "sqlTypeOther",
                }
            ]
            # Create the Feature, Feature Set using pydantic
            feature = _models.Feature(
                geometry=dict(shape),
                attributes=attributes,
                popupInfo=popup.dict(),
                symbol=symbol.dict(),
            )
            feature_set = _models.FeatureSet(
                features=[feature], geometryType=geometry_type
            ).dict()

        # Create the layer definition
        ld = {
            "geometryType": geometry_type,
            "fields": fields,
            "objectIdField": "OBJECTID",
            "type": "Feature Layer",
            "spatialReference": {"wkid": 4326},
            "name": title,
            "maxScale": 0,
            "minScale": 0,
            "drawingInfo": {"renderer": {"type": "simple", "symbol": symbol.dict()}},
        }
        # Feature Collection will be made pydantic when added to map. Can leave as arcgis FeatureCollection now.
        feature_collection = features.FeatureCollection(
            {"featureSet": feature_set, "layerDefinition": ld}
        )

        # Add the feature collection as a layer to the map
        index = len(self.layers)
        self.add(
            item=feature_collection,
            popup_info=popup,
            index=index,
        )

    def _create_default_symbol(self, geometry_type):
        """Return default symbol based on geometry type"""
        if geometry_type == "esriGeometryPolyline":
            symbol = symbols.SimpleLineSymbolEsriSLS(
                color=[0, 0, 0, 255],
                width=1.33,
                type="esriSLS",
                style=symbols.SimpleLineSymbolStyle.esri_sls_solid.value,
            )
        elif geometry_type in [
            "esriGeometryPolygon",
            "esriGeometryEnvelope",
        ]:
            symbol = symbols.SimpleFillSymbolEsriSFS(
                color=[0, 0, 0, 64],
                outline=symbols.SimpleLineSymbolEsriSLS(
                    color=[0, 0, 0, 255],
                    width=1.33,
                    type="esriSLS",
                    style=symbols.SimpleLineSymbolStyle.esri_sls_solid.value,
                ),
                type="esriSFS",
                style=symbols.SimpleFillSymbolStyle.esri_sfs_solid.value,
            )
        elif geometry_type in [
            "esriGeometryPoint",
            "esriGeometryMultipoint",
        ]:
            symbol = symbols.SimpleMarkerSymbolEsriSMS(
                type="esriSMS",
                color=[226, 29, 145, 158],
                angle=0,
                xoffset=0,
                yoffset=0,
                size=12,
                style=symbols.SimpleMarkerSymbolStyle.esri_sms_circle.value,
                outline=symbols.SimpleLineSymbolEsriSLS(
                    type="esriSLS",
                    color=[0, 0, 0, 255],
                    width=0.75,
                    style=symbols.SimpleLineSymbolStyle.esri_sls_solid.value,
                ),
            )
        return symbol

    def move_to_basemap(self, index: int) -> bool:
        """
        Move a layer to be a basemap layer.
        A basemap layer is a layer that provides geographic context to the map.
        A web map always contains a basemap. The following is a list of possible basemap layer types:

        * Image Service Layer

        * Image Service Vector Layer

        * Map Service Layer

        * Tiled Image Service Layer

        * Tiled Map Service Layer

        * Vector Tile Layer

        * WMS Layer

        =====================       ===================================================================
        **Parameter**                **Definition**
        ---------------------       -------------------------------------------------------------------
        index                       Required integer. The index of the layer from operational layers that
                                    will be moved to basemap layers.
                                    The list of available layers is found when calling the `layers`
                                    property on the Map.
        =====================       ===================================================================

        .. code-block:: python

            # Create a Map from an existing Map Item.
            wm = Map(item=<webmap_item_id>)
            # Get and add the layer to the map
            vtl = gis.content.get("<vector tile layer id>")
            wm.content.add(vtl.layers[0])
            # Move the layer to the basemap
            wm.content.move_to_basemap(0)
            wm.update()
        """
        self._helper._move_to_basemap(index)
        # Refresh the widget
        self._source._update_source()
        return True

    def layer_visibility(self, index: int) -> LayerVisibility:
        """
        Get an instance of the LayerVisibility class for the layer specified. Specify the layer through it's
        position in the list of layers. The list of layers can be accessed with the `layers` property.
        Through this class you can edit the visibility for your layer on the map and in the legend.

        ==================      =====================================================================
        **Parameter**           **Description**
        ------------------      ---------------------------------------------------------------------
        index                   Required index specifying the layer who's visibility properties you want to access.
                                To get a full list of layers use the `layers` property.


                                .. note::
                                    If the layer belongs inside a group layer then use the instance of the
                                    Group layer class returned from the `layers` property to access the layers
                                    within the group and to use the `layer_visibility` method in that class.
        ==================      =====================================================================

        :return: LayerVisibility class for the layer specified.
        """
        # Check if group layer
        if self._source._webmap.operational_layers[index].layer_type.lower() in [
            "group",
            "group layer",
            "grouplayer",
        ]:
            return Exception(
                "The layer cannot be a group layer. Use the layer_visibility method in the GroupLayer."
            )
        # Return initialized instance of the class
        # Must pass in pydantic class to edit
        return LayerVisibility(self._source._webmap.operational_layers[index])

    def popup(self, index: int, is_table: bool = False) -> popups.PopupManager:
        """
        Get an instance of the PopupManager class for the layer specified. Specify the layer through it's
        position in the list of layers. The list of layers can be accessed with the `layers` property.
        Through this class you can edit the popup for your layer.

        ==================      =====================================================================
        **Parameter**           **Description**
        ------------------      ---------------------------------------------------------------------
        index                   Required index specifying the layer who's popup you want to access.
                                To get a full list of layers use the `layers` property.


                                .. note::
                                    If the layer belongs inside a group layer then use the instance of the
                                    Group layer class returned from the `layers` property to access the layers
                                    within the group and to use the popup method in that class.
        ==================      =====================================================================

        :return: PopupManager class for the layer specified. If popups are not supported for the layer, you will get an error.
        """
        layer = (
            self._source._webmap.operational_layers[index]
            if not is_table
            else self._source._webmap.tables[index]
        )

        # Check if group layer
        if not is_table and layer.layer_type.lower() in [
            "group",
            "group layer",
            "grouplayer",
            "subtypgrouplayer",
            "subtypegrouptable",
        ]:
            return Exception(
                "The layer cannot be a type of group layer. Use the popup method in the respective Group Layer class."
            )
        # Return initialized instance of the class
        # Must pass in pydantic class to edit
        if hasattr(layer, "popup_info"):
            return popups.PopupManager(layer=layer, source=self)
        else:
            raise ValueError("This layer type does not support popups.")

    def renderer(self, index) -> renderers.RendererManager:
        """
        Get an instance of the RendererManager class for the layer specified. Specify the layer through it's
        position in the list of layers. The list of layers can be accessed with the `layers` property.
        Through this class you can edit the renderer for your layer.

        ==================      =====================================================================
        **Parameter**           **Description**
        ------------------      ---------------------------------------------------------------------
        index                   Required index specifying the layer who's renderer you want to access.
                                To get a full list of layers use the `layers` property.


                                .. note::
                                    If the layer belongs inside a group layer then use the instance of the
                                    Group layer class returned from the `layers` property to access the layers
                                    within the group and to use the renderer method in that class.
        ==================      =====================================================================

        :return: RendererManager class for the layer specified.
        """
        # Check if group layer
        if self._source._webmap.operational_layers[index].layer_type.lower() in [
            "group",
            "group layer",
            "grouplayer",
            "subtypgrouplayer",
            "subtypegrouptable",
        ]:
            return Exception(
                "The layer cannot be a group layer. Use the renderer method in the GroupLayer."
            )
        # Return initialized instance of the class
        # Must pass in pydantic class to edit
        return renderers.RendererManager(
            layer=self._source._webmap.operational_layers[index],
            source=self._source,
        )

    def form(self, index: int) -> forms.FormInfo:
        """
        Get an instance of the FormInfo dataclass for the layer specified. Specify the layer through it's
        position in the list of layers. The list of layers can be accessed with the `layers` property.

        Through this class you can edit the form properties for your layer.

        Layer types that have forms include: Feature Layer, Table, and Oriented Imagery Layer.

        ==================      =====================================================================
        **Parameter**           **Description**
        ------------------      ---------------------------------------------------------------------
        index                   Required index specifying the layer who's form properties you want to access.
                                To get a full list of layers use the `layers` property.


                                .. note::
                                    If the layer belongs inside a group layer then use the instance of the
                                    Group layer class returned from the `layers` property to access the layers
                                    within the group and to use the `forms` method in that class.
        ==================      =====================================================================

        :return: FormInfo dataclass for the layer specified.
        To see this dataclass, see the forms module where it is defined. You can also get the dataclass as a dict
        by calling the `dict` method on the dataclass.

        """
        # Check if group layer
        if self._source._webmap.operational_layers[index].layer_type.lower() in [
            "group",
            "group layer",
            "grouplayer",
            "subtypgrouplayer",
            "subtypegrouptable",
        ]:
            return Exception(
                "The layer cannot be a group layer. Use the forms method in the GroupLayer."
            )
        # Return initialized instance of the class
        # We are placing it in our own dataclass so we can edit it
        form_info = self._source._webmap.operational_layers[index].form_info

        if form_info is None:
            return None
        else:
            return forms.FormInfo(**form_info.dict())

    def reposition(self, current_index: int, new_index: int) -> None:
        """
        Reposition a layer in the Map Content's layers. You can do this by specifying the index of the current
        layer you want to move and what index it should be at.

        This method is useful if you have overlapping layers and you want to manage the order in which
        they are rendered on your map.

        ==================      =====================================================================
        **Parameter**           **Description**
        ------------------      ---------------------------------------------------------------------
        current_index           Required int. The index of where the layer currently is in the list of layers.
                                You can see the list of layers by calling the `layers` property on your
                                Group Layer instance.

                                Must be a layer. Cannot be a table.
        ------------------      ---------------------------------------------------------------------
        new_index               Required int. The index you want to move the layer to.
        ==================      =====================================================================

        """
        # Need to reposition in the _webmap operational layers as well as the Map Content layers.
        # This is because the layers property is a list of the Map Content layers.
        # The _webmap operational layers is what is used to create the layers property.
        # We need to reposition in both so that the layers property is correct.
        # Reposition in the _webmap operational layers
        self._source._webmap.operational_layers.insert(
            new_index,
            self._source._webmap.operational_layers.pop(current_index),
        )

        # Reposition in the Map Content layers
        self.layers.insert(new_index, self.layers.pop(current_index))

        # Refresh the widget
        self._source._update_source()

    def move(self, index: int, group: group.GroupLayer) -> None:
        """
        Move a layer into an existing GroupLayer's list of layers.

        ==================      =====================================================================
        **Parameter**           **Description**
        ------------------      ---------------------------------------------------------------------
        index                   Required int. The index of the layer you want to move.
        ------------------      ---------------------------------------------------------------------
        group                   Required GroupLayer instance. The group layer you want to move the layer to.
        ==================      =====================================================================

        """
        # 1. Remove from MapContent layers
        layer = self.layers.pop(index)
        # 2. Add to the list of GroupLayer layers
        group.layers.append(layer)
        # 3. Remove from _webmap operational layers (pydantic class)
        pydantic_layer = self._source._webmap.operational_layers.pop(index)
        # 4. Add to the GroupLayer._layer.layers (this points to the correct place in the webmap pydantic class)
        group._layer.layers.append(pydantic_layer)
        # 5. Refresh the widget
        self._source._update_source()

    def reposition_to_top(self, index: int) -> None:
        """
        Reposition a layer to the top of the list of layers. This will make the layer render on top of all other layers.

        ==================      =====================================================================
        **Parameter**           **Description**
        ------------------      ---------------------------------------------------------------------
        index                   Required int. The index of the layer you want to reposition to the top of the list.
        ==================      =====================================================================
        """
        # 1. Remove from MapContent layers
        layer = self.layers.pop(index)
        # 2. Add to the top of the MapContent layers, this means last in the list
        self.layers.append(layer)
        # 3. Remove from _webmap operational layers (pydantic class)
        op_layer = self._source._webmap.operational_layers.pop(index)
        # 4. Add to the top of the _webmap operational layers (pydantic class), this means last in the list
        self._source._webmap.operational_layers.append(op_layer)
        # 5. Refresh the widget
        self._source._update_source()
