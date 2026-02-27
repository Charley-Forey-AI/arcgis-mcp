from __future__ import annotations
import json
from arcgis.auth.tools import LazyLoader
from traitlets import Int, Unicode, Dict, Bool, observe
from typing import Optional, Any
from arcgis.map._utils import (
    _HelperMethods,
    refreshable_property,
    _DEFAULT_SPATIAL_REFERENCE,
)
import anywidget
import pathlib

arcgis = LazyLoader("arcgis")
basemapdef3d = LazyLoader("arcgis.map.definitions._3d_basemap_definitions")
features = LazyLoader("arcgis.features")
popups = LazyLoader("arcgis.map.popups")
renderer = LazyLoader("arcgis.map.renderers")
uuid = LazyLoader("uuid")
map_classes = LazyLoader("arcgis.map.map_widget")
arcgis_layers = LazyLoader("arcgis.layers")
_models = LazyLoader("arcgis.map.dataclasses.models")


class Scene(anywidget.AnyWidget):
    """
    The Scene can be displayed in a Jupyter Lab environment.

    The commands will be sent from the Scene class and all the methods in this class
    will affect what will be seen on the scene. The underlying web scene dictionary will be transformed in the
    Scene class.

    A Scene contains layers of geographic data that are displayed in 3D. Scenes allow you to display
    real-world visualizations of landscapes, objects, buildings, and other 3D objects.
    """

    # Connected to typescript code
    _esm = pathlib.Path(__file__).parent / "static" / "scene_widget.js"
    _css = pathlib.Path(__file__).parent / "static" / "scene_widget.css"
    # CDN for Enterprise Only
    js_api_path = Unicode("").tag(sync=True)
    _portal_token = Unicode("").tag(sync=True)
    _auth_mode = Unicode("").tag(sync=True)
    _portal_rest_url = Unicode("").tag(sync=True)
    _proxy_rule = Dict({}).tag(sync=True)
    _username = Unicode("").tag(sync=True)
    _show_legend = Bool(False).tag(sync=True)
    _show_layer_list = Bool(False).tag(sync=True)
    _show_weather = Bool(False).tag(sync=True)
    _show_daylight = Bool(False).tag(sync=True)
    _webscene_dict = Dict({}).tag(sync=True)
    # Time Slider Portion
    _show_time_slider = Bool(False).tag(sync=True)
    _time_slider_layer = Int().tag(sync=True)
    _time_slider_full_extent = Dict({}).tag(sync=True)
    _time_slider_time_interval = Dict({}).tag(sync=True)
    ###
    # Only for python code
    _linked_maps = []

    _view_state = Dict({}).tag(sync=True)

    @observe("_view_state")
    def _on_view_state_change(self, change):
        # This method will be called whenever the 'view_state' trait changes
        # Check the Initial State of the webmap. If the target geometry in the viewpoint is different, update it.
        # Don't update the webmap dict since we are dealing with a traitlets change
        new_camera = (
            _models.Camera(**change["new"].get("camera"))
            if change["new"].get("camera")
            else {}
        )
        old_camera = (
            self._webscene.initial_state.dict().get("viewpoint", {}).get("camera")
        )
        if not new_camera or json.dumps(old_camera, sort_keys=True) == json.dumps(
            new_camera.dict(), sort_keys=True
        ):
            # No change in camera, do nothing
            return
        if self._webscene.initial_state:
            self._webscene.initial_state.viewpoint.camera = new_camera
        else:
            self._webscene.initial_state = _models.SceneInitialState(
                viewpoint=_models.SceneViewpoint(camera=new_camera)
            )

    #####
    # Traitlets (properties) that can be set by user. These look a bit different in the documentation.
    # They act the same as if you put a property decorator over a method. They are linked to js side for widget use.
    theme = Unicode("light").tag(sync=True)
    """
    Get/Set the widget theme when displaying in a Jupyter environment.

    Values can be "light" or "dark"
    """

    @property
    def camera(self):
        """
        Get/Set the camera of the rendered Scene.

        ==================      ====================================================================
        **Parameter**           **Description**
        ------------------      --------------------------------------------------------------------
        camera                  A dictionary that represents the JSON of the scene
                                widget's camera. The dictionary included the keys: "position", "heading", and "tilt"
                                The position indicates the x, y, and z coordinates for the camera. The heading is
                                the heading of the camera in degrees. The tilt is the degrees with respect to the
                                surface as projected down.

                                Example:
                                scene.camera = {
                                    "position":{
                                        'x': -5938587.158752469,
                                        'y': -2827970.414173906,
                                        'z': 46809.31127560418
                                    },
                                    "heading": 400,
                                    "tilt": 0.5
                                }
        ==================      ====================================================================

        :return: A dictionary that represents the x,y, and z of the rendered Scene.

        .. code-block:: python

            # Usage example
            scene.camera = {
                "position":{
                    'x': -5938587.158752469,
                    'y': -2827970.414173906,
                    'z': 46809.31127560418
                },
                "heading": 400,
                "tilt": 0.5
            }

        """
        return (
            self._view_state.get("camera")
            or self._webscene.initial_state.viewpoint.camera.dict()
        )

    @camera.setter
    def camera(self, camera):
        if "position" not in camera:
            raise ValueError("Camera must have a position key.")
        if "heading" not in camera:
            raise ValueError("Camera must have a heading key.")
        if "tilt" not in camera:
            raise ValueError("Camera must have a tilt key.")
        self._view_state = {"camera": camera}

    @property
    def extent(self):
        """
        Get/Set the rendered map's extent.

        .. note::
            Must provide a spatial reference key in the extent dictionary.

        ==================      ====================================================================
        **Parameter**           **Description**
        ------------------      --------------------------------------------------------------------
        value                   Required dict.
                                map.extent = {
                                    'spatialReference': {'latestWkid': 3857, 'wkid': 102100},
                                    'xmax': 5395723.072123609,
                                    'xmin': -137094.78326911852,
                                    'ymax': 10970767.576996306,
                                    'ymin': 7336034.007980572
                                }
        ==================      ====================================================================

        :return: A dictionary representing the extent

        """
        extent = self._view_state.get("extent")
        if not extent and self._webscene.initial_state.viewpoint.target_geometry:
            extent = self._webscene.initial_state.viewpoint.target_geometry.dict()
        extent = extent or {}
        return extent

    @extent.setter
    def extent(self, extent):
        if extent and "spatialReference" not in extent:
            raise ValueError("Extent must have a spatial reference key.")
        self._view_state = {"extent": extent}

    @property
    def center(self):
        """
        Get/Set the center of the rendered Scene.

        ==================     ====================================================================
        **Parameter**           **Description**
        ------------------     --------------------------------------------------------------------
        center                 A `[lat, long]` list that represents the JSON of the scene
                                widget's center.
        ==================     ====================================================================

        :return: A list that represents the latitude and longitude of the scene's center.

        .. code-block:: python

            # Usage example
            scene.center = [34.05, -118.24]

        """
        return self._view_state.get("center", [0, 0])

    @center.setter
    def center(self, center):
        if not isinstance(center, list) or len(center) != 2:
            raise ValueError("Center must be a list of [lat, long].")
        self._view_state = {"center": center}

    @property
    def zoom(self):
        """
        Get/Set the level of zoom applied to the rendered Scene.

        ==================     ====================================================================
        **Parameter**           **Description**
        ------------------     --------------------------------------------------------------------
        value                   Required int.
                                .. note::
                                    The higher the number, the more zoomed in you are.
        ==================     ====================================================================

        :return: Int value that represents the zoom level

        .. code-block:: python

            # Usage example
            scene.zoom = 10

        """
        return self._view_state.get("zoom", -1)

    @zoom.setter
    def zoom(self, zoom):
        if not isinstance(zoom, int):
            raise ValueError("Zoom must be an integer.")
        self._view_state = {"zoom": zoom}

    @property
    def scale(self):
        """
        Get/Set the scene scale at the center of the rendered Scene. If set to X, the scale
        of the scene would be 1:X.
        """
        return self._view_state.get("scale", -1)

    @scale.setter
    def scale(self, scale):
        if not isinstance(scale, (int, float)):
            raise ValueError("Scale must be a number.")
        self._view_state = {"scale": scale}

    #####

    def __init__(
        self,
        location: str | None = None,
        *,
        item: arcgis.gis.Item | str | None = None,
        gis: arcgis.gis.GIS | None = None,
        **kwargs,
    ) -> None:
        """
        Create a visual scene in jupyter labs.

        ==================      =====================================================================
        **Parameter**           **Description**
        ------------------      ---------------------------------------------------------------------
        location                Optional string. The address where the scene is to be centered.
        ------------------      ---------------------------------------------------------------------
        item                    Optional webscene item to initiate the scene with.
        ------------------      ---------------------------------------------------------------------
        gis                     Optional GIS object. The GIS object to use for the scene. If None then
                                the active GIS will be used.
        ==================      =====================================================================
        """
        # Initialize
        super().__init__(**kwargs)

        # Fix parameter if someone passed item first
        if isinstance(location, arcgis.gis.Item):
            item = location
            location = None

        # Set up the helper method class
        self._helper = _HelperMethods(self)

        # Set up GIS
        self._helper._setup_gis_properties(gis)

        # Set up the scene that will be used
        # either existing or new instance
        self._setup_webscene_properties(item)

        # Assign the definition to helper class
        self._helper._set_widget_definition(self._webscene)

        # Set up the widget (scene that is rendered)
        # this sets camera, location, etc.
        geocoder = kwargs.pop("geocoder", None)
        self._helper._setup_location_properties(location, geocoder)

        # Set up basemap and content managers
        self._content: SceneContent = None
        self._basemap_manager: map_classes.BasemapManager = None

    def _setup_webscene_properties(self, item):
        """
        Set up the webscene property to be used. This can either be from an
        existing 'Web Scene' item or it can be a new webscene. A pydantic webscene instance
        will be created in either case and this is what we will use to make edits and save.
        """
        # Set up the webscene dictionary
        if item:
            # Existing Scene was passed in
            if isinstance(item, str):
                # Get Item from itemid
                item = self._gis.content.get(item)
                if item is None:
                    # No item found with associated gis
                    raise ValueError("No item was found corresponding to this item id.")
            if item.type.lower() != "web scene":
                # Has to be a pre-existing item of type webscene
                raise TypeError("Item must be of type Map or Web Scene.")

            # Keep track of the item
            self.item = item
            # Set up webscene data
            data = self.item.get_data()
            if "version" in data:
                del data["version"]  # webscene spec will update this

            # Use pydantic dataclass from webscene spec generation
            self._webscene = _models.Webscene(**data)
        else:
            # New Scene
            self.item = None
            # Set default spatial reference
            spatial_reference = _models.SpatialReference(latestWkid=3857, wkid=102100)
            # Create pydantic dataclass with generic default
            basemap = _models.SceneBasemap(**basemapdef3d.basemap_dict["topo-3d"])
            # If authenticated check if default basemap to use (first see if 3d set, otherwise use 2d)
            if self._gis._is_authenticated:
                if self._gis.properties.get("default3DBasemapQuery"):
                    bid = self._gis.properties.get("default3DBasemapQuery")
                    if bid is not None and "id:" in bid:
                        bid = bid.split("id:")[1]
                    # get webscene item
                    webscene_item = self._gis.content.get(bid)
                    if webscene_item is not None:
                        basemap = _models.SceneBasemap(
                            **webscene_item.get_data().get("baseMap", {})
                        )
                    else:
                        # Fall back to default basemap if webscene_item is not found
                        # basemap is already set to default above, so we can optionally log or pass
                        pass
                elif self._gis.properties.get("defaultBasemap"):
                    # Find the org's default basemap
                    for idx, dbmap in enumerate(
                        self._gis.properties.get("defaultBasemap", {}).get(
                            "baseMapLayers", []
                        )
                    ):
                        # Account for Enterprise basemaps not having title
                        if "title" not in dbmap:
                            self._gis.properties["defaultBasemap"]["baseMapLayers"][
                                idx
                            ]["title"] = str(uuid.uuid4())[0:7]
                    # Create pydantic dataclass with basemap
                    basemap = _models.SceneBasemap(
                        **self._gis.properties["defaultBasemap"]
                    )

            # Get the ground property
            ground = _models.Ground(
                layers=[
                    {
                        "id": "globalElevation",
                        "layerType": "ArcGISTiledElevationServiceLayer",
                        "listMode": "hide",
                        "title": "Terrain3D",
                        "url": "https://elevation3d.arcgis.com/arcgis/rest/services/WorldElevation3D/Terrain3D/ImageServer",
                        "visibility": True,
                    }
                ],
                navigationConstraint={"type": "stayAbove"},
                transparency=0,
            )
            # Create Environment
            environment = _models.Environment(
                weather=_models.SunnyWeather(),
            )
            # Create Camera, this will be updated JS side
            # need to set to adhere webscene spec
            camera = _models.Camera(
                tilt=0.4,
                position={
                    "spatialReference": {"latestWkid": 3857, "wkid": 102100},
                    "x": -13021993,
                    "y": 3992143,
                    "z": 300000,
                },
                heading=0.0,
            )

            # Create Initial State
            initial_state = _models.SceneInitialState(
                environment=environment, viewpoint=_models.SceneViewpoint(camera=camera)
            )
            # New Webscene from pydantic dataclass from webscene spec generation
            self._webscene = _models.Webscene(
                operationalLayers=[],
                baseMap=basemap,
                ground=ground,
                viewingMode="global",
                spatialReference=spatial_reference,
                authoringApp="ArcGISPythonAPI",
                authoringAppVersion=str(arcgis.__version__),
                initialState=initial_state,
            )

    def _repr_mimebundle_(self, include=None, exclude=None, **kwargs):
        # Refresh dict incase changes made before rendering
        self._webscene_dict = self._webscene.dict()
        # Render the widget
        return super(Scene, self)._repr_mimebundle_(
            include=include, exclude=exclude, **kwargs
        )

    def _update_source(self):
        self._webscene_dict = self._webscene.dict()

    def js_requirement(self):
        """
        Return the JS API version needed to work with the current version of the mapping module in a disconnected environment.

        :return: A string representing the JS API version.
        """
        return self._helper._js_requirement()

    @property
    def tilt(self) -> float:
        """
        Get/Set the tilt of the rendered Scene.
        The tilt of the camera in degrees with respect to the surface as projected down
        from the camera position. Tilt is zero when looking straight down at the surface
        and 90 degrees when the camera is looking parallel to the surface.

        ==================      ====================================================================
        **Parameter**           **Description**
        ------------------      --------------------------------------------------------------------
        tilt                    A float that represents the tilt.
        ==================      ====================================================================

        :return: A float that represents the tilt value.
        """
        if self.camera:
            return self.camera["tilt"]
        else:
            raise ValueError("This value is only available when a camera is set.")

    @tilt.setter
    def tilt(self, value):
        if isinstance(value, int):
            # Create new dict of camera
            camera = {**self.camera}
            # Change the tilt
            camera["tilt"] = value
            # Reassign to provoke change in widget
            self.camera = camera

    @property
    def heading(self) -> float:
        """
        Get/Set the heading of the camera in degrees. Heading is zero when north is the top of the screen.
        It increases as the view rotates clockwise.

        ==================      ====================================================================
        **Parameter**           **Description**
        ------------------      --------------------------------------------------------------------
        heading                 A float that represents the heading in degrees.
        ==================      ====================================================================
        """
        if self.camera:
            return self.camera["heading"]
        else:
            raise ValueError("This value is only available when a camera is set.")

    @heading.setter
    def heading(self, value):
        if isinstance(value, int):
            # Create new dict of camera
            camera = {**self.camera}
            # Change the tilt
            camera["heading"] = value
            # Reassign to provoke change in widget
            self.camera = camera

    @property
    def viewing_mode(self) -> str:
        """
        Get/Set the viewing mode.

        The viewing mode (local or global). Global scenes render the earth as a sphere. Local scenes
        render the earth on a flat plane and allow for navigation and feature display in a localized
        or clipped area. Users may also navigate the camera of a local scene below the surface of a basemap.

        * "global" : Global scenes allow the entire globe to render in the view, showing the curvature of the earth.
        * "local" : Local scenes render the earth on a flat surface. They can be constrained to only show a "local"
        area by setting the clippingArea property. Local scenes also allow for displaying and exploring data that
        would otherwise be hidden by the surface of the earth.

        ==================      ====================================================================
        **Parameter**           **Description**
        ------------------      --------------------------------------------------------------------
        mode                    A string that represents the mode. Either "global" or "local". Default
                                is "global".
        ==================      ====================================================================

        :return: String that represents the viewing mode.
        """
        return self._webscene.dict()["viewingMode"]

    @viewing_mode.setter
    def viewing_mode(self, mode):
        # only change mode if not already set
        if mode != self.viewing_mode and mode in ["global", "local"]:
            self._webscene.viewing_mode = mode
            self._update_source()

    @property
    def basemap(self) -> map_classes.BasemapManager:
        """
        Returns a BasemapManager object that can be used to handle
        basemap related properties and methods on the Scene.
        """
        if self._basemap_manager is None:
            self._basemap_manager = map_classes.BasemapManager(self)
        return self._basemap_manager

    @property
    def content(self) -> SceneContent:
        """
        Returns a SceneContent object that can be used to access the layers and tables
        in the scene. This is useful for adding, updating, getting, and removing content
        from the Scene.
        """
        if self._content is None:
            self._content = SceneContent(self)
        return self._content

    @property
    def legend(self) -> map_classes.Legend:
        """
        Get an instance of the Legend class. You can use this class to enable or disable
        the legend widget. Best used inside of Jupyter Lab.
        """
        return map_classes.Legend(self)

    @property
    def layer_list(self) -> map_classes.LayerList:
        """
        Get an instance of the LayerList class. You can use this class to enable or disable
        the layer list widget. Best used inside of Jupyter Lab.
        """
        return map_classes.LayerList(self)

    @property
    def time_slider(self) -> map_classes.TimeSlider:
        """
        Get an instance of the TimeSlider class. You can use this class to enable or disable the
        time slider on the widget. Best used inside of Jupyter Lab
        """
        return map_classes.TimeSlider(self)

    @property
    def environment(self) -> Environment:
        """
        Get an instance of the Environment class. You can use this class to enable or disable
        widgets such as weather and daylight. You can also set these properties using python code.
        """
        return Environment(self)

    def save(
        self,
        item_properties: dict[str, Any],
        thumbnail: Optional[str] = None,
        metadata: Optional[str] = None,
        owner: Optional[str] = None,
        folder: Optional[str] = None,
    ) -> arcgis.gis.Item:
        """
        Saves the ``Scene`` object as a new Web Scene Item in your :class:`~arcgis.gis.GIS`.

        .. note::
            If you started with a ``Scene`` object from an existing web scene item,
            calling this method will create a new item with your changes. If you want to
            update the existing ``Scene`` item found in your portal with your changes, call the
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
        folder              Optional string. Name of the folder into which the web scene should be
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
            :class:`~arcgis.gis.Item` object corresponding to the new web scene Item created.

            # save the web scene
            webscene_item_properties = {'title':'Ebola incidents and facilities',
                         'snippet':'Scene created using Python API showing locations of Ebola treatment centers',
                         'tags':['automation', 'ebola', 'world health', 'python'],
                         'extent': {'xmin': -122.68, 'ymin': 45.53, 'xmax': -122.45, 'ymax': 45.6, 'spatialReference': {'wkid': 4326}}}

            new_ws_item = webscene.save(webscene_item_properties, thumbnail='./webscene_thumbnail.png')
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
        The ``update`` method updates the Web Scene Item in your :class:`~arcgis.gis.GIS`
        with the changes you made to the ``Scene`` object. In addition, you can update
        other item properties, thumbnail and metadata.

        .. note::
            If you started with a ``Scene`` object from an existing web scene item, calling this method will update the item
            with your changes.

            If you started out with a fresh Scene object (without a web scene item), calling this method will raise a
            RuntimeError exception. If you want to save the Scene object into a new web scene item, call the
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
        item: (
            arcgis.gis.Item
            | features.FeatureSet
            | features.FeatureCollection
            | features.Layer
            | features.DataFrame
            | str
        ),
    ) -> None:
        """
        The ``zoom_to_layer`` method snaps the scene to the extent of the provided :class:`~arcgis.gis.Item` object(s).

        ==================     ====================================================================
        **Parameter**           **Description**
        ------------------     --------------------------------------------------------------------
        item                   The item at which you want to zoom your scene to.
                               This can be a single extent or an :class:`~arcgis.gis.Item`, ``Layer`` , ``DataFrame`` ,
                               :class:`~arcgis.features.FeatureSet`, or :class:`~arcgis.features.FeatureCollection`
                               object.
        ==================     ====================================================================
        """
        self._helper._zoom_to_layer(item)

    def sync_navigation(self, scene: Scene | list[Scene]) -> None:
        """
        The ``sync_navigation`` method synchronizes the navigation from one rendered Scene to
        another rendered Scene instance so panning/zooming/navigating in one will update the other.

        .. note::
            Users can sync more than two instances together by passing in a list of Scene instances to
            sync. The syncing will be remembered

        ==================      ===================================================================
        **Parameter**           **Description**
        ------------------      -------------------------------------------------------------------
        scene                   Either a single Scene instance, or a list of ``Scene``
                                instances to synchronize to.
        ==================      ===================================================================

        """
        self._helper._sync_navigation(scene)

    def unsync_navigation(self, scene: Optional[Scene | list[Scene]] = None) -> None:
        """
        The ``unsync_navigation`` method unsynchronizes connections made to other rendered Scene instances
        made via the sync_navigation method.

        ==================     ===================================================================
        **Parameter**           **Description**
        ------------------     -------------------------------------------------------------------
        scene                  Optional, either a single `Scene` instance, or a list of
                               `Scene` instances to unsynchronize. If not specified, will
                               unsynchronize all synced `Scene` instances.
        ==================     ===================================================================
        """
        self._helper._unsync_navigation(scene)


class Environment:
    """
    A class that can be used to enable the environment widgets such as daylight and weather on a rendered Scene. This class is most useful when used
    inside a Jupyter Lab environment.


    .. note::
        This class should now be created by a user but rather called through the `environment` property on
        a Scene instance.
    """

    def __init__(self, scene: Scene) -> None:
        self._source = scene

    def __str__(self) -> str:
        return "Environment"

    def __repr__(self) -> str:
        return "Environment()"

    def __setattr__(self, name, value):
        # Allow private attributes and existing properties
        if name.startswith("_") or hasattr(self.__class__, name):
            super().__setattr__(name, value)
        else:
            raise AttributeError(
                f"'{self.__class__.__name__}' object has no attribute '{name}'"
            )

    @property
    def daylight_enabled(self) -> bool:
        """
        The Daylight widget can be used to manipulate the lighting conditions of a Scene. For this, the widget modifies the
        lighting property of environment. To illuminate the scene, one can either use a configuration of date and time
        to position the sun or switch to the virtual mode, where the light source is relative to the camera.
        """
        return self._source._show_daylight

    @daylight_enabled.setter
    def daylight_enabled(self, value):
        if isinstance(value, bool):
            if value != self._source._show_daylight:
                # only set if it changed
                self._source._show_daylight = value

    @property
    def weather_enabled(self) -> bool:
        """
        The Daylight widget can be used to manipulate the lighting conditions of a Scene. For this, the widget modifies the
        lighting property of environment. To illuminate the scene, one can either use a configuration of date and time
        to position the sun or switch to the virtual mode, where the light source is relative to the camera.
        """
        return self._source._show_weather

    @weather_enabled.setter
    def weather_enabled(self, value):
        if isinstance(value, bool):
            if value != self._source._show_weather:
                # only set if it changed
                self._source._show_weather = value


class SceneContent:
    """
    This class allows users to manage the content of a Scene. Users can view the operational layers,
    add layers, reorder layers, and remove layers.

    This class is created from the `scene_content` property on a Scene instance.
    """

    layers: list = None
    tables: list = None

    def __init__(self, scene: Scene) -> None:
        self._source = scene
        self._helper = scene._helper

    def __str__(self) -> str:
        return "Scene Content"

    def __repr__(self) -> str:
        return "SceneContent()"

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

    def layer_info(self):
        """
        Get the layers in the scene.

        :return: A list of dictionaries representing the layers in the scene.
        """
        return self._helper._layer_info()

    def get_layer(self, title: str) -> arcgis.gis.Layer:
        """
        Get a layer or table from the scene by title. If the layer is added more than once only the
        first instance of the layer will be returned.

        ..note::
            To find a layer by index you can use the `layer_info` method to see the layers in a table form.

        ==================      =====================================================================
        **Parameter**           **Description**
        ------------------      ---------------------------------------------------------------------
        title                   Required string. The title of the layer to get.
        ==================      ===================================================================

        :return: A layer or table from the scene.
        """
        return self._helper._get_layer(title)

    def add(
        self,
        item: arcgis.gis.Item | arcgis.gis.Layer,
        drawing_info: dict | None = None,
        popup_info: dict | None = None,
        index: int | None = None,
    ) -> None:
        """
        Add a layer or table to the scene.

        ==================      =====================================================================
        **Parameter**           **Description**
        ------------------      ---------------------------------------------------------------------
        item                    Required Portal Item or Layer to add to the scene.

                                    * If an item is passed in: Creates a new layer instance of the
                                    appropriate layer class from an ArcGIS Online or ArcGIS Enterprise portal
                                    item.
                                    * If the item points to a feature service with multiple layers,
                                    then a GroupLayer is created.
                                    * If the item points to a service with a single
                                    layer, then it resolves to a layer of the same type of class as the service.
        ------------------      ---------------------------------------------------------------------
        drawing_info            Optional dictionary representing the drawing info to apply to the layer.
                                The keys can include any combination of: 'renderer', 'labelingInfo', 'scaleSymbols',
                                'showLabels', 'transparency'. This can only be applied when adding one layer.

                                **NOTICE**: The scene viewer only accepts 3D Symbols. Please see: `3D Symbols <https://developers.arcgis.com/javascript/latest/api-reference/esri-symbols-Symbol3D.html>`_ for more information.

                                Example Structure:
                                drawing_info = {
                                    "labelingInfo": <list>,
                                    "renderer": <dict>,
                                    "scaleSymbols": <bool>,
                                    "showLabels": <bool>,
                                    "transparency": <float>
                                }
        ------------------      ---------------------------------------------------------------------
        popup_info              Optional dictionary representing the popup info for the feature layer.
                                This can only be applied when adding one layer.

                                Example:
                                popup_info = {
                                    "popupElements": <list>,
                                    "showAttachments": <bool>,
                                    "fieldInfos": <list>,
                                    "title": <str>
                                }
        ------------------      ---------------------------------------------------------------------
        index                   Optional integer. Determines at what index the layer should be added.
        ==================      =====================================================================
        """
        # Set the index
        index = index if index is not None else len(self.layers)
        # Set the drawing info default
        if drawing_info is None:
            drawing_info = {}
        if popup_info is None:
            popup_info = {}
        # Declare layer param that will be found in next steps
        layer = None

        if isinstance(item, str):
            # If url, pass to Service class
            if item.startswith("http"):
                item = arcgis_layers.Service(item)
            else:
                raise Exception("Invalid item format.")
        # Section: Item or Layer Item
        if isinstance(item, arcgis.gis.Item):
            # User provided instance of Item
            if len(item.layers) == 1:
                # Only one layer, create the layer and add
                self._helper._create_layer_from_service(
                    item.layers[0], drawing_info, popup_info
                )
                if isinstance(layer, _models.Table):
                    self.tables.insert(index, item)
                else:
                    self.layers.insert(index, item.layers[0])
            elif len(item.layers) > 1:
                # Need to create a group layer and then add, this will be done in the method
                self._helper._create_layer_from_service(item)
                # Add layer to webscene, this needs to be called before the insert for group layers
                self._source._webscene.operational_layers.insert(index, layer)
                # Insert the group layer into the layers
                self.layers.insert(
                    index, self._helper._infer_layer(layer.dict(), index)
                )
                # Refresh the widget to have latest webscene dict
                self._source._update_source()
                return
        else:
            # Only one layer, create the layer and add
            self._helper._create_layer_from_service(item, drawing_info, popup_info)
            if isinstance(layer, _models.Table):
                self.tables.insert(index, item)
            else:
                self.layers.insert(index, item)

        # Refresh the widget to have latest webscene dict
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
    ) -> None:
        """
        This method can be used to update certain properties on a layer that is in your scene.

        ==================      =====================================================================
        **Parameter**           **Description**
        ------------------      ---------------------------------------------------------------------
        index                   Optional integer specifying the index for the layer you want to update.
                                To see a list of layers use the layers property. This cannot be a group layer.
                                To update a group layer, use the `update_layer` method in the group layer class.
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
                                in screen units regardless of the scene scale.
        ------------------      ---------------------------------------------------------------------
        transparency            Optional int. Value ranging between 0 (no transparency) to 100 (completely transparent).
        ==================      =====================================================================
        """
        self._helper.update_layer(
            index, labeling_info, renderer, scale_symbols, transparency
        )

        SceneContent.layers.clear(self)

    def remove(self, index: int | None = None, is_layer: bool = True) -> bool:
        """
        Remove a layer from the scene either by specifying the index or passing in the layer dictionary.

        ==================      =====================================================================
        **Parameter**           **Description**
        ------------------      ---------------------------------------------------------------------
        layer                   Optional layer object from the list of layers found through the layers
                                property that you want to remove from the scene. This will remove the first
                                instance of this layer if you have it more than once on the scene.
                                This parameter cannot be passed with the index.
        ------------------      ---------------------------------------------------------------------
        index                   Optional integer specifying the index for the layer you want to remove.
                                To see a list of layers use the layers property.
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
        Remove all layers and tables from the scene.
        """
        self._helper.remove_all()
        # Refresh the widget
        self._source._update_source()
        return True

    def move_to_basemap(self, index: int) -> None:
        """
        Move a layer to be a basemap layer.
        A basemap layer is a layer that provides geographic context to the scene.
        A web scene always contains a basemap. The following is a list of possible basemap layer types:

        * Image Service Layer

        * Image Service Vector Layer

        * Map Service Layer

        * Tiled Image Service Layer

        * Tiled Map Service Layer

        * Vector Tile Layer

        =====================       ===================================================================
        **Parameter**                **Definition**
        ---------------------       -------------------------------------------------------------------
        index                       Required integer. The index of the layer from operational layers that
                                    will be moved to basemap layers.
                                    The list of available layers is found when calling the `layers`
                                    property on the Scene.
        =====================       ===================================================================

        .. code-block:: python

            # Create a Scene from an existing Scene Item.
            ws = Scene(item=<webscene_item_id>)
            # Get and add the layer to the scene
            vtl = gis.content.get("<vector tile layer id>")
            ws.content.add(vtl.layers[0])
            # Move the layer to the basemap
            ws.move_to_basemap(0)
            ws.update()
        """
        self._helper._move_to_basemap(index)
        # Refresh the widget
        self._source._update_source()

    def layer_visibility(self, index: int) -> map_classes.LayerVisibility:
        """
        Get an instance of the LayerVisibility class for the layer specified. Specify the layer through it's
        position in the list of layers. The list of layers can be accessed with the `layers` property.
        Through this class you can edit the visibility for your layer on the scene and in the legend.

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
        if self._source._webscene.operational_layers[index].layer_type.lower() in [
            "group",
            "group layer",
            "grouplayer",
        ]:
            return Exception(
                "The layer cannot be a group layer. Use the layer_visibility method in the GroupLayer."
            )
        # Return initialized instance of the class
        # Must pass in pydantic class to edit
        return map_classes.LayerVisibility(
            self._source._webscene.operational_layers[index]
        )

    def popup(self, index: int) -> popups.PopupManager:
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

        :return: PopupManager class for the layer specified.
        """
        layer = self._source._webscene.operational_layers[index]
        # Check if group layer
        if layer.layer_type.lower() in [
            "group",
            "group layer",
            "grouplayer",
        ]:
            return Exception(
                "The layer cannot be a group layer. Use the popup method in the GroupLayer."
            )
        # Return initialized instance of the class
        # Must pass in pydantic class to edit
        if hasattr(layer, "popup_info"):
            return popups.PopupManager(layer=layer, source=self)
        else:
            raise ValueError("This layer type does not support popups.")
