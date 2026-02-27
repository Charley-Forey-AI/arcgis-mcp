from __future__ import annotations  # Enables postponed evaluation of type hints

from .base_model import BaseModel, common_config
from pydantic import Field, confloat, constr, conint, field_validator
from .geometry import Extent, SpatialReference, PointGeometry, PolygonGeometry
from typing import Literal, Any
from .layers import (
    AnnotationLayer,
    CatalogLayer,
    CSVLayer,
    DimensionLayer,
    GeoJSONLayer,
    FeatureLayer,
    GeoRSSLayer,
    GroupLayer,
    ImageServiceLayer,
    ImageServiceVectorLayer,
    KMLLayer,
    KnowledgeGraphLayer,
    MapServiceLayer,
    OGCFeatureLayer,
    Object3DTilesLayer,
    OrientedImageryLayer,
    StreamLayer,
    SubtypeGroupLayer,
    TiledImageServiceLayer,
    TiledMapServiceLayer,
    VectorTileLayer,
    VideoLayer,
    WCSLayer,
    WebTiledLayer,
    WFSLayer,
    WMSLayer,
    MediaLayer,
    SubtypeGroupTable,
    Table,
    BingLayer,
    OpenStreetMapLayer,
    EditableLayers,
    ConnectedOnlineLayer,
    FacilityLayer,
    LevelLayer,
    SiteLayer,
    BuildingSceneLayer,
    VoxelLayer,
    IntegratedMeshLayer,
    LineOfSightLayer,
    RasterDataLayer,
    IntegratedMesh3DTilesLayer,
    PointCloudLayer,
    SceneLayer,
    TiledElevationLayer,
    RasterDataElevationLayer,
    ReadonlyLayers,
    ViewshedLayer,
)
from ..enums.mapping import (
    EnterExitRule,
    FeedAccuracyMode,
    InteractionMode,
    FenceNotificationRule,
    SlideLayout,
    SnowCover,
    ViewingMode,
    HeightModel,
    HeightUnit,
    NavigationType,
)
from .renderers import ExpressionInfo
from ..enums.layer_definition import TimeIntervalUnits
from .layer_definition import FeatureFilter, FieldModel
from .popups import SlidePopupInfo, LayerReference


class MapRangeInformation(BaseModel):
    """
    Map range information
    """

    model_config = common_config
    active_range_name: str = Field(
        ...,
        alias="activeRangeName",
        description="Active range ID that slider/picker acts upon.",
    )
    current_range_extent: list[float] | None = Field(
        None,
        alias="currentRangeExtent",
        description="Current range for the active range.",
        max_length=2,
        min_length=2,
    )
    full_range_extent: list[float] | None = Field(
        None,
        alias="fullRangeExtent",
        description="Full range extent for the active range to be presented in the UI.",
        max_length=2,
        min_length=2,
    )


class ParcelFabric(BaseModel):
    """
    Identifies the central object for parcel fabric schema information to access parcel fabric-related functionality, such as managing parcel records.
    """

    model_config = common_config
    id: str | None = Field(
        None,
        description="A unique identifying string for the parcel fabric.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the parcel fabric layer that can be used in a table of contents.",
    )
    url: str = Field(
        ...,
        description="A string value indicating the URL to the parcel fabric layer.",
    )


class Thumbnail(BaseModel):
    """
    Object containing a thumbnail image.
    """

    model_config = common_config
    url: str = Field(
        ...,
        description="The URI pointing to the thumbnail image. Can be a URL or a base64-encoded image.",
    )


class Camera(BaseModel):
    """
    The camera defines the position, tilt, and heading of the point from which the SceneView's visible extent is observed.
    """

    model_config = common_config
    fov: confloat(ge=1.0, le=170.0) | None = Field(
        55.0,
        description="The diagonal field of view (fov) angle for the camera. The range of angles must be between 1 and 170 degrees.",
    )
    heading: float = Field(
        ...,
        description="The heading of the camera in degrees. Heading is zero when north is the top of the screen. It increases as the view rotates clockwise.",
    )
    position: PointGeometry = Field(
        ..., description="The position of the camera defined by a map point."
    )
    tilt: float = Field(
        ...,
        description="The tilt of the camera in degrees with respect to the surface as projected down from the camera position. Tilt is zero when looking straight down at the surface and 90 degrees when the camera is looking parallel to the surface.",
    )


class SceneViewpoint(BaseModel):
    """
    The location or camera position from which to view the scene.
    """

    model_config = common_config
    camera: Camera
    rotation: confloat(ge=0.0, le=360.0) | None = Field(
        None,
        description="The rotation of due north in relation to the top of the view in degrees.",
    )
    scale: float | None = Field(None, description="The scale of the viewpoint.")
    target_geometry: Extent | dict | None = Field(
        None,
        alias="targetGeometry",
        description="The target geometry framed by the viewpoint.",
    )


class MapViewpoint(BaseModel):
    """
    Represents the location displayed on the map. When multiple properties are defined, the `targetGeometry` is applied first, then the `scale`, then the `rotation`.
    """

    model_config = common_config
    rotation: confloat(ge=0.0, le=360.0) | None = Field(
        None,
        description="The clockwise rotation of due north in relation to the top of the view in degrees. Default is `0`.",
    )
    scale: float | None = Field(
        None,
        description="The scale of the viewpoint. If scale is not defined, it will be automatically calculated such that it is just small enough to fit the entire targetGeometry within the view.",
    )
    target_geometry: Extent | dict = Field(
        ...,
        alias="targetGeometry",
        description="[Required] The target extent framed by the viewpoint.",
    )


class AppleIPS(BaseModel):
    model_config = common_config

    enabled: bool | None = Field(
        True,
        description="A boolean value indicating whether the Indoor Positioning System (IPS) is enabled.",
    )


class GNSS(BaseModel):
    model_config = common_config

    enabled: bool | None = Field(
        True,
        description="A boolean value indicating whether the Global Navigation Satellite System (GNSS) is enabled.",
    )


class PositioningService(BaseModel):
    """
    An object containing indoor positioning data service information.
    """

    model_config = common_config
    item_id: str = Field(
        ...,
        alias="itemId",
        description="Feature Service item representing indoor positioning data service.",
    )


class PathSnapping(BaseModel):
    """
    Defines the path snapping capabilities for the configuration.
    """

    model_config = common_config
    distance: float | None = Field(
        5,
        description="A floating-point number representing path snapping distance. Default is `5`.",
    )
    enabled: bool | None = Field(
        True,
        description="Indicates whether the path snapping capabilities for the configuration is turned on or not.",
    )
    units: Literal["feet", "meter"] | None = Field(
        "meter",
        description="Defines the units for the path snapping distance. Default is `meter`.",
    )


class Smoothing(BaseModel):
    """
    Defines the smoothing properties for the configuration.
    """

    model_config = common_config

    enabled: bool | None = Field(
        True,
        description="Indicates whether the smoothing property for the configuration is turned on or not.",
    )


class MapIPSInfoConfiguration(BaseModel):
    """
    An object containing configuration parameters for the map."""

    model_config = common_config

    appleIPS: AppleIPS | None = Field(
        None,
        alias="AppleIPS",
        description="Defines whether the AppleIPS capabilities for the configuration is turned on or not.",
    )
    gnss: GNSS | None = Field(
        None,
        alias="GNSS",
        description="Defines whether the GNSS capabilities for the configuration is turned on or not.",
    )
    path_snapping: PathSnapping | None = Field(
        None,
        alias="pathSnapping",
        description="Defines the path snapping capabilities for the configuration.",
    )
    smoothing: Smoothing | None = Field(
        None,
        description="Defines the smoothing property for the configuration is turned on or not.",
    )


class TraceConfiguration(BaseModel):
    """
    Identifies a set of utility network parameters that define elements of a trace or of a subnetwork.
    """

    model_config = common_config

    id: str = Field(
        ...,
        description="A unique identifying string for the trace configuration.",
    )
    title: str = Field(
        ...,
        description=" A user-friendly string title for the trace configuration that can be used in a table of contents.",
    )


class FloorFilter(BaseModel):
    """
    Configurable properties used by the floor filter widget.
    """

    model_config = common_config
    enabled: bool | None = Field(
        False,
        description="Indicates whether the floor filter is actively filtering the displayed content according to the floor filter selection.",
    )
    facility: str | None = Field(
        None,
        description="Contains a facility identifier for the initially selected facility in the floor filter.",
    )
    level: str | None = Field(
        None,
        description="Contains a level identifier for the initially selected floor, which is used when filtering floor-aware layers.",
    )
    long_names: bool | None = Field(
        False,
        alias="longNames",
        description="Indicates whether the floor filter is showing long names instead of short names for floor levels.",
    )
    minimized: bool | None = Field(
        False,
        description="Indicates whether the floor filter has been minimized to show only the levels list instead of showing the full set of breadcrumbs.",
    )
    pinned_levels: bool | None = Field(
        False,
        alias="pinnedLevels",
        description="Indicates whether the floor filter has been pinned to show the floor levels list, instead of including the levels as a breadcrumb dropdown.",
    )
    site: str | None = Field(
        None,
        description="Contains a site identifier for the initially selected site in the floor filter.",
    )


class Range(BaseModel):
    """
    Range object.
    """

    model_config = common_config
    interaction_mode: InteractionMode = Field(
        None,
        alias="interactionMode",
        description="Indicates the mode in which the active range should be presented to the user.",
    )
    number_of_stops: int | None = Field(
        None,
        alias="numberOfStops",
        description="This is used to generate the activeRangeValues if activeRangeValues are not specified.",
    )
    stop_interval: float | None = Field(
        None,
        alias="stopInterval",
        description="Interval in which stops should be generated.",
    )


class Offlinebasemap(BaseModel):
    """
    Object detailing offline basemap options.
    """

    model_config = common_config
    reference_basemap_name: str | None = Field(
        None,
        alias="referenceBasemapName",
        description="The filename of a basemap that has been copied to a mobile device. This can be used instead of the default basemap for the map to reduce downloads.",
    )


class LocationInfo(BaseModel):
    """
    Defines how location information will be retrieved from a [CSV](csvLayer.md) file referenced through the web, ie. referenced by URL.
    """

    model_config = common_config
    latitude_field_name: str = Field(
        ...,
        alias="latitudeFieldName",
        description="A string defining the field name that holds the latitude (Y) coordinate.",
    )
    location_type: Literal["coordinates"] = Field(
        "coordinates",
        alias="locationType",
        description="String value indicating location type.",
    )
    longitude_field_name: str = Field(
        ...,
        alias="longitudeFieldName",
        description="A string defining the field name that holds the longitude (X) coordinate.",
    )


class Info(BaseModel):
    """
    An object containing additional information specifying layer and update interval time used in the [locationTracking](locationTracking.md) object.
    """

    model_config = common_config
    layer_id: str | None = Field(
        None,
        alias="layerId",
        description="A string value indicating the given layer id specified in the web map.",
    )
    update_interval: str | None = Field(
        "300",
        alias="updateInterval",
        description="A numeric value indicating the time interval used to update the feature service. Default value is 300 seconds.",
    )

    @field_validator("update_interval", mode="before")
    def check_update_interval(cls, v):
        # ensure that the update_interval is a string, else make it a str
        # in map viewer classic this was int
        return str(v)


class TimeStopInterval(BaseModel):
    """
    The interval set for the time slider widget.
    """

    model_config = common_config
    interval: float | None = Field(None, description="The length of the time interval.")
    units: TimeIntervalUnits | None = Field(
        None, description="The units used to define the interval."
    )


class FeatureCollectionSubLayerSource(BaseModel):
    """
    The source for a layer within a feature collection to be used as fences for Geotriggers. For example, data from a map notes or sketch layer.
    """

    model_config = common_config
    layer_id: str = Field(
        ...,
        alias="layerId",
        description="A unique identifying string that must match the `id` property of a feature layer, with a feature collection, in an associated map. The fence parameters will use the same underlying data as the feature collection sub-layer in the map.",
    )
    sub_layer_id: int = Field(
        ...,
        alias="subLayerId",
        description="A reference to a layer within the feature collection specified by `layerId`. This must match the `id` property within the `layerDefinition`.",
    )
    type: Literal["featureCollectionSubLayer"] = Field(
        "featureCollectionSubLayer",
        description="String indicating the type of source.",
    )


class FeatureLayerSource(BaseModel):
    """
    The source for a feature layer to be used as fences for Geotriggers. For example, data from an online feature service or offline geodatabase table.
    """

    model_config = common_config

    layer_id: str | None = Field(
        None,
        alias="layerId",
        description="A unique identifying string that must match the `id` property of a feature layer in an associated map. The fence parameters will use the same underlying data as the feature layer in the map.",
    )
    layer_url: str | None = Field(
        None,
        alias="layerUrl",
        description="For online data, a URL to a feature layer that will be used for all queries.",
    )
    table_path: constr(pattern=r"^file:.+\.geodatabase\?itemId=\d+$") | None = Field(
        None,
        alias="tablePath",
        description="For offline data, a path to a geodatabase file. A URI format is used starting with `file:` followed by a file system path with a `.geodatabase` extension. A query parameter `itemId` must also be present specifying the ID of a table in the geodatabase's `GDB_ServiceItems` table. A relative path must be from the file which defines the layer. For example `file:../p20/northamerica.geodatabase?itemId=1`.",
    )
    type: Literal["featureLayer"] = Field(
        "featureLayer", description="String indicating the type of source."
    )


class Background(BaseModel):
    """
    Defines the appearance for the background of the map.
    """

    model_config = common_config
    color: list[conint(ge=0, le=255)] | None = Field(
        None,
        description="To define the color of the background of the map (which is shown when no data is shown).",
        title="color",
    )


class ColorBackground(BaseModel):
    """
    Specifies a single color to fill the background of the scene with. The scene background is displayed behind any scene objects, stars and atmosphere.
    """

    model_config = common_config
    color: list[conint(ge=0, le=255)] = Field(
        ...,
        description="Color is represented as a three or four-element array. The four elements represent values for red, green, blue, and alpha in that order. Values range from 0 through 255. If color is undefined for a symbol, the color value is null.",
        title="color",
    )
    transparency: conint(ge=0, le=100) | None = Field(
        None,
        description="The value has to lie between `100` (full transparency) and `0` (full opacity).",
    )
    type: Literal["color"] = Field("color", description="The type of background.")


class Bookmark(BaseModel):
    """
    Predefined bookmarks for use by the application. A bookmark is a saved map extent that allows end users to quickly navigate to a particular area of interest.
    """

    model_config = common_config
    extent: Extent | dict = Field(
        ...,
        description="An extent object containing a spatial reference, a lower left coordinate, and an upper right coordinate defining the rectangular area of the bookmark. The spatial reference must be the same as the map spatial reference. Documentation for the envelope is in the [Geometry Objects topic of the ArcGIS REST API help](https://developers.arcgis.com/documentation/common-data-types/geometry-objects.htm). If viewpoint is defined, ignore the extent property. For backwards compatibility, please save both extent and viewpoint properties.",
    )
    name: str | None = Field("Untitled", description="A string name for the bookmark.")
    thumbnail: Thumbnail | None = None
    time_extent: list[int | None] | None = Field(
        None,
        alias="timeExtent",
        description="Represents the time extent for the bookmark. If the bookmark has a time extent set, it will update the time extent of the view when the bookmark is selected. Otherwise, if the time extent is not set on the bookmark, the time extent of the view will not be changed when the bookmark is selected.",
        title="timeExtent",
    )
    viewpoint: MapViewpoint | None = Field(
        None,
        description="Represents the location displayed on the map. If viewpoint is defined, ignore the extent. If viewpoint is not defined, use the extent property. For backwards compatibility, please save both extent and viewpoint properties.",
    )


class SunLighting(BaseModel):
    """
    Object containing information for the sun lighting type. The position of the light is set to the sun's location.
    """

    model_config = common_config
    datetime: float | None = Field(
        None,
        description="The current date and time of the simulated sun. It is a number representing the number of milliseconds since 1 January, 1970 UTC.",
    )
    direct_shadows: bool | None = Field(
        False,
        alias="directShadows",
        description="Indicates whether to show shadows cast by the main light source.",
    )
    display_utc_offset: float | None = Field(
        None,
        alias="displayUTCOffset",
        description="UTC offset in decimal hours. Not to be applied to datetime for sun position, only to adjust display of datetime in UI. If displayUTCOffset is null, offset is calculated for the current location (approximate only).",
    )
    type: Literal["sun"] | None = Field("sun", description="The type of lighting")


class VirtualLighting(BaseModel):
    """
    Object containing information for the virtual lighting type. The position of the light follows the camera and is set behind the camera with a small offset to the left side.
    """

    model_config = common_config
    direct_shadows: bool | None = Field(
        False,
        alias="directShadows",
        description="Indicates whether to show shadows cast by the main light source.",
    )
    type: Literal["virtual"] = Field("virtual", description="The type of lighting")


class CloudyWeather(BaseModel):
    """
    Object containing information for changing the weather conditions in the scene to cloudy.
    """

    model_config = common_config
    cloud_cover: confloat(ge=0.0, le=1.0) | None = Field(
        0.5,
        alias="cloudCover",
        description="Specifies the amount of cloud cover in the sky.",
    )
    type: Literal["cloudy"] = Field("cloudy", description="The type of weather.")


class FoggyWeather(BaseModel):
    """
    Object containing information for changing the weather conditions in the scene to foggy.
    """

    model_config = common_config
    fog_strength: confloat(ge=0.0, le=1.0) | None = Field(
        0.5,
        alias="fogStrength",
        description="Specifies the amount of fog used in the scene.",
    )
    type: Literal["foggy"] = Field("foggy", description="The type of weather.")


class RainyWeather(BaseModel):
    """
    Object containing information for changing the weather conditions in the scene to rainy.
    """

    model_config = common_config
    cloud_cover: confloat(ge=0.0, le=1.0) | None = Field(
        0.5,
        alias="cloudCover",
        description="Specifies the amount of cloud cover in the sky.",
    )
    precipitation: confloat(ge=0.0, le=1.0) | None = Field(
        0.5, description="Specifies the amount of rainfall in the scene."
    )
    type: Literal["rainy"] = Field("rainy", description="The type of weather.")


class SnowyWeather(BaseModel):
    """
    Object containing information for changing the weather conditions in the scene to snowy.
    """

    model_config = common_config
    cloud_cover: confloat(ge=0.0, le=1.0) | None = Field(
        0.5,
        alias="cloudCover",
        description="Specifies the amount of cloud cover in the sky.",
    )
    precipitation: confloat(ge=0.0, le=1.0) | None = Field(
        0.5, description="Specifies the amount of snowfall in the scene."
    )
    snow_cover: SnowCover | None = Field(
        SnowCover.disabled,
        validate_default=True,
        alias="snowCover",
        description="Display surfaces covered with snow.",
    )
    type: Literal["snowy"] = Field("snowy", description="The type of weather.")


class SunnyWeather(BaseModel):
    """
    Object containing information for changing the weather conditions in the scene to sunny.
    """

    model_config = common_config
    cloud_cover: confloat(ge=0.0, le=1.0) | None = Field(
        0.5,
        alias="cloudCover",
        description="Specifies the amount of cloud cover in the sky.",
    )
    type: Literal["sunny"] = Field("sunny", description="The type of weather.")


class Environment(BaseModel):
    """
    Represents settings that affect the environment in which the web scene is displayed. It is entirely stored as part of the initial state of the web scene, and partially in the slides in the presentation.
    """

    model_config = common_config
    atmosphere_enabled: bool | None = Field(
        True,
        alias="atmosphereEnabled",
        description="Whether the atmosphere should be visualized. This includes sky and haze effects.",
    )
    background: ColorBackground | None = Field(
        None,
        description="The background is what is displayed behind any scene objects, stars and atmosphere.",
    )
    lighting: SunLighting | VirtualLighting | None = Field(
        None,
        description="Object containing information on how the scene is lit.",
        title="lighting",
    )
    stars_enabled: bool | None = Field(
        True,
        alias="starsEnabled",
        description="Whether stars should be displayed in the sky.",
    )
    weather: (
        CloudyWeather | FoggyWeather | RainyWeather | SnowyWeather | SunnyWeather | None
    ) = Field(
        {"$ref": "#/definitions/sunnyWeather_schema.json"},
        description="Indicates the type of weather visualization in the scene.",
    )


class SlicePlane(BaseModel):
    model_config = common_config
    heading: float | int = Field(
        ...,
        le=360,
        ge=0,
        description="The heading of the slice plane in degrees. Heading is zero when north is the top of the screen. It increases as the view rotates clockwise.",
    )
    height: float | int = Field(
        ...,
        ge=0,
        description="The height of the slice plane in the scene. The height is measured from the ground surface to the slice plane.",
    )
    position: PointGeometry = Field(
        ...,
        description="The position of the slice plane in the scene. The position is defined by a map point with a spatial reference.",
    )
    tilt: float | int = Field(
        ...,
        le=360,
        ge=0,
        description="The tilt of the slice plane in degrees with respect to the surface as projected down from the slice plane position. Tilt is zero when looking straight down at the surface and 90 degrees when the slice plane is looking parallel to the surface.",
    )
    type: Literal["plane"] = Field("plane", description="The type of the slice plane.")
    width: float | int = Field(
        ...,
        ge=0,
        description="The width of the slice plane in the scene. The width is measured from the left edge to the right edge of the slice plane.",
    )


class SliceAnalyses(BaseModel):
    model_config = common_config
    excluded_layers: list[LayerReference] | None = Field(
        None,
        alias="excludedLayers",
        description="Collection of layers or sublayers which are to be excluded from slicing.",
    )
    exclude_ground_surface: bool = Field(
        False,
        alias="excludeGroundSurface",
        description="Boolean property determining whether the ground surface is excluded from slicing.",
    )
    shape: SlicePlane = Field(
        ..., description="The shape used to slice elements in a 3D scene."
    )
    tilt_enabled: bool = Field(
        False,
        alias="tiltEnabled",
        description="Boolean property determining whether the slice pane is tilted or not.",
    )
    type: Literal["slice"] = Field(
        "slice", description="The type of the slice analysis."
    )


class SceneInitialState(BaseModel):
    """
    An object that provides information about the initial environment settings and viewpoint of the web scene.
    """

    model_config = common_config
    analyses: list[SliceAnalyses] | None = Field(
        None,
        description="An array of analysis objects that define the analyses to be performed on the scene.",
    )
    environment: Environment
    time_extent: list[int] | None = Field(
        None,
        alias="timeExtent",
        description="Represents the time extent for the data displayed on the scene.",
    )
    viewpoint: SceneViewpoint = Field(
        ...,
        description="Describes a point of view for a 2D or 3D view. In a 3D view, it is determined using a camera position.",
    )


class MapInitialState(BaseModel):
    """
    Defines the initial state for web map.
    """

    model_config = common_config
    time_extent: list[int | None] | None = Field(
        None,
        alias="timeExtent",
        description="Represents the time extent for the data displayed on the map.",
        title="timeExtent",
    )
    viewpoint: MapViewpoint = Field(
        ..., description="Represents the location displayed on the map."
    )


class MapFloorInfo(BaseModel):
    """
    Contains floor-awareness information for the map. Defines the layers and required fields for each layer that are used for floor filtering.
    """

    model_config = common_config
    facility_layer: FacilityLayer = Field(
        ...,
        alias="facilityLayer",
        description="Defines the layer and field properties for the Facility layer used for floor filtering.",
    )
    level_layer: LevelLayer = Field(
        ...,
        alias="levelLayer",
        description="Defines the layer and field properties for the Level layer used for floor filtering.",
    )
    site_layer: SiteLayer | None = Field(
        None,
        alias="siteLayer",
        description="Defines the layer and field properties for the Site layer used for floor filtering. This property is optional.",
    )


class MapIPSInfo(BaseModel):
    """
    Contains indoor positioning system information for the map. Defines indoor position data service and related properties that help applications compute device location inside a building.
    """

    model_config = common_config
    configuration: MapIPSInfoConfiguration | None = Field(
        None,
        description="An object containing configuration parameters for the map.",
    )
    positioning_service: PositioningService = Field(
        ...,
        alias="positioningService",
        description="Defines the portal item for the positioning data service.",
    )


class UtilityNetwork(BaseModel):
    """
    Identifies the central object for utility network schema information to access utility-related functionality, such as tracing and querying associations.
    """

    model_config = common_config
    id: str | None = Field(
        None,
        description="A unique identifying string for the utility network.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the utility network that can be used in a table of contents.",
    )
    trace_configurations: list[TraceConfiguration] | None = Field(
        None,
        alias="traceConfigurations",
        description="An array of pre-configured trace configurations for quickly running common traces.",
    )
    url: str = Field(
        ...,
        description="A string value indicating the URL to the utility network layer.",
    )


class OfflineTable(BaseModel):
    model_config = common_config
    include_rows: bool | None = Field(
        True,
        alias="includeRows",
        description="A boolean value indicating whether or not to include rows in the offline table.",
    )


class Offline(BaseModel):
    """
    Use if working with offline maps.
    """

    model_config = common_config
    connected_online_layers: list[ConnectedOnlineLayer] | None = Field(
        None,
        alias="connectedOnlineLayers",
        description="List of layers which should be included in an offline map as connected online layers.",
    )
    editable_layers: EditableLayers | None = Field(
        None,
        alias="editableLayers",
        description="Object detailing the available offline editing options.",
    )
    offlinebasemap: Offlinebasemap | None = None
    readonly_layers: ReadonlyLayers | None = Field(
        None,
        alias="readonlyLayers",
        description="Object indicating what to do with attachments in read-only layers.",
    )
    sync_data_options: float | None = Field(
        None,
        alias="syncDataOptions",
        description="An integer value that corresponds to `syncDataOptions` for [Create Replica](https://developers.arcgis.com/rest/services-reference/enterprise/create-replica.htm). This value determines what additional capabilities will be included when going offline for a feature layer (e.g. contingent values, annotation) that are not included by default. This value applies to all feature layers in the webmap, however if the requested capability is not supported or present in the feature layer it will be ignored.",
    )
    tables: list[OfflineTable] | OfflineTable | None = Field(
        None,
        description="An array of tables that are included in the offline map. Each table is represented by an `OfflineTable` object.",
    )


class LocationTracking(BaseModel):
    """
    If locationTracking is set and enabled, the collector app will update the feature service at the defined interval with the current location of the user logged into the collector application.
    """

    model_config = common_config
    enabled: bool | None = Field(
        None,
        description="A boolean value indicating whether or not location tracking is enabled on the webmap.",
    )
    info: Info | None = Field(
        None,
        description="An object of additional information specifying layer and update interval time.",
    )


class TimeSliderProperties(BaseModel):
    """
    Configurable properties used within the TimeSlider widget.
    """

    model_config = common_config
    current_time_extent: list[int | None] | None = Field(
        None,
        alias="currentTimeExtent",
        description="An optional array of numbers indicating the slider's start to end time extent.",
    )
    end_time: float | None = Field(
        None,
        alias="endTime",
        description="The optional end of the time slider. If not specified, the slider defaults to the full time extent of all time-enabled layers.",
    )
    loop: bool | None = Field(
        False,
        description="When `true`, the time slider will play its animation in a loop. The default is `false`.",
    )
    number_of_stops: int | None = Field(
        None,
        alias="numberOfStops",
        description="Number of stops within the timeSlider widget.",
    )
    start_time: float | None = Field(
        None,
        alias="startTime",
        description="The optional start of the time slider. If not specified, the slider defaults to the full time extent of all time-enabled layers.",
    )
    stops: list[int] | None = Field(
        None,
        description="An optional array of numbers that defines stops for the time slider. Can be used to create irregularly spaced stops representing specific moments of importance.",
    )
    thumb_count: int | None = Field(
        2,
        alias="thumbCount",
        description="The default value for the thumbCount is 2.",
    )
    thumb_moving_rate: float | None = Field(
        2000,
        alias="thumbMovingRate",
        description="Rate at which the time animation plays. Units are in milliseconds. Default is `2000`.",
    )
    time_stop_interval: TimeStopInterval | None = Field(
        None,
        alias="timeStopInterval",
        description="The interval which has been defined for the time slider.",
    )


class DeviceLocationGeotriggerFeed(BaseModel):
    """
    A Geotrigger feed which uses the device location to provide updates.
    """

    model_config = common_config
    filter_expression: ExpressionInfo | None = Field(
        None,
        alias="filterExpression",
        description="An optional Arcade expression that controls whether a location update will be used by a geotrigger. For example, the expression could reject GPS updates with a poor horizontal accuracy. This expression uses the [Location Update Constraint](https://developers.arcgis.com/arcade/guide/profiles/#location-update-constraint) Arcade profile. The expression should return a Boolean where false indicates the location will not be used.",
    )
    type: Literal["deviceLocation"] = Field(
        "deviceLocation",
        description="String indicating the type of Geotrigger feed.",
    )


class GeotriggerNotificationOptions(BaseModel):
    """
    Options that control the notification information sent to a client app client when a Geotrigger condition is met.
    """

    model_config = common_config

    expression_info: ExpressionInfo | None = Field(
        None,
        alias="expressionInfo",
        description="An optional Arcade expression which can be used to configure notification information when the Geotrigger condition is met. The expression uses the [Geotrigger Notification](https://developers.arcgis.com/arcade/guide/profiles/#geotrigger-notification) Arcade profile. The expression can return either a String that will be used for a user facing message or a Dictionary that can include a user facing message with the key 'message', and a recommended list of app actions using the key 'actions'.",
    )
    requested_actions: list[str] | None = Field(
        None,
        alias="requestedActions",
        description="An optional list of strings indicating the set of possible actions resulting from this Geotrigger. This property shows the full list of recommended actions that the author intends to be taken for this Geotrigger. Note that there is no requirement to take any action when a notification is delivered, but these provide a way to understand the intention of the author of the Geotrigger. These strings can be displayed to a user to make them aware of the Geotrigger's expected behavior, or used by client apps to determine whether the desired actions are supported. If a client app receives notification information containing an unrecognized action they should ignore it. Actions can be any strings that are supported by geotrigger enabled apps in your organization and should cover all possible 'action' values returned from evaluation of expressionInfo.expression.",
    )


class Editing(BaseModel):
    """
    An object containing all the editing properties within the web map.
    """

    model_config = common_config
    location_tracking: LocationTracking | None = Field(
        None,
        alias="locationTracking",
        description=" If locationTracking is set and enabled, the collector app will update the feature service at the defined interval with the current location of the user logged into the collector application.",
    )


class TimeSlider(BaseModel):
    """
    Time animation is controlled by a configurable time slider. Those configurations are saved to the web scene as a timeSlider widget.
    """

    model_config = common_config

    properties: dict[str, Any] | TimeSliderProperties | None = Field(
        None, description="The properties of the time slider."
    )


class Widgets(BaseModel):
    """
    The widgets object contains widgets that should be exposed to the user.
    """

    model_config = common_config
    floor_filter: FloorFilter | None = Field(
        None,
        alias="floorFilter",
        description="Configurable properties used by the floor filter widget.",
    )
    range: Range | None = Field(None, description="Active range.", title="Range")
    time_slider: TimeSlider | None = Field(
        None,
        alias="timeSlider",
        description="Time animation is controlled by a configurable time slider. The time slider widget will act upon all the time aware layers in the map.",
    )


class FeatureFenceParameters(BaseModel):
    """
    Fence parameters for a Geotrigger that uses feature data from an online feature service, offline geodatabase table, or layer within a feature collection.
    """

    model_config = common_config
    buffer_distance: float | None = Field(
        0,
        alias="bufferDistance",
        description="An optional buffer distance to apply to fence features in meters.",
    )
    fence_source: FeatureCollectionSubLayerSource | FeatureLayerSource = Field(
        ...,
        alias="fenceSource",
        description="An object defining the source for a feature layer to be used as fences.",
    )
    filter: FeatureFilter | None = Field(
        None,
        description="An optional filter to reduce the features used for the parameters.",
    )
    type: Literal["features"] = Field(
        "features",
        description="String indicating the fence parameters type.",
    )


class FenceGeotrigger(BaseModel):
    """
    A condition which monitors the dynamic elements of the geotrigger feed for enter/exit against the fences defined by the Fence Parameters
    """

    enter_exit_rule: EnterExitRule | None = Field(
        None,
        alias="enterExitRule",
        description="The rule that determines whether a fence polygon has been entered or exited by the geometry from a feed. If this value is 'enterIntersectsAndExitDoesNotIntersect', a fence polygon is entered when it intersects a feed geometry and exited when it no longer intersects. If this value is 'enterContainsAndExitDoesNotContain', a fence polygon is entered when it contains a feed geometry and exited when it is no longer contained. If this value is 'enterContainsAndExitDoesNotIntersect' a fence polygon is entered when it contains a feed geometry and exited when it no longer intersects. If not set, the default behavior is `enterContainsAndExitDoesNotIntersect`. The 'feedAccuracyMode' must be set to 'useGeometryWithAccuracy' for this property to have an effect.",
    )
    feed: DeviceLocationGeotriggerFeed = Field(
        ..., description="The feed for this Geotrigger."
    )
    feed_accuracy_mode: FeedAccuracyMode | None = Field(
        None,
        alias="feedAccuracyMode",
        description="Indicates how the geotrigger will use accuracy information from a feed. If this value is 'useGeometry', the reported geometry from a feed will be used. If this value is 'useGeometryWithAccuracy' the feed geometry will be used in conjunction with accuracy information. If not set, the default behavior is `useGeometry`.",
    )
    fence_notification_rule: FenceNotificationRule = Field(
        ...,
        alias="fenceNotificationRule",
        description="Indicates the type of event that will trigger notifications for the Fence Geotrigger. For example, a value of 'enter' will result in notifications when the geometry of the feed enters a fence polygon.",
    )
    fence_parameters: FeatureFenceParameters = Field(
        ...,
        alias="fenceParameters",
        description="An object defining the fences to use for this Geotrigger.",
    )
    name: str | None = Field(None, description="The name for this Geotrigger.")
    notification_options: GeotriggerNotificationOptions | None = Field(
        None,
        alias="notificationOptions",
        description="Options that control the notification information sent to a client app when a Geotrigger condition is met.",
    )
    type: Literal["fence"] = Field(
        "fence",
        description="String indicating the Geotrigger condition type.",
    )


class SearchLayer(BaseModel):
    """
    Layer configuration for search.
    """

    model_config = common_config
    field: FieldModel
    id: str = Field(..., description="A string identifying the layer.")
    sub_layer: int | None = Field(
        None, alias="subLayer", description="Optional index for a sublayer."
    )


class SearchTable(BaseModel):
    """
    Search configuration for table.
    """

    model_config = common_config
    field: FieldModel
    id: str = Field(..., description="A string identifying the table.")


class Search(BaseModel):
    """
    An object specifying the search parameters set within the web map.
    """

    model_config = common_config
    disable_place_finder: bool = Field(
        None,
        alias="disablePlaceFinder",
        description="[Required] A boolean value indicating whether or not to disable the place finder.",
    )
    enabled: bool = Field(
        True,
        description="[Required] A boolean value indicating whether search (find) functionality is enabled in the web map.",
    )
    hint_text: str | None = Field(
        None,
        alias="hintText",
        description="A string value used to indicate the hint provided with the search dialog.",
    )
    layers: list[SearchLayer] | None = Field(
        None,
        description="An array of objects that define search fields and search criteria for layers in the web map.",
    )
    tables: list[SearchTable] | None = Field(
        None,
        description="An array of objects that define search fields and search criteria for tables in the web map.",
    )


class GeotriggersInfo(BaseModel):
    """
    Information relating to a list of Geotriggers.
    """

    geotriggers: list[FenceGeotrigger] = Field(
        ..., description="A list of Geotriggers."
    )


class Viewing(BaseModel):
    """
    An object containing all the viewing properties of the web map. If this is *null* or not defined, the client should assume a logical default.
    """

    model_config = common_config
    search: Search | None = Field(
        None,
        description="An object specifying search parameters within the webmap.",
    )


class ApplicationProperties(BaseModel):
    """
    The applicationProperties object is one of the objects at the top level of the JSON web map JSON schema. This is responsible for containing the viewing and editing properties of the web map. There are specific objects within this object that are applicable only to Collector and are explained within the property descriptions.
    """

    model_config = common_config
    editing: Editing | None = Field(
        None,
        description="If locationTracking is set and enabled, the Collector application will update the feature service at the defined interval with the current location of the user logged into the Collector app.",
    )
    offline: Offline | None = Field(
        None, description="Use if working with offline maps."
    )
    viewing: Viewing | None = Field(
        None,
        description="An object containing all the viewing properties of the web map. If this is *null* or not defined, the client should assume a logical default.",
    )


class SceneBasemap(BaseModel):
    """
    The basemap provides geographic context to the map and scene.
    """

    model_config = common_config
    base_map_layers: list[
        ImageServiceLayer
        | MapServiceLayer
        | OpenStreetMapLayer
        | RasterDataLayer
        | TiledImageServiceLayer
        | WebTiledLayer
        | WMSLayer
        | SceneLayer
        | TiledMapServiceLayer
        | VectorTileLayer
        | WCSLayer
    ] = Field(
        ...,
        alias="baseMapLayers",
        description="An array of baseMapLayer objects defining the basemaps used in the web scene.",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the basemap."
    )
    title: str = Field(
        ...,
        description="Required string title for the basemap that can be used in a table of contents.",
    )


class MapBasemap(BaseModel):
    """
    A basemap layer is a layer that provides geographic context to the map. A web map always contains a basemap. The basemap has a title and is the combination of each baseMapLayer. It is required that a baseMap be saved within the web map.
    """

    model_config = common_config
    base_map_layers: list[
        BingLayer
        | ImageServiceLayer
        | ImageServiceVectorLayer
        | MapServiceLayer
        | OpenStreetMapLayer
        | TiledImageServiceLayer
        | TiledMapServiceLayer
        | VectorTileLayer
        | WebTiledLayer
        | WMSLayer
        | WCSLayer
    ] = Field(
        ...,
        alias="baseMapLayers",
        description="An array of baseMapLayer objects defining the basemaps used in the web map.",
    )
    title: str = Field(
        ...,
        description="Required string title for the basemap that can be used in a table of contents. It takes the title of the first `baseMapLayer` in the array.",
    )


class Webmap(BaseModel):
    """
    The web map data lists the basemap, operational layers, and bookmarks to be used in the web map. It also contains information about popup windows and layer styling overrides to be used in the web map. A version property allows you to supply the version of the web map JSON format being used.
    """

    model_config = common_config
    application_properties: ApplicationProperties | None = Field(
        None,
        alias="applicationProperties",
        description="Viewing and editing properties of the webmap.",
    )
    authoring_app: str | None = Field(
        None,
        alias="authoringApp",
        description="[Required] String value indicating the application that last authored the webmap.",
    )
    authoring_app_version: str | None = Field(
        None,
        alias="authoringAppVersion",
        description="[Required] String value indicating the version number of the application that last authored the webmap.",
    )
    background: Background | None = None
    base_map: MapBasemap = Field(
        ...,
        alias="baseMap",
        description="[Required] Basemaps give the web map a geographic context.",
    )
    bookmarks: list[Bookmark] | None = Field(
        None,
        description="A bookmark is a saved geographic extent that allows end users to quickly navigate to a particular area of interest.",
    )
    geotriggers_info: GeotriggersInfo | None = Field(
        None,
        alias="geotriggersInfo",
        description="Information on any Geotrigger conditions defined for this map.",
    )
    initial_state: MapInitialState | None = Field(
        None,
        alias="initialState",
        description="The initial state at which to open the map.",
    )
    map_floor_info: MapFloorInfo | None = Field(
        None,
        alias="mapFloorInfo",
        description="Contains floor-awareness information for the map.",
    )
    map_ips_info: MapIPSInfo | None = Field(
        None,
        alias="mapIPSInfo",
        description="Contains indoor positioning system information for the map.",
    )
    map_range_info: MapRangeInformation | None = Field(
        None, alias="mapRangeInfo", description="Map range information."
    )
    operational_layers: (
        list[
            AnnotationLayer
            | CatalogLayer
            | CSVLayer
            | DimensionLayer
            | FeatureLayer
            | GeoJSONLayer
            | GeoRSSLayer
            | GroupLayer
            | ImageServiceLayer
            | ImageServiceVectorLayer
            | KMLLayer
            | KnowledgeGraphLayer
            | MapServiceLayer
            | OGCFeatureLayer
            | OrientedImageryLayer
            | StreamLayer
            | SubtypeGroupLayer
            | TiledImageServiceLayer
            | TiledMapServiceLayer
            | VectorTileLayer
            | WebTiledLayer
            | WFSLayer
            | WMSLayer
            | MediaLayer
            | WCSLayer
            | VideoLayer
        ]
        | None
    ) = Field(
        None,
        alias="operationalLayers",
        description="Operational layers contain business data which are used to make thematic maps.",
    )
    parcel_fabric: ParcelFabric | None = Field(
        None,
        alias="parcelFabric",
        description="A Parcel Fabric object that the map can use to access Parcel Fabric related functionality, such as managing parcel records.",
    )
    reference_scale: confloat(ge=0.0) | None = Field(
        0,
        alias="referenceScale",
        description="A floating-point number representing the reference scale which map symbols are drawn relative to. The number is the scale's denominator. When the reference scale is 0, symbols are always drawn at the same size regardless of the map scale. The referenceScale is only used for Feature Layers that have scaleSymbols:true. Not all applications or layer types support referenceScale yet. In particular, ArcGISOnline will not use the referenceScale when drawing symbols in the browser.",
    )
    spatial_reference: SpatialReference = Field(
        ...,
        alias="spatialReference",
        description="[Required] An object used to specify the spatial reference of the given geometry.",
        title="spatialReference",
    )
    tables: list[SubtypeGroupTable | Table] | None = Field(
        None,
        description="An array of objects representing non-spatial datasets used in the web map.",
    )
    time_zone: str | None = Field(
        None,
        alias="timeZone",
        description="Time zone of the webmap. When applicable, dates and times will be displayed using this time zone. The time zone can be `system`, `unknown` or any named [IANA](https://www.iana.org/time-zones) time zone. See [here](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) for a comprehensive list of time zones. The `system` keyword indicates the webmap will use the time zone currently used by the device loading the webmap. Whereas `unknown` means that dates will be treated as if they do not have an associated time zone. When the property is not defined in the webmap, the client uses its default behavior (JavaScript Maps SDK uses `system`, while ArcGIS Pro uses `unknown`).",
    )
    utility_networks: list[UtilityNetwork] | None = Field(
        None,
        alias="utilityNetworks",
        description="An array of utility network objects the map can use to access utility-related functionality, such as tracing and querying associations.",
    )
    version: str = Field("2.34", description="The webmap version being used")
    widgets: Widgets | None = Field(
        None,
        description="The widgets object contains widgets that should be exposed to the user.",
    )


class FocusAreaOutline(BaseModel):
    model_config = common_config
    color: list[float] | None = Field(
        None, description="The color of the focus area outline."
    )


class FocusArea(BaseModel):
    """
    Focus area defines an area of interest in a scene.
    """

    model_config = common_config
    enabled: bool = Field(True, description="Whether the focus area is enabled or not.")
    geometries: list[PolygonGeometry] | str = Field(
        ...,
        description="URL to a focus area geometries json file, typically stored in `ITEM/resources`.",
    )
    id: str = Field(..., description="A unique identifier for the focus area.")
    outline: FocusAreaOutline | None = Field(
        None,
        description="The outline of the focus area, which can be used to highlight the area.",
    )
    title: str | None = Field(
        None, description="The title of the focus area, which can be used in the UI."
    )


class FocusAreas(BaseModel):
    """
    Container holding the scene's focus areas.
    """

    model_config = common_config
    areas: list[FocusArea] = Field(..., description="An array of focus area objects.")
    style: Literal["bright", "dark"] | None = Field(
        None, description="The global visual effect applied to all focus areas."
    )


class ClippingArea(BaseModel):
    """
    Defines area to be clipped for display.
    """

    model_config = common_config
    clip: bool | None = Field(None, description="enable / disable clipping")
    geometry: Extent | dict | None = Field(None, description="envelope of clip area")


class NavigationConstraint(BaseModel):
    """
    Object determining whether the camera is constrained to navigate only above, or also under the ground surface.
    """

    model_config = common_config
    type: NavigationType


class Ground(BaseModel):
    """
    Ground defines the main surface of the web scene, based on elevation layers.
    """

    model_config = common_config
    layers: list[RasterDataElevationLayer | TiledElevationLayer] = Field(
        None,
        description="An array of elevationLayer objects defining the elevation of the ground in the web scene.",
    )
    navigation_constraint: NavigationConstraint | None = Field(
        None,
        alias="navigationConstraint",
        description="Object determining whether the camera is constrained to navigate only above, or also under the ground surface. If not specified, navigation is constrained to above ground.",
    )
    surface_color: list[conint(ge=0, le=255)] | None = Field(
        None,
        alias="surfaceColor",
        description="Defines the color of the ground surface, displayed underneath the basemap. If no color, the default grid is shown.",
        title="color",
    )
    transparency: conint(ge=0, le=100) | None = Field(
        None,
        description="The transparency of the ground surface. It is used for seeing through the ground, therefore this property also changes the transparency of the basemap. Draped operational layers are not affected by this property. The value has to lie between `100` (full transparency) and `0` (full opacity).",
    )


class HeightModelInfo(BaseModel):
    """
    An object that defines the characteristics of the vertical coordinate system used by the web scene.
    """

    model_config = common_config
    height_model: HeightModel | None = Field(
        HeightModel.ellipsoidal,
        validate_default=True,
        alias="heightModel",
        description="The surface type or height model of the vertical coordinate system.",
    )
    height_unit: HeightUnit = Field(
        ...,
        alias="heightUnit",
        description='The unit of the vertical coordinate system.<a href="#heightUnit"><sup>1</sup></a>',
    )
    vert_crs: str | None = Field(
        None,
        alias="vertCRS",
        description="(Optional) The datum realization of the vertical coordinate system.",
    )


class TransportationNetwork(BaseModel):
    """
    The transportation network used for routing in the scene.
    """

    model_config = common_config
    name: str = Field(
        ...,
        description="The name of the transportation network. The name must be unique within a scene.",
    )
    path: constr(pattern=r"^file:.+\.geodatabase?$") = Field(
        ...,
        description="The path to the geodatabase which contains the network.",
    )
    title: str = Field(
        ...,
        description="A title or alias of the network layer. This can be used in the client user interface.",
    )


class Description(BaseModel):
    """
    Description object with text.
    """

    model_config = common_config
    text: str = Field(..., description="Text to display as slide description.")


class Title(BaseModel):
    """
    Text for the title of the individual slide.
    """

    model_config = common_config
    text: str = Field(..., description="Text to display as slide title.")


class VisibleLayer(BaseModel):
    """
    Object with layer id, used to indicate layer visibility in a slide.
    """

    model_config = common_config
    id: str | None = Field(
        None,
        description="The id of the layer as listed on the operational layer.",
    )
    sub_layer_ids: list[int] | None = Field(
        None,
        alias="subLayerIds",
        description="List of visible sublayer ids, as listed on the service of the layer.",
    )


class LegendInfo(BaseModel):
    """
    The legendInfo object contains configurations for the legend component set in a slide.
    """

    model_config = common_config
    visible: bool | None = Field(
        False,
        description="A boolean used to indicate the visibility of the legend component inside a slide.",
    )


class SlideElements(BaseModel):
    """
    The elements object contains configurations for components set in a slide.
    """

    model_config = common_config
    analyses: list[SliceAnalyses] | None = Field(
        None, description="List of analyses to be performed on the slice."
    )
    legend_info: LegendInfo | None = Field(
        None,
        alias="legendInfo",
        description="The component configuration for the legend component set in a slide.",
    )
    popup_info: SlidePopupInfo | None = Field(
        None,
        alias="popupInfo",
        description="The component configuration for the popup component set in a slide.",
    )


class Slide(BaseModel):
    """
    A slide object used within a presentation.
    """

    model_config = common_config
    base_map: SceneBasemap | None = Field(
        None,
        alias="baseMap",
        description="The basemap to be displayed on the slide.",
    )
    description: Description | None = Field(
        None,
        description="Text description of the individual presentation slide.",
    )
    elements: list[SlideElements] | None = Field(
        None,
        description="The configurations of components set in a slide.",
    )
    enabled_focus_areas: list[str] | None = Field(
        None,
        alias="enabledFocusAreas",
        description="List of enabled focus areas in the slide.",
    )
    environment: Environment | None = Field(
        None,
        description="Represents settings that affect the environment in which the web scene is displayed.",
    )
    ground: Ground | None = Field(
        None, description="The ground properties to be set in the slide."
    )
    hidden: bool | None = Field(
        False,
        description="A boolean used to indicate the visibility of the slide inside a presentation.",
    )
    id: str | None = Field(
        None,
        description="The unique id of a slide within the slides property of a Presentation.",
    )
    layout: SlideLayout | None = Field(
        SlideLayout.caption,
        description="Indicates the placement of visual elements in the slide.",
    )
    thumbnail: Thumbnail
    time_extent: list[int | None] | None = Field(
        None,
        alias="timeExtent",
        description="The time extent of the slide.",
    )
    title: Title
    viewpoint: SceneViewpoint
    visible_layers: list[VisibleLayer] = Field(
        ...,
        alias="visibleLayers",
        description="An array of objects used to indicate the visible layers of the web scene.",
    )


class Presentation(BaseModel):
    """
    A presentation consists of multiple slides, where each slide is a specific view into the web scene.
    """

    model_config = common_config
    slides: list[Slide] | None = Field(None, description="Array of slide objects.")


class Webscene(BaseModel):
    """
    The web scene data lists the basemap, operational layers, and presentation slides to be used in the web scene. It also contains information about pop-up windows and layer styling overrides to be used in the web scene. A version property allows you to supply the version of the web scene JSON format being used.
    """

    model_config = common_config
    application_properties: ApplicationProperties | None = Field(
        None, alias="applicationProperties"
    )
    authoring_app: str | None = Field(
        None,
        alias="authoringApp",
        description="String value indicating the application which authored the webmap",
    )
    authoring_app_version: str | None = Field(
        None,
        alias="authoringAppVersion",
        description="String value indicating the authoring App's version number.",
    )
    base_map: SceneBasemap | None = Field(
        None,
        alias="baseMap",
        description="Basemaps give the web scene a geographic context.",
    )
    clipping_area: ClippingArea | None = Field(None, alias="clippingArea")
    focus_areas: FocusAreas | None = Field(
        None,
        alias="focusAreas",
        description="An object containing the focus areas of the web scene.",
    )
    ground: Ground
    height_model_info: HeightModelInfo | None = Field(None, alias="heightModelInfo")
    initial_state: SceneInitialState | None = Field(None, alias="initialState")
    map_floor_info: MapFloorInfo | None = Field(None, alias="mapFloorInfo")
    map_range_info: MapRangeInformation | None = Field(
        None, alias="mapRangeInfo", description="Map Range Information"
    )
    operational_layers: list[
        BuildingSceneLayer
        | CSVLayer
        | DimensionLayer
        | FeatureLayer
        | GeoJSONLayer
        | GroupLayer
        | ImageServiceLayer
        | LineOfSightLayer
        | MapServiceLayer
        | OGCFeatureLayer
        | RasterDataLayer
        | TiledImageServiceLayer
        | VoxelLayer
        | WebTiledLayer
        | WFSLayer
        | WMSLayer
        | IntegratedMesh3DTilesLayer
        | IntegratedMeshLayer
        | KMLLayer
        | MediaLayer
        | PointCloudLayer
        | SceneLayer
        | TiledMapServiceLayer
        | VectorTileLayer
        | CatalogLayer
        | WCSLayer
        | OrientedImageryLayer
        | ViewshedLayer
        | Object3DTilesLayer
        | VideoLayer
    ] = Field(
        ...,
        alias="operationalLayers",
        description="Operational layers contain business data which are used to make thematic scenes.",
    )
    presentation: Presentation | None = None
    spatial_reference: SpatialReference = Field(
        ...,
        alias="spatialReference",
        description="An object used to specify the spatial reference of the given geometry.",
        title="spatialReference",
    )
    tables: list[Table] | None = Field(None, description="An array of table objects.")
    transportation_networks: list[TransportationNetwork] | None = Field(
        None,
        alias="transportationNetworks",
        description="Used to specify the transportation networks of the scene.",
    )
    version: str = Field("1.37", description="The webscene spec version being used")
    viewing_mode: ViewingMode = Field(..., alias="viewingMode")
    widgets: Widgets | None = Field(
        None,
        description="the widgets object contains widgets that should be exposed to the user.",
    )


Webmap.model_rebuild()
MapBasemap.model_rebuild()
Webscene.model_rebuild()
SceneBasemap.model_rebuild()
