from __future__ import annotations  # Enables postponed evaluation of type hints

from .base_model import BaseModel, common_config
from pydantic import Field, confloat
from typing import Any, Literal
from ..enums.layers import (
    BlendMode,
    GraphType,
)
from ..enums.link_charts import (
    IdealEdgeLengthType,
    LinkChartLayoutType,
    EventsTicksVisualizationType,
    TimeDirectionType,
)
from .popups import PopupInfo
from .layer_definition import (
    LayerDefinition,
    EffectFunctions,
    EffectFunctions1,
    EffectFunctions2,
    EffectFunctions3,
    EffectFunctions4,
    ScaleDependentEffect,
    FeatureEffect,
    ExpressionInfo,
)
from .layers import (
    AnnotationLayer,
    CatalogLayer,
    CSVLayer,
    DimensionLayer,
    FeatureLayer,
    GeoJSONLayer,
    GeoRSSLayer,
    GroupLayer,
    ImageServiceLayer,
    ImageServiceVectorLayer,
    KMLLayer,
    KnowledgeGraphLayer,
    MapServiceLayer,
    OGCFeatureLayer,
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
    Table,
)
from .mapping import (
    Background,
    MapBasemap,
    Bookmark,
    MapInitialState,
    MapFloorInfo,
    MapRangeInformation,
    ApplicationProperties,
    Widgets,
)
from .geometry import SpatialReference


class LinkChartChronologicalLayoutSettings(BaseModel):
    """
    Settings for chronological layout calculations.
    """

    model_config = common_config

    duration_line_width: int | None = Field(
        5,
        alias="durationLineWidth",
        description="An integer between 1 and 10 setting the width of the line, in points, representing the duration of events with non-zero durations. Applied only when 'showDurationLineForNonZeroDurationEntityEvents' is true. The default value is 5.",
        ge=1,
        le=10,
    )
    entity_position_at_duration_ratio: float | None = Field(
        1,
        alias="entityPositionAtDurationRatio",
        description="Determines the placement of an entity along a non-zero duration event interval. 0 represents the start of the interval, 1 represents the end of the interval. Only used in multi-timeline layouts. The default value is 1.",
        ge=0,
        le=1,
    )
    events_ticks_visualization: EventsTicksVisualizationType | None = Field(
        EventsTicksVisualizationType.start_and_end,
        alias="eventsTicksVisualization",
        description="Controls the display of start / end event ticks on the timeline. Default value is 'startAndEnd' where both start and end ticks are displayed.",
    )
    line_separation_multiplier: int | None = Field(
        1,
        alias="lineSeparationMultiplier",
        description="The multiplier to be used for line separation, where a higher multiplier leads to greater separation between lines. Utilized if 'separateTimeOverlaps' or 'separateTimelineOverlaps' is true. Default value is 1.",
        ge=0,
    )
    move_first_bends: bool | None = Field(
        True,
        alias="moveFirstBends",
        description="When 'separateTimeLineOverlaps' is true, indicates whether the first bend on a relationship line related to an event is raised up above the event location, to reduce overlapping. Default value is true. Only valid in mono-timeline layouts.",
    )
    second_best_ratio: float | None = Field(
        0.3,
        alias="secondBestRatio",
        description="Ratio from 0 to 1 controlling the position at which the second bend of a relationship occurs, for event and non-event relationships between event and non-event entities. Lower values move the second bend position lower. Default value is 0.3. Only used in the mono-timeline layout.",
        ge=0,
        le=1,
    )
    separated_line_shape_ratio: float | None = Field(
        0,
        alias="separatedLineShapeRatio",
        description="When 'separateTimeOverlaps' is true, adjusts the angle between the extremities of the original and separated lines. When the ratio is high, the angle is small. Default value is 0.",
        ge=0,
        le=1,
    )
    separate_timeline_overlaps: bool | None = Field(
        True,
        alias="separateTimelineOverlaps",
        description="Indicates whether to separate overlapping timelines. Default value is true.",
    )
    show_duration_line_for_non_zero_duration_entity_events: bool | None = Field(
        True,
        alias="showDurationLineForNonZeroDurationEntityEvents",
        description="Determines whether or not lines representing the duration of events on entities with non-zero durations are shown. The default is true, meaning the lines are displayed.",
    )
    show_non_zero_duration_interval_bounds: bool | None = Field(
        False,
        alias="showNonZeroDurationIntervalBounds",
        description="Determines whether to display interval bounds (tick lines at the start and end) of relationship lines for events with non-zero durations. Only used in multi-timeline layouts. Default is false.",
    )
    space_separated_lines_evenly: bool | None = Field(
        False,
        alias="spaceSeparatedLinesEvenly",
        description="Determines whether or not to space separated lines evenly, when either 'separateTimeOverlaps' or 'separateTimelineOverlaps' is true. When true, the offset for the i-th overlapping line is proportional to 'i'. If false, the offset is proportional to the square root of i. Default is false.",
    )
    time_banner_utc_offset_in_minutes: int | None = Field(
        0,
        alias="timeBannerUtcOffsetInMinutes",
        description="The UTC offset in minutes to be used for the time banner. Default value is 0.",
    )
    time_direction: TimeDirectionType | None = Field(
        TimeDirectionType.right,
        alias="timeDirection",
        description="Controls the time axis orientation and the direction along which time increases in the layout, e.g. 'right' means that time axis is horizontal and time increases from left to right, 'bottom' means the time axis is vertical and time increases from top to bottom. Default is 'right'.",
    )
    use_bezier_curves: bool | None = Field(
        False,
        alias="useBezierCurves",
        description="Determines whether or not to use Bezier curves for separated lines. This setting will have no effect where Bezier curves are not supported. Default value is false.",
    )


class LinkChartAggregationLayer(BaseModel):
    """
    Link chart aggregation layer represents aggregated entities or relationships.
    This is a child of a link chart sub layer and the graphTypeName uniquely identifies the
    name of the entity or relationship being aggregated.
    """

    model_config = common_config
    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    charts: list[dict[str, Any]] | None = Field(
        None,
        description="An array of chart items of type WebChart available on the layer.",
    )
    disable_popup: bool | None = Field(
        True,
        alias="disablePopup",
        description="Indicates whether a client should ignore popups defined in this layer",
    )
    display_expression_info: ExpressionInfo | None = Field(
        None,
        alias="displayExpressionInfo",
        description="Object defining Arcade expression that will return a display name used for listing entities or relationships. This Arcade expression profile expects the returnType to be always a string.",
    )
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
        title="Effect",
    )
    feature_effect: FeatureEffect | None = Field(None, alias="featureEffect")
    graph_type: GraphType = Field(
        ...,
        alias="graphType",
        description="Indicates the type of graph object.",
    )
    graph_type_name: str = Field(
        ...,
        alias="graphTypeName",
        description="Represents a unique identifier for a knowledge graph entity or relationship. If graphTypeName represents entity, features can be fetched from the featureset persisted as pbf specified as aggregatedEntitiesUrl in the parent's [link chart properties](linkChartProperties.md) and relationship features from aggregatedRelationshipsUrl.",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    layer_definition: LayerDefinition | None = Field(
        None,
        alias="layerDefinition",
        description="A layerDefinition object defining the attribute schema and drawing information for the layer.",
    )
    layer_type: Literal["LinkChartAggregationLayer"] = Field(
        "LinkChartAggregationLayer",
        alias="layerType",
        description="String indicating the layer type.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    popup_info: PopupInfo | None = Field(
        None,
        alias="popupInfo",
        description="A popupInfo object defining the content of popup window when you click a feature on the map.",
    )
    refresh_interval: float | None = Field(
        0,
        alias="refreshInterval",
        description="Refresh interval of the layer in minutes. Non-zero value indicates automatic layer refresh at the specified interval. Value of 0 indicates auto refresh is not enabled.",
    )
    show_labels: bool | None = Field(
        False,
        alias="showLabels",
        description="Labels will display if this property is set to `true` and the layer also has a [labelingInfo](labelingInfo.md) property associated with it. This property can get stored in the web map config and in the item/data.",
    )
    show_legend: bool | None = Field(
        True,
        alias="showLegend",
        description="Boolean value indicating whether to display the layer in the legend. Default value is `true`.",
    )
    time_animation: bool | None = Field(
        True,
        alias="timeAnimation",
        description="Indicates whether to disable time animation if the layer supports it.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )


class LinkChartSubLayer(BaseModel):
    """
    Link chart sub layer represents entity or relationship in a knowledge graph. graphTypeName uniquely identifies the name of the entity or relationship. The features corresponding entities and relationships can be fetched by using graphTypeName filter.
    """

    model_config = common_config

    aggregation_layer: LinkChartAggregationLayer | None = Field(
        None,
        alias="aggregationLayer",
        description="Represents a aggregated link chart sub layer object that contains aggregated entities or relationships specific to graphTypeName for this link chart sub layer.",
    )
    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    charts: list[dict[str, Any]] | None = Field(
        None,
        description="An array of chart items of type WebChart available on the layer.",
    )
    disable_popup: bool | None = Field(
        True,
        alias="disablePopup",
        description="Indicates whether a client should ignore popups defined in this layer",
    )
    display_expression_info: ExpressionInfo | None = Field(
        None,
        alias="displayExpressionInfo",
        description="Object defining Arcade expression that will return a display name used for listing entities or relationships. This Arcade expression profile expects the returnType to be always a string.",
    )
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
    )
    feature_effect: FeatureEffect | None = Field(None, alias="featureEffect")
    graph_type: GraphType = Field(
        ...,
        alias="graphType",
        description="Indicates the type of graph object.",
    )
    graph_type_name: str = Field(
        ...,
        alias="graphTypeName",
        description="Represents a unique identifier for a knowledge graph entity or relationship. If graphTypeName represents entity, features can be fetched from the featureset persisted as pbf specified as entitiesUrl in the parent's [link chart properties](linkChartProperties.md) and relationship features from relationshipsUrl.",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    layer_definition: LayerDefinition | None = Field(
        None,
        alias="layerDefinition",
        description="A layerDefinition object defining the attribute schema and drawing information for the layer.",
    )
    layer_type: Literal["LinkChartSubLayer"] = Field(
        "LinkChartSubLayer",
        alias="layerType",
        description="String indicating the layer type.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    popup_info: PopupInfo | None = Field(
        None,
        alias="popupInfo",
        description="A popupInfo object defining the content of popup window when you click a feature on the map.",
    )
    refresh_interval: float | None = Field(
        0,
        alias="refreshInterval",
        description="Refresh interval of the layer in minutes. Non-zero value indicates automatic layer refresh at the specified interval. Value of 0 indicates auto refresh is not enabled.",
    )
    show_labels: bool | None = Field(
        False,
        alias="showLabels",
        description="Labels will display if this property is set to `true` and the layer also has a [labelingInfo](labelingInfo.md) property associated with it. This property can get stored in the web map config and in the item/data.",
    )
    show_legend: bool | None = Field(
        True,
        alias="showLegend",
        description="Boolean value indicating whether to display the layer in the legend. Default value is `true`.",
    )
    time_animation: bool | None = Field(
        True,
        alias="timeAnimation",
        description="Indicates whether to disable time animation if the layer supports it.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )


class LinkChartLayer(BaseModel):
    """
    Link chart layer can be created by referencing a Knowledge Graph Service. A Link chart layer is a group layer with collections of feature layers and tables representing geospatial and non-geospatial entity and relationship types.
    """

    model_config = common_config

    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    custom_parameters: dict[str, Any] | None = Field(
        None,
        alias="customParameters",
        description="Custom parameters for the layer.",
    )
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
        title="Effect",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="Optional string containing the item ID of the service if it's registered on ArcGIS Online or your organization's portal.",
    )
    layers: list[LinkChartSubLayer] | None = Field(
        None,
        description="An array of Link chart sub layers, each representing graph entity type or relationship in Link chart server.",
    )
    layer_type: Literal["LinkChartLayer"] = Field(
        "LinkChartLayer",
        alias="layerType",
        description="String indicating the layer type.",
    )
    max_scale: float | None = Field(
        0,
        alias="maxScale",
        description="A number representing the maximum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    min_scale: float | None = Field(
        0,
        alias="minScale",
        description="A number representing the minimum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    refresh_interval: int | None = Field(
        0,
        alias="refreshInterval",
        description="Refresh interval of the layer in minutes. Non-zero value indicates automatic layer refresh at the specified interval. Value of 0 indicates auto refresh is not enabled.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    url: str = Field(..., description="A URL to the Knowledge Graph Service.")
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )


class LinkChartOrganicLayoutSettings(BaseModel):
    """
    Settings for organic layout calculations
    """

    absolute_ideal_edge_length: float | None = Field(
        1,
        alias="absoluteIdealEdgeLength",
        description="The value, in degrees, to use for the ideal edge length during layout calculations when the idealEdgeLengthType is 'absoluteValue'. Only used for geographic layouts. Default value is 1.",
        ge=0,
        le=360,
    )
    auto_repulsion_radius: bool = Field(
        True,
        alias="autoRepulsionRadius",
        description="Determines whether the repulsion radius should be calculated automatically (true), or computed according to 'repulsionRadiusMultiplier' (false). Default value is true.",
    )
    ideal_edge_length_type: IdealEdgeLengthType = Field(
        IdealEdgeLengthType.multiplier.value,
        alias="idealEdgeLengthType",
        description="The type of ideal edge length to use during layout calculations. Default value is 'absoluteValue'.",
    )
    multiplication_ideal_edge_length: float | None = Field(
        1,
        alias="multiplicationIdealEdgeLength",
        description="The value to use for the ideal edge length during layout calculations when the idealEdgeLengthType is 'multiplier'. Only used for geographic layouts. Default value is 1.",
        ge=0,
        le=5,
    )
    repulsion_radius_multiplier: float | None = Field(
        1,
        alias="repulsionRadiusMultiplier",
        description="Value to be used for the repulsion radius multiplier in organic layout calculations. The repulsion radius is calculated by multiplying the repulsionRadiusMultiplier by the actual ideal edge length. Default value is 1.",
        ge=1,
        le=99,
    )


class LinkChartLayoutSettings(BaseModel):
    """
    Link chart layout settings.
    """

    model_config = common_config

    chronological_layout_settings: LinkChartChronologicalLayoutSettings | None = Field(
        None,
        alias="chronologicalLayoutSettings",
        description="Settings for chronological layout calculations.",
    )
    organic_layout_settings: LinkChartOrganicLayoutSettings | None = Field(
        None,
        alias="organicLayoutSettings",
        description="Settings for organic layout calculations.",
    )


class NonspatialDataDisplay(BaseModel):
    model_config = common_config
    mode: Literal["hidden", "visible"] = Field(
        "visible",
        description="Category of nonspatial data display mode. Default value is 'visible'.",
    )


class LinkChartProperties(BaseModel):
    """
    Properties that contain source information, layout configurations and other settings for a Link chart.
    """

    model_config = common_config

    aggregated_entities_url: str | None = Field(
        None,
        alias="aggregatedEntitiesUrl",
        description="Url pointing to a binary reference containing a serialized representation of the internal aggregated entity table.",
    )
    aggregated_relationships_url: str | None = Field(
        None,
        alias="aggregatedRelationshipsUrl",
        description="Url pointing to a binary reference containing a serialized representation of the internal aggregated relationship table.",
    )
    auto_collapse_relationships: bool | None = Field(
        True,
        alias="autoCollapseRelationships",
        description="Indicates whether to automatically collapses eligible relationships.",
    )
    centrality_is_up_to_date: bool | None = Field(
        True,
        alias="centralityIsUpToDate",
        description="Indicates whether the Centrality scores found in the entity and relationship tables were computed using the current Link Chart topology. Default value is true.",
    )
    entities_url: str | None = Field(
        None,
        alias="entitiesUrl",
        description="Url pointing to a resource containing featureSet as PBF reference containing a serialized representation of the internal entity table.",
    )
    layout_settings: LinkChartLayoutSettings | None = Field(
        None,
        alias="layoutSettings",
        description="Link chart layout settings.",
    )
    layout_type: LinkChartLayoutType | None = Field(
        LinkChartLayoutType.organic_standard.value,
        alias="layoutType",
        description="Knowledge Graph Link Chart layout algorithm used. Default value is 'organic-standard'.",
    )
    nonspatial_data_display: NonspatialDataDisplay | None = Field(
        None,
        alias="nonspatialDataDisplay",
        description="Object that defines instructions on the visualization of nonspatial link chart data.",
    )
    relationships_url: str | None = Field(
        None,
        alias="relationshipsUrl",
        description="Url pointing to a resource containing featureSet as PBF reference containing a serialized representation of the internal relationship table.",
    )


class WebLinkChart(BaseModel):
    """
    Represents a web link chart.
    """

    model_config = common_config

    application_properties: ApplicationProperties | None = Field(
        None,
        alias="applicationProperties",
        description="Viewing and editing properties of the webmap.",
    )
    authoring_app: str = Field(
        ...,
        alias="authoringApp",
        description="[Required] String value indicating the application that last authored the webmap.",
    )
    authoring_app_version: str = Field(
        ...,
        alias="authoringAppVersion",
        description="[Required] String value indicating the version of the application that last authored the webmap.",
    )
    background: Background | None = Field(
        None,
        description="Background color, background image, and color of the background image.",
    )
    base_map: MapBasemap | None = Field(
        None,
        alias="baseMap",
        description="Basemaps give the web map a geographic context.",
    )
    bookmarks: list[Bookmark] | None = Field(
        None,
        description="A bookmark is a saved extent that allows end users to quickly navigate to a particular area of interest.",
    )
    initial_state: MapInitialState | None = Field(
        None,
        alias="initialState",
        description="Initial state at which to open the map.",
    )
    link_chart_properties: LinkChartProperties | None = Field(
        None,
        alias="linkChartProperties",
        description="Link Chart Properties contains information about source Knowledge Graph, resources and properties to display a link chart.",
    )
    map_floor_info: MapFloorInfo | None = Field(
        None,
        alias="mapFloorInfo",
        description="Contains floor-awareness information for the map.",
    )
    map_range_info: MapRangeInformation | None = Field(
        None, alias="mapRangeInfo", description="Map range information."
    )
    map_type: Literal["webLinkChart"] = Field(
        "webLinkChart",
        alias="mapType",
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
            | LinkChartLayer
            | WCSLayer
            | VideoLayer
        ]
        | None
    ) = Field(
        None,
        alias="operationalLayers",
        description="Operational layers contain business data which are used to make thematic maps.",
    )
    reference_scale: float | None = Field(
        0,
        alias="referenceScale",
        description="A floating-point number representing the reference scale which map symbols are drawn relative to. The number is the scale's denominator. When the reference scale is 0, symbols are always drawn at the same size regardless of the map scale. The referenceScale is only used for Feature Layers that have scaleSymbols:true. Not all applications or layer types support referenceScale yet. In particular, ArcGISOnline will not use the referenceScale when drawing symbols in the browser.",
        ge=0,
    )
    spatial_reference: SpatialReference | None = Field(
        ...,
        alias="spatialReference",
        description="[Required] An object used to specify the spatial reference of the given geometry.",
    )
    tables: list[Table] | None = Field(
        None,
        description="An array of objects representing non-spatial datasets used in the web map.",
    )
    time_zone: str | None = Field(
        None,
        alias="timeZone",
        description="Time zone of the webmap. When applicable, dates and times will be displayed using this time zone. The time zone can be `system`, `unknown` or any named [IANA](https://www.iana.org/time-zones) time zone. See [here](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) for a comprehensive list of time zones. The `system` keyword indicates the webmap will use the time zone currently used by the device loading the webmap. Whereas `unknown` means that dates will be treated as if they do not have an associated time zone.",
    )
    version: str | None = Field(
        "2.32", description="Version of the webmap specification."
    )
    widgets: Widgets | None = Field(
        None,
        description="The widgets object contains widgets that should be exposed to the user.",
    )


LinkChartAggregationLayer.model_rebuild()
LinkChartLayer.model_rebuild()
LinkChartSubLayer.model_rebuild()
WebLinkChart.model_rebuild()
