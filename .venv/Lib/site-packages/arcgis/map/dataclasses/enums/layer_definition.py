from enum import Enum


class DateFormat(Enum):
    """
    A string used with date fields to specify how the date should be formatted.
    """

    day_short_month_year = "dayShortMonthYear"
    day_short_month_year_long_time = "dayShortMonthYearLongTime"
    day_short_month_year_long_time24 = "dayShortMonthYearLongTime24"
    day_short_month_year_short_time = "dayShortMonthYearShortTime"
    day_short_month_year_short_time24 = "dayShortMonthYearShortTime24"
    long_date = "longDate"
    long_date_long_time = "longDateLongTime"
    long_date_long_time24 = "longDateLongTime24"
    long_date_short_time = "longDateShortTime"
    long_date_short_time24 = "longDateShortTime24"
    long_month_day_year = "longMonthDayYear"
    long_month_day_year_long_time = "longMonthDayYearLongTime"
    long_month_day_year_long_time24 = "longMonthDayYearLongTime24"
    long_month_day_year_short_time = "longMonthDayYearShortTime"
    long_month_day_year_short_time24 = "longMonthDayYearShortTime24"
    long_month_year = "longMonthYear"
    short_date = "shortDate"
    short_date_le = "shortDateLE"
    short_date_le_long_time = "shortDateLELongTime"
    short_date_le_long_time24 = "shortDateLELongTime24"
    short_date_le_short_time = "shortDateLEShortTime"
    short_date_le_short_time24 = "shortDateLEShortTime24"
    short_date_long_time = "shortDateLongTime"
    short_date_long_time24 = "shortDateLongTime24"
    short_date_short_time = "shortDateShortTime"
    short_date_short_time24 = "shortDateShortTime24"
    short_month_year = "shortMonthYear"
    year = "year"


class RotationType(Enum):
    """
    Defines the origin and direction of rotation depending on how the angle of rotation was measured. Possible values are `geographic` which rotates the symbol from the north in a clockwise direction and `arithmetic` which rotates the symbol from the east in a counter-clockwise direction.
    """

    arithmetic = "arithmetic"
    geographic = "geographic"


class PointCloudMode(Enum):
    """
    Defines if values should be included or excluded.
    """

    exclude = "exclude"
    include = "include"


class ElevationMode(Enum):
    """
    Determines how the service elevation values are combined with the elevation of the scene.
    """

    absolute_height = "absoluteHeight"
    on_the_ground = "onTheGround"
    relative_to_ground = "relativeToGround"
    relative_to_scene = "relativeToScene"


class VoxelRenderMode(Enum):
    """
    Current rendering mode for the voxel layer. Depending on the rendering mode different voxel layer representations can be shown. `volume` draws the voxel layer as rectangular cuboid (but any slices defined for the voxel layer will change the volume to the area of interest). `surfaces` will represent the layer as a set of surfaces, for example, dynamic sections can define a plane through the volume or isosurfaces can show a specific value as surface.
    """

    surfaces = "surfaces"
    volume = "volume"


class ExaggerationMode(Enum):
    """
    Determines how the vertical exaggeration is applied. `scale-height` scales from the voxel dataset origin only, for example, if a voxel layer has its minimum at sea level the layer will be exaggerated starting from sea level. `scale-height` is the default. `scale-position` also scales the space between voxel dataset origin in the coordinate system origin. This exaggeration mode is identical with exaggeration applied to other layers like feature layers (use the scale position option if you want to draw the voxel layer together with feature based data).
    """

    scale_height = "scale-height"
    scale_position = "scale-position"


class VoxelInterpolation(Enum):
    """
    Interpolation mode
    """

    linear = "linear"
    nearest = "nearest"


class JoinType(Enum):
    """
    The type of join (left outer or left inner).
    """

    esri_left_inner_join = "esriLeftInnerJoin"
    esri_left_outer_join = "esriLeftOuterJoin"


class BarrierWeight(Enum):
    """
    Optional weight of features in AnnotationLayers and DimensionLayers when considered as barriers to labeling. If not set but required, the default value is assumed to be High.
    """

    high = "High"
    low = "Low"
    medium = "Medium"
    none = "None"


class FilterMode(Enum):
    """
    Display filter mode.
    """

    manual = "manual"
    scale = "scale"


class LabelOverlap(Enum):
    """
    String describing whether other labels are allowed to overlap this entity (e.g. feature or label already on the map).<br>`allow` means that labels are allowed to overlap this entity. `avoid` means that labels that would overlap will move as much possible to minimize the overlap. `exclude` means that labels that would overlap are not placed.
    """

    allow = "allow"
    avoid = "avoid"
    exclude = "exclude"


class DeconflictionStrategy(Enum):
    """
    Specifies the approach to use for deconflicting labels with this class against existing, more important, labels. The option 'none' uses the preferred position and can overlap existing labels and features. The option 'static' uses the preferred position but will not overlap existing labels or features. The option 'dynamic' will try to find a position to avoid overlap of labels and features. The option 'dynamicNeverRemove' will choose the position that minimizes overlap of labels and features but can overlap them if necessary.
    """

    dynamic = "dynamic"
    dynamic_never_remove = "dynamicNeverRemove"
    none = "none"
    static = "static"


class LabelPlacement(Enum):
    """
    Preferred position of the label with respect to its feature symbology. If missing, then the default depends on the geometry of the feature: `esriServerPointLabelPlacementAboveRight` for point feature geometries, `esriServerLinePlacementAboveAlong` for line feature geometries, and `esriServerPolygonPlacementAlwaysHorizontal` for polygon feature geometries.
    """

    esri_server_line_placement_above_after = "esriServerLinePlacementAboveAfter"
    esri_server_line_placement_above_along = "esriServerLinePlacementAboveAlong"
    esri_server_line_placement_above_before = "esriServerLinePlacementAboveBefore"
    esri_server_line_placement_above_end = "esriServerLinePlacementAboveEnd"
    esri_server_line_placement_above_start = "esriServerLinePlacementAboveStart"
    esri_server_line_placement_below_after = "esriServerLinePlacementBelowAfter"
    esri_server_line_placement_below_along = "esriServerLinePlacementBelowAlong"
    esri_server_line_placement_below_before = "esriServerLinePlacementBelowBefore"
    esri_server_line_placement_below_end = "esriServerLinePlacementBelowEnd"
    esri_server_line_placement_below_start = "esriServerLinePlacementBelowStart"
    esri_server_line_placement_center_after = "esriServerLinePlacementCenterAfter"
    esri_server_line_placement_center_along = "esriServerLinePlacementCenterAlong"
    esri_server_line_placement_center_before = "esriServerLinePlacementCenterBefore"
    esri_server_line_placement_center_end = "esriServerLinePlacementCenterEnd"
    esri_server_line_placement_center_start = "esriServerLinePlacementCenterStart"
    esri_server_point_label_placement_above_center = (
        "esriServerPointLabelPlacementAboveCenter"
    )
    esri_server_point_label_placement_above_left = (
        "esriServerPointLabelPlacementAboveLeft"
    )
    esri_server_point_label_placement_above_right = (
        "esriServerPointLabelPlacementAboveRight"
    )
    esri_server_point_label_placement_below_center = (
        "esriServerPointLabelPlacementBelowCenter"
    )
    esri_server_point_label_placement_below_left = (
        "esriServerPointLabelPlacementBelowLeft"
    )
    esri_server_point_label_placement_below_right = (
        "esriServerPointLabelPlacementBelowRight"
    )
    esri_server_point_label_placement_center_center = (
        "esriServerPointLabelPlacementCenterCenter"
    )
    esri_server_point_label_placement_center_left = (
        "esriServerPointLabelPlacementCenterLeft"
    )
    esri_server_point_label_placement_center_right = (
        "esriServerPointLabelPlacementCenterRight"
    )
    esri_server_polygon_placement_always_horizontal = (
        "esriServerPolygonPlacementAlwaysHorizontal"
    )


class LineConnection(Enum):
    """
    Specifies the approach to use for connecting line labels with this class.  The option 'none' specifies that line connection should not be performed.  The option 'minimizeLabels' connects lines through intersections while 'unambiguousLabels' allows for labels on sides of intersections to clarify ambiguity with label and feature relationships.
    """

    minimize_labels = "minimizeLabels"
    none = "none"
    unambiguous_labels = "unambiguousLabels"


class MeasureType(Enum):
    """
    Defines whether the horizontal, vertical or direct distance between the start and end points is measured.
    """

    direct = "direct"
    horizontal = "horizontal"
    vertical = "vertical"


class LineOrientation(Enum):
    """
    String specifying whether `labelPlacement` of `Above` (or `Below`) will be interpreted as `Above` (or `Below`) on the `page`, or with respect to the direction of line's geometry (that is, the digitization order in which the vertices are listed). If the `lineOrientation` is set to `page`, then `labelPlacement` of `Above` means the label will be offset perpendicularly from its line segment towards the **top** of the page. If the `lineOrientation` is set to `direction`, then `labelPlacement` of `Above` means the label will be offset perpendicularly **left** from its line segment. If the `lineOrientation` is set to `unconstrained`, then the label will be offset perpendicularly to whichever side of the line geometry has space (defaulting to `Above`, in the `page` sense). `labelPlacement` of `Below` would have the corresponding interpretations.
    """

    direction = "direction"
    page = "page"
    unconstrained = "unconstrained"


class MultiPart(Enum):
    """
    Specifies the approach to use for labeling parts and segments of geometries.
    """

    label_largest = "labelLargest"
    label_per_feature = "labelPerFeature"
    label_per_part = "labelPerPart"
    label_per_segment = "labelPerSegment"


class RemoveDuplicates(Enum):
    """
    Specifies whether or not to remove duplicates and if removing duplicate labels whether or not to do it within just this label class, within all labels of that feature type (e.g. point layers) or across all layers. The removeDuplicatesDistance is used when a value other than none is set.
    """

    all = "all"
    feature_type = "featureType"
    label_class = "labelClass"
    none = "none"


class StackAlignment(Enum):
    """
    This string property indicates whether or not to derive stacking from the text symbol or have dynamic stacking based on the relative position of the label to the feature.
    """

    dynamic = "dynamic"
    text_symbol = "textSymbol"


class StackBreakPosition(Enum):
    """
    This string property indicates whether a row of text should be broken before or after it exceeds the ideal length. If stacking is turned on we can insert a linebreak `before` or `after` the breaking word that overruns the maximum number of characters per row. Using the `before` option means rows will generally be shorter than the stackRowLength although will overrun for individual words larger than this count.
    """

    after = "after"
    before = "before"


class SpatialRelationship(Enum):
    """
    Specifies the spatial relationship used for the filter. `disjoint`: Display features that do not intersect any filter polygon. `contains`: Display features completely inside any filter polygon
    """

    contains = "contains"
    disjoint = "disjoint"


class IncludedReturn(Enum):
    first_of_many = "firstOfMany"
    last = "last"
    last_of_many = "lastOfMany"
    single = "single"


class BreakPosition(Enum):
    """
    Optional property indicating whether a row of text should be broken before or after the character is encountered. We can insert a linebreak `before` or `after` the separator character. This is only useful if the separator character is visible after a linebreak is inserted. Using the `before` option means rows will generally be shorter than the stackRowLength although will overrun for individual words larger than this count. `automatic` will choose the appropriate default for each feature-geometry (currently `after` in all cases). This setting for an individual separator overrides the `labelingInfo.stackBreakPosition` property.
    """

    after = "after"
    automatic = "automatic"
    before = "before"


class TextOrientation(Enum):
    direction = "direction"
    page = "page"


class TextLayout(Enum):
    """
    String describing, once the text is positioned, how the text should be oriented based on the feature geometry. If this property is present, it must be one of the following values: <ul><li>`followFeature`</li><li>`horizontal`</li><li>`perpendicular`</li><li>`straight`</li></ul><br>A value of `followFeature` will make the text curve to follow a line feature (e.g. road or river). A value of `horizontal` will make the text be written horizontally with respect to the page. A value of `straight` will make the text straight and angled depending on the feature geometry: (point) rotated by the specified angle, (line) placed at an angle that follows the line, (polygon) angled to represent the shape of the polygon. A value of `perpendicular` will make the text rotated 90 degrees clockwise from the angle it would have used for `straight`.<br>The default value is `horizontal` for labels attached to point and polygon features, and `followFeature` for labels attached to line features.
    """

    follow_feature = "followFeature"
    horizontal = "horizontal"
    perpendicular = "perpendicular"
    straight = "straight"


class BinType(Enum):
    """
    Determines the type or shape of bins used in the aggregation.
    """

    flat_hexagon = "flatHexagon"
    geohash = "geohash"
    pointy_hexagon = "pointyHexagon"
    square = "square"


class StatisticType(Enum):
    """
    Defines the statistic method for aggregating data in the onStatisticField or `onStatisticExpression` returned from features in a cluster or bin.
    """

    avg = "avg"
    count = "count"
    max = "max"
    min = "min"
    mode = "mode"
    stddev = "stddev"
    sum = "sum"
    var = "var"


class HtmlPopupType(Enum):
    """
    String value indicating the HTML popup type.
    """

    esri_server_html_popup_type_as_html_text = "esriServerHTMLPopupTypeAsHTMLText"
    esri_server_html_popup_type_as_url = "esriServerHTMLPopupTypeAsURL"
    esri_server_html_popup_type_none = "esriServerHTMLPopupTypeNone"


class DrawingTool(Enum):
    """
    An optional string that can define a client-side drawing tool to be used with this feature. For example, map notes used by the Online Map Viewer use this to represent the viewer's different drawing tools.
    """

    esri_feature_edit_tool_auto_complete_polygon = (
        "esriFeatureEditToolAutoCompletePolygon"
    )
    esri_feature_edit_tool_circle = "esriFeatureEditToolCircle"
    esri_feature_edit_tool_down_arrow = "esriFeatureEditToolDownArrow"
    esri_feature_edit_tool_ellipse = "esriFeatureEditToolEllipse"
    esri_feature_edit_tool_freehand = "esriFeatureEditToolFreehand"
    esri_feature_edit_tool_left_arrow = "esriFeatureEditToolLeftArrow"
    esri_feature_edit_tool_line = "esriFeatureEditToolLine"
    esri_feature_edit_tool_none = "esriFeatureEditToolNone"
    esri_feature_edit_tool_point = "esriFeatureEditToolPoint"
    esri_feature_edit_tool_polygon = "esriFeatureEditToolPolygon"
    esri_feature_edit_tool_rectangle = "esriFeatureEditToolRectangle"
    esri_feature_edit_tool_right_arrow = "esriFeatureEditToolRightArrow"
    esri_feature_edit_tool_text = "esriFeatureEditToolText"
    esri_feature_edit_tool_triangle = "esriFeatureEditToolTriangle"
    esri_feature_edit_tool_up_arrow = "esriFeatureEditToolUpArrow"


class TimeIntervalUnits(Enum):
    """
    Temporal unit in which the time interval is measured.
    """

    esri_time_units_centuries = "esriTimeUnitsCenturies"
    esri_time_units_days = "esriTimeUnitsDays"
    esri_time_units_decades = "esriTimeUnitsDecades"
    esri_time_units_hours = "esriTimeUnitsHours"
    esri_time_units_milliseconds = "esriTimeUnitsMilliseconds"
    esri_time_units_minutes = "esriTimeUnitsMinutes"
    esri_time_units_months = "esriTimeUnitsMonths"
    esri_time_units_seconds = "esriTimeUnitsSeconds"
    esri_time_units_unknown = "esriTimeUnitsUnknown"
    esri_time_units_weeks = "esriTimeUnitsWeeks"
    esri_time_units_years = "esriTimeUnitsYears"


class LayerDefinitionType(Enum):
    """
    Indicates whether the layerDefinition applies to a Feature Layer or a Table.
    """

    feature_layer = "Feature Layer"
    table = "Table"


class GeometryType(Enum):
    """
    The type of geometry.
    """

    esri_geometry_envelope = "esriGeometryEnvelope"
    esri_geometry_multipoint = "esriGeometryMultipoint"
    esri_geometry_point = "esriGeometryPoint"
    esri_geometry_polygon = "esriGeometryPolygon"
    esri_geometry_polyline = "esriGeometryPolyline"


class FieldType(Enum):
    """
    A string defining the field type.
    """

    esri_field_type_big_integer = "esriFieldTypeBigInteger"
    esri_field_type_blob = "esriFieldTypeBlob"
    esri_field_type_date = "esriFieldTypeDate"
    esri_field_type_date_only = "esriFieldTypeDateOnly"
    esri_field_type_double = "esriFieldTypeDouble"
    esri_field_type_geometry = "esriFieldTypeGeometry"
    esri_field_type_global_id = "esriFieldTypeGlobalID"
    esri_field_type_guid = "esriFieldTypeGUID"
    esri_field_type_integer = "esriFieldTypeInteger"
    esri_field_type_oid = "esriFieldTypeOID"
    esri_field_type_raster = "esriFieldTypeRaster"
    esri_field_type_single = "esriFieldTypeSingle"
    esri_field_type_small_integer = "esriFieldTypeSmallInteger"
    esri_field_type_string = "esriFieldTypeString"
    esri_field_type_time_only = "esriFieldTypeTimeOnly"
    esri_field_type_timestamp_offset = "esriFieldTypeTimestampOffset"
    esri_field_type_xml = "esriFieldTypeXML"


class EffectFunctionsType(Enum):
    """
    Effect type.
    """

    brightness = "brightness"
    contrast = "contrast"
    grayscale = "grayscale"
    invert = "invert"
    opacity = "opacity"
    saturate = "saturate"
    sepia = "sepia"


class MosaicMethod(Enum):
    """
    A string value that determines how the selected rasters are ordered.
    """

    esri_mosaic_attribute = "esriMosaicAttribute"
    esri_mosaic_center = "esriMosaicCenter"
    esri_mosaic_lock_raster = "esriMosaicLockRaster"
    esri_mosaic_nadir = "esriMosaicNadir"
    esri_mosaic_none = "esriMosaicNone"
    esri_mosaic_northwest = "esriMosaicNorthwest"
    esri_mosaic_seamline = "esriMosaicSeamline"
    esri_mosaic_viewpoint = "esriMosaicViewpoint"


class MosaicOperation(Enum):
    """
    Use the mosaic operation to resolve overlap pixel values: from first or last raster, use the min, max or mean of the pixel values, or blend them.
    """

    mt_blend = "MT_BLEND"
    mt_first = "MT_FIRST"
    mt_last = "MT_LAST"
    mt_max = "MT_MAX"
    mt_mean = "MT_MEAN"
    mt_min = "MT_MIN"
    mt_sum = "MT_SUM"


class FunctionType(Enum):
    """
    Defines whether the `function` is applied to a mosaic dataset. Indicates the level of the mosaic processing is used. Only applies to mosaic based image services. `0` - function is applied after mosaicking; `1` - function is applied on each raster item before mosaicking; `2` - function is applied to a group of raster items before mosaicking.
    """

    number_0 = 0
    number_1 = 1
    number_2 = 2


class OrientedImageryType(Enum):
    """
    String that defines the imagery type used in the particular Oriented Imagery Layer.
    """

    field_360 = "360"
    aerial_360_video = "Aerial360Video"
    aerial_frame_video = "AerialFrameVideo"
    horizontal = "Horizontal"
    inspection = "Inspection"
    nadir = "Nadir"
    oblique = "Oblique"
    terrestrial_360_video = "Terrestrial360Video"
    terrestrial_frame_video = "TerrestrialFrameVideo"


class OrientedImageryTimeUnit(Enum):
    """
    Defines the unit of time used in the viewer's time selector tool. Images will be filtered in the viewer based on the Time Unit value defined here.
    """

    days = "Days"
    hours = "Hours"
    minutes = "Minutes"
    months = "Months"
    weeks = "Weeks"
    years = "Years"


class VerticalMeasurementUnit(Enum):
    """
    Defines the primary unit to be used for all vertical measurements.
    """

    feet = "Feet"
    meter = "Meter"
