from __future__ import annotations  # Enables postponed evaluation of type hints
from typing import ForwardRef
from .base_model import BaseModel, common_config, SymbolValidatorMixin
from pydantic import Field, constr, confloat
from typing import Literal, Any
from ..enums.layers import PixelType, ImageFormat
from ..enums.layer_definition import (
    DateFormat,
    RotationType,
    FieldType,
    FilterMode,
    BreakPosition,
    LabelOverlap,
    DeconflictionStrategy,
    LabelPlacement,
    LineConnection,
    LineOrientation,
    MultiPart,
    RemoveDuplicates,
    StackAlignment,
    StackBreakPosition,
    StatisticType,
    BinType,
    TextLayout,
    GeometryType,
    DrawingTool,
    TextOrientation,
    TimeIntervalUnits,
    JoinType,
    SpatialRelationship,
    ElevationMode,
    IncludedReturn,
    PointCloudMode,
    BarrierWeight,
    HtmlPopupType,
    LayerDefinitionType,
    EffectFunctionsType,
    FunctionType,
    MosaicMethod,
    MosaicOperation,
    OrientedImageryType,
    OrientedImageryTimeUnit,
    VerticalMeasurementUnit,
    ExaggerationMode,
    VoxelInterpolation,
    MeasureType,
    VoxelRenderMode,
)
from .geometry import Extent, SpatialReference
from .renderers import (
    ClassBreaksRenderer,
    DictionaryRenderer,
    DotDensityRenderer,
    FlowRenderer,
    HeatmapRenderer,
    PieChartRenderer,
    PredominanceRenderer,
    RasterColorMapRenderer,
    RasterShadedReliefRenderer,
    SimpleRenderer,
    StretchRenderer,
    TemporalRenderer,
    UniqueValueRenderer,
    VectorFieldRenderer,
    ExpressionInfo,
    PointCloudClassBreaksRenderer,
    PointCloudRGBRenderer,
    PointCloudStretchRenderer,
    PointCloudUniqueValueRenderer,
    RasterPresetRenderer,
)
from .geometry import (
    MultipointGeometry,
    PointGeometry,
    PolygonGeometry,
    PolylineGeometry,
)
from pydantic import field_validator
from .symbols import (
    SketchEdges,
    SolidEdges,
    TextSymbolEsriTS,
    SimpleFillSymbolEsriSFS,
    SimpleLineSymbolEsriSLS,
    SimpleMarkerSymbolEsriSMS,
    PictureFillSymbolEsriPFS,
    CimSymbolReference,
    PictureMarkerSymbolEsriPMS,
    LabelSymbol3D,
    LineSymbol3D,
    MeshSymbol3D,
    PointSymbol3D,
    PolygonSymbol3D,
)
from .forms import InheritedDomain, RangeDomain, CodedValue
from .popups import PopupInfo, OrderByField

DynamicDataLayer = ForwardRef(
    "DynamicDataLayer"
)  # Forward reference for DynamicDataLayer


class WCSInfo(BaseModel):
    """
    WCS (Web Coverage Service) information for the layer.
    """

    coverage_id: str | None = Field(
        ...,
        alias="coverageId",
        description="The coverage ID of the WCS service.",
    )
    version: Literal["1.0.0", "1.1.0", "1.1.1", "1.1.2", "2.0.1"] | None = Field(
        ...,
        description="The version of the WCS service.",
    )


class AttributeTableAttachmentElement(BaseModel):
    """Defines how attachments display within a table."""

    model_config = common_config

    description: str | None = Field(
        None, description="A string that describes the element in detail."
    )
    display_type: Literal["auto"] = Field(
        "auto",
        alias="displayType",
        description="This property applies to elements of type `attachment`. A string value indicating how to display the attachment. ",
    )
    label: str | None = Field(
        None, description="A string value indicating the column label in the table."
    )
    type: Literal["attachment"] = Field(
        "attachment",
        description="String value indicating what the element represents.",
    )


class AttributeTableFieldElement(BaseModel):
    """Defines how a field in the dataset is presented when viewing data in tabular form."""

    model_config = common_config

    field_name: str = Field(
        ...,
        alias="fieldName",
        description="A string containing the field name as defined by the feature layer.",
    )
    type: Literal["field"] = Field(
        "field",
        description="String value indicating what the element represents.",
    )


class AttributeTableRelationshipElement(BaseModel):
    """An object that displays any associated related records for each feature in the table."""

    model_config = common_config

    description: str | None = Field(
        None, description="A string that describes the element in detail."
    )
    label: str | None = Field(
        None, description="A string value indicating the column label in the table."
    )
    relationship_id: int = Field(
        ...,
        alias="relationshipId",
        description="The id of the relationship as defined in the feature layer definition.",
    )
    type: Literal["relationship"] = Field(
        "relationship",
        description="String value indicating what the element represents.",
    )


class AttributeTableGroupElement(BaseModel):
    """Defines a container that holds a set of table elements that can be displayed together."""

    model_config = common_config

    attribute_table_elements: list[
        AttributeTableAttachmentElement
        | AttributeTableFieldElement
        | AttributeTableRelationshipElement
    ] = Field(
        ...,
        alias="attributeTableElements",
        description="An array of Element objects that represent an ordered list of table elements. Nested group elements are not supported.",
    )
    description: str | None = Field(
        None, description="A string that describes the element in detail."
    )
    label: str | None = Field(
        None,
        description="A unique string representing the group column label in the table.",
    )
    type: Literal["group"] = Field(
        "group",
        description="String value indicating what the element represents.",
    )


class AttributeTableInfo(BaseModel):
    """Defines the look and feel of the data table when showing data in a tabular format."""

    model_config = common_config

    attribute_table_elements: list[
        AttributeTableAttachmentElement
        | AttributeTableFieldElement
        | AttributeTableGroupElement
        | AttributeTableRelationshipElement
    ] = Field(
        ...,
        alias="attributeTableElements",
        description="An array of Element objects that represent an ordered list of table elements.",
    )
    order_by_fields: list[OrderByField] | None = Field(
        None,
        alias="orderByFields",
        description="Array of orderByField objects indicating the display order for the related records, and whether they should be sorted in ascending <code>'asc'</code> or descending <code>'desc'</code> order.",
    )


class Parameter(BaseModel):
    """
    Objects defined by a [definitionEditor](definitionEditor.md) input.
    """

    model_config = common_config
    default_value: float | str | None = Field(
        None,
        alias="defaultValue",
        description="The default value that is automatically given is nothing is provided.",
    )
    field_name: str | None = Field(
        None,
        alias="fieldName",
        description="A string value representing the name of the field to query.",
    )
    parameter_id: int | None = Field(
        None,
        alias="parameterId",
        description="Number given to uniquely identify the specified parameter.",
    )
    type: FieldType | None = Field(
        None, description="The field type for the specified field parameter."
    )
    utc_value: int | None = Field(
        None,
        alias="utcValue",
        description="An integer value representing exact UNIX time used when `defaultValue` is a date string.",
    )


class Input(BaseModel):
    """
    The input objects specified by the [definitionEditor](definitionEditor.md).
    """

    model_config = common_config
    hint: str | None = Field(
        None, description="A string value representing a hint for the input."
    )
    parameters: list[Parameter] | None = Field(
        None, description="An array of parameter objects."
    )
    prompt: str | None = Field(
        None,
        description="A string value representing the prompt for the input.",
    )


class DefinitionEditor(BaseModel):
    """
    The definitionEditor stores interactive filters at the same level as layerDefinition.
    """

    model_config = common_config
    inputs: list[Input] | None = Field(None, description="An array of input objects.")
    parameterized_expression: str | None = Field(
        None,
        alias="parameterizedExpression",
        description="A string value representing the where clause for the interactive filter.",
    )


class DisplayFilter(BaseModel):
    """
    Display filters information.
    """

    model_config = common_config
    id: str = Field(..., description="Display filter identifier.")
    max_scale: float | None = Field(
        0,
        alias="maxScale",
        description="Maximum, but excluding, scale up to which this display filter should be active and honored in the display. A value of 0 implies no maximum scale is specified.",
    )
    min_scale: float | None = Field(
        0,
        alias="minScale",
        description="Minimal, and including, scale up to which the display filter should be active and honored in the display. A value of 0 implies no minimum scale is specified.",
    )
    title: str | None = Field(
        None, description="Human-readable title for the display filter."
    )
    where: str | None = Field(
        None,
        description="SQL-based where clause that narrows the data to be rendered for display purposes. When this element is empty or missing all features will be rendered for this display filter.",
    )


class DisplayFilterInfo(BaseModel):
    """
    Display filters provide information about which features should be rendered on the display. Unlike definition expression which filters the data for tables/charts, display filters are meant for decluttering the display only. These filters should be applied only while drawing features. These filters should not be applied when showing data in a table, tools that are participating in editing data, highlighting etc. Display filters are applied before the feature reduction like binning or clustering.
    """

    model_config = common_config
    active_filter_id: str | None = Field(
        None,
        alias="activeFilterId",
        description="Active display filter id. This property is required when the mode (`filterMode`) is set to `manual`.",
    )
    filter_mode: FilterMode = Field(
        ..., alias="filterMode", description="Display filter mode."
    )
    filters: list[DisplayFilter] = Field(
        ...,
        description="Display filters that contain filters describing which features should be rendered on display. For the `scale` mode, zero or more filters to applied are selected based on the map scale. Filters cannot have overlapping scale ranges. When two or more filters are found for the map scale, the first filter in the list is applied.",
    )


class FieldInfoFormat(BaseModel):
    """
    The format object can be used with numerical or date fields to provide more detail about how values should be formatted for display.
    """

    model_config = common_config
    date_format: DateFormat | None = Field(
        None,
        alias="dateFormat",
        description="A string used with date fields to specify how the date should be formatted.",
    )
    digit_separator: bool | None = Field(
        None,
        alias="digitSeparator",
        description="A Boolean used with numerical fields. A value of true allows the number to have a digit (or thousands) separator. Depending on the locale, this separator is a decimal point or a comma. A value of false means that no separator will be used.",
    )
    places: int | None = Field(
        None,
        description="An integer used with numerical fields to specify the number of decimal places. Any places beyond this value are rounded.",
    )


class FieldInfoLabelingInfo(BaseModel):
    """
    Defines how a field in the dataset is formatted when displayed as label on the map.
    """

    model_config = common_config
    field_name: str | None = Field(
        None,
        alias="fieldName",
        description="A string containing a field name as defined by the service.",
    )
    format: FieldInfoFormat | None = Field(
        None,
        description="An object specifying number or date formatting options.",
    )


class LabelAngleInfo(BaseModel):
    """
    This object specifies the angular positions and layout directions for labels on or around point feature symbols. This may be different for each feature (driven by one or more feature attributes) or constant for all features (specified by a fixed number)
    """

    model_config = common_config
    angle_expression_info: ExpressionInfo = Field(
        ...,
        alias="angleExpressionInfo",
        description="This `expressionInfo` object allows specifies how the angle (in degrees) for a label is calculated from the feature attributes. It may use attributes, fixed numbers, or a combination of both. If missing, an angle value of zero is assumed.",
    )
    rotation_type: RotationType | None = Field(
        RotationType.arithmetic,
        validate_default=True,
        alias="rotationType",
        description="Optional specification of whether the placement angle calculated by the `angleExpressionInfo` should be interpreted as `arithmetic` (counter-clockwise from East) or `geographic` (clockwise from North).",
    )


class LabelExpressionInfo(BaseModel):
    """
    The labelExpressionInfo allows label text to be read similar to that of Popups's description property.
    """

    model_config = common_config
    expression: str | None = Field(
        None,
        description="An [Arcade expression](https://developers.arcgis.com/arcade/) evaluating to either a string or a number.",
    )
    title: str | None = Field(
        None, description="The title of the expression. (optional)"
    )


class LabelStackSeparator(BaseModel):
    """
    The `labelStackSeparator` object specifies a character that indicates where a linebreak may, or must, be inserted into  a text label.
    """

    model_config = common_config
    break_position: BreakPosition | None = Field(
        BreakPosition.after,
        validate_default=True,
        alias="breakPosition",
        description="Optional property indicating whether a row of text should be broken before or after the character is encountered. We can insert a linebreak `before` or `after` the separator character. This is only useful if the separator character is visible after a linebreak is inserted. Using the `before` option means rows will generally be shorter than the stackRowLength although will overrun for individual words larger than this count. `automatic` will choose the appropriate default for each feature-geometry (currently `after` in all cases). This setting for an individual separator overrides the `labelingInfo.stackBreakPosition` property.",
    )
    forced: bool | None = Field(
        False,
        description="Optional property describing whether a linebreak must be inserted, when the character is encountered.",
    )
    separator: str = Field(..., description="Single character (unicode codepoint).")
    visible: bool | None = Field(
        False,
        description="Optional property describing whether the character still be visible, if the character is used as a linebreak (e.g. keep a hyphenation mark vs hide a separator)",
    )


class LabelingInfo(BaseModel):
    """
    The labelingInfo object specifies the label definition for a layer.
    """

    model_config = common_config
    allow_overlap_of_feature_boundary: LabelOverlap | None = Field(
        LabelOverlap.allow,
        validate_default=True,
        alias="allowOverlapOfFeatureBoundary",
        description="A string describing whether other labels are allowed to overlap this polygon feature's edge.<br>`allow` means that labels are allowed to overlap this polygon feature boundary. `avoid` means that labels that would overlap will move as much possible to minimize the overlap. `exclude` means that labels that would overlap are not placed.",
    )
    allow_overlap_of_feature_interior: LabelOverlap | None = Field(
        LabelOverlap.allow,
        validate_default=True,
        alias="allowOverlapOfFeatureInterior",
        description="A string describing how much other labels are allowed to overlap this feature.<br>`allow` means that labels are allowed to overlap this feature. `avoid` means that labels that would overlap will move as much possible to minimize the overlap. `exclude` means that labels that would overlap are not placed.",
    )
    allow_overlap_of_label: LabelOverlap | None = Field(
        LabelOverlap.exclude,
        validate_default=True,
        alias="allowOverlapOfLabel",
        description="A string describing whether other labels are allowed to overlap this label.<br>`allow` means that labels are allowed to overlap this label. `avoid` means that labels that would overlap will move as much possible to minimize the overlap. `exclude` means that labels that would overlap are not placed.",
    )
    allow_overrun: bool | None = Field(
        None,
        alias="allowOverrun",
        description="Specifies whether or not a label can overrun the geometry feature being labeled. Only applicable to labels for lines or polygons. If missing, then the default depends on the geometry of the feature: `false` for line feature geometries, and `true` for polygon feature geometries.",
    )
    deconfliction_strategy: DeconflictionStrategy | None = Field(
        DeconflictionStrategy.static,
        validate_default=True,
        alias="deconflictionStrategy",
        description="Specifies the approach to use for deconflicting labels with this class against existing, more important, labels. The option 'none' uses the preferred position and can overlap existing labels and features. The option 'static' uses the preferred position but will not overlap existing labels or features. The option 'dynamic' will try to find a position to avoid overlap of labels and features. The option 'dynamicNeverRemove' will choose the position that minimizes overlap of labels and features but can overlap them if necessary.",
    )
    field_infos: list[FieldInfoLabelingInfo] | None = Field(
        None,
        alias="fieldInfos",
        description="An array of objects providing formatting information for the label field.",
    )
    label_angle_info: LabelAngleInfo | None = Field(
        None,
        alias="labelAngleInfo",
        description="Optional object specifying how to position a label following the direction of an angle. These properties will be used if the expression is not empty. The `labelPlacement` will still be used to indicate whether offset or centered positioning is required, but the exact position will be given by the angle calculated for the feature. Once the position has been determined, `textLayout` and `textOrientation` are used to specify the layout of the text at that position.",
    )
    label_expression: str | None = Field(
        None,
        alias="labelExpression",
        description="Read-only property specifying text for labels using [simple expressions](https://resources.arcgis.com/en/help/rest/apiref/label.html#class). Prefer to use `labelExpressionInfo` instead. This `labelExpression` property is only used if `labelExpressionInfo` is missing. An empty expression will result in no labels being created.",
    )
    label_expression_info: LabelExpressionInfo | None = Field(
        None,
        alias="labelExpressionInfo",
        description="Expression script object specifying the text that should be used as a label for each feature. This expression may combine information from the feature attributes with fixed strings. An empty expression will result in no labels being created. If this object isn't present then the `labelExpression` property will be used as a fallback.",
    )
    label_placement: LabelPlacement | None = Field(
        None,
        alias="labelPlacement",
        description="Preferred position of the label with respect to its feature symbology. If missing, then the default depends on the geometry of the feature: `esriServerPointLabelPlacementAboveRight` for point feature geometries, `esriServerLinePlacementAboveAlong` for line feature geometries, and `esriServerPolygonPlacementAlwaysHorizontal` for polygon feature geometries.",
    )
    line_connection: LineConnection | None = Field(
        LineConnection.minimize_labels,
        validate_default=True,
        alias="lineConnection",
        description="Specifies the approach to use for connecting line labels with this class.  The option 'none' specifies that line connection should not be performed.  The option 'minimizeLabels' connects lines through intersections while 'unambiguousLabels' allows for labels on sides of intersections to clarify ambiguity with label and feature relationships.",
    )
    line_orientation: LineOrientation | None = Field(
        LineOrientation.page,
        validate_default=True,
        alias="lineOrientation",
        description="String specifying whether `labelPlacement` of `Above` (or `Below`) will be interpreted as `Above` (or `Below`) on the `page`, or with respect to the direction of line's geometry (that is, the digitization order in which the vertices are listed). If the `lineOrientation` is set to `page`, then `labelPlacement` of `Above` means the label will be offset perpendicularly from its line segment towards the **top** of the page. If the `lineOrientation` is set to `direction`, then `labelPlacement` of `Above` means the label will be offset perpendicularly **left** from its line segment. If the `lineOrientation` is set to `unconstrained`, then the label will be offset perpendicularly to whichever side of the line geometry has space (defaulting to `Above`, in the `page` sense). `labelPlacement` of `Below` would have the corresponding interpretations.",
    )
    max_scale: float | None = Field(
        None,
        alias="maxScale",
        description="Represents the maximum scale at which the layer definition will be applied.",
    )
    min_scale: float | None = Field(
        None,
        alias="minScale",
        description="Represents the minimum scale at which the layer definition will be applied.",
    )
    multi_part: MultiPart | None = Field(
        MultiPart.label_per_part,
        validate_default=True,
        alias="multiPart",
        description="Specifies the approach to use for labeling parts and segments of geometries.",
    )
    name: str | None = Field(
        None,
        description="The name of the label class. May be used to identify members within a collection of label classes e.g. attached to a feature layer",
    )
    offset_distance: float | None = Field(
        1,
        alias="offsetDistance",
        description="Specification of the screen distance (in points) between the feature symbol geometry and an offset label.",
    )
    priority: float | None = Field(
        None,
        description="The priority of the label class relative to other label classes. When there is not enough space on the map for all labels, important labels will be placed, at the expense of less important labels. Priorities should be positive with 0 being the most important and higher numbers being less important. If missing, the default depends on the geometry of the feature: `12` for point feature geometries, `15` for line feature geometries, and `18` for polygon feature geometries.",
    )
    remove_duplicates: RemoveDuplicates | None = Field(
        RemoveDuplicates.none,
        validate_default=True,
        alias="removeDuplicates",
        description="Specifies whether or not to remove duplicates and if removing duplicate labels whether or not to do it within just this label class, within all labels of that feature type (e.g. point layers) or across all layers. The removeDuplicatesDistance is used when a value other than none is set.",
    )
    remove_duplicates_distance: float | None = Field(
        0,
        alias="removeDuplicatesDistance",
        description="The screen distance in points to remove duplicates within. The value 0 is a special value and indicates to remove duplicates for the entire extent.",
    )
    repeat_label: bool | None = Field(
        None,
        alias="repeatLabel",
        description="A boolean value indicating whether or not to repeat the label along or across the feature. If true, the label will be repeated according to the repeatLabelDistance. If missing, the default depends on the geometry of the feature: false for point and polygon feature geometries, and true for line feature geometries.",
    )
    repeat_label_distance: float | None = Field(
        216,
        alias="repeatLabelDistance",
        description="The repeat label distance used when repeatLabel is true. It represents a screen distance in points.",
    )
    stack_alignment: StackAlignment | None = Field(
        StackAlignment.text_symbol,
        validate_default=True,
        alias="stackAlignment",
        description="This string property indicates whether or not to derive stacking from the text symbol or have dynamic stacking based on the relative position of the label to the feature.",
    )
    stack_break_position: StackBreakPosition | None = Field(
        StackBreakPosition.after,
        validate_default=True,
        alias="stackBreakPosition",
        description="This string property indicates whether a row of text should be broken before or after it exceeds the ideal length. If stacking is turned on we can insert a linebreak `before` or `after` the breaking word that overruns the maximum number of characters per row. Using the `before` option means rows will generally be shorter than the stackRowLength although will overrun for individual words larger than this count.",
    )
    stack_label: bool | None = Field(
        None,
        alias="stackLabel",
        description="A boolean value indicating whether or not to stack (i.e. insert linebreaks into) long labels for this label class. If missing, the default depends on the geometry of the feature: `true` for point and polygon feature geometries, and `false` for line feature geometries.",
    )
    stack_row_length: float | None = Field(
        -1,
        alias="stackRowLength",
        description="The ideal number of characters to place on a row of stacked text. This length guides the decision on when to break long text strings into separate rows. The linebreak will be inserted between words, not in the middle of a word, so rows may be longer or shorter than the ideal. Depending on `stackBreakPosition`, the break may be inserted before the breaking word, or after. `stackRowLength` values of one or higher will cause linebreaks to be added when a row will exceed this length. Values of zero will cause linebreaks to be added whenever possible i.e. after every word. Values less than zero will cause a default length to be used (currently 9 characters, but may vary by feature geometry type).",
    )
    stack_separators: list[LabelStackSeparator] | None = Field(
        None,
        alias="stackSeparators",
        description="Array of which characters in a text label can indicate a line-break. By default, space and comma indicate optional linebreaks, and newline indicates a forced linebreak. If an empty array of stackSeparators is specified, then these default optional indicators are turned off. If any separator list, other than the two default optional separators, is specified then that list replaces the default list. If the user requires a mixture of default separators and custom separators, then they need to specify all of them.",
    )
    symbol: TextSymbolEsriTS | LabelSymbol3D | None = Field(
        None, description="The text symbol used to label."
    )
    text_layout: LabelSymbol3D | TextLayout | None = Field(
        None,
        alias="textLayout",
        description="String describing, once the text is positioned, how the text should be oriented based on the feature geometry. If this property is present, it must be one of the following values: <ul><li>`followFeature`</li><li>`horizontal`</li><li>`perpendicular`</li><li>`straight`</li></ul><br>A value of `followFeature` will make the text curve to follow a line feature (e.g. road or river). A value of `horizontal` will make the text be written horizontally with respect to the page. A value of `straight` will make the text straight and angled depending on the feature geometry: (point) rotated by the specified angle, (line) placed at an angle that follows the line, (polygon) angled to represent the shape of the polygon. A value of `perpendicular` will make the text rotated 90 degrees clockwise from the angle it would have used for `straight`.<br>The default value is `horizontal` for labels attached to point and polygon features, and `followFeature` for labels attached to line features.",
    )
    text_orientation: TextOrientation | None = Field(
        TextOrientation.page.value,
        validate_default=True,
        alias="textOrientation",
        description="String specifying whether whether text should follow the placement angle direction even if it means being rendered upside-down, or whether text should be flipped through 180 degrees to keep it page-oriented.",
    )
    use_clipped_geometry: bool | None = Field(
        "true",
        alias="useClippedGeometry",
        description="Boolean value indicating whether label positioning should be based on the original unclipped geometry, or on the geometry after it has been clipped to the screen extent. Only applicable to labels for lines or polygons.",
    )
    use_coded_values: bool | None = Field(
        "false",
        alias="useCodedValues",
        description="Boolean value indicating whether to display the coded values for the field names referenced from the `labelExpression` or `labelExpressionInfo.value`. Note that if an Arcade `labelExpresionInfo.expression` is being used, then `useCodedValues` is ignored, as Arcade scripts explicitly specify when to decode and encode values.",
    )
    where: str | None = Field(
        None,
        description="SQL string template used to determine which features to label.",
    )


class RasterDrawingInfo(BaseModel):
    """
    Contains the drawing information for image service based layers.
    """

    model_config = common_config

    renderer: (
        RasterShadedReliefRenderer
        | RasterColorMapRenderer
        | FlowRenderer
        | StretchRenderer
        | VectorFieldRenderer
        | UniqueValueRenderer
        | ClassBreaksRenderer
        | None
    ) = Field(
        None,
        description="An object defining the symbology for the layer.",
    )


class RasterLayerDefinition(BaseModel):
    """
    An object that defines teh drawing information for image service based layers.
    """

    model_config = common_config

    active_preset_renderer_name: str | None = Field(
        None,
        alias="activePresetRendererName",
        description="The name of the active preset renderer.",
    )
    definition_expression: str | None = Field(
        None,
        alias="definitionExpression",
        description="SQL-based definition expression that narrows the data to be displayed in the layer.",
    )
    drawing_info: RasterDrawingInfo | None = Field(
        None,
        alias="drawingInfo",
        description="An object defining the drawing information for the layer.",
    )
    preset_renderers: list[RasterPresetRenderer] | None = Field(
        None,
        alias="presetRenderers",
        description="An array of preset renderers for image service layers, each contains associated information of multidimensional variable or raster function template.",
    )


class VideoLayerDrawingInfo(BaseModel):
    """
    The drawing info object contains drawing information for a video layer.
    """

    model_config = common_config
    frame_center_symbol: SimpleMarkerSymbolEsriSMS | None = Field(
        None,
        alias="frameCenterSymbol",
        description="The symbol used to represent the center point of the video frame.",
    )
    frame_outline_symbol: SimpleLineSymbolEsriSLS | None = Field(
        None,
        alias="frameOutlineSymbol",
        description="A symbol used to represent the coverage area of a video frame.",
    )
    sensor_sight_line_symbol: SimpleLineSymbolEsriSLS | None = Field(
        None,
        alias="sensorSightLineSymbol",
        description="A symbol used to represent the line of sight from the sensor to the center of the frame coverage area.",
    )
    sensor_symbol: (
        SimpleMarkerSymbolEsriSMS
        | PictureMarkerSymbolEsriPMS
        | CimSymbolReference
        | None
    ) = Field(
        None,
        alias="sensorSymbol",
        description="The symbol used to represent the sensor.",
    )
    sensor_trail_symbol: SimpleLineSymbolEsriSLS | None = Field(
        None,
        alias="sensorTrailSymbol",
        description="A symbol used to represent the trail of the sensor.",
    )


class DrawingInfo(BaseModel):
    """
    The drawingInfo object contains drawing information for a feature layer.
    """

    model_config = common_config
    fixed_symbols: bool | None = Field(
        None,
        alias="fixedSymbols",
        description="Only used for feature collections with a renderer. The feature's symbol is defined by the layer's renderer.",
    )
    labeling_info: list[LabelingInfo] | None = Field(
        None,
        alias="labelingInfo",
        description="An object defining the properties used for labeling the layer. If working with [Map Image layers](mapServiceLayer.md), this property is only applicable if the layer is enabled with [dynamic layers](http://enterprise.arcgis.com/en/server/latest/publish-services/windows/about-dynamic-layers.htm)",
    )
    renderer: (
        ClassBreaksRenderer
        | DictionaryRenderer
        | DotDensityRenderer
        | HeatmapRenderer
        | PointCloudClassBreaksRenderer
        | PointCloudRGBRenderer
        | PointCloudStretchRenderer
        | PointCloudUniqueValueRenderer
        | PieChartRenderer
        | PredominanceRenderer
        | SimpleRenderer
        | TemporalRenderer
        | UniqueValueRenderer
        | None
    ) = Field(
        None,
        description="An object defined which provides the symbology for the layer. If working with [Map Image layers](mapServiceLayer.md), this property is only applicable if the layer is enabled with [dynamic layers](http://enterprise.arcgis.com/en/server/latest/publish-services/windows/about-dynamic-layers.htm)",
        title="renderer",
    )
    scale_symbols: bool | None = Field(
        None,
        alias="scaleSymbols",
        description="Boolean property indicating whether symbols should stay the same size in screen units as you zoom in. A value of `false` means the symbols stay the same size in screen units regardless of the map scale.",
    )
    show_labels: bool | None = Field(
        None,
        alias="showLabels",
        description="Defines whether a labels should be shown or not. This is only valid for sublayers.",
    )
    transparency: confloat(ge=0.0, le=100.0) | None = Field(
        None,
        description="Number value ranging between 0 (no transparency) to 100 (completely transparent).",
    )


class FeatureReductionBinningDrawingInfo(BaseModel):
    """
    The feature reduction binning drawingInfo object contains drawing information, such as labelingInfo and renderer, for feature layer binning.
    """

    model_config = common_config
    labeling_info: list[LabelingInfo] | None = Field(
        None,
        alias="labelingInfo",
        description="An object defining the properties used for labeling bins.",
    )
    renderer: (
        ClassBreaksRenderer
        | DictionaryRenderer
        | DotDensityRenderer
        | FlowRenderer
        | HeatmapRenderer
        | PieChartRenderer
        | PredominanceRenderer
        | RasterColorMapRenderer
        | RasterShadedReliefRenderer
        | SimpleRenderer
        | StretchRenderer
        | TemporalRenderer
        | UniqueValueRenderer
        | VectorFieldRenderer
        | None
    ) = Field(
        None,
        description="An object defining the symbology of the bins.",
        title="renderer",
    )


class FeatureReductionClusterDrawingInfo(BaseModel):
    """
    The feature reduction cluster drawingInfo object contains drawing information, such as labelingInfo, for featureReduction (e.g. clustering) on a feature layer.
    """

    model_config = common_config
    labeling_info: list[LabelingInfo] | None = Field(
        None,
        alias="labelingInfo",
        description="An object defining the properties used for labeling clusters.",
    )
    renderer: (
        ClassBreaksRenderer
        | DictionaryRenderer
        | DotDensityRenderer
        | FlowRenderer
        | HeatmapRenderer
        | PieChartRenderer
        | PredominanceRenderer
        | RasterColorMapRenderer
        | RasterShadedReliefRenderer
        | SimpleRenderer
        | StretchRenderer
        | TemporalRenderer
        | UniqueValueRenderer
        | VectorFieldRenderer
        | None
    ) = Field(
        None,
        description="An object defining the symbology of the cluster which provides the symbology for the layer. If not defined, web clients will infer a cluster style from the layer's renderer. Other clients may infer a style or set a default cluster style. If no visual variables are present in a simpleRenderer or a pieChartRenderer, then cluster sizes will automatically be rendered based on the clusterMinSize and clusterMaxSize.",
        title="renderer",
    )


class AggregateField(BaseModel):
    """
    Defines an aggregate field for use in FeatureReductionBinning or FeatureReductionCluster.
    """

    model_config = common_config
    alias: str | None = Field(
        None,
        description="The alias or text used to describe the aggregate field in the legend and popup.",
    )
    is_auto_generated: bool | None = Field(
        False,
        alias="isAutoGenerated",
        description="Only applicable to cluster renderers. A Boolean indicating whether the field was automatically created internally by the JS API's rendering engine for use by an inferred cluster renderer. Set it to `false` for fields manually created by the user. Default is `false`.",
    )
    name: str | None = Field(
        None,
        description="A unique name identifying the aggregate field. For clarity, this name could include the layer's field being aggregated as defined in onStatisticField along with the statisticType. For example, when creating a  field aggregating the values from a layer's 'population' field, you could name the field 'population_sum'.",
    )
    on_statistic_expression: ExpressionInfo | None = Field(
        None,
        alias="onStatisticExpression",
        description="Defines an [Arcade expression](https://developers.arcgis.com/arcade/) evaluating to either a string or a number. This expression typically originates from the source layer's renderer. A string may be returned when calculating 'mode', but a number must be returned for all other statistic types. The `returnType` and `expression` properties in this object are required, but `title` is optional. This may be used as an alternative to `onStatisticField` for aggregating data.",
    )
    on_statistic_field: str | None = Field(
        None,
        alias="onStatisticField",
        description="A field from the source layer to aggregate based on a given statistic type. Alternatively, you can aggregate data based on an Arcade expression in `onStatisticExpression`. If present, this value always takes precedent over `onStatisticExpression`.",
    )
    statistic_type: StatisticType | None = Field(
        None,
        alias="statisticType",
        description="Defines the statistic method for aggregating data in the onStatisticField or `onStatisticExpression` returned from features in a cluster or bin.",
    )


class FeatureReductionScaleVisibilityInfo(BaseModel):
    """
    Properties describing how to set the visible scale range for displaying the renderer in a feature reduction visualization.
    """

    model_config = common_config
    max_scale: confloat(ge=0.0) | None = Field(
        0,
        alias="maxScale",
        description="Integer describing the maximum scale at which the feature reduction renderer is displayed.",
    )
    type: Literal["scale"] = Field(
        "scale",
        description="Describes the threshold type for determining the visibility of the feature reduction renderer",
    )


class FeatureReductionSelection(BaseModel):
    """
    Feature reduction of type `selection` declutters the screen by hiding features that would otherwise intersect with other features on screen. The default behavior of this feature reduction type is to select features by depth order, i.e. hide all features that would otherwise be overlapped by at least one other feature which is closer to the viewer.
    """

    model_config = common_config

    type: Literal["selection"] | None = Field(
        None,
        description="A string value indicating the feature reduction type.",
    )


class FeatureReductionBinning(BaseModel):
    """
    Binning aggregates data spatially to a regular grid of polygons called \"bins\", thus summarizing the density of data in real-world space. This can be an effective method for visualizing the density of large datasets. Binning is a visualization-only feature intended solely for reducing the visual clutter of many overlapping features to a simplified view of a few aggregate features that communicate patterns of density. It is not intended to be used for analytical purposes. Binning is not appropriate for all datasets. Therefore, it is important to consider the implications of binning based on geometry type, data distribution, and the purpose of the visualization. Clients that implement a UX or API for binning should clearly document when binning is appropriate and when it is not.
    """

    model_config = common_config
    bin_type: BinType = Field(
        ...,
        alias="binType",
        description="Determines the type or shape of bins used in the aggregation.",
    )
    disable_popup: bool | None = Field(
        True,
        alias="disablePopup",
        description="Indicates whether to disable popups describing aggregate data in a binning visualization.",
    )
    drawing_info: FeatureReductionBinningDrawingInfo | None = Field(
        None,
        alias="drawingInfo",
        description="Contains labeling and rendering information for the bins.",
    )
    fields: list[AggregateField] | None = Field(
        None,
        description="An array of objects defining the aggregate fields to use in binning popups, labels, and renderers.",
    )
    fixed_bin_level: float | None = Field(
        None,
        alias="fixedBinLevel",
        description="Indicates the fixed geohash level used to create bins. When defined, bins defined at this level are static and do not regenerate on zoom. When undefined, bin resolution will vary as you zoom in and out. Dynamically changing bin resolution on zoom is currently not supported on web clients.",
    )
    popup_info: PopupInfo | None = Field(
        None,
        alias="popupInfo",
        description="Defines the popup used to describe aggregate data in the selected bin.",
    )
    show_labels: bool | None = Field(
        None,
        alias="showLabels",
        description="Defines whether labels should be shown in the bins.",
    )
    type: Literal["binning"] = Field(
        "binning", description="Type of feature reduction."
    )
    visibility_info: FeatureReductionScaleVisibilityInfo | None = Field(
        None,
        alias="visibilityInfo",
        description="Defines the threshold for toggling between when a layer should be drawn using the feature reduction configuration versus the layer's drawing info.",
    )


class FeatureReductionCluster(BaseModel):
    """
    Clustering spatially groups nearby features into \"clusters\" or \"aggregates\" in screen space.
    This is a visualization-only feature intended solely for reducing the visual clutter of many overlapping
    features to a simplified view of a few aggregate features that communicate patterns of density.
    This is not intended to be used for analytical purposes. Clustering is not appropriate for all
    datasets. Therefore, it is important to consider the implications of clustering based on geometry
    type, data distribution, and the purpose of the visualization. Clients that implement a UX or an
    API for clustering should clearly document when it is appropriate and when it is not.
    """

    model_config = common_config
    cluster_max_size: float | None = Field(
        None,
        alias="clusterMaxSize",
        description="Size of the largest cluster, in screen units (points).",
    )
    cluster_min_size: float | None = Field(
        None,
        alias="clusterMinSize",
        description="Size of the smallest cluster, in screen units (points).",
    )
    cluster_radius: float | None = Field(
        None,
        alias="clusterRadius",
        description="Strength of clustering, in screen units (points).",
    )
    disable_popup: bool | None = Field(
        True,
        alias="disablePopup",
        description="Indicates whether to disable viewing popups defined for the clusters.",
    )
    drawing_info: FeatureReductionClusterDrawingInfo | None = Field(
        None,
        alias="drawingInfo",
        description="Contains labeling and rendering information for the cluster.",
    )
    fields: list[AggregateField] | None = Field(
        None,
        description="An array of objects defining the aggregate fields to use in cluster popups, labels, and explicit renderers.",
    )
    popup_info: PopupInfo | None = Field(
        None,
        alias="popupInfo",
        description="Defines the popup used to describe aggregate data in the selected cluster.",
    )
    show_labels: bool | None = Field(
        None,
        alias="showLabels",
        description="Defines whether labels should be shown in the clusters.",
    )
    type: Literal["cluster"] | None = Field(
        None, description="Type of feature reduction."
    )
    visibility_info: FeatureReductionScaleVisibilityInfo | None = Field(
        None,
        alias="visibilityInfo",
        description="Defines the threshold for toggling between when a layer should be drawn using the feature reduction configuration versus the layer's drawing info.",
    )


class FieldOverride(BaseModel):
    """
    Defines overridden properties on a field for a specific view of the data.
    """

    model_config = common_config
    alias: str | None = Field(
        None, description="A string containing the overridden field alias."
    )
    editable: bool | None = Field(
        None,
        description="A Boolean determining whether users can edit this field.",
    )
    name: str | None = Field(
        None,
        description="A string containing the field name as defined by the service.",
    )


class CodedValues(BaseModel):
    """
    A set of valid coded values with unique names.
    """

    model_config = common_config
    code: float | str = Field(
        ..., description="The value stored in the feature attribute."
    )
    name: str = Field(..., description="User-friendly name for what the code means.")


class FieldModel(BaseModel):
    """
    Contains information about an attribute field.
    """

    model_config = common_config
    alias: str | None = Field(None, description="A string defining the field alias.")
    domain: CodedValue | InheritedDomain | RangeDomain | None = Field(
        None, description="The domain objects if applicable.", title="domain"
    )
    editable: bool | None = Field(
        None,
        description="A Boolean defining whether this field is editable.",
    )
    exact_match: bool | None = Field(
        None,
        alias="exactMatch",
        description="A Boolean defining whether or not the field is an exact match.",
    )
    length: int | None = Field(
        None,
        description="A number defining how many characters are allowed in a string. field.",
    )
    name: str | None = Field(None, description="A string defining the field name.")
    nullable: bool | None = Field(
        None,
        description="A Boolean defining whether this field can have a null value.",
    )
    type: FieldType | None = Field(
        None, description="A string defining the field type."
    )


class FloorInfo(BaseModel):
    """
    Contains floor-awareness information for a layer.
    """

    model_config = common_config
    floor_field: str = Field(
        ...,
        alias="floorField",
        description="The name of the attribute field that contains a floor's level ID used for floor filtering.",
    )


class RangeInformation(BaseModel):
    """
    Range Information.
    """

    model_config = common_config
    current_range_extent: list[float] | None = Field(
        None,
        alias="currentRangeExtent",
        description="Contains the min and max values within which the features are visible.",
        max_length=2,
        min_length=2,
    )
    field: str = Field(..., description="Field name to used for the range.")
    full_range_extent: list[float] | None = Field(
        None,
        alias="fullRangeExtent",
        description="Contains the min and max values of all the features for this rangeInfo.",
        max_length=2,
        min_length=2,
    )
    name: str = Field(
        ...,
        description="A unique name that can be referenced by webMap.activeRanges.",
    )
    type: Literal["rangeInfo"] = Field("rangeInfo", description="Type of range object.")


class Feature(SymbolValidatorMixin):
    """
    Contains information about an attribute field and feature geometry.
    """

    model_config = common_config
    attributes: dict[str, Any] | None = Field(
        None,
        description="The feature attributes. A JSON object that contains a dictionary of name-value pairs. The names are the feature field names. The values are the field values, and they can be any of the standard JSON types: string, number, and boolean. Note that date values are encoded as numbers. The number represents the number of milliseconds since epoch (January 1, 1970) in UTC.",
    )
    geometry: (
        MultipointGeometry | PointGeometry | PolygonGeometry | PolylineGeometry | None
    ) = Field(
        None,
        description="It can be any of the supported geometry types.",
        title="geometry",
    )
    popup_info: PopupInfo | None = Field(
        None,
        alias="popupInfo",
        description="A popupInfo object defining the content of popup window when you click a feature on the map. Applicable to features in a route and map notes feature layer only.",
    )
    symbol: (
        CimSymbolReference
        | PictureFillSymbolEsriPFS
        | PictureMarkerSymbolEsriPMS
        | SimpleFillSymbolEsriSFS
        | SimpleLineSymbolEsriSLS
        | SimpleMarkerSymbolEsriSMS
        | TextSymbolEsriTS
        | LineSymbol3D
        | MeshSymbol3D
        | PointSymbol3D
        | PolygonSymbol3D
        | None
    ) = Field(
        None,
        description="Symbol used for drawing the feature.",
        title="symbol",
    )


class FeatureSet(BaseModel):
    """
    A featureSet object contains the geometry and attributes of features in a layer. This object is used with feature collections only.
    """

    model_config = common_config
    features: list[Feature] = Field(
        ...,
        description="An array of feature objects containing geometry and a set of attributes.",
    )
    geometry_type: GeometryType = Field(
        ..., alias="geometryType", description="The type of geometry."
    )


class Template(BaseModel):
    """
    Templates describe features that can be created in a layer. They are generally used with feature collections and editable web-based CSV layers. Templates are not used with ArcGIS feature services as these already have templates defined in the service. They are also defined as properties of the layer definition when there are no defined types. Otherwise, templates are defined as properties of the types.
    """

    model_config = common_config
    description: str | None = Field(
        None,
        description="A string value containing a detailed description of the template.",
    )
    drawing_tool: DrawingTool | str | None = Field(
        None,
        alias="drawingTool",
        description="An optional string that can define a client-side drawing tool to be used with this feature. For example, map notes used by the Online Map Viewer use this to represent the viewer's different drawing tools.",
    )
    name: str | None = Field(
        None,
        description="A string containing a user-friendly name for the template.",
    )
    prototype: Feature | None = Field(
        None,
        description="A feature object representing a prototypical feature for the template.",
    )


class TimeInfoExportOptions(BaseModel):
    """
    The default time-related export options for a layer.
    """

    model_config = common_config
    time_data_cumulative: bool | None = Field(
        None,
        alias="timeDataCumulative",
        description="If true, draw all the features from the beginning of time for that data.",
    )
    time_offset: float | int | None = Field(
        None,
        alias="timeOffset",
        description="Time offset value for this layer so that it can be overlaid on the top of a previous or future time period.",
    )
    time_offset_units: TimeIntervalUnits | None = Field(
        None,
        alias="timeOffsetUnits",
        description="Temporal unit in which the time offset is measured.",
    )
    use_time: bool | None = Field(
        None,
        alias="useTime",
        description="If true, use the time extent specified by the time parameter.",
    )


class TimeReference(BaseModel):
    """
    Defines information about daylight savings time and the time zone in which data was collected.
    """

    model_config = common_config

    respects_daylight_saving: bool | None = Field(
        None,
        alias="respectsDaylightSaving",
        description="If true, dates will honor the daylight savings equivalent of 'timeZone'. This applies to certain 'timeZone' values only.",
    )
    time_zone: str | None = Field(
        None,
        alias="timeZone",
        description="The time zone in which the data was captured.",
    )
    time_zone_IANA: str | None = Field(
        None,
        alias="timeZoneIANA",
        description="The time zone in which the data was captured. Values are in IANA format.\nClients should use 'timeZoneIANA' over 'timeZone'/'respectsDaylightSaving'.",
    )


class LayerTimeInfo(BaseModel):
    """
    Time info if the layer/table supports querying and exporting maps based on time.
    """

    model_config = common_config
    end_time_field: str | None = Field(
        None,
        alias="endTimeField",
        description="The name of the attribute field that contains the end time information.",
    )
    export_options: TimeInfoExportOptions | str | None = Field(
        None,
        alias="exportOptions",
        description="The default time-related export options for this layer.",
    )
    has_live_data: bool | None = Field(
        None,
        alias="hasLiveData",
        description="Indicates whether service has live data.",
    )
    start_time_field: str | None = Field(
        None,
        alias="startTimeField",
        description="The name of the attribute field that contains the start time information.",
    )
    time_extent: list[float] | list[int] | list[None] | None = Field(
        None,
        alias="timeExtent",
        description="The time extent for all the data in the layer.",
        max_length=2,
        min_length=0,
    )
    time_interval: float | None = Field(
        None,
        alias="timeInterval",
        description="Time interval of the data in the layer. Typically used for the TimeSlider when animating the layer.",
    )
    time_interval_units: TimeIntervalUnits | None = Field(
        None,
        alias="timeIntervalUnits",
        description="Temporal unit in which the time interval is measured.",
    )
    time_reference: TimeReference | None = Field(
        None,
        alias="timeReference",
        description="Information about how the time was measured.",
    )
    track_id_field: str | None = Field(
        None,
        alias="trackIdField",
        description="The field that contains the trackId.",
    )


class AttributeFieldType(BaseModel):
    """
    Contains information about an attribute field.
    """

    model_config = common_config
    domains: (
        dict[constr(pattern=r".*"), CodedValue | InheritedDomain | RangeDomain] | None
    ) = Field(
        None,
        description="A comma-delimited series of domain objects for each domain in the type.",
    )
    id: float | str = Field(
        ..., description="A unique string or numerical ID for the type."
    )
    name: str | None = Field(None, description="A user-friendly name for the type.")
    templates: list[Template] | None = Field(
        None,
        description="Defined as a property of the layer definition when there are no types defined; otherwise, templates are defined as properties of the types.",
    )


class DynamicMapLayer(BaseModel):
    """
    A dynamic map layer refers to a layer in the current map service. More information on this can be found in the [ArcGIS REST API help](http://resources.arcgis.com/en/help/rest/apiref/layersource.html).
    """

    model_config = common_config
    gdb_version: str | None = Field(
        None,
        alias="gdbVersion",
        description="If applicable, specify this to use an alternate geodatabase version.",
    )
    map_layer_id: int = Field(
        ..., alias="mapLayerId", description="The current map layer's id."
    )
    type: Literal["mapLayer"] = Field(
        "mapLayer", description="A string value indicating the type."
    )


class QueryTableDataSource(BaseModel):
    """
    Query table data source is a layer/table that is defined by a SQL query.
    """

    model_config = common_config
    geometry_type: GeometryType | None = Field(
        None,
        alias="geometryType",
        description="The geometry type. When querying a table that does not have a geometry column, do not include geometryType.",
    )
    oid_fields: str | None = Field(
        None,
        alias="oidFields",
        description="Comma separated list of identifier fields. There are only certain field types that can be used as a unique identifier. These field types include integer, string, GUID, and date. If a single integer field is specified, map server uses the values in that field directly to uniquely identify all features and rows returned from a queryTable. However, if a single string field or a group of fields is used as the unique identifier, map server maps those unique values to an integer.",
    )
    query: str | None = Field(None, description="The SQL query.")
    spatial_reference: SpatialReference | None = Field(
        None,
        alias="spatialReference",
        description="The spatial reference of the geometry column. When querying a table that does not have a geometry column, do not include spatialReference.",
        title="spatialReference",
    )
    type: Literal["queryTable"] = Field(
        "queryTable",
        description="String value indicating the type for the dataSource.",
    )
    workspace_id: str | None = Field(
        None,
        alias="workspaceId",
        description="The unique string value used to identify the datasource's workspace.",
    )


class RasterDataSource(BaseModel):
    """
    Raster data source is a file-based raster that resides in a registered raster workspace.
    """

    model_config = common_config
    data_source_name: str | None = Field(
        None,
        alias="dataSourceName",
        description="The raster datasource's name.",
    )
    type: Literal["raster"] = Field(
        "raster",
        description="String value indicating the type for the dataSource.",
    )
    workspace_id: str | None = Field(
        None,
        alias="workspaceId",
        description="The unique string value used to identify the datasource's workspace.",
    )


class JoinTableDataSource(BaseModel):
    """
    Join Table data source is the result of a join operation. Nested joins are supported. To use nested joins, set either leftTableSource or rightTableSource to be a joinTable.
    """

    model_config = common_config
    join_type: JoinType | None = Field(
        None,
        alias="joinType",
        description="The type of join (left outer or left inner).",
    )
    left_table_key: str | None = Field(
        None,
        alias="leftTableKey",
        description="Field name from the left table.",
    )
    left_table_source: DynamicDataLayer | DynamicMapLayer | None = Field(
        None,
        alias="leftTableSource",
        description="The left source. If the leftTableSource is a table, the resulting joinTable is a table. If the leftTableSource is a layer, the resulting joinTable is a layer.",
        title="source",
    )
    right_table_key: str | None = Field(
        None,
        alias="rightTableKey",
        description="Field name from the right table.",
    )
    right_table_source: DynamicDataLayer | DynamicMapLayer | None = Field(
        None,
        alias="rightTableSource",
        description="The right table source.",
        title="source",
    )
    type: Literal["joinTable"] = Field(
        "joinTable",
        description="String value indicating the type for the dataSource.",
    )


class TableDataSource(BaseModel):
    """
    Table data source is a table, feature class, or raster that resides in a registered workspace (either a folder or geodatabase). In the case of a geodatabase, if versioned, use version to switch to an alternate geodatabase version. If version is empty or missing, the registered geodatabase version will be used.
    """

    model_config = common_config
    data_source_name: str | None = Field(
        None,
        alias="dataSourceName",
        description="The fully-qualified string value used to specify where the dataSource is derived.",
    )
    gdb_version: str | None = Field(
        None,
        alias="gdbVersion",
        description="If applicable, the value indicating the version of the geodatabase.",
    )
    type: Literal["table"] = Field(
        "table",
        description="String value indicating the type for the dataSource.",
    )
    workspace_id: str | None = Field(
        None,
        alias="workspaceId",
        description="The unique string value used to identify the datasource's workspace.",
    )


class DynamicDataLayer(BaseModel):
    """
    A dynamic data layer derived from a registered workspace. More information on this can be found in the [ArcGIS REST API help](http://resources.arcgis.com/en/help/rest/apiref/layersource.html).
    """

    model_config = common_config
    data_source: (
        JoinTableDataSource | QueryTableDataSource | RasterDataSource | TableDataSource
    ) = Field(
        ...,
        alias="dataSource",
        description="The layer's data source.",
        title="dataSource",
    )
    fields: list[FieldModel] | None = Field(
        None,
        description="An array of objects specifying information about an attribute field.",
    )
    type: Literal["dataLayer"] = Field(
        "dataLayer", description="A string value indicating the type."
    )


class PolygonFilter(BaseModel):
    """
    Filter features using polygons stored in external resources.
    """

    model_config = common_config

    geometries: str = Field(
        ...,
        description="URL to a polygon filter geometries json file, typically stored in `ITEM/resources`. Content of the file follows the $ref:[polygon filter geometries schema](polygonFilterGeometries_schema.json).",
    )
    spatial_relationship: SpatialRelationship = Field(
        ...,
        alias="spatialRelationship",
        description="Specifies the spatial relationship used for the filter. `disjoint`: Display features that do not intersect any filter polygon. `contains`: Display features completely inside any filter polygon",
    )


class ElevationInfo(BaseModel):
    """
    Elevation info defines how features are aligned to ground or other layers.
    """

    model_config = common_config
    feature_expression_info: ExpressionInfo | None = Field(
        None,
        alias="featureExpressionInfo",
        description="An object that defines an expression for per-feature elevation. If not set, geometry.z values are used for elevation. `unit` is applied to the resulting expression value.",
    )
    mode: ElevationMode = Field(
        ...,
        description="Determines how the service elevation values are combined with the elevation of the scene.",
    )
    offset: float | None = Field(
        None,
        description="Offset is always added to the result of the above logic except for onTheGround where offset is ignored.",
    )
    unit: str | None = Field(
        "meter",
        description='A string value indicating the unit for the values in elevationInfo. Applies to both `offset` and `featureExpressionInfo`. Defaults to `meter` if not set. <a href="#unit"><sup>1</sup></a>',
    )


class PointCloudReturnFilter(BaseModel):
    """
    Filters points based on the value of the return number/return count.
    """

    model_config = common_config
    field: str = Field(
        ..., description="The name of the field that is used for the filter."
    )
    included_returns: list[IncludedReturn] = Field(
        ...,
        alias="includedReturns",
        description="All points with at least one specified return status will be kept. Status values: `last`, `firstOfMany`, `lastOfMany`, `single`",
    )
    type: Literal["pointCloudReturnFilter"] = "pointCloudReturnFilter"


class PointCloudBitfieldFilter(BaseModel):
    """
    Filters points based on the value of the specified bitfield attribute.
    """

    model_config = common_config
    field: str = Field(
        ..., description="The name of the field that is used for the filter."
    )
    required_clear_bits: list[int] = Field(
        None,
        alias="requiredClearBits",
        description="List ALL bit numbers that must cleared (=0) for the point to be kept. bit 0 is LSB.",
    )
    required_set_bits: list[int] | None = Field(
        None,
        alias="requiredSetBits",
        description=" List ALL bit numbers that must set (=1) for the point to be kept. bit 0 is LSB.",
    )
    type: Literal["pointCloudBitfieldFilter"] = "pointCloudBitfieldFilter"


class PointCloudValueFilter(BaseModel):
    """
    Filters points based on the value of an specified attribute.
    """

    model_config = common_config
    field: str = Field(
        ..., description="The name of the field that is used for the filter."
    )
    mode: PointCloudMode = Field(
        ..., description="Defines if values should be included or excluded."
    )
    type: Literal["pointCloudValueFilter"] = Field(
        "pointCloudValueFilter",
        description="Filters points based on the value of an specified attribute.",
    )
    values: list[float] = Field(..., description="list of values")


class LayerDefinition(BaseModel):
    """
    An object that defines the attribute schema and drawing information for a layer drawn using client-side graphics.
    """

    model_config = common_config

    allow_geometry_updates: bool | None = Field(
        None,
        alias="allowGeometryUpdates",
        description="Boolean value indicating whether the geometry of the features in the layer can be edited.",
    )
    barrier_weight: BarrierWeight | None = Field(
        None,
        alias="barrierWeight",
        description="Optional weight of features in AnnotationLayers and DimensionLayers when considered as barriers to labeling. If not set but required, the default value is assumed to be High.",
    )
    capabilities: str | None = Field(
        None,
        description="A comma separated list of supported capabilities, e.g. `Query,Editing`.",
    )
    copyright_text: str | None = Field(
        None,
        alias="copyrightText",
        description="String value for the copyright text information for the layer.",
    )
    current_version: float | None = Field(
        None,
        alias="currentVersion",
        description="Numeric value indicating the server version of the layer.",
    )
    default_visibility: bool | None = Field(
        None,
        alias="defaultVisibility",
        description="Boolean value indicating whether the layer's visibility is turned on.",
    )
    definition_editor: DefinitionEditor | None = Field(
        None,
        alias="definitionEditor",
        description="Stores interactive filters.",
    )
    definition_expression: str | None = Field(
        None,
        alias="definitionExpression",
        description="SQL-based definition expression string that narrows the data to be displayed in the layer.",
    )
    definition_geometry: Extent | dict | None = Field(
        None,
        alias="definitionGeometry",
        description="An extent object used to filter features for StreamLayer. Only features that intersect the extent are displayed.",
    )
    description: str | None = Field(
        None,
        description="String value of the layer as defined in the map service.",
    )
    disable_display_filter: bool | None = Field(
        None,
        alias="disableDisplayFilter",
        description="When `true`, ignores the display filter defined on the layer by the [displayFilterInfo](displayFilterInfo.md) property.",
    )
    display_field: str | None = Field(
        None,
        alias="displayField",
        description="A string value that summarizes the feature.",
    )
    display_filter_info: DisplayFilterInfo | None = Field(
        None, alias="displayFilterInfo"
    )
    drawing_info: DrawingInfo | None = Field(
        None,
        alias="drawingInfo",
        description="Contains the drawing and labeling information.",
    )
    elevation_info: ElevationInfo | None = Field(None, alias="elevationInfo")
    extent: Extent | dict | None = Field(
        None, description="An object defining the rectangular area."
    )
    feature_reduction: (
        FeatureReductionSelection
        | FeatureReductionBinning
        | FeatureReductionCluster
        | None
    ) = Field(
        None,
        alias="featureReduction",
        description="An object defining how to aggregate dense data to clusters or bins. This is a visualization-only feature intended solely for reducing the visual complexity of many overlapping features to a few aggregate features that communicate patterns of density. This is not intended to be used for analytical purposes.",
        title="Feature Reduction",
    )
    field_overrides: list[FieldOverride] | None = Field(
        None,
        alias="fieldOverrides",
        description="The layer-specific overrides of field properties.  Used by SubtypeGroupLayer to modify field information for each subtype.  Any field missing from this array should be hidden.",
    )
    fields: list[FieldModel] | None = Field(
        None,
        description="An array of field objects containing information about the attribute fields for the feature collection or layer.",
    )
    filters: (
        list[PointCloudReturnFilter | PointCloudValueFilter | PointCloudBitfieldFilter]
        | None
    ) = Field(None, description="Filters for PointCloud layers")
    floor_info: FloorInfo | None = Field(
        None,
        alias="floorInfo",
        description="Contains floor-awareness information for the layer.",
    )
    geometry_type: str | None = Field(
        None,
        alias="geometryType",
        description="A string defining the type of geometry. Possible geometry types are: `esriGeometryPoint`, `esriGeometryMultipoint`, `esriGeometryPolyline`, `esriGeometryPolygon`, and `esriGeometryEnvelope`.",
    )
    global_id_field: str | None = Field(
        None,
        alias="globalIdField",
        description="The unique identifier for a feature or table row within a geodatabase.",
    )
    has_attachments: bool | None = Field(
        None,
        alias="hasAttachments",
        description="Indicates whether attachments should be loaded for the layer.",
    )
    has_m: bool | None = Field(
        None,
        alias="hasM",
        description="Boolean value indicating whether layer has M values.",
    )
    has_static_data: bool | None = Field(
        None,
        alias="hasStaticData",
        description="Boolean value indicating whether data changes. True if it does not.",
    )
    has_z: bool | None = Field(
        None,
        alias="hasZ",
        description="Boolean value indicating whether layer has Z values.",
    )
    html_popup_type: HtmlPopupType | None = Field(
        None,
        alias="htmlPopupType",
        description="String value indicating the HTML popup type.",
    )
    id: int | None = Field(None, description="The identifier assigned to the layer.")
    is_data_versioned: bool | None = Field(
        None,
        alias="isDataVersioned",
        description="Boolean value indicating whether the data is versioned.",
    )
    max_record_count: int | None = Field(
        None,
        alias="maxRecordCount",
        description="Numeric value indicating tbe maximum number of records that will be returned at once for a query.",
    )
    max_scale: float | None = Field(
        None,
        alias="maxScale",
        description="Integer property used to determine the maximum scale at which the layer is displayed.",
    )
    min_scale: float | None = Field(
        None,
        alias="minScale",
        description="Integer property used to determine the minimum scale at which the layer is displayed.",
    )
    name: str | None = Field(
        None,
        description="Contains a unique name for the layer that can be displayed in a legend.",
    )
    object_id_field: str | None = Field(
        None,
        alias="objectIdField",
        description="Indicates the name of the object ID field in the dataset.",
    )
    order_by: list[OrderByField] | None = Field(
        None,
        alias="orderBy",
        description="An array of orderByField objects specifying the feature display order. Features can be sorted in ascending or descending order of a numeric or date field only. If `ascending`, features with smaller values will be drawn on top of features with larger values. For date values, `ascending` order means features with older dates will be drawn on top of features with recent dates. If `descending`, the sort behavior is reversed. When this property is not defined, features are displayed in the order in which they are received by the client.",
        min_length=1,
    )
    override_symbols: bool | None = Field(
        None,
        alias="overrideSymbols",
        description="Dictates whether a client can support having an end user modify symbols on individual features.",
    )
    polygon_filter: PolygonFilter | None = Field(
        None,
        alias="polygonFilter",
        description="[Polygon filter](polygonFilter.md) for scene layer features.",
    )
    range_infos: list[RangeInformation] | None = Field(
        None,
        alias="rangeInfos",
        description="Indicates range information",
        min_length=1,
    )
    source: DynamicDataLayer | DynamicMapLayer | None = Field(
        None,
        description="An object indicating the layerDefinition's layer source.",
        title="source",
    )
    spatial_reference: SpatialReference | None = Field(
        None,
        alias="spatialReference",
        description="An object containing the WKID or WKT identifying the spatial reference of the layer's geometry.",
        title="spatialReference",
    )
    supported_query_formats: str | None = Field(
        None,
        alias="supportedQueryFormats",
        description="String value indicating the output formats that are supported in a query.",
    )
    supports_advanced_queries: bool | None = Field(
        None,
        alias="supportsAdvancedQueries",
        description="Boolean value indicating whether the layer supports orderByFields in a query operation.",
    )
    supports_attachments_by_upload_id: bool | None = Field(
        None,
        alias="supportsAttachmentsByUploadId",
        description="Boolean value indicating whether the layer supports uploading attachments with the Uploads operation. This can then be used in the Add Attachment and Update Attachment operations.",
    )
    supports_calculate: bool | None = Field(
        None,
        alias="supportsCalculate",
        description="Boolean value indicating whether the layer supports the Calculate REST operation when updating features.",
    )
    supports_rollback_on_failure_parameter: bool | None = Field(
        None,
        alias="supportsRollbackOnFailureParameter",
        description="Boolean value indicating whether the layer supports rolling back edits made on a feature layer if some of the edits fail.",
    )
    supports_statistics: bool | None = Field(
        None,
        alias="supportsStatistics",
        description="Boolean value indicating whether feature layer query operations support statistical functions.",
    )
    supports_validate_sql: bool | None = Field(
        None,
        alias="supportsValidateSql",
        description="Boolean value indicating whether the validateSQL operation is supported across a feature service layer.",
    )
    templates: list[Template] | None = Field(
        None,
        description="A property of the layer definition when there are no types defined; otherwise, templates are defined as properties of the types.",
    )
    time_info: LayerTimeInfo | None = Field(
        None,
        alias="timeInfo",
        description="An object that defines properties to enable time on a layer or table. Refer to the individual layerDefinition properties to see which layer types support this.",
    )
    type: LayerDefinitionType | None = Field(
        None,
        description="Indicates whether the layerDefinition applies to a Feature Layer or a Table.",
    )
    type_id_field: str | None = Field(
        None,
        alias="typeIdField",
        description="Contains the name of the field holding the type ID for the features.",
    )
    types: list[AttributeFieldType] | None = Field(
        None, description="Contains information about an attribute field."
    )
    visibility_field: str | None = Field(
        None,
        alias="visibilityField",
        description="String value indicating the attribute field that is used to control the visibility of a feature. If applicable, when rendering a feature the client should use this field to control visibility. The field's values are 0 = do not display, 1 = display.",
    )

    @field_validator("html_popup_type", mode="before")
    def check_html_popup_type(cls, v):
        # If '' passed in, change to None
        if v == "":
            return None

    @field_validator("type", mode="before")
    def check_type(cls, v):
        # If FeatureLayer passed in, change to Feature Layer
        if v == "FeatureLayer":
            return "Feature Layer"


class EffectFunctions(BaseModel):
    """
    Effect functions `brightness`, `contrast`, `grayscale`, `invert`, `opacity`, `saturate`, and `sepia`
    """

    model_config = common_config
    amount: float = Field(
        ...,
        description="Amount of effect. A value of 0 leaves the input unchanged. `grayscale`, `invert`, `sepia`, and `opacity` effects accept a maximum `amount` of 1 which applies the effect at 100%. `brightness`, `contrast`, and `saturate` can accept amount above 1. Negative values are not allowed.",
    )
    type: EffectFunctionsType = Field(..., description="Effect type.")


class EffectFunctions1(BaseModel):
    """
    Effect function `hue-rotate`
    """

    model_config = common_config
    angle: float = Field(
        ...,
        description="The relative change in hue as an angle in degree. A value of 0 leaves the input unchanged. A positive hue rotation increases the hue value, while a negative one decreases the hue value.",
    )
    type: Literal["hue-rotate"] = Field("hue-rotate", description="Effect type.")


class EffectFunctions2(BaseModel):
    """
    Effect function `blur`
    """

    model_config = common_config
    radius: float = Field(
        ...,
        description="The radius of the blur in points. It defines the value of the standard deviation to the Gaussian function. Negative values are not allowed.",
    )
    type: Literal["blur"] = Field("blur", description="Effect type.")


class EffectFunctions3(BaseModel):
    """
    Effect function `drop-shadow`
    """

    model_config = common_config
    blur_radius: float = Field(
        ...,
        alias="blurRadius",
        description="The radius of the blur in points. It defines the value of the standard deviation to the Gaussian function.",
    )
    color: list[confloat(ge=0, le=255)] = Field(
        ...,
        description="Color is represented as a four-element array. The four elements represent values for red, green, blue, and alpha in that order. Values range from 0 through 255.",
        title="color",
    )
    type: Literal["drop-shadow"] = Field("drop-shadow", description="Effect type.")
    xoffset: float = Field(
        ...,
        description="The distance of the shadow on the x-axis in points.",
    )
    yoffset: float = Field(
        ...,
        description="The distance of the shadow on the y-axis in points.",
    )


class EffectFunctions4(BaseModel):
    """
    Effect function `bloom`
    """

    model_config = common_config
    radius: float = Field(
        ...,
        description="Determines the radius of the blur. Negative values are not allowed. Leaves the pixels inside the radius untouched.",
    )
    strength: float = Field(
        ...,
        description="The intensity of the bloom effect. The higher the value, the brighter the glow. Negative values are not allowed.",
    )
    threshold: float = Field(
        ...,
        description="The mininum color luminosity for a pixel to bloom, where at 0 all pixels bloom and 1 only the pixels with 100% luminosity colors bloom.",
    )
    type: Literal["bloom"] = Field("bloom", description="Effect type.")


class ScaleDependentEffect(BaseModel):
    """
    An object describing the effect to apply at a scale stop
    """

    model_config = common_config
    scale: float = Field(
        ...,
        description="The scale of the view for the effect to take place.",
    )
    value: list[
        EffectFunctions
        | EffectFunctions1
        | EffectFunctions2
        | EffectFunctions3
        | EffectFunctions4
    ] = Field(
        ...,
        description="The effect to be applied at the corresponding scale.",
    )


class FeatureFilter(BaseModel):
    """
    Description of spatial and attribute filters that will be applied to Feature data. For example, used in Fence Parameters for Geotriggers.
    """

    model_config = common_config

    geometry: (
        MultipointGeometry | PointGeometry | PolygonGeometry | PolylineGeometry | None
    ) = Field(
        None,
        description="A geometry used to filter the features from a feature table. Any features that intersect the area of interest will be used. It can be any of the supported geometry types.",
        title="geometry",
    )
    where: str | None = Field(
        None,
        description="A SQL-based where clause that narrows the data to be used. Any features that satisfy the query will be used.",
    )


class FeatureEffect(BaseModel):
    """
    Feature Effect emphasizes or deemphasizes features that satisfy a filter using graphical effects
    """

    model_config = common_config
    excluded_effect: (
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
        alias="excludedEffect",
        description="The effect applied to features that do not meet the filter requirements.",
        title="Effect",
    )
    filter: FeatureFilter | None = Field(
        None,
        description="The client-side filter executed on each displayed feature to determine which of includedEffect or excludedEffect to apply.",
    )
    included_effect: (
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
        alias="includedEffect",
        description="The effect applied to features that meet the filter requirements.",
        title="Effect",
    )


class LocationInfo(BaseModel):
    """
    Defines how location information will be retrieved from a [CSV](csvLayer.md) file referenced through the web, ie. referenced by URL.
    """

    model_config = common_config
    latitude_field_name: str = Field(
        None,
        alias="latitudeFieldName",
        description="A string defining the field name that holds the latitude (Y) coordinate.",
    )
    location_type: Literal["coordinates"] = Field(
        "coordinates",
        alias="locationType",
        description="String value indicating location type.",
    )
    longitude_field_name: str = Field(
        None,
        alias="longitudeFieldName",
        description="A string defining the field name that holds the longitude (X) coordinate.",
    )


class Group(BaseModel):
    """
    Specifies the type of groups available in the feature collection.
    """

    model_config = common_config
    group_id: int | None = Field(
        None,
        alias="groupId",
        description="A number that uniquely identifies a specific type of group",
    )
    group_type: Literal["pointSymbolCallout"] | None = Field(
        None,
        alias="groupType",
        description="Type of group in the feature collection.",
    )


class DimensionalDefinition(BaseModel):
    """
    The dimensional definition defines a display filter based on one variable and one dimension. It is typically used when filtering data based on slices or ranges in one or more dimensions with [mosaicRule.description](mosaicRule.md). If the [multidimensionalSubset](multidimensionalSubset.md) is defined on the [TiledImageServiceLayer](tiledImageServiceLayer_schema.md) or the [mosaicRule](mosaicRule.md) then the multidimensionalDefinition must be within the defined multidimensionalSubset, otherwise nothing will be displayed.
    """

    model_config = common_config
    dimension_name: str | None = Field(
        None,
        alias="dimensionName",
        description="Type of dimension being used (ex. StdTime).",
    )
    is_slice: bool | None = Field(None, alias="isSlice", description="Is slice?")
    values: list[float] | None = Field(
        None, description="Numerical array of associated values."
    )
    variable_name: str | None = Field(
        None, alias="variableName", description="Name of the variable."
    )


class RasterFunctionTemplateArguments(BaseModel):
    """
    Raster function template or raster function variable used as argument.
    """

    model_config = common_config

    field_object_id: int | None = Field(
        None,
        alias="_object_id",
        description="The id of the raster function template argument.",
    )
    field_object_ref_id: int | None = Field(
        None,
        alias="_object_ref_id",
        description="The id of the raster function template by reference.",
    )
    type: str = Field(None, description="Type of the raster function argument.")


class RasterFunctionInformation(BaseModel):
    """
    Information about the `function` referenced in a raster function template.
    """

    model_config = common_config
    field_object_id: int | None = Field(
        None,
        alias="_object_id",
        description="Optional. The id of the raster function info.",
    )
    description: str | None = Field(
        None, description="Description of the raster function."
    )
    name: str = Field(
        ...,
        description="Name of the raster function used by the raster function template.",
    )
    pixel_type: PixelType = Field(
        ...,
        alias="pixelType",
        description="Pixel type of the output image processed by the raster function template.",
    )
    type: str = Field(
        ...,
        description="Type of the raster function used by the raster function template.",
    )


class Properties(BaseModel):
    """
    The properties used to define multidimensional dataset processing rules.
    """

    model_config = common_config

    field_object_id: int | None = Field(None, alias="_object_id")
    type: str


class RasterFunctionTemplate(BaseModel):
    """
    Raster function template containing one or more raster functions chained together to produce a processing workflow.
    """

    model_config = common_config
    field_object_id: int | None = Field(
        None,
        alias="_object_id",
        description="The id of the raster function template.",
    )
    aliases: list[str] | None = Field(
        None,
        description="Aliases for the `function` referenced in the template.",
    )
    arguments: RasterFunctionTemplateArguments = Field(
        ...,
        description="The arguments for the `function` referenced in the raster function template.",
        title="Raster Function Template Arguments",
    )
    definition: str | None = Field(
        None,
        description="A query definition to filter rasters when the layer's data source is a mosaic dataset.",
    )
    description: str = Field(
        ..., description="The description of the raster function template."
    )
    function: RasterFunctionInformation = Field(
        ...,
        description="The raster function referenced by the raster function template.",
    )
    function_type: FunctionType = Field(
        ...,
        alias="functionType",
        description="Defines whether the `function` is applied to a mosaic dataset. Indicates the level of the mosaic processing is used. Only applies to mosaic based image services. `0` - function is applied after mosaicking; `1` - function is applied on each raster item before mosaicking; `2` - function is applied to a group of raster items before mosaicking.",
    )
    group: str | None = Field(
        None,
        description="Group field name for item group function template ",
    )
    help: str | None = Field(
        None,
        description="The short description of what the raster function template does.",
    )
    name: str = Field(..., description="Name of the raster function template.")
    properties: Properties | None = Field(
        None,
        description="The properties used to define multidimensional dataset processing rules.",
    )
    tag: str | None = Field(
        None, description="Tag field name for item group function template ."
    )
    thumbnail: str | None = Field(
        None, description="Thumbnail url of the raster function template."
    )
    thumbnail_ex: str | None = Field(
        None,
        alias="thumbnailEx",
        description="Base64 encoded thumbnail of the raster function template.",
    )
    type: Literal["RasterFunctionTemplate"] | None = Field(
        None, description="Type of the raster function template."
    )


class RenderingRule(BaseModel):
    """
    Specifies the rendering rule for how the requested image should be rendered.
    """

    model_config = common_config
    output_pixel_type: PixelType | None = Field(
        PixelType.unknown,
        validate_default=True,
        alias="outputPixelType",
        description="the output pixel type defines the output image's pixel type.",
    )
    raster_function: str | None = Field(
        None,
        alias="rasterFunction",
        description="The raster function name identifies the processing or rendering to be performed. For a list of possible types, please see the [Raster Functions](https://developers.arcgis.com/documentation/common-data-types/raster-function-objects.htm) help topic for additional information on this.",
    )
    raster_function_arguments: dict[str, Any] | None = Field(
        None,
        alias="rasterFunctionArguments",
        description="The arguments for the referenced `rasterFunction`. For a description of arguments per raster function type, please see the [Raster Functions](https://developers.arcgis.com/documentation/common-data-types/raster-function-objects.htm) help topic for additional information on this.",
    )
    raster_function_definition: RasterFunctionTemplate | None = Field(
        None,
        alias="rasterFunctionDefinition",
        description="Specifies the raster function template for how the requested image should be processed.",
    )
    variable_name: str | None = Field(
        None,
        alias="variableName",
        description="Variable name for the raster function.",
    )


class MosaicRule(BaseModel):
    """
    The image service uses a mosaic rule to mosaick multiple rasters on the fly. The mosaic rule parameter is used by many image service operations, for example, export image and identify operations.
    """

    model_config = common_config
    ascending: bool | None = Field(
        None,
        description="Indicate whether to use ascending or descending order.",
    )
    fids: list[int] | None = Field(
        None,
        description="A list that defines a subset of rasters used in the mosaic, be aware that the rasters may not be visible at all scales.",
    )
    item_rendering_rule: RenderingRule | str | None = Field(
        None,
        alias="itemRenderingRule",
        description="The rendering rule applies on items before mosaicking.",
    )
    lock_raster_ids: list[int] | None = Field(
        None,
        alias="lockRasterIds",
        description="Lock a few rasters in the image service. Used together with `esriMosaicLockRaster`.",
    )
    mosaic_method: MosaicMethod = Field(
        ...,
        alias="mosaicMethod",
        description="A string value that determines how the selected rasters are ordered.",
    )
    mosaic_operation: MosaicOperation | None = Field(
        MosaicOperation.mt_first,
        alias="mosaicOperation",
        description="Use the mosaic operation to resolve overlap pixel values: from first or last raster, use the min, max or mean of the pixel values, or blend them.",
    )
    multidimensional_definition: list[DimensionalDefinition] | None = Field(
        None,
        alias="multidimensionalDefinition",
        description="An array of objects representing a slice from multidimensional data or multiple slices that are dynamically mosaicked and processed by the server. The dimensional definitions in this array are used to filter display data based on slices in one or more dimensions.",
    )
    sort_field: str | None = Field(
        None,
        alias="sortField",
        description="The field name used together with `esriMosaicAttribute` method.",
    )
    sort_value: float | str | None = Field(
        0,
        alias="sortValue",
        description="The base sort value used together with `esriMosaicAttribute` method and `sortField` parameter.",
    )
    viewpoint: PointGeometry | None = Field(
        None,
        description="Use a view point along with `esriMosaicViewpoint`.",
    )


class MultidimensionalSubset(BaseModel):
    """
    Represents a multidimensional subset of raster data. This includes subsets of both variables and dimensions. When the multidimensionalSubset is defined on a layer, the [layer.multidimensionalDefinition](multidimensionalDefinition.md) or the [mosaicRule.multidimensionalDefinition](mosaicRule.md) must be within the defined multidimensionalSubset, otherwise nothing will be displayed.
    """

    model_config = common_config
    area_of_interest: Extent | PolygonGeometry | dict | None = Field(
        None,
        alias="areaOfInterest",
        description="An optional area of interest for the entire multidimensional subset.",
    )
    subset_definitions: list[DimensionalDefinition] | None = Field(
        None,
        alias="subsetDefinitions",
        description="An optional array of objects representing dimension range subsets for selected variables from multidimensional data.",
    )


class ThematicGroup(BaseModel):
    """
    ThematicGroup is specifically for working with [ArcGISMapServiceLayer layer types](mapServiceLayer.md) that reference Esri's [demographic services](http://doc.arcgis.com/en/esri-demographics/). Since these services have multiple fields and layers, the `thematicGroup` provides a subset to use.
    """

    model_config = common_config

    field_names: list[str] | None = Field(
        None,
        alias="fieldNames",
        description="An array of string values indicating all the fields used within the webmap. All other fields can be disregarded and should not display in any field selection list.",
    )
    layer_ids: list[int] | None = Field(
        None,
        alias="layerIds",
        description="A zero-based array of integers indicating the layers to be used in the webmap. NOTE: All other layers should not be added to the TOC and may or may not be visible on the map as reference layers.",
    )
    name: str | None = Field(
        None,
        description="String property indicating the name for the thematic grouping of layers.",
    )


class OrientedImageryElevationSource(BaseModel):
    """
    Object defines the Digital Elevation Model (DEM) or a constant value to be used to compute the ground-to-image transformations for Oriented Imagery. The object defines properties to be used in the [OrientedImageryProperties](orientedImageryProperties.md) service.
    """

    model_config = common_config
    lod: float | None = Field(
        None,
        description="The scale in a tiling schema. The scale represents the zoom level value. Each successive level improves resolution and map scale by double when compared to the previous level. lod is applicable only when the url points to a tiled image service.",
    )
    raster_function: str | None = Field(
        None,
        alias="rasterFunction",
        description="The raster function processing template that can be applied to the image service.",
    )
    url: str = Field(
        ...,
        description="URL that references the input digital elevation model. A dynamic image service or a tile image service can be used as the digital elevation model.",
    )


class OrientedImageryElevationSource1(BaseModel):
    """
    Object defines the Digital Elevation Model (DEM) or a constant value to be used to compute the ground-to-image transformations for Oriented Imagery. The object defines properties to be used in the [OrientedImageryProperties](orientedImageryProperties.md) service.
    """

    model_config = common_config
    constant_elevation: float = Field(
        ...,
        alias="constantElevation",
        description="The constant ground elevation value for the entire dataset. The vertical measurement unit value will be used as the unit for constant elevation.",
    )


class OrientedImageryProperties(BaseModel):
    """
    Object that defines properties of layers in a feature service with layer type [OrientedImageryLayer](orientedImageryLayer.md). If a property is not defined in the layer field, corresponding value for that property defined here would be used instead.
    """

    model_config = common_config
    camera_heading: float | None = Field(
        None,
        alias="cameraHeading",
        description="Camera orientation defining the first rotation around z-axis of the camera. Defined in degrees. Heading values are measured in the positive clockwise direction where north is defined as 0 degrees. -999 is used when the orientation is unknown.",
    )
    camera_height: float | None = Field(
        None,
        alias="cameraHeight",
        description="The height of camera above the ground when the imagery was captured. The units are in meters. Camera height is used to determine the visible extent of the image, large values will result in a greater view extent. Values should not be less than 0.",
    )
    camera_pitch: float | None = Field(
        None,
        alias="cameraPitch",
        description="Camera orientation defining the second rotation around x-axis of the camera in the positive counterclockwise direction. Defined in degrees. The pitch is 0 degrees when the camera is facing straight down to ground. The valid range of pitch value is from 0 to 180 degrees, with 180 degrees for a camera facing straight up and 90 degrees for a camera facing horizon.",
    )
    camera_roll: float | None = Field(
        None,
        alias="cameraRoll",
        description="Camera orientation defining the final rotation around z-axis of the camera in the positive clockwise direction. The camera housing rotation is defined in degrees. Valid values range from -90 to +90.",
    )
    coverage_percent: float | None = Field(
        None,
        alias="coveragePercent",
        description="Modifies the extent of the image's ground footprint. The ground footprint of each image is computed to search for images that contain the selected location, which is identified as the red cross on the map. Valid values are from -50 to 50. Negative percentage values shrink the size of the ground footprint and positive values increase the size of the ground footprint.",
    )
    dem_path_prefix: str | None = Field(
        None,
        alias="demPathPrefix",
        description="Prefix used to build the DEM url path in conjunction with the elevationSource attribute.",
    )
    dem_path_suffix: str | None = Field(
        None,
        alias="demPathSuffix",
        description="Suffix used to build the DEM url path in conjunction with the elevationSource attribute.",
    )
    depth_image_path_prefix: str | None = Field(
        None,
        alias="depthImagePathPrefix",
        description="Prefix used to build the depth image url path in conjunction with the depth image attribute.",
    )
    depth_image_path_suffix: str | None = Field(
        None,
        alias="depthImagePathSuffix",
        description="Suffix used to build the depth image url path in conjunction with the depth image attribute.",
    )
    elevation_source: (
        OrientedImageryElevationSource | OrientedImageryElevationSource1 | None
    ) = Field(
        None,
        alias="elevationSource",
        description="The source of elevation as a JSON string, that will be used to compute ground to image transformations. The elevation source can be a digital elevation model (DEM) or a constant value. A dynamic image service or a tile image service can be used as the digital elevation model. The unit of constant elevation value should be in meters.",
        title="Oriented Imagery Elevation Source",
    )
    far_distance: float | None = Field(
        None,
        alias="farDistance",
        description="The farthest usable distance of the imagery from the camera position. FarDistance is used to determine the extent of the image footprint, which is used to determine if an image is returned when you click on the map, and for creating optional footprint features. The units are in meters. Far distance should be always greater than 0.",
    )
    horizontal_field_of_view: float | None = Field(
        None,
        alias="horizontalFieldOfView",
        description="The camera's scope in horizontal direction. The units are in degrees and valid values range from 0 to 360.",
    )
    horizontal_measurement_unit: str | None = Field(
        None,
        alias="horizontalMeasurementUnit",
        description="Defines the unit that will be used for all horizontal measurements. The unit will be obtained from the layer coordinate system and will be used for display purposes only.",
    )
    image_geometry_field: str | None = Field(
        "ImageGeometry",
        alias="imageGeometryField",
        description="Name of the field that stores the image pixel coordinates of the features created in the image space.",
    )
    image_path_prefix: str | None = Field(
        None,
        alias="imagePathPrefix",
        description="Prefix used to build the image url path in conjunction with the image attribute.",
    )
    image_path_suffix: str | None = Field(
        None,
        alias="imagePathSuffix",
        description="Suffix used to build the image url path in conjunction with the image attribute.",
    )
    image_reference_field: str | None = Field(
        "OIObjectID",
        alias="imageReferenceField",
        description="Name of the field that stores the reference of the image in the oriented imagery layer where the new features are created.",
    )
    image_rotation: float | None = Field(
        None,
        alias="imageRotation",
        description="The orientation of the camera in degrees relative to the scene when the image was captured. The rotation is added in addition to the camera roll. The value can range from -360 to +360.",
    )
    maximum_distance: confloat(ge=0.0) | None = Field(
        None,
        alias="maximumDistance",
        description="Maximum search distance to be used while querying the feature service specified in the Oriented Imagery Layer. The maximum distance can never be less than zero.",
    )
    near_distance: float | None = Field(
        None,
        alias="nearDistance",
        description="The nearest usable distance of the imagery from the camera position. The units are in meters. Near distance can never be less than 0.",
    )
    orientation_accuracy: str | None = Field(
        None,
        alias="orientationAccuracy",
        description="Semicolon-delimited string used to store standard deviation values. The standard deviation values are in the following order and format: Camera location in XY direction; camera height; camera heading; camera pitch; camera roll; near distance; far distance; elevation (all in meters).",
    )
    oriented_imagery_type: OrientedImageryType | None = Field(
        None,
        alias="orientedImageryType",
        description="String that defines the imagery type used in the particular Oriented Imagery Layer.",
    )
    reference_id_field: str | None = Field(
        "OBJECTID",
        alias="referenceIdField",
        description="Name of the field that stores the unique identifier of the features created in the image space.",
    )
    sequence_order_field: str | None = Field(
        "SequenceOrder",
        alias="sequenceOrderField",
        description="Name of the field that stores the sequential order of the images available in the Oriented Imagery Layer, dictating their position and availability while navigating through the list.",
    )
    time_interval_unit: OrientedImageryTimeUnit | None = Field(
        None,
        alias="timeIntervalUnit",
        description="Defines the unit of time used in the viewer's time selector tool. Images will be filtered in the viewer based on the Time Unit value defined here.",
    )
    vertical_field_of_view: float | None = Field(
        None,
        alias="verticalFieldOfView",
        description="The camera's scope in the vertical direction. The units are in degrees and valid values range from 0 to 180.",
    )
    vertical_measurement_unit: VerticalMeasurementUnit | None = Field(
        None,
        alias="verticalMeasurementUnit",
        description="Defines the primary unit to be used for all vertical measurements.",
    )
    video_path_prefix: str | None = Field(
        None,
        alias="videoPathPrefix",
        description="Prefix used to build the video url path in conjunction with the image attribute.",
    )
    video_path_suffix: str | None = Field(
        None,
        alias="videoPathSuffix",
        description="Suffix used to build the video url path in conjunction with the image attribute.",
    )


class TelemetryDisplay(BaseModel):
    model_config = common_config
    frame: bool = Field(
        False, description="Determines if the frame image is displayed."
    )
    frame_center: bool = Field(
        False,
        alias="frameCenter",
        description="Determines if the frame center is displayed.",
    )
    frame_outline: bool = Field(
        True,
        alias="frameOutline",
        description="Determines if the frame outline is displayed.",
    )
    line_of_sight: bool = Field(
        True,
        alias="lineOfSight",
        description="Determines if the line of sight is displayed.",
    )
    sensor_location: bool = Field(
        True,
        alias="sensorLocation",
        description="Determines if the sensor location is displayed.",
    )
    sensor_trail: bool = Field(
        True,
        alias="sensorTrail",
        description="Determines if the sensor trail is displayed.",
    )


class ExclusionArea(BaseModel):
    """
    Exclusion areas define [extent](extent.md) areas where no data will be fetched for a layer.
    """

    model_config = common_config
    geometry: (
        MultipointGeometry | PointGeometry | PolygonGeometry | PolylineGeometry
    ) = Field(
        ...,
        description="The geometry defining the area where no data will be fetched. Only [extent](extent.md) is supported.",
        title="geometry",
    )
    max_scale: float | None = Field(
        -1,
        alias="maxScale",
        description="The zoom level where the exclusion ends.",
    )
    max_zoom: float | None = Field(
        -1,
        alias="maxZoom",
        description="The zoom level where the exclusion ends.",
    )
    min_scale: float | None = Field(
        -1,
        alias="minScale",
        description="The zoom level where the exclusion starts.",
    )
    min_zoom: float | None = Field(
        -1,
        alias="minZoom",
        description="The zoom level where the exclusion starts.",
    )


class WebFeatureServiceInfo(BaseModel):
    """
    Object that defines and provides information about layers in a WFS service.
    """

    model_config = common_config
    custom_parameters: dict[constr(pattern=r".*"), str] | None = Field(
        None,
        alias="customParameters",
        description="A sequence of parameters used to append custom parameters to all WFS requests. These parameters are applied to `GetCapabilities`, `DescribeFeatureType`, and `GetFeatures`.",
    )
    feature_url: str | None = Field(
        None,
        alias="featureUrl",
        description="URL of the WFS service operation.",
    )
    max_features: int | None = Field(
        None,
        alias="maxFeatures",
        description="Set this to limit the number of requested features that a GetFeature request presents.",
    )
    name: str | None = Field(
        None,
        description="The name of the WFS layer. This is used to set layer visibility.",
    )
    supported_spatial_references: list[int] | None = Field(
        None,
        alias="supportedSpatialReferences",
        description="List of supported spatial reference IDs",
    )
    swap_xy: bool | None = Field(
        False,
        alias="swapXY",
        description="Boolean value indicating whether X and Y axis are flipped.",
    )
    version: str | None = Field(
        None,
        description="Value indicating which version of the WFS specification is used.",
    )
    wfs_namespace: str | None = Field(
        None,
        alias="wfsNamespace",
        description="String indicating namespace.",
    )


class ControlPoint(BaseModel):
    """
    A location in pixels in the media of the [MediaLayer](mediaLayer.md). The origin is located at the top-left corner of the media.
    """

    model_config = common_config
    x: float = Field(
        ...,
        description="The X coordinate in pixels. The value must be between 0, and the width of the [georeference](mediaLayer_georeference_schema.md)",
    )
    y: float = Field(
        ...,
        description="The Y coordinate in pixels. The value must be between 0, and the height of the [georeference](mediaLayer_georeference_schema.md)",
    )


class Georeference(BaseModel):
    """
    The georeference used to place a media in a [MediaLayer](mediaLayer.md).
    """

    model_config = common_config
    coefficients: list[float] = Field(
        ...,
        description="An array of 8 coefficients representing the [projective transformation](https://desktop.arcgis.com/en/arcmap/latest/manage-data/editing-existing-features/about-spatial-adjustment-transformations.htm#ESRI_SECTION1_EBB9C52B96934FE08A32CE852788EA02).",
        max_length=8,
        min_length=8,
    )
    control_points: list[ControlPoint] = Field(
        ...,
        alias="controlPoints",
        description="An array of 4 user defined control points placed on the media.",
        max_length=4,
        min_length=4,
    )
    height: float = Field(
        ...,
        description="Numeric value indicating the height of media in pixels.",
    )
    spatial_reference: SpatialReference = Field(
        ...,
        alias="spatialReference",
        description="The spatial reference can be defined using a well-known ID (wkid) or well-known text (WKT).",
        title="spatialReference",
    )
    width: float = Field(
        ...,
        description="Numeric value indicating the width of media in pixels.",
    )


class Lod(BaseModel):
    model_config = common_config
    level: int | None = Field(None, description="ID for each level.")
    level_value: str | None = Field(
        None,
        alias="levelValue",
        description="String to be used when constructing URL to access a tile from this LOD.",
    )
    resolution: float | None = Field(
        None,
        description="Resolution in map units of each pixel in a tile for each level.",
    )
    scale: float | None = Field(None, description="Scale for each level.")


class TileInfo(BaseModel):
    """
    Tile information, returned from the WMTS OGC Web Service. The tileInfo will contain the spatial reference of the layer. tileInfo is the same json representation as the ArcGIS Map/Image service tileInfo except that it may contain a levelValue on the lod objects that should be used instead of the level in the templateUrl.
    """

    model_config = common_config
    cols: int | None = Field(None, description="Requested tile's column.")
    compression_quality: confloat(ge=0.0, le=100.0) | None = Field(
        None,
        alias="compressionQuality",
        description="Compression quality of the tile.",
    )
    dpi: float | None = Field(None, description="The dpi of the tiling scheme.")
    format: ImageFormat | None = Field(
        None, description="Image format of the cached tiles."
    )
    lods: list[Lod] | None = Field(
        None,
        description="An array of levels of detail that define the tiling scheme.",
    )
    origin: PointGeometry | None = Field(None, description="The tiling scheme origin.")
    rows: int | None = Field(None, description="Requested tile's row.")
    spatial_reference: SpatialReference | None = Field(
        None,
        alias="spatialReference",
        description="The spatial reference of the tiling schema.",
        title="spatialReference",
    )


class WebMapTileServiceInfo(BaseModel):
    """
    Object defines and provides information about layers in a [WMTSLayer](webTiledLayer.md) service.
    """

    model_config = common_config
    custom_layer_parameters: dict[constr(pattern=r".*"), str] | None = Field(
        None,
        alias="customLayerParameters",
        description="A sequence of parameters used to append different custom parameters to a WMTS tile request. These parameters are applied to `GetTile`. The `customLayerParameters` property takes precedence if `customParameters` is also present.",
    )
    custom_parameters: dict[constr(pattern=r".*"), str] | None = Field(
        None,
        alias="customParameters",
        description="A sequence of parameters used to append custom parameters to all WMTS requests. These parameters are applied to `GetCapabilities` and `GetTile`. If used with the `customLayerParameters` property, `customParameters` will not take precedence.",
    )
    layer_identifier: str | None = Field(
        None,
        alias="layerIdentifier",
        description="Identifier for the specific layer used in the WMTS service. Required input by the user.",
    )
    tile_matrix_set: str | None = Field(
        None,
        alias="tileMatrixSet",
        description="Tiling schema, set by the WMTS service.",
    )
    url: str | None = Field(
        None,
        description="URL to the WMTS web service. Required input by the user.",
    )


class FilterModeWireFrame(BaseModel):
    """
    Draw elements of this filter block in wireframe mode. This mode draws only the edges of the features with the specified edge style.
    """

    model_config = common_config
    edges: SketchEdges | SolidEdges = Field(
        ...,
        description="An object defining edges of a feature.",
        title="Edges",
    )
    type: Literal["wireFrame"] = Field(
        "wireFrame", description="Declares filter mode of type wire frame."
    )


class FilterModeSolid(BaseModel):
    """
    Draw elements of this filter block in solid mode. This mode does not change the display of features.
    """

    model_config = common_config
    type: Literal["solid"] = Field(
        "solid", description="Declares filter mode of type solid."
    )


class FilterModeXRay(BaseModel):
    """
    Draw elements of this filter block in x-ray mode. `x-ray` renders elements semi-transparent with white color.
    """

    model_config = common_config
    type: Literal["x-ray"] = Field(
        "x-ray", description="Declares filter mode of type x-ray."
    )


class BuildingSceneLayerFilterBlock(BaseModel):
    """
    A filter block defines what elements will be filtered with a specific filter mode.  To ensure performance on client applications, it is not recommended to declare multiple filter blocks with the same filter mode. Filter blocks are contained in a filter for a building scene layer. Each filter includes at least one filter block.
    """

    model_config = common_config
    filter_expression: str = Field(
        ...,
        alias="filterExpression",
        description="SQL expression to select features that belong to this filter block.",
    )
    filter_mode: FilterModeSolid | FilterModeWireFrame | FilterModeXRay = Field(
        ...,
        alias="filterMode",
        description="Filter mode represents the way elements draw when participating in a filter block.",
        title="Filter Mode",
    )
    title: str = Field(..., description="Title of the filter block.")


class FilterType(BaseModel):
    """
    The file authoring information for a filter, including the filter type and its value settings.
    """

    model_config = common_config
    filter_type: str = Field(
        ...,
        alias="filterType",
        description="Represents the filter type name. Name is a unique identifier.",
    )
    filter_values: list[str] = Field(
        ...,
        alias="filterValues",
        description="Array of filter values. Filter values are the attributes that can be stored for individual fields in a layer.",
    )


class FilterAuthoringInfoForFilterBlocks(BaseModel):
    """
    The filter authoring info object contains metadata about the authoring process for creating a filter block object. This allows the authoring client to save specific, overridable settings.  The next time it is accessed via an authoring client, their selections are remembered. Non-authoring clients can ignore it.
    """

    model_config = common_config
    filter_types: list[FilterType] = Field(
        ...,
        alias="filterTypes",
        description="Array of defined filter types. Each filter type has an array of filter values.",
    )


class MetadataForCheckboxBasedFilterUI(BaseModel):
    """
    Client UI with checkbox representation for each filter type and filter value.
    """

    model_config = common_config
    filter_blocks: list[FilterAuthoringInfoForFilterBlocks] = Field(
        ...,
        alias="filterBlocks",
        description="Array of filter block authoring infos.",
    )
    type: Literal["checkbox"] = Field(
        "checkbox", description="Type of filter authoring info."
    )


class BuildingSceneLayerFilter(BaseModel):
    """
    Filter allows client applications to reduce the drawn elements of a building to specific types and values. Filters on the webscene override the list of existing filters on the service.
    """

    model_config = common_config
    description: str | None = Field(None, description="Description of the filter.")
    filter_authoring_info: MetadataForCheckboxBasedFilterUI | None = Field(
        None,
        alias="filterAuthoringInfo",
        description="Metadata about the authoring process for this filter.",
    )
    filter_blocks: list[BuildingSceneLayerFilterBlock] = Field(
        ...,
        alias="filterBlocks",
        description="Array of filter blocks defining the filter. A filter contains at least one filter block.",
        min_length=1,
    )
    id: str | None = Field(
        None,
        description="unique filter id (uuid). Either a new id to extend the list of filters, or an existing id to override properties of an existing filter.",
    )
    name: str = Field(..., description="Name of the filter.")


class FeatureReferenceLegacy(BaseModel):
    """
    References a feature by unique identifier, layer id, and sublayer id, if applicable.
    """

    model_config = common_config
    layer_id: float | str = Field(
        ...,
        alias="layerId",
        description="Identifies the layer to which the feature belongs.",
    )
    object_id: str | float = Field(
        ...,
        alias="objectId",
        description="ObjectId identifying the feature within the layer.",
    )


class LineOfSightObserver(BaseModel):
    """
    Defines the observer of a line of sight analysis in a LineOfSight layer.
    """

    model_config = common_config
    elevation_info: ElevationInfo | None = Field(None, alias="elevationInfo")
    feature: FeatureReferenceLegacy | None = None
    position: PointGeometry = Field(
        ..., description="Position of the line of sight observer."
    )


class LineOfSightTarget(BaseModel):
    """
    Defines the target of a line of sight analysis in a LineOfSight layer.
    """

    model_config = common_config
    elevation_info: ElevationInfo | None = Field(None, alias="elevationInfo")
    feature: FeatureReferenceLegacy | None = None
    position: PointGeometry = Field(
        ..., description="Position of the line of sight target."
    )


class VoxelSlice(BaseModel):
    """
    A slice is a plane through the voxel layer. Slices visually cut the voxel layer by removing portion of it. A slice or a combination of slices can define an area of interest showing only a portion of the original extent or show the entire of a voxel layer.
    """

    model_config = common_config
    enabled: bool | None = Field(True, description="Boolean defining slice visibility.")
    label: str | None = Field(None, description="Label for the slice.")
    normal: list[float] = Field(
        ...,
        description="Normal vector to the plane in voxel space. Defining the orientation of the slice.",
        max_length=3,
        min_length=3,
    )
    point: list[float] = Field(
        ...,
        description="Point belonging to the section plane in voxel space. Defining the position of the slice.",
        max_length=3,
        min_length=3,
    )


class VoxelSection(BaseModel):
    """
    A section that is locked in for a specific variable and/or time. Allowing you to view the section together with other variables. A locked (or static) section is `float32` raster. This JSON object contains the meta-data needed to display (and potentially re-generate) this raster.

     When creating a web scene the locked sections must have this information as all properties come the web scene. If no section information is given, it is assumed there are no sections in the web scene.
    """

    model_config = common_config
    enabled: bool | None = Field(
        True, description="Boolean defining section visibility."
    )
    href: str = Field(
        ...,
        description="Relative href to the static section raster binary. href is relative to the layer document which contains the href. Inside an SLPK archive, must be of the form `resources/sections/<file>.bin.gz` and the section raster should be gzip-compressed.",
    )
    id: int = Field(
        ...,
        description="The id of the section. Must be unique from other sections in the array.",
    )
    label: str | None = Field(None, description="The label for the section.")
    normal: list[float] = Field(
        ...,
        description="Normal vector to the plane in voxel space.",
        max_length=3,
        min_length=3,
    )
    point: list[float] = Field(
        ...,
        description="Point belonging to the section plane in voxel space.",
        max_length=3,
        min_length=3,
    )
    size_in_pixel: list[int] = Field(
        ...,
        alias="sizeInPixel",
        description="Array of size 2 which describes the dimension of the raster data in pixels.",
        max_length=2,
        min_length=2,
    )
    slices: list[VoxelSlice] | None = Field(
        None,
        description="A copy of the slices that were applied to the volume when the section was created when new UVs are discovered for subsequent time slices.",
    )
    time_id: int | None = Field(
        None,
        alias="timeId",
        description="Time slice id at which the section was created. `timeId` is only applicable when time is the 4th dimension.",
    )
    variable_id: int = Field(..., alias="variableId", description="Id of the variable.")


class VoxelDynamicSection(BaseModel):
    """
    A section is a plane through the voxel layer. A section can be moved by changing the position and orientation.
    """

    model_config = common_config
    enabled: bool | None = Field(
        True, description="Boolean defining section visibility."
    )
    label: str | None = Field(None, description="The label for the dynamic section.")
    normal: list[float] = Field(
        ...,
        description="Normal vector to the plane in voxel space. Defining the orientation of the dynamic section.",
        max_length=3,
        min_length=3,
    )
    point: list[float] = Field(
        ...,
        description="Point belonging to the section plane in voxel space. Defining the position of the dynamic section.",
        max_length=3,
        min_length=3,
    )


class VolumeStyle(BaseModel):
    """
    The volume style allows you to define the exaggeration and offset.
    """

    model_config = common_config
    dynamic_sections: list[VoxelDynamicSection] | None = Field(
        None,
        alias="dynamicSections",
        description="Array of dynamic sections of the volume. (Only if `style.renderMode = surfaces`). Dynamic sections are planes through the voxel layer used for visual analysis to inspect the voxel layer at specified positions. Sections are visible if the voxel layer shows surfaces. For example, you can define a cross section diagram using dynamic sections.",
    )
    exaggeration_mode: ExaggerationMode | None = Field(
        ExaggerationMode.scale_height,
        validate_default=True,
        alias="exaggerationMode",
        description="Determines how the vertical exaggeration is applied. `scale-height` scales from the voxel dataset origin only, for example, if a voxel layer has its minimum at sea level the layer will be exaggerated starting from sea level. `scale-height` is the default. `scale-position` also scales the space between voxel dataset origin in the coordinate system origin. This exaggeration mode is identical with exaggeration applied to other layers like feature layers (use the scale position option if you want to draw the voxel layer together with feature based data).",
    )
    slices: list[VoxelSlice] | None = Field(
        None,
        description="Array of slices applied to this volume. Slices reduce the voxel volume to an area of interest. Slices are applied to both voxel style renderModes (volume, surfaces).",
        min_length=1,
    )
    vertical_exaggeration: float | None = Field(
        1,
        alias="verticalExaggeration",
        description="Vertical exaggeration factor.",
    )
    vertical_offset: float | None = Field(
        0,
        alias="verticalOffset",
        description="Vertical offset value in vertical unit of the spatial reference of the voxel layer.",
    )
    volume_id: int | None = Field(
        0,
        alias="volumeId",
        description="Id of the volume in the volume array (a maximum of one style per volumeId).",
    )


class VoxelUniqueValue(BaseModel):
    """
    Describes the unique value of a variable with a discrete data type.
    """

    model_config = common_config
    color: list[confloat(ge=0, le=255)] = Field(
        ...,
        description="Color is represented as a three or four-element array. The four elements represent values for red, green, blue, and alpha in that order. Values range from 0 through 255. If color is undefined for a symbol, the color value is null.",
        title="color",
    )
    enabled: bool | None = Field(
        True, description="Show or hide all voxels equal to this value."
    )
    label: str | None = Field(None, description="Label for the unique value.")
    value: int = Field(..., description="Unique value of the variable.")


class VoxelRangeFilter(BaseModel):
    """
    Defines the range to filter values from. Voxel Values outside this range will be discarded.
    """

    model_config = common_config
    enabled: bool | None = Field(
        False, description="Determines if the range filter is enabled."
    )
    range: list[float] = Field(
        ...,
        description="Defines the minimum and maximum values of the range. Data outside of the range will be discarded.",
        max_length=2,
        min_length=2,
    )


class VoxelAlphaStop(BaseModel):
    """
    Defines transparency stop for a transfer function style.
    """

    model_config = common_config
    alpha: confloat(ge=0.0, le=1.0) = Field(
        ..., description="Opacity of the stop in [0,1]. 1 is fully opaque."
    )
    position: confloat(ge=0.0, le=1.0) = Field(
        ..., description="Normalized position of the stop in [0,1]."
    )


class VoxelColorStop(BaseModel):
    """
    Defines color stop for a transfer function style.
    """

    model_config = common_config
    color: list[confloat(ge=0, le=255)] = Field(
        ...,
        description="Color is represented as a three or four-element array. The four elements represent values for red, green, blue, and alpha in that order. Values range from 0 through 255. If color is undefined for a symbol, the color value is null.",
        title="color",
    )
    position: confloat(ge=0.0, le=1.0) = Field(
        ..., description="Normalized position of the stop in [0,1]."
    )


class VoxelTransferFunctionStyle(BaseModel):
    """
    Defines the mapping between voxel values and color and transparency.
    """

    model_config = common_config
    alpha_stops: list[VoxelAlphaStop] | None = Field(
        None,
        alias="alphaStops",
        description="Describes the transparency stops (transparency mapping).",
        min_length=2,
    )
    color_stops: list[VoxelColorStop] | None = Field(
        None,
        alias="colorStops",
        description="Describes the color stops defining the color ramp.",
        min_length=2,
    )
    interpolation: VoxelInterpolation | None = Field(
        None, description="Interpolation mode"
    )
    range_filter: VoxelRangeFilter | None = Field(
        None,
        alias="rangeFilter",
        description="Defines the range of voxels displayed. Voxels with values outside of this range will be discarded.",
    )
    stretch_range: list[float] = Field(
        ...,
        alias="stretchRange",
        description="Describes the low and high point for value-to-color mapping.",
        max_length=2,
        min_length=2,
    )


class VoxelIsosurfaceStyle(BaseModel):
    """
    The isosurface style describes the value and coloring of the isosurface. For example, for a given variable temperature you can define up to four isosurfaces based on a temperature value.
    """

    model_config = common_config
    color: list[confloat(ge=0, le=255)] = Field(
        ...,
        description="Color is represented as a three or four-element array. The four elements represent values for red, green, blue, and alpha in that order. Values range from 0 through 255. If color is undefined for a symbol, the color value is null.",
        title="color",
    )
    color_locked: bool | None = Field(
        False,
        alias="colorLocked",
        description="If false the isosurface color is automatically updated when the variable's colorStops are modified.",
    )
    enabled: bool | None = Field(
        True,
        description="Determines if the isosurface should be shown or hidden.",
    )
    label: str | None = Field(None, description="Label for the isosurface.")
    value: float = Field(..., description="Value of the variable.")


class VoxelVariableStyle(BaseModel):
    """
    The voxel variable style defines how the voxel layer will render for a variable. A variable can be discrete (integer values) or continuous (float values).
    """

    model_config = common_config
    isosurfaces: list[VoxelIsosurfaceStyle] | None = Field(
        None,
        description="Array of styles for isosurfaces. (Only if `variable.originalFormat.continuity = continuous`). An isosurface represents a surface at a specific value. A voxel layer can have up to four isosurfaces.",
        min_length=1,
    )
    label: str | None = Field(None, description="Label for the variable.")
    transfer_function: VoxelTransferFunctionStyle | None = Field(
        None,
        alias="transferFunction",
        description="Defines the stretch rendering of the voxel layer. The transfer function maps voxel values to color and transparency. Scalar field only. (Only if `variable.originalFormat.continuity = continuous`).",
    )
    unique_values: list[VoxelUniqueValue] | None = Field(
        None,
        alias="uniqueValues",
        description="Defines the unique value rendering as an array of unique value styles (Only if `variable.originalFormat.continuity = discrete`).",
        min_length=0,
    )
    variable_id: int = Field(
        ...,
        alias="variableId",
        description="Id of the variable. The variable styles will be defined for the variable with the given Id. If the `variableId` and the voxel style `currentVariableId` are identical the style will be shown. ",
    )


class VoxelShading(BaseModel):
    """
    Describes the shading properties of the voxel layer. Voxel layers use a simple shading model which provides specular highlights and diffuse shading proportional to the opacity of the voxel being accumulated during raycasting. Opaque voxels are shaded like a surface while semi-transparent voxels mostly contribute their color.
    """

    model_config = common_config
    diffuse_factor: confloat(ge=0.0, le=1.0) | None = Field(
        0.5, alias="diffuseFactor", description="Diffuse light coefficient."
    )
    specular_factor: confloat(ge=0.0, le=1.0) | None = Field(
        0.5,
        alias="specularFactor",
        description="Specular highlight coefficient.",
    )


class VoxelStyle(BaseModel):
    """
    A voxel style allows you to define the visualization of the voxel layer. A voxel layer can be represented as volume or surface depending on your analysis needs. You can change the visibility of the different representations and change their drawing options.
    """

    model_config = common_config
    current_variable_id: int = Field(
        ...,
        alias="currentVariableId",
        description="Id of the currently visible variable.",
    )
    enable_dynamic_sections: bool | None = Field(
        None,
        alias="enableDynamicSections",
        description='Enable dynamic section (only if `renderMode = "surfaces"`).',
    )
    enable_isosurfaces: bool | None = Field(
        None,
        alias="enableIsosurfaces",
        description='Enable isosurfaces (only if `renderMode = "surfaces"`).',
    )
    enable_sections: bool | None = Field(
        None, alias="enableSections", description="Enable static sections."
    )
    enable_slices: bool | None = Field(
        None, alias="enableSlices", description="Enable slices."
    )
    render_mode: VoxelRenderMode | None = Field(
        None,
        alias="renderMode",
        description="Current rendering mode for the voxel layer. Depending on the rendering mode different voxel layer representations can be shown. `volume` draws the voxel layer as rectangular cuboid (but any slices defined for the voxel layer will change the volume to the area of interest). `surfaces` will represent the layer as a set of surfaces, for example, dynamic sections can define a plane through the volume or isosurfaces can show a specific value as surface.",
    )
    shading: VoxelShading | None = Field(
        None, description="Defines the shading properties."
    )
    variable_styles: list[VoxelVariableStyle] | None = Field(
        None,
        alias="variableStyles",
        description="Array of variable styles. Each variable can have one variable style.",
        min_length=1,
    )
    volume_styles: list[VolumeStyle] | None = Field(
        None,
        alias="volumeStyles",
        description="Array of volume styles. Currently only one volume style is allowed.",
        min_length=1,
    )


class VoxelLayerDefinition(BaseModel):
    """
    The voxelLayerDefinition contains drawing information for a voxel layer.
    """

    model_config = common_config
    max_scale: float | None = Field(
        None,
        alias="maxScale",
        description="Represents the maximum scale (most zoomed in) at which the layer is visible in the view. If the web scene is zoomed in beyond this scale, the layer will not be visible. A value of 0 means the layer does not have a maximum scale. If set, the maxScale value should always be smaller than the minScale value, and greater than or equal to the service specification.",
    )
    min_scale: float | None = Field(
        None,
        alias="minScale",
        description="Represents the minimum scale (most zoomed out) at which the layer is visible in the view. If the web scene is zoomed out beyond this scale, the layer will not be visible. A value of 0 means the layer does not have a minimum scale. If set, the minScale value should always be larger than the maxScale value, and lesser than or equal to the service specification.",
    )
    sections: list[VoxelSection] | None = Field(
        None,
        description="Array of metadata about sections. A section is a static plane through the voxel layer showing the variable the section was created at.",
        min_length=0,
    )
    style: VoxelStyle | None = Field(
        None,
        description="Voxel style describes how the layer will be drawn including rendering and voxel representions that will be visible.",
    )


class DimensionSimpleStyle(BaseModel):
    """
    Specification of how dimensions and their labels are displayed.
    """

    model_config = common_config
    color: list[confloat(ge=0, le=255)] = Field(
        ..., description="Color of dimension lines.", title="color"
    )
    font_size: confloat(ge=0.0) = Field(
        ...,
        alias="fontSize",
        description="Font size of dimension label text in points.",
    )
    line_size: confloat(ge=0.0) = Field(
        ...,
        alias="lineSize",
        description="Width of dimension lines in points.",
    )
    text_background_color: list[confloat(ge=0, le=255)] = Field(
        ...,
        alias="textBackgroundColor",
        description="Background color of dimension labels.",
        title="color",
    )
    text_color: list[confloat(ge=0, le=255)] = Field(
        ...,
        alias="textColor",
        description="Color of dimension label text.",
        title="color",
    )
    type: Literal["simple"] = Field(
        "simple", description="Specifies the type of style used."
    )


class LengthDimension(BaseModel):
    """
    Defines the shape of a dimension that measures the distance between two points.
    """

    model_config = common_config
    end_point: PointGeometry = Field(
        ...,
        alias="endPoint",
        description="The position of the point that a dimension is measured to (last input point).",
    )
    measure_type: MeasureType = Field(
        ...,
        alias="measureType",
        description="Defines whether the horizontal, vertical or direct distance between the start and end points is measured.",
    )
    offset: float = Field(
        ...,
        description="The distance of the dimension line from the nearest input point in meters.",
    )
    orientation: float = Field(
        ...,
        description="The direction that the offset of a dimension with a 'direct' measureType extends in.",
    )
    start_point: PointGeometry = Field(
        ...,
        alias="startPoint",
        description="The position of the point that a dimension is measured from (first input point).",
    )
    type: Literal["length"] = Field(
        "length", description="Specifies the type of the dimension."
    )


class Viewshed(BaseModel):
    """
    Defines the shape of a viewshed that applies a visibility analysis.
    """

    model_config = common_config

    far_distance: float = Field(
        ...,
        alias="farDistance",
        description="The maximum distance from the observer in which to perform the viewshed analysis (in meters).",
    )
    feature: FeatureReferenceLegacy | None = Field(
        None,
        description="References a feature from which the observer is internally offset, provided that its sides are close enough to the observer. It is used to ensure that the analysis results remain independent of changes in the level of detail of the scene geometry.",
    )
    heading: float = Field(
        ...,
        description="The compass heading of the observer's view direction (in degrees). A heading of zero points the viewshed to north and it increases rotating in clock-wise order.",
        le=360.0,
        ge=0.0,
    )
    horizontal_field_of_view: float = Field(
        ...,
        alias="horizontalFieldOfView",
        description="The horizontal field of view (FOV) angle defines the width of the scope being analyzed (in degrees). A value of 360 means the observer's horizontal FOV captures their entire surroundings. Values closer to 0 narrow the horizontal FOV in the direction of the heading.",
        le=360.0,
        ge=0.0,
    )
    observer: PointGeometry = Field(
        ...,
        description="The position the viewshed is computed from.",
    )
    tilt: float = Field(
        ...,
        description="The tilt of the observer's view direction (in degrees). A tilt of zero points the viewshed looking straight down and 90 degrees points it looking parallel to the surface.",
        le=180.0,
        ge=0.0,
    )
    vertical_field_of_view: float = Field(
        ...,
        alias="verticalFieldOfView",
        description="The vertical field of view (FOV) angle defines the height of the scope being analyzed (in degrees). This value can vary from 0 to 180. Values closer to 0 narrow the vertical FOV in the direction of the tilt.",
        le=180.0,
        ge=0.0,
    )


DynamicDataLayer.model_rebuild()
DynamicMapLayer.model_rebuild()
