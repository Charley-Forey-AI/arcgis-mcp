from __future__ import annotations  # Enables postponed evaluation of type hints

from .base_model import BaseModel, common_config, SymbolValidatorMixin
from typing import Literal, Annotated
from pydantic import Field, StringConstraints, conint, field_validator
from ..enums.renderers import (
    RampAlgorithm,
    ColorRampType,
    RatioStyle,
    SpikeSymbolStyle,
    Theme,
    VisualVariableType,
    RendererType,
    FlowTheme,
    Focus,
    LengthUnit,
    StandardDeviationInterval,
    UnivariateSymbolStyle,
    TimeUnits,
    ClassificationMethod,
    UnivariateTheme,
    LegendOrder,
    Axis,
    ValueRepresentation,
    StretchType,
    FlowRepresentation,
    TrailCap,
    NormalizationType,
    InputOutputUnit,
    VectorFieldStyle,
    HillshadeType,
    ScalingType,
    FieldTransformType,
)
from ..enums.layer_definition import RotationType
from ..enums.popups import ArcadeReturnType
from .symbols import (
    CimSymbolReference,
    PictureFillSymbolEsriPFS,
    PictureMarkerSymbolEsriPMS,
    SimpleFillSymbolEsriSFS,
    SimpleLineSymbolEsriSLS,
    SimpleMarkerSymbolEsriSMS,
    TextSymbolEsriTS,
    PointSymbol3D,
    LineSymbol3D,
    PolygonSymbol3D,
    MeshSymbol3D,
    StyleSymbolReference,
)


class ColorRamp(BaseModel):
    """
    A colorRamp object is used to specify a range of colors that are applied to a group of symbols.
    """

    model_config = common_config

    algorithm: RampAlgorithm | None = Field(
        None, description="Algorithm used for calculating the ramp."
    )
    color_ramps: list["ColorRamp"] | None = Field(
        None,
        alias="colorRamps",
        description="A multipart color ramp is defined by a list of constituent color ramps.",
    )
    from_color: list[Annotated[int, Field(ge=0, le=255)]] | None = Field(
        None,
        alias="fromColor",
        description="Array representing the initial color to start the ramp from.",
    )
    to_color: list[Annotated[int, Field(ge=0, le=255)]] | None = Field(
        None,
        alias="toColor",
        description="Array representing the final color to end the ramp with.",
    )
    type: ColorRampType | None = Field(
        None, description="Value indicating the type of colorRamp."
    )


class AuthoringInfoVisualVariable(BaseModel):
    """
    This visual variable pertains specifically to authoringInfo and is different from visual variables directly on the renderer.
    """

    model_config = common_config

    end_time: float | str | None = Field(
        None,
        alias="endTime",
        description="A Unix stamp. Both `start_time` or `end_time` can be fields. If this is the case, their names must be different.",
    )
    field: str | None = Field(
        None,
        description="The attribute field the user chose in the Smart Mapping gallery. Applies to age style, `reference-size` theme, and `spike` theme. For age style, this value must also be specified in either `startTime` or `endTime`. This property is used for UI purposes only.",
    )
    max_slider_value: float | int | None = Field(
        None,
        alias="maxSliderValue",
        description="A numeric value indicating the maximum value displayed.",
    )
    min_slider_value: float | int | None = Field(
        None,
        alias="minSliderValue",
        description="A numeric value indicating the minimum value displayed.",
    )
    normalization_field: str | None = Field(
        None,
        alias="normalizationField",
        description="Only applicable when `theme` is `reference-size` or `spike`. Indicates the attribute field the user chose in the Smart Mapping gallery for normalizing data in the selected `field`. This property is used for UI purposes only.",
    )
    reference_size_scale: float | int | None = Field(
        0,
        alias="referenceSizeScale",
        description="A numeric value indicating the scale of the reference size. This is used to calculate the size of the symbol in the `reference-size` theme.",
        ge=0,
    )
    reference_size_symbol_style: UnivariateSymbolStyle | None = Field(
        None,
        alias="referenceSizeSymbolStyle",
        description="The symbol style to be used for the reference size. This is used in the `reference-size` theme.",
    )
    size_stops: list[SizeStop] | None = Field(
        None,
        alias="sizeStops",
        description="An array of objects that defines the thematic size ramp for a size-based visualization with a `reference-size` or `spike` theme. At least two stops are required. The stops must be listed in ascending order based on the value of the `value` property in each stop. This property is required when `theme` is `reference-size` or `spike` for the legend to properly render the symbol style along with its labels.",
    )
    spike_symbol_style: SpikeSymbolStyle | None = Field(
        None,
        alias="spikeSymbolStyle",
        description="Only applicable when `theme` is `spike`. This value describes the configuration of the generated spike symbol.  Visualizations with a spike theme must be defined in a ClassBreaksRenderer or UniqueValueRenderer with a CIMSymbol resembling a spike whose size is driven by a data value set in the renderer's field property, and configured in a primitive override of the CIMSymbol. These renderers must not contain size visual variables, but may contain other visual variable types. This property is used for UI purposes only. When defined, `AuthoringInfoVisualVariable.sizeStops` must also be defined for the legend to properly represent the visualization.",
    )
    start_time: float | str | None = Field(
        None,
        alias="startTime",
        description="A Unix time stamp. Both `start_time` or `end_time` can be fields. If this is the case, their names must be different.",
    )
    style: RatioStyle | None = Field(
        None,
        description="It is used to map the ratio between two numbers. It is possible to express that relationship as percentages, simple ratios, or an overall percentage.",
    )
    theme: Theme | None = Field(
        None,
        description="Theme to be used only when working with visual variables of type `colorInfo` or `sizeInfo`. Default is `high-to-low`. The `centered-on`, and `extremes` themes only apply to `colorInfo` visual variables.",
    )
    type: VisualVariableType | None = Field(
        None,
        description="A string value specifying the type of renderer's visual variable.",
    )
    units: TimeUnits | None = Field(
        None, description="Units for `start_time` and `end_time`."
    )


class AuthoringInfoStatistics(BaseModel):
    """
    Statistics queried from the layer to be used by the legend. The statistics can be used by the legend to avoid displaying data values that fall outside the data range despite the renderer's configuration. Only applies to `univariateColorSize` styles with an `above-and-below` `univariateTheme`.
    """

    model_config = common_config

    max: float | int | None = Field(
        None,
        description="A numeric value indicating the maximum value of the data represented by the renderer.",
    )
    min: float | int | None = Field(
        None,
        description="A numeric value indicating the minimum value of the data represented by the renderer.",
    )


class AuthoringInfoClassBreakInfo(BaseModel):
    """
    The classBreaksInfo object provides information about the class breaks associated with the Relationship renderer.
    """

    model_config = common_config

    max_value: float | int | None = Field(
        None,
        alias="maxValue",
        description="A numeric value used to specify the maximum value for a break.",
    )
    min_value: float | int | None = Field(
        None,
        alias="minValue",
        description="A numeric value used to specify the minimum value for a break.",
    )


class AuthoringInfoField(BaseModel):
    """
    Contains information about an attribute field relating to Relationship renderers.
    """

    model_config = common_config

    class_break_infos: list[AuthoringInfoClassBreakInfo] | None = Field(
        None, alias="classBreakInfos"
    )
    field: str | None = Field(None, description="Attribute field used for renderer.")
    label: str | None = Field(
        None,
        description="The label used to describe the field or attribute in the legend.",
    )
    normalization_field: str | None = Field(
        None,
        alias="normalizationField",
        description="Attribute field used to normalize the data.",
    )


class AuthoringInfo(BaseModel):
    """
    The authoringInfo is an object containing metadata about the authoring process for creating a renderer object. This allows the authoring clients to save specific overridable settings so that next time it is accessed via an authoring client, their selections are remembered. Non-authoring clients can ignore it. Properties for color/size/transparency sliders, theme selection, classification information, and additional properties are saved within this object.
    """

    model_config = common_config

    classification_method: ClassificationMethod | None = Field(
        None,
        alias="classificationMethod",
        validate_default=True,
        description="Used for classed color or size. The default value is `esriClassifyManual`. The `esriClassifyDefinedInterval` method is only applicable to raster class breaks renderer only.",
    )
    color_ramp: ColorRamp | None = Field(None, alias="colorRamp")
    fade_ratio: Annotated[float, Field(ge=0.0, le=1.0)] | None = Field(
        None,
        alias="fadeRatio",
        description="The degree with which to fade colors in a `heatmap`. A value of `0` indicates all color stops in the heatmap have an alpha of 1. A value of `1` indicates the color stop representing the highest density has an alpha value of 1, but all other color stops decrease in opacity significantly. Values between 0 and 1 will result in less transparent alpha values for each color stop. When the renderer is persisted, these alpha values will be persisted in the renderer's color stops.",
    )
    field1: AuthoringInfoField | None = None
    field2: AuthoringInfoField | None = None
    fields: list[str] | None = Field(
        None,
        description="An array of string values representing field names used for creating predominance renderers.",
    )
    flow_theme: FlowTheme | None = Field(
        None,
        alias="flowTheme",
        description="Theme to be used only when working with renderers of type `flow`.",
    )
    focus: Focus | None = Field(
        None,
        description="Optional. Used for Relationship renderer. If not set, the legend will default to being square.",
    )
    length_unit: LengthUnit | None = Field(
        None,
        alias="lengthUnit",
        description="Unit used in user interfaces to display world/map sizes and distances",
    )
    is_auto_generated: bool | None = Field(
        None,
        alias="isAutoGenerated",
        description="Only applicable to FeatureReductionCluster renderers. Indicates whether the renderer was automatically created internally in behalf of the user by the JS API's rendering engine. When a user manually creates a FeatureReductionCluster renderer, this option should be ignored.",
    )
    max_slider_value: float | int | None = Field(
        None,
        alias="maxSliderValue",
        description="Optional. Indicates the maximum value of a slider if one was used to generate the dot value for dot density renderer.",
    )
    min_slider_value: float | int | None = Field(
        None,
        alias="minSliderValue",
        description="Optional. Indicates the minimum value of a slider if one was used to generate the dot value for dot density renderer.",
    )
    num_classes: Annotated[int, Field(ge=2, le=4)] | None = Field(
        None,
        alias="numClasses",
        description="Number of classes to be associated with the relationship. Used for Relationship renderer.",
    )
    standard_deviation_interval: StandardDeviationInterval | None = Field(
        None,
        alias="standardDeviationInterval",
        description="Use this property if the classificationMethod is `esriClassifyStandardDeviation`.",
    )
    statistics: AuthoringInfoStatistics | None = Field(
        None,
        description="Statistics used by the legend to avoid representing data values that are beyond the dataset max and min. Only applies to renderers of type `univariateColorSize` with an 'above-and-below' `univariateTheme`.",
    )
    type: RendererType | None = None
    univariate_symbol_style: UnivariateSymbolStyle | None = Field(
        None,
        alias="univariateSymbolStyle",
        description="Symbol style or symbol pair used when creating a renderer of type `univariateColorSize` with an `above-and-below` univariateTheme. The `custom` style indicates the renderer uses a custom symbol pair not provided by the authoring application.",
    )
    univariate_theme: UnivariateTheme | None = Field(
        None,
        alias="univariateTheme",
        description="Theme to be used only when working with renderers of type `univariateColorSize`.",
    )
    visual_variables: list[AuthoringInfoVisualVariable] | None = Field(
        None,
        alias="visualVariables",
        description="An array of visualVariable objects containing additional information needed when authoring the renderer.",
    )


class HeatmapColorStop(BaseModel):
    """
    A colorStop object describes the renderer's color ramp with more specificity than just colors.
    """

    model_config = common_config

    color: list[Annotated[int, Field(ge=0, le=255)]]
    ratio: float = Field(
        ...,
        description="A number between 0-1. Describes what portion along the gradient the colorStop is added.",
    )


class LegendOptions(BaseModel):
    """
    Options available for the legend.
    """

    model_config = common_config

    dot_label: str | None = Field(
        None,
        alias="dotLabel",
        description="The text that should be displayed in the legend for each dot. This will display next to the number 1. If not present, it will be a localized string for the word 'Dot'. Only applicable to dot density renderers.",
    )
    max_label: str | None = Field(
        None,
        alias="maxLabel",
        description="Text in the legend that describes the hottest (most dense) part of the heatmap. Only applicable to Heatmap renderers. If not specified, then a localized label, for 'High' will display on the legend.",
    )
    min_label: str | None = Field(
        None,
        alias="minLabel",
        description="Text in the legend that describes the coldest (least dense) part of the heatmap. Only applicable to Heatmap renderers. If not specified, then a localized label, for 'Low' will display on the legend.",
    )
    order: LegendOrder | None = Field(
        None, description="Indicates the order in which the legend is displayed."
    )
    show_legend: bool | None = Field(
        None,
        alias="showLegend",
        description="Indicates whether to show the color/size/opacity ramp in the legend. **This property is not available directly under UniqueValueRenderer and/or ClassBreaksRenderer.**",
    )
    title: str | None = Field(None, description="The title of the legend.")
    unit: str | None = Field(
        None,
        description="Indicates the unit of the data being visualized in a dotDensity renderer. This will display next to the dot value in the title of the legend.",
    )


class ColorStop(BaseModel):
    """
    A colorStop object describes the renderer's color ramp with more specificity than just colors.
    """

    model_config = common_config

    color: list[Annotated[int, Field(ge=0, le=255)]] = Field(
        ...,
        description="A CSS color string or an array of rbga values. The color to place at the stop indicated by either a ratio or value.",
    )
    label: str | None = Field(
        None, description="Value if a label is needed on the legend for a stop."
    )
    value: float = Field(
        None,
        description="The pixel intensity value. Describes the pixel intensity value that the color should be associated with. Just like in colorInfo, using value will ignore `maxPixelIntensity` and `minPixelIntensity` properties. It will actually set those properties to maximum and minimum values you set in the colorStops array. The hard values are converted to ratios to create the color gradient that is used in the heatmap calculations. Setting `minPixelIntensity` or `maxPixelIntensity`, after setting colorStops with values, removes the hard link between the color ramp and the pixel intensity values that were used to create it.",
    )


class ColorInfoVisualVariable(BaseModel):
    """
    The colorInfo visual variable defines how a continuous color ramp is applied to features based on the values of a numeric field attribute.
    """

    model_config = common_config

    field: str | None = Field(
        None,
        description="Attribute field used for color rendering if no valueExpression is provided.",
    )
    legend_options: LegendOptions | None = Field(None, alias="legendOptions")
    normalization_field: str | None = Field(
        None,
        alias="normalizationField",
        description="Attribute field used to normalize the data.",
    )
    stops: list[ColorStop] | None = Field(None, description="An array of stop objects.")
    type: Literal["colorInfo"] = Field(
        "colorInfo", description="Specifies the type of visual variable."
    )
    value_expression: str | None = Field(
        None,
        alias="valueExpression",
        description="An [Arcade expression](https://developers.arcgis.com/arcade/) that computes a value in lieu of a value provided by an attribute `field`.",
    )
    value_expression_title: str | None = Field(
        None,
        alias="valueExpressionTitle",
        description="The title identifying and describing the associated [Arcade](https://developers.arcgis.com/arcade/) expression as defined in the `valueExpression` property.",
    )


class RotationInfoVisualVariable(BaseModel):
    """
    The rotation visual variable defines how features rendered with marker symbols are rotated. The rotation value is determined by a value in a field or an Arcade expression calculating a value. Use either the `field` property or `valueExpression` when specifying rotation values.
    """

    model_config = common_config
    axis: Axis | None = Field(
        Axis.heading,
        validate_default=True,
        description="Defines the rotation axis the visual variable should be applied to when rendering features with an ObjectSymbol3DLayer. [Detailed description](static/objectSymbolLayerOrientation.md).",
    )
    field: str | None = Field(
        None,
        description="Attribute field used for setting the rotation of a symbol if no `valueExpression` is provided.",
    )
    legend_options: LegendOptions | None = Field(None, alias="legendOptions")
    rotation_type: RotationType | None = Field(
        RotationType.geographic.value,
        alias="rotationType",
        description="Defines the origin and direction of rotation depending on how the angle of rotation was measured. Possible values are `geographic` which rotates the symbol from the north in a clockwise direction and `arithmetic` which rotates the symbol from the east in a counter-clockwise direction.",
    )
    type: Literal["rotationInfo"] = Field(
        "rotationInfo",
        description="A string value indicating the type of visual variable used for the renderer.",
    )
    value_expression: str | None = Field(
        None,
        alias="valueExpression",
        description="An [Arcade expression](https://developers.arcgis.com/arcade/) evaluating to a number.",
    )
    value_expression_title: str | None = Field(
        None,
        alias="valueExpressionTitle",
        description="The title identifying and describing the associated [Arcade expression] (https://developers.arcgis.com/arcade/) as defined in the `valueExpression` property.",
    )


class SizeStop(BaseModel):
    """
    A `sizeStop` object describes the size of the symbol at various values of the expression.
    """

    model_config = common_config
    label: str | None = Field(
        None,
        description="Value if a label is needed on the legend for a stop.",
    )
    size: float = Field(
        ..., description="Specifies the marker size to use for the specified value."
    )
    value: float = Field(
        ...,
        description="The value to be mapped to a size. Depending on how the visual variable is defined, the value may represent the value of an attribute field or the value returned by an expression.",
    )


class Size(BaseModel):
    """
    Specifies the marker size to use at any given map scale. This is required if valueUnit is set to `unknown`.
    """

    model_config = common_config

    expression: str = Field(
        None,
        description="The value which allows a size to be defined based on the map scale. Currently the only supported expression is `view.scale`.",
    )
    stops: list[SizeStop] = Field(
        ...,
        description="An array of objects that define the size of the symbol at various values of the expression. Each object in the array has a numeric size property and a numeric value property. If the value calculated from the expression matches the value of a stop, then the size corresponding to that stop is selected. For example, if expression is set to `view.scale`, the value corresponds to the map's scale. The size represents the symbol size (in points) that corresponds to this scale. If the map scale matches the scale value of a stop, the size corresponding to that stop value is used as the symbol size for the features. If the map scale value falls between two stops, the symbol size is interpolated between the sizes of the two stops. The minSize and maxSize stop values are usually the same, although it is possible to have different values depending on how minSize is calculated versus the maxSize.",
    )
    type: Literal["sizeInfo"] = Field(
        "sizeInfo", description="Value indicating the type of rendering."
    )
    value_expression: str | None = Field(
        None,
        alias="valueExpression",
        description="An [Arcade expression](https://developers.arcgis.com/arcade/) evaluating to a number.",
    )


class SizeInfoVisualVariable(BaseModel):
    """
    The sizeInfo visual variable defines how size is applied to features based on the values of a numeric field attribute. The minimum and maximum values of the data should be indicated along with their respective size values. You must specify `minSize` and `maxSize` or `stops` to construct the size ramp. All features with values falling in between the specified min and max data values (or stops) will be scaled proportionally between the provided min and max sizes.
    """

    model_config = common_config
    axis: Axis | None = Field(
        Axis.all,
        validate_default=True,
        description="Defines the axis the size visual variable should be applied to when rendering features with an ObjectSymbol3DLayer.",
    )
    field: str | None = Field(
        None,
        description="Attribute field used for size rendering if no valueExpression is provided.",
    )
    legend_options: LegendOptions | None = Field(None, alias="legendOptions")
    max_data_value: float | int | None = Field(
        None, alias="maxDataValue", description="The maximum data value."
    )
    max_size: Size | float | None = Field(
        None,
        alias="maxSize",
        description="Specifies the largest marker size to use at any given map scale. Can be either a fixed number or object, depending on whether the user chose a fixed range or not.",
    )
    min_data_value: float | int | None = Field(
        None, alias="minDataValue", description="The minimum data value."
    )
    min_size: Size | float | None = Field(
        None,
        alias="minSize",
        description="Specifies the smallest marker size to use at any given map scale. Can be either a fixed number or object, depending on whether the user chose a fixed range or not.",
    )
    normalization_field: str | None = Field(
        None,
        alias="normalizationField",
        description="Attribute field used to normalize the data.",
    )
    stops: list[SizeStop] | None = Field(
        None,
        description="An array of objects that defines the thematic size ramp in a sequence of data or expression stops. At least two stops are required. The stops must be listed in ascending order based on the value of the `value` property in each stop. This property is required if `minDataValue`, `maxDataValue`, `minSize`, and `maxSize` are not defined. This property is also required when setting a size visual variable to the `minSize` or `maxSize` properties based on `expression` (e.g. `expression: 'view.scale'`).",
    )
    target: Literal["outline"] | None = Field(
        None, description="Only used when sizeInfo is used for polygon outlines."
    )
    type: Literal["sizeInfo"] = Field(
        "sizeInfo", description="Specifies the type of visual variable."
    )
    use_symbol_value: bool | None = Field(
        None,
        alias="useSymbolValue",
        description="When setting a size visual variable on a renderer using an ObjectSymbol3DLayer, this property indicates whether to apply the value defined by the height, width, or depth properties to the corresponding axis of this visual variable instead of proportionally scaling this axis' value after other axes.",
    )
    value_expression: str | None = Field(
        None,
        alias="valueExpression",
        description="An [Arcade expression](https://developers.arcgis.com/arcade/) evaluating to a number. New style is similar to `$view.scale`. This is used in combination with the `target` `outline` property where the outline looks thinner at smaller scales and thicker at larger scales.",
    )
    value_expression_title: str | None = Field(
        None,
        alias="valueExpressionTitle",
        description="The title identifying and describing the associated [Arcade](https://developers.arcgis.com/arcade/) expression as defined in the `valueExpression` property.",
    )
    value_representation: ValueRepresentation | None = Field(
        None,
        alias="valueRepresentation",
        description="Specifies how to apply the data value when mapping real-world sizes. See table below for supported values.",
    )
    value_unit: str | None = Field(
        None,
        alias="valueUnit",
        description="A string value indicating the required unit of measurement.",
    )


class TransparencyStop(BaseModel):
    """
    The transparencyStop object defines the thematic opacity ramp in a sequence of stops. At least two stops are required. The stops must be listed in ascending order based on the value of the `value` property in each stop.
    """

    model_config = common_config

    label: str | None = Field(
        None, description="A string value used to label the stop in the legend."
    )
    transparency: int | float = Field(
        ...,
        description="A numeric transparancy value for a stop ranging from 0-100, where 0 is opaque and 100 is 100% transparent.",
    )
    value: float = Field(
        ...,
        description="The pixel intensity value. Describes the pixel intensity value that the color should be associated with.",
    )


class TransparencyInfoVisualVariable(BaseModel):
    """
    The transparencyInfo visual variable defines the transparency, or opacity, of each feature's symbol based on a numeric attribute field value.
    """

    model_config = common_config

    field: str | None = Field(
        None,
        description="Attribute field used for setting the transparency of a feature if no `valueExpression` is provided.",
    )
    legend_options: LegendOptions | None = Field(None, alias="legendOptions")
    normalization_field: str | None = Field(
        None,
        alias="normalizationField",
        description="Attribute field used to normalize the data.",
    )
    stops: list[TransparencyStop] | None = Field(
        None, description="An array of transparencyStop objects."
    )
    type: Literal["transparencyInfo"] = Field(
        "transparencyInfo", description="Specifies the type of visual variable."
    )
    value_expression: str | None = Field(
        None,
        alias="valueExpression",
        description="An [Arcade expression](https://developers.arcgis.com/arcade/) evaluating to a number.",
    )
    value_expression_title: str | None = Field(
        None,
        alias="valueExpressionTitle",
        description="The title identifying and describing the associated [Arcade](https://developers.arcgis.com/arcade/) expression as defined in the `valueExpression` property.",
    )


class UniqueValueInfo(SymbolValidatorMixin):
    """
    The following is a list of properties found on the uniqueValueInfo object, which is one of the properties on the renderer object. The uniqueValueInfo object contains the symbology for each uniquely drawn value in the renderer.
    """

    model_config = common_config

    alternate_symbols: list[CimSymbolReference] | None = Field(
        None,
        alias="alternateSymbols",
        description="An array of symbol alternatives to a primary symbol. When alternative symbols are present, each symbol has minimum scale and maximum scale at which the symbol should be made visible. For any renderer that support alternate symbols, there is a primary symbol that has minimum and maximum scale defined. When rendering these renderer classes, the renderer should pick only one symbol at a given map scale for a given renderer class. The order of picking a symbol should be starting with primary symbol to check if it falls within the map's scale first before it iterates through  alternate symbols in the array starting from 0. The renderer will then pick the first symbol that is visible at current map scale. A symbol is visible if the map scale is greater than symbol's maximum scale and less than or equal to symbol's minimum scale.",
    )
    description: str | None = Field(
        None, description="String value used to describe the drawn symbol."
    )
    label: str | None = Field(
        None, description="String value used to label the drawn symbol."
    )
    symbol: (
        CimSymbolReference
        | PictureFillSymbolEsriPFS
        | PictureMarkerSymbolEsriPMS
        | SimpleFillSymbolEsriSFS
        | SimpleLineSymbolEsriSLS
        | SimpleMarkerSymbolEsriSMS
        | TextSymbolEsriTS
        | PointSymbol3D
        | LineSymbol3D
        | PolygonSymbol3D
        | MeshSymbol3D
        | LineSymbol3D
        | MeshSymbol3D
        | PointSymbol3D
        | PolygonSymbol3D
        | StyleSymbolReference
        | None
    ) = Field(None, description="An object used to display the value.")
    value: str | int | float = Field(
        None, description="String value indicating the unique value."
    )


class AttributeColorInfo(BaseModel):
    """
    The following is a list of properties found on the attributeColorInfo object. This object defines colors used to represent numeric fields in a dotDensity renderer or a pieChart renderer.
    """

    model_config = common_config

    color: list[Annotated[int, Field(ge=0, le=255)]] = Field(
        ...,
        description="The color used to represent the field or valueExpression when rendering dots in a dotDensity renderer or slices in a pieChart renderer. Color is represented as a four-element array. The four elements represent values for red, green, blue, and alpha in that order. Values range from 0 through 255. If color is undefined for a symbol, the color value is null.",
    )
    field: str | None = Field(
        None, description="The name of a numeric attribute field."
    )
    label: str | None = Field(
        None,
        description="The label used to describe the field or attribute in the legend.",
    )
    value_expression: str | None = Field(
        None,
        alias="valueExpression",
        description="An [Arcade expression](https://developers.arcgis.com/arcade/) that computes a numeric value in lieu of a value provided by an attribute `field`.",
    )
    value_expression_title: str | None = Field(
        None,
        alias="valueExpressionTitle",
        description="The title identifying and describing the associated [Arcade](https://developers.arcgis.com/arcade/) expression as defined in the `valueExpression` property.",
    )


class HeatmapRenderer(BaseModel):
    """
    The HeatmapRenderer renders point data into a raster visualization that emphasizes areas of higher density or weighted values.
    """

    model_config = common_config

    authoring_info: AuthoringInfo | None = Field(
        None,
        alias="authoringInfo",
        description="An object containing metadata about the authoring process for creating a renderer object. This allows the authoring clients to save specific settings so that next time it is accessed via the UI, their selections are remembered. Non-authoring clients can ignore it.",
    )
    color_stops: list[HeatmapColorStop] = Field(
        ...,
        alias="colorStops",
        description="An array of colorStop objects describing the renderer's color ramp with more specificity than just colors.",
    )
    field: str | None = Field(
        None,
        description="This is optional as this renderer can be created if no field is specified. Each feature gets the same value/importance/weight or with a field where each feature is weighted by the field's value.",
    )
    legend_options: LegendOptions | None = Field(
        None,
        alias="legendOptions",
        description="Options for describing the heatmap in the legend.",
    )
    max_density: float | int | None = Field(
        None,
        alias="maxDensity",
        description="The density value assigned to the final color in the `colorStops`. Only used for heatmaps calculated with kernel density. This value must be set in conjunction with `radius` and `minDensity`.",
    )
    min_density: float | int | None = Field(
        None,
        alias="minDensity",
        description="The density value assigned to the first color in the `colorStops`. Only used for heatmaps calculated with kernel density. This value must be set in conjunction with `radius` and `maxDensity`.",
    )
    radius: float | int | None = Field(
        None,
        description="The radius (in points) of the circle representing each point. Only used for heatmaps calculated with kernel density. This value must be set in conjunction with `minDensity` and `maxDensity`.",
    )
    reference_scale: Annotated[float, Field(ge=0.0)] | None = Field(
        0,
        alias="referenceScale",
        description="When defined, the heatmap will maintain a consistent, fixed rendering across all scales according to its configuration at the scale defined here. The heatmap will not dynamically update as the user zooms in and out. For example, when a referenceScale is defined, the same geographic areas appearing hot/dense will always appear hot/dense as the user zooms in and out.",
    )
    type: Literal["heatmap"] = Field(
        "heatmap", description="Specifies the type of renderer used."
    )


class UniqueValueClass(SymbolValidatorMixin):
    """
    The following is a list of properties found on the uniqueValueClass object. The uniqueValueClass object contains the symbology for grouped unique values in the renderer.
    """

    model_config = common_config

    alternate_symbols: list[CimSymbolReference] | None = Field(
        None,
        alias="alternateSymbols",
        description="An array of symbol alternatives to a primary symbol. When alternative symbols are present, each symbol has minimum scale and maximum scale at which the symbol should be made visible. For any renderer that support alternate symbols, there is a primary symbol that has minimum and maximum scale defined. When rendering these renderer classes, the renderer should pick only one symbol at a given map scale for a given renderer class. The order of picking a symbol should be starting with primary symbol to check if it falls within the map's scale first before it iterates through  alternate symbols in the array starting from 0. The renderer will then pick the first symbol that is visible at current map scale. A symbol is visible if the map scale is greater than symbol's maximum scale and less than or equal to symbol's minimum scale.",
    )
    description: str | None = Field(
        None, description="String value used to describe the drawn symbol."
    )
    label: str | None = Field(
        None, description="String value used to label the drawn symbol."
    )
    symbol: (
        CimSymbolReference
        | PictureFillSymbolEsriPFS
        | PictureMarkerSymbolEsriPMS
        | SimpleFillSymbolEsriSFS
        | SimpleLineSymbolEsriSLS
        | SimpleMarkerSymbolEsriSMS
        | TextSymbolEsriTS
        | PointSymbol3D
        | LineSymbol3D
        | PolygonSymbol3D
        | MeshSymbol3D
        | LineSymbol3D
        | MeshSymbol3D
        | PointSymbol3D
        | PolygonSymbol3D
        | StyleSymbolReference
        | None
    ) = Field(..., description="An object used to display the value.")
    values: list[list[str]] = Field(
        ...,
        description="A list of unique values that should be rendered with the same symbol. Each item in the list represents a set of value combinations represented by the given symbol. The inner array must contain only one value if only field1 is specified, two values if field1 and field2 are specified, or three values if field1, field2, and field3 are specified. The inner arrays must not contain more than three values.",
    )


class UniqueValueGroup(BaseModel):
    """
    Represents a group of unique value classes (i.e. symbols). This is used to group symbols under a common heading and/or when representing multiple unique values with a single symbol.
    """

    model_config = common_config

    classes: list[UniqueValueClass] = Field(
        ...,
        description="Specifies the classes (i.e. symbols) to group under a common heading. Classes may be included here without a heading when representing multiple values with a single symbol.",
    )
    heading: str | None = Field(
        None,
        description="The heading to be displayed for the collection of classes defined in this group.",
    )


class UniqueValueRenderer(SymbolValidatorMixin):
    """
    This renderer symbolizes features based on one or more matching string attributes.
    """

    model_config = common_config

    authoring_info: AuthoringInfo | None = Field(
        None,
        alias="authoringInfo",
        description="An object containing metadata about the authoring process for creating a renderer object. This allows the authoring clients to save specific settings so that next time it is accessed via the UI, their selections are remembered. Non-authoring clients can ignore it.",
    )
    background_fill_symbol: (
        CimSymbolReference
        | PictureFillSymbolEsriPFS
        | PictureMarkerSymbolEsriPMS
        | SimpleLineSymbolEsriSLS
        | SimpleMarkerSymbolEsriSMS
        | TextSymbolEsriTS
        | SimpleFillSymbolEsriSFS
        | PolygonSymbol3D
        | None
    ) = Field(
        None,
        alias="backgroundFillSymbol",
        description="A symbol used for polygon features as a background if the renderer uses point symbols, e.g. for bivariate types & size rendering. Only applicable to polygon layers.",
    )
    default_label: str | None = Field(
        None,
        alias="defaultLabel",
        description="Default label for the default symbol used to draw unspecified values.",
    )
    default_symbol: (
        CimSymbolReference
        | PictureFillSymbolEsriPFS
        | PictureMarkerSymbolEsriPMS
        | SimpleFillSymbolEsriSFS
        | SimpleLineSymbolEsriSLS
        | SimpleMarkerSymbolEsriSMS
        | TextSymbolEsriTS
        | PointSymbol3D
        | LineSymbol3D
        | PolygonSymbol3D
        | MeshSymbol3D
        | LineSymbol3D
        | MeshSymbol3D
        | PointSymbol3D
        | PolygonSymbol3D
        | StyleSymbolReference
        | None
    ) = Field(
        None,
        alias="defaultSymbol",
        description="Symbol used when a value cannot be matched.",
    )
    draw_in_class_order: bool | None = Field(
        None,
        alias="drawInClassOrder",
        description="Indicates whether the order of the classes in the renderer definition should be used for the feature drawing order of the layer. If `orderBy` is set in the layerDefinition, then that will take precedence over this property.",
    )
    field1: str | None = Field(
        None, description="Attribute field renderer uses to match values."
    )
    field2: str | None = Field(
        None,
        description="If needed, specify an additional attribute field the renderer uses to match values.",
    )
    field3: str | None = Field(
        None,
        description="If needed, specify an additional attribute field the renderer uses to match values.",
    )
    field_delimiter: str | None = Field(
        None,
        alias="fieldDelimiter",
        description="String inserted between the values if multiple attribute fields are specified.",
    )
    legend_options: LegendOptions | None = Field(
        None,
        alias="legendOptions",
        description="Allows the user to override the layer title with a more descriptive title of the renderer.",
    )
    rotation_expression: str | None = Field(
        None,
        alias="rotationExpression",
        description="A constant value or an expression that derives the angle of rotation based on a feature attribute value. When an attribute name is specified, it's enclosed in square brackets.  Rotation is set using a visual variable of type `rotationInfo` with a specified `field` or `valueExpression` property.",
    )
    rotation_type: RotationType | None = Field(
        None,
        alias="rotationType",
        description="String property which controls the origin and direction of rotation. If the rotationType is defined as `arithmetic` the symbol is rotated from East in a counter-clockwise direction where East is the 0 degree axis. If the rotationType is defined as `geographic`, the symbol is rotated from North in a clockwise direction where North is the 0 degree axis.",
    )
    type: Literal["uniqueValue"] = Field(
        "uniqueValue", description="Specifies the type of renderer used."
    )
    unique_value_groups: list[UniqueValueGroup] | None = Field(
        None,
        alias="uniqueValueGroups",
        description="An array of uniqueValueGroup objects. If present, then `uniqueValueGroups` should be used in favor of `uniqueValueInfos`.",
    )
    unique_value_infos: list[UniqueValueInfo] | None = Field(
        None,
        alias="uniqueValueInfos",
        description="An array of uniqueValueInfo objects. If present, then `uniqueValueGroups` should not be used.",
    )
    value_expression: str | None = Field(
        None,
        alias="valueExpression",
        description="An [Arcade expression](https://developers.arcgis.com/arcade/) evaluating to either a string or a number.",
    )
    value_expression_title: str | None = Field(
        None,
        alias="valueExpressionTitle",
        description="The title identifying and describing the associated [Arcade](https://developers.arcgis.com/arcade/) expression as defined in the `valueExpression` property.",
    )
    visual_variables: (
        list[
            ColorInfoVisualVariable
            | RotationInfoVisualVariable
            | SizeInfoVisualVariable
            | TransparencyInfoVisualVariable
        ]
        | None
    ) = Field(
        None,
        alias="visualVariables",
        description="An array of objects used to set rendering properties.",
    )


class ClassBreakInfo(SymbolValidatorMixin):
    """
    The classBreaksInfo object provides information about the class breaks associated with the renderer.
    """

    model_config = common_config

    alternate_symbols: list[CimSymbolReference] | None = Field(
        None,
        alias="alternateSymbols",
        description="An array of symbol alternatives to a primary symbol. When alternative symbols are present, each symbol has minimum scale and maximum scale at which the symbol should be made visible. For any renderer that support alternate symbols, there is a primary symbol that has minimum and maximum scale defined. When rendering these renderer classes, the renderer should pick only one symbol at a given map scale for a given renderer class. The order of picking a symbol should be starting with primary symbol to check if it falls within the map's scale first before it iterates through  alternate symbols in the array starting from 0. The renderer will then pick the first symbol that is visibile at current map scale. A symbol is visible if the map scale is greater than symbol's maximum scale and less than or equal to symbol's minimum scale.",
    )
    class_max_value: float | int | None = Field(
        None,
        alias="classMaxValue",
        description="A numeric value used to specify the maximum value for a break.",
    )
    class_min_value: float | int | None = Field(
        None,
        alias="classMinValue",
        description="A numeric value used to specify discontinuous class breaks. If this value is null or is missing, the map server will calculate the minimum value based on the preceding class' maximum value.",
    )
    description: str | None = Field(
        None, description="String value used to describe the drawn symbol."
    )
    label: str | None = Field(
        None, description="String value used to label the drawn symbol."
    )
    symbol: (
        CimSymbolReference
        | PictureFillSymbolEsriPFS
        | PictureMarkerSymbolEsriPMS
        | SimpleFillSymbolEsriSFS
        | SimpleLineSymbolEsriSLS
        | SimpleMarkerSymbolEsriSMS
        | TextSymbolEsriTS
        | PointSymbol3D
        | LineSymbol3D
        | PolygonSymbol3D
        | MeshSymbol3D
        | LineSymbol3D
        | MeshSymbol3D
        | PointSymbol3D
        | PolygonSymbol3D
        | StyleSymbolReference
        | None
    ) = Field(..., description="An object used to display the value.")


class SimpleRenderer(SymbolValidatorMixin):
    """
    A simple renderer is a renderer that uses one symbol only.
    """

    model_config = common_config

    authoring_info: AuthoringInfo | None = Field(
        None,
        alias="authoringInfo",
        description="An object containing metadata about the authoring process for creating a renderer object. This allows the authoring clients to save specific overridable settings so that next time it is accessed via the UI, their selections are remembered. Non-authoring clients can ignore it.",
    )
    description: str | None = Field(None, description="Description of the renderer.")
    label: str | None = Field(
        None, description="The text string that is displayed in the table of contents."
    )
    rotation_expression: str | None = Field(
        None,
        alias="rotationExpression",
        description="A constant value or an expression that derives the angle of rotation based on a feature attribute value. When an attribute name is specified, it's enclosed in square brackets.  Rotation is set using a visual variable of type `rotationInfo` with a specified `field` or `valueExpression` property",
    )
    rotation_type: RotationType | None = Field(
        None,
        alias="rotationType",
        description="String value which controls the origin and direction of rotation on point features. If the rotationType is defined as `arithmetic`, the symbol is rotated from East in a counter-clockwise direction where East is the 0 degree axis. If the rotationType is defined as `geographic`, the symbol is rotated from North in a clockwise direction where North is the 0 degree axis.",
    )
    symbol: (
        CimSymbolReference
        | PictureFillSymbolEsriPFS
        | PictureMarkerSymbolEsriPMS
        | SimpleFillSymbolEsriSFS
        | SimpleLineSymbolEsriSLS
        | SimpleMarkerSymbolEsriSMS
        | TextSymbolEsriTS
        | PointSymbol3D
        | LineSymbol3D
        | PolygonSymbol3D
        | MeshSymbol3D
        | LineSymbol3D
        | MeshSymbol3D
        | PointSymbol3D
        | PolygonSymbol3D
        | StyleSymbolReference
        | None
    ) = Field(
        ..., description="An object that represents how all features will be drawn."
    )
    type: Literal["simple"] = Field(
        "simple", description="Specifies the type of renderer used."
    )
    visual_variables: list[
        ColorInfoVisualVariable
        | RotationInfoVisualVariable
        | SizeInfoVisualVariable
        | TransparencyInfoVisualVariable
    ] = Field(
        None,
        alias="visualVariables",
        description="An array of objects used to set rendering properties.",
    )


class DotDensityRenderer(BaseModel):
    """
    This renderer allows you to create dot density visualizations for polygon layers. Dot density visualizations randomly draw dots within each polygon to visualize the density of a population or some other variable. Each dot represents a fixed numeric value of an attribute or a subset of attributes.
    """

    model_config = common_config

    attributes: list[AttributeColorInfo] = Field(
        ...,
        description="An array of AttributeColorInfo objects defining the dot colors.",
    )
    authoring_info: AuthoringInfo | None = Field(
        None,
        alias="authoringInfo",
        description="An object containing metadata about the authoring process for creating a renderer object. This allows the authoring clients to save specific overridable settings so that next time it is accessed via the UI, their selections are remembered. Non-authoring clients can ignore it.",
    )
    background_color: list[Annotated[int, Field(ge=0, le=255)]] | None = Field(
        None,
        alias="backgroundColor",
        description="The color used to shade the polygon fill behind the dots. Color is represented as a four-element array. The four elements represent values for red, green, blue, and alpha in that order. Values range from 0 through 255. If color is undefined for a symbol, the color value is null.",
    )
    dot_blending_enabled: bool | None = Field(
        None,
        alias="dotBlendingEnabled",
        description="Only applicable when two or more `attributes` are specified. When `true`, indicates that colors for overlapping dots will blend.",
    )
    dot_size: float | int | None = Field(
        1,
        alias="dotSize",
        description="Defines the size of each dot in points. The default is `1` in maps that don't persist `dotSize`.",
    )
    dot_value: float | int | None = Field(
        None,
        alias="dotValue",
        description="Defines the dot value used for visualizing density. For example, if set to `100`, each dot will represent 100 units. If a `referenceScale` is provided, this value indicates the value of each dot at the view.scale matching the value in `referenceScale`.",
    )
    legend_options: LegendOptions | None = Field(
        None,
        alias="legendOptions",
        description="Options for describing the renderer in the legend. This includes the title and the units describing the `dotValue`.",
    )
    outline: SimpleLineSymbolEsriSLS | None = Field(
        None, description="Sets the outline of the polygon."
    )
    reference_scale: float | int | None = Field(
        None,
        alias="referenceScale",
        description="When defined, the renderer will recalculate the dot value linearly based on the change in the view's scale. The rendering will maintain the density of points as drawn at the provided scale across various scales.",
    )
    seed: float | int | None = Field(
        "1",
        description="The reference to a specific rendering of dots. This value ensures you can view the same dot density rendering for each draw.",
    )
    type: Literal["dotDensity"] = Field(
        "dotDensity", description="Specifies the type of renderer used."
    )
    visual_variables: list[
        ColorInfoVisualVariable
        | RotationInfoVisualVariable
        | SizeInfoVisualVariable
        | TransparencyInfoVisualVariable
    ] = Field(
        None,
        alias="visualVariables",
        description="An array of sizeInfo objects used to vary the outline width based on the view.scale.",
    )


class StretchRenderer(BaseModel):
    """
    This renderer displays continuous raster cell values across a gradual ramp of colors. Use this renderer to draw a single band of continuous data. This renderer works well when you have a large range of values to display, such as with imagery or scientific data.
    """

    model_config = common_config

    color_ramp: ColorRamp | None = Field(None, alias="colorRamp")
    compute_gamma: bool | None = Field(
        None,
        alias="computeGamma",
        description="Indicates if gamma values should be computed by default.",
    )
    dra: bool | None = Field(
        None, description="Indicates if Dynamic Range Adjustment should be applied."
    )
    gamma: list[float] | list[int] | None = Field(
        None, description="The list of Gamma value(s)."
    )
    max: float | int | None = Field(
        None, description="The current maximum output value."
    )
    max_percent: float | int | None = Field(
        None, alias="maxPercent", description="The current maximum percent value."
    )
    min: float | int | None = Field(
        None, description="The current minimum output value."
    )
    min_percent: float | int | None = Field(
        None, alias="minPercent", description="The current minimum percent value."
    )
    number_of_standard_deviations: float | int | None = Field(
        None,
        alias="numberOfStandardDeviations",
        description="The number of standard deviations for standard deviation stretch.",
    )
    sigmoid_strength_level: float | int | None = Field(
        None,
        alias="sigmoidStrengthLevel",
        description="Set this from (1 to 6) to adjust the curvature of Sigmoid curve used in color stretch.",
    )
    statistics: list[list[float]] | None = Field(
        None, description="The custom raster stretch statistics."
    )
    stretch_type: StretchType | None = Field(
        None,
        alias="stretchType",
        description="The stretch types for stretch raster function.",
    )
    type: Literal["rasterStretch"] | None = Field(
        None, description="Specifies the type of renderer used."
    )
    use_gamma: bool | None = Field(
        None,
        alias="useGamma",
        description="Indicates if the renderer applies Gamma stretch.",
    )

    @field_validator("statistics", mode="before")
    def validate_statistics(cls, v):
        # If None is in any of the list values, change to 0
        return [[x or 0 for x in lst] for lst in v]


class FlowRenderer(BaseModel):
    """
    A flow renderer is a renderer that uses animated streamlines to visualize U-V or Magnitude-Direction raster data. This renderer works with ImageryLayers and ImageryTileLayers.
    """

    model_config = common_config

    authoring_info: AuthoringInfo | None = Field(
        None,
        alias="authoringInfo",
        description="An object containing metadata about the authoring process for creating a renderer object. This allows the authoring clients to save specific overridable settings so that next time it is accessed via the UI, their selections are remembered. Non-authoring clients can ignore it.",
    )
    color: list[Annotated[int, Field(ge=0, le=255)]] | None = Field(
        [255, 255, 255, 255], description="The color of the streamlines."
    )
    density: float | int | None = Field(
        0.8,
        description="The density of the streamlines. Accepted values are between 0 and 1, where 0 is the least dense and 1 is the most dense.",
    )
    flow_representation: FlowRepresentation | None = Field(
        "flow-from",
        alias="flowRepresentation",
        description="Sets the flow direction of the data.",
    )
    flow_speed: float | int | None = Field(
        10,
        alias="flowSpeed",
        description="The speed of the animated streamlines, relative to simulation time. This serves as a multiple of the magnitude from the imagery layer. If the magnitude is 2 m/s, and flowSpeed is 10, then the actual speed of a streamline will be 20 pts/s. A speed of 0 will result in no animation.",
    )
    legend_options: LegendOptions | None = Field(
        None,
        alias="legendOptions",
        description="A legend containing one title, which is a string describing the renderer in the legend.",
    )
    max_path_length: float | int | None = Field(
        200,
        alias="maxPathLength",
        description="The maximum length the streamlines will travel in points.",
    )
    trail_cap: TrailCap | None = Field(
        TrailCap.butt.value,
        alias="trailCap",
        description="The style of the streamline's cap. The 'round' cap will only be applied if trailWidth is greater than 3pts.",
    )
    trail_length: float | int | None = Field(
        100,
        alias="trailLength",
        description="The approximate visible length of the streamline in points. This will be longer where the particle is moving faster, and shorter where the particle is moving slower.",
    )
    trail_width: float | int | None = Field(
        1.5, alias="trailWidth", description="The width of the streamlines in points."
    )
    type: Literal["flowRenderer"] = Field(
        "flowRenderer", description="Specifies the type of renderer used."
    )
    visual_variables: list[
        ColorInfoVisualVariable
        | RotationInfoVisualVariable
        | SizeInfoVisualVariable
        | TransparencyInfoVisualVariable
    ] = Field(
        None,
        alias="visualVariables",
        description="An array of objects used to set rendering properties. Supports color, size, and opacity visual variables.",
    )


class PredominanceRenderer(SymbolValidatorMixin):
    """
    This renderer is a type of UniqueValue renderer which is based off the `valueExpression` property rather than `field`. Optionally, `size` and/or `transparency` visual variables may be included with `valueExpression`. Note that this renderer is supported for ArcGIS Online hosted feature services and feature collections.
    """

    model_config = common_config

    authoring_info: AuthoringInfo | None = Field(
        None,
        alias="authoringInfo",
        description="An object containing metadata about the authoring process for creating a renderer object. This allows the authoring clients to save specific overridable settings so that next time it is accessed via the UI, their selections are remembered. Non-authoring clients can ignore it.",
    )
    background_fill_symbol: SimpleFillSymbolEsriSFS | None = Field(
        None,
        alias="backgroundFillSymbol",
        description="A symbol used for polygon features as a background if the renderer uses point symbols, e.g. for bivariate types & size rendering. Only applicable to polygon layers. PictureFillSymbolEsriPFS can also be used outside of the Map Viewer for Size and Predominance and Size renderers.",
    )
    default_label: str | None = Field(
        None,
        alias="defaultLabel",
        description="Default label for the default symbol used to draw unspecified values.",
    )
    default_symbol: (
        CimSymbolReference
        | PictureFillSymbolEsriPFS
        | PictureMarkerSymbolEsriPMS
        | SimpleFillSymbolEsriSFS
        | SimpleLineSymbolEsriSLS
        | SimpleMarkerSymbolEsriSMS
        | TextSymbolEsriTS
        | PointSymbol3D
        | LineSymbol3D
        | PolygonSymbol3D
        | MeshSymbol3D
        | LineSymbol3D
        | MeshSymbol3D
        | PointSymbol3D
        | PolygonSymbol3D
        | StyleSymbolReference
        | None
    ) = Field(
        None,
        alias="defaultSymbol",
        description="Symbol used when a value cannot be classified.",
    )
    rotation_expression: str | None = Field(
        None,
        alias="rotationExpression",
        description="A constant value or an expression that derives the angle of rotation based on a feature attribute value. When an attribute name is specified, it's enclosed in square brackets.  Rotation is set using a visual variable of type `rotationInfo` with a specified `field` or `valueExpression` property",
    )
    rotation_type: RotationType | None = Field(
        None,
        alias="rotationType",
        description="String value which controls the origin and direction of rotation on point features. If the rotationType is defined as `arithmetic`, the symbol is rotated from East in a counter-clockwise direction where East is the 0 degree axis. If the rotationType is defined as `geographic`, the symbol is rotated from North in a clockwise direction where North is the 0 degree axis.",
    )
    type: Literal["uniqueValue"] = Field(
        "uniqueValue", description="Specifies the type of renderer used."
    )
    unique_value_infos: list[UniqueValueInfo] = Field(
        ...,
        alias="uniqueValueInfos",
        description="An array of uniqueValueInfo objects.",
    )
    value_expression: str | None = Field(
        None,
        alias="valueExpression",
        description="An [Arcade expression](https://developers.arcgis.com/arcade/) evaluating to either a string or a number.",
    )
    value_expression_title: str | None = Field(
        None,
        alias="valueExpressionTitle",
        description="The title identifying and describing the associated [Arcade](https://developers.arcgis.com/arcade/) expression as defined in the `valueExpression` property.",
    )
    visual_variables: list[
        ColorInfoVisualVariable
        | RotationInfoVisualVariable
        | SizeInfoVisualVariable
        | TransparencyInfoVisualVariable
    ] = Field(
        None,
        alias="visualVariables",
        description="An array of objects used to set rendering properties.",
    )


class ClassBreaksRenderer(SymbolValidatorMixin):
    """
    A class breaks renderer symbolizes based on the value of some numeric attribute. The classBreakInfo define the values at which the symbology changes.
    """

    model_config = common_config

    authoring_info: AuthoringInfo | None = Field(
        None,
        alias="authoringInfo",
        description="An object containing metadata about the authoring process for creating a renderer object. This allows the authoring clients to save specific overridable settings so that next time it is accessed via the UI, their selections are remembered. Non-authoring clients can ignore it.",
    )
    background_fill_symbol: (
        CimSymbolReference
        | PictureFillSymbolEsriPFS
        | PictureMarkerSymbolEsriPMS
        | SimpleLineSymbolEsriSLS
        | SimpleMarkerSymbolEsriSMS
        | TextSymbolEsriTS
        | SimpleFillSymbolEsriSFS
        | None
    ) = Field(
        None,
        alias="backgroundFillSymbol",
        description="A symbol used for polygon features as a background if the renderer uses point symbols, e.g. for bivariate types & size rendering. Only applicable to polygon layers.",
    )
    class_break_infos: list[ClassBreakInfo] = Field(
        ..., alias="classBreakInfos", description="Array of classBreakInfo objects."
    )
    classification_method: ClassificationMethod | None = Field(
        None,
        alias="classificationMethod",
        description="Determines the classification method that was used to generate class breaks. This has been replaced by AuthoringInfo.",
    )
    default_label: str | None = Field(
        None,
        alias="defaultLabel",
        description="Label for the default symbol used to draw unspecified values.",
    )
    default_symbol: (
        CimSymbolReference
        | PictureFillSymbolEsriPFS
        | PictureMarkerSymbolEsriPMS
        | SimpleFillSymbolEsriSFS
        | SimpleLineSymbolEsriSLS
        | SimpleMarkerSymbolEsriSMS
        | TextSymbolEsriTS
        | PointSymbol3D
        | LineSymbol3D
        | PolygonSymbol3D
        | MeshSymbol3D
        | LineSymbol3D
        | MeshSymbol3D
        | PointSymbol3D
        | PolygonSymbol3D
        | StyleSymbolReference
        | None
    ) = Field(
        None,
        alias="defaultSymbol",
        description="Symbol used when a value cannot be classified.",
    )
    field: str | None = Field(None, description="Attribute field used for renderer.")
    legend_options: LegendOptions | None = Field(
        None,
        alias="legendOptions",
        description="A legend containing one title, which is a string describing the renderer in the legend.",
    )
    min_value: float | int | None = Field(
        None,
        alias="minValue",
        description="The minimum numeric data value needed to begin class breaks.",
    )
    normalization_field: str | None = Field(
        None,
        alias="normalizationField",
        description="Used when normalizationType is field. The string value indicating the attribute field by which the data value is normalized.",
    )
    normalization_total: float | int | None = Field(
        None,
        alias="normalizationTotal",
        description="Used when normalizationType is percent-of-total, this number property contains the total of all data values.",
    )
    normalization_type: NormalizationType | None = Field(
        None,
        alias="normalizationType",
        description="Determine how the data was normalized.",
    )
    rotation_expression: str | None = Field(
        None,
        alias="rotationExpression",
        description="A constant value or an expression that derives the angle of rotation based on a feature attribute value. When an attribute name is specified, it's enclosed in square brackets.  Rotation is set using a visual variable of type `rotationInfo` with a specified `field` or `valueExpression` property.",
    )
    rotation_type: RotationType | None = Field(
        None,
        alias="rotationType",
        description="A string property which controls the origin and direction of rotation. If the rotationType is defined as `arithmetic`, the symbol is rotated from East in a couter-clockwise direction where East is the 0 degree axis. If the rotationType is defined as `geographic`, the symbol is rotated from North in a clockwise direction where North is the 0 degree axis.",
    )
    type: Literal["classBreaks"] = Field(
        "classBreaks", description="Specifies the type of renderer used."
    )
    value_expression: str | None = Field(
        None,
        alias="valueExpression",
        description="An [Arcade expression](https://developers.arcgis.com/arcade/) evaluating to a number.",
    )
    value_expression_title: str | None = Field(
        None,
        alias="valueExpressionTitle",
        description="The title identifying and describing the associated [Arcade](https://developers.arcgis.com/arcade/) expression as defined in the `valueExpression` property.",
    )
    visual_variables: (
        list[
            ColorInfoVisualVariable
            | RotationInfoVisualVariable
            | SizeInfoVisualVariable
            | TransparencyInfoVisualVariable
        ]
        | None
    ) = Field(
        None,
        alias="visualVariables",
        description="An array of objects used to set rendering properties.",
    )


class ExpressionInfo(BaseModel):
    """
    Defines a script expression that can be used to compute values. Depending on the context, the script may refer to external data which will be available when the expression is being evaluated.
    """

    model_config = common_config

    expression: str | None = Field(
        None,
        description="Optional expression in the [Arcade expression](https://developers.arcgis.com/arcade/) language. If no expression is provided, then the default empty expression produces a null, empty string, zero or false when evaluated (depending on usage and context).",
    )
    return_type: ArcadeReturnType | None = Field(
        ArcadeReturnType.string.value,
        alias="returnType",
        description="Optional return type of the Arcade expression. Defaults to string value. Number values are assumed to be `double`. This can be determined by the authoring client by executing the expression using a sample feature, although it can be corrected by the user. Knowing the returnType allows the authoring client to present fields in relevant contexts. For example, numeric fields in numeric contexts such as charts.",
    )
    title: str | None = Field(
        None,
        description="Optional title of the expression. Typically used when presenting the expression to end-users e.g. in dialogs, table-of-contents or editing tools.",
    )


class DictionaryRenderer(BaseModel):
    """
    A renderer where symbols are drawn from a dictionary style.
    """

    model_config = common_config

    configuration: (
        dict[Annotated[str, StringConstraints(pattern=r".*")], str] | None
    ) = Field(
        None,
        description="An object representing the configuration properties for a symbol.",
    )
    dictionary_name: str | None = Field(
        None, alias="dictionaryName", description="The name of the symbol dictionary."
    )
    field_map: dict[Annotated[str, StringConstraints(pattern=r".*")], str] | None = (
        Field(
            None,
            alias="fieldMap",
            description="An object with key/ value pairs representing expected field name and actual field name.",
        )
    )
    scaling_expression_info: ExpressionInfo | None = Field(
        None,
        alias="scalingExpressionInfo",
        description="Optional expression script object specifying the scaling ratio as a number. A return value of 1 means no scaling, a return value of 2 means scale 2 times etc. Absence of this object also results in no scaling. Expected return type from the Arcade expression is number",
    )
    type: Literal["dictionary"] = Field(
        "dictionary", description="Specifies the type of renderer used."
    )
    url: str = Field(..., description="The URL to dictionary web style.")


class OthersThresholdColorInfo(BaseModel):
    """
    Defines the rules for how to aggregate small categories to a generic "others" category for categorical chart renderers, such as pie charts.
    """

    model_config = common_config

    color: list[Annotated[int, Field(ge=0, le=255)]] | None = Field(
        None,
        description='Defines the color used to represent all categories smaller than the percentage defined by `threshold`. This is typically used to represent a generic "others" category where categories would otherwise be too small to read.',
    )
    label: str | None = Field(
        None,
        description='The label used to describe the "others" category in the legend. When not specified, the legend will display a localized version of "Others".',
    )
    threshold: Annotated[float, Field(ge=0.0, le=1.0)] | None = Field(
        0,
        description='Represents the minimum size of individual categories as a percentage of all categories combined. All categories that make up a smaller percentage than the threshold will automatically be aggregated to an "others" category represented by the color specified in `color`. For example, if the threshold is 0.05, then all categories that make up less than 5% of all categories will be represented with `color`.',
    )


class PieChartRenderer(BaseModel):
    """
    This renderer allows you to create pie charts to compare numeric values between categories within the same group.
    """

    model_config = common_config

    attributes: list[AttributeColorInfo] = Field(
        ...,
        description="An array of AttributeColorInfo objects defining the color of each slice.",
    )
    authoring_info: AuthoringInfo | None = Field(
        None,
        alias="authoringInfo",
        description="An object containing metadata about the authoring process for creating a renderer object. This allows the authoring clients to save specific overridable settings so that next time it is accessed via the UI, their selections are remembered. Non-authoring clients can ignore it.",
    )
    background_fill_symbol: (
        CimSymbolReference
        | PictureFillSymbolEsriPFS
        | PictureMarkerSymbolEsriPMS
        | SimpleLineSymbolEsriSLS
        | SimpleMarkerSymbolEsriSMS
        | TextSymbolEsriTS
        | SimpleFillSymbolEsriSFS
        | None
    ) = Field(
        None,
        alias="backgroundFillSymbol",
        description="The symbols used to represent the polygon fill behind the pie charts.",
    )
    default_color: list[Annotated[int, Field(ge=0, le=255)]] | None = Field(
        None,
        alias="defaultColor",
        description="Defines the color used to represent features where all values of attributes are either null or zero. This is typically used to represent areas with 'no data' or 'no values' in the legend.",
    )
    default_label: str | None = Field(
        None,
        alias="defaultLabel",
        description="The text used to describe the `defaultColor` in the legend. Typically, this label will be something similar to 'No data' or 'No values'.",
    )
    hole_percentage: Annotated[float, Field(ge=0.0, le=1.0)] | None = Field(
        0,
        alias="holePercentage",
        description="Defines the size of the hole to cut from the center of the chart as a percentage of the size of the chart. For example, a value of `0` will render a full pie chart. A value of `0.5` will remove 50% of the center of the pie. This property is used to create a donut chart.",
    )
    legend_options: LegendOptions | None = Field(
        None,
        alias="legendOptions",
        description="Options for describing the renderer in the legend.",
    )
    others_category: OthersThresholdColorInfo | None = Field(
        None,
        alias="othersCategory",
        description='Defines how to aggregate small pie slices to a generic "others" category.',
    )
    outline: SimpleLineSymbolEsriSLS | None = Field(
        None,
        description="Sets the outline of the pie chart. The outline width is applied to the outer outline of the pie (and inner outline in the case of donut charts). The outline color is applied to the outer and inner outlines, and the boundaries of the slices.",
    )
    size: float | int | None = Field(
        12, description="The diameter of the pie chart in points."
    )
    type: Literal["pieChart"] = Field(
        "pieChart", description="Specifies the type of renderer used."
    )
    visual_variables: list[SizeInfoVisualVariable] | None = Field(
        None,
        alias="visualVariables",
        description="An array of sizeInfo objects used to vary the size of the pie charts.",
    )


class VectorFieldRenderer(BaseModel):
    """
    A vector field renderer is a renderer that uses symbolizes a U-V or Magnitude-Direction data.
    """

    model_config = common_config

    attribute_field: str | None = Field(
        None,
        alias="attributeField",
        description="Name of the feature attribute field that contains the data value.",
    )
    flow_representation: FlowRepresentation | None = Field(
        "flow-from",
        alias="flowRepresentation",
        description="Sets the flow direction of the data.",
    )
    input_unit: InputOutputUnit | None = Field(
        None, alias="inputUnit", description="Input unit for Magnitude."
    )
    output_unit: InputOutputUnit | None = Field(
        None, alias="outputUnit", description="Output unit for Magnitude."
    )
    rotation_type: RotationType | None = Field(
        None,
        alias="rotationType",
        description="String value which controls the origin and direction of rotation on point features. If the rotationType is defined as `arithmetic`, the symbol is rotated from East in a counter-clockwise direction where East is the 0 degree axis. If the rotationType is defined as `geographic`, the symbol is rotated from North in a clockwise direction where North is the 0 degree axis.",
    )
    style: VectorFieldStyle | None = Field(None, description="A predefined style.")
    symbol_tile_size: float | int | None = Field(
        50,
        alias="symbolTileSize",
        description="Determines the density of the symbols. Larger tile size, fewer symbols appear in the display. The VectorFieldRenderer draws one symbol within a defined tile size (in pixels). The default is 50 pixels.",
    )
    type: Literal["vectorField"] = Field(
        "vectorField", description="Specifies the type of renderer used."
    )
    visual_variables: (
        list[
            ColorInfoVisualVariable
            | RotationInfoVisualVariable
            | SizeInfoVisualVariable
            | TransparencyInfoVisualVariable
        ]
        | None
    ) = Field(
        None,
        alias="visualVariables",
        description="An array of objects used to set rendering properties.",
    )


class TemporalRenderer(BaseModel):
    """
    Temporal renderers provide time-based rendering of features in a feature layer. It can be useful to visualize historic or real-time data such as earthquake or hurricane occurrences. You can use a temporal renderer to define how observations (regular, historic, latest) and tracks are rendered. You can also show aging of features with respect to the map's time extent.
    """

    model_config = common_config

    latest_observation_renderer: SimpleRenderer | None = Field(
        None,
        alias="latestObservationRenderer",
        description="Simple renderer used to symbolize point geometries for the most current observations.",
    )
    observational_renderer: SimpleRenderer | None = Field(
        None,
        alias="observationalRenderer",
        description="Simple renderer used to symbolize regular/historic observations.",
    )
    track_renderer: SimpleRenderer | None = Field(
        None,
        alias="trackRenderer",
        description="Simple renderer used to symbolize the tracks.",
    )
    type: Literal["temporal"] | None = Field(
        None, description="Specifies the type of renderer used."
    )


class ColormapInfo(BaseModel):
    """
    The color map information used when [rendering](rasterColormapRenderer.md) raster data.
    """

    model_config = common_config
    color: list[conint(ge=0, le=255)] | None = Field(
        None, description="The pixel color.", title="color"
    )
    label: str | None = Field(None, description="The label used for the pixel value.")
    value: float | None = Field(None, description="The raster pixel value.")


class RasterColorMapRenderer(BaseModel):
    """
    A value/color look up table used for rendering raster data only. This renderer is similar to [unique value rendering](uniqueValueRenderer.md) in regards to its functionality. The rendering is automatically applied when a raster source has colormap information.
    """

    model_config = common_config
    colormap_infos: list[ColormapInfo] = Field(
        ...,
        alias="colormapInfos",
        description="An array of colormapInfo objects.",
    )
    type: Literal["rasterColormap"] = Field(
        "rasterColormap", description="Specifies the type of renderer used."
    )


class RasterShadedReliefRenderer(BaseModel):
    """
    This renderer produces a grayscale or colored 3D representations. This renderer can be used on either layers of type: [ImageServiceLayer](imageServiceLayer.md) or [TiledImageServiceLayer](tiledImageServiceLayer.md)
    """

    model_config = common_config
    altitude: float = Field(
        ...,
        description="Used when the `hillshadeType` is `traditional`. The  altitude of the illumination source.",
    )
    azimuth: float = Field(
        ...,
        description="Used when the `hillshadeType` is `traditional`. The azimuth of the illumination source.",
    )
    color_ramp: ColorRamp | None = Field(
        None,
        alias="colorRamp",
        description="The color ramp to display the shaded relief. when not specified, a grayscale hillshade is produced. When this is specified, grayscale hillshade is fused with colorized elevation.",
    )
    hillshade_type: HillshadeType = Field(
        ...,
        alias="hillshadeType",
        description="Use single (traditional), or multiple illumination sources to generate hillshade",
    )
    pixel_size_factor: float | None = Field(
        None,
        alias="pixelSizeFactor",
        description="Accounts for changes in scale as the viewer zooms in and out on the map display",
    )
    pixel_size_power: float | None = Field(
        None,
        alias="pixelSizePower",
        description="Accounts for the altitude changes (or scale) as the viewer zooms in and out on the map display",
    )
    scaling_type: ScalingType = Field(
        ...,
        alias="scalingType",
        description="Apply a constant or adjusted z-factor based on resolution changes. The `adjusted` type is suitable for worldwide elevation dataset. An adjusted `zFactor` is determined using this equation: `Adjusted Z-Factor = (Z Factor) + (Pixel Size)` <sup>(Pixel Size Power)</sup> `x Pixel Size Factor`",
    )
    type: Literal["rasterShadedRelief"] = Field(
        "rasterShadedRelief",
        description="Specifies the type of renderer used.",
    )
    z_factor: float = Field(
        ...,
        alias="zFactor",
        description="A conversion factor that adjusts the units of measure for the vertical (or elevation) units when they are different from the horizontal coordinate (x,y) units of the input surface. It is the number of ground x,y units in one surface z-unit.",
    )


class ColorClassBreakInfo(BaseModel):
    """
    The classBreaksInfo object provides information about the class breaks associated with the renderer.
    """

    model_config = common_config
    class_max_value: float = Field(
        ...,
        alias="classMaxValue",
        description="A numeric value used to specify the maximum value for a break.",
    )
    class_min_value: float = Field(
        ...,
        alias="classMinValue",
        description="A numeric value used to specify discontinuous class breaks. If this value is null or is missing, the map server will calculate the minimum value based on the preceding class' maximum value.",
    )
    color: list[conint(ge=0, le=255)] = Field(
        ...,
        description="Color is represented as a three or four-element array. The four elements represent values for red, green, blue, and alpha in that order. Values range from 0 through 255. If color is undefined for a symbol, the color value is null.",
        title="color",
    )
    description: str | None = Field(
        None, description="String value used to describe the class."
    )
    label: str | None = Field(None, description="String value used to label the class.")


class ColorModulationInfo(BaseModel):
    """
    Indicates whether modulation should be used to render the point.
    """

    model_config = common_config
    field: str = Field(
        ...,
        description="the attribute to use as a source for the modulation amplitude",
    )
    max_value: float = Field(
        ...,
        alias="maxValue",
        description="maximum value to compute modulation linear mapping",
    )
    min_value: float = Field(
        ...,
        alias="minValue",
        description="minimum value to compute modulation linear mapping",
    )


class RendererLegendOptions(BaseModel):
    """
    Options available for the renderer legend.
    """

    model_config = common_config
    title: str | None = Field(None, description="The title of the legend.")


class PointCloudFixedSizeAlgorithm(BaseModel):
    """
    Render points with fixed real world or screen space size.
    """

    model_config = common_config
    size: float = Field(
        ..., description="Symbol size in real world units or display unit"
    )
    type: Literal["pointCloudFixedSizeAlgorithm"] = "pointCloudFixedSizeAlgorithm"
    use_real_world_symbol_sizes: bool | None = Field(
        None,
        alias="useRealWorldSymbolSizes",
        description=" If true symbol size is in meters, display unit (pt) otherwise",
    )


class PointCloudSplatAlgorithm(BaseModel):
    """
    Render points using sizes depending on point density.
    """

    model_config = common_config
    scale_factor: float = Field(
        ...,
        alias="scaleFactor",
        description="Scale factor to applied to the computed point size (in real world)",
    )
    type: Literal["pointCloudSplatAlgorithm"] = "pointCloudSplatAlgorithm"


class PointCloudClassBreaksRenderer(BaseModel):
    """
    PointCloudClassBreaksRenderer defines the color of each point in a PointCloudLayer based on the value of a numeric attribute. Colors are assigned based on classes or ranges of data. Each point is assigned a symbol based on the class break in which the value of the attribute falls.
    """

    model_config = common_config
    color_class_break_infos: list[ColorClassBreakInfo] = Field(
        ...,
        alias="colorClassBreakInfos",
        description="Each element in the array is an object that provides information about a class break associated with the renderer.",
    )
    color_modulation: ColorModulationInfo | None = Field(None, alias="colorModulation")
    field: str = Field(
        ...,
        description="  The name of the field that is used for the renderer.",
    )
    field_transform_type: FieldTransformType | None = Field(
        None,
        alias="fieldTransformType",
        description="A transform that is applied to the field value before evaluating the renderer.",
    )
    legend_options: RendererLegendOptions | None = Field(
        None,
        alias="legendOptions",
        description="A legend containing one title, which is a string describing the renderer in the legend.",
    )
    point_size_algorithm: (
        PointCloudFixedSizeAlgorithm | PointCloudSplatAlgorithm | None
    ) = Field(
        None,
        alias="pointSizeAlgorithm",
        description="Size algorithms for point cloud performance.",
        title="PointCloud Size Algorithm",
    )
    points_per_inch: float | None = Field(
        None,
        alias="pointsPerInch",
        description="Number of point to draw per display inch.",
    )
    type: Literal["pointCloudClassBreaksRenderer"] = Field(
        "pointCloudClassBreaksRenderer",
        description="Specifies the type of renderer used.",
    )


class PointCloudRGBRenderer(BaseModel):
    """
    PointCloudRGBRenderer defines the color of each point in a PointCloudLayer based on the value of a color attribute.
    """

    model_config = common_config
    color_modulation: ColorModulationInfo | None = Field(None, alias="colorModulation")
    field: str = Field(
        ...,
        description="The name of the field that is used for the renderer.",
    )
    point_size_algorithm: (
        PointCloudFixedSizeAlgorithm | PointCloudSplatAlgorithm | None
    ) = Field(
        None,
        alias="pointSizeAlgorithm",
        description="Size algorithms for point cloud performance.",
        title="PointCloud Size Algorithm",
    )
    points_per_inch: float | None = Field(
        None,
        alias="pointsPerInch",
        description="Number of point to draw per display inch.",
    )
    type: Literal["pointCloudRGBRenderer"] = Field(
        "pointCloudRGBRenderer",
        description="Specifies the type of renderer used.",
    )


class PointCloudStretchRenderer(BaseModel):
    """
    PointCloudStretchRenderer defines the color of each point in a PointCloudLayer based on the value of a numeric attribute. They allow you to easily map continuous color ramps to minimum and maximum data values of one of the layer's numeric attribute fields.
    """

    model_config = common_config
    color_modulation: ColorModulationInfo | None = Field(None, alias="colorModulation")
    field: str = Field(
        ...,
        description="The name of the field that is used for the renderer.",
    )
    field_transform_type: FieldTransformType | None = Field(
        None,
        alias="fieldTransformType",
        description="A transform that is applied to the field value before evaluating the renderer.",
    )
    legend_options: RendererLegendOptions | None = Field(
        None,
        alias="legendOptions",
        description="A legend containing one title, which is a string describing the renderer in the legend.",
    )
    point_size_algorithm: (
        PointCloudFixedSizeAlgorithm | PointCloudSplatAlgorithm | None
    ) = Field(
        None,
        alias="pointSizeAlgorithm",
        description="Size algorithms for point cloud performance.",
        title="PointCloud Size Algorithm",
    )
    points_per_inch: float | None = Field(
        None,
        alias="pointsPerInch",
        description="Number of points to draw per display inch.",
    )
    stops: list[ColorStop] = Field(..., description="An array of color value pairs.")
    type: Literal["pointCloudStretchRenderer"] = Field(
        "pointCloudStretchRenderer",
        description="Specifies the type of renderer used.",
    )


class ColorUniqueValueInfo(BaseModel):
    """
    The colorUniqueValueInfo object matches a unique value with a specific color.
    """

    model_config = common_config
    color: list[conint(ge=0, le=255)] = Field(
        ...,
        description="Color is represented as a three or four-element array. The four elements represent values for red, green, blue, and alpha in that order. Values range from 0 through 255. If color is undefined for a symbol, the color value is null.",
        title="color",
    )
    description: str | None = Field(
        None, description="String value used to describe the class."
    )
    label: str | None = Field(None, description="String value used to label the class.")
    values: list[str] = Field(
        ..., description="List of string values used to classify points."
    )


class PointCloudUniqueValueRenderer(BaseModel):
    """
    PointCloudUniqueValueRenderer allows you to colorize points in a PointCloudLayer based on an attribute. This is done by using unique colors to represent points with equal attribute values.
    """

    model_config = common_config
    color_modulation: ColorModulationInfo | None = Field(None, alias="colorModulation")
    color_unique_value_infos: list[ColorUniqueValueInfo] = Field(
        ..., alias="colorUniqueValueInfos", description="Unique value infos."
    )
    field: str = Field(
        ...,
        description="The name of the field that is used for the renderer.",
    )
    field_transform_type: FieldTransformType | None = Field(
        None,
        alias="fieldTransformType",
        description="A transform that is applied to the field value before evaluating the renderer.",
    )
    legend_options: RendererLegendOptions | None = Field(
        None,
        alias="legendOptions",
        description="A legend containing one title, which is a string describing the renderer in the legend.",
    )
    point_size_algorithm: (
        PointCloudFixedSizeAlgorithm | PointCloudSplatAlgorithm | None
    ) = Field(
        None,
        alias="pointSizeAlgorithm",
        description="Size algorithms for point cloud performance.",
        title="PointCloud Size Algorithm",
    )
    points_per_inch: float | None = Field(
        None,
        alias="pointsPerInch",
        description="Number of points to draw per display inch.",
    )
    type: Literal["pointCloudUniqueValueRenderer"] = Field(
        "pointCloudUniqueValueRenderer",
        description="Specifies the type of renderer used.",
    )


class RasterPresetRenderer(BaseModel):
    """
    Defines a raster renderer with associated associated information of multidimensional variable or raster function template.
    """

    model_config = common_config
    band_ids: list[int] = Field(
        ...,
        alias="bandIds",
        description="The band IDs to be used for rendering.",
    )
    method: Literal["none", "rasterFunctionTemplate", "variable"] = Field(
        ...,
        description="The method used to render the raster.",
    )
    name: str = Field(
        ...,
        description="The unique name of the renderer.",
    )
    renderer: (
        ClassBreaksRenderer
        | FlowRenderer
        | StretchRenderer
        | UniqueValueRenderer
        | VectorFieldRenderer
        | RasterColorMapRenderer
        | RasterPresetRenderer
        | RasterShadedReliefRenderer
    ) = Field(
        ...,
        description="The renderer used to render the raster.",
    )
    value: str | None = Field(
        ...,
        description="The corresponding variable or raster function template name.",
    )


ColorRamp.model_rebuild()
