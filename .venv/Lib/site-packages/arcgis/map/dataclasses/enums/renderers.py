from enum import Enum


class RampAlgorithm(Enum):
    """
    Algorithm used for calculating the ramp.
    """

    esri_cie_lab_algorithm = "esriCIELabAlgorithm"
    esri_hsv_algorithm = "esriHSVAlgorithm"
    esri_lab_l_ch_algorithm = "esriLabLChAlgorithm"


class ColorRampType(Enum):
    """
    Value indicating the type of colorRamp.
    """

    algorithmic = "algorithmic"
    multipart = "multipart"


class HillshadeType(Enum):
    """
    Use single (traditional), or multiple illumination sources to generate hillshade
    """

    multi_directional = "multi-directional"
    traditional = "traditional"


class ScalingType(Enum):
    """
    Apply a constant or adjusted z-factor based on resolution changes. The `adjusted` type is suitable for worldwide elevation dataset. An adjusted `zFactor` is determined using this equation: `Adjusted Z-Factor = (Z Factor) + (Pixel Size)` <sup>(Pixel Size Power)</sup> `x Pixel Size Factor`
    """

    adjusted = "adjusted"
    none = "none"


class RatioStyle(Enum):
    """
    It is used to map the ratio between two numbers. It is possible to express that relationship as percentages, simple ratios, or an overall percentage.
    """

    percent = "percent"
    percent_total = "percentTotal"
    ratio = "ratio"


class ValueRepresentation(Enum):
    """
    Specifies how to apply the data value when mapping real-world sizes. See table below for supported values.
    """

    area = "area"
    diameter = "diameter"
    distance = "distance"
    radius = "radius"
    width = "width"


class FieldTransformType(Enum):
    """
    A transform that is applied to the field value before evaluating the renderer.
    """

    absolute_value = "absoluteValue"
    high_four_bit = "highFourBit"
    low_four_bit = "lowFourBit"
    modulo_ten = "moduloTen"
    none = "none"


class Axis(Enum):
    """
    Defines the axis the size visual variable should be applied to when rendering features with an ObjectSymbol3DLayer.
    """

    all = "all"
    depth = "depth"
    height = "height"
    width = "width"
    width_and_depth = "widthAndDepth"
    heading = "heading"
    roll = "roll"
    tilt = "tilt"


class LengthUnit(Enum):
    """
    Unit used in user interfaces to display world/map sizes and distances
    """

    centimeters = "centimeters"
    decimeters = "decimeters"
    feet = "feet"
    inches = "inches"
    kilometers = "kilometers"
    meters = "meters"
    miles = "miles"
    millimeters = "millimeters"
    nautical_miles = "nautical-miles"
    yards = "yards"


class Theme(Enum):
    """
    Theme to be used only when working with visual variables of type `colorInfo` or `sizeInfo`. Default is `high-to-low`. The `centered-on`, and `extremes` themes only apply to `colorInfo` visual variables.
    """

    above = "above"
    above_and_below = "above-and-below"
    below = "below"
    centered_on = "centered-on"
    extremes = "extremes"
    high_to_low = "high-to-low"
    spike = "spike"


class VisualVariableType(Enum):
    """
    A string value specifying the type of renderer's visual variable.
    """

    color_info = "colorInfo"
    rotation_info = "rotationInfo"
    size_info = "sizeInfo"
    transparency_info = "transparencyInfo"


class TimeUnits(Enum):
    """
    Units for `startTime` and `endTime`.
    """

    days = "days"
    hours = "hours"
    minutes = "minutes"
    months = "months"
    seconds = "seconds"
    years = "years"


class SpikeSymbolStyle(Enum):
    triangle_closed_outline = "triangle-closed-outline"
    triangle_gradient_fill_closed = "triangle-gradient-fill-closed"
    triangle_gradient_fill_closed_outline = "triangle-gradient-fill-closed-outline"
    triangle_gradient_fill_open = "triangle-gradient-fill-open"
    triangle_gradient_fill_open_outline = "triangle-gradient-fill-open-outline"
    triangle_open_outline = "triangle-open-outline"
    triangle_solid_fill_closed = "triangle-solid-fill-closed"
    triangle_solid_fill_closed_outline = "triangle-solid-fill-closed-outline"
    triangle_solid_fill_open = "triangle-solid-fill-open"
    triangle_solid_fill_open_outline = "triangle-solid-fill-open-outline"


class UnivariateSymbolStyle(Enum):
    """
    Symbol style or symbol pair used when creating a renderer of type `univariateColorSize` with an `above-and-below` univariateTheme. The `custom` style indicates the renderer uses a custom symbol pair not provided by the authoring application.
    """

    arrow = "arrow"
    caret = "caret"
    circle = "circle"
    circle_arrow = "circle-arrow"
    circle_caret = "circle-caret"
    circle_plus_minus = "circle-plus-minus"
    custom = "custom"
    happy_sad = "happy-sad"
    plus_minus = "plus-minus"
    square = "square"
    thumb = "thumb"
    triangle = "triangle"


class StandardDeviationInterval(Enum):
    """
    Use this property if the classificationMethod is `esriClassifyStandardDeviation`.
    """

    number_0_25 = 0.25
    number_0_33 = 0.33
    number_0_5 = 0.5
    number_1 = 1


class Focus(Enum):
    """
    Optional. Used for Relationship renderer. If not set, the legend will default to being square.
    """

    hh = "HH"
    hl = "HL"
    lh = "LH"
    ll = "LL"


class FlowTheme(Enum):
    """
    Theme to be used only when working with renderers of type `flow`.
    """

    flow_line = "flow-line"
    wave_front = "wave-front"


class ClassificationMethod(Enum):
    """
    Used for classed color or size. The default value is `esriClassifyManual`. The `esriClassifyDefinedInterval` method is only applicable to raster class breaks renderer only.
    """

    esri_classify_defined_interval = "esriClassifyDefinedInterval"
    esri_classify_equal_interval = "esriClassifyEqualInterval"
    esri_classify_manual = "esriClassifyManual"
    esri_classify_natural_breaks = "esriClassifyNaturalBreaks"
    esri_classify_quantile = "esriClassifyQuantile"
    esri_classify_standard_deviation = "esriClassifyStandardDeviation"


class RendererType(Enum):
    classed_color = "classedColor"
    classed_size = "classedSize"
    dot_density = "dotDensity"
    flow = "flow"
    predominance = "predominance"
    relationship = "relationship"
    univariate_color_size = "univariateColorSize"


class UnivariateTheme(Enum):
    """
    Theme to be used only when working with renderers of type `univariateColorSize`.
    """

    above = "above"
    above_and_below = "above-and-below"
    below = "below"
    high_to_low = "high-to-low"


class LegendOrder(Enum):
    """
    Indicates the order in which the legend is displayed.
    """

    ascending_values = "ascendingValues"
    descending_values = "descendingValues"


class StretchType(Enum):
    """
    The stretch types for stretch raster function.
    """

    histogram_equalization = "histogramEqualization"
    min_max = "minMax"
    none = "none"
    percent_clip = "percentClip"
    sigmoid = "sigmoid"
    standard_deviation = "standardDeviation"


class TrailCap(Enum):
    """
    The style of the streamline's cap. The 'round' cap will only be applied if trailWidth is greater than 3pts.
    """

    butt = "butt"
    round = "round"


class FlowRepresentation(Enum):
    """
    Sets the flow direction of the data.
    """

    flow_from = "flow_from"
    flow_to = "flow_to"


class NormalizationType(Enum):
    """
    Determine how the data was normalized.
    """

    esri_normalize_by_field = "esriNormalizeByField"
    esri_normalize_by_log = "esriNormalizeByLog"
    esri_normalize_by_percent_of_total = "esriNormalizeByPercentOfTotal"


class InputOutputUnit(Enum):
    """
    Input unit for Magnitude.
    """

    esri_feet_per_second = "esriFeetPerSecond"
    esri_kilometers_per_hour = "esriKilometersPerHour"
    esri_knots = "esriKnots"
    esri_meters_per_second = "esriMetersPerSecond"
    esri_miles_per_hour = "esriMilesPerHour"


class VectorFieldStyle(Enum):
    """
    A predefined style.
    """

    beaufort_ft = "beaufort_ft"
    beaufort_km = "beaufort_km"
    beaufort_kn = "beaufort_kn"
    beaufort_m = "beaufort_m"
    beaufort_mi = "beaufort_mi"
    classified_arrow = "classified_arrow"
    ocean_current_kn = "ocean_current_kn"
    ocean_current_m = "ocean_current_m"
    simple_scalar = "simple_scalar"
    single_arrow = "single_arrow"
    wind_speed = "wind_speed"
