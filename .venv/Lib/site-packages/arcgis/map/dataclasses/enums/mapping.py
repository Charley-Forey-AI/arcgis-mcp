from enum import Enum


class ViewingMode(Enum):
    global_ = "global"
    local = "local"


class SnowCover(Enum):
    """
    Display surfaces covered with snow.
    """

    disabled = "disabled"
    enabled = "enabled"


class NavigationType(Enum):
    none = "none"
    stay_above = "stayAbove"


class AuthoringInfoType(Enum):
    classed_color = "classedColor"
    classed_size = "classedSize"
    dot_density = "dotDensity"
    flow = "flow"
    predominance = "predominance"
    relationship = "relationship"
    univariate_color_size = "univariateColorSize"


class Algorithm(Enum):
    """
    Algorithm used for calculating the ramp.
    """

    esri_cie_lab_algorithm = "esriCIELabAlgorithm"
    esri_hsv_algorithm = "esriHSVAlgorithm"
    esri_lab_l_ch_algorithm = "esriLabLChAlgorithm"


class ReferenceSizeSymbolStyle(Enum):
    """
    Only applicable when `sizeInfoType` is `referenceSize`. This value specifies the style (or shape) of the symbols generated with a reference line. Typically, a reference line is used to visualize the maximum possible size (i.e. data value) of a data-driven proportional symbol. Visualizations with a reference size must be defined in a ClassBreaksRenderer or UniqueValueRenderer with a CIMSymbol containing two symbol layers: one visualizing the maximum size of the symbol as a hollow ring, and the other as a solid shape whose size is driven by a data value set in the renderer's field property, and configured in a primitive override of the CIMSymbol. These renderers must not contain size visual variables, but may contain other visual variable types. This property is used for UI purposes only. When defined, `AuthoringInfoVisualVariable.sizeStops` must also be defined for the legend to properly represent the visualization.
    """

    circle = "circle"
    diamond = "diamond"
    hexagon_flat = "hexagon-flat"
    hexagon_pointy = "hexagon-pointy"
    square = "square"


class GroupInitialState(Enum):
    """
    Defines if the group should be expanded or collapsed when the form is initially displayed. If not provided, the default value is `expanded`
    """

    collapsed = "collapsed"
    expanded = "expanded"


class EnterExitRule(Enum):
    """
    The rule that determines whether a fence polygon has been entered or exited by the geometry from a feed. If this value is 'enterIntersectsAndExitDoesNotIntersect', a fence polygon is entered when it intersects a feed geometry and exited when it no longer intersects. If this value is 'enterContainsAndExitDoesNotContain', a fence polygon is entered when it contains a feed geometry and exited when it is no longer contained. If this value is 'enterContainsAndExitDoesNotIntersect' a fence polygon is entered when it contains a feed geometry and exited when it no longer intersects. If not set, the default behavior is `enterContainsAndExitDoesNotIntersect`. The 'feedAccuracyMode' must be set to 'useGeometryWithAccuracy' for this property to have an effect.
    """

    enter_contains_and_exit_does_not_contain = "enterContainsAndExitDoesNotContain"
    enter_contains_and_exit_does_not_intersect = "enterContainsAndExitDoesNotIntersect"
    enter_intersects_and_exit_does_not_intersect = (
        "enterIntersectsAndExitDoesNotIntersect"
    )


class FeedAccuracyMode(Enum):
    """
    Indicates how the geotrigger will use accuracy information from a feed. If this value is 'useGeometry', the reported geometry from a feed will be used. If this value is 'useGeometryWithAccuracy' the feed geometry will be used in conjunction with accuracy information. If not set, the default behavior is `useGeometry`.
    """

    use_geometry = "useGeometry"
    use_geometry_with_accuracy = "useGeometryWithAccuracy"


class FenceNotificationRule(Enum):
    """
    Indicates the type of event that will trigger notifications for the Fence Geotrigger. For example, a value of 'enter' will result in notifications when the geometry of the feed enters a fence polygon.
    """

    enter = "enter"
    enter_or_exit = "enterOrExit"
    exit = "exit"


class InteractionMode(Enum):
    """
    Indicates the mode in which the active range should be presented to the user.
    """

    picker = "picker"
    slider = "slider"


class HeightModel(Enum):
    """
    The surface type or height model of the vertical coordinate system.
    """

    ellipsoidal = "ellipsoidal"
    gravity_related_height = "gravity_related_height"


class HeightUnit(Enum):
    """
    The unit of the vertical coordinate system.<a href="#heightUnit"><sup>1</sup></a>
    """

    field_150_kilometers = "150-kilometers"
    field_50_kilometers = "50-kilometers"
    benoit_1895_b_chain = "benoit-1895-b-chain"
    clarke_foot = "clarke-foot"
    clarke_link = "clarke-link"
    clarke_yard = "clarke-yard"
    foot = "foot"
    gold_coast_foot = "gold-coast-foot"
    indian_1937_yard = "indian-1937-yard"
    indian_yard = "indian-yard"
    meter = "meter"
    sears_1922_truncated_chain = "sears-1922-truncated-chain"
    sears_chain = "sears-chain"
    sears_foot = "sears-foot"
    sears_yard = "sears-yard"
    us_foot = "us-foot"


class SlideLayout(Enum):
    """
    The layout of the slide.
    """

    caption = "caption"
    cover = "cover"
    none = "none"
