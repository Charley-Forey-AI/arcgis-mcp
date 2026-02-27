from enum import Enum


class MarkerPlacement(Enum):
    """
    Indicates where the marker is placed.
    """

    begin = "begin"
    begin_end = "begin-end"
    end = "end"


class SimpleLineSymbolStyle(Enum):
    """
    String value representing the simple line symbol type.
    """

    esri_sls_dash = "esriSLSDash"
    esri_sls_dash_dot = "esriSLSDashDot"
    esri_sls_dash_dot_dot = "esriSLSDashDotDot"
    esri_sls_dot = "esriSLSDot"
    esri_sls_long_dash = "esriSLSLongDash"
    esri_sls_long_dash_dot = "esriSLSLongDashDot"
    esri_sls_null = "esriSLSNull"
    esri_sls_short_dash = "esriSLSShortDash"
    esri_sls_short_dash_dot = "esriSLSShortDashDot"
    esri_sls_short_dash_dot_dot = "esriSLSShortDashDotDot"
    esri_sls_short_dot = "esriSLSShortDot"
    esri_sls_solid = "esriSLSSolid"


class SimpleFillSymbolStyle(Enum):
    """
    String value representing the simple fill symbol type.
    """

    esri_sfs_backward_diagonal = "esriSFSBackwardDiagonal"
    esri_sfs_cross = "esriSFSCross"
    esri_sfs_diagonal_cross = "esriSFSDiagonalCross"
    esri_sfs_forward_diagonal = "esriSFSForwardDiagonal"
    esri_sfs_horizontal = "esriSFSHorizontal"
    esri_sfs_null = "esriSFSNull"
    esri_sfs_solid = "esriSFSSolid"
    esri_sfs_vertical = "esriSFSVertical"


class SimpleMarkerSymbolStyle(Enum):
    """
    String value representing the simple marker type.
    """

    esri_sms_circle = "esriSMSCircle"
    esri_sms_cross = "esriSMSCross"
    esri_sms_diamond = "esriSMSDiamond"
    esri_sms_square = "esriSMSSquare"
    esri_sms_triangle = "esriSMSTriangle"
    esri_smsx = "esriSMSX"


class TextDecoration(Enum):
    """
    The text decoration.
    """

    line_through = "line-through"
    none = "none"
    underline = "underline"


class TextStyle(Enum):
    """
    The text style.
    """

    italic = "italic"
    normal = "normal"
    oblique = "oblique"


class TextWeight(Enum):
    """
    The text weight.
    """

    bold = "bold"
    bolder = "bolder"
    lighter = "lighter"
    normal = "normal"


class HorizontalAlignment(Enum):
    """
    One of the following string values representing the horizontal alignment of the text.
    """

    center = "center"
    justify = "justify"
    left = "left"
    right = "right"


class VerticalAlignment(Enum):
    """
    One of the following string values representing the vertical alignment of the text.
    """

    baseline = "baseline"
    bottom = "bottom"
    middle = "middle"
    top = "top"


class Anchor(Enum):
    bottom = "bottom"
    bottom_left = "bottomLeft"
    bottom_right = "bottomRight"
    center = "center"
    left = "left"
    origin = "origin"
    relative = "relative"
    right = "right"
    top = "top"
    top_left = "topLeft"
    top_right = "topRight"


class LineCap(Enum):
    """
    Shape of the tips at the start and end of each line geometry. This also applies to the tips of each pattern segment along the line.
    """

    butt = "butt"
    round = "round"
    square = "square"


class Primitive(Enum):
    cone = "cone"
    cube = "cube"
    cylinder = "cylinder"
    diamond = "diamond"
    inverted_cone = "invertedCone"
    sphere = "sphere"
    tetrahedron = "tetrahedron"
    circle = "circle"
    cross = "cross"
    kite = "kite"
    square = "square"
    triangle = "triangle"
    x = "x"


class LineStyle(Enum):
    """
    String value representing the pattern used to render a line.
    """

    dash = "dash"
    dash_dot = "dash-dot"
    dash_dot_dot = "dash-dot-dot"
    dot = "dot"
    long_dash = "long-dash"
    long_dash_dot = "long-dash-dot"
    null = "null"
    short_dash = "short-dash"
    short_dash_dot = "short-dash-dot"
    short_dash_dot_dot = "short-dash-dot-dot"
    short_dot = "short-dot"
    solid = "solid"


class PolygonStyle(Enum):
    """
    String value representing predefined styles that can be set as polygon fills.
    """

    backward_diagonal = "backward-diagonal"
    cross = "cross"
    diagonal_cross = "diagonal-cross"
    forward_diagonal = "forward-diagonal"
    horizontal = "horizontal"
    none = "none"
    solid = "solid"
    vertical = "vertical"


class MarkerStyle(Enum):
    """
    Style of the marker.
    """

    arrow = "arrow"
    circle = "circle"
    cross = "cross"
    diamond = "diamond"
    square = "square"
    x = "x"


class Join(Enum):
    """
    Shape of the intersection of two line segments.
    """

    bevel = "bevel"
    miter = "miter"
    round = "round"


class PathCap(Enum):
    """
    Shape of the tips at the start and end of each path geometry.
    """

    butt = "butt"
    none = "none"
    round = "round"
    square = "square"


class Profile(Enum):
    """
    The shape which is extruded along the line.
    """

    circle = "circle"
    quad = "quad"


class ProfileRotation(Enum):
    """
    Specifies the axes about which the profile may be rotated at the joins. Constraining the rotation axes leads to a fixed orientation of the profile for the specified directions.
    """

    all = "all"
    heading = "heading"


class WaterbodySize(Enum):
    """
    Size of the waterbody the symbol layer represents. Applications will display waves that are appropriate for the chosen body of water, for example ocean versus marina versus swimming pool.
    """

    large = "large"
    medium = "medium"
    small = "small"


class WaveStrength(Enum):
    """
    The magnitude of the waves displayed on the waterbody. Strings roughly follow the [Douglas sea scale](https://en.wikipedia.org/wiki/Douglas_sea_scale), currently limited to lower degrees.
    """

    calm = "calm"
    moderate = "moderate"
    rippled = "rippled"
    slight = "slight"
