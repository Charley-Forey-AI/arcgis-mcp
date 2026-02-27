from __future__ import annotations  # Enables postponed evaluation of type hints

from typing import Literal, Annotated
from .base_model import BaseModel, common_config
from pydantic import Field, ConfigDict, confloat, conint, constr, field_validator
from ..enums.symbols import (
    LineStyle,
    LineCap,
    MarkerPlacement,
    MarkerStyle,
    Anchor,
    Join,
    PolygonStyle,
    Primitive,
    PathCap,
    Profile,
    ProfileRotation,
    HorizontalAlignment,
    VerticalAlignment,
    WaterbodySize,
    WaveStrength,
    SimpleLineSymbolStyle,
    SimpleMarkerSymbolStyle,
    TextDecoration,
    TextStyle,
    SimpleFillSymbolStyle,
    TextWeight,
)


class LinePattern(BaseModel):
    """
    A pattern used to render a line.
    """

    model_config = common_config
    style: LineStyle = Field(
        ...,
        description="String value representing the pattern used to render a line.",
    )
    type: Literal["style"] = Field(
        "style", description="The type of pattern applied to a line."
    )


class Material(BaseModel):
    """
    The material used to shade the geometry.
    """

    model_config = common_config
    color: list[confloat(ge=0, le=255)] | None = Field(
        None,
        description="Color is represented as a three or four-element array. The four elements represent values for red, green, blue, and alpha in that order. Values range from 0 through 255. If color is undefined for a symbol, the color value is null.",
        title="color",
    )
    transparency: conint(ge=0, le=100) | None = Field(
        None,
        description="A value between `100` (full transparency) and `0` (full opacity). Ignored if no color is specified.",
    )


class StyleOrigin(BaseModel):
    """
    The origin of the style from which the symbol was originally referenced. A reference to the style origin can be either by styleName or by styleUrl (but not both). It may be used to understand where a symbol was originally sourced from, but does not affect actual appearance or rendering of the symbol.
    """

    model_config = common_config
    name: str = Field(..., description="Identifies a symbol in the style by name.")
    style_name: str = Field(
        None,
        alias="styleName",
        description="A registered web style name, such as `EsriThematicShapesStyle`",
    )
    style_url: str | constr(pattern=r"^\./.+$") | None = Field(
        None, alias="styleUrl", description="URL to a style definition."
    )


class SketchEdges(BaseModel):
    """
    The sketch edge rendering configuration of a symbol layer. Edges of type `sketch` are rendered with a hand-drawn look in mind.
    """

    model_config = common_config
    color: list[confloat(ge=0, le=255)] = Field(
        ...,
        description="Color is represented as a three or four-element array. The four elements represent values for red, green, blue, and alpha in that order. Values range from 0 through 255. If color is undefined for a symbol, the color value is null.",
        title="color",
    )
    extension_length: float | None = Field(
        None,
        alias="extensionLength",
        description="A size in points by which to extend edges beyond their original end points.",
    )
    size: confloat(ge=0.0) | None = Field(
        None, description="Edge size in points, positive only"
    )
    transparency: conint(ge=0, le=100) | None = Field(
        None,
        description="The value has to lie between `100` (full transparency) and `0` (full opacity).",
    )
    type: Literal["sketch"] = Field(
        "sketch", description="The type of edge visualization."
    )


class SolidEdges(BaseModel):
    """
    The solid edge rendering configuration of a symbol layer. Edges of type `solid` are rendered in a single color, unaffected by scene lighting.
    """

    model_config = common_config
    color: list[confloat(ge=0, le=255)] = Field(
        ...,
        description="Color is represented as a three or four-element array. The four elements represent values for red, green, blue, and alpha in that order. Values range from 0 through 255. If color is undefined for a symbol, the color value is null.",
        title="color",
    )
    extension_length: float | None = Field(
        None,
        alias="extensionLength",
        description="A size in points by which to extend edges beyond their original end points.",
    )
    size: confloat(ge=0.0) | None = Field(
        None, description="Edge size in points, positive only"
    )
    transparency: conint(ge=0, le=100) | None = Field(
        None,
        description="The value has to lie between `100` (full transparency) and `0` (full opacity).",
    )
    type: Literal["solid"] = Field(
        "solid", description="The type of edge visualization."
    )


class Outline(BaseModel):
    """
    The outline of the symbol layer.
    """

    model_config = common_config
    color: list[confloat(ge=0, le=255)] = Field(
        None,
        description="Color is represented as a three or four-element array. The four elements represent values for red, green, blue, and alpha in that order. Values range from 0 through 255. If color is undefined for a symbol, the color value is null.",
        title="color",
    )
    pattern: LinePattern | None = None
    pattern_cap: LineCap | None = Field(LineCap.butt, alias="patternCap")
    size: confloat(ge=0.0) = Field(
        None, description="Outline size in points, positive only"
    )
    transparency: conint(ge=0, le=100) | None = Field(
        None,
        description="The value has to lie between `100` (full transparency) and `0` (full opacity).",
    )


class Pattern(BaseModel):
    """
    The pattern used to render the fill of the polygon (only applies to PolygonSymbol3D).
    """

    model_config = common_config
    style: PolygonStyle = Field(
        ...,
        description="String value representing predefined styles that can be set as polygon fills.",
    )
    type: Literal["style"] = Field(
        "style",
        description="The type of pattern applied to the polygon fill.",
    )


class FillSymbol3DLayer(BaseModel):
    """
    FillSymbol3DLayer is used to render the surfaces of flat 2D Polygon geometries and 3D volumetric meshes in a SceneView.
    """

    model_config = common_config
    cast_shadows: bool | None = Field(
        True,
        alias="castShadows",
        description="Boolean to control the shadow casting behavior of the rendered geometries (only applies to MeshSymbol3D).",
    )
    edges: SketchEdges | SolidEdges | None = Field(
        None,
        description="Specifies an edge visualization style (only applies to MeshSymbol3D). Edges describe the style applied to visually important edges of 3D objects.",
        title="Edges",
    )
    enable: bool | None = None
    material: Material | None = None
    outline: Outline | None = Field(
        None,
        description="The outline of the symbol layer (only applies to PolygonSymbol3D).",
    )
    pattern: Pattern | None = None
    type: Literal["Fill"] = Field(
        "Fill", description="Specifies the type of symbol used."
    )


class IconSymbol3DLayerResource(BaseModel):
    """
    The shape (primitive) or image URL (href) used to visualize the features.
    """

    model_config = common_config
    data_uri: constr(pattern=r"^data:image/(.|\n|\r)+$") | None = Field(
        None,
        alias="dataURI",
        description="an image encoded as base64 string, starting with `data:image/`",
    )
    href: constr(pattern=r"^https?://.+$") | constr(pattern=r"^\./.+$") | None = Field(
        None, description="URL to the returned image."
    )
    primitive: Primitive = Field(None, description="Specifies the type of symbol used.")


class IconSymbol3DLayer(BaseModel):
    """
    IconSymbol3DLayer is used to render Point geometries using a flat 2D icon (e.g. a circle) with a PointSymbol3D in a SceneView.
    """

    model_config = common_config
    anchor: Anchor | None = Anchor.center.value
    anchor_position: list[float] | None = Field(
        None,
        alias="anchorPosition",
        description="When `anchor` equals `relative`, this property specifies the position within the icon that should coincide with the feature geometry. Otherwise it is ignored. The position is defined as a factor of the icon dimensions that is added to the icon center: `positionInIcon = (0.5 + anchorPosition) * size`, where `size` is the original size of the icon resource.",
        max_length=2,
        min_length=2,
    )
    angle: confloat(ge=0.0, le=360.0) | None = Field(
        0.0,
        description="Rotation angle in degrees. The rotation is defined in screen space, with a rotation of 0 degrees (default value) pointing in the direction of the Y-axis. Positive values indicate clockwise rotation.",
    )
    enable: bool | None = None
    material: Material | None = None
    outline: Outline | None = Field(
        None,
        description="Sets properties of the outline of the IconSymbol3DLayer.",
    )
    resource: IconSymbol3DLayerResource | None = Field(
        None,
        description="The shape (primitive) or image URL (href) used to visualize the features.",
        title="IconSymbol3DLayer Resource",
    )
    size: confloat(ge=0.0) = Field(
        ..., description="Icon size in points, positive only"
    )
    type: Literal["Icon"] = Field(
        "Icon", description="Specifies the type of symbol used."
    )


class LineMarker(BaseModel):
    """
    Represents markers placed at the start and end of each line geometry, or both. Markers size is proportional to the width of the line.
    """

    model_config = common_config
    color: list[confloat(ge=0, le=255)] | None = Field(
        None,
        description="An option to color the markers differently from the line. By default the markers inherit the line's color.",
        title="color",
    )
    placement: MarkerPlacement | None = Field(
        None, description="Indicates where the marker is placed."
    )
    style: MarkerStyle | None = Field(None, description="Style of the marker.")
    type: Literal["style"] = Field(
        "style", description="The type of marker applied to a line."
    )


class MeshSymbol3D(BaseModel):
    """
    MeshSymbol3D is used to render 3D mesh features in a SceneLayer in a 3D SceneView.
    """

    model_config = common_config
    style_origin: StyleOrigin | None = Field(
        None,
        alias="styleOrigin",
        description="The origin of the style from which the symbol was originally referenced. A reference to the style origin can be either by styleName or by styleUrl (but not both). It may be used to understand where a symbol was originally sourced from, but does not affect actual appearance or rendering of the symbol.",
        title="styleOrigin",
    )
    symbol_layers: list[FillSymbol3DLayer] = Field(
        ...,
        alias="symbolLayers",
        description="A Collection of Symbol3DLayer objects used to visualize the graphic or feature.",
    )
    type: Literal["MeshSymbol3D"] = Field(
        "MeshSymbol3D", description="Specifies the type of symbol used"
    )


class VerticalOffset(BaseModel):
    """
    Shifts the symbol along the vertical world axis by a given length. The length is set in screen space units.
    """

    model_config = common_config
    max_world_length: float | None = Field(
        None,
        alias="maxWorldLength",
        description="The maximum vertical symbol lift in world units. It acts as an upper bound to avoid lift becoming too big.",
    )
    min_world_length: float | None = Field(
        0,
        alias="minWorldLength",
        description="The minimum vertical symbol lift in world units. It acts as a lower bound to avoid lift becoming too small.",
    )
    screen_length: float = Field(
        ...,
        alias="screenLength",
        description="Maximal screen length of lift in points.",
    )


class ObjectSymbol3DLayerResource(BaseModel):
    """
    The primitive shape (primitive) or external 3D model (href) used to visualize the points.
    """

    model_config = common_config
    href: constr(pattern=r"^https?://.+$") | constr(pattern=r"^\./.+$") | None = Field(
        None
    )
    primitive: Primitive | None = Field(
        None, description="Specifies the type of symbol used."
    )


class ExtrudeSymbol3DLayer(BaseModel):
    """
    ExtrudeSymbol3DLayer is used to render Polygon geometries by extruding them upward from the ground, creating a 3D volumetric object.
    """

    model_config = common_config
    cast_shadows: bool | None = Field(
        True,
        alias="castShadows",
        description="Boolean to control the shadow casting behavior of the rendered geometries.",
    )
    edges: SketchEdges | SolidEdges | None = Field(
        None,
        description="Specifies an edge visualization style.",
        title="Edges",
    )
    enable: bool | None = None
    material: Material | None = None
    size: float = Field(..., description="Extrusion height in meters.")
    type: Literal["Extrude"] = Field(
        "Extrude", description="Specifies the type of symbol used."
    )


class LineSymbol3DLayer(BaseModel):
    """
    LineSymbol3DLayer renders Polyline geometries using a flat 2D line with a LineSymbol3D in a 3D SceneView.
    """

    model_config = common_config
    cap: LineCap | str | None = "butt"
    enable: bool | None = None
    join: Join | None = Field(
        Join.miter.value,
        validate_default=True,
        description="Shape of the intersection of two line segments.",
    )
    marker: LineMarker | None = None
    material: Material | None = None
    pattern: LinePattern | None = None
    size: confloat(ge=0.0) = Field(
        ..., description="Line width in points, positive only"
    )
    type: Literal["Line"] = Field(
        "Line", description="Specifies the type of symbol used."
    )


class ObjectSymbol3DLayer(BaseModel):
    """
    ObjectSymbol3DLayer is used to render Point geometries using a volumetric 3D shape (e.g., a sphere or cylinder) with a Symbol3D in a SceneView.
    """

    model_config = common_config
    anchor: Anchor | None = Field(
        Anchor.origin.value,
        validate_default=True,
        description="The positioning of the object relative to the geometry.",
    )
    anchor_position: list[float] | None = Field(
        None,
        alias="anchorPosition",
        description="When `anchor` equals `relative`, this property specifies the positioning of the object relative to the geometry as a fraction of the symbol layer's bounding box. Otherwise it is ignored.",
        max_length=3,
        min_length=3,
    )
    cast_shadows: bool | None = Field(
        True,
        alias="castShadows",
        description="Boolean to control the shadow casting behavior of the rendered geometries.",
    )
    depth: confloat(ge=0.0) | None = Field(
        None, description="Object depth in meters, positive only"
    )
    enable: bool | None = None
    heading: float | None = Field(
        None,
        description="Rotation angle around Z axis in degrees. At 0 degrees, the model points in the direction of the Y-axis. Positive values indicate clockwise rotation (when looked at from the top). [Detailed description](static/objectSymbolLayerOrientation.md).",
    )
    height: confloat(ge=0.0) | None = Field(
        None, description="Object height in meters, positive only"
    )
    material: Material | None = None
    resource: ObjectSymbol3DLayerResource | None = Field(
        None,
        description="The primitive shape (primitive) or external 3D model (href) used to visualize the points.",
        title="ObjectSymbol3DLayer Resource",
    )
    roll: float | None = Field(
        None,
        description="Rotation angle around Y axis in degrees. At 0 degrees, the model is level. A positive value lifts the left part and lowers the right part of the model. [Detailed description](static/objectSymbolLayerOrientation.md).",
    )
    tilt: float | None = Field(
        None,
        description="Rotation angle around X axis in degrees. At 0 degrees, the model is level. A positive value lifts the front and lowers the back of the model. [Detailed description](static/objectSymbolLayerOrientation.md).",
    )
    type: Literal["Object"] = Field(
        "Object", description="Specifies the type of symbol used."
    )
    width: confloat(ge=0.0) | None = Field(
        None, description="Object width in meters, positive only"
    )


class Border(BaseModel):
    """
    Optional border on the line that is used to improve the contrast of the line color against various background colors.
    """

    model_config = common_config
    color: list[confloat(ge=0, le=255)] = Field(
        ...,
        description="Color is represented as a three or four-element array. The four elements represent values for red, green, blue, and alpha in that order. Values range from 0 through 255. If color is undefined for a symbol, the color value is null.",
        title="color",
    )
    transparency: conint(ge=0, le=100) | None = Field(
        None,
        description="A value between `100` (full transparency) and `0` (full opacity).",
    )


class Callout(BaseModel):
    """
    Callout configuration for a symbol.
    """

    model_config = common_config
    border: Border | None = None
    color: list[confloat(ge=0, le=255)] = Field(
        ..., description="The color of the line.", title="color"
    )
    size: confloat(ge=0.0) = Field(..., description="The width of the line in points.")
    transparency: conint(ge=0, le=100) | None = Field(
        None,
        description="A value between `100` (full transparency) and `0` (full opacity).",
    )
    type: Literal["line"] = Field(
        "line",
        description="The type of the callout. A callout of type `line` connects an offset symbol or label with its location.",
    )


class TextBackground(BaseModel):
    """
    Text background definition.
    """

    model_config = common_config
    color: list[confloat(ge=0, le=255)] | None = Field(
        None,
        description="Color is represented as a three or four-element array. The four elements represent values for red, green, blue, and alpha in that order. Values range from 0 through 255. If color is undefined for a symbol, the color value is null.",
        title="color",
    )
    transparency: conint(ge=0, le=100) | None = Field(
        None,
        description="A value between `100` (full transparency) and `0` (full opacity).",
    )


class Halo(BaseModel):
    """
    Halo definition.
    """

    model_config = common_config
    color: list[confloat(ge=0, le=255)] | None = Field(
        None,
        description="Color is represented as a three or four-element array. The four elements represent values for red, green, blue, and alpha in that order. Values range from 0 through 255. If color is undefined for a symbol, the color value is null.",
        title="color",
    )
    size: float | None = Field(None, description="Width of the halo in points.")
    transparency: conint(ge=0, le=100) | None = Field(
        None,
        description="A value between `100` (full transparency) and `0` (full opacity).",
    )


class TextSymbol3DLayer(BaseModel):
    """
    Symbol layer for text and font definitions.
    """

    model_config = common_config
    background: TextBackground | None = None
    enable: bool | None = None
    font: str | None = None
    halo: Halo | None = None
    horizontal_alignment: HorizontalAlignment | None = Field(
        HorizontalAlignment.center,
        validate_default=True,
        alias="horizontalAlignment",
        description="One of the following string values representing the horizontal alignment of the text.",
    )
    line_height: confloat(ge=0.1, le=4.0) | None = Field(
        1,
        alias="lineHeight",
        description="Multiplier to scale the vertical distance between the baselines of text with multiple lines.",
    )
    material: Material | None = None
    size: confloat(ge=0.0) | None = Field(
        None, description="Font size in points, positive only"
    )
    text: str | None = Field(
        None,
        description="Text content in the label. Typically this property is not set, as text content is read from labeling field.",
    )
    type: Literal["Text"] = Field(
        "Text", description="Specifies the type of symbol used."
    )
    vertical_alignment: VerticalAlignment | None = Field(
        VerticalAlignment.baseline,
        validate_default=True,
        alias="verticalAlignment",
        description="One of the following string values representing the vertical alignment of the text.",
    )


class PointSymbol3D(BaseModel):
    """
    PointSymbol3D is used to render features with Point geometry in a 3D SceneView.
    """

    model_config = common_config
    callout: Callout | None = None
    style_origin: StyleOrigin | None = Field(
        None,
        alias="styleOrigin",
        description="The origin of the style from which the symbol was originally referenced. A reference to the style origin can be either by styleName or by styleUrl (but not both). It may be used to understand where a symbol was originally sourced from, but does not affect actual appearance or rendering of the symbol.",
        title="styleOrigin",
    )
    symbol_layers: list[IconSymbol3DLayer | ObjectSymbol3DLayer | TextSymbol3DLayer] = (
        Field(
            ...,
            alias="symbolLayers",
            description="A Collection of Symbol3DLayer objects used to visualize the graphic or feature.",
        )
    )
    type: Literal["PointSymbol3D"] = Field(
        "PointSymbol3D", description="Specifies the type of symbol used"
    )
    vertical_offset: VerticalOffset | None = Field(None, alias="verticalOffset")


class PathSymbol3DLayer(BaseModel):
    """
    PathSymbol3DLayer renders polyline geometries by extruding a 2D profile along the line, resulting in visualizations like tubes, walls, etc.
    """

    model_config = common_config
    anchor: Anchor | None = Field(
        Anchor.center.value,
        validate_default=True,
        description="The position of the extrusion profile with respect to the polyline geometry.",
    )
    cap: PathCap | None = PathCap.butt.value
    cast_shadows: bool | None = Field(
        True,
        alias="castShadows",
        description="Boolean to control the shadow casting behavior of the rendered geometries.",
    )
    enable: bool | None = None
    height: confloat(ge=0.0) = Field(
        ...,
        description="Path height in meters. If unspecified, it is equal to `width`.",
    )
    join: Join | None = Field(
        Join.miter.value,
        validate_default=True,
        description="Shape of the intersection of two line segments.",
    )
    material: Material | None = None
    profile: Profile | None = Field(
        Profile.circle.value,
        validate_default=True,
        description="The shape which is extruded along the line.",
    )
    profile_rotation: ProfileRotation | None = Field(
        ProfileRotation.all.value,
        validate_default=True,
        alias="profileRotation",
        description="Specifies the axes about which the profile may be rotated at the joins. Constraining the rotation axes leads to a fixed orientation of the profile for the specified directions.",
    )
    size: confloat(ge=0.0) | None = Field(
        None,
        description="Path size (diameter) in meters. Ignored if either `width` or `height` are present.",
    )
    type: Literal["Path"] = Field(
        "Path", description="Specifies the type of symbol used."
    )
    width: confloat(ge=0.0) | None = Field(
        None,
        description="Path width in meters. If unspecified, it is equal to `height`.",
    )


class LineSymbol3D(BaseModel):
    """
    LineSymbol3D is used to render features with Polyline geometry in a 3D SceneView.
    """

    model_config = common_config
    style_origin: StyleOrigin | None = Field(
        None,
        alias="styleOrigin",
        description="The origin of the style from which the symbol was originally referenced. A reference to the style origin can be either by styleName or by styleUrl (but not both). It may be used to understand where a symbol was originally sourced from, but does not affect actual appearance or rendering of the symbol.",
        title="styleOrigin",
    )
    symbol_layers: list[LineSymbol3DLayer | PathSymbol3DLayer] = Field(
        ...,
        alias="symbolLayers",
        description="A Collection of Symbol3DLayer objects used to visualize the graphic or feature.",
    )
    type: Literal["LineSymbol3D"] = Field(
        "LineSymbol3D", description="Specifies the type of symbol used."
    )


class WaterSymbol3DLayer(BaseModel):
    """
    Symbol Layer that describes a water appearance on surfaces in a SceneView.
    """

    model_config = common_config
    color: list[confloat(ge=0, le=255)] | None = Field(
        [0, 119, 190], description="The dominant water color.", title="color"
    )
    enable: bool | None = None
    type: Literal["Water"] = Field(
        "Water", description="Specifies the type of symbol used."
    )
    waterbody_size: WaterbodySize | None = Field(
        WaterbodySize.medium.value,
        validate_default=True,
        alias="waterbodySize",
        description="Size of the waterbody the symbol layer represents. Applications will display waves that are appropriate for the chosen body of water, for example ocean versus marina versus swimming pool.",
    )
    wave_direction: confloat(ge=0.0, le=360.0) | None = Field(
        None,
        alias="waveDirection",
        description="Azimuthal bearing for direction of the waves. If ommitted, waves appear directionless. The value is interpreted as 'geographic' rotation, i.e. clockwise starting from north.",
    )
    wave_strength: WaveStrength | None = Field(
        WaveStrength.moderate.value,
        validate_default=True,
        alias="waveStrength",
        description="The magnitude of the waves displayed on the waterbody. Strings roughly follow the [Douglas sea scale](https://en.wikipedia.org/wiki/Douglas_sea_scale), currently limited to lower degrees.",
    )


class PolygonSymbol3D(BaseModel):
    """
    PolygonSymbol3D is used to render features with Polygon geometry in a 3D SceneView. Polygon features may also be rendered as points with icons or objects at the centroid of each polygon.
    """

    model_config = common_config
    style_origin: StyleOrigin | None = Field(
        None,
        alias="styleOrigin",
        description="The origin of the style from which the symbol was originally referenced. A reference to the style origin can be either by styleName or by styleUrl (but not both). It may be used to understand where a symbol was originally sourced from, but does not affect actual appearance or rendering of the symbol.",
        title="styleOrigin",
    )
    symbol_layers: list[
        ExtrudeSymbol3DLayer
        | FillSymbol3DLayer
        | IconSymbol3DLayer
        | LineSymbol3DLayer
        | ObjectSymbol3DLayer
        | TextSymbol3DLayer
        | WaterSymbol3DLayer
    ] = Field(
        ...,
        alias="symbolLayers",
        description="A Collection of Symbol3DLayer objects used to visualize the graphic or feature.",
    )
    type: Literal["PolygonSymbol3D"] = Field(
        "PolygonSymbol3D", description="Specifies the type of symbol used."
    )


class LabelSymbol3D(BaseModel):
    """
    LabelSymbol3D is used to render labels for features from a FeatureLayer in a 3D SceneView.
    """

    model_config = common_config
    callout: Callout | None = None
    symbol_layers: list[TextSymbol3DLayer] = Field(
        ...,
        alias="symbolLayers",
        description="A Collection of Symbol3DLayer objects used to visualize the graphic or feature.",
    )
    type: Literal["LabelSymbol3D"] = Field(
        "LabelSymbol3D", description="Specifies the type of symbol used."
    )
    vertical_offset: VerticalOffset | None = Field(None, alias="verticalOffset")


class Marker(BaseModel):
    """
    Represents markers placed along the line. Markers will have the same color as the line, and their size will be proportional to the width of the line.
    """

    model_config = common_config
    placement: MarkerPlacement | None = Field(
        None, description="Indicates where the marker is placed."
    )
    style: Literal["arrow"] | None = Field(None, description="Style of the marker.")


class SimpleLineSymbolEsriSLS(BaseModel):
    """
    Simple line symbols can be used to symbolize polyline geometries or outlines for polygon fills.
    """

    model_config = common_config

    color: list[Annotated[int, Field(ge=0, le=255)]] | None = Field(
        None,
        description="Color is represented as a four-element array. The four elements represent values for red, green, blue, and alpha in that order. Values range from 0 through 255. If color is undefined for a symbol, the color value is null.",
    )
    marker: Marker | None = None
    style: SimpleLineSymbolStyle | None = Field(
        None, description="String value representing the simple line symbol type."
    )
    type: Literal["esriSLS"] | None = Field(
        "esriSLS", description="Specifies the type of symbol used."
    )
    width: float | int | None = Field(
        None, description="Numeric value indicating the width of the line in points."
    )


class CimSymbolReference(BaseModel):
    """
    Represents a symbol reference that contains a CIM symbol. In addition to `type` listed below, a symbol reference will contain additional properties.
    """

    model_config = ConfigDict(
        extra="allow",
        populate_by_name=True,
    )
    type: Literal["CIMSymbolReference"] = Field(
        "CIMSymbolReference", description="Specifies the type of symbol used."
    )


class PictureFillSymbolEsriPFS(BaseModel):
    """
    Picture fill symbols can be used to symbolize polygon geometries.
    """

    model_config = common_config

    angle: Annotated[float, Field(ge=0.0, le=360.0)] | None = Field(
        None,
        description="Numeric value that defines the number of degrees ranging from 0-360, that a marker symbol is rotated. The rotation is from East in a counter-clockwise direction where East is the 0� axis.",
    )
    content_type: str | None = Field(
        None,
        alias="contentType",
        description="String value indicating the content type for the image.",
    )
    height: float | int | None = Field(
        None,
        description="Numeric value used if needing to resize the symbol. Specify a value in points. If images are to be displayed in their original size, leave this blank.",
    )
    image_data: str | None = Field(
        None,
        alias="imageData",
        description="String value indicating the base64 encoded data.",
    )
    outline: SimpleLineSymbolEsriSLS | None = Field(
        None, description="Sets the outline of the symbol."
    )
    type: Literal["esriPFS"] = Field(
        "esriPFS", description="Specifies the type of symbol used."
    )
    url: str | None = Field(
        None,
        description="String value indicating the URL of the image. The URL should be relative if working with static layers. A full URL should be used for map service dynamic layers. A relative URL can be dereferenced by accessing the map layer image resource or the feature layer image resource.",
    )
    width: float | int | None = Field(
        None,
        description="Numeric value used if needing to resize the symbol. Specify a value in points. If images are to be displayed in their original size, leave this blank.",
    )
    xoffset: float | int | None = Field(
        None, description="Numeric value indicating the offset on the x-axis in points."
    )
    xscale: float | int | None = Field(
        None, description="Numeric value indicating the scale factor in x direction."
    )
    yoffset: float | int | None = Field(
        None, description="Numeric value indicating the offset on the y-axis in points."
    )
    yscale: float | int | None = Field(
        None, description="Numeric value indicating the scale factor in y direction."
    )

    @field_validator("angle", mode="before")
    def check_angle(cls, v):
        if v:
            # constrain angle to 360 degrees; if negative, make it positive
            v = v % 360
        return v


class StyleSymbolReference(BaseModel):
    """
    The StyleSymbolReference is used to reference a symbol from a portal styleItem
    """

    model_config = common_config
    name: str = Field(..., description="Identifies a symbol in the style by name.")
    style_name: str | None = Field(
        None,
        alias="styleName",
        description="A registered web style name, such as `EsriThematicTreesStyle`",
    )
    style_url: str | None = Field(
        None, alias="styleUrl", description="URL to a style definition."
    )
    type: Literal["styleSymbolReference"] = Field(
        "styleSymbolReference", description="The type of the symbol"
    )


class PictureMarkerSymbolEsriPMS(BaseModel):
    """
    Picture marker symbols can be used to symbolize point geometries.
    """

    model_config = common_config

    angle: Annotated[float, Field(ge=0.0, le=360.0)] | None = Field(
        None,
        description="Numeric value that defines the number of degrees ranging from 0-360, that a marker symbol is rotated. The rotation is from East in a counter-clockwise direction where East is the 0� axis.",
    )
    content_type: str | None = Field(
        None,
        alias="contentType",
        description="String value indicating the content type for the image.",
    )
    height: float | int | None = Field(
        None,
        description="Numeric value used if needing to resize the symbol. Specify a value in points. If images are to be displayed in their original size, leave this blank.",
    )
    image_data: str | None = Field(
        None,
        alias="imageData",
        description="String value indicating the base64 encoded data.",
    )
    type: Literal["esriPMS"] = Field(
        "esriPMS", description="Specifies the type of symbol used."
    )
    url: str | None = Field(
        None,
        description="String value indicating the URL of the image. The URL should be relative if working with static layers. A full URL should be used for map service dynamic layers. A relative URL can be dereferenced by accessing the map layer image resource or the feature layer image resource.",
    )
    width: float | int | None = Field(
        None,
        description="Numeric value used if needing to resize the symbol. Specify a value in points. If images are to be displayed in their original size, leave this blank.",
    )
    xoffset: float | int | None = Field(
        None, description="Numeric value indicating the offset on the x-axis in points."
    )
    yoffset: float | int | None = Field(
        None, description="Numeric value indicating the offset on the y-axis in points."
    )

    @field_validator("angle", mode="before")
    def check_angle(cls, v):
        if v:
            # constrain angle to 360 degrees; if negative, make it positive
            v = v % 360
        return v


class SimpleFillSymbolEsriSFS(BaseModel):
    """
    Simple fill symbols that can be used to symbolize polygon geometries.
    """

    model_config = common_config

    color: list[confloat(ge=0, le=255)] | None = Field(
        None,
        description="Color is represented as a four-element array. The four elements represent values for red, green, blue, and alpha in that order. Values range from 0 through 255. If color is undefined for a symbol, the color value is null.",
    )
    outline: SimpleLineSymbolEsriSLS | None = Field(
        None, description="Sets the outline of the fill symbol."
    )
    style: SimpleFillSymbolStyle = Field(
        ..., description="String value representing the simple fill symbol type."
    )
    type: Literal["esriSFS"] = Field(
        "esriSFS", description="Specifies the type of symbol used."
    )


class SimpleMarkerSymbolEsriSMS(BaseModel):
    """
    Simple marker symbols can be used to symbolize point geometries.
    """

    model_config = common_config

    angle: Annotated[float, Field(ge=0.0, le=360.0)] | None = Field(
        None,
        description="Numeric value used to rotate the symbol. The symbol is rotated counter-clockwise. For example, The following, angle=-30, in will create a symbol rotated -30 degrees counter-clockwise; that is, 30 degrees clockwise.",
    )
    color: list[Annotated[int, Field(ge=0, le=255)]] | None = Field(
        None,
        description="Color is represented as a four-element array. The four elements represent values for red, green, blue, and alpha in that order. Values range from 0 through 255. If color is undefined for a symbol, the color value is null.",
    )
    outline: SimpleLineSymbolEsriSLS | None = Field(
        None, description="Sets the outline of the marker symbol."
    )
    size: float | int | None = Field(
        None, description="Numeric size of the symbol given in points."
    )
    style: SimpleMarkerSymbolStyle = Field(
        ..., description="String value representing the simple marker type."
    )
    type: Literal["esriSMS"] = Field(
        "esriSMS", description="Specifies the type of symbol used."
    )
    xoffset: float | int | None = Field(
        None, description="Numeric value indicating the offset on the x-axis in points."
    )
    yoffset: float | int | None = Field(
        None, description="Numeric value indicating the offset on the y-axis in points."
    )

    @field_validator("angle", mode="before")
    def check_angle(cls, v):
        if v:
            # constrain angle to 360 degrees; if negative, make it positive
            v = v % 360
        return v


class TextFont(BaseModel):
    """
    Font used for text symbols
    """

    model_config = common_config

    decoration: TextDecoration | None = Field(None, description="The text decoration.")
    family: str | None = Field(None, description="The font family.")
    size: float | int | None = Field(None, description="The font size in points.")
    style: TextStyle | None = Field(None, description="The text style.")
    weight: TextWeight | None = Field(None, description="The text weight.")


class TextSymbolEsriTS(BaseModel):
    """
    Text symbols are used to add text to a feature (labeling).
    """

    model_config = common_config

    angle: Annotated[float, Field(ge=0.0, le=360.0)] | None = Field(
        None,
        description="A numeric value that defines the number of degrees (0 to 360) that a text symbol is rotated. The rotation is from East in a counter-clockwise direction where East is the 0� axis.",
    )
    background_color: list[Annotated[int, Field(ge=0, le=255)]] | None = Field(
        None,
        alias="backgroundColor",
        description="Background color is represented as a four-element array. The four elements represent values for red, green, blue, and alpha in that order. Values range from 0 through 255. If color is undefined for a symbol, the color value is null.",
    )
    border_line_color: list[Annotated[int, Field(ge=0, le=255)]] | None = Field(
        None,
        alias="borderLineColor",
        description="Borderline color is represented as a four-element array. The four elements represent values for red, green, blue, and alpha in that order. Values range from 0 through 255. If color is undefined for a symbol, the color value is null.",
    )
    border_line_size: float | int | None = Field(
        None,
        alias="borderLineSize",
        description="Numeric value indicating the the size of the border line in points.",
    )
    color: list[Annotated[int, Field(ge=0, le=255)]] | None = Field(
        None,
        description="Color is represented as a four-element array. The four elements represent values for red, green, blue, and alpha in that order. Values range from 0 through 255. If color is undefined for a symbol, the color value is null.",
    )
    font: TextFont | None = Field(
        None, description="An object specifying the font used for the text symbol."
    )
    halo_color: list[Annotated[int, Field(ge=0, le=255)]] | None = Field(
        None, alias="haloColor", description="Color of the halo around the text."
    )
    halo_size: float | int | None = Field(
        None,
        alias="haloSize",
        description="Numeric value indicating the point size of a halo around the text symbol.",
    )
    horizontal_alignment: HorizontalAlignment | None = Field(
        None,
        alias="horizontalAlignment",
        description="One of the following string values representing the horizontal alignment of the text.",
    )
    kerning: bool | None = Field(
        None,
        description="Boolean value indicating whether to adjust the spacing between characters in the text string.",
    )
    right_to_left: bool | None = Field(
        None,
        alias="rightToLeft",
        description="Boolean value, set to true if using Hebrew or Arabic fonts.",
    )
    rotated: bool | None = Field(
        None,
        description="Boolean value indicating whether every character in the text string is rotated.",
    )
    text: str | None = Field(
        None, description="only applicable when specified as a client-side graphic."
    )
    type: Literal["esriTS"] = Field(
        "esriTS", description="Specifies the type of symbol used."
    )
    vertical_alignment: VerticalAlignment | None = Field(
        None,
        alias="verticalAlignment",
        description="One of the following string values representing the vertical alignment of the text.",
    )
    xoffset: float | int | None = Field(
        None, description="Numeric value indicating the offset on the x-axis in points."
    )
    yoffset: float | int | None = Field(
        None, description="Numeric value indicating the offset on the y-axis in points."
    )

    @field_validator("angle", mode="before")
    def check_angle(cls, v):
        if v:
            # constrain angle to 360 degrees; if negative, make it positive
            v = v % 360
        return v
