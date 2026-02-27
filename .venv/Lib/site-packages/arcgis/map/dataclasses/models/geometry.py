from __future__ import annotations  # Enables postponed evaluation of type hints
from .base_model import BaseModel, common_config
from pydantic import Field
from typing import Any, Literal


class SpatialReference(BaseModel):
    """
    The spatialReference object is located at the top level of the web map JSON hierarchy. In addition to this, it is also available within the operationalLayer and basemap objects.

    Many predefined spatial references have already been defined and are available for use. A spatial reference can be defined using a well-known ID (WKID) or well-known text (WKT). The default tolerance and resolution values for the associated coordinate system are used. It is required to have this property saved within the web map.
    """

    model_config = common_config
    false_m: float | int | None = Field(
        None,
        alias="falseM",
        description="(Optional) The false origin of coordinate M values. The default is determined by the spatial reference.",
    )
    false_x: float | int | None = Field(
        None,
        alias="falseX",
        description="(Optional) The false origin of coordinate X values. The default is determined by the spatial reference.",
    )
    false_y: float | int | None = Field(
        None,
        alias="falseY",
        description="(Optional) The false origin of coordinate Y values. The default is determined by the spatial reference.",
    )
    false_z: float | int | None = Field(
        None,
        alias="falseZ",
        description="(Optional) The false origin of coordinate Z values. The default is determined by the spatial reference.",
    )
    latest_wkid: int | None = Field(
        None,
        alias="latestWkid",
        description="(Optional) Identifies the current wkid value associated with the same spatial reference. For example a WKID of '102100' (Web Mercator) has a latestWKid of '3857'.",
    )
    m_tolerance: float | int | None = Field(
        None,
        description="(Optional) The tolerance value reflects the accuracy of coordinates. If one coordinate is within the tolerance value of another, they are interpreted as being at the same location. This value is used in relational and topological operations when determining whether two points are close enough to be given the same coordinate value or they are far enough apart to each have their own coordinate value. The default tolerance is set to 0.001 meters or its equivalent in map units.",
    )
    m_units: float | int | None = Field(
        None,
        alias="mUnits",
        description="(Optional) The number of distinct values that can be represented per one unit of floating point coordinates. The resolution for M is 1/mUnits. The default resolution is set to 0.0001 meters or its equivalent in map units.",
    )
    wkid: int | None = Field(
        None,
        description="The well-known ID (WKID) of the coordinate system.",
    )
    wkt: str | None = Field(
        None,
        description="The well-known text (WKT) of the coordinate system.",
    )
    wkt2: str | None = Field(
        None,
        description="The well-known text of the coordinate system as defined by OGC standard for well-known text strings.",
    )
    xy_tolerance: float | int | None = Field(
        None,
        alias="xyTolerance",
        description="(Optional) The tolerance value reflects the accuracy of coordinates. If one coordinate is within the tolerance value of another, they are interpreted as being at the same location. This value is used in relational and topological operations when determining whether two points are close enough to be given the same coordinate value or they are far enough apart to each have their own coordinate value. The default tolerance is set to 0.001 meters or its equivalent in map units.",
    )
    xy_units: float | int | None = Field(
        None,
        alias="xyUnits",
        description="(Optional) The number of distinct values that can be represented per one unit of floating point coordinates. The resolution for X and Y is 1/xyUnits. The default resolution is set to 0.0001 meters or its equivalent in map units.",
    )
    z_tolerance: float | int | None = Field(
        None,
        alias="zTolerance",
        description="(Optional) The tolerance value reflects the accuracy of coordinates. If one coordinate is within the tolerance value of another, they are interpreted as being at the same location. This value is used in relational and topological operations when determining whether two points are close enough to be given the same coordinate value or they are far enough apart to each have their own coordinate value. The default tolerance is set to 0.001 meters or its equivalent in map units.",
    )
    z_units: float | int | None = Field(
        None,
        alias="zUnits",
        description="(Optional) The number of distinct values that can be represented per one unit of floating point coordinates. The resolution for Z is 1/zUnits. The default resolution is set to 0.0001 meters or its equivalent in map units.",
    )


class Extent(BaseModel):
    """
    This object defines the bounding geometry given the lower-left and upper-right corners of the bounding box. A [spatial reference](spatialReference.md) is also required.
    """

    model_config = common_config
    spatial_reference: SpatialReference | None = Field(
        SpatialReference(wkid=102100, latestWkid=3857),
        alias="spatialReference",
        description="An object used to specify the spatial reference of the given geometry.",
        title="spatialReference",
    )
    xmax: float | None = Field(
        None,
        description="A numeric value indicating the top-right X-coordinate of an extent envelope.",
    )
    xmin: float | Literal["NaN"] | None = Field(
        None,
        description="A numeric value indicating the bottom-left X-coordinate of an extent envelope.",
    )
    ymax: float | None = Field(
        None,
        description="A numeric value indicating the top-right Y-coordinate of an extent envelope.",
    )
    ymin: float | None = Field(
        None,
        description="A numeric value indicating the bottom-left Y-coordinate of an extent envelope.",
    )


class PointGeometry(BaseModel):
    """
    Defines the JSON formats of the point and spatial reference objects.
    """

    model_config = common_config
    m: float | None = Field(
        None,
        description="M coordinate which contains measures used for linear referencing.",
    )
    spatial_reference: SpatialReference | None = Field(
        SpatialReference(wkid=102100, latestWkid=3857),
        alias="spatialReference",
        description="The spatial reference can be defined using a well-known ID (WKID) or well-known text (WKT).",
        title="spatialReference",
    )
    x: float | Literal["NaN"] | None = Field(
        ...,
        description="X coordinate which is measured along the east/west axis.",
    )
    y: float | None = Field(
        None,
        description="Y coordinate which is measured along the north/south axis.",
    )
    z: float | None = Field(
        None, description="Z coordinate which measures height or elevation."
    )


class PolygonGeometry(BaseModel):
    """
    A polygon contains an array of rings and a spatial reference.
    """

    model_config = common_config
    has_m: bool | None = Field(
        None,
        alias="hasM",
        description="Indicates whether the geometry contains M coordinate values.",
    )
    has_z: bool | None = Field(
        None,
        alias="hasZ",
        description="Indicates whether the geometry contains Z coordinate values.",
    )
    rings: list[list[list[Any]]] = Field(
        ...,
        description="Represents an array of points. Each point is an array of numbers.",
    )
    spatial_reference: SpatialReference | None = Field(
        SpatialReference(wkid=102100, latestWkid=3857),
        alias="spatialReference",
        description="The spatial reference can be defined using a well-known ID (WKID) or well-known text (WKT).",
        title="spatialReference",
    )


class MultipointGeometry(BaseModel):
    """
    Contains an array of points, along with a spatial reference field.
    """

    model_config = common_config
    has_m: bool | None = Field(
        None,
        alias="hasM",
        description="Indicates whether the geometry contains M coordinate values.",
    )
    has_z: bool | None = Field(
        None,
        alias="hasZ",
        description="Indicates whether the geometry contains Z coordinate values.",
    )
    points: list[list[float]] = Field(
        ..., description="An array that corresponds to 2D and 3D points."
    )
    spatial_reference: SpatialReference | None = Field(
        SpatialReference(wkid=102100, latestWkid=3857),
        alias="spatialReference",
        description="The spatial reference can be defined using a well-known ID (WKID) or well-known text (WKT).",
        title="spatialReference",
    )


class PolylineGeometry(BaseModel):
    """
    Contains an array of paths and a spatialReference.
    """

    model_config = common_config
    has_m: bool | None = Field(
        None,
        alias="hasM",
        description="Indicates whether the geometry contains M coordinate values.",
    )
    has_z: bool | None = Field(
        None,
        alias="hasZ",
        description="Indicates whether the geometry contains Z coordinate values.",
    )
    paths: list[list[list[Any]]] = Field(
        ..., description="Three nested arrays which correspond to a polyline"
    )
    spatial_reference: SpatialReference | None = Field(
        SpatialReference(wkid=102100, latestWkid=3857),
        alias="spatialReference",
        description="The spatial reference can be defined using a well-known ID (WKID) or well-known text (WKT).",
        title="spatialReference",
    )
