from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from arcgis.geometry import (
    Point,
    Polygon,
    Polyline,
    Envelope,
    MultiPoint,
    SpatialReference,
)


class SpatialRelationship(Enum):
    """
    ==================  ===============================================================================
    **Parameter**        **Description**
    ------------------  -------------------------------------------------------------------------------
    INTERSECTS          Query Geometry Intersects Target Geometry.
    ------------------  -------------------------------------------------------------------------------
    ENVELOPEINTERSECTS  Envelope of Query Geometry Intersects Envelope of Target Geometry.
    ------------------  -------------------------------------------------------------------------------
    INDEXINTERSECTS     Query Geometry Intersects Index entry for Target Geometry (Primary Index Filter).
    ------------------  -------------------------------------------------------------------------------
    TOUCHES             Query Geometry Touches Target Geometry.
    ------------------  -------------------------------------------------------------------------------
    OVERLAPS            Query Geometry Overlaps Target Geometry.
    ------------------  -------------------------------------------------------------------------------
    CROSSES             Query Geometry Crosses Target Geometry.
    ------------------  -------------------------------------------------------------------------------
    WITHIN              Query Geometry is Within Target Geometry.
    ------------------  -------------------------------------------------------------------------------
    CONTAINS            Query Geometry Contains Target Geometry.
    ==================  ===============================================================================
    """

    INTERSECTS = "esriSpatialRelIntersects"
    CONTAINS = "esriSpatialRelContains"
    CROSSES = "esriSpatialRelCrosses"
    ENVELOPEINTERSECTS = "esriSpatialRelEnvelopeIntersects"
    INDEXINTERSECTS = "esriSpatialRelIndexIntersects"
    OVERLAPS = "esriSpatialRelOverlaps"
    TOUCHES = "esriSpatialRelTouches"
    WITHIN = "esriSpatialRelWithin"


@dataclass
class SpatialFilter:
    """Creates a spatial filter that can be used in query and create view
    operations. A *SpatialFilter* object is also returned from the
    :attr:`~arcgis.gis._impl._dataclasses.ViewLayerDefParameter.spatial_filter`
    property of :class:`~arcgis.gis._impl._dataclasses.ViewLayerDefParameter`
    objects.

    .. code-block:: python

        #Usage Example 1: Get SpatialFilter from view
        >>> gis = GIS(profile="your_organization_profile")
        >>> view_item = gis.content.get("<view_item_id>")

        >>> vw_mgr = view_item.view_manager
        >>> vw_def = vw_mgr.get_definitions(view_item)[0]

        >>> vw_def.spatial_filter
        SpatialFilter()

        >>> vw_def.spatial_filter.sr
        {'latestWkid': 3857, 'wkid': 102100}

        >>> vw_def.spatial_filter.as_json()
            {'geometry': {'rings': [[[-9982417.919074, 4370975.02546],
                            [-9982417.919074, 4769966.75848],
                            [-8954750.737665, 4769966.75848],
                            [-8954750.737665, 4370975.02546],
                            [-9982417.919074, 4370975.02546]]],
                          'spatialReference': {'latestWkid': 3857, 'wkid': 102100}},
             'geometryType': 'esriGeometryPolygon',
             'spatialRel': 'esriSpatialRelIntersects',
             'inSR': {'latestWkid': 3857, 'wkid': 102100}}

        #Usage Example 2: Initialize a SpatialFilter
        >>> from arcgis.gis import SpatialFilter, SpatialRelationship
        >>> from arcgis.geomtry import Polygon, SpatialReference

        >>> filter_poly = Polygon(
                {'geometry': {'rings': [
                                        [[-12942501.854427, 5403724.074921],
                                         [-12942126.536675, 5409036.448386],
                                         [-12933473.476917, 5408301.189862],
                                         [-12933489.152479, 5404117.456892],
                                         [-12942501.854427, 5403724.074921]]
                                       ],
                              'spatialReference': {'latestWkid': 3857, 'wkid': 102100}
                              }
                }
        )

        >>> spatial_filt = SpatialFilter(
                geometry= filter_poly,
                spatial_rel= SpatialRelationship.INTERSECTS,
                sr=SpatialReference({'latestWkid': 3857, 'wkid': 102100})
             )
    """

    geometry: Point | Polygon | Polyline | Envelope | MultiPoint
    """
    An instance of an *arcgis.geometry.Geometry* class. Can be one of:
    
    * :class:`~arcgis.geometry.Point`
    * :class:`~arcgis.geometry.Polygon`
    * :class:`~arcgis.geometry.Polyline`
    * :class:`~arcgis.geometry.Envelope`
    * :class:`~arcgis.geometry.MultiPoint`    
    """

    spatial_rel: SpatialRelationship = SpatialRelationship.INTERSECTS
    """
    A member of the :class:`~arcgis.gis._impl._dataclasses.SpatialRelationship`
    enumeration.
    """
    sr: SpatialReference | None = None
    """A :class:`~arcgis.geometry.SpatialReference` object
    """

    def as_json(self):
        """Converts the *SpatialFilter* to a Python dictionary."""
        gt = {
            "point": "Point",
            "multipoint": "Multipoint",
            "polygon": "Polygon",
            "polyline": "Polyline",
            "envelope": "Envelope",
        }
        spatial_filter = {
            "geometry": self.geometry,
            "geometryType": "esriGeometry" + gt[str(self.geometry.type).lower()],
            "spatialRel": self.spatial_rel.value,
        }

        if self.sr is None:
            if "spatialReference" in self.geometry:
                sr = self.geometry["spatialReference"]

        else:
            spatial_filter["inSR"] = self.sr
        return spatial_filter
