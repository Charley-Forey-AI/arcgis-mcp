from __future__ import annotations
from __future__ import absolute_import
from __future__ import print_function
import json
from arcgis.geometry import Polygon, Polyline, Point, MultiPoint


########################################################################
class StatisticFilter(object):
    """
    The definitions for one or more field-based statistics to be calculated
    during a query. To use, initialize an empty object, then populate
    it using the :meth:`~arcgis._impl.common._filters.StatisticFilter.add` method.
    """

    _json = None
    _array = []

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        pass

    # ----------------------------------------------------------------------
    def add(self, statisticType, onStatisticField, outStatisticFieldName=None):
        """
        Adds the attribute field-based statistical metrics to calculate during
        a query.

        =======================     ============================================
        **Parameter**               **Description**
        -----------------------     --------------------------------------------
        statisticType               Required String. The stastic to calculate.
                                    Options:

                                    * *count*
                                    * *sum*
                                    * *min*
                                    * *max*
                                    * *avg*
                                    * *stddev*
                                    * *var*
        -----------------------     --------------------------------------------
        onStatisticField            Required String. The name of the attribute
                                    field whose values will be used to calculate
                                    statistics.
        -----------------------     --------------------------------------------
        outStatisticFieldName       Optional String. The name of the field in the
                                    output table that will contain the result
                                    value.
        =======================     ============================================

        .. code-block:: python

            # Usage example

            >>> from arcgis.gis import GIS, StatisticFilter
            >>> gis = GIS(profile='your_organization_profile')

            >>> hospitals_item = gis.content.get("<item_id>")
            >>> hosp_flyr = flyr_item.layers[0]

            >>> sfilter = StatisticFilter()
            >>> sfilter.add(
            >>>         statisticType="avg",
            >>>         onStatisticField="LICENSEDBE",
            >>>         outStatisticFieldName="avg_HospitalBeds"
            >>> )
            >>> sfilter.add(
            >>>         statisticType="sum",
            >>>         onStatisticField="MEDICAREBE",
            >>>         outStatisticFieldName="sum_Medcare_Beds"
            >>> )

            >>> stats = hosp_flyr.query(
            >>>                where="1=1",
            >>>                statistic_filter=sfilter,
            >>>                group_by_fields_for_statistics="TYPE"
            >>>         )

            >>> stats.features

            [
              {"attributes": {"avg_HospitalBeds": 214.32, "sum_Medcare_Beds": 8306, "TYPE": "GENERAL HOSPITAL"}},
              {"attributes": {"avg_HospitalBeds": 28.6, "sum_Medcare_Beds": 143, "TYPE": "CRITICAL ACCESS HOSPITAL"}},
              {"attributes": {"avg_HospitalBeds": 95.66, "sum_Medcare_Beds": 976, "TYPE": "PSYCHIATRIC HOSPITAL"}},
            ]
        """
        val = {
            "statisticType": statisticType,
            "onStatisticField": onStatisticField,
            "outStatisticFieldName": outStatisticFieldName,
        }
        if outStatisticFieldName is None:
            del val["outStatisticFieldName"]
        self._array.append(val)

    # ----------------------------------------------------------------------
    def remove(self, index):
        """removes the filter by index"""
        self._array.remove(index)

    # ----------------------------------------------------------------------
    def clear(self):
        """removes all the filters"""
        self._array = []

    # ----------------------------------------------------------------------
    @property
    def filter(self):
        """
        Returns a list of dictionaries with key/value pairs reflecting the
        arguments used with the :meth:`~arcgis._impl.common._filters.StatisticFilter.add`
        method to populate the :class:`~arcgis._impl.common._filters.StatisticFilter`.

        .. code-block:: python

            # Usage Example: Return from code used in the StatisticFilter.add() snippet

            >>> sfilter.filter

            [{'statisticType': 'avg', 'onStatisticField': 'LICENSEDBE', 'outStatisticFieldName': 'avg_HospitalBeds'},
             {'statisticType': 'sum', 'onStatisticField': 'MEDICAREBE', 'outStatisticFieldName': 'sum_Medcare_Beds'}]
        """
        return self._array


########################################################################
class LayerDefinitionFilter(object):
    """
    Allows you to filter the features of individual layers in the
    query by specifying definition expressions for those layers. A
    definition expression for a layer that is published with the
    service will always be honored.
    """

    _ids = []
    _filterTemplate = {"layerId": "", "where": "", "outFields": "*"}
    _filter = []

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        pass

    # ----------------------------------------------------------------------
    def addFilter(self, layer_id, where=None, outFields="*"):
        """adds a layer definition filter"""
        import copy

        f = copy.deepcopy(self._filterTemplate)
        f["layerId"] = layer_id
        f["outFields"] = outFields
        if where is not None:
            f["where"] = where
        if f not in self._filter:
            self._filter.append(f)

    # ----------------------------------------------------------------------
    def removeFilter(self, filter_index):
        """removes a layer filter based on position in filter list"""
        f = self._filter[filter_index]
        self._filter.remove(f)

    # ----------------------------------------------------------------------
    def removeAll(self):
        """removes all items from the filter"""
        self._filter = []

    # ----------------------------------------------------------------------
    @property
    def filter(self):
        """returns the filter object as a list of layer defs"""
        return self._filter


########################################################################
class GeometryFilter(object):
    """creates a geometry filter for queries
     Inputs:
        geomObject - a common.Geometry or arcpy.Geometry object
        spatialFilter - The spatial relationship to be applied on the
                        input geometry while performing the query. The
                        supported spatial relationships include
                        intersects, contains, envelope intersects,
                        within, etc. The default spatial relationship
                        is intersects (esriSpatialRelIntersects).
        bufferDistance - if filter type esriSpatialRelWithin is selected
                         and the service supports that select type, then
                         the geometry will be buffered at a given.
                         Can be of type integer or float.
        units - the value the distance units represents. Valid values
                are: "esriSRUnit_Meter", "esriSRUnit_StatuteMile",
                     "esriSRUnit_Foot", "esriSRUnit_Kilometer",
                      "esriSRUnit_NauticalMile", and
                      "esriSRUnit_USNauticalMile"
    Raises:
       AttributeError for invalid inputs
    """

    _allowedFilters = [
        "esriSpatialRelIntersects",
        "esriSpatialRelContains",
        "esriSpatialRelCrosses",
        "esriSpatialRelEnvelopeIntersects",
        "esriSpatialRelIndexIntersects",
        "esriSpatialRelOverlaps",
        "esriSpatialRelTouches",
        "esriSpatialRelWithin",
    ]
    _geomObject = None
    _spatialAction = None
    _geomType = None
    _spatialReference = None
    _buffer = None
    _units = None
    _allowed_units = [
        "esriSRUnit_Meter",
        "esriSRUnit_StatuteMile",
        "esriSRUnit_Foot",
        "esriSRUnit_Kilometer",
        "esriSRUnit_NauticalMile",
        "esriSRUnit_USNauticalMile",
    ]

    # ----------------------------------------------------------------------
    def __init__(
        self,
        geomObject,
        spatialFilter="esriSpatialRelIntersects",
        bufferDistance=None,
        units="esriSRUnit_Meter",
    ):
        """Constructor"""
        self.geometry = geomObject
        if spatialFilter in self._allowedFilters:
            self._spatialAction = spatialFilter
            self._spatialReference = self.geometry.spatialReference
        else:
            raise AttributeError(
                "geomObject must be a geometry object and "
                + "spatialFilter must be of value: "
                + "%s" % ", ".join(self._allowedFilters)
            )
        if (
            not bufferDistance is None
            and isinstance(bufferDistance, (int, float))
            and not units is None
            and units.lower() in [f.lower() for f in self._allowed_units]
        ):
            self._buffer = bufferDistance
            self._units = units

    # ----------------------------------------------------------------------
    @property
    def spatialRelation(self):
        """gets the filter type"""
        return self._spatialAction

    # ----------------------------------------------------------------------
    @spatialRelation.setter
    def spatialRelation(self, value):
        if value.lower() in [x.lower() for x in self._allowedFilters]:
            self._spatialAction = value
        else:
            raise AttributeError(
                "spatialRelation must be values of "
                + "%s" % ", ".join(self._allowedFilters)
            )

    # ----------------------------------------------------------------------
    @property
    def geometryType(self):
        """returns the geometry type"""
        return self._geomObject.type

    # ----------------------------------------------------------------------
    @property
    def geometry(self):
        """gets the geometry object used by the filter"""
        return self._geomObject

    # ----------------------------------------------------------------------
    @geometry.setter
    def geometry(self, geometry):
        """sets the geometry value"""

        if isinstance(geometry, (Polygon, Point, Polyline, MultiPoint)):
            self._geomObject = geometry
            self._geomType = geometry.type
        else:
            raise AttributeError("geometry must be a common.Geometry type.")

    # ----------------------------------------------------------------------
    @property
    def filter(self):
        """returns the key/value pair of a geometry filter"""

        val = {
            "geometryType": self.geometryType,
            "geometry": json.dumps(self._geomObject.asDictionary),
            "spatialRel": self.spatialRelation,
            "inSR": self._geomObject.spatialReference["wkid"],
        }
        if self._buffer is not None and self._units is not None:
            val["buffer"] = self._buffer
            val["units"] = self._units
        return val


########################################################################
class TimeFilter(object):
    """Implements the time filter"""

    _startTime = None
    _endTime = None

    # ----------------------------------------------------------------------
    def __init__(self, start_time, time_zone="UTC", end_time=None):
        """Constructor"""
        self._startTime = start_time
        self._endTime = end_time
        self._tz = time_zone

    # ----------------------------------------------------------------------
    @property
    def filter(self):
        if not self._endTime is None:
            val = "%s, %s" % (self._startTime, self._endTime)
            return val
        else:
            return "%s" % self._startTime
