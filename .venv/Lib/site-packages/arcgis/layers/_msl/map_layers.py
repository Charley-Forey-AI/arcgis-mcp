from __future__ import annotations
from string import digits
from functools import lru_cache
import requests
from typing import Any, Optional, Union

from arcgis._impl.common import _query
from arcgis._impl.common._filters import (
    StatisticFilter,
    TimeFilter,
    GeometryFilter,
)
from arcgis._impl.common._mixins import PropertyMap

from arcgis._impl.common._output_to_file import handle_response
from arcgis.gis import Item, Layer
from arcgis.auth.tools import LazyLoader
from arcgis.gis._impl._util import _get_item_url
from arcgis._impl.common._utils import _validate_url
from arcgis._impl.common._deprecate import deprecated

_dt = LazyLoader("_dt.datetime")
os = LazyLoader("os")
pd = LazyLoader("pandas")
json = LazyLoader("json")
tempfile = LazyLoader("tempfile")
time = LazyLoader("time")
arcgis = LazyLoader("arcgis")
_geometry = LazyLoader("arcgis.geometry")
_gis = LazyLoader("arcgis.gis")
_layers = LazyLoader("arcgis.layers")
_features = LazyLoader("arcgis.features")


###########################################################################
class MapFeatureLayer(Layer):
    """
    The ``MapFeatureLayer`` class represents Map Feature Layers.
    Map Feature Layers can be added to and visualized using maps.

    Map Feature Layers are created by publishing feature data to a :class:`~arcgis.gis.GIS`, and are exposed as a
    broader resource (:class:`~arcgis.gis.Item`) in the ``GIS``.
    `MapFeatureLayer` objects can be obtained through the layers attribute on map image service Items in the ``GIS``.
    """

    _metadatamanager = None
    _renderer = None
    _storage = None
    _dynamic_layer = None
    _attachments = None
    _time_filter = None

    # ----------------------------------------------------------------------
    def __init__(
        self,
        url: str,
        gis: _gis.GIS | None = None,
        container: MapImageLayer | None = None,
        dynamic_layer: dict | None = None,
        time_filter: _dt.datetime | list[_dt.datetime] | list[str] | None = None,
    ):
        """
        Constructs a map feature layer given a feature layer URL
        :param url: layer url
        :param gis: optional, the GIS that this layer belongs to. Required for secure map feature layers.
        :param container: optional, the MapImageLayer to which this layer belongs
        :param dynamic_layer: optional dictionary. If the layer is given a dynamic layer definition, this will be added to functions.
        """
        if gis is None:
            gis: _gis.GIS = arcgis.env.active_gis
            if gis is None:
                gis = arcgis.gis.GIS()
        if hasattr(gis, "session"):

            self._session = gis.session
        elif hasattr(gis, "_session"):
            self._session = gis._session

        if str(url).lower().endswith("/"):
            url = url[:-1]
        super(MapFeatureLayer, self).__init__(url, gis)

        self._attachments = None
        self._dynamic_layer = dynamic_layer
        self._time_filter = time_filter
        self._storage = container

    # ----------------------------------------------------------------------
    @property
    def _lyr_dict(self) -> dict:
        url = self.url

        lyr_dict = {"type": "FeatureLayer", "url": url}
        if self._token is not None:
            lyr_dict["serviceToken"] = self._token

        if self.filter is not None:
            lyr_dict["filter"] = self.filter
        if self._time_filter is not None:
            lyr_dict["time"] = self._time_filter
        return lyr_dict

    # ----------------------------------------------------------------------
    @property
    def _lyr_json(self) -> dict:
        url = self.url
        if self._token is not None:
            url += "?token=" + self._token

        lyr_dict = {"type": "FeatureLayer", "url": url}

        if self.filter is not None:
            lyr_dict["options"] = json.dumps({"definition_expression": self.filter})
        if self._time_filter is not None:
            lyr_dict["time"] = self._time_filter
        return lyr_dict

    @deprecated(
        deprecated_in="2.4.1",
        details="Use the attachments property instead.",
    )
    @property
    def attachements(self):
        return self.attachments

    # ----------------------------------------------------------------------
    @property
    @lru_cache(maxsize=10)
    def attachments(self) -> _features.managers.AttachmentManager:
        """
        The ``attachments`` property provides a manager to work with attachments if the ``MapFeatureLayer``
        supports this functionality.
        """
        if (
            "supportsQueryAttachments" in self.properties
            and self.properties["supportsQueryAttachments"]
            and self._attachments is None
        ):
            self._attachments = _features.managers.AttachmentManager(self)
        return self._attachments

    # ----------------------------------------------------------------------
    @property
    def time_filter(self) -> str:
        """
        Starting at Enterprise 10.7.1+, instead of querying time-enabled
        map service layers or time-enabled feature service layers, a
        time filter can be set using the ``time_filter`` property.
        Time can be filtered as Python `_dt.datetime <https://docs.python.org/3/library/_dt.datetime.html#_dt.datetime-objects>`_,
        objects or strings representing Unix epoch values in milliseconds.
        An extent can be specified by separating the start and stop values
        comma.

        .. code-block:: python

            >>> import _dt.datetime as dt

            >>> map_feature_lyr.time_filter = [dt._dt.datetime(2021, 1, 1), dt._dt.datetime(2022, 1, 10)]

        """
        return self._time_filter

    # ----------------------------------------------------------------------
    @time_filter.setter
    def time_filter(self, value: Union[_dt.datetime, list[_dt.datetime], list[str]]):
        """
        See main ``time_filter`` property docstring
        """
        v = []
        if isinstance(value, _dt._dt.datetime):
            self._time_filter = f"{int(value.timestamp() * 1000)}"  # means single time
        elif isinstance(value, (tuple, list)):
            for idx, d in enumerate(value):
                if idx > 1:
                    break
                if isinstance(d, _dt._dt.datetime):
                    v.append(f"{int(value.timestamp() * 1000)}")
                elif isinstance(d, str):
                    v.append(d)
                elif d is None:
                    v.append("null")
            self._time_filter = ",".join(v)
        elif isinstance(value, str):
            self._time_filter = value
        elif value is None:
            self._time_filter = None
        else:
            raise Exception("Invalid _dt.datetime filter")

    # ----------------------------------------------------------------------
    @property
    def renderer(self) -> dict | None:
        """
        Get/Set the Renderer of the Map Feature Layer.

        .. note::
            The ``renderer`` property overrides the default symbology when displaying it on a
            :class:`~arcgis.map.Map`.

        :return:
            A ``dict`` object used to update and alter JSON

        """

        if self._renderer is None and "drawingInfo" in self.properties:
            self._renderer = dict(self.properties.drawingInfo.renderer)
        return self._renderer

    # ----------------------------------------------------------------------
    @renderer.setter
    def renderer(self, value: dict | None):
        if isinstance(value, (dict, PropertyMap)):
            self._renderer = dict(value)
        elif value is None:
            self._renderer = None
        elif not isinstance(value, dict):
            raise ValueError("Invalid renderer type.")
        self._refresh = value

    # ----------------------------------------------------------------------
    @classmethod
    def fromitem(cls, item: Item, layer_id: int = 0) -> MapImageLayer:
        """
        The ``fromitem`` method creates a :class:`~arcgis.layers.MapFeatureLayer` from a GIS :class:`~arcgis.gis.Item`.


        ====================================     ====================================================================
        **Parameter**                             **Description**
        ------------------------------------     --------------------------------------------------------------------
        item                                     Required :class:`~arcgis.gis.Item` object. The type of item should be
                                                 a :class:`~arcgis.layers.MapServiceLayer` object.
        ------------------------------------     --------------------------------------------------------------------
        layer_id                                 Optional integer. The id of the layer in the Map Service's Layer.
                                                 The default is 0.
        ====================================     ====================================================================

        :return:
            A :class:`~arcgis.layers.MapFeatureLayer` object

        .. code-block:: python

            # USAGE EXAMPLE

            >>> from arcgis.layers import MapImageLayer, MapFeatureLayer
            >>> from arcgis.gis import GIS

            # connect to your GIS and get the web map item
            >>> gis = GIS(url, username, password)

            >>> map_image_item = gis.content.get("2aaddab96684405880d27f5261125061")
            >>> map_feature_layer = MapFeatureLayer.fromitem(item = map_image_item,
                                                             layer_id = 2)
            >>> print(f"{map_feature_layer.properties.name:30}{type(map_feature_layer)}")
            <State Boundaries              <class 'arcgis.layers._msl.layer.MapFeatureLayer'>>

        """

        return MapImageLayer.fromitem(item).layers[layer_id]

    # ----------------------------------------------------------------------
    @property
    def container(self) -> MapImageLayer:
        """
        The ``container`` property represents the :class:`~arcgis.layers.MapImageLayer` to which this layer belongs.
        """
        if self._storage is None:
            self._storage = MapImageLayer(
                url=self._url.rstrip(digits)[:-1], gis=self._gis
            )
        return self._storage

    # ----------------------------------------------------------------------
    def export_attachments(
        self, output_folder: str, label_field: str | None = None
    ) -> str:
        """
        The ``export_attachments`` method exports attachments from the map feature layer in ``Imagenet`` format using
        the ``output_label_field``.

        ====================================     ====================================================================
        **Parameter**                             **Description**
        ------------------------------------     --------------------------------------------------------------------
        output_folder                            Required String. Output folder path where the attachments will be stored.
        ------------------------------------     --------------------------------------------------------------------
        label_field                              Optional. Field which contains the label/category of each feature.
                                                 If None, a default folder is created.
        ====================================     ====================================================================

        :return:
            A path to the exported attachments
        """
        import pandas
        import urllib
        import hashlib

        if not self.properties["hasAttachments"]:
            raise Exception("Map Feature Layer doesn't have any attachments.")

        if not os.path.exists(output_folder):
            raise Exception("Invalid output folder path.")

        object_attachments_mapping = {}

        object_id_field = self.properties["objectIdField"]

        dataframe_merged = pandas.merge(
            self.query().sdf,
            self._attachments.search(as_df=True),
            left_on=object_id_field,
            right_on="PARENTOBJECTID",
        )

        token = self._con.token

        internal_folder = os.path.join(output_folder, "images")
        if not os.path.exists(internal_folder):
            os.mkdir(internal_folder)

        folder = "images"
        for row in dataframe_merged.iterrows():
            if label_field is not None:
                folder = row[1][label_field]

            path = os.path.join(internal_folder, folder)

            if not os.path.exists(path):
                os.mkdir(path)

            if token is not None:
                url = "{}/{}/attachments/{}?token={}".format(
                    self.url,
                    row[1][object_id_field],
                    row[1]["ID"],
                    self._con.token,
                )
            else:
                url = "{}/{}/attachments/{}".format(
                    self.url, row[1][object_id_field], row[1]["ID"]
                )

            if not object_attachments_mapping.get(row[1][object_id_field]):
                object_attachments_mapping[row[1][object_id_field]] = []

            content = urllib.request.urlopen(url).read()

            md5_hash = hashlib.md5(content).hexdigest()
            attachment_path = os.path.join(path, f"{md5_hash}.jpg")

            object_attachments_mapping[row[1][object_id_field]].append(
                os.path.join("images", os.path.join(folder, f"{md5_hash}.jpg"))
            )

            if os.path.exists(attachment_path):
                continue
            file = open(attachment_path, "wb")
            file.write(content)
            file.close()

        mapping_path = os.path.join(output_folder, "mapping.txt")
        file = open(mapping_path, "w")
        file.write(json.dumps(object_attachments_mapping))
        file.close()

    # ----------------------------------------------------------------------
    def generate_renderer(
        self, definition: dict[str, Any], where: str | None = None
    ) -> dict[str, Any]:
        """
        The ``generate_renderer`` operation groups data using the supplied definition
        (classification definition) and an optional where clause. The
        result is a renderer object. Use ``baseSymbol`` and ``colorRamp`` to define
        the symbols assigned to each class.

        .. note::
            If the operation is performed
            on a table, the result is a renderer object containing the data
            classes and no symbols.

        =================     ====================================================================
        **Parameter**          **Description**
        -----------------     --------------------------------------------------------------------
        definition            Required dict. The definition using the renderer that is generated.
                              Use either class breaks or unique value classification definitions.
                              See the
                              `classification definitions <https://resources.arcgis.com/en/help/rest/apiref/ms_classification.html>`_
                              page in the ArcGIS REST API documentation for more information.
        -----------------     --------------------------------------------------------------------
        where                 Optional string. A where clause for which the data needs to be
                              classified. Any legal SQL where clause operating on the fields in
                              the dynamic layer/table is allowed.
        =================     ====================================================================

        :return: dictionary

        """
        if self._dynamic_layer:
            url = "%s/generateRenderer" % self._url.split("?")[0]
        else:
            url = "%s/generateRenderer" % self._url
        params = {"f": "json", "classificationDef": definition}
        if where:
            params["where"] = where
        if self._dynamic_layer is not None:
            params["layer"] = self._dynamic_layer
        resp: requests.Response = self._session.post(url=url, data=params)
        resp.raise_for_status()
        return resp.json()

    # ----------------------------------------------------------------------
    def _add_attachment(self, oid, file_path):
        """
        Adds an attachment to a feature service

        =================     ====================================================================
        **Parameter**          **Description**
        -----------------     --------------------------------------------------------------------
        oid                   Required string/integer. OBJECTID value to add attachment to.
        -----------------     --------------------------------------------------------------------
        file_path             Required string. Location of the file to attach.
        =================     ====================================================================

        :return: dictionary

        """
        if (os.path.getsize(file_path) >> 20) <= 9:
            params = {"f": "json"}
            if self._dynamic_layer:
                attach_url = self._url.split("?")[0] + "/%s/addAttachment" % oid
                params["layer"] = self._dynamic_layer
            else:
                attach_url = self._url + "/%s/addAttachment" % oid
            files = {"attachment": file_path}
            resp: requests.Response = self._session.post(
                url=attach_url, data=params, files=files
            )
            resp.raise_for_status()
            return resp.json()
        else:
            params = {"f": "json"}
            container = self.container
            itemid = container.upload(file_path)
            if self._dynamic_layer:
                attach_url = self._url.split("?")[0] + "/%s/addAttachment" % oid
                params["layer"] = self._dynamic_layer
            else:
                attach_url = self._url + "/%s/addAttachment" % oid
            params["uploadId"] = itemid
            resp: requests.Response = self._session.post(url=attach_url, data=params)
            resp.raise_for_status()
            res = resp.json()
            if res["addAttachmentResult"]["success"] == True:
                container._delete_upload(itemid)
            return res

    # ----------------------------------------------------------------------
    def _delete_attachment(self, oid, attachment_id):
        """
        Removes an attachment from a feature service feature

        =================     ====================================================================
        **Parameter**          **Description**
        -----------------     --------------------------------------------------------------------
        oid                   Required string/integer. OBJECTID value to add attachment to.
        -----------------     --------------------------------------------------------------------
        attachment_id         Required integer. Id of the attachment to erase.
        =================     ====================================================================

        :return: dictionary
        """
        params = {"f": "json", "attachmentIds": "%s" % attachment_id}
        if self._dynamic_layer:
            url = self._url.split("?")[0] + "/%s/deleteAttachments" % oid
            params["layer"] = self._dynamic_layer
        else:
            url = self._url + "/%s/deleteAttachments" % oid
        resp: requests.Response = self._session.post(url=url, data=params)
        resp.raise_for_status()
        return resp.json()

    # ----------------------------------------------------------------------
    def _update_attachment(self, oid, attachment_id, file_path):
        """
        Updates an existing attachment with a new file

        =================     ====================================================================
        **Parameter**          **Description**
        -----------------     --------------------------------------------------------------------
        oid                   Required string/integer. OBJECTID value to add attachment to.
        -----------------     --------------------------------------------------------------------
        attachment_id         Required integer. Id of the attachment to erase.
        -----------------     --------------------------------------------------------------------
        file_path             Required string. Path to new attachment
        =================     ====================================================================

        :return: dictionary

        """
        params = {"f": "json", "attachmentId": "%s" % attachment_id}
        files = {"attachment": file_path}
        if self._dynamic_layer is not None:
            url = self.url.split("?")[0] + f"/{oid}/updateAttachment"
            params["layer"] = self._dynamic_layer
        else:
            url = self._url + f"/{oid}/updateAttachment"
        resp: requests.Response = self._session.post(url=url, data=params, files=files)
        resp.raise_for_status()
        return resp.json()

    # ----------------------------------------------------------------------
    def _list_attachments(self, oid):
        """list attachments for a given OBJECT ID"""

        params = {"f": "json"}
        if self._dynamic_layer is not None:
            url = self.url.split("?")[0] + "/%s/attachments" % oid
            params["layer"] = self._dynamic_layer
        else:
            url = self._url + "/%s/attachments" % oid
        resp: requests.Response = self._session.get(url=url, params=params)
        resp.raise_for_status()
        return resp.json()

    # ----------------------------------------------------------------------
    def get_unique_values(self, attribute: str, query_string: str = "1=1") -> list:
        """
        The ``get_unique_values`` method retrieves a list of unique values for a given attribute.

        ===============================     ====================================================================
        **Parameter**                        **Description**
        -------------------------------     --------------------------------------------------------------------
        attribute                           Required string. The map feature layer attribute to query.
        -------------------------------     --------------------------------------------------------------------
        query_string                        Optional string. SQL Query that will be used to filter attributes
                                            before unique values are returned.
        ===============================     ====================================================================

        :return:
            A List

        .. code-block:: python

            # USAGE EXAMPLE

            >>> from arcgis.layers import MapImageLayer, MapFeatureLayer
            >>> from arcgis.gis import GIS

            # connect to your GIS and get the web map item
            >>> gis = GIS(url, username, password)

            >>> map_image_item = gis.content.get("2aaddab96684405880d27f5261125061")
            >>> map_feature_layer = MapFeatureLayer.fromitem(item = map_image_item,
                                                             layer_id = 2)
            >>> unique_values = map_feature_layer.get_unique_values(attribute ="Name",
                                                    query_string ="name_2 like '%K%'")
            >>> type(unique_values)
            <List>
        """

        result = self.query(
            query_string,
            return_geometry=False,
            out_fields=attribute,
            return_distinct_values=True,
        )
        return [feature.attributes[attribute] for feature in result.features]

    # ----------------------------------------------------------------------
    def query(
        self,
        where: str = "1=1",
        text: str | None = None,  # new
        out_fields: str | list[str] = "*",
        time_filter: (
            list[int] | list[_dt.datetime] | dict[str, _dt.datetime] | None
        ) = None,
        geometry_filter: GeometryFilter | None = None,
        return_geometry: bool = True,
        return_count_only: bool = False,
        return_ids_only: bool = False,
        return_distinct_values: bool = False,
        return_extent_only: bool = False,
        group_by_fields_for_statistics: str | None = None,
        statistic_filter: StatisticFilter | None = None,
        result_offset: int | None = None,
        result_record_count: int | None = None,
        object_ids: str | None = None,
        distance: int | None = None,
        units: str | None = None,
        max_allowable_offset: float | None = None,
        out_sr: int | None = None,
        geometry_precision: int | None = None,
        gdb_version: str | None = None,
        order_by_fields: list[str] | str | None = None,
        out_statistics: list[dict[str, Any]] | None = None,
        return_z: bool = False,
        return_m: bool = False,
        multipatch_option=None,
        quantization_parameters: dict[str, Any] | None = None,
        return_centroid: bool = False,
        return_all_records: bool = True,
        result_type: str | None = None,
        historic_moment: int | _dt.datetime | None = None,
        sql_format: str | None = None,
        return_true_curves: bool = False,
        return_exceeded_limit_features: bool | None = None,
        as_df: bool = False,
        datum_transformation: int | dict[str, Any] | None = None,
        range_values: dict[str, Any] | None = None,
        parameter_values: dict[str, Any] | None = None,
        **kwargs,
    ) -> _features.FeatureSet | int | dict | pd.DataFrame:
        """
        The ``query`` method queries a map feature layer based on a sql statement.

        ===============================     ====================================================================
        **Parameter**                        **Description**
        -------------------------------     --------------------------------------------------------------------
        where                               Optional string. The default is 1=1. The selection sql statement.
        -------------------------------     --------------------------------------------------------------------
        text                                Optional String. A literal search text. If the layer has a display
                                            field associated with it, the server searches for this text in this
                                            field.
        -------------------------------     --------------------------------------------------------------------
        out_fields                          Optional List of field names to return. Field names can be specified
                                            either as a List of field names or as a comma separated string.
                                            The default is "*", which returns all the fields.
        -------------------------------     --------------------------------------------------------------------
        object_ids                          Optional string. The object IDs of this layer or table to be queried.
                                            The object ID values should be a comma-separated string.
        -------------------------------     --------------------------------------------------------------------
        distance                            Optional integer. The buffer distance for the input geometries.
                                            The distance unit is specified by units. For example, if the
                                            distance is 100, the query geometry is a point, units is set to
                                            meters, and all points within 100 meters of the point are returned.
        -------------------------------     --------------------------------------------------------------------
        units                               Optional string. The unit for calculating the buffer distance. If
                                            unit is not specified, the unit is derived from the geometry spatial
                                            reference. If the geometry spatial reference is not specified, the
                                            unit is derived from the feature service data spatial reference.
                                            This parameter only applies if `supportsQueryWithDistance` is
                                            `true`.

                                            Value options:
                                                    ``esriSRUnit_Meter`` | ``esriSRUnit_StatuteMile`` |
                                                    ``esriSRUnit_Foot`` | ``esriSRUnit_Kilometer`` |
                                                    ``esriSRUnit_NauticalMile`` | ``esriSRUnit_USNauticalMile``
        -------------------------------     --------------------------------------------------------------------
        time_filter                         Optional list of `startTime` and `endTime` values.
                                            :Syntax:

                                            .. code-block:: python

                                                >>> time_filter=[<startTime>, <endTime>]

                                            .. note::
                                                Specified as ``_dt.datetime.date``, ``_dt.datetime._dt.datetime`` or
                                                ``timestamp`` in milliseconds
        -------------------------------     --------------------------------------------------------------------
        geometry_filter                     Optional :class:`filter <arcgis.geometry.filters>` object. Allows for
                                            the information to be filtered on spatial relationship with another
                                            geometry.
        -------------------------------     --------------------------------------------------------------------
        max_allowable_offset                Optional float. This option can be used to specify the
                                            `max_allowable_offset` to be used for generalizing geometries
                                            returned by the query operation in the units of `out_sr`. If
                                            `out_sr`  is not specified, the value is in units of the spatial
                                            reference of the layer.
        -------------------------------     --------------------------------------------------------------------
        out_sr                              Optional Integer. The WKID for the spatial reference of the returned
                                            geometry.
        -------------------------------     --------------------------------------------------------------------
        geometry_precision                  Optional Integer. This option can be used to specify the number of
                                            decimal places in the response geometries returned by the query
                                            operation.
                                            This applies to X and Y values only (not m or z-values).
        -------------------------------     --------------------------------------------------------------------
        gdb_version                         Optional string. The geodatabase version to query. This parameter
                                            applies only if the `isDataVersioned` property of the layer is true.
                                            If not specified, the query will apply to the published map's
                                            version.
        -------------------------------     --------------------------------------------------------------------
        return_geometry                     Optional boolean. If `true`, geometry is returned with the query.
                                            Default is `true`.
        -------------------------------     --------------------------------------------------------------------
        return_distinct_values              Optional boolean.  If `True`, it returns distinct values based on
                                            fields specified in `out_fields`. This parameter applies only if the
                                            `supportsAdvancedQueries` property of the layer is true.
        -------------------------------     --------------------------------------------------------------------
        return_ids_only                     Optional boolean. Default is `False`.  If `True`, the response only
                                            includes an array of object IDs. Otherwise, the response is a
                                            :class:`~arcgis.features.FeatureSet`.
        -------------------------------     --------------------------------------------------------------------
        return_count_only                   Optional boolean. If `True`, the response only includes the count
                                            of features/records satisfying the query. Otherwise, the response is
                                            a :class:`~arcgis.features.FeatureSet`. The default is `False`. This
                                            option supersedes the `returns_ids_only` parameter. If
                                            ``returnCountOnly = True`` , the response will return both the count
                                            and the extent.
        -------------------------------     --------------------------------------------------------------------
        return_extent_only                  Optional boolean. If `True`, the response only includes the extent
                                            of the features satisfying the query. If `returnCountOnly=true`, the
                                            response will return both the count and the extent. The default is
                                            `False`. This parameter applies only if the
                                            `supportsReturningQueryExtent` property of the layer is `true`.
        -------------------------------     --------------------------------------------------------------------
        order_by_fields                     Optional string or list of strings. One or more field names by which to order the
                                            results. Use ``ASC`` or ``DESC`` for ascending
                                            or descending, respectively, following every field to be ordered:

                                            .. code-block:: python

                                                >>> order_by_fields = "STATE_NAME ASC, RACE DESC, GENDER ASC"

        -------------------------------     --------------------------------------------------------------------
        group_by_fields_for_statistics      Optional string. One or more field names on which to group results
                                            for calculating the statistics.

                                            .. code-block:: python

                                                >>> group_by_fields_for_statistics = "STATE_NAME, GENDER"
        -------------------------------     --------------------------------------------------------------------
        out_statistics                      Optional List. The definitions for one or more field-based
                                            statistics to be calculated.

                                            :Syntax:

                                            .. code-block:: python

                                                >>> out_statistics = [
                                                                        {
                                                                          "statisticType": "<count | sum | min | max | avg | stddev | var>",
                                                                          "onStatisticField": "Field1",
                                                                          "outStatisticFieldName": "Out_Field_Name1"
                                                                        },
                                                                        {
                                                                          "statisticType": "<count | sum | min | max | avg | stddev | var>",
                                                                          "onStatisticField": "Field2",
                                                                          "outStatisticFieldName": "Out_Field_Name2"
                                                                        }
                                                                     ]
        -------------------------------     --------------------------------------------------------------------
        return_z                            Optional boolean. If `True`, Z values are included in the results if
                                            the features have Z values. Otherwise, Z values are not returned.
                                            The default is `False`.
        -------------------------------     --------------------------------------------------------------------
        return_m                            Optional boolean. If `True`, M values are included in the results if
                                            the features have M values. Otherwise, M values are not returned.
                                            The default is `False`.
        -------------------------------     --------------------------------------------------------------------
        multipatch_option                   Optional x/y footprint. This option dictates how the geometry of
                                            a multipatch feature will be returned.
        -------------------------------     --------------------------------------------------------------------
        result_offset                       Optional integer. This option can be used for fetching query results
                                            by skipping the specified number of records and starting from the
                                            next record (that is, `resultOffset + ith` value). This option is
                                            ignored if `return_all_records` is `True` (i.e. by default).
                                            This parameter cannot be specified if the service does not support pagination.
        -------------------------------     --------------------------------------------------------------------
        result_record_count                 Optional integer. This option can be used for fetching query results
                                            up to the `result_record_count` specified. When `result_offset` is
                                            specified but this parameter is not, the map service defaults it to
                                            `max_record_count`. The maximum value for this parameter is the value
                                            of the layer's `maxRecordCount` property. This option is ignored if
                                            `return_all_records` is True (i.e. by default).
                                            This parameter cannot be specified if the service does not support pagination.
        -------------------------------     --------------------------------------------------------------------
        quantization_parameters             Optional dict. Used to project the geometry onto a virtual grid,
                                            likely representing pixels on the screen.
        -------------------------------     --------------------------------------------------------------------
        return_centroid                     Optional boolean. Used to return the geometry centroid associated
                                            with each feature returned. If `True`, the result includes the
                                            geometry centroid. The default is `False`.
        -------------------------------     --------------------------------------------------------------------
        return_all_records                  Optional boolean. When `True`, the query operation will call the
                                            service until all records that satisfy the `where_clause` are
                                            returned.

                                            .. note::
                                                `result_offset` and `result_record_count` will be
                                                ignored if set to `True`. If `return_count_only`, `return_ids_only`,
                                                or `return_extent_only` are `True`, this parameter is ignored.
        -------------------------------     --------------------------------------------------------------------
        result_type                         Optional string. Controls the number of features returned by the
                                            operation.
                                            Options: ``None`` | ``standard`` | ``tile``

                                            .. note::
                                                See `Query (Feature Service/Layer) <https://developers.arcgis.com/rest/services-reference/enterprise/query-feature-service-layer-.htm>`_
                                                for full explanation.
        -------------------------------     --------------------------------------------------------------------
        historic_moment                     Optional integer. The historic moment to query. This parameter
                                            applies only if the layer is archiving enabled and the
                                            `supportsQueryWithHistoricMoment` property is set to `true`. This
                                            property is provided in the layer's
                                            :attr:`~arcgis.features.FeatureLayer.properties` resource. If
                                            not specified, the query will apply to the current features.
        -------------------------------     --------------------------------------------------------------------
        sql_format                          Optional string.  The `sql_format` parameter can be either standard
                                            SQL92 or it can use the native SQL of the underlying
                                            datastore. The default is `None`, which means it depends on the
                                            `useStandardizedQuery` layer property.
                                            Values: ``None`` | ``standard`` | ``native``
        -------------------------------     --------------------------------------------------------------------
        return_true_curves                  Optional boolean. When set to `True`, returns true curves in output
                                            geometries. When set to `False`, curves are converted to densified
                                            polylines or polygons.
        -------------------------------     --------------------------------------------------------------------
        return_exceeded_limit_features      Optional boolean. Optional parameter which is true by default. When
                                            set to true, features are returned even when the results include
                                            the `exceededTransferLimit: True` property.

                                            When set to `False` and querying with `resultType = tile`, features
                                            are not returned when the results include
                                            `exceededTransferLimit: True`. This allows a client to find the
                                            resolution in which the transfer limit is no longer exceeded without
                                            making multiple calls.
        -------------------------------     --------------------------------------------------------------------
        as_df                               Optional boolean.  If `True`, the results are returned as a
                                            `DataFrame` instead of a :class:`~arcgis.features.FeatureSet`.
        -------------------------------     --------------------------------------------------------------------
        datum_transformation                Optional Integer/Dictionary.  This parameter applies a datum transformation while
                                            projecting geometries in the results when out_sr is different than the layer's spatial
                                            reference. When specifying transformations, you need to think about which datum
                                            transformation best projects the layer (not the feature service) to the `outSR` and
                                            `sourceSpatialReference` property in the layer properties. For a list of valid datum
                                            transformation ID values ad well-known text strings, see `Coordinate systems and
                                            transformations <https://developers.arcgis.com/net/latest/wpf/guide/coordinate-systems-and-transformations.htm>`_.
                                            For more information on datum transformations, please see the transformation
                                            parameter in the `Project operation <https://developers.arcgis.com/rest/services-reference/project.htm>`_.

                                            Example:


                                            ===========     ===================================
                                            Inputs          Description
                                            -----------     -----------------------------------
                                            WKID            Integer.

                                                            .. code-block:: python

                                                                >>> datum_transformation=4326

                                            -----------     -----------------------------------
                                            WKT             Dict.

                                                            .. code-block:: python

                                                                >>> datum_transformation = {"wkt": "<WKT>"}

                                            -----------     -----------------------------------
                                            Composite       Dict.

                                                            .. code-block:: python

                                                                >>> datum_transformation = {"geoTransforms" : [
                                                                                                               {"wkid" : "<id>",
                                                                                                                "forward" : True | False},
                                                                                                               {"wkt" : "WKT",
                                                                                                                "forward" : True: False}
                                                                                                              ]
                                                                                           }

                                            ===========     ===================================
        -------------------------------     --------------------------------------------------------------------
        range_values                        Optional List. Allows you to filter features from the layer that are
                                            within the specified range instant or extent.

                                            .. code-block:: python

                                                >>> range_values = [
                                                                    {
                                                                     "name": "range name" ,
                                                                     # single value or a value-range
                                                                     "value": <value> or [ <value1>, <value2> ]

                                                                    },
                                                                    {
                                                                     "name": "range name 2",
                                                                     "value": <value> or  [ <value3>, <value4> ]
                                                                    }
                                                                   ]


                                            .. note::

                                                `None` is allowed in value-range case to indicate infinity

                                                .. code-block:: python

                                                    # all features with values <= 1500
                                                    >>> range_values = [
                                                                        {"name" : "range name",
                                                                         "value" : [None, 1500]}
                                                                       ]

                                                    # all features with values >= 1000
                                                    >>> range_values = [
                                                                        {"name" : "range name",
                                                                         "value" : [1000, None]}
                                                                       ]

        -------------------------------     --------------------------------------------------------------------
        parameter_values                    Optional Dict. Allows you to filter the layers by specifying
                                            value(s) to an array of pre-authored parameterized filters for those
                                            layers. When value is not specified for any parameter in a request,
                                            the default value, that is assigned during authoring time, gets used
                                            instead.

                                            When a `parameterInfo` allows multiple values, you must pass them in
                                            an array.

                                            .. note::
                                                Check `parameterValues` at the `Query (Map Service/Layer) <https://developers.arcgis.com/rest/services-reference/enterprise/query-map-service-layer-.htm#GUID-403AC0F3-4B48-45BD-B473-E52E790FD296>`_
                                                for details on parameterized filters.
        -------------------------------     --------------------------------------------------------------------
        kwargs                              Optional dict. Optional parameters that can be passed to the Query
                                            function.  This will allow users to pass additional parameters not
                                            explicitly implemented on the function. A complete list of functions
                                            available is documented  at `Query (Feature Service/Layer) <https://developers.arcgis.com/rest/services-reference/enterprise/query-feature-service-layer-.htm>`_.
        ===============================     ====================================================================

        :return: A :class:`~arcgis.features.FeatureSet` containing the features matching the query unless another
        return type is specified, such as ``count``.

        .. code-block:: python

            # USAGE EXAMPLE

            >>> from arcgis.layers import MapImageLayer, MapFeatureLayer
            >>> from arcgis.gis import GIS

            # connect to your GIS and get the web map item
            >>> gis = GIS(url, username, password)

            >>> map_image_item = gis.content.get("2aaddab96684405880d27f5261125061")
            >>> map_feature_layer = MapFeatureLayer.fromitem(item = map_image_item,
                                                             layer_id = 2)
            >>> query_count = map_feature_layer.query(where "1=1",
                                        text = "Hurricane Data",
                                        units = "esriSRUnit_Meter",
                                        return_count_only = True,
                                        out_statistics = [
                                                            {
                                                            "statisticType": "count",
                                                            "onStatisticField": "Field1",
                                                            "outStatisticFieldName": "Out_Field_Name1"
                                                            },
                                                            {
                                                            "statisticType": "avg",
                                                            "onStatisticField": "Field2",
                                                            "outStatisticFieldName": "Out_Field_Name2"
                                                            }
                                                        ],
                                        range_values= [
                                                {
                                                  "name": "range name",
                                                  "value": [None, 1500]
                                                  },
                                                  {
                                                    "name": "range name 2",
                                                    "value":[1000, None]
                                                  }
                                                }
                                            ]
                                        )
            >>> query_count
            <149>
        """
        query_params = _query.QueryParameters(
            where=where,
            text=text,
            out_fields=out_fields,
            time_filter=time_filter,
            geometry_filter=geometry_filter,
            return_geometry=return_geometry,
            return_count_only=return_count_only,
            return_ids_only=return_ids_only,
            return_distinct_values=return_distinct_values,
            return_extent_only=return_extent_only,
            group_by_fields_for_statistics=group_by_fields_for_statistics,
            statistic_filter=statistic_filter,
            result_offset=result_offset,
            result_record_count=result_record_count,
            object_ids=object_ids,
            distance=distance,
            units=units,
            max_allowable_offset=max_allowable_offset,
            out_sr=out_sr,
            geometry_precision=geometry_precision,
            gdb_version=gdb_version,
            order_by_fields=order_by_fields,
            out_statistics=out_statistics,
            return_z=return_z,
            return_m=return_m,
            multipatch_option=multipatch_option,
            quantization_parameters=quantization_parameters,
            return_centroid=return_centroid,
            return_all_records=return_all_records,
            result_type=result_type,
            historic_moment=historic_moment,
            sql_format=sql_format,
            return_true_curves=return_true_curves,
            return_exceeded_limit_features=return_exceeded_limit_features,
            datum_transformation=datum_transformation,
            range_values=range_values,
            parameter_values=parameter_values,
        )
        return _query.Query(
            layer=self,
            parameters=query_params,
            as_df=as_df,
            is_layer=True,
        ).execute()

    # ----------------------------------------------------------------------
    def query_related_records(
        self,
        object_ids: str,
        relationship_id: str,
        out_fields: str | list[str] = "*",
        definition_expression: str | None = None,
        return_geometry: bool = True,
        max_allowable_offset: float | None = None,
        geometry_precision: int | None = None,
        out_wkid: int | None = None,
        gdb_version: str | None = None,
        return_z: bool = False,
        return_m: bool = False,
        historic_moment: int | _dt.datetime | None = None,
        return_true_curve: bool = False,
    ) -> dict:
        """
        The ``query_related_records`` operation is performed on a :class:`~arcgis.layers.MapFeatureLayer`
        resource. The result of this operation are :class:`~arcgis.features.FeatureSet` objects grouped
        by source layer/table object IDs. Each :class:`~arcgis.features.FeatureSet` contains
        :class:`~arcgis.features.Feature` objects including the values for the fields requested by
        the user.

        .. note::
            For related layers, if you request geometry
            information, the geometry of each feature is also returned in
            the feature set. For related tables, the feature set does not
            include geometries.

        .. note::
            See the :attr:`~arcgis.layers.MapFeatureLayer.query` method for more information.


        ======================     ====================================================================
        **Parameter**               **Description**
        ----------------------     --------------------------------------------------------------------
        object_ids                 Required string. The object IDs of the table/layer to be queried
        ----------------------     --------------------------------------------------------------------
        relationship_id            Required string. The ID of the relationship to be queried.
        ----------------------     --------------------------------------------------------------------
        out_fields                 Required string. the list of fields from the related table/layer
                                   to be included in the returned feature set. This list is a comma
                                   delimited list of field names. If you specify the shape field in the
                                   list of return fields, it is ignored. To request geometry, set
                                   return_geometry to true. You can also specify the wildcard "*" as
                                   the value of this parameter. In this case, the results will include
                                   all the field values.
        ----------------------     --------------------------------------------------------------------
        definition_expression      Optional string. The definition expression to be applied to the
                                   related table/layer. From the list of objectIds, only those records
                                   that conform to this expression are queried for related records.
        ----------------------     --------------------------------------------------------------------
        return_geometry            Optional boolean. If true, the feature set includes the geometry
                                   associated with each feature. The default is true.
        ----------------------     --------------------------------------------------------------------
        max_allowable_offset       Optional float. This option can be used to specify the
                                   max_allowable_offset to be used for generalizing geometries returned
                                   by the query operation. The max_allowable_offset is in the units of
                                   the outSR. If out_wkid is not specified, then max_allowable_offset
                                   is assumed to be in the unit of the spatial reference of the map.
        ----------------------     --------------------------------------------------------------------
        geometry_precision         Optional integer. This option can be used to specify the number of
                                   decimal places in the response geometries.
        ----------------------     --------------------------------------------------------------------
        out_wkid                   Optional Integer. The spatial reference of the returned geometry.
        ----------------------     --------------------------------------------------------------------
        gdb_version                Optional string. The geodatabase version to query. This parameter
                                   applies only if the isDataVersioned property of the layer queried is
                                   true.
        ----------------------     --------------------------------------------------------------------
        return_z                   Optional boolean. If true, Z values are included in the results if
                                   the features have Z values. Otherwise, Z values are not returned.
                                   The default is false.
        ----------------------     --------------------------------------------------------------------
        return_m                   Optional boolean. If true, M values are included in the results if
                                   the features have M values. Otherwise, M values are not returned.
                                   The default is false.
        ----------------------     --------------------------------------------------------------------
        historic_moment            Optional Integer/_dt.datetime. The historic moment to query. This parameter
                                   applies only if the supportsQueryWithHistoricMoment property of the
                                   layers being queried is set to true. This setting is provided in the
                                   layer resource.

                                   If historic_moment is not specified, the query will apply to the
                                   current features.

                                   Syntax:
                                        historic_moment=<Epoch time in milliseconds>
        ----------------------     --------------------------------------------------------------------
        return_true_curves         Optional boolean. Optional parameter that is false by default. When
                                   set to true, returns true curves in output geometries; otherwise,
                                   curves are converted to densified polylines or polygons.
        ======================     ====================================================================


        :return: dict


        """
        params = {
            "f": "json",
            "objectIds": object_ids,
            "relationshipId": relationship_id,
            "outFields": out_fields,
            "returnGeometry": return_geometry,
            "returnM": return_m,
            "returnZ": return_z,
        }
        if historic_moment:
            if hasattr(historic_moment, "timestamp"):
                historic_moment = int(historic_moment.timestamp() * 1000)
            params["historicMoment"] = historic_moment
        if return_true_curve:
            params["returnTrueCurves"] = return_true_curve
        if self._dynamic_layer is not None:
            params["layer"] = self._dynamic_layer
        if gdb_version is not None:
            params["gdbVersion"] = gdb_version
        if definition_expression is not None:
            params["definitionExpression"] = definition_expression
        if out_wkid is not None and isinstance(out_wkid, _geometry.SpatialReference):
            params["outSR"] = out_wkid
        elif out_wkid is not None and isinstance(out_wkid, dict):
            params["outSR"] = out_wkid
        if max_allowable_offset is not None:
            params["maxAllowableOffset"] = max_allowable_offset
        if geometry_precision is not None:
            params["geometryPrecision"] = geometry_precision
        if self._dynamic_layer is None:
            qrr_url = self._url + "/queryRelatedRecords"
        else:
            qrr_url = "%s/queryRelatedRecords" % self._url.split("?")[0]

        resp: requests.Response = self._session.post(url=qrr_url, data=params)
        resp.raise_for_status()
        return resp.json()

    # ----------------------------------------------------------------------
    def get_html_popup(self, oid: str) -> dict | str:
        """
        The ``get_html_popup`` resource provides details about the HTML pop-up
        authored by the user using ArcGIS Pro or ArcGIS Desktop.

        ===============     ====================================================================
        **Parameter**        **Description**
        ---------------     --------------------------------------------------------------------
        oid                 Optional string. Object id of the feature to get the HTML popup.
        ===============     ====================================================================


        :return:
            A string

        """
        if self.properties.htmlPopupType != "esriServerHTMLPopupTypeNone":
            pop_url = self._url + "/%s/htmlPopup" % oid
            params = {"f": "json"}
            resp: requests.Response = self._session.post(url=pop_url, data=params)
            resp.raise_for_status()
            return resp.json()
        return ""


###########################################################################
class MapRasterLayer(MapFeatureLayer):
    """
    The ``MapRasterLayer`` class represents a geo-referenced image hosted in a ``Map Service``.
    """

    @property
    def _lyr_dict(self):
        url = self.url

        if "lods" in self.container.properties:
            lyr_dict = {"type": "ArcGISTiledMapServiceLayer", "url": url}

        else:
            lyr_dict = {"type": type(self.container).__name__, "url": url}

        if self._token is not None:
            lyr_dict["serviceToken"] = self._token

        if self.filter is not None:
            lyr_dict["filter"] = self.filter
        if self._time_filter is not None:
            lyr_dict["time"] = self._time_filter
        return lyr_dict

    @property
    def _lyr_json(self):
        url = self.url
        if self._token is not None:
            url += "?token=" + self._token

        if "lods" in self.container.properties:
            lyr_dict = {
                "type": "ArcGISTiledMapServiceLayer",
                "url": self.container.url,
            }

        else:
            lyr_dict = {
                "type": type(self.container).__name__,
                "url": self.container.url,
            }

        if self.filter is not None:
            lyr_dict["options"] = json.dumps({"definition_expression": self.filter})
        if self._time_filter is not None:
            lyr_dict["time"] = self._time_filter
        return lyr_dict


###########################################################################
class MapTable(MapFeatureLayer):
    """
    The ``MapTable`` class represents entity classes with uniform properties.

    .. note::
        In addition to working with entities with ``location`` as
        features, the :class:`~arcgis.gis.GIS` can also work with non-spatial entities as rows in tables.

    Working with tables is similar to working with a :class:`~arcgis.layers.MapFeatureLayer`, except that the rows
    (:class:`~arcgis.features.Feature`) in a table do not have a geometry, and tables ignore any geometry related
    operation.
    """

    @classmethod
    def fromitem(cls, item: Item, table_id: int = 0) -> MapTable:
        """
        The ``fromitem`` method creates a :class:`~arcgis.layers.MapTable` from a GIS :class:`~arcgis.gis.Item`.


        ====================================     ====================================================================
        **Parameter**                             **Description**
        ------------------------------------     --------------------------------------------------------------------
        item                                     Required :class:`~arcgis.gis.Item` object. The type of item should be
                                                 a :class:`~arcgis.layers.MapImageService` object.
        ------------------------------------     --------------------------------------------------------------------
        layer_id                                 Optional integer. The id of the layer in the Map Service's Layer.
                                                 The default is 0.
        ====================================     ====================================================================

        :return:
            A :class:`~arcgis.layers.MapTable` object

        .. code-block:: python

            # USAGE EXAMPLE

            >>> from arcgis.layers import MapImageLayer, MapTable
            >>> from arcgis.gis import GIS

            # connect to your GIS and get the web map item
            >>> gis = GIS(url, username, password)

            >>> map_image_item = gis.content.get("2aaddab96684405880d27f5261125061")
            >>> map_table = MapFeatureLayer.fromitem(item = map_image_item,
                                                             layer_id = 2)
            >>> print(f"{map_table.properties.name:30}{type(map_table)}")
            <State Boundaries              <class 'arcgis.layers.MapTable'>>
        """
        return item.tables[table_id]

    # ----------------------------------------------------------------------
    @property
    def _lyr_dict(self):
        url = self.url

        lyr_dict = {"type": "FeatureLayer", "url": url}
        if self._token is not None:
            lyr_dict["serviceToken"] = self._token

        if self.filter is not None:
            lyr_dict["filter"] = self.filter
        if self._time_filter is not None:
            lyr_dict["time"] = self._time_filter
        return lyr_dict

    # ----------------------------------------------------------------------
    @property
    def _lyr_json(self):
        url = self.url
        if self._token is not None:
            url += "?token=" + self._token

        lyr_dict = {"type": "FeatureLayer", "url": url}

        if self.filter is not None:
            lyr_dict["options"] = json.dumps({"definition_expression": self.filter})
        if self._time_filter is not None:
            lyr_dict["time"] = self._time_filter
        return lyr_dict

    # ----------------------------------------------------------------------
    def query(
        self,
        where: str = "1=1",
        out_fields: str | list[str] = "*",
        time_filter: (
            _dt.datetime | list[_dt.datetime] | list[str] | dict[_dt.datetime] | None
        ) = None,
        return_count_only: bool = False,
        return_ids_only: bool = False,
        return_distinct_values: bool = False,
        group_by_fields_for_statistics: str | None = None,
        statistic_filter: StatisticFilter | None = None,
        result_offset: int | None = None,
        result_record_count: int | None = None,
        object_ids: str | None = None,
        gdb_version: str | None = None,
        order_by_fields: list[str] | str | None = None,
        out_statistics: list[dict] | None = None,
        return_all_records: bool = True,
        historic_moment: int | _dt.datetime | None = None,
        sql_format: str | None = None,
        return_exceeded_limit_features: bool | None = None,
        as_df: bool = False,
        range_values: list[dict[str, Any]] | None = None,
        parameter_values: list[dict[str, Any]] | None = None,
        **kwargs,
    ) -> int | dict | _features.FeatureSet:
        """
        The ``query`` method queries a Table Layer based on a set of criteria from a sql statement.

        ===============================     ====================================================================
        **Parameter**                        **Description**
        -------------------------------     --------------------------------------------------------------------
        where                               Optional string. The default is 1=1. The selection sql statement.
        -------------------------------     --------------------------------------------------------------------
        out_fields                          Optional List of field names to return. Field names can be specified
                                            either as a List of field names or as a comma separated string.
                                            The default is "*", which returns all the fields.
        -------------------------------     --------------------------------------------------------------------
        object_ids                          Optional string. The object IDs of this layer or table to be queried.
                                            The object ID values should be a comma-separated string.
        -------------------------------     --------------------------------------------------------------------
        time_filter                         Optional list. The format is of [<startTime>, <endTime>] using
                                            _dt.datetime.date, _dt.datetime._dt.datetime or timestamp in milliseconds.

                                            .. code-block:: python

                                                >>> time_filter=[<startTime>, <endTime>]

                                            Specified as ``_dt.datetime.date``, ``_dt.datetime._dt.datetime`` or
                                            ``timestamp`` in milliseconds.

                                            .. code-block:: python

                                                >>> import _dt.datetime as dt

                                                >>> time_filter = [dt._dt.datetime(2022, 1, 1), dt.datetime(2022, 1, 12)]

        -------------------------------     --------------------------------------------------------------------
        gdb_version                         Optional string. The geodatabase version to query. This parameter
                                            applies only if the `isDataVersioned` property of the layer is
                                            `true`. If this is not specified, the query will apply to the
                                            published map's version.
        -------------------------------     --------------------------------------------------------------------
        return_geometry                     Optional boolean. If `True`, geometry is returned with the query.
                                            Default is `True`.
        -------------------------------     --------------------------------------------------------------------
        return_distinct_values              Optional boolean.  If `True`, it returns distinct values based on
                                            the fields specified in `out_fields`. This parameter applies only if
                                            the `supportsAdvancedQueries` property of the layer is `true`.
        -------------------------------     --------------------------------------------------------------------
        return_ids_only                     Optional boolean. Default is False.  If `True`, the response only
                                            includes an array of object IDs. Otherwise, the response is a
                                            :class:`~arcgis.features.FeatureSet`.
        -------------------------------     --------------------------------------------------------------------
        return_count_only                   Optional boolean. If `True`, the response only includes the count
                                            (number of features/records) that would be returned by a query.
                                            Otherwise, the response is a :class:`~arcgis.features.FeatureSet`.
                                            The default is `False`. This option supersedes the
                                            `return_ids_only` parameter. If `return_count_only = True`, the
                                            response will return both the count and the extent.
        -------------------------------     --------------------------------------------------------------------
        order_by_fields                     Optional string or list of strings. One or more field names by which to order the
                                            results. Use ``ASC`` or ``DESC`` for ascending
                                            or descending, respectively, following every field to be ordered:

                                            .. code-block:: python

                                                >>> order_by_fields = "STATE_NAME ASC, RACE DESC, GENDER ASC"

        -------------------------------     --------------------------------------------------------------------
        group_by_fields_for_statistics      Optional string. One or more field names on which to group results
                                            for calculating the statistics.

                                            .. code-block:: python

                                                >>> group_by_fields_for_statistics = "STATE_NAME, GENDER"

        -------------------------------     --------------------------------------------------------------------
        out_statistics                      Optional string. The definitions for one or more field-based
                                            statistics to be calculated.

                                            :Syntax:

                                            .. code-block:: python

                                                >>> out_statistics = [
                                                                        {
                                                                          "statisticType": "<count | sum | min | max | avg | stddev | var>",
                                                                          "onStatisticField": "Field1",
                                                                          "outStatisticFieldName": "Out_Field_Name1"
                                                                        },{
                                                                           "statisticType": "<count | sum | min | max | avg | stddev | var>",
                                                                           "onStatisticField": "Field2",
                                                                           "outStatisticFieldName": "Out_Field_Name2"
                                                                          }
                                                                    ]
        -------------------------------     --------------------------------------------------------------------
        result_offset                       Optional integer. This option can be used for fetching query results
                                            by skipping the specified number of records and starting from the
                                            next record (that is, `result_offset + ith`). This option is ignored
                                            if `return_all_records` is `True` (i.e. by default).
        -------------------------------     --------------------------------------------------------------------
        result_record_count                 Optional integer. This option can be used for fetching query results
                                            up to the `result_record_count` specified. When `result_offset` is
                                            specified but this parameter is not, the map service defaults it to
                                            `max_record_count`. The maximum value for this parameter is the value
                                            of the layer's `maxRecordCount` property. This option is ignored if
                                            `return_all_records` is `True` (i.e. by default).
        -------------------------------     --------------------------------------------------------------------
        return_all_records                  Optional boolean. When `True`, the query operation will call the
                                            service until all records that satisfy the `where_clause` are
                                            returned. Note: `result_offset` and `result_record_count` will be
                                            ignored if `return_all_records` is True. Also, if
                                            `return_count_only`, `return_ids_only`, or `return_extent_only` are
                                            `True`, this parameter will be ignored.
        -------------------------------     --------------------------------------------------------------------
        historic_moment                     Optional integer. The historic moment to query. This parameter
                                            applies only if the layer is archiving enabled and the
                                            `supportsQueryWithHistoricMoment` property is set to `true`. This
                                            property is provided in the layer resource.

                                            .. note::
                                                See `Query (Feature Service/Layer) <https://developers.arcgis.com/rest/services-reference/enterprise/query-feature-service-layer-.htm>`_
                                                for full explanation of layer properties. Use :attr:`~arcgis.features.FeatureLayer.properties`
                                                to examine layer properties.

                                            If `historic_moment` is not specified, the query will apply to the
                                            current features.
        -------------------------------     --------------------------------------------------------------------
        sql_format                          Optional string.  The `sql_format` parameter can be either standard
                                            SQL92 or it can use the native SQL of the underlying
                                            datastore. The default is none which means the sql_format
                                            depends on the `useStandardizedQuery` parameter.
                                            Values: ``none`` | ``standard`` | ``native``
        -------------------------------     --------------------------------------------------------------------
        return_exceeded_limit_features      Optional boolean. Optional parameter which is `true` by default.
                                            When set to `true`, features are returned even when the results
                                            include the `exceededTransferLimit: true` property.

                                            When set to false and querying with `resultType = 'tile'`, features
                                            are not returned when the results include
                                            `exceededTransferLimit: True`. This allows a client to find the
                                            resolution in which the transfer limit is no longer exceeded without
                                            making multiple calls.
        -------------------------------     --------------------------------------------------------------------
        as_df                               Optional boolean.  If `True`, the results are returned as a
                                            `DataFrame` instead of a :class:`~arcgis.features.FeatureSet`.
        -------------------------------     --------------------------------------------------------------------
        range_values                        Optional List. Allows you to filter features from the layer that are
                                            within the specified range instant or extent.

                                            :Syntax:

                                            .. code-block:: python

                                                >>> range_values =     [
                                                                        {
                                                                          "name": "range name",
                                                                          "value": <value> or [ <value1>, <value2> ]
                                                                          },
                                                                          {
                                                                            "name": "range name 2",
                                                                            "value": <value> or  [ <value3>, <value4>]
                                                                          }
                                                                        }
                                                                       ]

                                            .. note::

                                                None is allowed in value-range case -- that means infinity

                                                .. code-block:: python

                                                    # all features with values <= 1500

                                                    >>> range_values = {"name" : "range name",
                                                                         "value :[None, 1500]}

                                                    # all features with values >= 1000

                                                    >>> range_values = {"name" : "range name",
                                                                        "value" : [1000, None]}

        -------------------------------     --------------------------------------------------------------------
        parameter_values                    Optional Dict. Allows you to filter the features layers by specifying
                                            value(s) to an array of pre-authored parameterized filters for those
                                            layers. When value is not specified for any parameter in a request,
                                            the default value, that is assigned during authoring time, gets used
                                            instead.

                                            When `parameterInfo` allows multiple values, you must pass them in
                                            an array.

                                            Note: Check `parameterInfos` at the layer
                                            :attr:`properties <arcgis.features.FeatureLayer.properties>` for
                                            the available parameterized filters, their default values and
                                            expected data type.
        -------------------------------     --------------------------------------------------------------------
        kwargs                              Optional dict. Optional parameters that can be passed to the Query
                                            function.  This will allow users to pass additional parameters not
                                            explicitly implemented on the function. A complete list of possible
                                            parameters is documented at `Query (Map Service/Layer) <https://developers.arcgis.com/rest/services-reference/enterprise/query-map-service-layer-.htm>`_
        ===============================     ====================================================================

        :return:
            A :class:`~arcgis.features.FeatureSet` or Panda's DataFrame containing the :class:`~arcgis.features.Feature`
            objects matching the query, unless another return type is specified, such as ``count``

        .. code-block:: python

            # USAGE EXAMPLE

            >>> from arcgis.layers import MapImageLayer, MapFeatureLayer
            >>> from arcgis.gis import GIS

            # connect to your GIS and get the web map item
            >>> gis = GIS(url, username, password)

            >>> map_image_item = gis.content.get("2aaddab96684405880d27f5261125061")
            >>> map_feature_layer = MapFeatureLayer.fromitem(item = map_image_item,
                                                             layer_id = 2)
            >>> query_count = map_feature_layer.query(where "1=1",
                                        text = "Hurricane Data",
                                        units = "esriSRUnit_Meter",
                                        return_count_only = True,
                                        out_statistics = [
                                                            {
                                                            "statisticType": "count",
                                                            "onStatisticField": "Field1",
                                                            "outStatisticFieldName": "Out_Field_Name1"
                                                            },
                                                            {
                                                            "statisticType": "avg",
                                                            "onStatisticField": "Field2",
                                                            "outStatisticFieldName": "Out_Field_Name2"
                                                            }
                                                        ],
                                        range_values= [
                                                {
                                                  "name": "range name",
                                                  "value": [None, 1500]
                                                  },
                                                  {
                                                    "name": "range name 2",
                                                    "value":[1000, None]
                                                  }
                                                }
                                            ]
                                        )
            >>> query_count
            <149>
        """
        query_params = _query.QueryParameters(
            where=where,
            out_fields=out_fields,
            time_filter=time_filter,
            return_count_only=return_count_only,
            return_ids_only=return_ids_only,
            return_distinct_values=return_distinct_values,
            group_by_fields_for_statistics=group_by_fields_for_statistics,
            statistic_filter=statistic_filter,
            result_offset=result_offset,
            result_record_count=result_record_count,
            object_ids=object_ids,
            gdb_version=gdb_version,
            order_by_fields=order_by_fields,
            out_statistics=out_statistics,
            return_all_records=return_all_records,
            historic_moment=historic_moment,
            sql_format=sql_format,
            return_exceeded_limit_features=return_exceeded_limit_features,
            range_values=range_values,
            parameter_values=parameter_values,
        )
        return _query.Query(
            layer=self,
            parameters=query_params,
            is_layer=False,
            as_df=as_df,
        ).execute()


###########################################################################
class _MSILayerFactory(type):
    """
    Factory that generates the Map Service Layers

    ==================     ====================================================================
    **Parameter**           **Description**
    ------------------     --------------------------------------------------------------------
    url                    Required string, specify the url ending in /MapServer/<index>
    ------------------     --------------------------------------------------------------------
    gis                    Optional :class:`~arcgis.gis.GIS`  object. If not specified, the active GIS connection is
                           used.
    ==================     ====================================================================

    .. code-block:: python

        # USAGE EXAMPLE 1: Instantiating a Map Service Layer object

        from arcgis.layers import SceneLayer
        ms_layer = MapServiceLayer(url='https://your_portal.com/arcgis/rest/services/service_name/MapServer/0')

        type(ms_layer)
        >> arcgis.layers.MapTable

        print(s_layer.properties.name)
        >> 'pipe_properties'
    """

    def __call__(cls, url, gis=None, container=None, dynamic_layer=None):
        lyr = Layer(url=url, gis=gis)
        props = dict(lyr.properties)
        ltype = props.get("type", "").lower()
        if ltype == "table":
            return MapTable(
                url=url,
                gis=gis,
                container=container,
                dynamic_layer=dynamic_layer,
            )
        if ltype == "raster layer":
            return MapRasterLayer(
                url=url,
                gis=gis,
                container=container,
                dynamic_layer=dynamic_layer,
            )
        if ltype == "feature layer":
            time_filter = props.get("timeInfo", {}).get("timeExtent")
            return MapFeatureLayer(
                url=url,
                gis=gis,
                container=container,
                dynamic_layer=dynamic_layer,
                time_filter=time_filter,
            )
        return lyr


###########################################################################
class MapServiceLayer(Layer, metaclass=_MSILayerFactory):
    """
    The ``MapServiceLayer`` class is a factory that generates the Map Service Layers.

    ==================     ====================================================================
    **Parameter**           **Description**
    ------------------     --------------------------------------------------------------------
    url                    Required string, specify the url ending in /MapServer/<index>
    ------------------     --------------------------------------------------------------------
    gis                    Optional :class:`~arcgis.gis.GIS` object. If not specified, the active GIS connection is
                           used.
    ==================     ====================================================================

    .. code-block:: python

        # USAGE EXAMPLE 1: Instantiating a Map Service Layer object

        from arcgis.layers import MapServiceLayer
        ms_layer = MapServiceLayer(url='https://your_portal.com/arcgis/rest/services/service_name/MapServer/0')

        type(ms_layer)
        >> arcgis.layers.MapTable

        print(ms_layer.properties.name)
        >> 'pipe_properties'

    """

    def __init__(self, url, gis=None, container=None, dynamic_layer=None):
        """
        Constructs a Map Services Layer given a URL and GIS
        """
        super(MapServiceLayer, self).__init__(
            url=url,
            gis=gis,
            container=container,
            dynamic_layer=dynamic_layer,
        )


###########################################################################
class EnterpriseMapImageLayerManager(_gis._GISResource):
    """
    The ``EnterpriseMapImageLayerManager`` class allows administration (if access permits) of ArcGIS Enterprise Map Image Layers and Tile Layers.
    A :class:`~arcgis.layers.MapImageLayer` offers access to layer content.

    .. note:: Url must be admin url such as: ``https://services.myserver.com/arcgis/rest/admin/services/serviceName/MapServer/``
    """

    def __init__(self, url, gis=None, map_img_lyr=None):
        if url.split("/")[-1].isdigit():
            url = url.replace(f"/{url.split('/')[-1]}", "")
        super(EnterpriseMapImageLayerManager, self).__init__(url, gis)
        self._ms = map_img_lyr

    @property
    def lifecycleinfo(self) -> dict:
        """Returns the date and time information (in Unix format) of when a service was created or updated."""
        url: str = f"{self.url}/lifecycleinfos"
        params: dict = {
            "f": "json",
        }
        return self._gis.session.get(url, params=params).json()

    def build_cache(
        self,
        levels: float = None,
        extent: list[float] | None = None,
        area_of_interest: _geometry.Polygon | None = None,
    ) -> str:
        """"""
        if area_of_interest is None and extent is None:
            raise ValueError("An `extent` or `area_of_interest` must be provided.")
        values = self._ms.url.split("/rest/services/")[1].split("/")
        if len(values) == 2:
            service_url: str = f"{values[0]}:{values[1]}"
        elif len(values) == 3:
            service_url: str = f"{values[0]}/{values[1]}:{values[2]}"

        update_mode: str = "RECREATE_ALL_TILES"
        return self._enterprise_cache_tool(
            service_url=service_url,
            levels=levels,
            update_mode=update_mode,
            thread_count=-1,
            extent=extent,
            job_id=None,
            area_of_interest=area_of_interest,
        )

    def _enterprise_cache_tool(
        self,
        service_url: str,
        levels: float | int,
        update_mode: str,
        thread_count: int = -1,
        extent: list[float] | None = None,
        job_id: str | None = None,
        area_of_interest: _geometry.Polygon | None = None,
    ) -> str:
        """ """
        update_modes: list[str] = [
            "RECREATE_EMPTY_TILES",
            "RECREATE_ALL_TILES",
            "DELETE_TILES",
            "COMPLETE_JOB",
            "FIX_JOB",
            "RESUME_JOB",
        ]
        if not update_mode in update_modes:
            raise ValueError(
                f"{update_mode} is not a valid value for `update_mode`. Allowed values are: {','.join(update_modes)}"
            )

        base_url: str = self._ms.url.split("/rest/services/")[0]
        url: str = base_url + "/rest/services/System/CachingControllers/GPServer"
        reporting_url: str = base_url + "/rest/services/System/ReportingTools/GPServer"
        from arcgis.layers import Service

        gp = Service(url_or_item=url, server=self._ms._gis)
        reporting_gp = Service(url_or_item=reporting_url, server=self._ms._gis)
        job = gp.manage_map_cache_tiles(
            service_url=service_url,
            levels=levels,
            thread_count=thread_count,
            update_mode=update_mode,
            constraining_extent=extent,
            jobid=job_id,
            area_of_interest=area_of_interest,
            future=True,
        )

        result = job.result()

        status_result = reporting_gp.report_cache_status(
            service_url=result,
            report_mode="esriCacheStatus",
            job_id=None,
            level_id=None,
            error_start=None,
            error_count=None,
            gis=None,
            future=False,
            estimate=False,
        )
        while status_result.get("status") in [
            "EXISTS",
            "NONE",
            "COMPLETED",
            "FAILED",
            "FAILURE",
            "FAILING",
        ]:
            if status_result.get("status") in [
                "EXISTS",
                "NONE",
                "COMPLETED",
                "FAILED",
                "FAILURE",
                "FAILING",
            ]:
                return status_result
            status_result = reporting_gp.report_cache_status(
                service_url=result,
                report_mode="esriCacheStatus",
                job_id=None,
                level_id=None,
                error_start=None,
                error_count=None,
                gis=None,
                future=False,
                estimate=False,
            )
        return status_result

    # ----------------------------------------------------------------------
    def edit(self, service_dictionary: dict) -> bool:
        """
        To edit a service, you need to submit the complete JSON
        representation of the service, which includes the updates to the
        service properties. Editing a service causes the service to be
        restarted with updated properties.

        ===================     ====================================================================
        **Parameter**            **Description**
        -------------------     --------------------------------------------------------------------
        service_dictionary      Required dict. The service JSON as a dictionary.
        ===================     ====================================================================


        :return: boolean
        """
        mil_service = _layers.Service(self.url, self._gis)
        return mil_service.edit(service_dictionary)

    # ----------------------------------------------------------------------
    def start(self):
        """starts the specific service"""
        mil_service = _layers.Service(self.url, self._gis)
        return mil_service.start()

    # ----------------------------------------------------------------------
    def stop(self):
        """stops the specific service"""
        mil_service = _layers.Service(self.url, self._gis)
        return mil_service.stop()

    # ----------------------------------------------------------------------
    def change_provider(self, provider: str) -> bool:
        """
        Allows for the switching of the service provide and how it is hosted on the ArcGIS Server instance.

        Provider parameter options:

        + `ArcObjects` means the service is running under the ArcMap runtime i.e. published from ArcMap
        + `ArcObjects11`: means the service is running under the ArcGIS Pro runtime i.e. published from ArcGIS Pro
        + `DMaps`: means the service is running in the shared instance pool (and thus running under the ArcGIS Pro provider runtime)

        :return: Boolean

        """
        mil_service = _layers.Service(self.url, self._gis)
        return mil_service.change_provider(provider)

    # ----------------------------------------------------------------------
    def delete(self):
        """deletes a service from arcgis server"""
        mil_service = _layers.Service(self.url, self._gis)
        return mil_service.delete()


###########################################################################
class MapImageLayerManager(_gis._GISResource):
    """
    The ``MapImageLayerManager`` class allows administration (if access permits) of ArcGIS Online Hosted Tile Layers
    or Cached Map Services.
    A :class:`~arcgis.layers.MapImageLayer` offers access to the Map Server endpoints
    that allow you to edit the tile service, update tiles, refresh, and more.

    To use this manager off of the MapImageLayer Class, pass in a url ending with /MapServer
    when instantiating that class.

    .. note::
        Map Image Layers are created from Enterprise Services and their manager can
        be accessed through the EnterpriseMapImageLayerManager.
    """

    def __init__(
        self,
        url: str,
        gis: _gis.GIS | None = None,
        map_img_lyr: MapImageLayer | None = None,
    ):
        self._session = gis.session
        if url.split("/")[-1].isdigit():
            url = url.replace(f"/{url.split('/')[-1]}", "")
        super(MapImageLayerManager, self).__init__(url, gis)
        self._ms = map_img_lyr

    # ----------------------------------------------------------------------
    def refresh(self) -> dict:
        """
        The ``refresh`` operation refreshes a service, which clears the web
        server cache for the service.
        """
        url = self._url + "/refresh"
        params = {"f": "json"}

        resp: requests.Response = self._session.post(url=url, data=params)
        resp.raise_for_status()
        res = resp.json()

        super(MapImageLayerManager, self)._refresh()
        if self._ms:
            self._ms._refresh()

        return res

    # ----------------------------------------------------------------------
    def cancel_job(self, job_id: str) -> dict:
        """
        The ``cancel_job`` operation supports cancelling a job while update
        tiles is running from a hosted feature service. The result of this
        operation is a response indicating success or failure with error
        code and description.

        ===============     ====================================================
        **Parameter**        **Description**
        ---------------     ----------------------------------------------------
        job_id               Required String. The job id to cancel.
        ===============     ====================================================

        """
        url = self._url + "/jobs/%s/cancel" % job_id
        params = {"f": "json"}
        resp: requests.Response = self._session.post(url=url, data=params)
        resp.raise_for_status()
        return resp.json()

    # ----------------------------------------------------------------------
    def job_statistics(self, job_id: str) -> dict:
        """
        Returns the job statistics for the given jobId

        """
        url = self._url + "/jobs/%s" % job_id
        params = {"f": "json"}
        resp: requests.Response = self._session.post(url=url, data=params)
        resp.raise_for_status()
        return resp.json()

    # ----------------------------------------------------------------------
    def import_tiles(
        self,
        item: _gis.Item,
        levels: str | list[int] | None = None,
        extent: str | dict[str, int] | None = None,
        merge: bool = False,
        replace: bool = False,
    ) -> dict:
        """
        The ``import_tiles`` method imports tiles from an :class:`~arcgis.gis.Item` object.

        Before executing this operation, you will need to make certain the following prerequisites are met:

        - Upload the TPK you wish to merge with the existing service, take note of its item ID.
        - Make certain that the uploaded TPK, TPKX item's tiling scheme matches with the service you wish to import into.
        - The source service LOD's should include all the LOD's that are part of the imported TPK item. For example, if the source service has tiles from levels 0 through 10, you can import tiles only within these levels and not above it.


        ===============     ====================================================
        **Parameter**        **Description**
        ---------------     ----------------------------------------------------
        item                Required ItemId or :class:`~arcgis.gis.Item` object. The TPK file's item id.
                            This TPK file contains to-be-extracted bundle files
                            which are then merged into an existing cache service.
        ---------------     ----------------------------------------------------
        levels              Optional String / List of integers, The level of details
                            to update. Example: "1,2,10,20" or [1,2,10,20]
        ---------------     ----------------------------------------------------
        extent              Optional String / Dict. The area to update as Xmin, YMin, XMax, YMax
                            example: "-100,-50,200,500" or
                            {'xmin':100, 'ymin':200, 'xmax':105, 'ymax':205}
        ---------------     ----------------------------------------------------
        merge               Optional Boolean. Default is false and applicable to
                            compact cache storage format. It controls whether
                            the bundle files from the TPK file are merged with
                            the one in the existing cached service. Otherwise,
                            the bundle files are overwritten.
        ---------------     ----------------------------------------------------
        replace             Optional Boolean. Default is false, applicable to
                            compact cache storage format and used when
                            merge=true. It controls whether the new tiles will
                            replace the existing ones when merging bundles.
        ===============     ====================================================
        :return:
            A dictionary

        .. code-block:: python

            # USAGE EXAMPLE
            >>> from arcgis.layers import MapImageLayer
            >>> from arcgis.gis import GIS
            # connect to your GIS and get the web map item
            >>> gis = GIS(url, username, password)
            >>> map_image_layer = MapImageLayer("<url>", gis)
            >>> mil_manager = map_image_layer.manager
            >>> imported_tiles = mil_manager.import_tiles(item="<item-id>",
                                                          levels = "11-20",
                                                          extent = {"xmin":6224324.092137296,
                                                                    "ymin":487347.5253569535,
                                                                    "xmax":11473407.698535524,
                                                                    "ymax":4239488.369818687,
                                                                    "spatialReference":{"wkid":102100}
                                                                    },
                                                          merge = True,
                                                        replace = True
                                                          )
            >>> type(imported_tiles)
            <Dictionary>
        """
        params = {
            "f": "json",
            "sourceItemId": None,
            "extent": extent,
            "levels": levels,
            "mergeBundle": merge,
            "replaceTiles": replace,
        }
        if isinstance(item, str):
            params["sourceItemId"] = item
        elif isinstance(item, _gis.Item):
            params["sourceItemId"] = item.itemid
        else:
            raise ValueError("The `item` must be a string or Item")
        if self._gis.version >= [10, 3]:
            url = self._url + "/import"
        else:
            url = self._url + "/importTiles"
        resp: requests.Response = self._session.post(url=url, data=params)
        resp.raise_for_status()
        return resp.json()

    # ----------------------------------------------------------------------
    def update_tiles(
        self,
        levels: str | list[int] | None = None,
        extent: str | dict[str, int] | None = None,
        merge: bool = False,
        replace: bool = False,
    ) -> dict | None:
        """
        The ``update_tiles`` method starts tile generation for ArcGIS Online. The levels of detail
        and the extent are needed to determine the area where tiles need
        to be rebuilt.

        .. note::
            The ``update_tiles`` operation is for ArcGIS Online only.

        ===============     ====================================================
        **Parameter**        **Description**
        ---------------     ----------------------------------------------------
        levels              Optional String / List of integers, The level of details
                            to update. Example: "1,2,10,20" or [1,2,10,20]
        ---------------     ----------------------------------------------------
        extent              Optional String / Dict. The area to update as Xmin, YMin, XMax, YMax
                            Example:
                                "-100,-50,200,500" or {'xmin':100, 'ymin':200, 'xmax':105, 'ymax':205}
        ---------------     ----------------------------------------------------
        merge               Optional Boolean. Default is false and applicable to
                            compact cache storage format. It controls whether
                            the bundle files from the TPK file are merged with
                            the one in the existing cached service. Otherwise,
                            the bundle files are overwritten.
        ---------------     ----------------------------------------------------
        replace             Optional Boolean. Default is false, applicable to
                            compact cache storage format and used when
                            merge=true. It controls whether the new tiles will
                            replace the existing ones when merging bundles.
        ===============     ====================================================

        :return:
           Dictionary. If the product is not ArcGIS Online tile service, the
           result will be None.

        .. code-block:: python

            # USAGE EXAMPLE

            >>> from arcgis.layers import MapImageLayer
            >>> from arcgis.gis import GIS

            # connect to your GIS and get the web map item
            >>> gis = GIS(url, username, password)
            >>> map_image_layer = MapImageLayer("<url>", gis)
            >>> mil_manager = map_image_layer.manager
            >>> update_tiles = mil_manager.update_tiles(levels = "11-20",
                                                        extent = {"xmin":6224324.092137296,
                                                                    "ymin":487347.5253569535,
                                                                    "xmax":11473407.698535524,
                                                                    "ymax":4239488.369818687,
                                                                    "spatialReference":{"wkid":102100}
                                                                    }
                                                        )
            >>> type(update_tiles)
            <Dictionary>
        """
        if self._gis._portal.is_arcgisonline:

            url = "%s/updateTiles" % self._url

            params = {
                "f": "json",
                "mergeBundle": merge,
                "replaceTiles": replace,
            }
            if levels:
                if isinstance(levels, list):
                    levels = ",".join(str(e) for e in levels)
                params["levels"] = levels
            if extent:
                if isinstance(extent, dict):
                    extent2 = "{},{},{},{}".format(
                        extent["xmin"],
                        extent["ymin"],
                        extent["xmax"],
                        extent["ymax"],
                    )
                    extent = extent2
                params["extent"] = extent
            resp: requests.Response = self._session.post(url=url, data=params)
            resp.raise_for_status()
            return resp.json()
        return None

    # ----------------------------------------------------------------------
    @property
    def rerun_job(self, job_id: str, code: str) -> dict:
        """
        The ``rerun_job`` operation supports re-running a canceled job from a
        hosted map service. The result of this operation is a response
        indicating success or failure with error code and description.

        ===============     ====================================================
        **Parameter**        **Description**
        ---------------     ----------------------------------------------------
        job_id              required string, job to reprocess
        ---------------     ----------------------------------------------------
        code                required string, parameter used to re-run a given
                            jobs with a specific error
                            code: ``ALL | ERROR | CANCELED``
        ===============     ====================================================

        :return:
           A boolean or dictionary
        """
        url = self._url + "/jobs/%s/rerun" % job_id
        params = {"f": "json", "rerun": code}
        resp: requests.Response = self._session.post(url=url, data=params)
        resp.raise_for_status()
        return resp.json()

    # ----------------------------------------------------------------------
    def edit_tile_service(
        self,
        service_definition: str | None = None,
        min_scale: float | None = None,
        max_scale: float | None = None,
        source_item_id: str | None = None,
        export_tiles_allowed: bool = False,
        max_export_tile_count: float = 100000,
    ) -> dict:
        """
        The ``edit_tile_service`` operation updates a Tile Service's properties.

        =====================     ======================================================
        **Parameter**              **Description**
        ---------------------     ------------------------------------------------------
        service_definition        Required String. Updates a service definition.
        ---------------------     ------------------------------------------------------
        min_scale                 Required float. Sets the services minimum scale for
                                  caching.
        ---------------------     ------------------------------------------------------
        max_scale                 Required float. Sets the services maximum scale for
                                  caching.
        ---------------------     ------------------------------------------------------
        source_item_id            Required String. The Source Item ID is the
                                  GeoWarehouse Item ID of the map service
        ---------------------     ------------------------------------------------------
        export_tiles_allowed      Required boolean. ``exports_tiles_allowed`` sets the
                                  value to let users export tiles
        ---------------------     ------------------------------------------------------
        max_export_tile_count     Optional float. ``max_export_tile_count`` sets the
                                  maximum amount of tiles to be exported from a single
                                  call.

                                  .. note::
                                      The default value is 100000.
        =====================     ======================================================

        .. code-block:: python

            # USAGE EXAMPLE

            >>> from arcgis.layers import MapImageLayer
            >>> from arcgis.gis import GIS

            # connect to your GIS and get the web map item
            >>> gis = GIS(url, username, password)
            >>> map_image_layer = MapImageLayer("<url>", gis)
            >>> mil_manager = map_image_layer.manager
            >>> mil_manager.edit_tile_service(service_definition = "updated service definition",
                                              min_scale = 50,
                                              max_scale = 100,
                                              source_item_id = "geowarehouse_item_id",
                                              export_tiles_allowed = True,
                                              max_Export_Tile_Count = 10000
                                             )
        """
        params = {
            "f": "json",
        }
        if not service_definition is None:
            params["serviceDefinition"] = service_definition
        if not min_scale is None:
            params["minScale"] = float(min_scale)
        if not max_scale is None:
            params["maxScale"] = float(max_scale)
        if not source_item_id is None:
            params["sourceItemId"] = source_item_id
        if not export_tiles_allowed is None:
            params["exportTilesAllowed"] = export_tiles_allowed
        if not max_export_tile_count is None:
            params["maxExportTileCount"] = int(max_export_tile_count)
        url = self._url + "/edit"
        resp: requests.Response = self._session.post(url=url, data=params)
        resp.raise_for_status()
        return resp.json()

    # ----------------------------------------------------------------------
    def delete_tiles(self, levels: str, extent: dict[str, int] | None = None) -> dict:
        """
        The ``delete_tiles`` method deletes tiles from the current cache.

        ===============     ====================================================
        **Parameter**        **Description**
        ---------------     ----------------------------------------------------
        levels              Required string, The level to delete.
                            Example, 0-5,10,11-20 or 1,2,3 or 0-5
        ---------------     ----------------------------------------------------
        extent              Optional dictionary,  If specified, the tiles within
                            this extent will be deleted or will be deleted based
                            on the service's full extent.
        ===============     ====================================================

        :return:
           A dictionary

        .. code-block:: python

            # USAGE EXAMPLE

            >>> from arcgis.layers import MapImageLayer
            >>> from arcgis.gis import GIS

            # connect to your GIS and get the web map item
            >>> gis = GIS(url, username, password)
            >>> map_image_layer = MapImageLayer("<url>", gis)
            >>> mil_manager = map_image_layer.manager
            >>> deleted_tiles = mil_manager.delete_tiles(levels = "11-20",
                                                  extent = {"xmin":6224324.092137296,
                                                            "ymin":487347.5253569535,
                                                            "xmax":11473407.698535524,
                                                            "ymax":4239488.369818687,
                                                            "spatialReference":{"wkid":102100}
                                                            }
                                                  )
            >>> type(deleted_tiles)
            <Dictionary>
        """
        params = {
            "f": "json",
            "levels": levels,
        }
        if extent:
            params["extent"] = extent
        url = self._url + "/deleteTiles"
        resp: requests.Response = self._session.post(url=url, data=params)
        resp.raise_for_status()
        return resp.json()


###########################################################################
class MapImageLayer(_gis.Layer):
    """
    The ``MapImageLayer`` allows you to display and analyze data from sublayers defined in a map service,
    exporting images instead of features. Map service images are dynamically generated on the server based on a request,
    which includes an LOD (level of detail), a bounding box, dpi, spatial reference and other options.
    The exported image is of the entire map extent specified.

    .. note::
        ``MapImageLayer`` does not display tiled images. To display tiled map service layers, see ``TileLayer``.
    """

    def __init__(self, url: str, gis: _gis.GIS | None = None):
        """
        .. Creates a map image layer given a URL. The URL will typically look like the following.

            https://<hostname>/arcgis/rest/services/<service-name>/MapServer

        :param url: the layer location
        :param gis: the GIS to which this layer belongs
        """
        super(MapImageLayer, self).__init__(url, gis)

        self._populate_layers()
        self._admin = None
        if gis is None:
            gis = _gis.GIS()
        if hasattr(gis, "session"):

            self._session = gis.session
        elif hasattr(gis, "_session"):
            self._session = gis._session
        else:
            raise ValueError("Please validate that the GIS object is correct.")
        try:
            from arcgis.gis.server._service._adminfactory import (
                AdminServiceGen,
            )

            self.service = AdminServiceGen(service=self, gis=gis)
        except:
            pass

    @classmethod
    def fromitem(cls, item: _gis.Item) -> MapImageLayer:
        if not item.type == "Map Service":
            raise TypeError("item must be a type of Map Service, not " + item.type)
        if item._gis._use_private_url_only:
            url: str = _get_item_url(item=item)
        else:
            url: str = _validate_url(item.url, item._gis)
        return cls(url, item._gis)

    @property
    def _lyr_dict(self):
        url = self.url

        if "lods" in self.properties:
            lyr_dict = {"type": "ArcGISTiledMapServiceLayer", "url": url}

        else:
            lyr_dict = {"type": type(self).__name__, "url": url}

        if self._token is not None:
            lyr_dict["serviceToken"] = self._token or self._con.token

        if self.filter is not None:
            lyr_dict["filter"] = self.filter
        if self._time_filter is not None:
            lyr_dict["time"] = self._time_filter
        return lyr_dict

    @property
    def _lyr_json(self):
        url = self.url
        if self._token is not None:
            token = self._token or self._con.token
            url += "?token=" + token

        if "lods" in self.properties:
            lyr_dict = {"type": "ArcGISTiledMapServiceLayer", "url": url}
        else:
            lyr_dict = {"type": type(self).__name__, "url": url}

        if self.filter is not None:
            lyr_dict["options"] = json.dumps({"definition_expression": self.filter})
        if self._time_filter is not None:
            lyr_dict["time"] = self._time_filter
        return lyr_dict

    def _populate_layers(self):
        layers = []
        tables = []
        if "layers" in self.properties and self.properties.layers:
            for lyr in self.properties.layers:
                if "subLayerIds" in lyr and lyr.subLayerIds is not None:  # Group Layer
                    lyr = _gis.Layer(self.url + "/" + str(lyr.id), self._gis)
                else:
                    lyr = MapServiceLayer(self.url + "/" + str(lyr.id), self._gis)
                layers.append(lyr)
        if "tables" in self.properties and self.properties.tables:
            for lyr in self.properties.tables:
                lyr = MapServiceLayer(self.url + "/" + str(lyr.id), self._gis, self)
                tables.append(lyr)

        self.layers = layers
        self.tables = tables

    def _str_replace(self, mystring, rd):
        """Replaces a value based on a key/value pair where the
        key is the text to replace and the value is the new value.

        The find/replace is case insensitive.

        """
        import re

        patternDict = {}
        for key, value in rd.items():
            pattern = re.compile(re.escape(key), re.IGNORECASE)
            patternDict[value] = pattern
        for key in patternDict:
            regex_obj = patternDict[key]
            mystring = regex_obj.sub(key, mystring)
        return mystring

    @property
    def manager(
        self,
    ) -> MapImageLayerManager | EnterpriseMapImageLayerManager:
        """
        The ``manager`` property returns an instance of :class:`~arcgis.layers.MapImageLayerManager` class
        for ArcGIS Online and :class:`~arcgis.layers.EnterpriseMapImageLayerManager` class for ArcGIS Enterprise
        which provides methods and properties for administering this service.
        """
        if self._admin is None:
            if self._gis._portal.is_arcgisonline:
                rd = {"/rest/services/": "/rest/admin/services/"}
                adminURL = self._str_replace(self._url, rd)
                if adminURL.split("/")[-1].isdigit():
                    adminURL = adminURL.replace(f'/{adminURL.split("/")[-1]}', "")
                self._admin = MapImageLayerManager(adminURL, self._gis, self)
            else:
                rd = {"/rest/": "/admin/", "/MapServer": ".MapServer"}
                adminURL = self._str_replace(self._url, rd)
                if adminURL.split("/")[-1].isdigit():
                    adminURL = adminURL.replace(f'/{adminURL.split("/")[-1]}', "")
                self._admin = EnterpriseMapImageLayerManager(adminURL, self._gis, self)
        return self._admin

    # ----------------------------------------------------------------------
    def create_dynamic_layer(
        self, layer: dict[str, Any]
    ) -> _features.FeatureLayer | None:
        """
        The ``create_dynamic_layer`` method creates a dynamic layer.
        A dynamic layer / table represents a single layer / table of a map service published by ArcGIS Server
        or of a registered workspace. This resource is supported only when the map image layer
        supports dynamic layers, as indicated by ``supportsDynamicLayers`` on
        the map image layer properties.

        =================     ====================================================================
        **Parameter**          **Description**
        -----------------     --------------------------------------------------------------------
        layer                 Required dict.  Dynamic layer/table source definition.

                              Syntax:

                                  | {
                                  |   "id": <layerOrTableId>,
                                  |   "source": <layer source>, //required
                                  |   "definitionExpression": "<definitionExpression>",
                                  |   "drawingInfo":
                                  |   {
                                  |     "renderer": <renderer>,
                                  |     "transparency": <transparency>,
                                  |     "scaleSymbols": <true,false>,
                                  |     "showLabels": <true,false>,
                                  |     "labelingInfo": <labeling info>
                                  |   },
                                  |   "layerTimeOptions": //supported only for time enabled map layers
                                  |   {
                                  |     "useTime" : <true,false>,
                                  |     "timeDataCumulative" : <true,false>,
                                  |     "timeOffset" : <timeOffset>,
                                  |     "timeOffsetUnits" : "<esriTimeUnitsCenturies,esriTimeUnitsDays,
                                  |                       esriTimeUnitsDecades,esriTimeUnitsHours,
                                  |                       esriTimeUnitsMilliseconds,esriTimeUnitsMinutes,
                                  |                       esriTimeUnitsMonths,esriTimeUnitsSeconds,
                                  |                       esriTimeUnitsWeeks,esriTimeUnitsYears |
                                  |                       esriTimeUnitsUnknown>"
                                  |   }
                                  | }
        =================     ====================================================================

        :return:
            :class:`~arcgis.features.FeatureLayer` or None (if not enabled)

        .. code-block:: python

            # USAGE EXAMPLE

            >>> from arcgis.layers import MapImageLayer
            >>> from arcgis.gis import GIS

            # connect to your GIS and get the web map item
            >>> gis = GIS(url, username, password)

            >>> map_image_item = gis.content.get("2aaddab96684405880d27f5261125061")
            >>> layer_to_add ={
                                "id": <layerId>,
                                "source": <layer source>
                                "definitionExpression": "<definitionExpression>",
                                "drawingInfo":
                                {
                                  "renderer": <renderer>,
                                  "transparency": <transparency>,
                                  "scaleSymbols": <true>,
                                  "showLabels": <true>,
                                  "labelingInfo": <labeling info>
                                },
                                "layerTimeOptions":
                                {
                                  "useTime" : <true,false>,
                                  "timeDataCumulative" : <true>,
                                  "timeOffset" : <timeOffset>,
                                  "timeOffsetUnits" : "<esriTimeUnitsCenturies>"
                                }
                              }
            >>> new_layer = map_image_item.create_dynamic_layer(layer= layer_to_add)
            >>>type(new_layer)
            <arcgis.features.FeatureLayer>

        """
        if (
            "supportsDynamicLayers" in self.properties
            and self.properties["supportsDynamicLayers"]
        ):
            from urllib.parse import urlencode

            url = "%s/dynamicLayer" % self._url
            d = urlencode(layer)
            url += "?layer=%s" % d
            return _features.FeatureLayer(url=url, gis=self._gis, dynamic_layer=layer)
        return None

    # ----------------------------------------------------------------------
    @property
    def kml(self) -> dict:
        """
        The ``kml`` method retrieves the KML file for the layer.

        :return:
            A KML file
        """
        url = "{url}/kml/mapImage.kmz".format(url=self._url)
        params = {"f": "json"}
        resp: requests.Response = self._session.get(
            url=url,
            params=params,
        )
        return handle_response(
            resp=resp,
            file_name="mapImage.kmz",
            out_path=tempfile.gettempdir(),
            try_json=True,
        )

    # ----------------------------------------------------------------------
    @property
    def item_info(self) -> dict:
        """
        The ``item_info`` method retrieves the service's item's information.

        :return:
            A dictionary
        """
        url = "{url}/info/iteminfo".format(url=self._url)
        resp: requests.Response = self._session.post(url=url)
        resp.raise_for_status()
        return resp.json()

    # ----------------------------------------------------------------------
    @property
    def legend(self) -> dict:
        """
        The ``legend`` property represents a map service's legend. It returns
        the legend information for all layers in the service. Each layer's
        legend information includes the symbol images and labels for each
        symbol. Each symbol is an image of size 20 x 20 pixels at 96 DPI.
        Additional information for each layer such as the layer ID, name,
        and min and max scales are also included.

        .. note::
            The legend symbols include the base64 encoded imageData as well as
            a url that could be used to retrieve the image from the server.

        :returns:
            Dictionary of legend information
        """
        url = "%s/legend" % self._url
        resp: requests.Response = self._session.post(url=url)
        resp.raise_for_status()
        return resp.json()

    # ----------------------------------------------------------------------
    @property
    def metadata(self) -> str:
        """
        The ``metadata`` property retrieves the service's XML metadata file

        :return:
            An XML metadata file
        """
        url = "{url}/info/metadata".format(url=self._url)
        resp: requests.Response = self._session.post(url=url)
        resp.raise_for_status()
        return resp.text

    # ----------------------------------------------------------------------
    def thumbnail(self, out_path: str | None = None) -> dict:
        """
        The ``thumbnail`` method retrieves the thumbnail.

        .. note::
            If a thumbnail is present, this operation will download the image to local disk.

        :return:
            A path to the downloaded thumbnail, or None.
        """
        if out_path is None:
            out_path = tempfile.gettempdir()
        url = "{url}/info/thumbnail".format(url=self._url)
        if out_path is None:
            out_path = tempfile.gettempdir()
        resp: requests.Response = self._session.post(url, {"f": "json"})
        return handle_response(
            out_path=out_path, file_name="thumbnail.png", try_json=True, resp=resp
        )

    # ----------------------------------------------------------------------
    def identify(
        self,
        geometry: _geometry.Geometry | list,
        map_extent: str,
        image_display: str | None = None,
        geometry_type: str = "Point",
        sr: dict[str, Any] | str | _geometry.SpatialReference = None,
        layer_defs: dict[str, Any] | None = None,
        time_value: list[str] | str = None,
        time_options: Optional[dict] = None,
        layers: str = "all",
        tolerance: int | None = None,
        return_geometry: bool = True,
        max_offset: int | None = None,
        precision: int = 4,
        dynamic_layers: dict[str, Any] | None = None,
        return_z: bool = False,
        return_m: bool = False,
        gdb_version: str | None = None,
        return_unformatted: bool = False,
        return_field_name: bool = False,
        transformations: list[dict] | list[int] | None = None,
        map_range_values: list[dict[str, Any]] | None = None,
        layer_range_values: dict[str, Any] | None = None,
        layer_parameters: list[dict[str, Any]] | None = None,
        **kwargs,
    ) -> dict:
        """
        The ``identify`` operation is performed on a map service resource
        to discover features at a geographic location. The result of this
        operation is an identify results resource.

        .. note::
            Each identified result includes its ``name``, ``layer ID``, ``layer name``, ``geometry``,
            ``geometry type``, and other attributes of that result as name-value pairs.

        ==================     ====================================================================
        **Parameter**           **Description**
        ------------------     --------------------------------------------------------------------
        geometry               Required :class:`~arcgis.geometry.Geometry` or list. The geometry
                               to identify on. The type of the geometry is specified by the
                               `geometryType` parameter. The structure of the geometries is same as
                               the structure of the JSON geometry objects returned by the API (See
                               `Geometry Objects <https://developers.arcgis.com/documentation/common-data-types/geometry-objects.htm>`_).
                               In addition to the JSON structures, for points and envelopes, you
                               can specify the geometries with a simpler comma-separated syntax.
        ------------------     --------------------------------------------------------------------
        geometry_type          Required string.The type of geometry specified by the geometry
                               parameter. The geometry type could be a point, line, polygon, or an
                               envelope.
                               Values:
                                    "Point" | "Multipoint" | "Polyline" | "Polygon" | "Envelope"
        ------------------     --------------------------------------------------------------------
        map_extent             Required string. The extent or bounding box of the map currently
                               being viewed.
        ------------------     --------------------------------------------------------------------
        sr                     Optional dict, string, or SpatialReference. The well-known ID of the
                               spatial reference of the input and output geometries as well as the
                               map_extent. If sr is not specified, the geometry and the map_extent
                               are assumed to be in the spatial reference of the map, and the
                               output geometries are also in the spatial reference of the map.
        ------------------     --------------------------------------------------------------------
        layer_defs             Optional dict. Allows you to filter the features of individual
                               layers in the exported map by specifying definition expressions for
                               those layers. Definition expression for a layer that is
                               published with the service will be always honored.
        ------------------     --------------------------------------------------------------------
        time_value             Optional list. The time instant or the time extent of the features
                               to be identified.
        ------------------     --------------------------------------------------------------------
        time_options           Optional dict. The time options per layer. Users can indicate
                               whether or not the layer should use the time extent specified by the
                               time parameter or not, whether to draw the layer features
                               cumulatively or not and the time offsets for the layer.
        ------------------     --------------------------------------------------------------------
        layers                 Optional string. The layers to perform the identify operation on.
                               There are three ways to specify which layers to identify on:

                               - ``top``: Only the top-most layer at the specified location.
                               - ``visible``: All visible layers at the specified location.
                               - ``all``: All layers at the specified location.
        ------------------     --------------------------------------------------------------------
        tolerance              Optional integer. The distance in screen pixels from the specified
                               geometry within which the ``identify`` operation should be performed. The value for
                               the tolerance is an integer.
        ------------------     --------------------------------------------------------------------
        image_display          Optional string. The screen image display parameters (width, height,
                               and DPI) of the map being currently viewed. The mapExtent and the
                               image_display parameters are used by the server to determine the
                               layers visible in the current extent. They are also used to
                               calculate the distance on the map to search based on the tolerance
                               in screen pixels.

                               Syntax:

                                    <width>, <height>, <dpi>
        ------------------     --------------------------------------------------------------------
        return_geometry        Optional boolean. If true, the result set will include the geometries
                               associated with each result. The default is true.
        ------------------     --------------------------------------------------------------------
        max_offset             Optional integer. This option can be used to specify the maximum
                               allowable offset to be used for generalizing geometries returned by
                               the identify operation.
        ------------------     --------------------------------------------------------------------
        precision              Optional integer. This option can be used to specify the number of
                               decimal places in the response geometries returned by the identify
                               operation. This applies to X and Y values only (not m or z-values).
        ------------------     --------------------------------------------------------------------
        dynamic_layers         Optional dict. Use dynamicLayers property to reorder layers and
                               change the layer data source. dynamicLayers can also be used to add
                               new layer that was not defined in the map used to create the map
                               service. The new layer should have its source pointing to one of the
                               registered workspaces that was defined at the time the map service
                               was created.
                               The order of dynamicLayers array defines the layer drawing order.
                               The first element of the dynamicLayers is stacked on top of all
                               other layers. When defining a dynamic layer, source is required.
        ------------------     --------------------------------------------------------------------
        return_z               Optional boolean. If true, Z values will be included in the results
                               if the features have Z values. Otherwise, Z values are not returned.
                               The default is false.
        ------------------     --------------------------------------------------------------------
        return_m               Optional boolean.If true, M values will be included in the results
                               if the features have M values. Otherwise, M values are not returned.
                               The default is false.
        ------------------     --------------------------------------------------------------------
        gdb_version            Optional string. Switch map layers to point to an alternate
                               geodatabase version.
        ------------------     --------------------------------------------------------------------
        return_unformatted     Optional boolean. If true, the values in the result will not be
                               formatted i.e. numbers will be returned as is and dates will be
                               returned as epoch values. The default is False.
        ------------------     --------------------------------------------------------------------
        return_field_name      Optional boolean. Default is False. If true, field names will be
                               returned instead of field aliases.
        ------------------     --------------------------------------------------------------------
        transformations        Optional list. Use this parameter to apply one or more datum
                               transformations to the map when sr is different than the map
                               service's spatial reference. It is an array of transformation
                               elements.
                               Transformations specified here are used to project features from
                               layers within a map service to sr.
        ------------------     --------------------------------------------------------------------
        map_range_values       Optional list of dictionary(ies). Allows for the filtering features in
                               the exported map from all layer that are within the specified range
                               instant or extent.
        ------------------     --------------------------------------------------------------------
        layer_range_values     Optional Dictionary. Allows for the filtering of features
                               for each individual layer that are within the specified range instant or
                               extent.
        ------------------     --------------------------------------------------------------------
        layer_parameters       Optional list of dictionary(ies). Allows for the filtering of the
                               features of individual layers in the exported map by specifying value(s)
                               to an array of pre-authored parameterized filters for those layers. When
                               value is not specified for any parameter in a request, the default
                               value, that is assigned during authoring time, gets used instead.
        ==================     ====================================================================

        :return:
            A dictionary

        .. code-block:: python

            # USAGE EXAMPLE

            >>> from arcgis.layers import MapImageLayer
            >>> from arcgis.gis import GIS

            # connect to your GIS and get the web map item
            >>> gis = GIS(url, username, password)

            >>> map_image_item = gis.content.get("2aaddab96684405880d27f5261125061")
            >>> identified = map_image_item.identify(geometry = geom1,
                                        geometry_type = "Multipoint",
                                        image_display = "width",
                                        return_geometry =True,
                                        return_z = True,
                                        return_m = True,
                                        return_field_name = True,
                                        )
            >>> type(identified)
            <Dictionary>
        """

        if geometry_type.find("esriGeometry") == -1:
            geometry_type = "esriGeometry" + geometry_type

        def get_param(key, default):
            # see if param in kwargs, if not, return default
            return kwargs.pop(key, default)

        # Check if these parameters are in kwargs
        sr = sr or get_param("sr", None)
        layer_defs = layer_defs or get_param("layerDefs", None)
        time_value = time_value or get_param("layerTimeOptions", None)
        return_geometry = (
            return_geometry
            if return_geometry is not None
            else get_param("returnGeometry", True)
        )
        layers = layers or get_param("layers", "all")
        time_options = time_options or get_param("layerTimeOptions", None)
        return_m = return_m if return_m is not None else get_param("returnM", False)
        return_z = return_z if return_z is not None else get_param("returnZ", False)
        max_offset = max_offset or get_param("maxAllowableOffset", None)
        precision = precision or get_param("geometryPrecision", None)
        dynamic_layers = dynamic_layers or get_param("dynamicLayers", None)
        gdb_version = gdb_version or get_param("gdbVersion", None)

        # check geometry input is a json format
        if isinstance(geometry, _geometry.Geometry):
            geometry = geometry.JSON
        elif isinstance(geometry, list):
            if all(isinstance(g, _geometry.Geometry) for g in geometry):
                geometry = [g.JSON for g in geometry]

        # start building params dict and include defined parameters
        params = {
            "f": "json",
            "geometry": geometry,
            "geometryType": geometry_type,
            "tolerance": tolerance,
            "mapExtent": map_extent,
            "imageDisplay": image_display,
            **{
                key: value
                for key, value in {
                    "sr": sr,
                    "layerDefs": layer_defs,
                    "time": time_value,
                    "layerTimeOptions": time_options,
                    "layers": layers,
                    "returnGeometry": return_geometry,
                    "returnM": return_m,
                    "returnZ": return_z,
                    "maxAllowableOffset": max_offset,
                    "geometryPrecision": precision,
                    "dynamicLayers": dynamic_layers,
                    "gdbVersion": gdb_version,
                    "returnUnformattedValues": return_unformatted,
                    "returnFieldName": return_field_name,
                    "datumTransformations": transformations,
                    "mapRangeValues": map_range_values,
                    "layerRangeValues": layer_range_values,
                    "layerParameterValues": layer_parameters,
                }.items()
                if value is not None
            },
        }

        identifyURL = "{url}/identify".format(url=self._url)

        resp: requests.Response = self._session.post(url=identifyURL, data=params)
        resp.raise_for_status()
        return resp.json()

    # ----------------------------------------------------------------------
    def find(
        self,
        search_text: str,
        layers: str,
        contains: bool = True,
        search_fields: str | None = None,
        sr: dict[str, Any] | str | _geometry.SpatialReference | None = None,
        layer_defs: dict[str, Any] | None = None,
        return_geometry: bool = True,
        max_offset: int | None = None,
        precision: int | None = None,
        dynamic_layers: dict[str, Any] | None = None,
        return_z: bool = False,
        return_m: bool = False,
        gdb_version: str | None = None,
        return_unformatted: bool = False,
        return_field_name: bool = False,
        transformations: list[int] | list[dict[str, Any]] | None = None,
        map_range_values: list[dict[str, Any]] | None = None,
        layer_range_values: dict[str, Any] | None = None,
        layer_parameters: list[dict[str, Any]] | None = None,
        **kwargs,
    ) -> dict:
        """
        The ``find`` method performs the map service ``find`` operation.

        ==================     ====================================================================
        **Parameter**           **Description**
        ------------------     --------------------------------------------------------------------
        search_text            Required string.The search string. This is the text that is searched
                               across the layers and fields the user specifies.
        ------------------     --------------------------------------------------------------------
        layers                 Optional string. The layers to perform the identify operation on.
                               There are three ways to specify which layers to identify on:

                               - top: Only the top-most layer at the specified location.
                               - visible: All visible layers at the specified location.
                               - all: All layers at the specified location.
        ------------------     --------------------------------------------------------------------
        contains               Optional boolean. If false, the operation searches for an exact
                               match of the search_text string. An exact match is case sensitive.
                               Otherwise, it searches for a value that contains the search_text
                               provided. This search is not case sensitive. The default is true.
        ------------------     --------------------------------------------------------------------
        search_fields          Optional string. List of field names to look in.
        ------------------     --------------------------------------------------------------------
        sr                     Optional dict, string, or SpatialReference. The well-known ID of the
                               spatial reference of the input and output geometries as well as the
                               map_extent. If sr is not specified, the geometry and the map_extent
                               are assumed to be in the spatial reference of the map, and the
                               output geometries are also in the spatial reference of the map.
        ------------------     --------------------------------------------------------------------
        layer_defs             Optional dict. Allows you to filter the features of individual
                               layers in the exported map by specifying definition expressions for
                               those layers. Definition expression for a layer that is
                               published with the service will be always honored.
        ------------------     --------------------------------------------------------------------
        return_geometry        Optional boolean. If true, the result set will include the geometries
                               associated with each result. The default is true.
        ------------------     --------------------------------------------------------------------
        max_offset             Optional integer. This option can be used to specify the maximum
                               allowable offset to be used for generalizing geometries returned by
                               the identify operation.
        ------------------     --------------------------------------------------------------------
        precision              Optional integer. This option can be used to specify the number of
                               decimal places in the response geometries returned by the identify
                               operation. This applies to X and Y values only (not m or z-values).
        ------------------     --------------------------------------------------------------------
        dynamic_layers         Optional dict. Use dynamicLayers property to reorder layers and
                               change the layer data source. dynamicLayers can also be used to add
                               new layer that was not defined in the map used to create the map
                               service. The new layer should have its source pointing to one of the
                               registered workspaces that was defined at the time the map service
                               was created.
                               The order of dynamicLayers array defines the layer drawing order.
                               The first element of the dynamicLayers is stacked on top of all
                               other layers. When defining a dynamic layer, source is required.
        ------------------     --------------------------------------------------------------------
        return_z               Optional boolean. If true, Z values will be included in the results
                               if the features have Z values. Otherwise, Z values are not returned.
                               The default is false.
        ------------------     --------------------------------------------------------------------
        return_m               Optional boolean.If true, M values will be included in the results
                               if the features have M values. Otherwise, M values are not returned.
                               The default is false.
        ------------------     --------------------------------------------------------------------
        gdb_version            Optional string. Switch map layers to point to an alternate
                               geodatabase version.
        ------------------     --------------------------------------------------------------------
        return_unformatted     Optional boolean. If true, the values in the result will not be
                               formatted i.e. numbers will be returned as is and dates will be
                               returned as epoch values.
        ------------------     --------------------------------------------------------------------
        return_field_name      Optional boolean. If true, field names will be returned instead of
                               field aliases.
        ------------------     --------------------------------------------------------------------
        transformations        Optional list. Use this parameter to apply one or more datum
                               transformations to the map when sr is different from the map
                               service's spatial reference. It is an array of transformation
                               elements.
        ------------------     --------------------------------------------------------------------
        map_range_values       Optional list. Allows you to filter features in the exported map
                               from all layer that are within the specified range instant or
                               extent.
        ------------------     --------------------------------------------------------------------
        layer_range_values     Optional dictionary. Allows you to filter features for each
                               individual layer that are within the specified range instant or
                               extent. Note: Check range infos at the layer resources for the
                               available ranges.
        ------------------     --------------------------------------------------------------------
        layer_parameters       Optional list. Allows you to filter the features of individual
                               layers in the exported map by specifying value(s) to an array of
                               pre-authored parameterized filters for those layers. When value is
                               not specified for any parameter in a request, the default value,
                               that is assigned during authoring time, gets used instead.
        ==================     ====================================================================

        :return:
            A dictionary

        .. code-block:: python

            # USAGE EXAMPLE

            >>> from arcgis.layers import MapImageLayer
            >>> from arcgis.gis import GIS

            # connect to your GIS and get the web map item
            >>> gis = GIS(url, username, password)

            >>> map_image_item = gis.content.get("2aaddab96684405880d27f5261125061")
            >>> search_results = map_image_item.find(search_text = "Hurricane Data",
                                    contains = True,
                                    layers = "top",
                                    return_geometry = False,
                                    max_offset = 100,
                                    return_z = True,
                                    return_m = False,
                                    )
            >>> type(search_results)
            <Dictionary>

        """
        url = "{url}/find".format(url=self._url)
        params = {
            "f": "json",
            "searchText": search_text,
            "contains": contains,
        }
        if search_fields:
            params["searchFields"] = search_fields
        if sr:
            params["sr"] = sr
        if layer_defs:
            params["layerDefs"] = layer_defs
        if return_geometry is not None:
            params["returnGeometry"] = return_geometry
        if max_offset:
            params["maxAllowableOffset"] = max_offset
        if precision:
            params["geometryPrecision"] = precision
        if dynamic_layers:
            params["dynamicLayers"] = dynamic_layers
        if return_z is not None:
            params["returnZ"] = return_z
        if return_m is not None:
            params["returnM"] = return_m
        if gdb_version:
            params["gdbVersion"] = gdb_version
        if layers:
            params["layers"] = layers
        if return_unformatted is not None:
            params["returnUnformattedValues"] = return_unformatted
        if return_field_name is not None:
            params["returnFieldName"] = return_field_name
        if transformations:
            params["datumTransformations"] = transformations
        if map_range_values:
            params["mapRangeValues"] = map_range_values
        if layer_range_values:
            params["layerRangeValues"] = layer_range_values
        if layer_parameters:
            params["layerParameterValues"] = layer_parameters
        if len(kwargs) > 0:
            for k, v in kwargs.items():
                params[k] = v

        resp: requests.Response = self._session.post(url=url, data=params)
        resp.raise_for_status()
        return resp.json()

    # ----------------------------------------------------------------------
    def generate_kml(
        self,
        save_location: str,
        name: str,
        layers: str,
        options: str = "composite",
    ) -> str:
        """
        The ``generate_Kml`` operation is performed on a map service resource.
        The result of this operation is a KML document wrapped in a KMZ
        file.

        .. note::
            The document contains a network link to the KML Service
            endpoint with properties and parameters you specify.

        =================     ====================================================================
        **Parameter**          **Description**
        -----------------     --------------------------------------------------------------------
        save_location         Required string. Save folder.
        -----------------     --------------------------------------------------------------------
        name                  Required string. The name of the resulting KML document.
                              This is the name that appears in the Places panel of Google Earth.
        -----------------     --------------------------------------------------------------------
        layers                Required string. the layers to perform the generateKML operation on.
                              The layers are specified as a comma-separated list of layer ids.
        -----------------     --------------------------------------------------------------------
        options               Required string. The layer drawing options. Based on the option
                              chosen, the layers are drawn as one composite image, as separate
                              images, or as vectors. When the KML capability is enabled, the
                              ArcGIS Server administrator has the option of setting the layer
                              operations allowed. If vectors are not allowed, then the caller will
                              not be able to get vectors. Instead, the caller receives a single
                              composite image.
                              values: composite, separateImage, nonComposite
        =================     ====================================================================

        :return:
            A string to the file path

        """
        kmlURL = self._url + "/generateKml"
        params = {
            "f": "json",
            "docName": name,
            "layers": layers,
            "layerOptions": options,
        }
        resp: requests.Response = self._session.get(
            url=kmlURL, params=params, out_folder=save_location
        )
        resp.raise_for_status()
        return resp.json()

    # ----------------------------------------------------------------------
    def export_map(
        self,
        bbox: str,
        bbox_sr: int | None = None,
        size: str = "600,550",
        dpi: int = 200,
        image_sr: int | None = None,
        image_format: int = "png",
        layer_defs: dict[str, Any] | None = None,
        layers: str | None = None,
        transparent: bool = False,
        time_value: list[int] | list[_dt.datetime._dt.datetime] | None = None,
        time_options: dict[str, Any] | None = None,
        dynamic_layers: dict[str, Any] | None = None,
        gdb_version: str | None = None,
        scale: float | None = None,
        rotation: float | None = None,
        transformation: list[int] | list[dict[str, Any]] | None = None,
        map_range_values: list[dict[str, Any]] | None = None,
        layer_range_values: list[dict[str, Any]] | None = None,
        layer_parameter: list[dict[str, Any]] | None = None,
        f: str = "json",
        save_folder: str | None = None,
        save_file: str | None = None,
        **kwargs,
    ) -> str:
        """
        The ``export_map`` operation is performed on a map service resource.
        The result of this operation is a map image resource. This
        resource provides information about the exported map image such
        as its URL, its width and height, extent and scale.

        ==================     ====================================================================
        **Parameter**           **Description**
        ------------------     --------------------------------------------------------------------
        bbox                   Required string. The extent (bounding box) of the exported image.
                               Unless the bbox_sr parameter has been specified, the bbox is assumed
                               to be in the spatial reference of the map.
        ------------------     --------------------------------------------------------------------
        bbox_sr                Optional integer, :class:`~arcgis.geometry.SpatialReference`. The spatial reference of the bbox.
        ------------------     --------------------------------------------------------------------
        size                   Optional string. size - size of image in pixels
        ------------------     --------------------------------------------------------------------
        dpi                    Optional integer. dots per inch
        ------------------     --------------------------------------------------------------------
        image_sr               Optional integer, :class:`~arcgis.geometry.SpatialReference`.
                               The spatial reference of the output image.
        ------------------     --------------------------------------------------------------------
        image_format           Optional string. The format of the exported image.
                               The default format is .png.
                               Values:
                                    png | png8 | png24 | jpg | pdf | bmp | gif | svg | svgz | emf | ps | png32
        ------------------     --------------------------------------------------------------------
        layer_defs             Optional dict. Allows you to filter the features of individual
                               layers in the exported map by specifying definition expressions for
                               those layers. Definition expression for a layer that is
                               published with the service will be always honored.
        ------------------     --------------------------------------------------------------------
        layers                 Optional string. Determines which layers appear on the exported map.
                               There are four ways to specify which layers are shown:

                               ``show``: Only the layers specified in this list will be exported.

                               ``hide``: All layers except those specified in this list will be exported.

                               ``include``: In addition to the layers exported by default, the layers specified in this list will be exported.

                               ``exclude``: The layers exported by default excluding those specified in this list will be exported.
        ------------------     --------------------------------------------------------------------
        transparent            Optional boolean. If true, the image will be exported with the
                               background color of the map set as its transparent color. The
                               default is false.

                               .. note::
                                Only the .png and .gif formats support
                                transparency.
        ------------------     --------------------------------------------------------------------
        time_value             Optional list. The time instant or the time extent of the features
                               to be identified.
        ------------------     --------------------------------------------------------------------
        time_options           Optional dict. The time options per layer. Users can indicate
                               whether or not the layer should use the time extent specified by the
                               time parameter or not, whether to draw the layer features
                               cumulatively or not and the time offsets for the layer.
        ------------------     --------------------------------------------------------------------
        dynamic_layers         Optional dict. Use dynamicLayers property to reorder layers and
                               change the layer data source. dynamicLayers can also be used to add
                               new layer that was not defined in the map used to create the map
                               service. The new layer should have its source pointing to one of the
                               registered workspaces that was defined at the time the map service
                               was created.
                               The order of dynamicLayers array defines the layer drawing order.
                               The first element of the dynamicLayers is stacked on top of all
                               other layers. When defining a dynamic layer, source is required.
        ------------------     --------------------------------------------------------------------
        gdb_version            Optional string. Switch map layers to point to an alternate
                               geodatabase version.
        ------------------     --------------------------------------------------------------------
        scale                  Optional float. Use this parameter to export a map image at a
                               specific map scale, with the map centered around the center of the
                               specified bounding box (bbox)
        ------------------     --------------------------------------------------------------------
        rotation               Optional float. Use this parameter to export a map image rotated at
                               a specific angle, with the map centered around the center of the
                               specified bounding box (bbox). It could be positive or negative
                               number.
        ------------------     --------------------------------------------------------------------
        transformations        Optional list. Use this parameter to apply one or more datum
                               transformations to the map when sr is different than the map
                               service's spatial reference. It is an array of transformation
                               elements.
        ------------------     --------------------------------------------------------------------
        map_range_values       Optional list. Allows you to filter features in the exported map
                               from all layer that are within the specified range instant or
                               extent.
        ------------------     --------------------------------------------------------------------
        layer_range_values     Optional dictionary. Allows you to filter features for each
                               individual layer that are within the specified range instant or
                               extent. Note: Check range infos at the layer resources for the
                               available ranges.
        ------------------     --------------------------------------------------------------------
        layer_parameter        Optional list. Allows you to filter the features of individual
                               layers in the exported map by specifying value(s) to an array of
                               pre-authored parameterized filters for those layers. When value is
                               not specified for any parameter in a request, the default value,
                               that is assigned during authoring time, gets used instead.
        ==================     ====================================================================

        :return:
            A string, image of the map.

        .. code-block:: python

            # USAGE EXAMPLE

            >>> from arcgis.layers import MapImageLayer
            >>> from arcgis.gis import GIS

            # connect to your GIS and get the web map item
            >>> gis = GIS(url, username, password)

            >>> map_image_item = gis.content.get("2aaddab96684405880d27f5261125061")
            >>> map_image_item.export_map(bbox="-104,35.6,-94.32,41",
                                          bbox_sr = 4326,
                                          image_format ="png",
                                          layers = "include",
                                          transparent = True,
                                          scale = 40.0,
                                          rotation = -45.0
                                          )
        """

        params = {}
        params["f"] = f
        params["bbox"] = bbox
        if bbox_sr:
            params["bboxSR"] = bbox_sr
        if dpi is not None:
            params["dpi"] = dpi
        if size is not None:
            params["size"] = size
        if image_sr is not None and isinstance(image_sr, int):
            params["imageSR"] = {"wkid": image_sr}
        if image_format is not None:
            params["format"] = image_format
        if layer_defs is not None:
            params["layerDefs"] = layer_defs
        if layers is not None:
            params["layers"] = layers
        if transparent is not None:
            params["transparent"] = transparent
        if time_value is not None:
            params["time"] = time_value
        if time_options is not None:
            params["layerTimeOptions"] = time_options
        if dynamic_layers is not None:
            params["dynamicLayers"] = dynamic_layers
        if scale is not None:
            params["mapScale"] = scale
        if rotation is not None:
            params["rotation"] = rotation
        if gdb_version is not None:
            params["gdbVersion"] = gdb_version
        if transformation is not None:
            params["datumTransformations"] = transformation
        if map_range_values is not None:
            params["mapRangeValues"] = map_range_values
        if layer_range_values is not None:
            params["layerRangeValues"] = layer_range_values
        if layer_parameter:
            params["layerParameterValues"] = layer_parameter
        url = self._url + "/export"
        if len(kwargs) > 0:
            for k, v in kwargs.items():
                params[k] = v
        # return self._con.get(exportURL, params)

        if f == "json":
            resp: requests.Response = self._session.post(url=url, data=params)
            resp.raise_for_status()
            return resp.json()
        elif f == "image":
            if save_folder is not None and save_file is not None:
                resp: requests.Response = self._session.post(
                    url=url,
                    data=params,
                )
                return handle_response(
                    resp=resp, out_path=save_folder, file_name=save_file, try_json=True
                )
            else:
                resp: requests.Response = self._session.post(
                    url=url, data=params, force_bytes=True
                )
                resp.raise_for_status()
                return resp.json()
        elif f == "kmz":
            resp: requests.Response = self._session.post(
                url=url,
                data=params,
            )
            return handle_response(
                resp=resp, out_path=save_folder, file_name=save_file, try_json=True
            )
        else:
            print("Unsupported output format")

    # ----------------------------------------------------------------------
    def estimate_export_tiles_size(
        self,
        export_by: str,
        levels: str,
        tile_package: bool = False,
        export_extent: str = "DEFAULT",
        area_of_interest: dict[str, Any] | _geometry.Polygon | None = None,
        asynchronous: bool = True,
        **kwargs,
    ) -> dict:
        """
        The ``estimate_export_tiles_size`` method is an asynchronous task that
        allows estimation of the size of the tile package or the cache data
        set that you download using the :attr:`~arcgis.layers.MapImageLayer.export_tiles` operation. This
        operation can also be used to estimate the tile count in a tile
        package and determine if it will exceed the ``maxExportTileCount``
        limit set by the administrator of the service. The result of this
        operation is ``MapServiceJob``. This job response contains reference
        to ``Map Service Result`` resource that returns the total size of the
        cache to be exported (in bytes) and the number of tiles that will
        be exported.

        ==================     ====================================================================
        **Parameter**           **Description**
        ------------------     --------------------------------------------------------------------
        export_by              Required string. The criteria that will be used to select the tile
                               service levels to export. The values can be Level IDs, cache scales
                               or the Resolution (in the case of image services).
                               Values:
                                    "levelId" | "resolution" | "scale"
        ------------------     --------------------------------------------------------------------
        levels                 Required string. Specify the tiled service levels for which you want
                               to get the estimates. The values should correspond to Level IDs,
                               cache scales or the Resolution as specified in export_by parameter.
                               The values can be comma separated values or a range.

                               Example 1: 1,2,3,4,5,6,7,8,9
                               Example 2: 1-4,7-9
        ------------------     --------------------------------------------------------------------
        tile_package           Optional boolean. Allows estimating the size for either a tile
                               package or a cache raster data set. Specify the value true for tile
                               packages format and false for Cache Raster data set. The default
                               value is False
        ------------------     --------------------------------------------------------------------
        export_extent          The extent (bounding box) of the tile package or the cache dataset
                               to be exported. If extent does not include a spatial reference, the
                               extent values are assumed to be in the spatial reference of the map.
                               The default value is full extent of the tiled map service.
                               Syntax: <xmin>, <ymin>, <xmax>, <ymax>
                               Example: -104,35.6,-94.32,41
        ------------------     --------------------------------------------------------------------
        area_of_interest       Optional dictionary or Polygon. This allows exporting tiles within
                               the specified polygon areas. This parameter supersedes extent
                               parameter.

                               Example:

                                    | { "features": [{"geometry":{"rings":[[[-100,35],
                                    |       [-100,45],[-90,45],[-90,35],[-100,35]]],
                                    |       "spatialReference":{"wkid":4326}}}]}
        ------------------     --------------------------------------------------------------------
        asynchronous           Optional boolean. The estimate function is run asynchronously
                               requiring the tool status to be checked manually to force it to
                               run synchronously the tool will check the status until the
                               estimation completes.  The default is True, which means the status
                               of the job and results need to be checked manually.  If the value
                               is set to False, the function will wait until the task completes.
        ==================     ====================================================================

        :return: dictionary

        """
        if self.properties["exportTilesAllowed"] == False:
            return
        import time

        url = self._url + "/estimateExportTilesSize"
        params = {
            "f": "json",
            "levels": levels,
            "exportBy": export_by,
            "tilePackage": tile_package,
            "exportExtent": export_extent,
        }
        params["levels"] = levels
        if len(kwargs) > 0:
            for k, v in kwargs.items():
                params[k] = v
        if not area_of_interest is None:
            params["areaOfInterest"] = area_of_interest
        if asynchronous == True:
            resp: requests.Response = self._session.get(url=url, params=params)
            resp.raise_for_status()
            return resp.json()
        else:
            resp: requests.Response = self._session.get(url=url, params=params)
            resp.raise_for_status()
            exportJob = resp.json()

            path = "%s/jobs/%s" % (url, exportJob["jobId"])

            params = {"f": "json"}
            resp: requests.Response = self._session.post(url=url, data=params)
            resp.raise_for_status()
            job_response = resp.json()

            if "status" in job_response or "jobStatus" in job_response:
                status = job_response.get("status") or job_response.get("jobStatus")
                while not status == "esriJobSucceeded":
                    time.sleep(5)

                    resp: requests.Response = self._session.post(url=url, data=params)
                    resp.raise_for_status()
                    job_response = resp.json()
                    status = job_response.get("status") or job_response.get("jobStatus")
                    if status in [
                        "esriJobFailed",
                        "esriJobCancelling",
                        "esriJobCancelled",
                        "esriJobTimedOut",
                    ]:
                        print(str(job_response["messages"]))
                        raise Exception("Job Failed with status " + status)
                    else:
                        path += (
                            "/" + job_response["results"]["out_service_url"]["paramUrl"]
                        )
                        resp: requests.Response = self._session.post(
                            url=path, data={"f": "json"}
                        )
                        resp.raise_for_status()
                        out_service_resp = resp.json()["value"]
                        return out_service_resp
                path += "/" + job_response["results"]["out_service_url"]["paramUrl"]
                resp: requests.Response = self._session.post(
                    url=path, data={"f": "json"}
                )
                resp.raise_for_status()
                out_service_resp = resp.json()["value"]
                return out_service_resp
            else:
                raise Exception("No job results.")

    # ----------------------------------------------------------------------
    def export_tiles(
        self,
        levels: str,
        export_by: str = "LevelID",
        tile_package: bool = True,
        export_extent: dict[str, Any] | str | None = None,
        optimize_for_size: bool = True,
        compression: int = 75,
        area_of_interest: dict[str, Any] | _geometry.Polygon | None = None,
        asynchronous: bool = False,
        storage_format: str | None = None,
        **kwargs,
    ) -> str | dict:
        """
        The ``export_Tiles`` operation is performed as an asynchronous task and
        allows client applications to download map tiles from a server for
        offline use. This operation is performed on a ``Map Service`` that
        allows clients to export cache tiles. The result of this operation
        is a ``Map Service Job``. This job response contains a reference to the
        ``Map Service Result`` resource, which returns a URL to the resulting
        tile package (.tpk) or a cache raster dataset.
        ``export_Tiles`` can be enabled in a service by using ArcGIS Desktop
        or the ArcGIS Server Administrator Directory. In ArcGIS Desktop
        make an admin or publisher connection to the server, go to service
        properties, and enable ``Allow Clients`` to ``Export Cache Tiles`` in the
        advanced caching page of the ``Service Editor``. You can also specify
        the maximum tiles clients will be allowed to download.

        .. note::
            The default maximum allowed tile count is 100,000. To enable this capability
            using the Administrator Directory, edit the service, and set the
            properties ``exportTilesAllowed`` = ``True`` and ``maxExportTilesCount`` = 100000.

        .. note::
            In ArcGIS Server 10.2.2 and later versions, exportTiles is supported as an
            operation of the Map Server. The use of the
            ``http://Map_Service/exportTiles/submitJob`` operation is deprecated.
            You can provide arguments to the exportTiles operation as defined
            in the following parameters table:


        ==================     ====================================================================
        **Parameter**           **Description**
        ------------------     --------------------------------------------------------------------
        levels                 Required string. Specifies the tiled service levels to export. The
                               values should correspond to Level IDs, cache scales. or the
                               resolution as specified in export_by parameter. The values can be
                               comma separated values or a range. Make sure tiles are present at
                               the levels where you attempt to export tiles.
                               Example 1: 1,2,3,4,5,6,7,8,9
                               Example 2: 1-4,7-9
        ------------------     --------------------------------------------------------------------
        export_by              Required string. The criteria that will be used to select the tile
                               service levels to export. The values can be Level IDs, cache scales.
                               or the resolution.  The default is 'LevelID'.
                               Values:
                                    `levelId | resolution | scale`
        ------------------     --------------------------------------------------------------------
        tile_package           Optional boolean. Allows exporting either a tile package or a cache
                               raster data set. If the value is true, output will be in tile
                               package format, and if the value is false, a cache raster data
                               set is returned. The default value is True.
        ------------------     --------------------------------------------------------------------
        export_extent          Optional dictionary or string. The extent (bounding box) of the tile
                               package or the cache dataset to be exported. If extent does not
                               include a spatial reference, the extent values are assumed to be in
                               the spatial reference of the map. The default value is full extent
                               of the tiled map service.
                               Syntax:
                                    <xmin>, <ymin>, <xmax>, <ymax>
                               Example 1: -104,35.6,-94.32,41
                               Example 2:
                                    | {"xmin" : -109.55, "ymin" : 25.76,
                                    | "xmax" : -86.39, "ymax" : 49.94,
                                    | "spatialReference" : {"wkid" : 4326}}
        ------------------     --------------------------------------------------------------------
        optimize_for_size      Optional boolean. Use this parameter to enable compression of JPEG
                               tiles and reduce the size of the downloaded tile package or the
                               cache raster data set. Compressing tiles slightly compromises the
                               quality of tiles but helps reduce the size of the download. Try
                               sample compressions to determine the optimal compression before
                               using this feature.
                               The default value is True.
        ------------------     --------------------------------------------------------------------
        compression=75,        Optional integer. When optimize_for_size=true, you can specify a
                               compression factor. The value must be between 0 and 100. The value
                               cannot be greater than the default compression already set on the
                               original tile. For example, if the default value is 75, the value
                               of compressionQuality must be between 0 and 75. A value greater
                               than 75 in this example will attempt to up sample an already
                               compressed tile and will further degrade the quality of tiles.
        ------------------     --------------------------------------------------------------------
        area_of_interest       Optional dictionary, Polygon. The area_of_interest polygon allows
                               exporting tiles within the specified polygon areas. This parameter
                               supersedes the exportExtent parameter.

                               Example:

                                   | { "features": [{"geometry":{"rings":[[[-100,35],
                                   |   [-100,45],[-90,45],[-90,35],[-100,35]]],
                                   |   "spatialReference":{"wkid":4326}}}]}
        ------------------     --------------------------------------------------------------------
        asynchronous           Optional boolean. Default False, this value ensures the returns are
                               returned to the user instead of the user having the check the job
                               status manually.
        ------------------     --------------------------------------------------------------------
        storage_format         Optional string. Specifies the type of tile package that will be created.

                               | ``tpk`` - Tiles are stored using Compact storage format. It is supported across the ArcGIS platform.
                               | ``tpkx`` - Tiles are stored using CompactV2 storage format, which provides better performance on network shares and cloud store directories. This improved and simplified package structure type is supported by newer versions of ArcGIS products such as ArcGIS Online 7.1, ArcGIS Enterprise 10.7, and ArcGIS Runtime 100.5. This is the default.
        ==================     ====================================================================

        :return:
            A path to download file is asynchronous is ``False``. If ``True``, a dictionary is returned.
        """

        params = {
            "f": "json",
            "tilePackage": tile_package,
            "exportExtent": export_extent,
            "optimizeTilesForSize": optimize_for_size,
            "compressionQuality": compression,
            "exportBy": export_by,
            "levels": levels,
        }
        if not storage_format is None:
            storage_lu = {
                "esriMapCacheStorageModeCompact": "esriMapCacheStorageModeCompact",
                "esrimapcachestoragemodecompact": "esriMapCacheStorageModeCompact",
                "esriMapCacheStorageModeCompactV2": "esriMapCacheStorageModeCompactV2",
                "esrimapcachestoragemodecompactv2": "esriMapCacheStorageModeCompactV2",
                "tpk": "esriMapCacheStorageModeCompact",
                "tpkx": "esriMapCacheStorageModeCompactV2",
            }
            params["storageFormat"] = storage_lu[str(storage_format).lower()]
        if len(kwargs) > 0:
            for k, v in kwargs.items():
                params[k] = v
        url = self._url + "/exportTiles"
        if area_of_interest is not None:
            params["areaOfInterest"] = area_of_interest

        if asynchronous == True:
            resp: requests.Response = self._session.get(url=url, params=params)
            resp.raise_for_status()
            return resp.json()
        else:
            resp: requests.Response = self._session.get(url=url, params=params)
            resp.raise_for_status()
            exportJob = resp.json()

            path = "%s/jobs/%s" % (url, exportJob["jobId"])

            params = {"f": "json"}
            resp: requests.Response = self._session.post(url=path, data=params)
            resp.raise_for_status()
            job_response = resp.json()

            if "status" in job_response or "jobStatus" in job_response:
                status = job_response.get("status") or job_response.get("jobStatus")
                while not status == "esriJobSucceeded":
                    time.sleep(5)

                    resp: requests.Response = self._session.post(url=path, data=params)
                    resp.raise_for_status()
                    job_response = resp.json()
                    status = job_response.get("status") or job_response.get("jobStatus")
                    if status in [
                        "esriJobFailed",
                        "esriJobCancelling",
                        "esriJobCancelled",
                        "esriJobTimedOut",
                    ]:
                        print(str(job_response["messages"]))
                        raise Exception("Job Failed with status " + status)
            else:
                raise Exception("No job results.")

            if "results" in job_response:
                allResults = job_response["results"]

                for k, v in allResults.items():
                    if k == "out_service_url":
                        value = list(v.values())[0]
                        params = {"f": "json"}
                        resp: requests.Response = self._session.get(
                            url=path + "/" + value, params=params
                        )
                        resp.raise_for_status()
                        gpRes = resp.json()
                        if tile_package == True:
                            resp: requests.Response = self._session.get(
                                url=gpRes["value"]
                            )
                            resp.raise_for_status()
                            gpOutput = resp.json()
                            files = []
                            for f in gpOutput["files"]:
                                name = f["name"]
                                dlURL = f["url"]
                                files.append(
                                    handle_response(
                                        resp=self._session.get(
                                            url=dlURL, params=params
                                        ),
                                        out_path=tempfile.gettempdir(),
                                        file_name=name,
                                        try_json=True,
                                    )
                                )
                            return files
                        else:
                            resp: requests.Response = self._session.get(
                                url=gpRes["value"]
                            )
                            resp.raise_for_status()
                            return resp.json()["folders"]
                    else:
                        return None
            elif "output" in job_response:
                allResults = job_response["output"]
                if allResults["itemId"]:
                    return _gis.Item(gis=self._gis, itemid=allResults["itemId"])
                else:
                    if self._gis._portal.is_arcgisonline:
                        return [
                            self._session.get(
                                url, try_json=False, add_token=False
                            ).json()
                            for url in allResults["outputUrl"]
                        ]
                    else:
                        return [
                            self._session.get(url, try_json=False).json()
                            for url in allResults["outputUrl"]
                        ]
            else:
                raise Exception(job_response)


###########################################################################
