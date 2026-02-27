from __future__ import annotations
import uuid

from arcgis.gis import GIS, Item
from arcgis import env as _env
import pandas as pd
from ._base import BaseOpenData


###########################################################################
class CSVLayer(BaseOpenData):
    """
    Represents a CSV File Hosted on a Server.


    ===============     ====================================================================
    **Parameter**        **Description**
    ---------------     --------------------------------------------------------------------
    url                 Required String or Item. The web address or :class:`~arcgis.gis.Item` to the CSV resource.
    ---------------     --------------------------------------------------------------------
    gis                 Optional :class:`~arcgis.gis.GIS`. The GIS used to reference the service. The :attr:`~arcgis.env.active_gis` is used if not specified.
    ---------------     --------------------------------------------------------------------
    copyright           Optional String. Describes limitations and usage of the data.
    ---------------     --------------------------------------------------------------------
    delimiter           Optional String. The separator value. This can be the following:

                            , (comma), ' ' (space), | (pipe), \\r (tab), or ; (semicolon).
    ---------------     --------------------------------------------------------------------
    fields              Optional List. An array of dictionaries containing the field information.
    ---------------     --------------------------------------------------------------------
    opacity             Optional Float.  This value can range between 1 and 0, where 0 is 100 percent transparent and 1 is completely opaque.
    ---------------     --------------------------------------------------------------------
    scale               Optional Tuple. The min/max scale of the layer where the positions are: (min, max) as float values.
    ---------------     --------------------------------------------------------------------
    sql_expression      Optional String. Optional query string to apply to the layer when displayed on the widget or web map.
    ---------------     --------------------------------------------------------------------
    title               Optional String. The title of the layer used to identify it in places such as the Legend and Layer List widgets.
    ===============     ====================================================================

    """

    _url = None
    _gis = None
    _data = None
    _nrows = 15  # default number of rows to peak at.
    _id = None
    _renderer = None
    _latitude = None
    _longitude = None
    _type = "CSV"

    # ----------------------------------------------------------------------
    def __init__(self, url: Item | str, gis: GIS | None = None, **kwargs):
        """initializer"""
        super(CSVLayer, self)
        if isinstance(url, str):
            self._url = url
            self._item = None
        elif isinstance(url, Item):
            self._item = url
            self._url = None
        if self._item:

            self._gis = gis or _env.active_gis or self._item._gis or GIS()
        else:
            self._gis = gis or _env.active_gis or GIS()

        self._copyright = kwargs.pop("copyright", None)
        self._delimiter = kwargs.pop("delimiter", ",")
        self._fields = kwargs.pop("fields", None)
        self._sql = kwargs.pop("sql_expression", None)
        self._id = kwargs.pop("id", uuid.uuid4().hex)
        self._title = kwargs.pop("title", "CSV Layer")
        self._min_scale, self._max_scale = kwargs.pop("scale", (0, 0))
        self._opacity = kwargs.pop("opacity", 1)

    # ----------------------------------------------------------------------
    def __str__(self):
        if self._item:
            return f"<CSV @ {self._item.itemid}>"
        return f"< CSV @ {self._url} >"

    # ----------------------------------------------------------------------
    @property
    def latitude(self) -> str:
        """
        The latitude field name. If not specified, the class will look for
        following field names in the CSV source:
            "lat", "latitude", "y", "ycenter", "latitude83", "latdecdeg", "POINT-Y"
        """
        auto_lat = [
            "lat",
            "latitude",
            "y",
            "ycenter",
            "latitude83",
            "latdecdeg",
            "point-y",
        ]
        if self._latitude is None:
            for f in self.fields:
                if f["name"].lower() in auto_lat:
                    self._latitude = f["name"]
                    break
        return self._latitude

    # ----------------------------------------------------------------------
    @latitude.setter
    def latitude(self, value):
        if value != self._latitude and value in [f["name"] for f in self.fields]:
            self._latitude = value

    # ----------------------------------------------------------------------
    @property
    def longitude(self) -> str:
        """
        The longitude field name. If not specified, the `CSVLayer` will
        look for following field names in the CSV source:
            "lon", "lng","long", "longitude", "x", "xcenter", "longitude83", "longdecdeg", "POINT-X"
        """
        auto_lat = [
            "lon",
            "lng",
            "long",
            "longitude",
            "x",
            "xcenter",
            "longitude83",
            "longdecdeg",
            "point-x",
        ]
        if self._longitude is None:
            for f in self.fields:
                if f["name"].lower() in auto_lat:
                    self._longitude = f["name"]
                    break
        return self._longitude

    # ----------------------------------------------------------------------
    @longitude.setter
    def longitude(self, value):
        if value != self._longitude and value in [f["name"] for f in self.fields]:
            self._longitude = value

    # ----------------------------------------------------------------------
    @property
    def renderer(self) -> dict:
        """
        Get/Set the Renderer of the CSV Layer

        :return:
            A ``dict`` like object used to update and alter JSON

        """

        if self._renderer is None:
            symbol = {
                "type": "esriSMS",
                "color": [226, 29, 145, 158],
                "angle": 0,
                "xoffset": 0,
                "yoffset": 0,
                "size": 12,
                "style": "esriSMSCircle",
                "outline": {
                    "type": "esriSLS",
                    "color": [0, 0, 0, 255],
                    "width": 0.75,
                    "style": "esriSLSSolid",
                },
            }
            self._renderer = {"type": "simple", "symbol": symbol}
        return self._renderer

    # ----------------------------------------------------------------------
    @renderer.setter
    def renderer(self, value):
        """
        Get/Set the Renderer of the CSV Layer

        :return:
            ```InsensitiveDict```: A case-insensitive ``dict`` like object used to update and alter JSON
            A variants of a case-less dictionary that allows for dot and bracket notation.

        """
        if isinstance(value, dict):
            self._renderer = dict(value)
        elif value is None:
            self._renderer = None
        self._refresh = value

    # ----------------------------------------------------------------------
    @property
    def delimiter(self) -> str:
        r"""
        Gets/Sets the delimiter for the CSV Layer.  The default is `,`

        ===========   ==========================================
        **Values**    **Description**
        -----------   ------------------------------------------
        ,             comma
        -----------   ------------------------------------------
        " "           space
        -----------   ------------------------------------------
        ;             semicolon
        -----------   ------------------------------------------
        `\|`          pipe
        -----------   ------------------------------------------
        `\r`          tab
        ===========   ==========================================

        :return: String

        """
        if self._delimiter is None:
            self._delimiter = ","
        return self._delimiter

    # ----------------------------------------------------------------------
    @delimiter.setter
    def delimiter(self, value: str):
        """
        See main ``delimiter`` property docstring
        """
        if value in [",", " ", ";", "|", "\r"] and self._delimiter != value:
            self._delimiter = value

    # ----------------------------------------------------------------------
    @property
    def fields(self) -> list[str]:
        """
        Returns the fields values for the CSV source.

        :return: list of strings
        """
        import numpy as np
        import datetime

        if self._data is None and self._fields is None:
            fields = []
            self._data = self._df(True)
            for col in self._data.columns:
                try:
                    idx = self._data[col].first_valid_index()
                    col_val = self._data[col].loc[idx]
                except:
                    col_val = ""
                if isinstance(col_val, (str, str)):
                    fields.append({"name": col, "type": "string", "alias": col})
                elif isinstance(
                    col_val,
                    (
                        datetime.datetime,
                        pd.Timestamp,
                        np.datetime64,
                    ),
                ):
                    fields.append({"name": col, "type": "date", "alias": col})
                elif isinstance(col_val, (np.int32, np.int16, np.int8)):
                    fields.append({"name": col, "type": "long", "alias": col})
                elif isinstance(col_val, (int, np.int64)):
                    fields.append({"name": col, "type": "integer", "alias": col})
                elif isinstance(col_val, (float, np.float64)):
                    fields.append({"name": col, "type": "double", "alias": col})
                elif isinstance(col_val, (np.float32)):
                    fields.append({"name": col, "type": "single", "alias": col})
            self._fields = fields
        return self._fields

    # ----------------------------------------------------------------------
    @property
    def _lyr_json(self):
        """Represents the Map's JSON format"""
        add_layer = {
            "type": self._type,
            "delimiter": self.delimiter,
            "copyright": self.copyright or "",
            "definitionExpression": self.sql_expression or "",
            "layerDefinition": {
                "fields": self.fields,
                "objectIDField": "__OBJECTID",
                "drawingInfo": {"renderer": self.renderer},
            },
            "id": self._id,
            "title": self.title,
            "opacity": self.opacity,
            "maxScale": self.scale[1],
            "minScale": self.scale[0],
            "locationInfo": {
                "locationType": "coordinates",
                "longitudeFieldName": self.longitude,
                "latitudeFieldName": self.latitude,
            },
        }
        if self._item:
            add_layer["portalItem"] = {"id": self._item.itemid}
        else:
            add_layer["url"] = self._url
        return add_layer

    @property
    def _operational_layer_json(self):
        """Represents the Map's JSON format"""
        return self._lyr_json

    # ----------------------------------------------------------------------
    def _df(self, glance=False):
        """returns the data as a pd.DataFrame"""
        if glance:
            nrows = self._nrows
        else:
            nrows = None
        if self._url:
            return pd.read_csv(
                self._url,
                sep=self.delimiter,
                nrows=nrows,
                parse_dates=True,
            )
        elif self._item:
            if self._item._gis._con.token:
                url = f"{self._item._gis._portal.resturl}content/items/{self._item.itemid}/data?token={self._item._gis._con.token}"
            else:
                url = f"{self._item._gis._portal.resturl}content/items/{self._item.itemid}/data"
            return pd.read_csv(
                url,
                sep=self.delimiter,
                nrows=nrows,
                parse_dates=True,
            )
        else:
            return None

    # ----------------------------------------------------------------------
    @property
    def df(self) -> pd.DataFrame:
        """
        returns the CSV file as a DataFrame

        :return: `Pandas DataFrame <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`_
        """
        return self._df(False)
