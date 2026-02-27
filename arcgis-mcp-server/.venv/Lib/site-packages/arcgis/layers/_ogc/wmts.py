from __future__ import annotations
import requests
import uuid
from urllib.parse import urlencode, urlparse, urlunparse, parse_qs, ParseResult
import xml.etree.cElementTree as ET
from io import BytesIO

from arcgis.gis import GIS
from arcgis import env as _env

from ._base import BaseOGC


###########################################################################
class WMTSLayer(BaseOGC):
    """
    Represents a Web Map Tile Service, which is an OGC web service endpoint.

    ===============     ====================================================================
    **Parameter**        **Description**
    ---------------     --------------------------------------------------------------------
    url                 Required string. The web address of the endpoint.
    ---------------     --------------------------------------------------------------------
    version             Optional String. The version number of the WMTS service.  The default is `1.0.0`
    ---------------     --------------------------------------------------------------------
    gis                 Optional :class:`~arcgis.gis.GIS` . The GIS used to reference the service by. The arcgis.env.active_gis is used if not specified.
    ---------------     --------------------------------------------------------------------
    copyright           Optional String. Describes limitations and usage of the data.
    ---------------     --------------------------------------------------------------------
    opacity             Optional Float.  This value can range between 1 and 0, where 0 is 100 percent transparent and 1 is completely opaque.
    ---------------     --------------------------------------------------------------------
    scale               Optional Tuple. The min/max scale of the layer where the positions are: (min, max) as float values.
    ---------------     --------------------------------------------------------------------
    title               Optional String. The title of the layer used to identify it in places such as the Legend and Layer List widgets.
    ===============     ====================================================================



    """

    _gis = None
    _con = None
    _url = None
    _reader = None
    _cap_reader = None
    _properties = None

    # ----------------------------------------------------------------------
    def __init__(self, url, version="1.0.0", gis=None, **kwargs):
        super(WMTSLayer, self)
        self._gis = gis or _env.active_gis or GIS()
        self._con = self._gis._con
        assert isinstance(
            self._gis, GIS
        ), "gis is required and must be of type arcgis.GIS"
        self._id = kwargs.pop("id", uuid.uuid4().hex)
        self._version = version
        self._session = self._gis.session
        self._title = kwargs.pop("title", "WMTS Layer")
        self._url = url.rstrip("/")
        self._add_token = str(self._con._auth).lower() == "builtin"
        self._min_scale, self._max_scale = kwargs.pop("scale", (0, 0))
        self._opacity = kwargs.pop("opacity", 1)
        self._type = "WebTiledLayer"
        self._properties = self.properties
        self._lyr_identifiers = self._get_lyr_identifiers()
        self._spatial_reference = self._get_spatial_reference()

    def _get_spatial_reference(self):
        """
        Returns the spatial reference of the layer
        """
        if "BoundingBox" in self._properties["Capabilities"]["Contents"]["Layer"]:
            crs = self._properties["Capabilities"]["Contents"]["Layer"]["BoundingBox"][
                "@crs"
            ]
            return int(crs.split(":")[-1])
        return 4326

    def _get_lyr_identifiers(self):
        """
        Returns the identifiers of the layers
        """
        if isinstance(self._properties["Capabilities"]["Contents"]["Layer"], list):
            return [
                lyr["Identifier"]
                for lyr in self._properties["Capabilities"]["Contents"]["Layer"]
            ]
        else:
            # Only one layer so it's a dict
            return [self._properties["Capabilities"]["Contents"]["Layer"]["Identifier"]]

    def _get_capabilities_xml(self, urls: list[str]) -> str:
        """
        Retrieves the capabilities XML from the WMTS service
        using the first URL that returns a valid response,
        raises an exception if none of the URLs return a valid response
        """
        # use gis session and vanilla requests
        # some gis sessions change the request
        # also use vanilla requests for a unencumbered request
        get_funcs = [self._session.get, requests.get]
        for url in urls:
            for get_func in get_funcs:
                try:
                    resp: requests.Response = get_func(url)
                    resp.raise_for_status()
                    if (
                        "<Capabilities xmlns" in resp.text
                        and "<?xml" not in resp.text.lower()
                    ):
                        # add the xml tag to beginning of text and return
                        resp_text = resp.text
                        xml_tag = (
                            f'<?xml version="{self._version}" encoding="UTF-8"?>\n'
                        )
                        resp_text = xml_tag + resp_text
                        return resp_text
                    if "<?xml" not in resp.text.lower():
                        raise ValueError(
                            f"Could not retrieve valid XML from WebMap Tile Service Capabilities Endpoint; Got:\n{resp.text}"
                        )
                    return resp.text
                except (requests.exceptions.RequestException, ValueError):
                    pass
        raise Exception("Could not retrieve valid XML from any of the provided URLs")

    # ----------------------------------------------------------------------
    @property
    def properties(self) -> dict:
        """
        Returns the properties of the Layer.

        :return: dict
        """
        if self._properties:
            return self._properties

        capabilities_urls = [
            self._capabilities_url(
                service_url=self._url,
                version=self._version,
                vendor_kwargs={"token": self._con.token} if self._add_token else None,
            )
        ]
        if self._add_token:
            # try without token if token call fails
            capabilities_urls.append(
                self._capabilities_url(service_url=self._url, version=self._version)
            )
        text = self._get_capabilities_xml(capabilities_urls)

        self._properties = self._get_dict_from_xml(text)
        return self._properties

    @staticmethod
    def _get_dict_from_xml(xml_str):
        sss = BytesIO()
        sss.write(xml_str.encode())
        sss.seek(0)
        tree = ET.XML(text=sss.read())
        # TODO try ET.fromstring(xml_str) once we have unit tests
        return WMTSLayer._xml_to_dictionary(tree)

    @staticmethod
    def _capabilities_url(service_url, version="1.0.0", vendor_kwargs=None):
        """Return a capabilities url"""
        pieces = urlparse(service_url)
        args = parse_qs(pieces.query)
        args["service"] = args.get("service", "WMTS")
        args["request"] = args.get("request", "GetCapabilities")
        args["version"] = args.get("version", version)
        args.update(vendor_kwargs or {})
        pieces = pieces._replace(query=urlencode(args, doseq=True))
        return urlunparse(pieces)

    @staticmethod
    def _xml_to_dictionary(t):
        """converts the xml to a dictionary object (recursively)"""

        def _format_tags(tag):
            """attempts to format tags by stripping out the {text} from the keys"""
            import re

            regex = r".*\}(.*)"
            matches = re.search(regex, tag)
            if matches:
                return matches.groups()[0]
            return tag

        import json
        from collections import defaultdict

        d = {_format_tags(t.tag): {} if t.attrib else None}
        children = list(t)
        if children:
            dd = defaultdict(list)
            for dc in map(WMTSLayer._xml_to_dictionary, children):
                for k, v in dc.items():
                    dd[_format_tags(k)].append(v)
            d = {
                _format_tags(t.tag): {
                    _format_tags(k): v[0] if len(v) == 1 else v for k, v in dd.items()
                }
            }
        if t.attrib:
            d[_format_tags(t.tag)].update(
                [(f"@{_format_tags(k)}", v) for k, v in t.attrib.items()]
            )
        if t.text:
            text = t.text.strip()
            if children or t.attrib:
                if text:
                    d[_format_tags(t.tag)]["#text"] = text
            else:
                d[_format_tags(t.tag)] = text
        removals = [
            "{http://www.opengis.net/wmts/1.0}",
            "{http://www.opengis.net/ows/1.1}",
            "{http://www.w3.org/1999/xlink}",
        ]
        d = json.dumps(d)
        for remove in removals:
            d = d.replace(remove, "")
        return json.loads(d)

    # ----------------------------------------------------------------------
    @property
    def _lyr_json(self):
        """Represents the Map's widget JSON format"""
        return {
            "id": self._id,
            "title": self._title or "WMTS Layer",
            "url": self._url,
            "version": self._version,
            "minScale": self.scale[0],
            "maxScale": self.scale[1],
            "opacity": self.opacity,
            "type": self._type,
        }

    @staticmethod
    def _get_operational_layer_config(url: str, properties: dict, idx=None) -> dict:
        """Returns the operational layer configuration"""
        layer = None
        tile_matrix = None
        tile_matrix_identifier = None

        if idx is not None and isinstance(
            properties["Capabilities"]["Contents"]["Layer"], (list, tuple)
        ):
            layer = properties["Capabilities"]["Contents"]["Layer"][idx]
        elif isinstance(properties["Capabilities"]["Contents"]["Layer"], (list, tuple)):
            layer = properties["Capabilities"]["Contents"]["Layer"][0]
        elif isinstance(properties["Capabilities"]["Contents"]["Layer"], (dict)):
            layer = properties["Capabilities"]["Contents"]["Layer"]
            tile_matrix = properties["Capabilities"]["Contents"]["TileMatrixSet"]
            if isinstance(tile_matrix, (list, tuple)):
                tile_matrix = tile_matrix[0]
        else:
            raise ValueError("Could not parse the results properly.")

        # Get the tile_matrix
        if tile_matrix is None:
            tile_matrix_identifier = layer["TileMatrixSetLink"]["TileMatrixSet"]
            for t in properties["Capabilities"]["Contents"]["TileMatrixSet"]:
                if t["Identifier"] == tile_matrix_identifier:
                    tile_matrix = t
                    break
        resource_url_template = (
            layer["ResourceURL"][0]["@template"]
            if isinstance(layer["ResourceURL"], (list, tuple))
            else layer["ResourceURL"]["@template"]
        )
        url_template = (
            resource_url_template.replace("{TileMatrix}", "{level}")
            .replace("{Style}", layer["Style"]["Identifier"])
            .replace("{TileRow}", "{row}")
            .replace("{TileCol}", "{col}")
            .replace("{TileMatrixSet}", tile_matrix["Identifier"])
        )
        if "Dimension" in layer:
            url_template = url_template.replace("{Time}", layer["Dimension"]["Default"])
        bounding_box_name = (
            "BoundingBox" if "BoundingBox" in layer else "WGS84BoundingBox"
        )
        fullExtent = [
            float(coord)
            for coord in layer[bounding_box_name]["LowerCorner"].strip().split(" ")
        ] + [
            float(coord)
            for coord in layer[bounding_box_name]["UpperCorner"].strip().split(" ")
        ]
        lods = []
        METER_PER_PIXEL_AT_SCALE_1 = (
            0.00028  # Constant for pixel size in meters at scale denominator = 1
        )
        EARTH_CIRCUMFERENCE = 40075000  # Approximate Earth circumference in meters
        METERS_PER_DEGREE = EARTH_CIRCUMFERENCE / 360

        for l in tile_matrix["TileMatrix"]:
            scale_denominator = float(l["ScaleDenominator"])
            resolution_meters = scale_denominator * METER_PER_PIXEL_AT_SCALE_1
            resolution_degrees = resolution_meters / METERS_PER_DEGREE

            lods.append(
                {
                    "level": int(l["Identifier"]),
                    "levelValue": l["Identifier"],
                    "resolution": resolution_degrees,  # Degrees per pixel for Map Viewer
                    "scale": scale_denominator,  # Scale remains unchanged
                }
            )
        if bounding_box_name == "WGS84BoundingBox":
            spatial_reference = {"wkid": 4326}
        else:
            spatial_reference = {
                "wkid": int(layer[bounding_box_name]["@crs"].split(":")[-1])
            }

        return {
            "templateUrl": url_template,
            "copyright": "",
            "fullExtent": {
                "xmin": fullExtent[0],
                "ymin": fullExtent[1],
                "xmax": fullExtent[2],
                "ymax": fullExtent[3],
                "spatialReference": spatial_reference,
            },
            "tileInfo": {
                "rows": tile_matrix["TileMatrix"][0]["TileHeight"],
                "cols": tile_matrix["TileMatrix"][0]["TileWidth"],
                "dpi": 96,
                "origin": {
                    "x": (fullExtent[2] + fullExtent[0]) / 2,
                    "y": (fullExtent[3] + fullExtent[1]) / 2,
                    "spatialReference": spatial_reference,
                },
                "spatialReference": spatial_reference,
                "lods": lods,
            },
            "wmtsInfo": {
                "url": url,
                "layerIdentifier": layer["Identifier"],
                "tileMatrixSet": [tile_matrix["Identifier"]],
            },
            "title": layer["Title"],
        }

    @property
    def __text__(self) -> dict:
        """gets the item's text properties for the first layer"""
        return self._get_operational_layer_config(self._url, self.properties)

    @property
    def _operational_layer_json(self) -> dict:
        """Represents the Map's JSON format for the first layer"""
        return self.__text__

    def operational_layer_json(self, identifier: str) -> dict:
        """
        Represents the JSON Format for the specified layer.

        ===============     ====================================================================
        **Parameter**        **Description**
        ---------------     --------------------------------------------------------------------
        identifier          Required string. The layer's Identifier to get the JSON format for.

                            You can find this by looping through the layers in the `properties` attribute.

                            ex:
                            ```
                            for lyr in wmts.properties["Capabilities"]["Contents"]["Layer"]:
                                print(lyr["Identifier"])
                            ```
        ===============     ====================================================================

        :return: dict
        """
        # User the property to get the layer index
        layer_index = self._lyr_identifiers.index(identifier)

        return self._get_operational_layer_config(
            self._url, self.properties, layer_index
        )
