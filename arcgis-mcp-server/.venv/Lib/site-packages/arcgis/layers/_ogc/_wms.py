from __future__ import annotations
import uuid
import re
from io import BytesIO
import xml.etree.cElementTree as ET
from urllib.parse import urlencode, urlparse, urlunparse, parse_qs, ParseResult
import requests
from arcgis.gis import GIS
from arcgis import env as _env
from ._base import BaseOGC


###########################################################################
class WMSLayer(BaseOGC):
    """
    Represents a Web Map Service, which is an OGC web service endpoint.

    ===============     ====================================================================
    **Parameter**        **Description**
    ---------------     --------------------------------------------------------------------
    url                 Required string. The administration URL for the ArcGIS Server.
    ---------------     --------------------------------------------------------------------
    version             Optional String. The version number of the WMS service.  The default is `1.3.0`.
    ---------------     --------------------------------------------------------------------
    gis                 Optional :class:`~arcgis.gis.GIS`. The GIS used to reference the service by. The arcgis.env.active_gis is used if not specified.
    ---------------     --------------------------------------------------------------------
    copyright           Optional String. Describes limitations and usage of the data.
    ---------------     --------------------------------------------------------------------
    scale               Optional Tuple. The min/max scale of the layer where the positions are: (min, max) as float values.
    ---------------     --------------------------------------------------------------------
    opacity             Optional Float.  This value can range between 1 and 0, where 0 is 100 percent transparent and 1 is completely opaque.
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
    _type = "WMS"

    # ----------------------------------------------------------------------
    def __init__(self, url, version="1.3.0", gis=None, **kwargs):
        super(WMSLayer, self)
        if gis:
            gis = gis
        elif gis is None and _env.active_gis:
            gis = _env.active_gis
        else:
            gis = GIS()
        assert isinstance(gis, GIS)
        self._id = kwargs.pop("id", uuid.uuid4().hex)
        self._version = version
        self._session = gis.session
        self._title = kwargs.pop("title", "WMS Layer")
        self._gis = gis
        if url[-1] == "/":
            url = url[:-1]
        self._url = url
        self._con = gis._con
        self._add_token = str(self._con._auth).lower() == "builtin"
        self._opacity = kwargs.pop("opacity", 1)
        self._min_scale, self._max_scale = kwargs.pop("scale", (0, 0))

    # ----------------------------------------------------------------------
    @property
    def properties(self) -> dict:
        """
        Returns the properties of the Layer, including sublayers if present.

        :return: dict
        """
        if self._properties is None:
            # Construct the WMS GetCapabilities request
            if self._add_token:
                url = self._capabilities_url(
                    service_url=self._url, vendor_kwargs={"token": self._con.token}
                )
            else:
                url = self._capabilities_url(service_url=self._url)

            # First trial and error
            try:
                # Fetch and parse the response
                resp: requests.Response = self._session.get(url=url)
                resp.raise_for_status()
                text = resp.text
            except requests.HTTPError as e:
                url = self._capabilities_url(service_url=self._url)
                resp: requests.Response = self._session.get(url=url)
                resp.raise_for_status()
                text = resp.text

            if "<html>" in text.lower():
                raise Exception("Invalid response from Web Map Service")
            elif "<?xml version=" not in text:
                raise Exception(
                    "Could not parse response as XML. Please check the WMS service."
                )

            # Use the detected encoding
            sss = BytesIO(text.encode("UTF-8"))
            sss.seek(0)
            tree = ET.parse(sss)
            root = tree.getroot()

            # Process the XML to a dictionary
            self._properties = self._xml_to_dictionary(root)

        return self._properties

    # ---------------------------------------------------------------------
    @property
    def _extents(self) -> list:
        """list of extents from the service in the form of
        [[minx, miny], [maxx, maxy]] for each entry in the list
        """
        try:
            bboxes = self.properties.WMS_Capabilities.Capability.Layer.BoundingBox
            output = []
            for bbox in bboxes:
                output.append(
                    [
                        [float(bbox["@minx"]), float(bbox["@miny"])],
                        [float(bbox["@maxx"]), float(bbox["@maxy"])],
                    ]
                )
            return output
        except Exception:
            return [
                [[0, 0], [0, 0]],
            ]

    @property
    def _spatial_references(self) -> list:
        try:
            crss = self.properties.WMS_Capabilities.Capability.Layer.CRS
            output = []
            for crs_str in crss:
                output += [int(crs_num) for crs_num in re.findall(r"[0-9]+", crs_str)]
            return output
        except Exception as e:
            return []

    # ----------------------------------------------------------------------
    @property
    def layers(self) -> list:
        """Returns all layers from the WMS service, excluding the top-level group layer if present."""
        try:
            # Access the base layer structure
            layers = self.properties["WMS_Capabilities"]["Capability"]["Layer"]

            # Flatten the layer hierarchy
            def extract_layers(layer):
                """Recursively extract layers and sublayers, excluding the top-level group layer."""
                layer_list = []
                if "Layer" in layer:  # Check for nested sublayers
                    sublayers = layer["Layer"]
                    if isinstance(sublayers, list):
                        for sublayer in sublayers:
                            layer_list.extend(extract_layers(sublayer))
                    else:
                        layer_list.extend(extract_layers(sublayers))
                elif (
                    "Name" in layer
                ):  # Only include actual layers with a "Name" property
                    layer_list.append(layer)
                return layer_list

            # If the top layer is a group, only process its sublayers
            if isinstance(layers, list):
                all_layers = []
                for layer in layers:
                    all_layers.extend(extract_layers(layer))
            else:
                all_layers = extract_layers(layers)

            return all_layers
        except KeyError:
            return []  # Return an empty list if no layers are found

    # ----------------------------------------------------------------------
    def _capabilities_url(self, service_url: str, vendor_kwargs: dict = None) -> str:
        """Return a capabilities url"""
        pieces = urlparse(service_url)
        args = parse_qs(pieces.query)
        if "service" not in args:
            args["service"] = "WMS"
        if "request" not in args:
            args["request"] = "GetCapabilities"
        if "version" not in args:
            args["version"] = self._version
        if vendor_kwargs:
            args.update(vendor_kwargs)
        query = urlencode(args, doseq=True)
        pieces = ParseResult(
            pieces.scheme,
            pieces.netloc,
            pieces.path,
            pieces.params,
            query,
            pieces.fragment,
        )
        return urlunparse(pieces)

    # ----------------------------------------------------------------------
    def _format_tags(self, tag: str) -> str:
        """attempts to format tags by stripping out the {text} from the keys"""
        import re

        regex = r".*\}(.*)"
        matches = re.search(regex, tag)
        if matches:
            return matches.groups()[0]
        return tag

    # ----------------------------------------------------------------------
    def _xml_to_dictionary(self, t) -> dict:
        """Converts the XML tree to a nested dictionary structure, handling namespaces and nested layers."""
        from collections import defaultdict

        def clean_tag(tag):
            # Removes namespace if present
            return tag.split("}")[-1] if "}" in tag else tag

        def fix_encoding(value: str) -> str:
            """
            Fix any encoding issues in the text.
            Attempts to decode incorrectly encoded characters and re-encode them to UTF-8.
            """
            try:
                return value.encode("latin1").decode("utf-8", errors="ignore")
            except (UnicodeDecodeError, UnicodeEncodeError):
                return value  # Return as-is if decoding fails

        d = {clean_tag(t.tag): {} if t.attrib else None}
        children = list(t)

        # Recursively process child elements
        if children:
            child_dict = defaultdict(list)
            for dc in map(self._xml_to_dictionary, children):
                for k, v in dc.items():
                    child_dict[clean_tag(k)].append(v)

            d = {
                clean_tag(t.tag): {
                    clean_tag(k): v[0] if len(v) == 1 else v
                    for k, v in child_dict.items()
                }
            }

        # Process attributes
        if t.attrib:
            d[clean_tag(t.tag)].update(
                {f"@{clean_tag(k)}": v for k, v in t.attrib.items()}
            )

        # Add element text if it exists
        if t.text:
            text = t.text.strip()
            if text:
                # Fix encoding issues in the text
                text = fix_encoding(text)
                if children or t.attrib:
                    d[clean_tag(t.tag)]["#text"] = text
                else:
                    d[clean_tag(t.tag)] = text

        return d

    # ----------------------------------------------------------------------
    @property
    def _lyr_json(self) -> dict:
        """Represents the Map's widget JSON format"""
        layers = self.layers or []  # Ensure layers is a list, even if empty

        # Generate the JSON with sublayer information, handling potential missing 'Name' key
        return {
            "type": self._type,
            "id": self._id,
            "title": self._title or "WMS Layer",
            "url": self._url,
            "version": self._version,
            "sublayers": [{"name": lyr.get("Name", "Unnamed Layer")} for lyr in layers],
            "minScale": self.scale[0] if self.scale else None,
            "maxScale": self.scale[1] if self.scale else None,
            "opacity": self.opacity,
        }

    @property
    def _operational_layer_json(self) -> dict:
        """Represents the Map's JSON format"""
        new_layer = self._lyr_json
        layers = self.layers or []  # Ensure layers is a list, even if empty

        # Add sublayer names and titles, with default values for safety
        new_layer["layers"] = [
            {
                "name": subLyr.get("Name", "Unnamed Layer"),
                "title": subLyr.get("Title", "Untitled Layer"),
            }
            for subLyr in layers
        ]

        # Default visibleLayers to an empty list, then add all layers
        new_layer["visibleLayers"] = [lyr["name"] for lyr in new_layer["layers"]]

        # Safely set the extent and spatial references with default values if missing
        new_layer["extent"] = (
            self._extents[0] if self._extents and len(self._extents) > 0 else {}
        )
        new_layer["spatialReferences"] = self._spatial_references or []

        return new_layer
