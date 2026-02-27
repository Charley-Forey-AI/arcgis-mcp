import re
from arcgis._impl.common._arcgis2geojson import ringIsClockwise, closeRing
from typing import Any


def convert_polygon(geo_json: dict[str, Any], sr=None) -> dict[str, Any]:
    """converts a geojson polygon/multipolygon to esri json polygon.

    backwards compatible with `"crs"` in geo_json:
    ```
        {
           "type": "Polygon",
           "coordinates": [[...]],
           "crs": {"type": "name", "properties": {"name": "EPSG:4326"}}
        }
    ```
    """
    if "crs" in geo_json:
        # support for GeoJSON's legacy `crs` property
        if geo_json["crs"]["type"] == "name":
            try:
                # currently only supports EPSG codes
                m = re.search(
                    r"EPSG[:/](\d+)", geo_json["crs"]["properties"]["name"].upper()
                )
                wkid = int(m.group(1))
                sr = {"wkid": wkid}
            except (ValueError, IndexError):
                raise ValueError(f"Invalid CRS: {geo_json['crs']}")
        elif geo_json["crs"]["type"] == "link":
            raise NotImplementedError(
                "GeoJSON with `crs` type `link` is not supported."
            )
    elif sr is None:
        sr = {"wkid": 4326}
    gtype = geo_json.get("type").lower().strip()
    if gtype == "polygon":
        polys = [geo_json["coordinates"]]  # wrap as multipolygon
    elif gtype == "multipolygon":
        polys = geo_json["coordinates"]
    else:
        raise ValueError(f"Unsupported polygon type: {gtype!r}")

    rings = []  # flat list of all rings for Esri JSON

    for poly in polys:
        if not poly:
            continue  # skip empty parts

        # outer ring: GeoJSON outer is CCW -> Esri JSON outer is CW
        outer = closeRing(poly[0])
        if not ringIsClockwise(outer):  # GeoJSON outer is CCW -> flip
            outer = outer[::-1]
        rings.append(outer)

        # holes: GeoJSON holes are CW -> Esri JSON holes are CCW
        for hole in poly[1:]:
            hole = closeRing(hole)
            if ringIsClockwise(hole):  # GeoJSON hole is CW -> flip
                hole = hole[::-1]
            rings.append(hole)

    max_dim = len(polys[0][0][0]) if polys else 2

    esri_json = {
        "rings": rings,
        "hasZ": max_dim >= 3,  # by default assume the third dim is z
        "hasM": max_dim >= 4,
        "spatialReference": sr,
    }
    return esri_json
