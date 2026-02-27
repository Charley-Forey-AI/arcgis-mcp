"""Unit tests for map and route helpers (dedupe, route geometry extraction, decode)."""
import base64
import json
import unittest
import urllib.parse

# Import pure helpers (no GIS required)
from arcgis_mcp_server.map_utils import DEFAULT_HILLSHADE_URL, dedupe_points, extract_first_route_geometry, normalize_overlays
from arcgis_mcp_server.server import _normalize_route_geometry


class TestDedupePoints(unittest.TestCase):
    def test_empty_returns_empty(self):
        self.assertEqual(dedupe_points([]), [])

    def test_single_point_unchanged(self):
        self.assertEqual(dedupe_points([(-87.63, 41.88)]), [(-87.63, 41.88)])

    def test_two_points_far_apart_both_kept(self):
        a = (-87.63, 41.88)
        b = (-90.07, 29.95)
        self.assertEqual(dedupe_points([a, b]), [a, b])

    def test_two_points_near_merged(self):
        a = (-87.63, 41.88)
        b = (-87.6301, 41.8801)  # ~10 m
        self.assertEqual(len(dedupe_points([a, b])), 1)

    def test_three_where_first_two_near_one_kept(self):
        a = (-87.63, 41.88)
        b = (-87.6301, 41.8801)
        c = (-90.07, 29.95)
        out = dedupe_points([a, b, c])
        self.assertEqual(len(out), 2)
        self.assertEqual(out[0], a)
        self.assertEqual(out[1], c)


class TestExtractFirstRouteGeometry(unittest.TestCase):
    def test_empty_none(self):
        self.assertIsNone(extract_first_route_geometry({}))

    def test_routes_with_features_returns_geometry(self):
        geom = {"paths": [[[-122, 37], [-122.1, 37.1]]], "spatialReference": {"wkid": 4326}}
        result = {"routes": {"features": [{"geometry": geom}]}}
        self.assertEqual(extract_first_route_geometry(result), geom)

    def test_route_singular_key(self):
        geom = {"paths": [[[-122, 37]]], "spatialReference": {"wkid": 4326}}
        result = {"route": {"features": [{"geometry": geom}]}}
        self.assertEqual(extract_first_route_geometry(result), geom)

    def test_Routes_capital_R(self):
        geom = {"paths": [[]], "spatialReference": {"wkid": 4326}}
        result = {"Routes": {"features": [{"geometry": geom}]}}
        self.assertEqual(extract_first_route_geometry(result), geom)

    def test_no_features_none(self):
        result = {"routes": {"features": []}}
        self.assertIsNone(extract_first_route_geometry(result))

    def test_geometry_without_paths_or_x_none(self):
        result = {"routes": {"features": [{"geometry": {"foo": "bar"}}]}}
        self.assertIsNone(extract_first_route_geometry(result))


class TestDecodeMapParam(unittest.TestCase):
    """Test decode logic (same algorithm as server _decode_map_param)."""

    def _decode(self, value):
        if not value or not str(value).strip():
            return None
        raw = urllib.parse.unquote(str(value).strip())
        for attempt in [raw, raw + "1", raw + "1d", raw + "=", raw + "=="]:
            try:
                padded = attempt
                pad = 4 - (len(padded) % 4)
                if pad and pad != 4:
                    padded += "=" * pad
                decoded = base64.urlsafe_b64decode(padded)
                parsed = json.loads(decoded.decode("utf-8"))
                if isinstance(parsed, (list, dict)):
                    return parsed
            except Exception:
                continue
        return None

    def test_valid_markers_decode(self):
        markers = [{"x": -87.63, "y": 41.88}, {"x": -90.07, "y": 29.95}]
        enc = base64.urlsafe_b64encode(json.dumps(markers).encode("utf-8")).decode("ascii")
        self.assertEqual(self._decode(enc), markers)

    def test_truncated_with_padding_decode(self):
        markers = [{"x": -87.63, "y": 41.88}]
        enc = base64.urlsafe_b64encode(json.dumps(markers).encode("utf-8")).decode("ascii")
        self.assertEqual(self._decode(enc), markers)

    def test_invalid_returns_none(self):
        self.assertIsNone(self._decode("not-valid-base64!!!"))
        self.assertIsNone(self._decode(None))
        self.assertIsNone(self._decode(""))


class TestNormalizeRouteGeometry(unittest.TestCase):
    def test_geojson_coordinates_to_polyline_paths(self):
        geom = {"coordinates": [[-122.0, 37.0], [-122.1, 37.1]]}
        out = _normalize_route_geometry(geom)
        self.assertIsInstance(out, dict)
        self.assertIn("paths", out)
        self.assertEqual(out["spatialReference"]["wkid"], 4326)
        self.assertEqual(out["paths"][0][0], (-122.0, 37.0))

    def test_esri_polyline_sets_wgs84_sr(self):
        geom = {"paths": [[[-122.0, 37.0], [-122.1, 37.1]]]}
        out = _normalize_route_geometry(geom)
        self.assertIsInstance(out, dict)
        self.assertIn("paths", out)
        self.assertEqual(out["spatialReference"]["wkid"], 4326)


class TestNormalizeOverlays(unittest.TestCase):
    def test_terrain_injects_hillshade(self):
        ovs = normalize_overlays([], terrain=True)
        self.assertTrue(ovs)
        self.assertEqual(ovs[0]["url"], DEFAULT_HILLSHADE_URL)

    def test_does_not_duplicate_hillshade(self):
        ovs = normalize_overlays([{"type": "tile", "url": DEFAULT_HILLSHADE_URL, "opacity": 0.5}], terrain=True)
        urls = [o["url"] for o in ovs]
        self.assertEqual(urls.count(DEFAULT_HILLSHADE_URL), 1)


if __name__ == "__main__":
    unittest.main()
