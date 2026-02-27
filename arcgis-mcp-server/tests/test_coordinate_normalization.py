import unittest


class TestCoordinateNormalization(unittest.TestCase):
    def test_valid_lon_lat_no_warning(self):
        from arcgis_mcp_server.server import _normalize_lon_lat

        pt, w = _normalize_lon_lat(-122.34, 47.62)
        self.assertEqual(pt, (-122.34, 47.62))
        self.assertIsNone(w)

    def test_swapped_lat_lon_is_auto_swapped_with_warning(self):
        from arcgis_mcp_server.server import _normalize_lon_lat

        pt, w = _normalize_lon_lat(47.62, -122.34)  # (lat, lon) mistakenly
        self.assertEqual(pt, (-122.34, 47.62))
        self.assertTrue(isinstance(w, str) and "auto-swapped" in w.lower())

    def test_invalid_range_returns_none(self):
        from arcgis_mcp_server.server import _normalize_lon_lat

        pt, w = _normalize_lon_lat(5000, 5000)
        self.assertIsNone(pt)
        self.assertTrue(isinstance(w, str) and "invalid" in w.lower())

    def test_resolve_location_list_swapped(self):
        from arcgis_mcp_server.server import _resolve_location

        pt, w = _resolve_location([47.62, -122.34])
        self.assertEqual(pt, (-122.34, 47.62))
        self.assertTrue(isinstance(w, str) and "swapped" in w.lower())


if __name__ == "__main__":
    unittest.main()

