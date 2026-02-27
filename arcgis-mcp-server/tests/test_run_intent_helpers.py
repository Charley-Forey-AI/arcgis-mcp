import unittest
from unittest.mock import patch
import json


class TestRunIntentHelpers(unittest.TestCase):
    def test_intent_kind(self):
        from arcgis_mcp_server.server import _intent_kind

        self.assertEqual(_intent_kind("Directions from A to B"), "directions")
        self.assertEqual(_intent_kind("Demographics for Tempe, AZ"), "demographics")
        self.assertEqual(_intent_kind("Best map style for layer"), "auto_map")

    def test_extract_route_stops(self):
        from arcgis_mcp_server.server import _extract_route_stops

        o, d = _extract_route_stops("Get directions from Seattle Space Needle to Pike Place Market.")
        self.assertEqual(o, "Seattle Space Needle")
        self.assertEqual(d, "Pike Place Market")

    def test_extract_layer_ref(self):
        from arcgis_mcp_server.server import _extract_layer_ref

        item_id, url = _extract_layer_ref("Best map for layer e1018631be3c4069b57c2aff151aa013")
        self.assertEqual(item_id, "e1018631be3c4069b57c2aff151aa013")
        self.assertIsNone(url)

        item_id2, url2 = _extract_layer_ref("Style https://sampleserver6.arcgisonline.com/arcgis/rest/services/Census/MapServer/3")
        self.assertIsNone(item_id2)
        self.assertTrue(url2.startswith("https://sampleserver6"))

    @patch("arcgis_mcp_server.server._route_and_show_map")
    @patch("arcgis_mcp_server.server._whoami")
    def test_run_intent_directions_ok_schema(self, mock_whoami, mock_route):
        from arcgis_mcp_server.server import _run_intent

        mock_whoami.return_value = json.dumps({"username": "demo_user", "portal": "ArcGIS Online"})
        mock_route.return_value = json.dumps(
            {
                "schemaVersion": 3,
                "map_url": "https://example.com/map/s/abc",
                "total_time": 900,
                "total_length": 2.2,
                "directions": ["Head south", "Turn right", "Arrive"],
            }
        )

        out = json.loads(_run_intent("Directions from Seattle Space Needle to Pike Place Market"))
        # MapState top-level
        self.assertEqual(out.get("schemaVersion"), 3)
        self.assertIn("intent_result", out)
        self.assertEqual(out["intent_result"].get("status"), "ok")
        self.assertTrue(out["intent_result"].get("confidence", 0) >= 0.9)
        self.assertIn("plan", out["intent_result"])
        self.assertIn("executed_steps", out["intent_result"])
        self.assertEqual(out["intent_result"].get("intent_class"), "directions")

    @patch("arcgis_mcp_server.server._whoami")
    def test_run_intent_directions_needs_input(self, mock_whoami):
        from arcgis_mcp_server.server import _run_intent

        mock_whoami.return_value = json.dumps({"username": None, "portal": "ArcGIS Online"})
        out = json.loads(_run_intent("directions please"))
        self.assertEqual(out.get("status"), "needs_input")
        self.assertEqual(out.get("intent_class"), "directions")
        self.assertIn("needs_input", out)

    @patch("arcgis_mcp_server.server._whoami")
    def test_run_intent_auto_map_needs_layer_ref(self, mock_whoami):
        from arcgis_mcp_server.server import _run_intent

        mock_whoami.return_value = json.dumps({"username": "demo_user", "portal": "ArcGIS Online"})
        out = json.loads(_run_intent("best map for this layer"))
        self.assertEqual(out.get("status"), "needs_input")
        self.assertEqual(out.get("intent_class"), "auto_map")
        self.assertIn("needs_input", out)

    @patch("arcgis_mcp_server.server._buffer_and_show")
    @patch("arcgis_mcp_server.server._whoami")
    def test_run_intent_buffer_returns_mapstate_with_intent_result(self, mock_whoami, mock_buf):
        from arcgis_mcp_server.server import _run_intent

        mock_whoami.return_value = json.dumps({"username": "demo_user", "portal": "ArcGIS Online"})
        mock_buf.return_value = json.dumps(
            {
                "schemaVersion": 3,
                "center": {"longitude": -122.34, "latitude": 47.61},
                "zoom": 10,
                "bbox": [-122.5, 47.5, -122.2, 47.7],
                "markers": [{"x": -122.34, "y": 47.61}],
                "routeGeometry": None,
                "basemapId": "streets-navigation-vector",
                "graphics": [{"geometry": {"rings": []}, "symbol": {"type": "simple-fill"}}],
            }
        )

        out = json.loads(_run_intent("Buffer 2 miles around Pike Place Market and show it"))
        # MapState top-level
        self.assertEqual(out.get("schemaVersion"), 3)
        self.assertIn("intent_result", out)
        self.assertEqual(out["intent_result"].get("status"), "ok")
        self.assertEqual(out["intent_result"].get("intent_class"), "buffer")

    @patch("arcgis_mcp_server.server._whoami")
    def test_run_intent_buffer_needs_input(self, mock_whoami):
        from arcgis_mcp_server.server import _run_intent

        mock_whoami.return_value = json.dumps({"username": "demo_user", "portal": "ArcGIS Online"})
        out = json.loads(_run_intent("buffer around Pike Place"))
        self.assertEqual(out.get("status"), "needs_input")
        self.assertEqual(out.get("intent_class"), "buffer")


if __name__ == "__main__":
    unittest.main()

