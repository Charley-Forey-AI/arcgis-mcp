import unittest

from arcgis_mcp_server.tools.symbology_intelligence import suggest_symbology


class _FakeCon:
    def __init__(self, responses):
        self._responses = responses

    def get(self, url, params=None):
        params = params or {}
        key = (url, tuple(sorted(params.items())))
        if key not in self._responses:
            raise KeyError(f"missing fake response for {key}")
        return self._responses[key]


class _FakeContent:
    def get(self, _item_id):
        return None


class _FakeGIS:
    def __init__(self, con):
        self._con = con
        self.content = _FakeContent()


class TestSuggestSymbology(unittest.TestCase):
    def test_numeric_class_breaks(self):
        url = "https://example.com/FeatureServer/0"
        meta = {
            "id": 0,
            "name": "Layer",
            "geometryType": "esriGeometryPolygon",
            "advancedQueryCapabilities": {"supportsStatistics": True},
            "fields": [{"name": "V", "type": "esriFieldTypeDouble"}],
        }
        stats = (
            '['
            '{"statisticType": "count", "onStatisticField": "V", "outStatisticFieldName": "count"}, '
            '{"statisticType": "min", "onStatisticField": "V", "outStatisticFieldName": "min"}, '
            '{"statisticType": "max", "onStatisticField": "V", "outStatisticFieldName": "max"}, '
            '{"statisticType": "avg", "onStatisticField": "V", "outStatisticFieldName": "avg"}, '
            '{"statisticType": "stddev", "onStatisticField": "V", "outStatisticFieldName": "stddev"}'
            ']'
        )
        con = _FakeCon(
            {
                (url, (("f", "json"),)): meta,
                (url + "/query", (("f", "json"), ("outFields", "V"), ("outStatistics", stats), ("resultRecordCount", "1"), ("returnGeometry", "false"), ("where", "1=1"))): {
                    "features": [{"attributes": {"min": 0, "max": 10, "count": 10, "avg": 5, "stddev": 2}}]
                },
            }
        )
        gis = _FakeGIS(con)
        out = suggest_symbology(gis, layer_url=url, goal="numeric")
        self.assertEqual(out.get("mode"), "numeric")
        self.assertEqual((out.get("renderer") or {}).get("type"), "class-breaks")

    def test_categorical_unique_values(self):
        url = "https://example.com/FeatureServer/0"
        meta = {
            "id": 0,
            "name": "Layer",
            "geometryType": "esriGeometryPoint",
            "advancedQueryCapabilities": {"supportsStatistics": True},
            "fields": [{"name": "K", "type": "esriFieldTypeString"}],
        }
        dv_stats = '[{"statisticType": "count", "onStatisticField": "K", "outStatisticFieldName": "count"}]'
        con = _FakeCon(
            {
                (url, (("f", "json"),)): meta,
                (
                    url + "/query",
                    (
                        ("f", "json"),
                        ("groupByFieldsForStatistics", "K"),
                        ("orderByFields", "count DESC"),
                        ("outFields", "K"),
                        ("outStatistics", dv_stats),
                        ("resultRecordCount", "10"),
                        ("returnGeometry", "false"),
                        ("where", "1=1"),
                    ),
                ): {
                    "features": [{"attributes": {"K": "a", "count": 2}}, {"attributes": {"K": "b", "count": 1}}]
                },
            }
        )
        gis = _FakeGIS(con)
        out = suggest_symbology(gis, layer_url=url, goal="category")
        self.assertEqual(out.get("mode"), "categorical")
        self.assertEqual((out.get("renderer") or {}).get("type"), "unique-value")


if __name__ == "__main__":
    unittest.main()

