import unittest


from arcgis_mcp_server.tools.layer_intelligence import describe_layer, distinct_values, field_stats


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
    def __init__(self, items):
        self._items = items

    def get(self, item_id):
        return self._items.get(item_id)


class _FakeItem:
    def __init__(self, item_id="abc", title="T", type_="Feature Service", owner="me", url=None):
        self.id = item_id
        self.title = title
        self.type = type_
        self.owner = owner
        self.url = url


class _FakeGIS:
    def __init__(self, con=None, items=None):
        self._con = con
        self.content = _FakeContent(items or {})


class TestDescribeLayer(unittest.TestCase):
    def test_describe_layer_from_url(self):
        url = "https://example.com/ArcGIS/rest/services/Test/FeatureServer/0"
        meta = {
            "id": 0,
            "name": "Test",
            "geometryType": "esriGeometryPoint",
            "capabilities": "Query",
            "maxRecordCount": 2000,
            "advancedQueryCapabilities": {"supportsStatistics": True, "supportsPagination": True, "supportsOrderBy": True},
            "fields": [{"name": "A", "type": "esriFieldTypeInteger", "alias": "A"}],
        }
        con = _FakeCon({(url, (("f", "json"),)): meta})
        gis = _FakeGIS(con=con)
        out = describe_layer(gis, layer_url=url, layer_index=0)
        self.assertIn("layer", out)
        self.assertEqual(out["layer"]["geometryType"], "esriGeometryPoint")
        self.assertTrue(out["layer"]["supportsStatistics"])
        self.assertEqual(out["fields"][0]["name"], "A")

    def test_describe_layer_missing_source(self):
        gis = _FakeGIS(con=_FakeCon({}))
        out = describe_layer(gis)
        self.assertIn("error", out)


class TestDistinctValues(unittest.TestCase):
    def test_distinct_values_group_by_counts(self):
        url = "https://example.com/FeatureServer/0"
        q_url = url + "/query"
        con = _FakeCon(
            {
                (q_url, (("f", "json"), ("groupByFieldsForStatistics", "K"), ("orderByFields", "count DESC"), ("outFields", "K"), ("outStatistics", '[{"statisticType": "count", "onStatisticField": "K", "outStatisticFieldName": "count"}]'), ("resultRecordCount", "25"), ("returnGeometry", "false"), ("where", "1=1"))): {
                    "features": [
                        {"attributes": {"K": "x", "count": 2}},
                        {"attributes": {"K": "y", "count": 1}},
                    ]
                }
            }
        )
        gis = _FakeGIS(con=con)
        out = distinct_values(gis, layer_url=url, field="K")
        self.assertTrue(out.get("has_counts"))
        self.assertEqual(out["values"][0]["value"], "x")


class TestFieldStats(unittest.TestCase):
    def test_field_stats_basic(self):
        url = "https://example.com/FeatureServer/0"
        q_url = url + "/query"
        out_stats = (
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
                (q_url, (("f", "json"), ("outFields", "V"), ("outStatistics", out_stats), ("resultRecordCount", "1"), ("returnGeometry", "false"), ("where", "1=1"))): {
                    "features": [{"attributes": {"count": 10, "min": 1, "max": 5, "avg": 3, "stddev": 1.2}}]
                }
            }
        )
        gis = _FakeGIS(con=con)
        out = field_stats(gis, layer_url=url, numeric_field="V", histogram_bins=0)
        self.assertIn("rows", out)
        self.assertEqual(out["rows"][0]["max"], 5)


if __name__ == "__main__":
    unittest.main()

