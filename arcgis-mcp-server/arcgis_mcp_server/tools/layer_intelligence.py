"""Layer intelligence primitives (read-only).

These functions are designed to be called from MCP tools. They accept a
request-scoped ArcGIS `GIS` instance for auth and network behavior.
"""

from __future__ import annotations

import json
import re
from typing import Any


def _normalized_layer_url(*, item_url: str | None, layer_url: str | None, layer_index: int | None) -> str | None:
    """Normalize service root URLs into a concrete sublayer URL when possible."""

    url = (layer_url or item_url or "").strip()
    if not url:
        return None
    idx = 0
    if layer_index is not None:
        try:
            idx = int(layer_index)
        except (TypeError, ValueError):
            idx = 0
    normalized = url.rstrip("/")
    if re.search(r"/(FeatureServer|MapServer|ImageServer|VectorTileServer)$", normalized, flags=re.IGNORECASE):
        normalized = normalized + f"/{idx}"
    return normalized


def _get_item_and_url(gis: Any, *, layer_item_id: str | None, layer_url: str | None, layer_index: int | None) -> tuple[dict, str] | tuple[None, None]:
    """Return (item_info, concrete_layer_url)."""

    iid = (layer_item_id or "").strip() or None
    url = (layer_url or "").strip() or None
    item_info: dict | None = None
    item_url: str | None = None

    if iid:
        item = gis.content.get(iid)
        if item is None:
            return None, None
        item_info = {
            "id": getattr(item, "id", None),
            "title": getattr(item, "title", None),
            "type": getattr(item, "type", None),
            "owner": getattr(item, "owner", None),
            "url": getattr(item, "url", None),
        }
        item_url = (getattr(item, "url", None) or "").strip() or None

    concrete = _normalized_layer_url(item_url=item_url, layer_url=url, layer_index=layer_index)
    if not concrete:
        return item_info, None
    return item_info, concrete


def _rest_get_json(gis: Any, url: str, params: dict[str, Any]) -> dict:
    """Fetch JSON via ArcGIS connection when possible, else urllib."""

    # Prefer ArcGIS SDK connection so secured services work with the request token.
    con = getattr(gis, "_con", None)
    if con is not None and hasattr(con, "get"):
        return con.get(url, params=params)  # type: ignore[no-any-return]

    import urllib.parse
    import urllib.request

    q = urllib.parse.urlencode(params)
    sep = "&" if "?" in url else "?"
    u = url + sep + q
    with urllib.request.urlopen(u, timeout=30) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    parsed = json.loads(raw)
    return parsed if isinstance(parsed, dict) else {"value": parsed}


def describe_layer(
    gis: Any,
    *,
    layer_item_id: str | None = None,
    layer_url: str | None = None,
    layer_index: int | None = 0,
) -> dict:
    """Describe an ArcGIS layer/service sublayer via REST metadata (`f=json`)."""

    item_info, url = _get_item_and_url(gis, layer_item_id=layer_item_id, layer_url=layer_url, layer_index=layer_index)
    if (layer_item_id or "").strip() and item_info is None:
        return {"error": f"Item not found: {layer_item_id}"}
    if not url:
        return {
            "error": "Missing layer source.",
            "hint": "Provide layer_item_id (portal item id) or layer_url (FeatureServer/MapServer/etc).",
        }

    meta = _rest_get_json(gis, url, {"f": "json"})
    if not isinstance(meta, dict):
        return {"error": "Invalid layer metadata response."}
    if meta.get("error"):
        err = meta.get("error") or {}
        msg = err.get("message") if isinstance(err, dict) else str(err)
        return {"error": msg or "Layer metadata request failed.", "details": meta.get("error")}

    fields = meta.get("fields") if isinstance(meta.get("fields"), list) else []
    out_fields: list[dict] = []
    for f in fields:
        if not isinstance(f, dict):
            continue
        out_fields.append(
            {
                "name": f.get("name"),
                "type": f.get("type"),
                "alias": f.get("alias"),
                "domain": f.get("domain"),
                "length": f.get("length"),
            }
        )

    # Capability flags
    adv = meta.get("advancedQueryCapabilities") if isinstance(meta.get("advancedQueryCapabilities"), dict) else {}
    supports_pagination = bool(adv.get("supportsPagination")) or bool(meta.get("supportsPagination"))
    supports_stats = bool(adv.get("supportsStatistics")) or bool(meta.get("supportsStatistics"))
    supports_order_by = bool(adv.get("supportsOrderBy")) or bool(meta.get("supportsOrderBy"))
    max_record_count = meta.get("maxRecordCount")

    return {
        "source": {
            "layer_item_id": (layer_item_id or "").strip() or None,
            "layer_url": (layer_url or "").strip() or None,
            "layer_index": int(layer_index or 0),
            "resolved_url": url,
            "item": item_info,
        },
        "layer": {
            "id": meta.get("id"),
            "name": meta.get("name"),
            "type": meta.get("type"),
            "geometryType": meta.get("geometryType"),
            "extent": meta.get("extent"),
            "timeInfo": meta.get("timeInfo"),
            "capabilities": meta.get("capabilities"),
            "maxRecordCount": max_record_count,
            "supportsPagination": supports_pagination,
            "supportsStatistics": supports_stats,
            "supportsOrderBy": supports_order_by,
            "advancedQueryCapabilities": adv,
        },
        "fields": out_fields,
    }


def sample_features(
    gis: Any,
    *,
    layer_item_id: str | None = None,
    layer_url: str | None = None,
    layer_index: int | None = 0,
    where: str = "1=1",
    out_fields: str = "*",
    limit: int = 10,
    offset: int = 0,
    order_by: str | None = None,
    return_geometry: bool = True,
) -> dict:
    """Sample/query features with basic pagination controls."""

    item_info, url = _get_item_and_url(gis, layer_item_id=layer_item_id, layer_url=layer_url, layer_index=layer_index)
    if (layer_item_id or "").strip() and item_info is None:
        return {"error": f"Item not found: {layer_item_id}"}
    if not url:
        return {"error": "Missing layer source.", "hint": "Provide layer_item_id or layer_url."}

    lim = max(1, min(int(limit or 10), 2000))
    off = max(0, int(offset or 0))
    try:
        q_url = url.rstrip("/") + "/query"
        params: dict[str, Any] = {
            "f": "json",
            "where": (where or "1=1").strip() or "1=1",
            "outFields": (out_fields or "*").strip() or "*",
            "returnGeometry": "true" if bool(return_geometry) else "false",
            "resultRecordCount": str(lim),
            "resultOffset": str(off),
        }
        if isinstance(order_by, str) and order_by.strip():
            params["orderByFields"] = order_by.strip()
        res = _rest_get_json(gis, q_url, params)
    except Exception as e:
        return {"error": str(e), "hint": "Check where/out_fields/order_by; layer may not support pagination or query."}

    if not isinstance(res, dict):
        return {"error": "Invalid query response."}
    if res.get("error"):
        return {"error": (res.get("error") or {}).get("message", "Query failed"), "details": res.get("error")}
    feats = res.get("features") if isinstance(res.get("features"), list) else []
    exceeded = bool(res.get("exceededTransferLimit", False))
    return {
        "source": {"resolved_url": url, "item": item_info},
        "where": (where or "1=1").strip() or "1=1",
        "out_fields": (out_fields or "*").strip() or "*",
        "limit": lim,
        "offset": off,
        "returned": len(feats),
        "exceeded_transfer_limit": exceeded,
        "next_offset": (off + lim) if (exceeded or len(feats) == lim) else None,
        "features": feats,
    }


def distinct_values(
    gis: Any,
    *,
    layer_item_id: str | None = None,
    layer_url: str | None = None,
    layer_index: int | None = 0,
    field: str = "",
    where: str = "1=1",
    max_values: int = 25,
) -> dict:
    """Return distinct values (with best-effort counts)."""

    field = (field or "").strip()
    if not field:
        return {"error": "Missing required argument: field."}

    item_info, url = _get_item_and_url(gis, layer_item_id=layer_item_id, layer_url=layer_url, layer_index=layer_index)
    if (layer_item_id or "").strip() and item_info is None:
        return {"error": f"Item not found: {layer_item_id}"}
    if not url:
        return {"error": "Missing layer source.", "hint": "Provide layer_item_id or layer_url."}

    mv = max(1, min(int(max_values or 25), 200))
    w = (where or "1=1").strip() or "1=1"

    # Preferred: group-by + count statistic (gives counts server-side).
    try:
        q_url = url.rstrip("/") + "/query"
        stats = [{"statisticType": "count", "onStatisticField": field, "outStatisticFieldName": "count"}]
        params = {
            "f": "json",
            "where": w,
            "outFields": field,
            "returnGeometry": "false",
            "groupByFieldsForStatistics": field,
            "outStatistics": json.dumps(stats),
            "orderByFields": "count DESC",
            "resultRecordCount": str(mv),
        }
        res = _rest_get_json(gis, q_url, params)
        values: list[dict] = []
        feats = res.get("features") if isinstance(res, dict) else None
        if isinstance(feats, list):
            for f in feats:
                attrs = f.get("attributes") if isinstance(f, dict) else None
                if isinstance(attrs, dict):
                    values.append({"value": attrs.get(field), "count": attrs.get("count")})
        return {
            "source": {"resolved_url": url, "item": item_info},
            "field": field,
            "where": w,
            "values": values[:mv],
            "has_counts": True,
        }
    except Exception:
        pass

    # Fallback: returnDistinctValues (no counts)
    try:
        q_url = url.rstrip("/") + "/query"
        params = {
            "f": "json",
            "where": w,
            "outFields": field,
            "returnGeometry": "false",
            "returnDistinctValues": "true",
            "resultRecordCount": str(mv),
        }
        data = _rest_get_json(gis, q_url, params)
        feats = data.get("features") if isinstance(data.get("features"), list) else []
        vals = []
        for f in feats:
            if isinstance(f, dict) and isinstance(f.get("attributes"), dict):
                vals.append({"value": f["attributes"].get(field)})
        return {
            "source": {"resolved_url": url, "item": item_info},
            "field": field,
            "where": w,
            "values": vals[:mv],
            "has_counts": False,
        }
    except Exception as e:
        return {"error": str(e), "hint": "Layer may not support distinct values or statistics."}


def field_stats(
    gis: Any,
    *,
    layer_item_id: str | None = None,
    layer_url: str | None = None,
    layer_index: int | None = 0,
    numeric_field: str = "",
    where: str = "1=1",
    group_by_field: str | None = None,
    max_groups: int = 25,
    histogram_bins: int = 0,
    histogram_sample_size: int = 500,
) -> dict:
    """Compute basic statistics (and optional sampled histogram) for a numeric field."""

    nf = (numeric_field or "").strip()
    if not nf:
        return {"error": "Missing required argument: numeric_field."}

    item_info, url = _get_item_and_url(gis, layer_item_id=layer_item_id, layer_url=layer_url, layer_index=layer_index)
    if (layer_item_id or "").strip() and item_info is None:
        return {"error": f"Item not found: {layer_item_id}"}
    if not url:
        return {"error": "Missing layer source.", "hint": "Provide layer_item_id or layer_url."}

    w = (where or "1=1").strip() or "1=1"
    gb = (group_by_field or "").strip() or None
    mg = max(1, min(int(max_groups or 25), 200))
    stats = [
        {"statisticType": "count", "onStatisticField": nf, "outStatisticFieldName": "count"},
        {"statisticType": "min", "onStatisticField": nf, "outStatisticFieldName": "min"},
        {"statisticType": "max", "onStatisticField": nf, "outStatisticFieldName": "max"},
        {"statisticType": "avg", "onStatisticField": nf, "outStatisticFieldName": "avg"},
        {"statisticType": "stddev", "onStatisticField": nf, "outStatisticFieldName": "stddev"},
    ]
    try:
        q_url = url.rstrip("/") + "/query"
        params: dict[str, Any] = {
            "f": "json",
            "where": w,
            "outFields": (gb or nf),
            "returnGeometry": "false",
            "outStatistics": json.dumps(stats),
            "resultRecordCount": str(mg if gb else 1),
        }
        if gb:
            params["groupByFieldsForStatistics"] = gb
        res = _rest_get_json(gis, q_url, params)
    except Exception as e:
        return {"error": str(e), "hint": "Layer may not support statistics; check field name and where clause."}

    rows: list[dict] = []
    feats = res.get("features") if isinstance(res, dict) else None
    if isinstance(feats, list):
        for f in feats:
            attrs = f.get("attributes") if isinstance(f, dict) else None
            if not isinstance(attrs, dict):
                continue
            rows.append(
                {
                    "group": attrs.get(gb) if gb else None,
                    "count": attrs.get("count"),
                    "min": attrs.get("min"),
                    "max": attrs.get("max"),
                    "avg": attrs.get("avg"),
                    "stddev": attrs.get("stddev"),
                }
            )

    out: dict = {
        "source": {"resolved_url": url, "item": item_info},
        "numeric_field": nf,
        "where": w,
        "group_by_field": gb,
        "rows": rows[:mg] if gb else rows[:1],
    }

    # Optional histogram (sampled, client-safe)
    hb = int(histogram_bins or 0)
    if hb > 0 and not gb:
        hb = max(2, min(hb, 50))
        sample_n = max(10, min(int(histogram_sample_size or 500), 5000))
        try:
            q_url = url.rstrip("/") + "/query"
            sample_res = _rest_get_json(
                gis,
                q_url,
                {
                    "f": "json",
                    "where": w,
                    "outFields": nf,
                    "returnGeometry": "false",
                    "resultRecordCount": str(sample_n),
                },
            )
            vals: list[float] = []
            feats = sample_res.get("features") if isinstance(sample_res, dict) else None
            feats = feats if isinstance(feats, list) else []
            for f in feats:
                attrs = f.get("attributes") if isinstance(f, dict) else None
                v = attrs.get(nf) if isinstance(attrs, dict) else None
                try:
                    if v is not None:
                        vals.append(float(v))
                except Exception:
                    continue
            if vals:
                vmin, vmax = min(vals), max(vals)
                if vmin == vmax:
                    bins = [{"min": vmin, "max": vmax, "count": len(vals)}]
                else:
                    step = (vmax - vmin) / hb
                    counts = [0] * hb
                    for v in vals:
                        idx = int((v - vmin) / step)
                        if idx == hb:
                            idx = hb - 1
                        counts[idx] += 1
                    bins = []
                    for i, c in enumerate(counts):
                        b0 = vmin + step * i
                        b1 = vmin + step * (i + 1)
                        bins.append({"min": b0, "max": b1, "count": c})
                out["histogram"] = {"bins": bins, "sample_size": len(vals)}
        except Exception:
            # Histogram is best-effort only
            pass

    return out

