"""Deterministic symbology intelligence (no LLM required)."""

from __future__ import annotations

import math
from typing import Any

from .layer_intelligence import describe_layer, distinct_values, field_stats


_NUMERIC_TYPES = {
    "esriFieldTypeSmallInteger",
    "esriFieldTypeInteger",
    "esriFieldTypeSingle",
    "esriFieldTypeDouble",
}

_STRING_TYPES = {
    "esriFieldTypeString",
}


def _is_probably_id_field(name: str) -> bool:
    n = (name or "").strip().lower()
    return n in {"objectid", "fid", "oid", "globalid"} or n.endswith("objectid") or n.endswith("_id")


def _pick_field(layer_desc: dict, *, preferred_field: str | None, goal: str | None) -> tuple[str | None, str | None]:
    fields = layer_desc.get("fields") if isinstance(layer_desc.get("fields"), list) else []
    pf = (preferred_field or "").strip()
    if pf:
        for f in fields:
            if isinstance(f, dict) and (f.get("name") or "").strip() == pf:
                return pf, (f.get("type") or None)

    g = (goal or "").strip().lower()
    want_category = g in {"category", "unique", "unique_values", "categorical"}

    # Prefer domain-coded string field for categorical
    if want_category:
        for f in fields:
            if not isinstance(f, dict):
                continue
            name = (f.get("name") or "").strip()
            if not name or _is_probably_id_field(name):
                continue
            ftype = f.get("type")
            dom = f.get("domain")
            if ftype in _STRING_TYPES and isinstance(dom, dict) and dom.get("codedValues"):
                return name, ftype
        for f in fields:
            if not isinstance(f, dict):
                continue
            name = (f.get("name") or "").strip()
            if not name or _is_probably_id_field(name):
                continue
            ftype = f.get("type")
            if ftype in _STRING_TYPES:
                return name, ftype

    # Numeric default
    for f in fields:
        if not isinstance(f, dict):
            continue
        name = (f.get("name") or "").strip()
        if not name or _is_probably_id_field(name):
            continue
        ftype = f.get("type")
        if ftype in _NUMERIC_TYPES:
            return name, ftype

    # Fallback: any string field
    for f in fields:
        if isinstance(f, dict):
            name = (f.get("name") or "").strip()
            if name and not _is_probably_id_field(name):
                return name, (f.get("type") or None)
    return None, None


def _symbol_for_geom(geometry_type: str | None, *, fill: str, outline: str = "#ffffff", width: float = 1.0) -> dict:
    gt = (geometry_type or "").lower()
    if "polygon" in gt:
        return {
            "type": "simple-fill",
            "color": fill,
            "outline": {"type": "simple-line", "color": outline, "width": width},
        }
    if "polyline" in gt or "line" in gt:
        return {"type": "simple-line", "color": fill, "width": max(1.0, width + 1.5)}
    # points
    return {
        "type": "simple-marker",
        "style": "circle",
        "color": fill,
        "size": 8,
        "outline": {"type": "simple-line", "color": outline, "width": width},
    }


def _class_breaks(min_v: float, max_v: float, n: int = 5) -> list[tuple[float, float]]:
    if not math.isfinite(min_v) or not math.isfinite(max_v):
        return [(0.0, 1.0)]
    if min_v == max_v:
        return [(min_v, max_v)]
    n = max(2, min(int(n), 9))
    step = (max_v - min_v) / n
    out = []
    for i in range(n):
        a = min_v + step * i
        b = min_v + step * (i + 1)
        out.append((a, b))
    return out


def suggest_symbology(
    gis: Any,
    *,
    layer_item_id: str | None = None,
    layer_url: str | None = None,
    layer_index: int | None = 0,
    preferred_field: str | None = None,
    goal: str | None = None,
    where: str = "1=1",
    max_categories: int = 10,
) -> dict:
    """Suggest renderer + optional feature reduction based on layer metadata and cheap stats."""

    desc = describe_layer(gis, layer_item_id=layer_item_id, layer_url=layer_url, layer_index=layer_index)
    if desc.get("error"):
        return desc
    geom_type = (desc.get("layer") or {}).get("geometryType") if isinstance(desc.get("layer"), dict) else None
    field_name, field_type = _pick_field(desc, preferred_field=preferred_field, goal=goal)
    if not field_name:
        return {"error": "Could not pick a field for symbology.", "hint": "Provide preferred_field."}

    g = (goal or "").strip().lower()
    want_category = g in {"category", "unique", "unique_values", "categorical"}
    want_cluster = g in {"cluster", "points", "auto", ""} and "point" in str(geom_type or "").lower()

    # Palette (safe defaults)
    cat_palette = ["#2E86DE", "#10AC84", "#F368E0", "#FF9F43", "#576574", "#EE5253", "#0ABDE3", "#5F27CD", "#01A3A4", "#C8D6E5"]
    ramp = ["#deebf7", "#c6dbef", "#9ecae1", "#6baed6", "#3182bd", "#08519c"]

    where_clause = (where or "1=1").strip() or "1=1"

    # Categorical
    if want_category or field_type in _STRING_TYPES:
        dv = distinct_values(
            gis,
            layer_item_id=layer_item_id,
            layer_url=layer_url,
            layer_index=layer_index,
            field=field_name,
            where=where_clause,
            max_values=max(2, min(int(max_categories or 10), 20)),
        )
        vals = dv.get("values") if isinstance(dv, dict) else None
        vals = vals if isinstance(vals, list) else []
        infos = []
        for i, v in enumerate(vals[: len(cat_palette)]):
            value = v.get("value") if isinstance(v, dict) else None
            if value is None:
                continue
            infos.append(
                {
                    "value": value,
                    "label": str(value),
                    "symbol": _symbol_for_geom(geom_type, fill=cat_palette[i]),
                }
            )
        renderer = {"type": "unique-value", "field": field_name, "uniqueValueInfos": infos}
        feature_reduction = {"type": "cluster"} if want_cluster else None
        return {
            "geometryType": geom_type,
            "mode": "categorical",
            "field": field_name,
            "renderer": renderer,
            "feature_reduction": feature_reduction,
            "rationale": "Used a unique-value renderer for a categorical field; clustering is enabled for point layers when appropriate.",
        }

    # Numeric
    st = field_stats(
        gis,
        layer_item_id=layer_item_id,
        layer_url=layer_url,
        layer_index=layer_index,
        numeric_field=field_name,
        where=where_clause,
        group_by_field=None,
        histogram_bins=0,
    )
    rows = st.get("rows") if isinstance(st, dict) else None
    row0 = rows[0] if isinstance(rows, list) and rows else {}
    try:
        min_v = float(row0.get("min")) if row0.get("min") is not None else 0.0
        max_v = float(row0.get("max")) if row0.get("max") is not None else 1.0
    except Exception:
        min_v, max_v = 0.0, 1.0

    breaks = _class_breaks(min_v, max_v, 5)
    infos = []
    for i, (a, b) in enumerate(breaks):
        color = ramp[min(i, len(ramp) - 1)]
        infos.append(
            {
                "minValue": a,
                "maxValue": b,
                "label": f"{a:g} – {b:g}",
                "symbol": _symbol_for_geom(geom_type, fill=color, outline="#ffffff", width=0.8),
            }
        )
    renderer = {"type": "class-breaks", "field": field_name, "classBreakInfos": infos}
    feature_reduction = {"type": "cluster"} if want_cluster else None
    return {
        "geometryType": geom_type,
        "mode": "numeric",
        "field": field_name,
        "renderer": renderer,
        "feature_reduction": feature_reduction,
        "rationale": "Used a class-breaks renderer for a numeric field (equal-interval breaks from min/max).",
    }

