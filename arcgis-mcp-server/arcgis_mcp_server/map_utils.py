"""Pure helpers for map/route logic.

These helpers are intentionally free of ArcGIS runtime dependencies so they can
be unit-tested without a configured GIS connection.
"""

from __future__ import annotations

from typing import Any

DEFAULT_HILLSHADE_URL = "https://services.arcgisonline.com/ArcGIS/rest/services/Elevation/World_Hillshade/MapServer"


def dedupe_points(points: list[tuple[float, float]], tolerance_km: float = 0.05) -> list[tuple[float, float]]:
    """Merge consecutive points within tolerance (default ~50 m).

    Order preserved; first of each near-duplicate run is kept.
    """

    if not points or tolerance_km <= 0:
        return list(points)
    # Approximate km per degree at mid-lat (good enough for ~50 m)
    import math

    result = [points[0]]
    for p in points[1:]:
        last = result[-1]
        dy = (p[1] - last[1]) * 111.0  # lat to km
        dx = (p[0] - last[0]) * 111.0 * math.cos(math.radians((p[1] + last[1]) / 2))
        if (dx * dx + dy * dy) ** 0.5 > tolerance_km:
            result.append(p)
    return result


def geometry_to_dict(geom: Any) -> dict | None:
    """Return a JSON-serializable dict for Esri geometry or None."""

    if geom is None:
        return None
    if isinstance(geom, dict):
        return geom if geom.get("paths") or geom.get("x") else None
    if hasattr(geom, "as_dict"):
        d = geom.as_dict() if callable(geom.as_dict) else geom.as_dict
        return d if isinstance(d, dict) and (d.get("paths") or d.get("x")) else None
    if hasattr(geom, "__geo_interface__"):
        gi = geom.__geo_interface__
        return gi if isinstance(gi, dict) else None
    return None


def extract_first_route_geometry(result: dict) -> dict | None:
    """Get first route's geometry from a route solve result dict."""

    if not result:
        return None
    routes = result.get("routes") or result.get("route") or result.get("Routes")
    if routes is None:
        return None
    if isinstance(routes, dict):
        features = routes.get("features") or routes.get("results") or []
    else:
        features = getattr(routes, "features", None) or getattr(routes, "get", lambda _: None)("features") or []
    if not features:
        return None
    first = features[0]
    if isinstance(first, dict):
        geom = first.get("geometry")
    else:
        geom = getattr(first, "geometry", None) or (
            (getattr(first, "as_dict", None) and (first.as_dict() if callable(first.as_dict) else first.as_dict)) or {}
        ).get("geometry")
    return geometry_to_dict(geom)


def normalize_overlays(overlays: Any, *, terrain: bool | None = None) -> list[dict]:
    """Normalize overlays list; optionally inject default hillshade overlay."""

    out: list[dict] = []
    if isinstance(overlays, list):
        for o in overlays:
            if not isinstance(o, dict):
                continue
            t = (o.get("type") or "").strip()
            u = (o.get("url") or "").strip()
            if not t or not u:
                continue
            entry = {"type": t, "url": u}
            if o.get("opacity") is not None:
                try:
                    entry["opacity"] = float(o.get("opacity"))
                except Exception:
                    pass
            if o.get("title"):
                entry["title"] = str(o.get("title"))
            if o.get("id"):
                entry["id"] = str(o.get("id"))
            if o.get("order") is not None:
                try:
                    entry["order"] = int(o.get("order"))
                except Exception:
                    pass
            out.append(entry)

    if terrain:
        has_hillshade = any(isinstance(o, dict) and o.get("url") == DEFAULT_HILLSHADE_URL for o in out)
        if not has_hillshade:
            out.insert(
                0,
                {
                    "type": "tile",
                    "url": DEFAULT_HILLSHADE_URL,
                    "opacity": 0.6,
                    "title": "Hillshade",
                },
            )

    # Stable ordering when caller provides order
    def _k(o: dict) -> tuple[int, int]:
        ord_val = o.get("order")
        return (0, int(ord_val)) if isinstance(ord_val, int) else (1, 0)

    out.sort(key=_k)
    return out

