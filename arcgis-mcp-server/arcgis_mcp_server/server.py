"""ArcGIS MCP server: streamable HTTP transport and ArcGIS tools."""

from __future__ import annotations

import base64
import contextlib
import json
import logging
import os
import re
import secrets
import time
import urllib.parse
from collections.abc import AsyncIterator
from contextvars import ContextVar
from pathlib import Path

import anyio
import click
from mcp import types
from mcp.server import Server
from pydantic import AnyUrl
from mcp.server.lowlevel.helper_types import ReadResourceContents
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from starlette.applications import Starlette
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from starlette.routing import Mount, Route
from starlette.types import Receive, Scope, Send

from .config import get_gis
from .state_store import create_store_from_env, new_state_id
from .tools.layer_intelligence import describe_layer as _describe_layer  # noqa: E402
from .tools.layer_intelligence import distinct_values as _distinct_values  # noqa: E402
from .tools.layer_intelligence import field_stats as _field_stats  # noqa: E402
from .tools.layer_intelligence import sample_features as _sample_features  # noqa: E402
from .tools.symbology_intelligence import suggest_symbology as _suggest_symbology  # noqa: E402

logger = logging.getLogger(__name__)

MAP_STATE_SCHEMA_VERSION = 3
DEFAULT_BASEMAP_ID = "streets"
# Public ArcGIS Online services (do not require portal item IDs)
DEFAULT_HILLSHADE_URL = "https://services.arcgisonline.com/ArcGIS/rest/services/Elevation/World_Hillshade/MapServer"
DEFAULT_ELEVATION3D_URL = "https://elevation3d.arcgis.com/arcgis/rest/services/WorldElevation3D/Terrain3D/ImageServer"

# Pure helpers (kept as server-level names for backwards compatibility)
from .map_utils import (  # noqa: E402
    DEFAULT_HILLSHADE_URL as DEFAULT_HILLSHADE_URL,  # re-export
    dedupe_points as _dedupe_points,
    extract_first_route_geometry as _extract_first_route_geometry,
    normalize_overlays as _normalize_overlays,
)

def _map_state_ttl_seconds() -> int:
    raw = (os.environ.get("ARCGIS_MAP_STATE_TTL_SECONDS") or "").strip()
    try:
        v = int(raw) if raw else 7 * 24 * 60 * 60
        return max(60, min(v, 30 * 24 * 60 * 60))
    except ValueError:
        return 7 * 24 * 60 * 60


# Guardrail: prevent huge in-memory map states (renderer payloads, etc.)
def _map_state_max_bytes() -> int:
    raw = (os.environ.get("ARCGIS_MAP_STATE_MAX_BYTES") or "").strip()
    try:
        v = int(raw) if raw else 250_000
        return max(50_000, min(v, 2_000_000))
    except ValueError:
        return 250_000


def _session_token_ttl_seconds() -> int:
    """TTL for session-scoped ArcGIS tokens stored server-side."""
    raw = (os.environ.get("ARCGIS_SESSION_TOKEN_TTL_SECONDS") or "").strip()
    try:
        v = int(raw) if raw else 7 * 24 * 60 * 60
        return max(60, min(v, 30 * 24 * 60 * 60))
    except ValueError:
        return 7 * 24 * 60 * 60


# Map state storage is handled by the shared store (_store).


def _sanitize_map_state_for_storage(state: dict) -> dict:
    """Store only whitelisted, non-secret map state fields."""
    allow = {
        "schemaVersion",
        "center",
        "zoom",
        "bbox",
        "markers",
        "routeGeometry",
        "routeSummary",
        "graphics",
        "layer",
        "layers",
        "basemapId",
        "overlays",
        "viewMode",
        "ground",
        "analysis",
        "message",
    }
    out = {k: v for k, v in (state or {}).items() if k in allow}
    # Never persist share URLs inside the state (prevents recursion and accidental leakage)
    out.pop("map_url", None)
    out.pop("map_url_query", None)
    out.pop("map_state_id", None)
    return out


def _cleanup_map_state_store(now: float | None = None) -> None:
    """Legacy no-op (map state TTL handled by store)."""
    return None


def _store_map_state(state: dict) -> str:
    state_id = new_state_id()
    sanitized = _sanitize_map_state_for_storage(state)
    try:
        payload = json.dumps(sanitized, default=_json_serial_default).encode("utf-8")
    except Exception:
        payload = json.dumps(sanitized).encode("utf-8")
    if len(payload) > _map_state_max_bytes():
        raise ValueError(f"Map state too large ({len(payload)} bytes). Reduce layers/graphics or styling payloads.")
    _store.map_state_put(state_id=state_id, state=sanitized, ttl_seconds=_map_state_ttl_seconds())
    return state_id


def _get_map_state(state_id: str) -> dict | None:
    return _store.map_state_get(state_id=state_id)


def _json_serial_default(obj):
    """JSON serializer for ArcGIS SDK objects (geometry, FeatureSet, etc.)."""
    if hasattr(obj, "as_dict"):
        v = getattr(obj, "as_dict")
        return v() if callable(v) else v
    if hasattr(obj, "__geo_interface__"):
        return obj.__geo_interface__
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _geometry_to_dict(geom) -> dict | None:
    """Return a JSON-serializable dict for Esri geometry (paths + spatialReference) or None."""
    if geom is None:
        return None
    if isinstance(geom, dict):
        return geom if geom.get("paths") or geom.get("x") else None
    if hasattr(geom, "as_dict"):
        d = geom.as_dict() if callable(geom.as_dict) else geom.as_dict
        return d if isinstance(d, dict) and (d.get("paths") or d.get("x")) else None
    if hasattr(geom, "__geo_interface__"):
        return geom.__geo_interface__
    return None


def _extract_first_route_geometry(result: dict) -> dict | None:
    """Get first route's geometry from Route solve result (dict or object). Tries 'routes', 'route', 'Routes'."""
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
        geom = getattr(first, "geometry", None) or (getattr(first, "as_dict", None) and (first.as_dict() if callable(first.as_dict) else first.as_dict) or {}).get("geometry")
    return _geometry_to_dict(geom)


def _normalize_route_geometry(route_geojson: dict | None) -> dict | None:
    """Normalize route geometry to an Esri polyline dict in WGS84 when possible.

    Accepts:
    - GeoJSON-like LineString: {"coordinates": [[lon,lat], ...]}
    - Esri polyline: {"paths": [[[x,y], ...], ...], "spatialReference": {...}}
    Returns:
    - Esri polyline: {"paths": ..., "spatialReference": {"wkid": 4326}}
    """
    if not isinstance(route_geojson, dict) or not route_geojson:
        return None

    # GeoJSON-ish LineString -> Esri polyline
    if isinstance(route_geojson.get("coordinates"), list) and route_geojson.get("paths") is None:
        coords = route_geojson.get("coordinates") or []
        if coords and isinstance(coords[0], (list, tuple)) and len(coords[0]) >= 2:
            paths = [[(float(c[0]), float(c[1])) for c in coords if isinstance(c, (list, tuple)) and len(c) >= 2]]
            return {"paths": paths, "spatialReference": {"wkid": 4326}}

    # Esri polyline passthrough (optionally project to WGS84)
    if isinstance(route_geojson.get("paths"), list):
        sr = route_geojson.get("spatialReference") if isinstance(route_geojson.get("spatialReference"), dict) else {}
        wkid = sr.get("wkid") or sr.get("latestWkid")
        if wkid in (None, 4326):
            out = dict(route_geojson)
            out["spatialReference"] = {"wkid": 4326}
            return out
        # Best-effort projection using portal geometry service (if available)
        try:
            import arcgis.geometry
            gis = _get_gis()
            projected = arcgis.geometry.project(
                geometries=[route_geojson],
                in_sr=int(wkid),
                out_sr=4326,
                gis=gis,
            )
            if projected and isinstance(projected, list) and isinstance(projected[0], dict) and projected[0].get("paths"):
                projected[0]["spatialReference"] = {"wkid": 4326}
                return projected[0]
        except Exception:
            # Fall back to original geometry with declared spatial reference
            return route_geojson
        return route_geojson

    return None


def _normalize_overlays(overlays, terrain: bool | None = None) -> list[dict]:
    """Normalize overlays list; optionally inject default hillshade overlay when terrain=True."""
    out: list[dict] = []
    if isinstance(overlays, list):
        for o in overlays:
            if not isinstance(o, dict):
                continue
            t = (o.get("type") or "").strip().lower()
            url = (o.get("url") or "").strip()
            if not t or not url:
                continue
            entry = {
                "type": t,
                "url": url,
            }
            if o.get("opacity") is not None:
                try:
                    entry["opacity"] = max(0.0, min(1.0, float(o.get("opacity"))))
                except (TypeError, ValueError):
                    pass
            if o.get("title"):
                entry["title"] = str(o.get("title"))
            if o.get("id"):
                entry["id"] = str(o.get("id"))
            if o.get("order") is not None:
                try:
                    entry["order"] = int(o.get("order"))
                except (TypeError, ValueError):
                    pass
            out.append(entry)
    if terrain:
        # Insert hillshade first unless user already supplied a hillshade-like overlay
        has_hillshade = any((x.get("url") == DEFAULT_HILLSHADE_URL) or ("hillshade" in (x.get("title") or "").lower()) for x in out)
        if not has_hillshade:
            out.insert(0, {"id": "hillshade", "type": "tile", "url": DEFAULT_HILLSHADE_URL, "opacity": 0.6, "title": "Hillshade"})
    # Stable order: explicit order then insertion order
    out.sort(key=lambda x: (x.get("order", 0), x.get("id", ""), x.get("title", "")))
    return out

# MCP App UI resources (MCP Apps)
RESOURCE_MIME_TYPE = "text/html;profile=mcp-app"
ARCGIS_MAP_APP_URI = "ui://arcgis-map/mcp-app.html"
ARCGIS_STUDIO_APP_URI = "ui://arcgis-studio/mcp-app.html"
ARCGIS_EXAMPLES_APP_URI = "ui://arcgis-examples/mcp-app.html"

def _get_arcgis_map_app_html_path() -> Path:
    """Path to the packaged ArcGIS map app HTML file."""
    return Path(__file__).resolve().parent / "app" / "arcgis-map-app.html"

def _get_arcgis_studio_app_html_path() -> Path:
    """Path to the packaged ArcGIS studio app HTML file."""
    return Path(__file__).resolve().parent / "app" / "arcgis-studio-app.html"


def _get_arcgis_examples_app_html_path() -> Path:
    """Path to the packaged ArcGIS examples app HTML file."""
    return Path(__file__).resolve().parent / "app" / "arcgis-examples-app.html"


def _map_viewer_html(state: dict) -> str:
    """Build HTML for GET /map: standalone map viewer with state from query params (no MCP bridge)."""
    state_json = json.dumps(state)
    # Escape for embedding in script (prevent </script> in JSON breaking the page)
    state_json_escaped = state_json.replace("</", "<\\/")
    api_key = (os.environ.get("ARCGIS_API_KEY") or "").strip().replace("\\", "\\\\").replace('"', '\\"')
    api_key_script = f'<script>window.__arcgis_config__ = {{ apiKey: "{api_key}" }};</script>\n  ' if api_key else ""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>ArcGIS Map</title>
  {api_key_script}<script type="module" src="https://js.arcgis.com/5.0/"></script>
  <link rel="stylesheet" href="https://js.arcgis.com/5.0/esri/themes/light/main.css">
  <style>html, body {{ height: 100%; margin: 0; padding: 0; }} #arcgis-map {{ min-height: 100%; }} #map-loading {{ position: fixed; inset: 0; background: #f5f5f5; display: flex; align-items: center; justify-content: center; z-index: 9999; font-family: sans-serif; }}</style>
</head>
<body>
  <div id="map-loading" aria-live="polite">Loading map…</div>
  <div id="map-route-summary" role="status" style="position:fixed;bottom:12px;left:50%;transform:translateX(-50%);background:rgba(0,0,0,0.75);color:#fff;padding:6px 12px;border-radius:6px;font-size:14px;z-index:1000;display:none;"></div>
  <arcgis-map id="arcgis-map" basemap="streets" style="width:100%;height:100%;" aria-label="Map">
    <arcgis-zoom slot="top-left"></arcgis-zoom>
    <arcgis-search slot="top-right"></arcgis-search>
    <arcgis-scale-bar slot="bottom-left"></arcgis-scale-bar>
    <arcgis-basemap-toggle slot="bottom-right"></arcgis-basemap-toggle>
  </arcgis-map>
  <arcgis-scene id="arcgis-scene" basemap="streets" style="width:100%;height:100%;display:none;" aria-label="3D Scene">
    <arcgis-zoom slot="top-left"></arcgis-zoom>
    <arcgis-search slot="top-right"></arcgis-search>
    <arcgis-scale-bar slot="bottom-left"></arcgis-scale-bar>
    <arcgis-basemap-toggle slot="bottom-right"></arcgis-basemap-toggle>
  </arcgis-scene>
  <script type="module">
    window.__MAP_STATE__ = {state_json_escaped};
  </script>
  <script type="module">
    (async function () {{
      const loadingEl = document.getElementById("map-loading");
      try {{
      await customElements.whenDefined("arcgis-map");
      const state = window.__MAP_STATE__ || {{}};
      const mapEl = document.getElementById("arcgis-map");
      const sceneEl = document.getElementById("arcgis-scene");
      await Promise.race([customElements.whenDefined("arcgis-scene"), new Promise(r => setTimeout(r, 1000))]);
      await Promise.race([mapEl.viewOnReady(), new Promise(r => setTimeout(r, 10000))]);
      if (sceneEl) {{ try {{ await Promise.race([sceneEl.viewOnReady(), new Promise(r => setTimeout(r, 10000))]); }} catch (_) {{}} }}
      const arcgis = globalThis.$arcgis || (typeof window !== "undefined" && window.$arcgis);
      if (!arcgis) {{ if (loadingEl) loadingEl.style.display = "none"; return; }}
      const Graphic = await arcgis.import("@arcgis/core/Graphic.js");
      const Point = await arcgis.import("@arcgis/core/geometry/Point.js");
      const Polyline = await arcgis.import("@arcgis/core/geometry/Polyline.js");
      const SimpleMarkerSymbol = await arcgis.import("@arcgis/core/symbols/SimpleMarkerSymbol.js");
      const SimpleLineSymbol = await arcgis.import("@arcgis/core/symbols/SimpleLineSymbol.js");
      const SpatialReference = await arcgis.import("@arcgis/core/geometry/SpatialReference.js");
      const Layer = await arcgis.import("@arcgis/core/layers/Layer.js");
      const FeatureLayer = await arcgis.import("@arcgis/core/layers/FeatureLayer.js");
      const TileLayer = await arcgis.import("@arcgis/core/layers/TileLayer.js");
      const ElevationLayer = await arcgis.import("@arcgis/core/layers/ElevationLayer.js");
      let overlayLayers = [];
      let opLayers = [];
      let groundLayer = null;
      function getActiveEl() {{
        const mode = (state && state.viewMode) ? String(state.viewMode).toLowerCase() : "2d";
        if (mode === "3d" && sceneEl && sceneEl.view) {{
          sceneEl.style.display = "block";
          mapEl.style.display = "none";
          return sceneEl;
        }}
        mapEl.style.display = "block";
        if (sceneEl) sceneEl.style.display = "none";
        return mapEl;
      }}
      const activeEl = getActiveEl();
      if (!activeEl || !activeEl.view) return;
      const view = activeEl.view;
      const layer = activeEl.graphics;
      function clearGraphics() {{ if (layer && layer.length) layer.removeAll(); }}
      async function applyOverlays() {{
        if (!activeEl.map) return;
        for (const lyr of overlayLayers) {{ try {{ activeEl.map.layers.remove(lyr); }} catch (_) {{}} }}
        overlayLayers = [];
        if (!Array.isArray(state.overlays) || !state.overlays.length) return;
        for (const o of state.overlays) {{
          if (!o || typeof o !== "object") continue;
          const type = (o.type || "").toString().toLowerCase();
          const url = (o.url || "").toString();
          if (!type || !url) continue;
          try {{
            let lyr = null;
            if (type === "tile") {{
              lyr = new TileLayer({{ url }});
            }} else {{
              if (/\\/(FeatureServer)\\/\\d+\\/?$/i.test(url)) {{
                lyr = new FeatureLayer({{ url }});
              }} else {{
                lyr = await Layer.fromArcGISServerUrl({{ url }});
              }}
            }}
            if (!lyr) continue;
            if (o.opacity != null) {{
              const op = Number(o.opacity);
              if (!Number.isNaN(op)) lyr.opacity = Math.max(0, Math.min(1, op));
            }}
            activeEl.map.layers.add(lyr);
            overlayLayers.push(lyr);
          }} catch (e) {{ console.warn("Could not load overlay", e); }}
        }}
      }}
      async function applyGround() {{
        const mode = (state && state.viewMode) ? String(state.viewMode).toLowerCase() : "2d";
        if (mode !== "3d" || !activeEl.map || !activeEl.map.ground) return;
        const gc = (state && typeof state.ground === "object" && state.ground) ? state.ground : {{}};
        const url = (gc.elevationUrl || gc.url || "").toString().trim() || "{DEFAULT_ELEVATION3D_URL}";
        try {{
          if (groundLayer) {{ activeEl.map.ground.layers.remove(groundLayer); groundLayer = null; }}
          const el = new ElevationLayer({{ url }});
          activeEl.map.ground.layers.add(el);
          groundLayer = el;
        }} catch (e) {{ console.warn("Could not set ground", e); }}
      }}
      if (state.basemapId) {{ try {{ activeEl.basemap = state.basemapId; }} catch (_) {{}} }}
      await applyGround();
      await applyOverlays();
      if (state.center && state.center.longitude != null && state.center.latitude != null)
        view.center = [state.center.longitude, state.center.latitude];
      if (state.zoom != null) view.zoom = state.zoom;
      if (state.bbox && state.bbox.length === 4)
        view.goTo({{ type: "extent", xmin: state.bbox[0], ymin: state.bbox[1], xmax: state.bbox[2], ymax: state.bbox[3], spatialReference: {{ wkid: 4326 }} }}).catch(() => {{}});
      clearGraphics();
      if (state.markers && state.markers.length) {{
        const sym = new SimpleMarkerSymbol({{ color: [255, 0, 0, 0.8], size: 12, outline: {{ color: [255, 255, 255], width: 2 }} }});
        state.markers.forEach((m, i) => {{
          const pt = new Point({{ x: m.x, y: m.y, spatialReference: new SpatialReference({{ wkid: 4326 }}) }});
          const title = state.markers.length > 1 ? "Stop " + (i + 1) : "Location";
          const content = m.y.toFixed(5) + ", " + m.x.toFixed(5);
          layer.add(new Graphic({{ geometry: pt, symbol: sym, popupTemplate: {{ title: title, content: content }} }}));
        }});
      }}
      if (state.routeGeometry && state.routeGeometry.coordinates) {{
        const coords = state.routeGeometry.coordinates;
        const paths = [coords.map(c => [c[0], c[1]])];
        const polyline = new Polyline({{ paths, spatialReference: new SpatialReference({{ wkid: 4326 }}) }});
        layer.add(new Graphic({{ geometry: polyline, symbol: new SimpleLineSymbol({{ color: [0, 122, 255, 0.9], width: 3 }}) }}));
      }} else if (state.routeGeometry && state.routeGeometry.paths) {{
        const polyline = new Polyline({{ paths: state.routeGeometry.paths, spatialReference: state.routeGeometry.spatialReference ? new SpatialReference(state.routeGeometry.spatialReference) : new SpatialReference({{ wkid: 4326 }}) }});
        layer.add(new Graphic({{ geometry: polyline, symbol: new SimpleLineSymbol({{ color: [0, 122, 255, 0.9], width: 3 }}) }}));
      }}
      function applyGraphicsFromState() {{
        if (!Array.isArray(state.graphics) || !state.graphics.length) return;
        for (const g of state.graphics) {{
          if (!g || typeof g !== "object") continue;
          const geom = g.geometry && typeof g.geometry === "object" ? g.geometry : null;
          if (!geom) continue;
          if (!geom.spatialReference) geom.spatialReference = {{ wkid: 4326 }};
          const graphic = new Graphic({{
            geometry: geom,
            symbol: (g.symbol && typeof g.symbol === "object") ? g.symbol : undefined,
            attributes: (g.attributes && typeof g.attributes === "object") ? g.attributes : undefined,
            popupTemplate: (g.popupTemplate && typeof g.popupTemplate === "object") ? g.popupTemplate : undefined,
          }});
          layer.add(graphic);
        }}
      }}

      async function clearOperationalLayers() {{
        if (!activeEl.map) return;
        for (const lyr of opLayers) {{
          try {{ activeEl.map.layers.remove(lyr); }} catch (_) {{}}
        }}
        opLayers = [];
      }}

      async function applyLayerEntry(entry) {{
        if (!entry || typeof entry !== "object") return null;
        const src = (entry.source && typeof entry.source === "object") ? entry.source : entry;
        const itemId = (src.item_id || src.itemId || "").toString().trim();
        const idxRaw = src.layer_index != null ? src.layer_index : (src.layerId != null ? src.layerId : null);
        const idx = (idxRaw != null && idxRaw !== "") ? Number(idxRaw) : null;
        const urlRaw = (src.url || "").toString().trim();
        let opLayer = null;
        if (itemId) {{
          opLayer = await Layer.fromPortalItem({{
            portalItem: {{ id: itemId }},
            layerId: idx != null && !Number.isNaN(idx) ? idx : undefined,
          }});
        }} else if (urlRaw) {{
          let url = urlRaw;
          if (idx != null && !Number.isNaN(idx) && /\\/(FeatureServer|MapServer|ImageServer|VectorTileServer)\\/?$/i.test(url)) {{
            url = url.replace(/\\/+$/, "") + "/" + String(idx);
          }}
          if (/\\/(FeatureServer)\\/\\d+\\/?$/i.test(url)) {{
            opLayer = new FeatureLayer({{ url }});
          }} else {{
            opLayer = await Layer.fromArcGISServerUrl({{ url }});
          }}
        }}
        if (!opLayer) return null;
        try {{
          if (entry.opacity != null) {{
            const op = Number(entry.opacity);
            if (!Number.isNaN(op)) opLayer.opacity = Math.max(0, Math.min(1, op));
          }}
        }} catch (_) {{}}
        try {{ if (entry.definitionExpression && ("definitionExpression" in opLayer)) opLayer.definitionExpression = String(entry.definitionExpression); }} catch (_) {{}}
        try {{ if (entry.outFields != null && ("outFields" in opLayer)) opLayer.outFields = entry.outFields; }} catch (_) {{}}
        try {{ if (entry.renderer && ("renderer" in opLayer)) opLayer.renderer = entry.renderer; }} catch (_) {{}}
        try {{
          if (entry.labelingInfo && ("labelingInfo" in opLayer)) {{
            opLayer.labelingInfo = Array.isArray(entry.labelingInfo) ? entry.labelingInfo : [entry.labelingInfo];
            opLayer.labelsVisible = true;
          }}
        }} catch (_) {{}}
        try {{ if (entry.featureReduction && ("featureReduction" in opLayer)) opLayer.featureReduction = entry.featureReduction; }} catch (_) {{}}
        try {{ if (entry.elevationInfo && ("elevationInfo" in opLayer)) opLayer.elevationInfo = entry.elevationInfo; }} catch (_) {{}}
        try {{ if (entry.popupTemplate && ("popupTemplate" in opLayer)) opLayer.popupTemplate = entry.popupTemplate; }} catch (_) {{}}
        // Default popups + outFields so clicking features shows metadata
        try {{
          if (("popupEnabled" in opLayer)) opLayer.popupEnabled = true;
          if (!entry.popupTemplate && ("popupTemplate" in opLayer) && !opLayer.popupTemplate) {{
            opLayer.popupTemplate = {{ title: "Feature", content: "{{*}}" }};
          }}
        }} catch (_) {{}}
        try {{
          if (entry.outFields == null && ("outFields" in opLayer) && (!opLayer.outFields || !opLayer.outFields.length)) {{
            opLayer.outFields = ["*"];
          }}
        }} catch (_) {{}}
        return opLayer;
      }}

      async function applyOperationalLayers() {{
        if (!activeEl.map) return;
        await clearOperationalLayers();
        const entries = Array.isArray(state.layers) ? state.layers : (state.layer ? [state.layer] : []);
        if (!entries.length) return;
        for (let i = 0; i < entries.length; i++) {{
          const entry = entries[i];
          try {{
            const opLayer = await applyLayerEntry(entry);
            if (!opLayer) continue;
            activeEl.map.layers.add(opLayer, i);
            opLayers.push(opLayer);
          }} catch (e) {{ console.warn("Could not load layer entry", e); }}
        }}
      }}

      applyGraphicsFromState();
      await applyOperationalLayers();
      if ((!state.center || state.center.longitude == null || state.center.latitude == null) && (!state.bbox || state.bbox.length !== 4) && opLayers.length) {{
        try {{
          const first = opLayers[0];
          await first.when();
          if (first.fullExtent) view.goTo(first.fullExtent).catch(() => {{}});
        }} catch (_) {{}}
      }}
      const summaryEl = document.getElementById("map-route-summary");
      if (summaryEl && state.routeSummary) {{
        const min = state.routeSummary.total_time_min;
        const mi = state.routeSummary.total_length_mi;
        let s = "";
        if (min != null) s += "~" + Math.round(min / 60) + " h";
        if (mi != null) s += (s ? ", " : "") + Math.round(mi) + " mi";
        if (s) {{ summaryEl.textContent = s; summaryEl.style.display = "block"; }}
      }}
      }} catch (e) {{
        if (loadingEl) {{ loadingEl.textContent = "Map could not load."; loadingEl.dataset.error = "1"; }}
      }} finally {{
        if (loadingEl && !loadingEl.dataset.error) loadingEl.style.display = "none";
      }}
    }})();
  </script>
</body>
</html>"""


# Per-request ArcGIS token: from header X-ArcGIS-Token or from session store (keyed by Mcp-Session-Id)
_arcgis_token_var: ContextVar[str | None] = ContextVar("arcgis_token", default=None)
_arcgis_referer_var: ContextVar[str | None] = ContextVar("arcgis_referer", default=None)
_current_session_id_var: ContextVar[str | None] = ContextVar("arcgis_session_id", default=None)
_request_id_var: ContextVar[str | None] = ContextVar("request_id", default=None)

# Shared store (in-memory by default; Redis optional via ARCGIS_REDIS_URL)
_store = create_store_from_env()
_CACHE_TTL_SECONDS = 60 * 60 * 24  # 24h
_CODE_TTL_SECONDS = 300  # 5 minutes

# Shared GIS instance at startup (fallback when no per-request token)
_gis = None


def _ctx() -> dict:
    """Best-effort request context for logs/responses."""
    return {
        "request_id": _request_id_var.get(),
        "session_id": _current_session_id_var.get(),
    }


_tool_limiters: dict[str, anyio.CapacityLimiter] = {}


def _tool_limiter(tool_name: str) -> anyio.CapacityLimiter:
    """Per-tool concurrency limiter (prevents portal overload)."""
    name = (tool_name or "").strip() or "unknown"
    if name in _tool_limiters:
        return _tool_limiters[name]
    # Env overrides: ARCGIS_TOOL_CONCURRENCY_<TOOLNAME>, else ARCGIS_TOOL_CONCURRENCY_DEFAULT
    import re

    env_suffix = re.sub(r"[^A-Za-z0-9]+", "_", name).upper()
    raw = (os.environ.get(f"ARCGIS_TOOL_CONCURRENCY_{env_suffix}") or os.environ.get("ARCGIS_TOOL_CONCURRENCY_DEFAULT") or "").strip()
    try:
        n = int(raw) if raw else 8
    except ValueError:
        n = 8
    n = max(1, min(n, 64))
    lim = anyio.CapacityLimiter(n)
    _tool_limiters[name] = lim
    return lim


def _rate_limit(key: str, limit: int, per_seconds: float) -> bool:
    """Return True if allowed; False if rate limited."""
    return bool(_store.rate_limit_allow(key=key, limit=int(limit), per_seconds=float(per_seconds)))


def _with_retries(fn, *, tries: int = 3, base_delay_s: float = 0.3):
    """Run fn with simple exponential backoff retries for transient errors."""
    last = None
    for i in range(max(1, tries)):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001 - intentional: shields tool from transient network failures
            last = e
            if i >= tries - 1:
                raise
            time.sleep(base_delay_s * (2**i))
    raise last  # pragma: no cover

def _gis_from_token(token: str, referer: str) -> "object":
    """Create a GIS instance from an OAuth/portal token (e.g. from Okta SSO)."""
    from arcgis.gis import GIS
    return GIS(token=token, referer=referer)


def _get_gis():
    """Return GIS for this request: per-request token (context var or session/pending fallback), else global _gis."""
    token = _arcgis_token_var.get()
    referer = _arcgis_referer_var.get() or os.environ.get("ARCGIS_URL", "").strip() or "https://www.arcgis.com"
    if not token or not token.strip():
        # Context var may be unset when tool runs in a different task than middleware (stateful mode).
        # Fall back to session store or pending token so login works even when client does not send Mcp-Session-Id.
        session_id = _current_session_id_var.get()
        if session_id:
            stored = _store.session_token_get(session_id=session_id)
            if isinstance(stored, dict):
                token = stored.get("token")
                referer = referer or stored.get("referer")
        pending = _store.pending_token_get()
        if (not token or not token.strip()) and isinstance(pending, dict):
            token = pending.get("token")
            referer = referer or pending.get("referer")
    if token and token.strip():
        return _gis_from_token(token.strip(), referer or "https://www.arcgis.com")
    if _gis is None:
        raise RuntimeError("GIS not initialized")
    return _gis


# --- Tool definitions for list_tools ---

TOOLS_LIST = [
    types.Tool(
        name="search_content",
        description="Search portal items by query and optional type filter.",
        inputSchema={
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query string"},
                "item_type": {"type": "string", "description": "Optional item type filter (e.g. Feature Service, Map Service)"},
                "max_items": {"type": "integer", "description": "Maximum number of items to return (default 10)", "default": 10},
            },
            "required": ["query"],
        },
    ),
    types.Tool(
        name="geocode",
        description="Geocode an address and return candidate locations. Prefer run_intent unless you need low-level geocoding control.",
        inputSchema={
            "type": "object",
            "properties": {
                "address": {"type": "string", "description": "Address or place name to geocode"},
                "max_locations": {"type": "integer", "description": "Maximum number of results (default 5)", "default": 5},
            },
            "required": ["address"],
        },
    ),
    types.Tool(
        name="get_item",
        description="Get a portal item by ID.",
        inputSchema={
            "type": "object",
            "properties": {
                "item_id": {"type": "string", "description": "Portal item ID"},
            },
            "required": ["item_id"],
        },
    ),
    types.Tool(
        name="resolve_layer",
        description="Resolve a portal item id or an ArcGIS service URL into a layer descriptor usable by show_map/route_and_show_map (works even when content search returns no items).",
        inputSchema={
            "type": "object",
            "properties": {
                "item_id": {"type": "string", "description": "Optional. Portal item ID of a layer/service."},
                "url": {"type": "string", "description": "Optional. ArcGIS service URL (FeatureServer/MapServer/ImageServer/VectorTileServer)."},
                "layer_index": {"type": "integer", "description": "Optional 0-based layer index when resolving a service root URL. Default 0.", "default": 0},
            },
            "required": [],
        },
    ),
    types.Tool(
        name="whoami",
        description="Return current user and portal info (no arguments).",
        inputSchema={"type": "object", "properties": {}},
    ),
    types.Tool(
        name="get_arcgis_login_url",
        description="Return the URL for the user to sign in to ArcGIS (Okta SSO). User opens the URL, signs in; the callback page shows a one-time code. When the user says that code (e.g. 'use ArcGIS code a1b2c3d4'), call set_arcgis_token_with_code with the code argument, NOT set_arcgis_token.",
        inputSchema={
            "type": "object",
            "properties": {
                "base_url": {"type": "string", "description": "Optional. Server base URL (e.g. https://your-ngrok.ngrok-free.app). If omitted, uses ARCGIS_AUTH_BASE_URL from env."},
            },
            "required": [],
        },
    ),
    types.Tool(
        name="set_arcgis_token",
        description="Store an ArcGIS token for this session. After the user signs in at the login URL, they copy the token from the callback page and pass it here. All subsequent tool calls in this session use this token. Prefer set_arcgis_token_with_code when the user gets a one-time code (avoids content filters).",
        inputSchema={
            "type": "object",
            "properties": {
                "token": {"type": "string", "description": "ArcGIS OAuth/portal token from the /auth/callback page after sign-in."},
                "referer": {"type": "string", "description": "Optional. Referer for the token (e.g. https://your-org.maps.arcgis.com). Defaults to ARCGIS_URL or https://www.arcgis.com."},
            },
            "required": ["token"],
        },
    ),
    types.Tool(
        name="set_arcgis_token_with_code",
        description="Store an ArcGIS token for this session using a one-time code. After the user signs in at the login URL, the callback page shows a short code (e.g. a1b2c3d4). The user tells the agent that code; call this tool with it. Use this when pasting the raw token triggers content filters.",
        inputSchema={
            "type": "object",
            "properties": {
                "code": {"type": "string", "description": "The one-time code shown on the /auth/callback page after sign-in (e.g. a1b2c3d4)."},
            },
            "required": ["code"],
        },
    ),
    types.Tool(
        name="reverse_geocode",
        description="Convert x,y (longitude, latitude) to an address. Returns the nearest address or place at the given location.",
        inputSchema={
            "type": "object",
            "properties": {
                "longitude": {"type": "number", "description": "Longitude (x) of the point."},
                "latitude": {"type": "number", "description": "Latitude (y) of the point."},
                "distance": {"type": "number", "description": "Optional. Radial distance in meters to search (default 100)."},
            },
            "required": ["longitude", "latitude"],
        },
    ),
    types.Tool(
        name="suggest",
        description="Get address/place suggestions as the user types. Use for autocomplete.",
        inputSchema={
            "type": "object",
            "properties": {
                "text": {"type": "string", "description": "Partial address or place name to get suggestions for."},
                "latitude": {"type": "number", "description": "Optional. Origin latitude to boost results near this location."},
                "longitude": {"type": "number", "description": "Optional. Origin longitude to boost results near this location."},
                "max_suggestions": {"type": "integer", "description": "Maximum number of suggestions (default 5).", "default": 5},
            },
            "required": ["text"],
        },
    ),
    types.Tool(
        name="get_item_details",
        description="Get extended details for a portal item: description, snippet, size, created/modified, thumbnail URL, etc.",
        inputSchema={
            "type": "object",
            "properties": {
                "item_id": {"type": "string", "description": "Portal item ID"},
            },
            "required": ["item_id"],
        },
    ),
    types.Tool(
        name="list_my_content",
        description="List the current user's portal items. Requires signed-in user.",
        inputSchema={
            "type": "object",
            "properties": {
                "folder": {"type": "string", "description": "Optional. Folder name or ID to list items from."},
                "item_type": {"type": "string", "description": "Optional. Filter by item type (e.g. Feature Service, Web Map)."},
                "max_items": {"type": "integer", "description": "Maximum number of items to return (default 20).", "default": 20},
            },
            "required": [],
        },
    ),
    types.Tool(
        name="query_layer",
        description="Query a feature layer or table by portal item ID. Returns features matching the where clause.",
        inputSchema={
            "type": "object",
            "properties": {
                "item_id": {"type": "string", "description": "Portal item ID of the feature service or feature layer."},
                "layer_index": {"type": "integer", "description": "Layer or table index (0-based). Default 0.", "default": 0},
                "where": {"type": "string", "description": "SQL WHERE clause (default 1=1).", "default": "1=1"},
                "out_fields": {"type": "string", "description": "Comma-separated field names or * for all. Default *.", "default": "*"},
                "return_geometry": {"type": "boolean", "description": "Include geometry in results. Default true.", "default": True},
                "max_records": {"type": "integer", "description": "Maximum features to return (default 100).", "default": 100},
            },
            "required": ["item_id"],
        },
    ),
    types.Tool(
        name="geometry_buffer",
        description="Buffer a point or line geometry by a distance. Returns polygon(s).",
        inputSchema={
            "type": "object",
            "properties": {
                "geometry": {"type": "object", "description": "Geometry as JSON: point {x, y, spatialReference}, or polyline paths, etc. For a simple point use x, y, in_sr instead."},
                "x": {"type": "number", "description": "Longitude (if geometry omitted)."},
                "y": {"type": "number", "description": "Latitude (if geometry omitted)."},
                "in_sr": {"type": "integer", "description": "Well-known ID of input spatial reference (e.g. 4326 for WGS84). Default 4326.", "default": 4326},
                "distance": {"type": "number", "description": "Buffer distance in the given unit."},
                "unit": {"type": "string", "description": "Distance unit: Meter, Kilometer, Foot, Mile, etc. Default Meter.", "default": "Meter"},
                "out_sr": {"type": "integer", "description": "Optional. Well-known ID for output spatial reference."},
            },
            "required": ["distance"],
        },
    ),
    types.Tool(
        name="geometry_project",
        description="Reproject geometry from one spatial reference to another.",
        inputSchema={
            "type": "object",
            "properties": {
                "geometry": {"type": "object", "description": "Geometry as JSON (point: {x, y}, or with spatialReference)."},
                "in_sr": {"type": "integer", "description": "Well-known ID of input spatial reference (e.g. 4326 for WGS84)."},
                "out_sr": {"type": "integer", "description": "Well-known ID of output spatial reference (e.g. 3857 for Web Mercator)."},
            },
            "required": ["geometry", "in_sr", "out_sr"],
        },
    ),
    types.Tool(
        name="route",
        description="Solve a route between stops. Stops can be [longitude, latitude] pairs or address strings (geocoded). Uses the portal's route service or an optional route layer item. The response includes route_geometry and directions. For 'directions from A to B and show me on a map', use route_and_show_map instead so the route line appears on the map.",
        inputSchema={
            "type": "object",
            "properties": {
                "stops": {
                    "type": "array",
                    "description": "List of stops: each is [longitude, latitude] or an address string to geocode.",
                    "items": {},
                },
                "item_id": {"type": "string", "description": "Optional. Portal item ID of a route service. If omitted, uses the portal's default route service."},
            },
            "required": ["stops"],
        },
    ),
    types.Tool(
        name="route_and_show_map",
        description="Solve a route between stops and return map state with the route line drawn. Prefer run_intent for natural-language directions. For directions from A to B, pass exactly two stops: [origin, destination]. Stops are [longitude, latitude] or address strings. Response includes directions and map_url when base URL is set.",
        inputSchema={
            "type": "object",
            "properties": {
                "stops": {
                    "type": "array",
                    "description": "Exactly two stops for A-to-B directions: [origin, destination]. Each is [longitude, latitude] or an address string. More than two for multi-stop routes.",
                    "items": {},
                },
                "item_id": {"type": "string", "description": "Optional. Portal item ID of a route service."},
                "zoom": {"type": "integer", "description": "Optional map zoom level. Omitted uses default."},
                "layer_item_id": {"type": "string", "description": "Optional portal item ID of a feature layer or web map to add to the map."},
                "layer_url": {"type": "string", "description": "Optional ArcGIS service URL to add as a layer (FeatureServer/MapServer/ImageServer/VectorTileServer). Prefer this when you already have a service URL or portal item search fails."},
                "layer_index": {"type": "integer", "description": "Optional 0-based layer index when layer_item_id has multiple layers. Default 0."},
                "basemap_id": {"type": "string", "description": "Optional basemap id (e.g. streets, topo-vector, satellite)."},
                "view_mode": {"type": "string", "description": "Optional view mode: '2d' or '3d'. Default is 2d.", "enum": ["2d", "3d"]},
                "terrain": {"type": "boolean", "description": "Optional. If true, adds a default terrain hillshade overlay (and enables elevation in 3D).", "default": False},
                "overlays": {"type": "array", "description": "Optional overlays to add (e.g. hillshade tiles, imagery). Each overlay is {type,url,opacity?,title?,id?,order?}.", "items": {"type": "object"}},
                "ground": {"type": "object", "description": "Optional ground config for 3D, e.g. {elevationUrl,opacity?}."},
            },
            "required": ["stops"],
        },
        meta={"ui": {"resourceUri": ARCGIS_MAP_APP_URI}},
    ),
    types.Tool(
        name="show_map",
        description="Display an interactive map. Prefer run_intent for most user requests. Provide center and/or locations (one marker per location; near-duplicates merged). For directions with a route line, use route_and_show_map.",
        inputSchema={
            "type": "object",
            "properties": {
                "center": {"type": "string", "description": "Center of the map: address or 'longitude,latitude'. Required if locations and route_geojson are omitted."},
                "zoom": {"type": "integer", "description": "Optional zoom level. Omitted uses default."},
                "locations": {
                    "type": "array",
                    "description": "Optional points to show as markers: each is an address or [longitude, latitude]. One marker per location (near-duplicate points merged).",
                    "items": {},
                },
                "route_geojson": {
                    "type": "object",
                    "description": "Optional route geometry to draw on the map. Use the geometry from a prior 'route' tool result (e.g. routes.features[0].geometry with 'paths', or GeoJSON with 'coordinates'). Pass this when the user asks to see the route on the map.",
                },
                "layer_item_id": {"type": "string", "description": "Optional portal item ID of a feature layer or web map to add to the map (from search_content or get_item). Public layers load without auth; secured layers require the client to supply a token."},
                "layer_url": {"type": "string", "description": "Optional ArcGIS service URL to add as a layer (FeatureServer/MapServer/ImageServer/VectorTileServer). Prefer this when you already have a service URL or portal item search fails."},
                "layer_index": {"type": "integer", "description": "Optional 0-based layer index when layer_item_id is a map service with multiple layers. Default 0."},
                "basemap_id": {"type": "string", "description": "Optional basemap id (e.g. streets, topo-vector, satellite)."},
                "view_mode": {"type": "string", "description": "Optional view mode: '2d' or '3d'. Default is 2d.", "enum": ["2d", "3d"]},
                "terrain": {"type": "boolean", "description": "Optional. If true, adds a default terrain hillshade overlay (and enables elevation in 3D).", "default": False},
                "overlays": {"type": "array", "description": "Optional overlays to add (e.g. hillshade tiles, imagery). Each overlay is {type,url,opacity?,title?,id?,order?}.", "items": {"type": "object"}},
                "ground": {"type": "object", "description": "Optional ground config for 3D, e.g. {elevationUrl,opacity?}."},
            },
            "required": [],
        },
        meta={"ui": {"resourceUri": ARCGIS_MAP_APP_URI}},
    ),
    types.Tool(
        name="open_arcgis_studio",
        description="Open ArcGIS Studio: a richer in-chat UI to search portal content, add layers, apply styles, and run analysis tools.",
        inputSchema={
            "type": "object",
            "properties": {
                "center": {"type": "string", "description": "Optional initial center (address or 'longitude,latitude')."},
                "zoom": {"type": "integer", "description": "Optional initial zoom."},
                "basemap_id": {"type": "string", "description": "Optional basemap id."},
                "view_mode": {"type": "string", "description": "Optional view mode: '2d' or '3d'.", "enum": ["2d", "3d"]},
            },
            "required": [],
        },
        meta={"ui": {"resourceUri": ARCGIS_STUDIO_APP_URI}},
    ),
    types.Tool(
        name="make_renderer_rotation_color",
        description="Build a renderer spec for a point/3D icon layer using rotation and color visual variables (returns JSON renderer object; does not display a map).",
        inputSchema={
            "type": "object",
            "properties": {
                "field": {"type": "string", "description": "Numeric field to drive rotation and color (e.g. a trend percent)."},
                "icon_url": {"type": "string", "description": "Icon image URL (PNG/SVG)."},
                "icon_size_pt": {"type": "integer", "description": "Icon size in points (default 30).", "default": 30},
                "icon_initial_angle": {"type": "number", "description": "Initial icon angle in degrees (default 90 for left-arrow pointing up).", "default": 90},
                "rotation_type": {"type": "string", "description": "Rotation type (default arithmetic).", "default": "arithmetic"},
                "color_stops": {"type": "array", "description": "Optional color stops: [{value:number,color:string},...].", "items": {"type": "object"}},
            },
            "required": ["field", "icon_url"],
        },
    ),
    types.Tool(
        name="make_renderer_classbreaks",
        description="Build a class-breaks renderer spec (returns JSON renderer object; does not display a map).",
        inputSchema={
            "type": "object",
            "properties": {
                "field": {"type": "string", "description": "Numeric field to classify."},
                "class_break_infos": {"type": "array", "description": "Class break infos (ArcGIS JS API spec).", "items": {"type": "object"}},
            },
            "required": ["field", "class_break_infos"],
        },
    ),
    types.Tool(
        name="make_renderer_unique_value",
        description="Build a unique-value renderer spec (returns JSON renderer object; does not display a map).",
        inputSchema={
            "type": "object",
            "properties": {
                "field": {"type": "string", "description": "Field to symbolize by."},
                "unique_value_infos": {"type": "array", "description": "Unique value infos (ArcGIS JS API spec).", "items": {"type": "object"}},
            },
            "required": ["field", "unique_value_infos"],
        },
    ),
    types.Tool(
        name="show_layer_with_renderer",
        description="Show a layer on the map with optional styling (renderer/labels/feature reduction/elevation). Returns MapState with layers[] so the in-chat map and map_url share link render the same visualization.",
        inputSchema={
            "type": "object",
            "properties": {
                "center": {"type": "string", "description": "Optional. Center: address or 'longitude,latitude'. If omitted, the map will zoom to the layer extent when possible."},
                "zoom": {"type": "integer", "description": "Optional zoom level."},
                "locations": {"type": "array", "description": "Optional marker locations: addresses or [lon,lat] pairs.", "items": {}},
                "layer_item_id": {"type": "string", "description": "Portal item ID of a layer/service (preferred when you have an item id)."},
                "layer_url": {"type": "string", "description": "ArcGIS service URL (FeatureServer/MapServer/ImageServer/VectorTileServer)."},
                "layer_index": {"type": "integer", "description": "0-based sublayer index when using a service root URL or multi-layer item (default 0).", "default": 0},
                "opacity": {"type": "number", "description": "Optional layer opacity (0..1)."},
                "definition_expression": {"type": "string", "description": "Optional definitionExpression (SQL where clause) for FeatureLayer."},
                "out_fields": {"description": "Optional outFields for FeatureLayer (array or '*')."},
                "renderer": {"type": "object", "description": "Optional renderer JSON (ArcGIS JS API spec)."},
                "labeling_info": {"description": "Optional labelingInfo (array or single object)."},
                "feature_reduction": {"type": "object", "description": "Optional featureReduction config (e.g. clustering/binning/selection)."},
                "elevation_info": {"type": "object", "description": "Optional elevationInfo for 3D scenes (e.g. {mode:'relative-to-scene'})."},
                "popup_template": {"type": "object", "description": "Optional popupTemplate for the layer."},
                "basemap_id": {"type": "string", "description": "Optional basemap id (e.g. streets, topo-vector, satellite)."},
                "view_mode": {"type": "string", "description": "Optional view mode: '2d' or '3d'.", "enum": ["2d", "3d"]},
                "terrain": {"type": "boolean", "description": "Optional. If true, adds a default hillshade overlay (and enables elevation in 3D).", "default": False},
                "overlays": {"type": "array", "description": "Optional overlays: [{type,url,opacity?,title?,id?,order?},...].", "items": {"type": "object"}},
                "ground": {"type": "object", "description": "Optional ground config for 3D, e.g. {elevationUrl,opacity?}."},
            },
            "required": [],
        },
        meta={"ui": {"resourceUri": ARCGIS_MAP_APP_URI}},
    ),
    types.Tool(
        name="show_rotation_icon_layer_3d",
        description="One-call 3D thematic visualization: rotation+color arrows driven by a numeric field, with percent labels and decluttering. Returns MapState with layers[].",
        inputSchema={
            "type": "object",
            "properties": {
                "layer_item_id": {"type": "string", "description": "Optional portal item id for the layer. If omitted, uses a safe public sample layer."},
                "layer_url": {"type": "string", "description": "Optional layer service URL instead of portal item id."},
                "layer_index": {"type": "integer", "description": "Optional sublayer index (default 0).", "default": 0},
                "field": {"type": "string", "description": "Numeric field to drive rotation and color (default Property_Value_Diff_Percent).", "default": "Property_Value_Diff_Percent"},
                "icon_url": {"type": "string", "description": "Optional icon URL (defaults to a safe public arrow PNG)."},
                "center": {"type": "string", "description": "Optional map center (address or lon,lat)."},
                "zoom": {"type": "integer", "description": "Optional zoom."},
                "basemap_id": {"type": "string", "description": "Optional basemap id."},
                "terrain": {"type": "boolean", "description": "Optional. If true, adds hillshade overlay and 3D ground elevation (default true).", "default": True},
            },
            "required": [],
        },
        meta={"ui": {"resourceUri": ARCGIS_MAP_APP_URI}},
    ),
    types.Tool(
        name="buffer_and_show",
        description="Buffer a location and show the resulting polygon on the map (as a graphic overlay). Returns MapState.",
        inputSchema={
            "type": "object",
            "properties": {
                "location": {"type": "string", "description": "Address or 'longitude,latitude' to buffer."},
                "distance": {"type": "number", "description": "Buffer distance."},
                "unit": {"type": "string", "description": "Distance unit (default Meter).", "default": "Meter"},
                "zoom": {"type": "integer", "description": "Optional zoom."},
                "basemap_id": {"type": "string", "description": "Optional basemap id."},
                "view_mode": {"type": "string", "description": "Optional view mode: '2d' or '3d'.", "enum": ["2d", "3d"]},
                "terrain": {"type": "boolean", "description": "Optional. If true, adds hillshade overlay (and enables elevation in 3D).", "default": False},
                "overlays": {"type": "array", "description": "Optional overlays.", "items": {"type": "object"}},
                "ground": {"type": "object", "description": "Optional ground config for 3D."},
            },
            "required": ["location", "distance"],
        },
        meta={"ui": {"resourceUri": ARCGIS_MAP_APP_URI}},
    ),
    types.Tool(
        name="nearest_and_show",
        description="Find nearest features to a location and show them on the map (as point graphics). Returns MapState.",
        inputSchema={
            "type": "object",
            "properties": {
                "location": {"type": "string", "description": "Address or 'longitude,latitude'."},
                "item_id": {"type": "string", "description": "Portal item ID of a feature layer/service to search."},
                "layer_index": {"type": "integer", "description": "Layer index (default 0).", "default": 0},
                "max_count": {"type": "integer", "description": "Max results (default 10).", "default": 10},
                "max_distance_m": {"type": "number", "description": "Max search distance in meters (default 50000).", "default": 50000},
                "zoom": {"type": "integer", "description": "Optional zoom."},
                "basemap_id": {"type": "string", "description": "Optional basemap id."},
                "view_mode": {"type": "string", "description": "Optional view mode: '2d' or '3d'.", "enum": ["2d", "3d"]},
                "terrain": {"type": "boolean", "description": "Optional terrain hillshade/elevation.", "default": False},
                "overlays": {"type": "array", "description": "Optional overlays.", "items": {"type": "object"}},
                "ground": {"type": "object", "description": "Optional ground config for 3D."},
            },
            "required": ["location", "item_id"],
        },
        meta={"ui": {"resourceUri": ARCGIS_MAP_APP_URI}},
    ),
    types.Tool(
        name="summarize_layer_stats",
        description="Compute basic count/min/max/avg for a numeric field, optionally grouped by a field. Uses server-side out_statistics when possible.",
        inputSchema={
            "type": "object",
            "properties": {
                "item_id": {"type": "string", "description": "Portal item ID of the feature layer/service."},
                "layer_index": {"type": "integer", "description": "Layer index (default 0).", "default": 0},
                "where": {"type": "string", "description": "SQL WHERE clause (default 1=1).", "default": "1=1"},
                "numeric_field": {"type": "string", "description": "Numeric field to summarize. If omitted, returns count only."},
                "group_by_field": {"type": "string", "description": "Optional field to group by (categorical)."},
                "max_groups": {"type": "integer", "description": "Max groups to return (default 25).", "default": 25},
            },
            "required": ["item_id"],
        },
    ),
    types.Tool(
        name="get_map_viewer_url",
        description="Return the ArcGIS Online/Enterprise map viewer URL for a portal web map or web scene. The item_id must be a portal item ID (e.g. from search_content or get_item), not a tool result id (lc_...). For a link to the map from show_map (the in-chat/directions map), use the map_url field from the show_map response when present; do not call this tool with a result id.",
        inputSchema={
            "type": "object",
            "properties": {
                "item_id": {"type": "string", "description": "Portal item ID of a web map or web scene (from ArcGIS Online/Enterprise). Not a tool result id (e.g. lc_...)."},
            },
            "required": ["item_id"],
        },
    ),
    types.Tool(
        name="export_layer_geojson",
        description="Query a feature layer by portal item ID and return the result as a GeoJSON FeatureCollection. Same inputs as query_layer (item_id, layer_index, where, max_records).",
        inputSchema={
            "type": "object",
            "properties": {
                "item_id": {"type": "string", "description": "Portal item ID of the feature service or feature layer."},
                "layer_index": {"type": "integer", "description": "Layer or table index (0-based). Default 0.", "default": 0},
                "where": {"type": "string", "description": "SQL WHERE clause (default 1=1).", "default": "1=1"},
                "max_records": {"type": "integer", "description": "Maximum features to return (default 100).", "default": 100},
            },
            "required": ["item_id"],
        },
    ),
    types.Tool(
        name="batch_geocode",
        description="Geocode a list of addresses in one call. Returns a list of locations (address, location, score) for each input.",
        inputSchema={
            "type": "object",
            "properties": {
                "addresses": {
                    "type": "array",
                    "description": "List of address or place strings to geocode.",
                    "items": {"type": "string"},
                },
            },
            "required": ["addresses"],
        },
    ),
    types.Tool(
        name="share_item",
        description="Change sharing for a portal item. You can share with everyone, the organization, and/or specific groups. This modifies item sharing; confirm with the user before calling.",
        inputSchema={
            "type": "object",
            "properties": {
                "item_id": {"type": "string", "description": "Portal item ID to share."},
                "group_ids": {
                    "type": "array",
                    "description": "Optional list of group IDs to share the item with.",
                    "items": {"type": "string"},
                },
                "allow_org": {"type": "boolean", "description": "Share with the organization (default false).", "default": False},
                "allow_everyone": {"type": "boolean", "description": "Share with everyone (public) (default false).", "default": False},
            },
            "required": ["item_id"],
        },
    ),
    types.Tool(
        name="enrich",
        description="Get demographics and key facts for a location (address or longitude,latitude). Prefer run_intent for natural-language demographics questions. Uses ArcGIS GeoEnrichment; requires ArcGIS Online or Enterprise with Business Analyst/geoenrichment.",
        inputSchema={
            "type": "object",
            "properties": {
                "location": {"type": "string", "description": "Address to geocode or 'longitude,latitude' (e.g. '-122.4,37.8')."},
                "buffer_km": {"type": "number", "description": "Buffer distance in kilometers around the point (default 1).", "default": 1},
                "data_collection": {"type": "string", "description": "Optional data collection (e.g. KeyGlobalFacts). Default KeyGlobalFacts."},
            },
            "required": ["location"],
        },
    ),
    types.Tool(
        name="find_nearest",
        description="Find features nearest to a location from a portal feature layer. Returns features within max_distance (meters), sorted by straight-line distance.",
        inputSchema={
            "type": "object",
            "properties": {
                "location": {"type": "string", "description": "Address or 'longitude,latitude'."},
                "item_id": {"type": "string", "description": "Portal item ID of the feature layer (or feature service)."},
                "layer_index": {"type": "integer", "description": "0-based layer index (default 0).", "default": 0},
                "max_count": {"type": "integer", "description": "Maximum number of features to return (default 10).", "default": 10},
                "max_distance_m": {"type": "number", "description": "Maximum straight-line distance in meters (default 50000).", "default": 50000},
            },
            "required": ["location", "item_id"],
        },
    ),
    types.Tool(
        name="summarize_nearby",
        description="Count (or summarize) features from a portal feature layer within a distance of a location.",
        inputSchema={
            "type": "object",
            "properties": {
                "location": {"type": "string", "description": "Address or 'longitude,latitude'."},
                "item_id": {"type": "string", "description": "Portal item ID of the feature layer."},
                "layer_index": {"type": "integer", "description": "0-based layer index (default 0).", "default": 0},
                "distance_m": {"type": "number", "description": "Distance in meters (default 5000).", "default": 5000},
                "where": {"type": "string", "description": "Optional SQL where clause (default 1=1).", "default": "1=1"},
            },
            "required": ["location", "item_id"],
        },
    ),
    types.Tool(
        name="describe_layer",
        description="Describe a layer (fields, geometry type, capabilities, max record count). Provide layer_item_id (portal item) or layer_url (service URL).",
        inputSchema={
            "type": "object",
            "properties": {
                "layer_item_id": {"type": "string", "description": "Optional portal item id for the layer/service."},
                "layer_url": {"type": "string", "description": "Optional ArcGIS service URL (FeatureServer/MapServer/etc)."},
                "layer_index": {"type": "integer", "description": "0-based sublayer index when using a service root URL (default 0).", "default": 0},
            },
            "required": [],
        },
    ),
    types.Tool(
        name="sample_features",
        description="Sample/query features from a layer with pagination controls (limit/offset).",
        inputSchema={
            "type": "object",
            "properties": {
                "layer_item_id": {"type": "string", "description": "Optional portal item id."},
                "layer_url": {"type": "string", "description": "Optional service URL."},
                "layer_index": {"type": "integer", "description": "0-based sublayer index (default 0).", "default": 0},
                "where": {"type": "string", "description": "SQL WHERE clause (default 1=1).", "default": "1=1"},
                "out_fields": {"type": "string", "description": "Comma-separated outFields (default *).", "default": "*"},
                "limit": {"type": "integer", "description": "Max features to return (default 10).", "default": 10},
                "offset": {"type": "integer", "description": "Result offset for pagination (default 0).", "default": 0},
                "order_by": {"type": "string", "description": "Optional orderByFields string (e.g. \"FIELD ASC\")."},
                "return_geometry": {"type": "boolean", "description": "Include geometry (default true).", "default": True},
            },
            "required": [],
        },
    ),
    types.Tool(
        name="distinct_values",
        description="Return distinct values for a field from a layer (best-effort counts).",
        inputSchema={
            "type": "object",
            "properties": {
                "layer_item_id": {"type": "string", "description": "Optional portal item id."},
                "layer_url": {"type": "string", "description": "Optional service URL."},
                "layer_index": {"type": "integer", "description": "0-based sublayer index (default 0).", "default": 0},
                "field": {"type": "string", "description": "Field name to return distinct values for."},
                "where": {"type": "string", "description": "Optional SQL WHERE clause (default 1=1).", "default": "1=1"},
                "max_values": {"type": "integer", "description": "Max values to return (default 25).", "default": 25},
            },
            "required": ["field"],
        },
    ),
    types.Tool(
        name="field_stats",
        description="Compute basic statistics for a numeric field (count/min/max/avg/stddev). Optionally group-by and/or sampled histogram.",
        inputSchema={
            "type": "object",
            "properties": {
                "layer_item_id": {"type": "string", "description": "Optional portal item id."},
                "layer_url": {"type": "string", "description": "Optional service URL."},
                "layer_index": {"type": "integer", "description": "0-based sublayer index (default 0).", "default": 0},
                "numeric_field": {"type": "string", "description": "Numeric field name."},
                "where": {"type": "string", "description": "Optional SQL WHERE clause (default 1=1).", "default": "1=1"},
                "group_by_field": {"type": "string", "description": "Optional group-by field (categorical)."},
                "max_groups": {"type": "integer", "description": "Max groups when grouped (default 25).", "default": 25},
                "histogram_bins": {"type": "integer", "description": "Optional histogram bin count (0 disables; default 0).", "default": 0},
                "histogram_sample_size": {"type": "integer", "description": "Optional histogram sample size (default 500).", "default": 500},
            },
            "required": ["numeric_field"],
        },
    ),
    types.Tool(
        name="suggest_symbology",
        description="Suggest a renderer (and optional clustering) for a layer based on fields/stats. Read-only.",
        inputSchema={
            "type": "object",
            "properties": {
                "layer_item_id": {"type": "string", "description": "Optional portal item id."},
                "layer_url": {"type": "string", "description": "Optional service URL."},
                "layer_index": {"type": "integer", "description": "0-based sublayer index (default 0).", "default": 0},
                "preferred_field": {"type": "string", "description": "Optional preferred field name for styling."},
                "goal": {
                    "type": "string",
                    "description": "Styling goal (auto, numeric, category, cluster).",
                    "enum": ["auto", "numeric", "category", "cluster"],
                },
                "where": {"type": "string", "description": "Optional SQL where clause used for stats (default 1=1).", "default": "1=1"},
                "max_categories": {"type": "integer", "description": "Max categories for unique-value renderer (default 10).", "default": 10},
            },
            "required": [],
        },
    ),
    types.Tool(
        name="smart_show_layer",
        description="Show a layer with automatically suggested symbology (renderer + optional clustering). Returns MapState (same shape as show_layer_with_renderer).",
        inputSchema={
            "type": "object",
            "properties": {
                "center": {"type": "string", "description": "Optional center: address or 'longitude,latitude'."},
                "zoom": {"type": "integer", "description": "Optional zoom."},
                "locations": {"type": "array", "description": "Optional marker locations: addresses or [lon,lat].", "items": {}},
                "layer_item_id": {"type": "string", "description": "Optional portal item id."},
                "layer_url": {"type": "string", "description": "Optional service URL."},
                "layer_index": {"type": "integer", "description": "0-based sublayer index (default 0).", "default": 0},
                "where": {"type": "string", "description": "Optional layer filter (definitionExpression). Default 1=1.", "default": "1=1"},
                "preferred_field": {"type": "string", "description": "Optional preferred field name for styling."},
                "goal": {"type": "string", "description": "Styling goal (auto, numeric, category, cluster).", "enum": ["auto", "numeric", "category", "cluster"]},
                "max_categories": {"type": "integer", "description": "Max categories for unique-value renderer (default 10).", "default": 10},
                "basemap_id": {"type": "string", "description": "Optional basemap id."},
                "view_mode": {"type": "string", "description": "Optional view mode: '2d' or '3d'.", "enum": ["2d", "3d"]},
                "terrain": {"type": "boolean", "description": "Optional terrain hillshade/elevation.", "default": False},
                "overlays": {"type": "array", "description": "Optional overlays.", "items": {"type": "object"}},
                "ground": {"type": "object", "description": "Optional ground config for 3D."},
                "opacity": {"type": "number", "description": "Optional layer opacity (0..1)."},
            },
            "required": [],
        },
        meta={"ui": {"resourceUri": ARCGIS_MAP_APP_URI}},
    ),
    types.Tool(
        name="auto_map",
        description="Map autopilot: infer map style from intent + constraints and return display-ready MapState. Prefer run_intent unless you need low-level styling control.",
        inputSchema={
            "type": "object",
            "properties": {
                "layer_item_id": {"type": "string", "description": "Optional portal item id."},
                "layer_url": {"type": "string", "description": "Optional service URL."},
                "layer_index": {"type": "integer", "description": "0-based sublayer index (default 0).", "default": 0},
                "user_intent": {"type": "string", "description": "Intent, e.g. 'show value differences', 'cluster dense points', 'category map'."},
                "audience": {"type": "string", "description": "Audience hint, e.g. executive, operations, analyst."},
                "constraints": {"type": "object", "description": "Optional constraints, e.g. {preferred_field,max_categories}."},
                "where": {"type": "string", "description": "Optional SQL filter (default 1=1).", "default": "1=1"},
                "center": {"type": "string", "description": "Optional map center."},
                "zoom": {"type": "integer", "description": "Optional zoom."},
                "view_mode": {"type": "string", "description": "Optional view mode: '2d' or '3d'.", "enum": ["2d", "3d"]},
                "basemap_id": {"type": "string", "description": "Optional basemap id."},
                "terrain": {"type": "boolean", "description": "Optional terrain toggle.", "default": False},
            },
            "required": [],
        },
        meta={"ui": {"resourceUri": ARCGIS_MAP_APP_URI}},
    ),
    types.Tool(
        name="run_intent",
        description="Default entry point for most user requests. Accepts one natural-language intent string, chooses the safest internal tool chain (e.g., route_and_show_map, buffer_and_show, nearest_and_show, enrich, auto_map), and returns UI-ready output (map state when applicable) plus structured metadata.",
        inputSchema={
            "type": "object",
            "properties": {
                "intent": {"type": "string", "description": "User's plain-language request (e.g. 'directions from A to B', 'demographics for Tempe, AZ', 'best map for layer <item id>')."},
            },
            "required": ["intent"],
        },
        meta={"ui": {"resourceUri": ARCGIS_MAP_APP_URI}},
    ),
    types.Tool(
        name="open_example_app",
        description="Open a packaged ArcGIS example MCP App by name.",
        inputSchema={
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "Example name.",
                    "enum": ["analysis-area-measurement", "ai-assistant"],
                },
                "initial_state": {"type": "object", "description": "Optional initial state payload for future example options."},
            },
            "required": ["name"],
        },
        meta={"ui": {"resourceUri": ARCGIS_EXAMPLES_APP_URI}},
    ),
    types.Tool(
        name="export_map_app",
        description="Export a standalone single-file HTML map from map_state or map_state_id.",
        inputSchema={
            "type": "object",
            "properties": {
                "map_state": {"type": "object", "description": "MapState object to export."},
                "map_state_id": {"type": "string", "description": "Optional map_state_id created by show_map/map share link."},
                "title": {"type": "string", "description": "Optional output title/filename stem."},
            },
            "required": [],
        },
    ),
]


# --- Blocking ArcGIS helpers (run in thread) ---

def _search_content(query: str, item_type: str | None = None, max_items: int = 10) -> str:
    q = (query or "").strip()
    if not q:
        return json.dumps({
            "error": "Search requires a non-empty 'query' (portal requires at least one of: q, bbox, filter, categories).",
            "hint": "Pass a search term, e.g. query='parcels' or query='owner:username'.",
        })
    gis = _get_gis()
    sess = _current_session_id_var.get() or "anon"
    if not _rate_limit(f"search_content:{sess}", limit=60, per_seconds=60):
        return json.dumps({"error": "Rate limited.", "hint": "Too many content searches; retry in a minute."}, indent=2)
    result = _with_retries(lambda: gis.content.search(q, item_type=item_type or None, max_items=max_items), tries=3)
    items = [{"id": i.id, "title": i.title, "type": i.type} for i in result]
    return json.dumps({"items": items}, indent=2)


def _geocode(address: str, max_locations: int = 5) -> str:
    import arcgis.geocoding

    gis = _get_gis()
    addr = (address or "").strip()
    if not addr:
        return json.dumps({"error": "Missing address.", "hint": "Pass a non-empty address/place string."}, indent=2)
    sess = _current_session_id_var.get() or "anon"
    if not _rate_limit(f"geocode:{sess}", limit=60, per_seconds=60):
        return json.dumps({"error": "Rate limited.", "hint": "Too many geocode requests; retry in a minute."}, indent=2)
    portal = getattr(getattr(gis, "properties", None), "portalHostname", None) or "portal"
    cache_key = f"{portal}|{max_locations}|{addr.lower()}"
    cached_val = _store.geocode_cache_get(key=cache_key)
    if cached_val is not None:
        result = cached_val
    else:
        try:
            result = _with_retries(lambda: arcgis.geocoding.geocode(addr, max_locations=max_locations), tries=3)
        except Exception as e:  # noqa: BLE001 - translate SDK errors into tool-friendly JSON
            msg = str(e).split("\n")[0] if str(e) else "Geocode failed"
            return json.dumps(
                {"error": msg, "hint": "Try a more specific address or use [longitude, latitude]."},
                indent=2,
            )
        _store.geocode_cache_set(key=cache_key, value=result, ttl_seconds=_CACHE_TTL_SECONDS)
    out = []
    for r in (result or []):
        if not isinstance(r, dict):
            continue
        loc = r.get("location") if isinstance(r.get("location"), dict) else None
        lon = None
        lat = None
        if loc:
            lon = loc.get("x", loc.get("longitude"))
            lat = loc.get("y", loc.get("latitude"))
        try:
            lon_f = float(lon) if lon is not None else None
            lat_f = float(lat) if lat is not None else None
        except (TypeError, ValueError):
            lon_f, lat_f = None, None
        entry = {
            "address": r.get("address"),
            "location": loc,
            "longitude": lon_f,
            "latitude": lat_f,
            "lonlat": [lon_f, lat_f] if (lon_f is not None and lat_f is not None) else None,
            "score": r.get("score"),
        }
        out.append(entry)
    return json.dumps({"locations": out}, indent=2)


def _get_item(item_id: str) -> str:
    gis = _get_gis()
    item = gis.content.get(item_id)
    if item is None:
        return json.dumps({"error": f"Item not found: {item_id}"})
    return json.dumps({
        "id": item.id,
        "title": item.title,
        "type": item.type,
        "owner": getattr(item, "owner", None),
    }, indent=2)


def _resolve_layer(item_id: str | None = None, url: str | None = None, layer_index: int = 0) -> str:
    """Resolve a portal item id or service URL to a map layer descriptor."""
    gis = _get_gis()
    iid = (item_id or "").strip() or None
    svc_url = (url or "").strip() or None
    title = None
    item_type = None
    if iid:
        item = gis.content.get(iid)
        if item is None:
            return json.dumps({"error": f"Item not found: {iid}"}, indent=2)
        title = getattr(item, "title", None)
        item_type = getattr(item, "type", None)
        svc_url = (getattr(item, "url", None) or "").strip() or None
        if not svc_url:
            return json.dumps({"error": "Item has no service URL.", "hint": "Resolve requires a layer/service item with a URL."}, indent=2)
    if not svc_url:
        return json.dumps({"error": "Provide item_id or url.", "hint": "Pass a portal item id or a FeatureServer/MapServer/ImageServer/VectorTileServer URL."}, indent=2)
    try:
        idx = int(layer_index)
    except (TypeError, ValueError):
        idx = 0
    # If URL points at a service root, append index
    import re
    normalized = svc_url.rstrip("/")
    if re.search(r"/(FeatureServer|MapServer|ImageServer|VectorTileServer)$", normalized, flags=re.IGNORECASE):
        normalized = normalized + f"/{idx}"
    out = {
        "source": "portal" if iid else "url",
        "item_id": iid,
        "title": title,
        "item_type": item_type,
        "service_url": svc_url,
        "layer": {"url": normalized, "layer_index": idx},
    }
    return json.dumps(out, indent=2)


def _whoami() -> str:
    gis = _get_gis()
    me = gis.users.me
    out = {
        "portal": gis.properties.portalName,
        "username": me.username if me else None,
        "fullName": getattr(me, "fullName", None) if me else None,
    }
    if not me:
        out["_note"] = "Anonymous. Use get_arcgis_login_url to get a sign-in link. After signing in, tell the agent the one-time code (e.g. 'use ArcGIS code a1b2c3d4') so it can call set_arcgis_token_with_code."
    return json.dumps(out, indent=2)


def _reverse_geocode(longitude: float, latitude: float, distance: float | None = None) -> str:
    import arcgis.geocoding

    gis = _get_gis()
    geocoders = arcgis.geocoding.get_geocoders(gis)
    if not geocoders:
        return json.dumps({"error": "No geocoder available for this portal.", "hint": "Reverse geocoding requires a geocode service."})
    location = [longitude, latitude]
    try:
        result = arcgis.geocoding.reverse_geocode(location, distance=distance, geocoder=geocoders[0])
    except Exception as e:
        return json.dumps({"error": str(e), "hint": "Check coordinates (longitude, latitude) and that the geocoder supports reverse geocode."})
    return json.dumps({"address": result.get("address"), "location": result.get("location")}, indent=2)


def _suggest(text: str, latitude: float | None = None, longitude: float | None = None, max_suggestions: int = 5) -> str:
    import arcgis.geocoding

    gis = _get_gis()
    geocoders = arcgis.geocoding.get_geocoders(gis)
    if not geocoders:
        return json.dumps({"error": "No geocoder available for this portal.", "hint": "Suggest requires a geocode service with suggest capability."})
    location = None
    if latitude is not None and longitude is not None:
        location = {"x": longitude, "y": latitude}
    try:
        result = arcgis.geocoding.suggest(
            text.strip(),
            location=location,
            max_suggestions=max_suggestions,
            geocoder=geocoders[0],
        )
    except Exception as e:
        return json.dumps({"error": str(e), "hint": "Ensure text is non-empty and the geocoder supports suggest."})
    suggestions = result.get("suggestions", []) if isinstance(result, dict) else []
    return json.dumps({"suggestions": suggestions}, indent=2)


def _get_item_details(item_id: str) -> str:
    gis = _get_gis()
    item = gis.content.get(item_id)
    if item is None:
        return json.dumps({"error": f"Item not found: {item_id}"})
    out = {
        "id": item.id,
        "title": item.title,
        "type": item.type,
        "owner": getattr(item, "owner", None),
        "description": getattr(item, "description", None) or "",
        "snippet": getattr(item, "snippet", None) or "",
        "size": getattr(item, "size", None),
        "created": getattr(item, "created", None),
        "modified": getattr(item, "modified", None),
        "tags": getattr(item, "tags", []) or [],
        "typeKeywords": getattr(item, "typeKeywords", []) or [],
        "url": getattr(item, "url", None),
    }
    if getattr(item, "thumbnail", None):
        out["thumbnail"] = item.thumbnail
    return json.dumps(out, indent=2)


def _list_my_content(folder: str | None = None, item_type: str | None = None, max_items: int = 20) -> str:
    gis = _get_gis()
    me = gis.users.me
    if not me:
        return json.dumps({
            "error": "Not signed in.",
            "hint": "Use get_arcgis_login_url and set_arcgis_token_with_code to sign in, then list_my_content.",
        })
    try:
        gen = me.items(folder=folder or None, max_items=max_items)
        items = []
        for i, item in enumerate(gen):
            if i >= max_items:
                break
            if item_type and item.type != item_type:
                continue
            items.append({"id": item.id, "title": item.title, "type": item.type})
        return json.dumps({"items": items}, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e), "hint": "Check folder name and that you are signed in."})


def _query_layer(
    item_id: str,
    layer_index: int = 0,
    where: str = "1=1",
    out_fields: str = "*",
    return_geometry: bool = True,
    max_records: int = 100,
) -> str:
    gis = _get_gis()
    item = gis.content.get(item_id)
    if item is None:
        return json.dumps({"error": f"Item not found: {item_id}"})
    layers = getattr(item, "layers", []) or []
    tables = getattr(item, "tables", []) or []
    if not layers and not tables:
        return json.dumps({
            "error": "Item has no feature layers or tables.",
            "hint": "Use a Feature Service or similar item that has layers.",
        })
    if layers:
        layer = layers[layer_index] if 0 <= layer_index < len(layers) else layers[0]
    else:
        layer = tables[layer_index] if 0 <= layer_index < len(tables) else tables[0]
    try:
        result = layer.query(
            where=where or "1=1",
            out_fields=out_fields or "*",
            return_geometry=return_geometry,
            result_record_count=max_records,
        )
    except Exception as e:
        return json.dumps({"error": str(e), "hint": "Check where clause and out_fields; layer may not support the query."})
    features = [f.as_dict for f in result.features]
    out = {"features": features, "count": len(features)}
    if result.fields:
        out["fields"] = result.fields
    return json.dumps(out, indent=2)


def _geometry_buffer(
    geometry: dict | None,
    x: float | None,
    y: float | None,
    in_sr: int,
    distance: float,
    unit: str,
    out_sr: int | None,
) -> str:
    import arcgis.geometry.functions as geom_funcs

    gis = _get_gis()
    if geometry is None or geometry == {}:
        if x is None or y is None:
            return json.dumps({"error": "Provide either geometry or x and y.", "hint": "For a point use x, y and in_sr (e.g. 4326)."})
        geometry = {"x": x, "y": y, "spatialReference": {"wkid": in_sr}}
    if isinstance(geometry, dict) and "spatialReference" not in geometry:
        geometry = dict(geometry)
        geometry["spatialReference"] = geometry.get("spatialReference") or {"wkid": in_sr}
    try:
        result = geom_funcs.buffer(
            [geometry],
            in_sr,
            distance,
            unit,
            out_sr=out_sr,
            gis=gis,
        )
    except Exception as e:
        return json.dumps({"error": str(e), "hint": "Check geometry format, in_sr, distance, and unit."})
    if not result:
        return json.dumps({"error": "Buffer returned no geometry."})
    out_geom = result[0]
    if hasattr(out_geom, "as_dict"):
        out_geom = out_geom.as_dict
    elif hasattr(out_geom, "__dict__"):
        out_geom = getattr(out_geom, "__dict__", out_geom)
    return json.dumps({"geometry": out_geom}, indent=2)


def _geometry_project(geometry: dict, in_sr: int, out_sr: int) -> str:
    import arcgis.geometry.functions as geom_funcs

    gis = _get_gis()
    try:
        result = geom_funcs.project([geometry], in_sr, out_sr, gis=gis)
    except Exception as e:
        return json.dumps({"error": str(e), "hint": "Check geometry and in_sr/out_sr (e.g. 4326, 3857)."})
    if not result:
        return json.dumps({"error": "Project returned no geometry."})
    out_geom = result[0]
    if hasattr(out_geom, "as_dict"):
        out_geom = out_geom.as_dict
    elif isinstance(out_geom, dict):
        pass
    else:
        out_geom = getattr(out_geom, "__dict__", str(out_geom))
    return json.dumps({"geometry": out_geom}, indent=2)


def _max_route_stops() -> int:
    try:
        return max(2, min(100, int(os.environ.get("ARCGIS_MAX_ROUTE_STOPS", "25").strip())))
    except (ValueError, TypeError):
        return 25

def _route(stops: list, item_id: str | None) -> str:
    from arcgis.network import RouteLayer
    from arcgis._impl.common._utils import _validate_url
    import arcgis.geocoding

    gis = _get_gis()
    sess = _current_session_id_var.get() or "anon"
    if not _rate_limit(f"route:{sess}", limit=20, per_seconds=60):
        return json.dumps({"error": "Rate limited.", "hint": "Too many route requests; retry in a minute."}, indent=2)
    if not stops or len(stops) < 2:
        return json.dumps({"error": "At least two stops are required.", "hint": "Pass stops as [[lon, lat], [lon, lat]] or [address1, address2]."})
    if len(stops) > _max_route_stops():
        return json.dumps({"error": f"Too many stops (max {_max_route_stops()}).", "hint": "Use fewer stops or contact admin for async routing."})
    point_features = []
    for i, stop in enumerate(stops):
        if isinstance(stop, (list, tuple)) and len(stop) >= 2:
            try:
                lon, lat = float(stop[0]), float(stop[1])
            except (TypeError, ValueError):
                return json.dumps({"error": f"Stop {i + 1}: invalid coordinates. Use [longitude, latitude]."})
            point_features.append({"geometry": {"x": lon, "y": lat}})
        elif isinstance(stop, str) and stop.strip():
            geocoders = arcgis.geocoding.get_geocoders(gis)
            if not geocoders:
                return json.dumps({"error": "Geocoding not available; use [lon, lat] for stops.", "hint": "Provide coordinates instead of addresses."})
            res = arcgis.geocoding.geocode(stop.strip(), max_locations=1, geocoder=geocoders[0])
            if not res or not res[0].get("location"):
                return json.dumps({"error": f"Could not geocode stop {i + 1}: {stop[:50]}."})
            loc = res[0]["location"]
            point_features.append({"geometry": {"x": loc.get("x", loc.get("longitude")), "y": loc.get("y", loc.get("latitude"))}})
        else:
            return json.dumps({"error": f"Stop {i + 1}: use [longitude, latitude] or an address string."})
    try:
        if item_id:
            item = gis.content.get(item_id)
            if item is None:
                return json.dumps({"error": f"Item not found: {item_id}"})
            url = getattr(item, "url", None) or ""
            if not url:
                return json.dumps({"error": "Item has no URL; use a route service item."})
            url = _validate_url(url, gis)
        else:
            hs = getattr(gis.properties, "helperServices", None) or {}
            route_svc = hs.get("route") if isinstance(hs, dict) else getattr(hs, "route", None)
            if not route_svc:
                return json.dumps({
                    "error": "No route service available.",
                    "hint": "Portal has no default route service; pass item_id of a route service.",
                })
            url = _validate_url(route_svc.get("url", "") if isinstance(route_svc, dict) else getattr(route_svc, "url", ""), gis)
        route_layer = RouteLayer(url, gis=gis)
        directions_lang = (os.environ.get("ARCGIS_DIRECTIONS_LANGUAGE") or "").strip() or None
        result = _with_retries(
            lambda: route_layer.solve(
                stops=point_features,
                return_directions=True,
                return_routes=True,
                return_stops=False,
                output_lines="esriNAOutputLineTrueShape",
                start_time="now",
                directions_language=directions_lang,
            ),
            tries=3,
        )
    except Exception as e:
        msg = str(e).split("\n")[0] if str(e) else "Route solve failed"
        return json.dumps({"error": msg, "hint": "Check stops (coordinates or addresses) and that a route service is available."})
    if not result:
        return json.dumps({"error": "Route solve returned no result."})
    if isinstance(result, dict):
        if result.get("error"):
            err = result.get("error") or {}
            msg = err.get("message", err.get("details", "Route solve failed")) if isinstance(err, dict) else str(err)
            if isinstance(msg, list):
                msg = msg[0] if msg else "Route solve failed"
            return json.dumps({"error": msg, "hint": "Check stops and route service; retry with coordinates if geocoding fails."}, indent=2)
        out = {k: v for k, v in result.items() if k != "error"}
        # Extract route polyline from REST response so it can be drawn on the map (try "routes" and "route", handle dict/object)
        if out.get("route_geometry") is None:
            out["route_geometry"] = _extract_first_route_geometry(result)
    else:
        out = {}
        geom = getattr(result, "route_geometry", None)
        if geom is not None:
            out["route_geometry"] = _geometry_to_dict(geom) or (geom if isinstance(geom, dict) else None)
        if out.get("route_geometry") is None:
            # SDK may return object with .routes or .output_routes
            try:
                as_dict = getattr(result, "__dict__", None) or (vars(result) if hasattr(result, "__dict__") else {})
                if not as_dict and hasattr(result, "as_dict"):
                    as_dict = result.as_dict() if callable(result.as_dict) else result.as_dict
                if isinstance(as_dict, dict):
                    out["route_geometry"] = _extract_first_route_geometry(as_dict)
            except Exception:
                pass
        if getattr(result, "directions", None) is not None:
            out["directions"] = result.directions
        if getattr(result, "total_time", None) is not None:
            out["total_time"] = result.total_time
        if getattr(result, "total_length", None) is not None:
            out["total_length"] = result.total_length
    if isinstance(out, dict) and isinstance(out.get("route_geometry"), dict):
        out["route_geometry"] = _normalize_route_geometry(out["route_geometry"]) or out["route_geometry"]
    return json.dumps(out, indent=2, default=_json_serial_default)


def _route_and_show_map(
    stops: list,
    item_id: str | None = None,
    zoom: int | None = None,
    layer_item_id: str | None = None,
    layer_index: int | None = None,
    basemap_id: str | None = None,
    overlays: list | None = None,
    view_mode: str | None = None,
    ground: dict | None = None,
    terrain: bool | None = None,
    layer_url: str | None = None,
) -> str:
    """Solve route then return show_map state with route geometry so the route line is drawn. Includes directions/total_time/total_length in the response."""
    route_json = _route(stops, item_id)
    try:
        route_data = json.loads(route_json)
    except json.JSONDecodeError:
        return route_json
    if route_data.get("error"):
        return route_json
    route_geometry = route_data.get("route_geometry")
    route_summary = None
    if route_data.get("total_time") is not None or route_data.get("total_length") is not None:
        route_summary = {}
        if route_data.get("total_time") is not None:
            route_summary["total_time_min"] = route_data["total_time"]
        if route_data.get("total_length") is not None:
            route_summary["total_length_mi"] = route_data["total_length"]
    map_json = _show_map(
        center=None,
        zoom=zoom,
        locations=stops,
        route_geojson=route_geometry,
        layer_item_id=layer_item_id,
        layer_index=layer_index,
        route_summary=route_summary,
        basemap_id=basemap_id,
        overlays=overlays,
        view_mode=view_mode,
        ground=ground,
        terrain=terrain,
        layer_url=layer_url,
    )
    try:
        map_data = json.loads(map_json)
    except json.JSONDecodeError:
        return map_json
    if route_data.get("directions") is not None:
        map_data["directions"] = route_data["directions"]
    if route_data.get("total_time") is not None:
        map_data["total_time"] = route_data["total_time"]
    if route_data.get("total_length") is not None:
        map_data["total_length"] = route_data["total_length"]
    return json.dumps(map_data, indent=2)


def _parse_center(center: str | None):
    """Return (lon, lat) from center string: 'lon,lat' or geocode address. Returns None if center is empty."""
    if not center or not str(center).strip():
        return None
    s = str(center).strip()
    if "," in s and len(s) < 100:
        parts = s.split(",", 1)
        try:
            return (float(parts[0].strip()), float(parts[1].strip()))
        except (ValueError, IndexError):
            pass
    import arcgis.geocoding
    gis = _get_gis()
    geocoders = arcgis.geocoding.get_geocoders(gis)
    if not geocoders:
        return None
    res = arcgis.geocoding.geocode(s, max_locations=1, geocoder=geocoders[0])
    if not res or not res[0].get("location"):
        return None
    loc = res[0]["location"]
    return (loc.get("x", loc.get("longitude")), loc.get("y", loc.get("latitude")))


def _resolve_location(loc) -> tuple[tuple[float, float] | None, str | None]:
    """Resolve a single location: [lon, lat] (preferred) or address string.

    Returns (point, warning). point is (lon, lat) in EPSG:4326 or None if unresolvable/invalid.
    """
    if isinstance(loc, (list, tuple)) and len(loc) >= 2:
        try:
            lon = float(loc[0])
            lat = float(loc[1])
        except (TypeError, ValueError):
            return None, "Invalid numeric coordinates; expected [longitude, latitude]."
        return _normalize_lon_lat(lon, lat)
    if isinstance(loc, str) and loc.strip():
        pt = _parse_center(loc)
        if not pt:
            return None, "Could not geocode/parse location string."
        return _normalize_lon_lat(pt[0], pt[1])
    return None, "Unsupported location type; use [longitude, latitude] or an address string."


def _is_finite_number(x: float) -> bool:
    import math
    return x is not None and isinstance(x, (int, float)) and math.isfinite(float(x))


def _is_valid_lon_lat(lon: float, lat: float) -> bool:
    if not (_is_finite_number(lon) and _is_finite_number(lat)):
        return False
    lon_f = float(lon)
    lat_f = float(lat)
    return (-180.0 <= lon_f <= 180.0) and (-90.0 <= lat_f <= 90.0)


def _normalize_lon_lat(lon: float, lat: float) -> tuple[tuple[float, float] | None, str | None]:
    """Normalize (lon, lat) with an 'obvious swap' heuristic.

    - If (lon, lat) is valid: return it.
    - Else if swapping yields a valid (lon, lat): return swapped + warning.
    - Else return None + warning.
    """
    if _is_valid_lon_lat(lon, lat):
        return (float(lon), float(lat)), None

    # Obvious/strong swap heuristic: if the swapped pair becomes valid, accept it.
    if _is_valid_lon_lat(lat, lon):
        return (float(lat), float(lon)), "Detected swapped coordinate order; auto-swapped to [longitude, latitude]."

    return None, "Invalid coordinate range; expected longitude in [-180, 180] and latitude in [-90, 90]."


def _dedupe_points(points: list[tuple[float, float]], tolerance_km: float = 0.05) -> list[tuple[float, float]]:
    """Merge consecutive points that are within tolerance (default ~50 m) so duplicate/near-duplicate stops become one marker. Order preserved; first of each run kept."""
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


def _add_map_urls(state: dict) -> dict:
    """Add shareable map URLs when ARCGIS_MAP_BASE_URL/ARCGIS_AUTH_BASE_URL is configured."""
    base_url = (os.environ.get("ARCGIS_MAP_BASE_URL") or os.environ.get("ARCGIS_AUTH_BASE_URL") or "").strip().rstrip("/")
    if not base_url:
        return state
    # Primary: short, shareable link (avoids URL truncation issues)
    try:
        state_id = _store_map_state(state)
        state["map_state_id"] = state_id
        state["map_url"] = base_url + "/map/s/" + urllib.parse.quote(state_id, safe="")
    except Exception as e:
        msg = (state.get("message") or "").strip()
        extra = f"Share link unavailable: {e}"
        state["message"] = (msg + " " + extra).strip() if msg else extra
        return state
    # Secondary: legacy query URL (useful for debugging/backwards compatibility) when center is available
    try:
        c = state.get("center") or {}
        if isinstance(c, dict) and c.get("longitude") is not None and c.get("latitude") is not None:
            center_pt = (float(c["longitude"]), float(c["latitude"]))
            parts = [
                "center=" + urllib.parse.quote(f"{center_pt[0]},{center_pt[1]}", safe=""),
                "zoom=" + str(int(state.get("zoom") or 10)),
            ]
            markers = state.get("markers")
            if isinstance(markers, list) and markers:
                enc = base64.urlsafe_b64encode(json.dumps(markers).encode("utf-8")).decode("ascii")
                parts.append("markers=" + urllib.parse.quote(enc, safe=""))
            if state.get("routeGeometry"):
                enc = base64.urlsafe_b64encode(json.dumps(state["routeGeometry"]).encode("utf-8")).decode("ascii")
                parts.append("route=" + urllib.parse.quote(enc, safe=""))
            if state.get("routeSummary"):
                sm = state["routeSummary"]
                if isinstance(sm, dict):
                    if sm.get("total_time_min") is not None:
                        parts.append("summary_min=" + str(int(sm["total_time_min"])))
                    if sm.get("total_length_mi") is not None:
                        parts.append("summary_mi=" + urllib.parse.quote(str(round(sm["total_length_mi"], 1)), safe=""))
            if state.get("basemapId"):
                parts.append("basemap=" + urllib.parse.quote(str(state["basemapId"]), safe=""))
            if state.get("viewMode"):
                parts.append("view=" + urllib.parse.quote(str(state["viewMode"]), safe=""))
            # Only include portal layer item_id in query fallback (URLs can be long)
            if state.get("layer") and isinstance(state["layer"], dict) and state["layer"].get("item_id"):
                parts.append("layer=" + urllib.parse.quote(state["layer"]["item_id"], safe=""))
                if state["layer"].get("layer_index"):
                    parts.append("layer_index=" + str(state["layer"]["layer_index"]))
            state["map_url_query"] = base_url + "/map?" + "&".join(parts)
    except Exception:
        # Best-effort only; never fail tool because query fallback couldn't be built
        pass
    return state


def _build_base_map_state(
    *,
    center: str | None = None,
    zoom: int | None = None,
    locations: list | None = None,
    route_geojson: dict | None = None,
    route_summary: dict | None = None,
    basemap_id: str | None = None,
    overlays: list | None = None,
    view_mode: str | None = None,
    ground: dict | None = None,
    terrain: bool | None = None,
) -> dict:
    """Build a MapState dict without requiring markers/route or an operational layer."""
    warnings: list[str] = []
    points: list[tuple[float, float]] = []
    if center:
        pt = _parse_center(center)
        if pt:
            npt, w = _normalize_lon_lat(pt[0], pt[1])
            if w:
                warnings.append(w)
            if npt:
                points.append(npt)
    if locations:
        for loc in locations:
            pt, w = _resolve_location(loc)
            if w:
                warnings.append(w)
            if pt:
                points.append(pt)
    points = _dedupe_points(points)

    bbox = None
    center_obj = None
    if points:
        lons = [p[0] for p in points]
        lats = [p[1] for p in points]
        min_lon, max_lon = min(lons), max(lons)
        min_lat, max_lat = min(lats), max(lats)
        pad_lon = max(0.01, (max_lon - min_lon) * 0.1) or 0.01
        pad_lat = max(0.01, (max_lat - min_lat) * 0.1) or 0.01
        bbox = [min_lon - pad_lon, min_lat - pad_lat, max_lon + pad_lon, max_lat + pad_lat]
        center_obj = {"longitude": (min_lon + max_lon) / 2, "latitude": (min_lat + max_lat) / 2}

    out: dict = {
        "schemaVersion": MAP_STATE_SCHEMA_VERSION,
        "center": center_obj,
        "zoom": zoom if zoom is not None else 10,
        "bbox": bbox,
        "markers": [{"x": p[0], "y": p[1]} for p in points],
        "routeGeometry": _normalize_route_geometry(route_geojson) or route_geojson,
    }
    if warnings:
        # Best-effort diagnostics for agents/UI; safe to ignore.
        out["warnings"] = warnings
    if route_summary and isinstance(route_summary, dict):
        out["routeSummary"] = route_summary
    bm = (basemap_id or "").strip() or None
    out["basemapId"] = bm or DEFAULT_BASEMAP_ID
    vm = (view_mode or "").strip().lower() or None
    if vm in ("2d", "3d"):
        out["viewMode"] = vm
    if isinstance(ground, dict) and ground:
        out["ground"] = ground
    elif out.get("viewMode") == "3d":
        out["ground"] = {"elevationUrl": DEFAULT_ELEVATION3D_URL}
    ov = _normalize_overlays(overlays, terrain=bool(terrain)) if (overlays is not None or terrain) else []
    if ov:
        out["overlays"] = ov
    if not points and not route_geojson:
        out["message"] = "Provide center/locations, a route, or add a layer to position the map."
    return out


def _webmercator_to_wgs84(x: float, y: float) -> tuple[float, float]:
    """Convert EPSG:3857 meters to lon/lat (EPSG:4326)."""
    import math
    lon = (x / 20037508.34) * 180.0
    lat = (y / 20037508.34) * 180.0
    lat = (180.0 / math.pi) * (2.0 * math.atan(math.exp(lat * math.pi / 180.0)) - math.pi / 2.0)
    return lon, lat


def _extent_to_bbox4326(ext: dict) -> list[float] | None:
    """Convert an ArcGIS REST extent dict into [minLon,minLat,maxLon,maxLat] when possible."""
    if not isinstance(ext, dict):
        return None
    try:
        xmin = float(ext.get("xmin"))
        ymin = float(ext.get("ymin"))
        xmax = float(ext.get("xmax"))
        ymax = float(ext.get("ymax"))
    except Exception:
        return None
    sr = ext.get("spatialReference") if isinstance(ext.get("spatialReference"), dict) else {}
    wkid = sr.get("wkid") or sr.get("latestWkid")
    if wkid in (4326, "4326"):
        return [xmin, ymin, xmax, ymax]
    if wkid in (3857, 102100, "3857", "102100"):
        min_lon, min_lat = _webmercator_to_wgs84(xmin, ymin)
        max_lon, max_lat = _webmercator_to_wgs84(xmax, ymax)
        return [min(min_lon, max_lon), min(min_lat, max_lat), max(min_lon, max_lon), max(min_lat, max_lat)]
    return None


def _fetch_layer_extent_from_url(url: str, timeout_s: float = 10.0) -> list[float] | None:
    """Best-effort: fetch REST layer extent and return bbox in EPSG:4326."""
    import urllib.request
    u = (url or "").strip()
    if not u:
        return None
    sep = "&" if "?" in u else "?"
    pjson_url = u + sep + "f=pjson"
    try:
        with urllib.request.urlopen(pjson_url, timeout=timeout_s) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    return _extent_to_bbox4326(data.get("extent") if isinstance(data.get("extent"), dict) else {})


def _open_arcgis_studio(
    *,
    center: str | None = None,
    zoom: int | None = None,
    basemap_id: str | None = None,
    view_mode: str | None = None,
) -> str:
    """Return an initial MapState for the ArcGIS Studio UI."""
    st = _build_base_map_state(
        center=center,
        zoom=zoom,
        locations=None,
        route_geojson=None,
        route_summary=None,
        basemap_id=basemap_id,
        overlays=None,
        view_mode=view_mode,
        ground=None,
        terrain=None,
    )
    st = _add_map_urls(st)
    return json.dumps(st, indent=2)


def _show_map(
    center: str | None = None,
    zoom: int | None = None,
    locations: list | None = None,
    route_geojson: dict | None = None,
    layer_item_id: str | None = None,
    layer_index: int | None = None,
    route_summary: dict | None = None,
    basemap_id: str | None = None,
    overlays: list | None = None,
    view_mode: str | None = None,
    ground: dict | None = None,
    terrain: bool | None = None,
    layer_url: str | None = None,
) -> str:
    """Build JSON for the map app: bbox, center, markers, routeGeometry, optional layer, optional routeSummary.

    MapState v2 additions:
    - schemaVersion, basemapId, overlays[], viewMode, ground
    """
    gis = _get_gis()
    warnings: list[str] = []
    points: list[tuple[float, float]] = []
    if center:
        pt = _parse_center(center)
        if pt:
            npt, w = _normalize_lon_lat(pt[0], pt[1])
            if w:
                warnings.append(w)
            if npt:
                points.append(npt)
    if locations:
        for loc in locations:
            pt, w = _resolve_location(loc)
            if w:
                warnings.append(w)
            if pt:
                points.append(pt)
    # Merge near-duplicate consecutive points (e.g. "Chicago" and "Chicago, IL") into one marker
    points = _dedupe_points(points)
    if not points and not route_geojson:
        out_err: dict = {
            "error": "No valid locations resolved.",
            "hint": "Pass locations as [longitude, latitude] pairs or address strings.",
        }
        if warnings:
            out_err["warnings"] = warnings
        return json.dumps(out_err, indent=2)
    lons = [p[0] for p in points]
    lats = [p[1] for p in points]
    if lons and lats:
        min_lon, max_lon = min(lons), max(lons)
        min_lat, max_lat = min(lats), max(lats)
        pad_lon = max(0.01, (max_lon - min_lon) * 0.1) or 0.01
        pad_lat = max(0.01, (max_lat - min_lat) * 0.1) or 0.01
        bbox = [min_lon - pad_lon, min_lat - pad_lat, max_lon + pad_lon, max_lat + pad_lat]
        center_pt = ((min_lon + max_lon) / 2, (min_lat + max_lat) / 2)
    else:
        bbox = None
        center_pt = points[0] if points else (0.0, 0.0)
    markers = [{"x": p[0], "y": p[1]} for p in points]
    route_polyline = _normalize_route_geometry(route_geojson) or route_geojson
    out = {
        "schemaVersion": MAP_STATE_SCHEMA_VERSION,
        "center": {"longitude": center_pt[0], "latitude": center_pt[1]},
        "zoom": zoom if zoom is not None else 10,
        "bbox": bbox,
        "markers": markers,
        "routeGeometry": route_polyline,
    }
    if warnings:
        out["warnings"] = warnings
    if route_summary and isinstance(route_summary, dict):
        out["routeSummary"] = route_summary
    bm = (basemap_id or "").strip() or None
    if bm:
        out["basemapId"] = bm
    else:
        out["basemapId"] = DEFAULT_BASEMAP_ID
    vm = (view_mode or "").strip().lower() or None
    if vm in ("2d", "3d"):
        out["viewMode"] = vm
    if isinstance(ground, dict) and ground:
        out["ground"] = ground
    elif out.get("viewMode") == "3d":
        # Default ground elevation source for 3D scenes
        out["ground"] = {"elevationUrl": DEFAULT_ELEVATION3D_URL}
    ov = _normalize_overlays(overlays, terrain=bool(terrain)) if (overlays is not None or terrain) else []
    if ov:
        out["overlays"] = ov
    lyr_idx = int(layer_index) if layer_index is not None else 0
    url = (layer_url or "").strip() or None
    if url:
        out["layer"] = {"url": url, "layer_index": lyr_idx}
    else:
        layer_id = (layer_item_id or "").strip() or None
        if layer_id and not layer_id.startswith("lc_"):
            out["layer"] = {"item_id": layer_id, "layer_index": lyr_idx}
    out = _add_map_urls(out)
    return json.dumps(out, indent=2)


def _make_renderer_rotation_color(
    *,
    field: str,
    icon_url: str,
    icon_size_pt: int = 30,
    icon_initial_angle: float = 90,
    rotation_type: str = "arithmetic",
    color_stops: list[dict] | None = None,
) -> dict:
    """Build a JS API renderer spec: icon + rotation visualVariable + color visualVariable."""
    stops = color_stops or [
        {"value": 80, "color": "#00ffaaff"},
        {"value": 10, "color": "#17a67dff"},
        {"value": 0, "color": "#403c35ff"},
        {"value": -10, "color": "#b02626ff"},
        {"value": -80, "color": "#ff0000ff"},
    ]
    return {
        "type": "simple",
        "symbol": {
            "type": "point-3d",
            "symbolLayers": [
                {
                    "type": "icon",
                    "size": str(int(icon_size_pt)),
                    "resource": {"href": icon_url},
                    "material": {"color": "white"},
                    "angle": float(icon_initial_angle),
                }
            ],
            "verticalOffset": {"screenLength": 3, "maxWorldLength": 18, "minWorldLength": 3},
            "callout": {"type": "line", "color": "white", "size": 1},
        },
        "visualVariables": [
            {"type": "rotation", "field": field, "rotationType": rotation_type},
            {"type": "color", "field": field, "stops": stops},
        ],
    }


def _make_renderer_classbreaks(*, field: str, class_break_infos: list[dict]) -> dict:
    """Build a class-breaks renderer spec."""
    return {
        "type": "class-breaks",
        "field": field,
        "classBreakInfos": class_break_infos,
    }


def _make_renderer_unique_value(*, field: str, unique_value_infos: list[dict]) -> dict:
    """Build a unique-value renderer spec."""
    return {
        "type": "unique-value",
        "field": field,
        "uniqueValueInfos": unique_value_infos,
    }


def _make_labeling_percent(field: str) -> list[dict]:
    """Return a labelingInfo array (Arcade) to show +/- field value as percent."""
    expr = (
        f"var v = Round($feature.{field}, 0)\n"
        "if (v >= 0) {\n"
        "  return '+' + v + '%'\n"
        "} else {\n"
        "  return v + '%'\n"
        "}"
    )
    return [
        {
            "labelExpressionInfo": {"expression": expr},
            "labelPlacement": "above-center",
            "minScale": 0,
            "symbol": {
                "type": "label-3d",
                "symbolLayers": [
                    {
                        "type": "text",
                        "material": {"color": "#ffffff"},
                        "halo": {"color": "black", "size": 1.5},
                        "size": 12,
                    }
                ],
            },
        }
    ]


def _show_layer_with_renderer(
    *,
    center: str | None = None,
    zoom: int | None = None,
    locations: list | None = None,
    basemap_id: str | None = None,
    overlays: list | None = None,
    view_mode: str | None = None,
    ground: dict | None = None,
    terrain: bool | None = None,
    layer_item_id: str | None = None,
    layer_url: str | None = None,
    layer_index: int | None = None,
    opacity: float | None = None,
    definition_expression: str | None = None,
    out_fields: list | str | None = None,
    renderer: dict | None = None,
    labeling_info: list | dict | None = None,
    feature_reduction: dict | None = None,
    elevation_info: dict | None = None,
    popup_template: dict | None = None,
) -> str:
    """Return MapState with layers[] entry applying styling (renderer, labels, reduction, elevation)."""
    st = _build_base_map_state(
        center=center,
        zoom=zoom,
        locations=locations,
        route_geojson=None,
        route_summary=None,
        basemap_id=basemap_id,
        overlays=overlays,
        view_mode=view_mode,
        ground=ground,
        terrain=terrain,
    )

    lyr_idx = int(layer_index) if layer_index is not None else 0
    layer_item_id = (layer_item_id or "").strip() or None
    layer_url = (layer_url or "").strip() or None
    if not layer_item_id and not layer_url:
        return json.dumps(
            {"error": "Missing layer source.", "hint": "Provide layer_item_id (portal item id) or layer_url (FeatureServer/MapServer/etc)."},
            indent=2,
        )
    if layer_item_id and layer_item_id.startswith("lc_"):
        return json.dumps(
            {"error": "layer_item_id must be a portal item id, not a tool result id.", "hint": "Use search_content/get_item to find a portal item id, or use layer_url."},
            indent=2,
        )

    src: dict = {"layer_index": lyr_idx}
    if layer_url:
        src["url"] = layer_url
        st["layer"] = {"url": layer_url, "layer_index": lyr_idx}
    else:
        src["item_id"] = layer_item_id
        st["layer"] = {"item_id": layer_item_id, "layer_index": lyr_idx}

    entry: dict = {"source": src}
    if opacity is not None:
        try:
            entry["opacity"] = float(opacity)
        except Exception:
            pass
    if definition_expression and definition_expression.strip():
        entry["definitionExpression"] = definition_expression.strip()
    if out_fields is not None:
        entry["outFields"] = out_fields
    if isinstance(renderer, dict) and renderer:
        entry["renderer"] = renderer
    if labeling_info is not None:
        entry["labelingInfo"] = labeling_info
    if isinstance(feature_reduction, dict) and feature_reduction:
        entry["featureReduction"] = feature_reduction
    if isinstance(elevation_info, dict) and elevation_info:
        entry["elevationInfo"] = elevation_info
    if isinstance(popup_template, dict) and popup_template:
        entry["popupTemplate"] = popup_template

    st["layers"] = [entry]
    # If caller didn't provide a center/bbox, try to zoom to layer extent server-side for robustness
    if (not st.get("center") or not isinstance(st.get("center"), dict)) and (not st.get("bbox")):
        bbox = None
        if layer_url:
            bbox = _fetch_layer_extent_from_url(layer_url)
        if bbox and len(bbox) == 4:
            st["bbox"] = bbox
            st["center"] = {"longitude": (bbox[0] + bbox[2]) / 2.0, "latitude": (bbox[1] + bbox[3]) / 2.0}
    # If we successfully added a layer, remove the generic "provide center" message
    if st.get("layers") or st.get("layer"):
        st.pop("message", None)
    st = _add_map_urls(st)
    return json.dumps(st, indent=2)


def _smart_show_layer(
    *,
    center: str | None = None,
    zoom: int | None = None,
    locations: list | None = None,
    basemap_id: str | None = None,
    overlays: list | None = None,
    view_mode: str | None = None,
    ground: dict | None = None,
    terrain: bool | None = None,
    layer_item_id: str | None = None,
    layer_url: str | None = None,
    layer_index: int | None = None,
    where: str = "1=1",
    preferred_field: str | None = None,
    goal: str | None = None,
    max_categories: int = 10,
    opacity: float | None = None,
) -> str:
    """Suggest symbology then show layer with renderer. Returns MapState JSON."""

    sug = _suggest_symbology(
        _get_gis(),
        layer_item_id=layer_item_id,
        layer_url=layer_url,
        layer_index=layer_index,
        preferred_field=preferred_field,
        goal=goal,
        where=where,
        max_categories=max_categories,
    )
    if isinstance(sug, dict) and sug.get("error"):
        return json.dumps(sug, indent=2)
    renderer = sug.get("renderer") if isinstance(sug, dict) else None
    feature_reduction = sug.get("feature_reduction") if isinstance(sug, dict) else None
    return _show_layer_with_renderer(
        center=center,
        zoom=zoom,
        locations=locations,
        basemap_id=basemap_id,
        overlays=overlays,
        view_mode=view_mode,
        ground=ground,
        terrain=terrain,
        layer_item_id=layer_item_id,
        layer_url=layer_url,
        layer_index=layer_index,
        opacity=opacity,
        definition_expression=None if (where or "").strip() in ("", "1=1") else (where or "").strip(),
        out_fields="*",
        renderer=renderer if isinstance(renderer, dict) else None,
        feature_reduction=feature_reduction if isinstance(feature_reduction, dict) else None,
    )


def _infer_goal_from_intent(user_intent: str | None) -> str:
    txt = (user_intent or "").strip().lower()
    if any(k in txt for k in ["cluster", "dense points", "many points", "heatmap"]):
        return "cluster"
    if any(k in txt for k in ["category", "categorical", "unique", "type", "class"]):
        return "category"
    if any(k in txt for k in ["numeric", "values", "gradient", "choropleth", "range"]):
        return "numeric"
    return "auto"


def _auto_map(
    *,
    layer_item_id: str | None = None,
    layer_url: str | None = None,
    layer_index: int | None = None,
    user_intent: str | None = None,
    audience: str | None = None,
    constraints: dict | None = None,
    where: str = "1=1",
    center: str | None = None,
    zoom: int | None = None,
    view_mode: str | None = None,
    basemap_id: str | None = None,
    terrain: bool | None = None,
) -> str:
    """Map autopilot: infer intent, suggest style, and return display-ready MapState."""
    goal = _infer_goal_from_intent(user_intent)
    preferred_field = None
    max_categories = 10
    if isinstance(constraints, dict):
        pf = constraints.get("preferred_field")
        preferred_field = str(pf).strip() if pf is not None and str(pf).strip() else None
        mc = constraints.get("max_categories")
        try:
            if mc is not None:
                max_categories = int(mc)
        except Exception:
            pass
    vm = (view_mode or "").strip().lower() or None
    if not vm and isinstance(user_intent, str) and "3d" in user_intent.lower():
        vm = "3d"
    if vm not in ("2d", "3d"):
        vm = "2d"
    map_json = _smart_show_layer(
        center=center,
        zoom=zoom,
        locations=None,
        basemap_id=basemap_id,
        overlays=None,
        view_mode=vm,
        ground=None,
        terrain=terrain,
        layer_item_id=layer_item_id,
        layer_url=layer_url,
        layer_index=layer_index,
        where=where,
        preferred_field=preferred_field,
        goal=goal,
        max_categories=max_categories,
        opacity=None,
    )
    try:
        data = json.loads(map_json)
    except Exception:
        return map_json
    if isinstance(data, dict) and data.get("error"):
        return map_json
    rationale = []
    rationale.append(f"Intent '{(user_intent or 'auto')}' mapped to styling goal '{goal}'.")
    if vm == "3d":
        rationale.append("3D view selected based on intent/arguments.")
    if where and where.strip() not in ("", "1=1"):
        rationale.append("Applied filter constraint to focus the map.")
    next_actions = [
        "Refine with a filter (where clause) for a narrower message.",
        "Run field_stats on the styled field for evidence-backed narration.",
        "Try cluster goal for dense points or category goal for classes.",
    ]
    data["auto_map"] = {
        "goal": goal,
        "audience": (audience or "").strip() or "general",
        "rationale": " ".join(rationale),
        "next_best_actions": next_actions,
    }
    return json.dumps(data, indent=2)


def _clamp_confidence(v: float) -> float:
    try:
        return max(0.0, min(1.0, round(float(v), 2)))
    except Exception:
        return 0.0


def _intent_schema_ok(
    intent: str,
    intent_class: str,
    used_tools: list[str],
    display: dict,
    *,
    confidence: float,
    plan: list[dict] | None = None,
    executed_steps: list[dict] | None = None,
    map_state: dict | None = None,
    warnings: list[str] | None = None,
) -> str:
    out: dict = {
        "schemaVersion": 1,
        "type": "intent_result",
        "status": "ok",
        "intent": intent,
        "intent_class": intent_class,
        "confidence": _clamp_confidence(confidence),
        "used_tools": used_tools,
        "display": display,
    }
    if plan:
        out["plan"] = plan
    if executed_steps:
        out["executed_steps"] = executed_steps
    if isinstance(map_state, dict):
        out["map_state"] = map_state
    if warnings:
        out["warnings"] = warnings
    return json.dumps(out, indent=2)


def _intent_schema_needs_input(
    intent: str,
    intent_class: str,
    used_tools: list[str],
    needs_input: list[dict],
    *,
    confidence: float,
    plan: list[dict] | None = None,
    warnings: list[str] | None = None,
) -> str:
    out: dict = {
        "schemaVersion": 1,
        "type": "intent_result",
        "status": "needs_input",
        "intent": intent,
        "intent_class": intent_class,
        "confidence": _clamp_confidence(confidence),
        "used_tools": used_tools,
        "needs_input": needs_input,
    }
    if plan:
        out["plan"] = plan
    if warnings:
        out["warnings"] = warnings
    return json.dumps(out, indent=2)


def _intent_schema_error(
    intent: str,
    intent_class: str,
    used_tools: list[str],
    error: str,
    *,
    confidence: float,
    plan: list[dict] | None = None,
    executed_steps: list[dict] | None = None,
    hint: str | None = None,
) -> str:
    out: dict = {
        "schemaVersion": 1,
        "type": "intent_result",
        "status": "error",
        "intent": intent,
        "intent_class": intent_class,
        "confidence": _clamp_confidence(confidence),
        "used_tools": used_tools,
        "error": {"message": error},
    }
    if plan:
        out["plan"] = plan
    if executed_steps:
        out["executed_steps"] = executed_steps
    if hint:
        out["error"]["hint"] = hint
    return json.dumps(out, indent=2)


def _parse_json_obj(text: str) -> dict:
    try:
        data = json.loads(text)
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _attach_intent_result(map_state: dict, intent_result: dict) -> dict:
    """Attach intent metadata to a MapState payload without breaking map rendering."""
    if not isinstance(map_state, dict):
        return map_state
    meta = intent_result if isinstance(intent_result, dict) else {}
    # Reserve a single field so map apps can ignore it safely.
    map_state["intent_result"] = meta
    return map_state


def _intent_kind(intent: str) -> str:
    s = (intent or "").strip().lower()
    if any(k in s for k in ["direction", "route", "driving", "drive", "from ", " to "]):
        return "directions"
    if any(k in s for k in ["buffer", "radius", "within ", "circle around", "around "]):
        return "buffer"
    if any(k in s for k in ["nearest", "closest", "nearby", "near "]):
        return "nearest"
    if any(k in s for k in ["demographic", "population", "household", "income", "enrich"]):
        return "demographics"
    if any(k in s for k in ["auto map", "best map", "choropleth", "cluster", "style", "symbol", "renderer"]):
        return "auto_map"
    return "unknown"


def _extract_route_stops(intent: str) -> tuple[str | None, str | None]:
    s = (intent or "").strip()
    # Common pattern: directions from A to B
    m = re.search(r"\bfrom\s+(.+?)\s+to\s+(.+?)(?:[.?!]|$)", s, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip(" \"'"), m.group(2).strip(" \"'")
    # Fallback: first " to " split
    low = s.lower()
    idx = low.find(" to ")
    if idx > 0:
        left = s[:idx].strip(" \"'")
        right = s[idx + 4 :].strip(" \"'")
        left = re.sub(r"^(route|directions|driving)\s+", "", left, flags=re.IGNORECASE).strip(" \"'")
        return (left or None), (right or None)
    return None, None


def _extract_place_for_demographics(intent: str) -> str | None:
    s = (intent or "").strip()
    m = re.search(r"\b(?:for|at|in)\s+(.+?)(?:[.?!]|$)", s, flags=re.IGNORECASE)
    if m:
        place = m.group(1).strip(" \"'")
        return place or None
    return s or None


def _extract_layer_ref(intent: str) -> tuple[str | None, str | None]:
    s = (intent or "").strip()
    m_url = re.search(r"https?://\S+", s)
    if m_url:
        return None, m_url.group(0).rstrip(".,)")
    m_id = re.search(r"\b[a-fA-F0-9]{32}\b", s)
    if m_id:
        return m_id.group(0), None
    return None, None


def _intent_confidence(intent_class: str, *, has_required_slots: bool) -> float:
    if intent_class == "unknown":
        return 0.3
    if has_required_slots:
        return 0.92
    return 0.62


def _make_directions_plan(origin: str | None, destination: str | None) -> list[dict]:
    return [
        {"step": 1, "tool": "whoami", "args": {}, "reason": "Check ArcGIS session/auth context."},
        {
            "step": 2,
            "tool": "route_and_show_map",
            "args": {
                "stops": [origin or "<origin>", destination or "<destination>"],
                "zoom": 13,
                "basemap_id": "streets-navigation-vector",
            },
            "reason": "Solve route and return map-ready output in one call.",
        },
    ]


def _make_demographics_plan(place: str | None) -> list[dict]:
    return [
        {"step": 1, "tool": "whoami", "args": {}, "reason": "Check ArcGIS session/auth context."},
        {
            "step": 2,
            "tool": "enrich",
            "args": {"location": place or "<location>", "buffer_km": 1},
            "reason": "Run GeoEnrichment for demographic attributes.",
        },
    ]


def _make_auto_map_plan(layer_item_id: str | None, layer_url: str | None, user_intent: str) -> list[dict]:
    return [
        {"step": 1, "tool": "whoami", "args": {}, "reason": "Check ArcGIS session/auth context."},
        {
            "step": 2,
            "tool": "auto_map",
            "args": {
                "layer_item_id": layer_item_id,
                "layer_url": layer_url,
                "layer_index": 0,
                "user_intent": user_intent,
                "basemap_id": "streets-navigation-vector",
            },
            "reason": "Generate a display-ready map style from intent.",
        },
    ]


def _unit_to_arcgis(u: str) -> str:
    s = (u or "").strip().lower()
    if s in ("mi", "mile", "miles"):
        return "Mile"
    if s in ("km", "kilometer", "kilometers"):
        return "Kilometer"
    if s in ("m", "meter", "meters"):
        return "Meter"
    if s in ("ft", "foot", "feet"):
        return "Foot"
    return "Meter"


def _extract_buffer_params(intent: str) -> tuple[str | None, float | None, str | None]:
    s = (intent or "").strip()
    # buffer 2 miles around Pike Place Market
    m = re.search(
        r"\bbuffer\s+([0-9]+(?:\.[0-9]+)?)\s*(miles?|mi|kilometers?|km|meters?|m|feet|ft)\s+(?:around|of|near|at)\s+(.+?)(?:[.?!]|$)",
        s,
        flags=re.IGNORECASE,
    )
    if m:
        try:
            dist = float(m.group(1))
        except Exception:
            dist = None
        unit = _unit_to_arcgis(m.group(2))
        loc = m.group(3).strip(" \"'")
        return (loc or None), dist, unit
    return None, None, None


def _make_buffer_plan(location: str | None, distance: float | None, unit: str | None) -> list[dict]:
    return [
        {"step": 1, "tool": "whoami", "args": {}, "reason": "Check ArcGIS session/auth context."},
        {
            "step": 2,
            "tool": "buffer_and_show",
            "args": {
                "location": location or "<location>",
                "distance": distance if distance is not None else "<distance>",
                "unit": unit or "<unit>",
                "basemap_id": "streets-navigation-vector",
            },
            "reason": "Buffer a place and return map-ready state with polygon graphic.",
        },
    ]


def _make_nearest_plan(location: str | None, item_id: str | None, max_count: int = 10) -> list[dict]:
    return [
        {"step": 1, "tool": "whoami", "args": {}, "reason": "Check ArcGIS session/auth context."},
        {
            "step": 2,
            "tool": "nearest_and_show",
            "args": {
                "location": location or "<location>",
                "item_id": item_id or "<item_id>",
                "layer_index": 0,
                "max_count": max_count,
                "basemap_id": "streets-navigation-vector",
            },
            "reason": "Find nearest features and return map-ready graphics for display.",
        },
    ]


def _run_intent(intent: str) -> str:
    raw_intent = (intent or "").strip()
    if not raw_intent:
        return _intent_schema_needs_input(
            intent="",
            intent_class="unknown",
            used_tools=[],
            needs_input=[{"field": "intent", "prompt": "What would you like to do? Example: directions from A to B."}],
            confidence=0.0,
        )

    intent_class = _intent_kind(raw_intent)
    used_tools: list[str] = []
    warnings: list[str] = []
    executed_steps: list[dict] = []

    who = _parse_json_obj(_whoami())
    used_tools.append("whoami")
    executed_steps.append(
        {
            "tool": "whoami",
            "status": "ok",
            "result_summary": {"username": who.get("username"), "portal": who.get("portal")},
        }
    )
    if not who.get("username"):
        warnings.append("Anonymous ArcGIS session; some premium/private operations may fail.")

    if intent_class == "directions":
        origin, destination = _extract_route_stops(raw_intent)
        plan = _make_directions_plan(origin, destination)
        if not origin or not destination:
            missing = []
            if not origin:
                missing.append({"field": "origin", "prompt": "What is the starting location?"})
            if not destination:
                missing.append({"field": "destination", "prompt": "What is the destination?"})
            return _intent_schema_needs_input(
                intent=raw_intent,
                intent_class=intent_class,
                used_tools=used_tools,
                needs_input=missing,
                confidence=_intent_confidence(intent_class, has_required_slots=False),
                plan=plan,
                warnings=warnings,
            )
        route_json = _route_and_show_map(
            stops=[origin, destination],
            item_id=None,
            zoom=13,
            basemap_id="streets-navigation-vector",
        )
        used_tools.append("route_and_show_map")
        executed_steps.append(
            {
                "tool": "route_and_show_map",
                "status": "ok",
                "args": {"stops": [origin, destination], "zoom": 13, "basemap_id": "streets-navigation-vector"},
            }
        )
        route_data = _parse_json_obj(route_json)
        if route_data.get("error"):
            executed_steps[-1]["status"] = "error"
            executed_steps[-1]["result_summary"] = {"error": route_data.get("error")}
            return _intent_schema_error(
                intent=raw_intent,
                intent_class=intent_class,
                used_tools=used_tools,
                error=str(route_data.get("error")),
                confidence=_intent_confidence(intent_class, has_required_slots=True),
                plan=plan,
                executed_steps=executed_steps,
                hint=route_data.get("hint"),
            )
        dirs = route_data.get("directions")
        first_steps = []
        if isinstance(dirs, list):
            first_steps = dirs[:3]
        intent_meta = {
            "schemaVersion": 1,
            "type": "intent_result",
            "status": "ok",
            "intent": raw_intent,
            "intent_class": intent_class,
            "confidence": _clamp_confidence(_intent_confidence(intent_class, has_required_slots=True)),
            "used_tools": used_tools,
            "plan": plan,
            "executed_steps": executed_steps,
            "warnings": warnings,
            "display": {
                "map_url": route_data.get("map_url"),
                "total_time": route_data.get("total_time"),
                "total_length": route_data.get("total_length"),
                "first_3_directions": first_steps,
            },
        }
        return json.dumps(_attach_intent_result(route_data, intent_meta), indent=2)

    if intent_class == "buffer":
        location, distance, unit = _extract_buffer_params(raw_intent)
        plan = _make_buffer_plan(location, distance, unit)
        missing = []
        if not location:
            missing.append({"field": "location", "prompt": "What location should I buffer (address or place name)?"})
        if distance is None:
            missing.append({"field": "distance", "prompt": "What buffer distance?"})
        if not unit:
            missing.append({"field": "unit", "prompt": "What unit (miles, km, meters)?"})
        if missing:
            return _intent_schema_needs_input(
                intent=raw_intent,
                intent_class=intent_class,
                used_tools=used_tools,
                needs_input=missing,
                confidence=_intent_confidence(intent_class, has_required_slots=False),
                plan=plan,
                warnings=warnings,
            )
        buf_json = _buffer_and_show(
            location=location,
            distance=float(distance),
            unit=str(unit),
            basemap_id="streets-navigation-vector",
            view_mode=None,
            terrain=None,
            overlays=None,
            ground=None,
            zoom=None,
        )
        used_tools.append("buffer_and_show")
        executed_steps.append(
            {
                "tool": "buffer_and_show",
                "status": "ok",
                "args": {"location": location, "distance": float(distance), "unit": str(unit), "basemap_id": "streets-navigation-vector"},
            }
        )
        buf_data = _parse_json_obj(buf_json)
        if buf_data.get("error"):
            executed_steps[-1]["status"] = "error"
            executed_steps[-1]["result_summary"] = {"error": buf_data.get("error")}
            return _intent_schema_error(
                intent=raw_intent,
                intent_class=intent_class,
                used_tools=used_tools,
                error=str(buf_data.get("error")),
                confidence=_intent_confidence(intent_class, has_required_slots=True),
                plan=plan,
                executed_steps=executed_steps,
                hint=buf_data.get("hint"),
            )
        intent_meta = {
            "schemaVersion": 1,
            "type": "intent_result",
            "status": "ok",
            "intent": raw_intent,
            "intent_class": intent_class,
            "confidence": _clamp_confidence(_intent_confidence(intent_class, has_required_slots=True)),
            "used_tools": used_tools,
            "plan": plan,
            "executed_steps": executed_steps,
            "warnings": warnings,
        }
        return json.dumps(_attach_intent_result(buf_data, intent_meta), indent=2)

    if intent_class == "nearest":
        # Require item_id for now (by plan); location inferred from 'near/around/at/in/for' phrases
        item_id, _url = _extract_layer_ref(raw_intent)
        loc = _extract_place_for_demographics(raw_intent)  # reuse simple "for/at/in" parser
        plan = _make_nearest_plan(loc, item_id, max_count=10)
        missing = []
        if not loc:
            missing.append({"field": "location", "prompt": "Near what location?"})
        if not item_id:
            missing.append({"field": "item_id", "prompt": "Provide the portal item_id (32-char) of the layer to search."})
        if missing:
            return _intent_schema_needs_input(
                intent=raw_intent,
                intent_class=intent_class,
                used_tools=used_tools,
                needs_input=missing,
                confidence=_intent_confidence(intent_class, has_required_slots=False),
                plan=plan,
                warnings=warnings,
            )
        near_json = _nearest_and_show(
            location=loc,
            item_id=item_id,
            layer_index=0,
            max_count=10,
            max_distance_m=50000,
            basemap_id="streets-navigation-vector",
            view_mode=None,
            terrain=None,
            overlays=None,
            ground=None,
            zoom=None,
        )
        used_tools.append("nearest_and_show")
        executed_steps.append(
            {
                "tool": "nearest_and_show",
                "status": "ok",
                "args": {"location": loc, "item_id": item_id, "layer_index": 0, "max_count": 10, "basemap_id": "streets-navigation-vector"},
            }
        )
        near_data = _parse_json_obj(near_json)
        if near_data.get("error"):
            executed_steps[-1]["status"] = "error"
            executed_steps[-1]["result_summary"] = {"error": near_data.get("error")}
            return _intent_schema_error(
                intent=raw_intent,
                intent_class=intent_class,
                used_tools=used_tools,
                error=str(near_data.get("error")),
                confidence=_intent_confidence(intent_class, has_required_slots=True),
                plan=plan,
                executed_steps=executed_steps,
                hint=near_data.get("hint"),
            )
        intent_meta = {
            "schemaVersion": 1,
            "type": "intent_result",
            "status": "ok",
            "intent": raw_intent,
            "intent_class": intent_class,
            "confidence": _clamp_confidence(_intent_confidence(intent_class, has_required_slots=True)),
            "used_tools": used_tools,
            "plan": plan,
            "executed_steps": executed_steps,
            "warnings": warnings,
        }
        return json.dumps(_attach_intent_result(near_data, intent_meta), indent=2)

    if intent_class == "demographics":
        place = _extract_place_for_demographics(raw_intent)
        plan = _make_demographics_plan(place)
        if not place:
            return _intent_schema_needs_input(
                intent=raw_intent,
                intent_class=intent_class,
                used_tools=used_tools,
                needs_input=[{"field": "location", "prompt": "Which place should I enrich demographics for?"}],
                confidence=_intent_confidence(intent_class, has_required_slots=False),
                plan=plan,
                warnings=warnings,
            )
        enrich_json = _enrich(place, buffer_km=1, data_collection=None)
        used_tools.append("enrich")
        executed_steps.append(
            {
                "tool": "enrich",
                "status": "ok",
                "args": {"location": place, "buffer_km": 1},
            }
        )
        enrich_data = _parse_json_obj(enrich_json)
        if enrich_data.get("error"):
            executed_steps[-1]["status"] = "error"
            executed_steps[-1]["result_summary"] = {"error": enrich_data.get("error")}
            return _intent_schema_error(
                intent=raw_intent,
                intent_class=intent_class,
                used_tools=used_tools,
                error=str(enrich_data.get("error")),
                confidence=_intent_confidence(intent_class, has_required_slots=True),
                plan=plan,
                executed_steps=executed_steps,
                hint=enrich_data.get("hint"),
            )
        attrs = enrich_data.get("attributes") if isinstance(enrich_data.get("attributes"), dict) else {}
        return _intent_schema_ok(
            intent=raw_intent,
            intent_class=intent_class,
            used_tools=used_tools,
            confidence=_intent_confidence(intent_class, has_required_slots=True),
            plan=plan,
            executed_steps=executed_steps,
            display={
                "location": enrich_data.get("location"),
                "buffer_km": enrich_data.get("buffer_km"),
                "attributes": attrs,
            },
            map_state=None,
            warnings=warnings,
        )

    if intent_class == "auto_map":
        layer_item_id, layer_url = _extract_layer_ref(raw_intent)
        plan = _make_auto_map_plan(layer_item_id, layer_url, raw_intent)
        if not layer_item_id and not layer_url:
            return _intent_schema_needs_input(
                intent=raw_intent,
                intent_class=intent_class,
                used_tools=used_tools,
                needs_input=[{"field": "layer_item_id_or_url", "prompt": "Provide a portal layer item id (32-char) or ArcGIS service URL."}],
                confidence=_intent_confidence(intent_class, has_required_slots=False),
                plan=plan,
                warnings=warnings,
            )
        auto_json = _auto_map(
            layer_item_id=layer_item_id,
            layer_url=layer_url,
            layer_index=0,
            user_intent=raw_intent,
            basemap_id="streets-navigation-vector",
        )
        used_tools.append("auto_map")
        executed_steps.append(
            {
                "tool": "auto_map",
                "status": "ok",
                "args": {
                    "layer_item_id": layer_item_id,
                    "layer_url": layer_url,
                    "layer_index": 0,
                    "user_intent": raw_intent,
                    "basemap_id": "streets-navigation-vector",
                },
            }
        )
        auto_data = _parse_json_obj(auto_json)
        if auto_data.get("error"):
            executed_steps[-1]["status"] = "error"
            executed_steps[-1]["result_summary"] = {"error": auto_data.get("error")}
            return _intent_schema_error(
                intent=raw_intent,
                intent_class=intent_class,
                used_tools=used_tools,
                error=str(auto_data.get("error")),
                confidence=_intent_confidence(intent_class, has_required_slots=True),
                plan=plan,
                executed_steps=executed_steps,
                hint=auto_data.get("hint"),
            )
        intent_meta = {
            "schemaVersion": 1,
            "type": "intent_result",
            "status": "ok",
            "intent": raw_intent,
            "intent_class": intent_class,
            "confidence": _clamp_confidence(_intent_confidence(intent_class, has_required_slots=True)),
            "used_tools": used_tools,
            "plan": plan,
            "executed_steps": executed_steps,
            "warnings": warnings,
            "display": {
                "map_url": auto_data.get("map_url"),
                "map_state_id": auto_data.get("map_state_id"),
                "auto_map": auto_data.get("auto_map"),
            },
        }
        return json.dumps(_attach_intent_result(auto_data, intent_meta), indent=2)

    return _intent_schema_needs_input(
        intent=raw_intent,
        intent_class=intent_class,
        used_tools=used_tools,
        needs_input=[
            {
                "field": "intent_type",
                "prompt": "I can do directions, demographics, or auto-map. Which one do you want?",
                "options": ["directions", "demographics", "auto_map"],
            }
        ],
        confidence=_intent_confidence(intent_class, has_required_slots=False),
        warnings=warnings,
    )


def _export_map_app(map_state: dict | None = None, map_state_id: str | None = None, title: str | None = None) -> str:
    """Export a deployable single-file HTML map from MapState or map_state_id."""
    state = None
    if isinstance(map_state, dict):
        state = dict(map_state)
    elif map_state_id and str(map_state_id).strip():
        state = _get_map_state(str(map_state_id).strip())
        if not state:
            return json.dumps({"error": "Map state not found.", "hint": "Provide a valid map_state_id or pass map_state directly."}, indent=2)
    if not isinstance(state, dict):
        return json.dumps({"error": "Missing map_state/map_state_id.", "hint": "Provide map_state from show_map/auto_map or a map_state_id."}, indent=2)
    html = _map_viewer_html(state)
    safe_title = (title or "arcgis-map-export").strip() or "arcgis-map-export"
    out = {
        "filename": safe_title.replace(" ", "_") + ".html",
        "html": html,
        "bytes": len(html.encode("utf-8")),
        "note": "This is a standalone HTML map artifact. Save to a .html file and open in a browser.",
    }
    return json.dumps(out, indent=2)


def _show_rotation_icon_layer_3d(
    *,
    layer_item_id: str | None = None,
    layer_url: str | None = None,
    layer_index: int | None = None,
    field: str = "Property_Value_Diff_Percent",
    icon_url: str | None = None,
    center: str | None = None,
    zoom: int | None = None,
    basemap_id: str | None = None,
    terrain: bool | None = True,
) -> str:
    """One-call demo: 3D rotation+color trend arrows with Arcade % labels and decluttering."""
    # Defaults from Esri sample (safe public content)
    default_icon = "https://jsapi.maps.arcgis.com/sharing/rest/content/items/0e6d7969d8b248f6aadb58affc75020e/data"
    default_layer_item_id = "e1018631be3c4069b57c2aff151aa013"
    icon_url = (icon_url or default_icon).strip()
    layer_item_id = (layer_item_id or "").strip() or None
    layer_url = (layer_url or "").strip() or None
    if not layer_item_id and not layer_url:
        layer_item_id = default_layer_item_id

    renderer = _make_renderer_rotation_color(field=field, icon_url=icon_url)
    labeling_info = _make_labeling_percent(field)
    feature_reduction = {"type": "selection"}
    elevation_info = {"mode": "relative-to-scene"}
    return _show_layer_with_renderer(
        center=center,
        zoom=zoom,
        locations=None,
        basemap_id=basemap_id,
        overlays=None,
        view_mode="3d",
        ground=None,
        terrain=terrain,
        layer_item_id=layer_item_id,
        layer_url=layer_url,
        layer_index=layer_index,
        renderer=renderer,
        labeling_info=labeling_info,
        feature_reduction=feature_reduction,
        elevation_info=elevation_info,
    )


def _buffer_and_show(
    *,
    location: str,
    distance: float,
    unit: str = "Meter",
    basemap_id: str | None = None,
    view_mode: str | None = None,
    terrain: bool | None = None,
    overlays: list | None = None,
    ground: dict | None = None,
    zoom: int | None = None,
) -> str:
    """Buffer a location and return a MapState that draws the buffer polygon."""
    sess = _current_session_id_var.get() or "anon"
    if not _rate_limit(f"buffer_and_show:{sess}", limit=60, per_seconds=60):
        return json.dumps({"error": "Rate limited.", "hint": "Too many buffer requests; retry in a minute."}, indent=2)
    pt, w = _resolve_location(location)
    if not pt:
        return json.dumps({"error": "Could not resolve location.", "hint": "Use an address or 'longitude,latitude'."}, indent=2)
    buf_str = _geometry_buffer(
        geometry=None,
        x=float(pt[0]),
        y=float(pt[1]),
        in_sr=4326,
        distance=float(distance),
        unit=str(unit or "Meter"),
        out_sr=None,
    )
    try:
        buf = json.loads(buf_str)
    except Exception:
        buf = {}
    if not isinstance(buf, dict) or "error" in buf:
        return buf_str
    geom = buf.get("geometry")
    if not isinstance(geom, dict):
        return json.dumps({"error": "Buffer returned no geometry."}, indent=2)
    if "spatialReference" not in geom:
        geom["spatialReference"] = {"wkid": 4326}

    st = _build_base_map_state(
        center=f"{pt[0]},{pt[1]}",
        zoom=zoom,
        locations=None,
        route_geojson=None,
        route_summary=None,
        basemap_id=basemap_id,
        overlays=overlays,
        view_mode=view_mode,
        ground=ground,
        terrain=terrain,
    )
    if w:
        st.setdefault("warnings", [])
        if isinstance(st["warnings"], list):
            st["warnings"].append(w)
    st["graphics"] = [
        {
            "geometry": geom,
            "symbol": {
                "type": "simple-fill",
                "color": [0, 122, 255, 0.18],
                "outline": {"type": "simple-line", "color": [0, 122, 255, 0.9], "width": 2},
            },
            "popupTemplate": {
                "title": "Buffer",
                "content": f"{distance:g} {unit}",
            },
        }
    ]
    st["analysis"] = {
        "type": "buffer",
        "location": [float(pt[0]), float(pt[1])],
        "distance": float(distance),
        "unit": str(unit),
    }
    st = _add_map_urls(st)
    return json.dumps(st, indent=2)


def _nearest_and_show(
    *,
    location: str,
    item_id: str,
    layer_index: int = 0,
    max_count: int = 10,
    max_distance_m: float = 50000,
    basemap_id: str | None = None,
    view_mode: str | None = None,
    terrain: bool | None = None,
    overlays: list | None = None,
    ground: dict | None = None,
    zoom: int | None = None,
) -> str:
    """Find nearest features and return a MapState with result points rendered as graphics."""
    sess = _current_session_id_var.get() or "anon"
    if not _rate_limit(f"nearest_and_show:{sess}", limit=30, per_seconds=60):
        return json.dumps({"error": "Rate limited.", "hint": "Too many nearest requests; retry in a minute."}, indent=2)
    result_str = _find_nearest(location, item_id, layer_index, max_count, max_distance_m)
    try:
        data = json.loads(result_str)
    except Exception:
        return result_str
    if not isinstance(data, dict) or "error" in data:
        return result_str
    loc = data.get("location")
    if not (isinstance(loc, list) and len(loc) >= 2):
        return json.dumps({"error": "Nearest result missing location."}, indent=2)
    try:
        lon, lat = float(loc[0]), float(loc[1])
    except Exception:
        return json.dumps({"error": "Nearest result location invalid."}, indent=2)

    st = _build_base_map_state(
        center=f"{lon},{lat}",
        zoom=zoom,
        locations=None,
        route_geojson=None,
        route_summary=None,
        basemap_id=basemap_id,
        overlays=overlays,
        view_mode=view_mode,
        ground=ground,
        terrain=terrain,
    )
    st["markers"] = [{"x": lon, "y": lat}]

    def _trim_attributes(attrs: dict, max_keys: int = 12) -> dict:
        if not isinstance(attrs, dict):
            return {}
        out = {}
        for i, (k, v) in enumerate(attrs.items()):
            if i >= max_keys:
                break
            out[str(k)] = v
        return out

    graphics: list[dict] = []
    analysis_rows: list[dict] = []
    for f in (data.get("features") or []):
        if not isinstance(f, dict):
            continue
        geom = f.get("geometry")
        attrs = f.get("attributes") if isinstance(f.get("attributes"), dict) else {}
        if not isinstance(geom, dict):
            continue
        if "spatialReference" not in geom:
            geom["spatialReference"] = {"wkid": 4326}
        dist_m = f.get("distance_m")
        title = "Nearest feature"
        if dist_m is not None:
            title = f"Nearest feature (~{dist_m} m)"
        analysis_rows.append({"distance_m": dist_m, "attributes": _trim_attributes(attrs)})
        graphics.append(
            {
                "geometry": geom,
                "symbol": {
                    "type": "simple-marker",
                    "style": "circle",
                    "color": [0, 122, 255, 0.85],
                    "size": 10,
                    "outline": {"type": "simple-line", "color": [255, 255, 255, 1], "width": 1.5},
                },
                "attributes": attrs,
                "popupTemplate": {"title": title, "content": "{*}"},
            }
        )
    if graphics:
        st["graphics"] = graphics
    st["analysis"] = {
        "type": "nearest",
        "location": [lon, lat],
        "item_id": item_id,
        "layer_index": layer_index,
        "count": int(data.get("count") or len(analysis_rows)),
        "rows": analysis_rows,
    }
    st = _add_map_urls(st)
    return json.dumps(st, indent=2)


def _summarize_layer_stats(
    *,
    item_id: str,
    layer_index: int = 0,
    where: str = "1=1",
    numeric_field: str | None = None,
    group_by_field: str | None = None,
    max_groups: int = 25,
) -> str:
    """Compute basic statistics for a layer, optionally grouped."""
    sess = _current_session_id_var.get() or "anon"
    if not _rate_limit(f"summarize_layer_stats:{sess}", limit=30, per_seconds=60):
        return json.dumps({"error": "Rate limited.", "hint": "Too many summary requests; retry in a minute."}, indent=2)
    gis = _get_gis()
    item = gis.content.get(item_id)
    if item is None:
        return json.dumps({"error": f"Item not found: {item_id}"}, indent=2)
    layers = getattr(item, "layers", []) or []
    tables = getattr(item, "tables", []) or []
    if not layers and not tables:
        return json.dumps({"error": "Item has no feature layers or tables."}, indent=2)
    layer = (layers[layer_index] if 0 <= layer_index < len(layers) else layers[0]) if layers else (tables[layer_index] if 0 <= layer_index < len(tables) else tables[0])

    nf = (numeric_field or "").strip() or None
    gb = (group_by_field or "").strip() or None
    if not nf:
        # Default: count only
        try:
            res = layer.query(where=where or "1=1", return_count_only=True)
            count = res if isinstance(res, int) else (getattr(res, "count", None) or 0)
            return json.dumps({"item_id": item_id, "layer_index": layer_index, "where": where, "count": count}, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e), "hint": "Layer may not support count-only query."}, indent=2)

    # Use server-side out_statistics when available (fast, avoids downloading features)
    try:
        out_stats = [
            {"statisticType": "count", "onStatisticField": nf, "outStatisticFieldName": "count"},
            {"statisticType": "min", "onStatisticField": nf, "outStatisticFieldName": "min"},
            {"statisticType": "max", "onStatisticField": nf, "outStatisticFieldName": "max"},
            {"statisticType": "avg", "onStatisticField": nf, "outStatisticFieldName": "avg"},
        ]
        kwargs = {
            "where": where or "1=1",
            "out_statistics": out_stats,
            "return_geometry": False,
        }
        if gb:
            kwargs["group_by_fields_for_statistics"] = gb
            kwargs["result_record_count"] = int(max(1, min(200, max_groups)))
        fs = layer.query(**kwargs)
        feats = list(fs.features) if fs and getattr(fs, "features", None) else []
        rows = [f.as_dict.get("attributes", {}) for f in feats]
        return json.dumps(
            {
                "item_id": item_id,
                "layer_index": layer_index,
                "where": where,
                "numeric_field": nf,
                "group_by_field": gb,
                "rows": rows,
            },
            indent=2,
            default=_json_serial_default,
        )
    except Exception as e:
        return json.dumps({"error": str(e), "hint": "Layer may not support out_statistics; try without group_by_field."}, indent=2)


def _get_map_viewer_url(item_id: str) -> str:
    """Return the portal map viewer URL for a web map or web scene."""
    id_str = (item_id or "").strip()
    if id_str.startswith("lc_") and len(id_str) >= 10:
        return json.dumps({
            "error": "item_id must be a portal web map or web scene ID from ArcGIS Online/Enterprise, not a tool result id.",
            "hint": "Tool result ids (e.g. lc_...) are not portal items. Use search_content to find web maps, or pass the item id from get_item/search_content (e.g. 32-character hex). The in-chat map does not have a shareable URL.",
        }, indent=2)
    gis = _get_gis()
    item = gis.content.get(id_str)
    if item is None:
        return json.dumps({"error": f"Item not found: {item_id}"})
    item_type = (getattr(item, "type", None) or "").lower()
    portal_url = getattr(gis.properties, "portalHostname", None) or "www.arcgis.com"
    if not portal_url.startswith("http"):
        portal_url = "https://" + portal_url
    base = portal_url.rstrip("/")
    if "web scene" in item_type or item_type == "webscene":
        return json.dumps({"url": f"{base}/home/webmap/viewer.html?webscene={id_str}"}, indent=2)
    return json.dumps({"url": f"{base}/home/webmap/viewer.html?webmap={id_str}"}, indent=2)


def _enrich(location: str, buffer_km: float = 1, data_collection: str | None = None) -> str:
    """Enrich a location (address or lon,lat) with demographics. Requires GeoEnrichment/Business Analyst."""
    import math
    gis = _get_gis()
    pt = _parse_center(location)
    if not pt and location and location.strip():
        import arcgis.geocoding
        geocoders = arcgis.geocoding.get_geocoders(gis)
        if geocoders:
            res = arcgis.geocoding.geocode(location.strip(), max_locations=1, geocoder=geocoders[0])
            if res and res[0].get("location"):
                loc = res[0]["location"]
                pt = (loc.get("x", loc.get("longitude")), loc.get("y", loc.get("latitude")))
    if not pt:
        return json.dumps({"error": "Could not resolve location.", "hint": "Use an address or 'longitude,latitude'."}, indent=2)
    try:
        from arcgis.geoenrichment import enrich as geoenrich
        from arcgis.geometry import Geometry
        study_areas = [Geometry({"x": pt[0], "y": pt[1], "spatialReference": {"wkid": 4326}})]
        kwargs = {"return_geometry": False, "gis": gis}
        if buffer_km and buffer_km > 0:
            kwargs["proximity_type"] = "straight_line"
            kwargs["proximity_value"] = buffer_km
            kwargs["proximity_metric"] = "kilometers"
        if data_collection:
            kwargs["data_collections"] = [data_collection]
        df = geoenrich(study_areas=study_areas, **kwargs)
    except ImportError:
        return json.dumps({"error": "GeoEnrichment not available.", "hint": "Requires arcgis.geoenrichment (ArcGIS Online or Enterprise with Business Analyst)."}, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e), "hint": "GeoEnrichment may require an org with Business Analyst; check location and buffer."}, indent=2)
    if df is None or (hasattr(df, "empty") and df.empty):
        return json.dumps({"message": "No enrichment data returned.", "location": list(pt)}, indent=2)
    try:
        row = df.iloc[0] if hasattr(df, "iloc") else df.to_dict("records")[0]
        out = {"location": list(pt), "buffer_km": buffer_km}
        if hasattr(row, "to_dict"):
            out["attributes"] = {k: v for k, v in row.to_dict().items() if not (isinstance(v, float) and math.isnan(v))}
        else:
            out["attributes"] = dict(row)
        return json.dumps(out, indent=2)
    except Exception as e:
        return json.dumps({"location": list(pt), "raw": str(df), "error": str(e)}, indent=2)


def _find_nearest(
    location: str,
    item_id: str,
    layer_index: int = 0,
    max_count: int = 10,
    max_distance_m: float = 50000,
) -> str:
    """Find features nearest to a point from a feature layer (spatial query + sort by distance)."""
    gis = _get_gis()
    pt, _w = _resolve_location(location)
    if not pt:
        return json.dumps({"error": "Could not resolve location.", "hint": "Use an address or 'longitude,latitude'."}, indent=2)
    item = gis.content.get(item_id)
    if item is None:
        return json.dumps({"error": f"Item not found: {item_id}"}, indent=2)
    layers = getattr(item, "layers", []) or []
    tables = getattr(item, "tables", []) or []
    if not layers and not tables:
        return json.dumps({"error": "Item has no feature layers or tables."}, indent=2)
    layer = (layers[layer_index] if 0 <= layer_index < len(layers) else layers[0]) if layers else (tables[layer_index] if 0 <= layer_index < len(tables) else tables[0])
    try:
        from arcgis.geometry import Point as AGSPoint
        geom = AGSPoint({"x": pt[0], "y": pt[1], "spatialReference": {"wkid": 4326}})
        result = layer.query(
            where="1=1",
            geometry=geom,
            distance=max_distance_m,
            units="meters",
            out_fields="*",
            return_geometry=True,
            result_record_count=max_count * 3,
        )
    except Exception as e:
        return json.dumps({"error": str(e), "hint": "Layer may not support spatial query."}, indent=2)
    features = list(result.features) if result and result.features else []
    import math
    def dist(f):
        g = getattr(f, "geometry", None) or getattr(f, "as_dict", lambda: {})().get("geometry")
        if not g: return float("inf")
        x = g.get("x", g.get("longitude"))
        y = g.get("y", g.get("latitude"))
        if x is None or y is None:
            return float("inf")
        return math.hypot(x - pt[0], y - pt[1])
    features.sort(key=dist)
    features = features[:max_count]
    m_per_deg_lon = 111320 * math.cos(math.radians(pt[1]))
    m_per_deg_lat = 111320
    def dist_m(f):
        g = getattr(f, "geometry", None) or getattr(f, "as_dict", lambda: {})().get("geometry")
        if not g: return None
        x, y = g.get("x", g.get("longitude")), g.get("y", g.get("latitude"))
        if x is None or y is None: return None
        dx = (x - pt[0]) * m_per_deg_lon
        dy = (y - pt[1]) * m_per_deg_lat
        return math.sqrt(dx * dx + dy * dy)
    out = {"location": list(pt), "count": len(features), "features": [f.as_dict for f in features]}
    for i, f in enumerate(features):
        dm = dist_m(f)
        if dm is not None:
            out["features"][i]["distance_m"] = round(dm, 2)
    return json.dumps(out, indent=2)


def _summarize_nearby(
    location: str,
    item_id: str,
    layer_index: int = 0,
    distance_m: float = 5000,
    where: str = "1=1",
) -> str:
    """Count features in a layer within distance of a location."""
    gis = _get_gis()
    pt, _w = _resolve_location(location)
    if not pt:
        return json.dumps({"error": "Could not resolve location.", "hint": "Use an address or 'longitude,latitude'."}, indent=2)
    item = gis.content.get(item_id)
    if item is None:
        return json.dumps({"error": f"Item not found: {item_id}"}, indent=2)
    layers = getattr(item, "layers", []) or []
    tables = getattr(item, "tables", []) or []
    if not layers and not tables:
        return json.dumps({"error": "Item has no feature layers or tables."}, indent=2)
    layer = (layers[layer_index] if 0 <= layer_index < len(layers) else layers[0]) if layers else (tables[layer_index] if 0 <= layer_index < len(tables) else tables[0])
    try:
        from arcgis.geometry import Point as AGSPoint
        geom = AGSPoint({"x": pt[0], "y": pt[1], "spatialReference": {"wkid": 4326}})
        result = layer.query(
            where=where or "1=1",
            geometry=geom,
            distance=distance_m,
            units="meters",
            return_geometry=False,
            return_count_only=True,
        )
    except Exception as e:
        return json.dumps({"error": str(e), "hint": "Layer may not support spatial query."}, indent=2)
    count = result if isinstance(result, int) else (getattr(result, "count", None) or len(getattr(result, "features", [])))
    return json.dumps({"location": list(pt), "distance_m": distance_m, "count": count}, indent=2)


def _export_layer_geojson(
    item_id: str,
    layer_index: int = 0,
    where: str = "1=1",
    max_records: int = 100,
) -> str:
    """Query layer and return GeoJSON FeatureCollection."""
    result_str = _query_layer(
        item_id=item_id,
        layer_index=layer_index,
        where=where,
        out_fields="*",
        return_geometry=True,
        max_records=max_records,
    )
    data = json.loads(result_str)
    if "error" in data:
        return result_str
    features = data.get("features", [])
    geojson_features = []
    for f in features:
        geom = f.get("geometry")
        props = dict(f.get("attributes", f))
        if "geometry" in props:
            props = {k: v for k, v in props.items() if k != "geometry"}
        if geom:
            geojson_geom = _arcgis_geom_to_geojson(geom)
        else:
            geojson_geom = None
        geojson_features.append({"type": "Feature", "geometry": geojson_geom, "properties": props})
    fc = {"type": "FeatureCollection", "features": geojson_features}
    return json.dumps(fc, indent=2)


def _arcgis_geom_to_geojson(geom: dict) -> dict | None:
    """Convert ArcGIS JSON geometry to GeoJSON geometry."""
    if not geom:
        return None
    sr = geom.get("spatialReference", {})
    wkid = sr.get("wkid") or sr.get("latestWkid")
    if "x" in geom and "y" in geom:
        return {"type": "Point", "coordinates": [geom["x"], geom["y"]]}
    if "paths" in geom:
        paths = geom["paths"]
        if len(paths) == 1:
            return {"type": "LineString", "coordinates": [[c[0], c[1]] for c in paths[0]]}
        return {"type": "MultiLineString", "coordinates": [[[c[0], c[1]] for c in p] for p in paths]}
    if "rings" in geom:
        rings = geom["rings"]
        return {"type": "Polygon", "coordinates": [[[c[0], c[1]] for c in r] for r in rings]}
    return None


def _batch_geocode(addresses: list[str]) -> str:
    """Geocode a list of addresses."""
    import arcgis.geocoding
    gis = _get_gis()
    if not addresses or not isinstance(addresses, list):
        return json.dumps({"error": "addresses must be a non-empty list of strings."})
    addrs = [str(a).strip() for a in addresses if str(a).strip()]
    if not addrs:
        return json.dumps({"error": "No valid address strings in addresses."})
    geocoders = arcgis.geocoding.get_geocoders(gis)
    if not geocoders:
        return json.dumps({"error": "No geocoder available for this portal.", "hint": "Batch geocoding requires a geocode service."})
    try:
        result = arcgis.geocoding.batch_geocode(addrs, geocoder=geocoders[0])
    except Exception as e:
        return json.dumps({"error": str(e), "hint": "Check that the geocoder supports batch geocode."})
    out = [{"address": r.get("address"), "location": r.get("location"), "score": r.get("score")} for r in (result or [])]
    return json.dumps({"locations": out}, indent=2)


def _share_item(
    item_id: str,
    group_ids: list[str] | None = None,
    allow_org: bool = False,
    allow_everyone: bool = False,
) -> str:
    """Share a portal item with org, everyone, and/or groups."""
    gis = _get_gis()
    item = gis.content.get(item_id)
    if item is None:
        return json.dumps({"error": f"Item not found: {item_id}"})
    try:
        item.share(
            org=allow_org,
            everyone=allow_everyone,
            groups=(group_ids or []),
        )
    except Exception as e:
        return json.dumps({"error": str(e), "hint": "Ensure you have permission to share this item."})
    return json.dumps({
        "message": "Sharing updated.",
        "item_id": item_id,
        "allow_org": allow_org,
        "allow_everyone": allow_everyone,
        "group_ids": group_ids or [],
    }, indent=2)


# --- Server and handlers (PyPI mcp decorator API) ---

def _create_server() -> Server:
    server = Server("arcgis-mcp")

    @server.list_tools()
    async def list_tools(_req: types.ListToolsRequest | None = None) -> types.ListToolsResult:
        return types.ListToolsResult(tools=TOOLS_LIST)

    @server.list_resources()
    async def list_resources() -> list[types.Resource]:
        return [
            types.Resource(
                uri=AnyUrl(ARCGIS_MAP_APP_URI),
                name="ArcGIS Map App",
                mimeType=RESOURCE_MIME_TYPE,
            ),
            types.Resource(
                uri=AnyUrl(ARCGIS_STUDIO_APP_URI),
                name="ArcGIS Studio",
                mimeType=RESOURCE_MIME_TYPE,
            ),
            types.Resource(
                uri=AnyUrl(ARCGIS_EXAMPLES_APP_URI),
                name="ArcGIS Examples",
                mimeType=RESOURCE_MIME_TYPE,
            ),
        ]

    @server.read_resource()
    async def read_resource(uri: AnyUrl) -> list[ReadResourceContents]:
        uri_str = str(uri)
        if uri_str == ARCGIS_MAP_APP_URI:
            path = _get_arcgis_map_app_html_path()
        elif uri_str == ARCGIS_STUDIO_APP_URI:
            path = _get_arcgis_studio_app_html_path()
        elif uri_str == ARCGIS_EXAMPLES_APP_URI:
            path = _get_arcgis_examples_app_html_path()
        else:
            raise ValueError(f"Unknown resource: {uri_str}")
        if not path.exists():
            raise FileNotFoundError(f"App HTML not found: {path}")
        html = path.read_text(encoding="utf-8")
        csp_meta = {
            "ui": {
                "csp": {
                    "connectDomains": [
                        "https://js.arcgis.com",
                        "https://*.arcgis.com",
                        "https://esm.sh",
                    ],
                    "resourceDomains": [
                        "https://js.arcgis.com",
                        "https://*.arcgis.com",
                        "https://esm.sh",
                    ],
                },
            },
        }
        return [
            ReadResourceContents(content=html, mime_type=RESOURCE_MIME_TYPE, meta=csp_meta),
        ]

    PROMPTS_LIST = [
        types.Prompt(
            name="directions_and_map",
            description="Get directions between two places and show the route on a map. Use route_and_show_map with exactly two stops: origin and destination. Share map_url from the response.",
            arguments=[
                types.PromptArgument(name="origin", description="Starting address or place", required=True),
                types.PromptArgument(name="destination", description="Ending address or place", required=True),
            ],
        ),
        types.Prompt(
            name="demographics_at_place",
            description="Get demographics and key facts at a location. Use geocode to resolve the address, then enrich with the returned coordinates (or pass the address to enrich). Summarize population, households, and other KeyGlobalFacts for the user.",
            arguments=[
                types.PromptArgument(name="place", description="Address or place name", required=True),
            ],
        ),
        types.Prompt(
            name="map_with_my_layer",
            description="Show a map with a portal feature layer or web map. Use list_my_content or search_content to find a layer or web map by name; then call show_map (or route_and_show_map) with layer_item_id set to that item's id. Provide center or locations so the map has a view.",
            arguments=[
                types.PromptArgument(name="layer_name_or_id", description="Name of the layer to find, or portal item id", required=True),
            ],
        ),
        types.Prompt(
            name="best_map_for_layer",
            description="Autopilot map for a layer using auto_map. Picks renderer, map settings, and returns rationale + next actions.",
            arguments=[
                types.PromptArgument(name="layer_item_id_or_url", description="Portal item id or service URL", required=True),
                types.PromptArgument(name="intent", description="What insight the map should communicate", required=False),
            ],
        ),
        types.Prompt(
            name="choropleth_from_field",
            description="Create a numeric choropleth style for a layer using smart_show_layer/auto_map with numeric intent.",
            arguments=[
                types.PromptArgument(name="layer_item_id_or_url", description="Portal item id or service URL", required=True),
                types.PromptArgument(name="field", description="Numeric field name", required=True),
            ],
        ),
        types.Prompt(
            name="cluster_points",
            description="Create a clustered point map for dense point layers using smart_show_layer or auto_map with cluster intent.",
            arguments=[
                types.PromptArgument(name="layer_item_id_or_url", description="Portal item id or service URL", required=True),
            ],
        ),
        types.Prompt(
            name="time_series_map",
            description="Create a map optimized for time-enabled layers and recommend time-aware next steps.",
            arguments=[
                types.PromptArgument(name="layer_item_id_or_url", description="Portal item id or service URL", required=True),
                types.PromptArgument(name="time_field", description="Optional time field hint", required=False),
            ],
        ),
    ]

    @server.list_prompts()
    async def list_prompts(_req: types.ListPromptsRequest | None = None) -> types.ListPromptsResult:
        return types.ListPromptsResult(prompts=PROMPTS_LIST)

    @server.get_prompt()
    async def get_prompt(name: str, arguments: dict[str, str] | None = None) -> types.GetPromptResult:
        args = arguments or {}
        if name == "directions_and_map":
            origin = args.get("origin", "origin")
            destination = args.get("destination", "destination")
            text = f"Get directions from {origin} to {destination} and show the route on a map. Use the route_and_show_map tool with exactly two stops: [\"{origin}\", \"{destination}\"]. Share the map_url from the response so the user can open the map in a browser."
        elif name == "demographics_at_place":
            place = args.get("place", "this place")
            text = f"Get demographics and key facts for {place}. First geocode \"{place}\" if needed, then use the enrich tool with that location (or the address). Summarize population, households, and other variables for me."
        elif name == "map_with_my_layer":
            layer_name = args.get("layer_name_or_id", "my layer")
            text = f"Show a map with the layer: {layer_name}. Use list_my_content or search_content to find the item, then call show_map with center or locations and layer_item_id set to that item's id. Give me the map_url if available."
        elif name == "best_map_for_layer":
            layer_ref = args.get("layer_item_id_or_url", "layer")
            intent = args.get("intent", "communicate the key pattern")
            text = (
                f"Build the best map for {layer_ref}. Use auto_map with user_intent '{intent}'. "
                "If the value looks like a URL, pass layer_url; otherwise pass layer_item_id. "
                "Return the map_url when available and include the auto_map rationale."
            )
        elif name == "choropleth_from_field":
            layer_ref = args.get("layer_item_id_or_url", "layer")
            field = args.get("field", "value_field")
            text = (
                f"Create a numeric choropleth for {layer_ref} using field {field}. "
                "Call smart_show_layer (or auto_map) with goal 'numeric' and preferred_field set to that field. "
                "Share map_url when available."
            )
        elif name == "cluster_points":
            layer_ref = args.get("layer_item_id_or_url", "layer")
            text = (
                f"Create a clustered map for dense point data in {layer_ref}. "
                "Call smart_show_layer (or auto_map) with goal 'cluster'. "
                "If needed, apply a where filter first."
            )
        elif name == "time_series_map":
            layer_ref = args.get("layer_item_id_or_url", "layer")
            time_field = args.get("time_field", "time field")
            text = (
                f"Create a time-aware map for {layer_ref}. First call describe_layer to inspect timeInfo and fields, "
                f"then style with auto_map or smart_show_layer. Use {time_field} if present and summarize recommended "
                "time-series next steps for the user."
            )
        else:
            text = "Unknown prompt. Use directions_and_map, demographics_at_place, map_with_my_layer, best_map_for_layer, choropleth_from_field, cluster_points, or time_series_map."
        return types.GetPromptResult(
            messages=[types.PromptMessage(role="user", content=types.TextContent(type="text", text=text))],
        )

    @server.call_tool()
    async def call_tool(tool_name: str, arguments: dict) -> types.CallToolResult:
        t0 = time.monotonic()
        logger.info("tool_call", extra={"tool": tool_name, **_ctx()})
        args = arguments or {}
        lim = _tool_limiter(tool_name)
        await lim.acquire()
        try:
            if tool_name == "search_content":
                query = (args.get("query") or "").strip()
                if not query:
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({
                            "error": "Missing or empty required argument: query.",
                            "hint": "Provide a search term, e.g. 'parcels', 'maps', or 'owner:username'.",
                        }, indent=2))],
                        isError=True,
                    )
                text = await anyio.to_thread.run_sync(
                    _search_content,
                    query,
                    args.get("item_type"),
                    int(args.get("max_items", 10)),
                )
            elif tool_name == "geocode":
                text = await anyio.to_thread.run_sync(
                    _geocode,
                    args.get("address", ""),
                    int(args.get("max_locations", 5)),
                )
            elif tool_name == "get_item":
                item_id = args.get("item_id")
                if not item_id:
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text="Missing required argument: item_id")],
                        isError=True,
                    )
                text = await anyio.to_thread.run_sync(_get_item, item_id)
            elif tool_name == "resolve_layer":
                item_id = (args.get("item_id") or "").strip() or None
                url = (args.get("url") or "").strip() or None
                layer_index = args.get("layer_index")
                layer_index = int(layer_index) if layer_index is not None else 0
                text = await anyio.to_thread.run_sync(_resolve_layer, item_id, url, layer_index)
            elif tool_name == "whoami":
                text = await anyio.to_thread.run_sync(_whoami)
            elif tool_name == "get_arcgis_login_url":
                base_url = (args.get("base_url") or os.environ.get("ARCGIS_AUTH_BASE_URL", "")).strip()
                if not base_url:
                    text = json.dumps({
                        "error": "No base_url provided and ARCGIS_AUTH_BASE_URL not set.",
                        "hint": "Set ARCGIS_AUTH_BASE_URL in .env to your server URL (e.g. https://your-ngrok.ngrok-free.app) or pass base_url to this tool.",
                    }, indent=2)
                else:
                    login_url = base_url.rstrip("/") + "/auth/login"
                    text = json.dumps({
                        "login_url": login_url,
                        "message": "Open this URL in a browser to sign in. After signing in, the page shows a one-time code. Tell the agent that code (e.g. 'use ArcGIS code a1b2c3d4'); the agent should call set_arcgis_token_with_code with the code, not set_arcgis_token.",
                    }, indent=2)
            elif tool_name == "set_arcgis_token":
                token = (args.get("token") or "").strip()
                if not token:
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({"error": "Missing required argument: token"}, indent=2))],
                        isError=True,
                    )
                referer = (args.get("referer") or os.environ.get("ARCGIS_URL", "") or "https://www.arcgis.com").strip()
                session_id = _current_session_id_var.get()
                # Never error on missing session id; store as pending when client doesn't send Mcp-Session-Id.
                if session_id:
                    _store.session_token_set(session_id=session_id, token=token, referer=referer, ttl_seconds=_session_token_ttl_seconds())
                    text = json.dumps({
                        "message": "Token stored for this session. Subsequent tool calls (whoami, search_content, etc.) will use your ArcGIS identity.",
                    }, indent=2)
                else:
                    _store.pending_token_set(token=token, referer=referer)
                    logger.info("ArcGIS token stored as pending (no MCP session id); will be used for tool calls.")
                    text = json.dumps({
                        "message": "Token accepted. It will be used for your requests. If your client sends a session id on the next request, the token will be stored for that session.",
                    }, indent=2)
            elif tool_name == "set_arcgis_token_with_code":
                code = (args.get("code") or "").strip().lower()
                if not code:
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({"error": "Missing required argument: code"}, indent=2))],
                        isError=True,
                    )
                entry = _store.one_time_code_pop(code=code)
                if not isinstance(entry, dict):
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({
                            "error": "Invalid or expired code.",
                            "hint": "Sign in again at the login URL to get a new code. Codes expire after 5 minutes.",
                        }, indent=2))],
                        isError=True,
                    )
                session_id = _current_session_id_var.get()
                referer_val = entry.get("referer", "https://www.arcgis.com")
                # Never error on missing session id: clients like Trimble AI may not send Mcp-Session-Id; use pending token for all requests.
                if session_id:
                    _store.session_token_set(session_id=session_id, token=entry["token"], referer=referer_val, ttl_seconds=_session_token_ttl_seconds())
                    text = json.dumps({
                        "message": "Token stored for this session. Subsequent tool calls (whoami, search_content, etc.) will use your ArcGIS identity.",
                    }, indent=2)
                else:
                    _store.pending_token_set(token=entry["token"], referer=referer_val)
                    logger.info("ArcGIS token stored as pending (no MCP session id); will be used for tool calls.")
                    text = json.dumps({
                        "message": "Token accepted. It will be used for your requests (whoami, search_content, etc.). If your client sends a session id on the next request, the token will be stored for that session.",
                    }, indent=2)
            elif tool_name == "reverse_geocode":
                try:
                    lon = float(args.get("longitude"))
                    lat = float(args.get("latitude"))
                except (TypeError, ValueError):
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({
                            "error": "Missing or invalid longitude/latitude.",
                            "hint": "Pass numeric longitude and latitude.",
                        }, indent=2))],
                        isError=True,
                    )
                dist = args.get("distance")
                distance = float(dist) if dist is not None else None
                text = await anyio.to_thread.run_sync(_reverse_geocode, lon, lat, distance)
            elif tool_name == "suggest":
                text_arg = (args.get("text") or "").strip()
                if not text_arg:
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({"error": "Missing required argument: text."}, indent=2))],
                        isError=True,
                    )
                lat = args.get("latitude")
                lon = args.get("longitude")
                text = await anyio.to_thread.run_sync(
                    _suggest,
                    text_arg,
                    float(lat) if lat is not None else None,
                    float(lon) if lon is not None else None,
                    int(args.get("max_suggestions", 5)),
                )
            elif tool_name == "get_item_details":
                item_id = args.get("item_id")
                if not item_id:
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({"error": "Missing required argument: item_id."}, indent=2))],
                        isError=True,
                    )
                text = await anyio.to_thread.run_sync(_get_item_details, item_id)
            elif tool_name == "list_my_content":
                folder = (args.get("folder") or "").strip() or None
                item_type = (args.get("item_type") or "").strip() or None
                text = await anyio.to_thread.run_sync(
                    _list_my_content,
                    folder,
                    item_type,
                    int(args.get("max_items", 20)),
                )
            elif tool_name == "query_layer":
                item_id = args.get("item_id")
                if not item_id:
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({"error": "Missing required argument: item_id."}, indent=2))],
                        isError=True,
                    )
                text = await anyio.to_thread.run_sync(
                    _query_layer,
                    item_id,
                    int(args.get("layer_index", 0)),
                    (args.get("where") or "1=1").strip(),
                    (args.get("out_fields") or "*").strip(),
                    bool(args.get("return_geometry", True)),
                    int(args.get("max_records", 100)),
                )
            elif tool_name == "geometry_buffer":
                dist = args.get("distance")
                if dist is None:
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({"error": "Missing required argument: distance."}, indent=2))],
                        isError=True,
                    )
                try:
                    distance = float(dist)
                except (TypeError, ValueError):
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({"error": "distance must be a number."}, indent=2))],
                        isError=True,
                    )
                geom = args.get("geometry") if isinstance(args.get("geometry"), dict) else None
                x = args.get("x")
                y = args.get("y")
                if x is not None:
                    x = float(x)
                if y is not None:
                    y = float(y)
                text = await anyio.to_thread.run_sync(
                    _geometry_buffer,
                    geom,
                    x,
                    y,
                    int(args.get("in_sr", 4326)),
                    distance,
                    (args.get("unit") or "Meter").strip(),
                    int(args["out_sr"]) if args.get("out_sr") is not None else None,
                )
            elif tool_name == "geometry_project":
                geom = args.get("geometry")
                in_sr = args.get("in_sr")
                out_sr = args.get("out_sr")
                if not isinstance(geom, dict):
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({"error": "Missing or invalid geometry (object with x, y or paths)."}, indent=2))],
                        isError=True,
                    )
                if in_sr is None or out_sr is None:
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({"error": "Missing in_sr or out_sr (well-known ID, e.g. 4326, 3857)."}, indent=2))],
                        isError=True,
                    )
                text = await anyio.to_thread.run_sync(_geometry_project, geom, int(in_sr), int(out_sr))
            elif tool_name == "route":
                stops = args.get("stops")
                if not isinstance(stops, list):
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({"error": "Missing or invalid stops. Pass a list of [lon, lat] or address strings.", "hint": "At least two stops required."}, indent=2))],
                        isError=True,
                    )
                if len(stops) < 2:
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({"error": "At least two stops required.", "hint": "Pass [origin, destination] or more."}, indent=2))],
                        isError=True,
                    )
                if len(stops) > _max_route_stops():
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({"error": f"Too many stops (max {_max_route_stops()}).", "hint": "Use fewer stops."}, indent=2))],
                        isError=True,
                    )
                item_id = (args.get("item_id") or "").strip() or None
                text = await anyio.to_thread.run_sync(_route, stops, item_id)
            elif tool_name == "route_and_show_map":
                stops = args.get("stops")
                if not isinstance(stops, list):
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({"error": "Missing or invalid stops. Pass a list of [lon, lat] or address strings.", "hint": "Use exactly two stops for A-to-B directions."}, indent=2))],
                        isError=True,
                    )
                if len(stops) < 2:
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({"error": "At least two stops required.", "hint": "Pass [origin, destination]."}, indent=2))],
                        isError=True,
                    )
                if len(stops) > _max_route_stops():
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({"error": f"Too many stops (max {_max_route_stops()}).", "hint": "Use fewer stops."}, indent=2))],
                        isError=True,
                    )
                item_id = (args.get("item_id") or "").strip() or None
                zoom = args.get("zoom")
                zoom = int(zoom) if zoom is not None else None
                layer_item_id = (args.get("layer_item_id") or "").strip() or None
                layer_url = (args.get("layer_url") or "").strip() or None
                layer_index = args.get("layer_index")
                layer_index = int(layer_index) if layer_index is not None else None
                basemap_id = (args.get("basemap_id") or "").strip() or None
                overlays = args.get("overlays") if isinstance(args.get("overlays"), list) else None
                view_mode = (args.get("view_mode") or "").strip() or None
                ground = args.get("ground") if isinstance(args.get("ground"), dict) else None
                terrain = bool(args.get("terrain", False)) if args.get("terrain") is not None else None
                text = await anyio.to_thread.run_sync(
                    _route_and_show_map,
                    stops,
                    item_id,
                    zoom,
                    layer_item_id,
                    layer_index,
                    basemap_id,
                    overlays,
                    view_mode,
                    ground,
                    terrain,
                    layer_url,
                )
            elif tool_name == "show_map":
                center = (args.get("center") or "").strip() or None
                zoom = args.get("zoom")
                zoom = int(zoom) if zoom is not None else None
                locations = args.get("locations") if isinstance(args.get("locations"), list) else None
                route_geojson = args.get("route_geojson") if isinstance(args.get("route_geojson"), dict) else None
                if not center and not locations and not route_geojson:
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({"error": "Provide center, locations, or route_geojson.", "hint": "At least one required to position or draw the map."}, indent=2))],
                        isError=True,
                    )
                layer_item_id = (args.get("layer_item_id") or "").strip() or None
                layer_url = (args.get("layer_url") or "").strip() or None
                layer_index = args.get("layer_index")
                layer_index = int(layer_index) if layer_index is not None else None
                basemap_id = (args.get("basemap_id") or "").strip() or None
                overlays = args.get("overlays") if isinstance(args.get("overlays"), list) else None
                view_mode = (args.get("view_mode") or "").strip() or None
                ground = args.get("ground") if isinstance(args.get("ground"), dict) else None
                terrain = bool(args.get("terrain", False)) if args.get("terrain") is not None else None
                text = await anyio.to_thread.run_sync(
                    _show_map,
                    center,
                    zoom,
                    locations,
                    route_geojson,
                    layer_item_id,
                    layer_index,
                    None,  # route_summary
                    basemap_id,
                    overlays,
                    view_mode,
                    ground,
                    terrain,
                    layer_url,
                )
            elif tool_name == "open_arcgis_studio":
                center = (args.get("center") or "").strip() or None
                zoom = args.get("zoom")
                zoom = int(zoom) if zoom is not None else None
                basemap_id = (args.get("basemap_id") or "").strip() or None
                view_mode = (args.get("view_mode") or "").strip() or None
                text = await anyio.to_thread.run_sync(lambda: _open_arcgis_studio(
                    center=center,
                    zoom=zoom,
                    basemap_id=basemap_id,
                    view_mode=view_mode,
                ))
            elif tool_name == "make_renderer_rotation_color":
                field = (args.get("field") or "").strip()
                icon_url = (args.get("icon_url") or "").strip()
                if not field or not icon_url:
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({"error": "Missing required arguments: field, icon_url."}, indent=2))],
                        isError=True,
                    )
                icon_size_pt = int(args.get("icon_size_pt", 30))
                icon_initial_angle = float(args.get("icon_initial_angle", 90))
                rotation_type = (args.get("rotation_type") or "arithmetic").strip() or "arithmetic"
                color_stops = args.get("color_stops") if isinstance(args.get("color_stops"), list) else None
                renderer = _make_renderer_rotation_color(
                    field=field,
                    icon_url=icon_url,
                    icon_size_pt=icon_size_pt,
                    icon_initial_angle=icon_initial_angle,
                    rotation_type=rotation_type,
                    color_stops=color_stops,
                )
                text = json.dumps(renderer, indent=2)
            elif tool_name == "make_renderer_classbreaks":
                field = (args.get("field") or "").strip()
                infos = args.get("class_break_infos")
                if not field or not isinstance(infos, list):
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({"error": "Missing required arguments: field, class_break_infos (array)."}, indent=2))],
                        isError=True,
                    )
                renderer = _make_renderer_classbreaks(field=field, class_break_infos=infos)
                text = json.dumps(renderer, indent=2)
            elif tool_name == "make_renderer_unique_value":
                field = (args.get("field") or "").strip()
                infos = args.get("unique_value_infos")
                if not field or not isinstance(infos, list):
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({"error": "Missing required arguments: field, unique_value_infos (array)."}, indent=2))],
                        isError=True,
                    )
                renderer = _make_renderer_unique_value(field=field, unique_value_infos=infos)
                text = json.dumps(renderer, indent=2)
            elif tool_name == "show_layer_with_renderer":
                center = (args.get("center") or "").strip() or None
                zoom = args.get("zoom")
                zoom = int(zoom) if zoom is not None else None
                locations = args.get("locations") if isinstance(args.get("locations"), list) else None
                layer_item_id = (args.get("layer_item_id") or "").strip() or None
                layer_url = (args.get("layer_url") or "").strip() or None
                layer_index = args.get("layer_index")
                layer_index = int(layer_index) if layer_index is not None else None
                basemap_id = (args.get("basemap_id") or "").strip() or None
                overlays = args.get("overlays") if isinstance(args.get("overlays"), list) else None
                view_mode = (args.get("view_mode") or "").strip() or None
                ground = args.get("ground") if isinstance(args.get("ground"), dict) else None
                terrain = bool(args.get("terrain", False)) if args.get("terrain") is not None else None
                opacity = args.get("opacity")
                opacity = float(opacity) if opacity is not None else None
                definition_expression = (args.get("definition_expression") or "").strip() or None
                out_fields = args.get("out_fields")
                renderer = args.get("renderer") if isinstance(args.get("renderer"), dict) else None
                labeling_info = args.get("labeling_info")
                if labeling_info is not None and not isinstance(labeling_info, (dict, list)):
                    labeling_info = None
                feature_reduction = args.get("feature_reduction") if isinstance(args.get("feature_reduction"), dict) else None
                elevation_info = args.get("elevation_info") if isinstance(args.get("elevation_info"), dict) else None
                popup_template = args.get("popup_template") if isinstance(args.get("popup_template"), dict) else None
                text = await anyio.to_thread.run_sync(lambda: _show_layer_with_renderer(
                    center=center,
                    zoom=zoom,
                    locations=locations,
                    basemap_id=basemap_id,
                    overlays=overlays,
                    view_mode=view_mode,
                    ground=ground,
                    terrain=terrain,
                    layer_item_id=layer_item_id,
                    layer_url=layer_url,
                    layer_index=layer_index,
                    opacity=opacity,
                    definition_expression=definition_expression,
                    out_fields=out_fields,
                    renderer=renderer,
                    labeling_info=labeling_info,
                    feature_reduction=feature_reduction,
                    elevation_info=elevation_info,
                    popup_template=popup_template,
                ))
            elif tool_name == "show_rotation_icon_layer_3d":
                layer_item_id = (args.get("layer_item_id") or "").strip() or None
                layer_url = (args.get("layer_url") or "").strip() or None
                layer_index = args.get("layer_index")
                layer_index = int(layer_index) if layer_index is not None else None
                field = (args.get("field") or "Property_Value_Diff_Percent").strip() or "Property_Value_Diff_Percent"
                icon_url = (args.get("icon_url") or "").strip() or None
                center = (args.get("center") or "").strip() or None
                zoom = args.get("zoom")
                zoom = int(zoom) if zoom is not None else None
                basemap_id = (args.get("basemap_id") or "").strip() or None
                terrain = bool(args.get("terrain", True)) if args.get("terrain") is not None else True
                text = await anyio.to_thread.run_sync(lambda: _show_rotation_icon_layer_3d(
                    layer_item_id=layer_item_id,
                    layer_url=layer_url,
                    layer_index=layer_index,
                    field=field,
                    icon_url=icon_url,
                    center=center,
                    zoom=zoom,
                    basemap_id=basemap_id,
                    terrain=terrain,
                ))
            elif tool_name == "buffer_and_show":
                loc = (args.get("location") or "").strip()
                dist = args.get("distance")
                if not loc or dist is None:
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({"error": "Missing required arguments: location, distance."}, indent=2))],
                        isError=True,
                    )
                try:
                    distance = float(dist)
                except (TypeError, ValueError):
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({"error": "distance must be a number."}, indent=2))],
                        isError=True,
                    )
                unit = (args.get("unit") or "Meter").strip() or "Meter"
                zoom = args.get("zoom")
                zoom = int(zoom) if zoom is not None else None
                basemap_id = (args.get("basemap_id") or "").strip() or None
                view_mode = (args.get("view_mode") or "").strip() or None
                overlays = args.get("overlays") if isinstance(args.get("overlays"), list) else None
                ground = args.get("ground") if isinstance(args.get("ground"), dict) else None
                terrain = bool(args.get("terrain", False)) if args.get("terrain") is not None else None
                text = await anyio.to_thread.run_sync(lambda: _buffer_and_show(
                    location=loc,
                    distance=distance,
                    unit=unit,
                    basemap_id=basemap_id,
                    view_mode=view_mode,
                    terrain=terrain,
                    overlays=overlays,
                    ground=ground,
                    zoom=zoom,
                ))
            elif tool_name == "nearest_and_show":
                loc = (args.get("location") or "").strip()
                iid = (args.get("item_id") or "").strip()
                if not loc or not iid:
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({"error": "Missing required arguments: location, item_id."}, indent=2))],
                        isError=True,
                    )
                layer_index = int(args.get("layer_index", 0))
                max_count = int(args.get("max_count", 10))
                max_distance_m = float(args.get("max_distance_m", 50000))
                zoom = args.get("zoom")
                zoom = int(zoom) if zoom is not None else None
                basemap_id = (args.get("basemap_id") or "").strip() or None
                view_mode = (args.get("view_mode") or "").strip() or None
                overlays = args.get("overlays") if isinstance(args.get("overlays"), list) else None
                ground = args.get("ground") if isinstance(args.get("ground"), dict) else None
                terrain = bool(args.get("terrain", False)) if args.get("terrain") is not None else None
                text = await anyio.to_thread.run_sync(lambda: _nearest_and_show(
                    location=loc,
                    item_id=iid,
                    layer_index=layer_index,
                    max_count=max_count,
                    max_distance_m=max_distance_m,
                    basemap_id=basemap_id,
                    view_mode=view_mode,
                    terrain=terrain,
                    overlays=overlays,
                    ground=ground,
                    zoom=zoom,
                ))
            elif tool_name == "summarize_layer_stats":
                iid = (args.get("item_id") or "").strip()
                if not iid:
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({"error": "Missing required argument: item_id."}, indent=2))],
                        isError=True,
                    )
                layer_index = int(args.get("layer_index", 0))
                where = (args.get("where") or "1=1").strip()
                numeric_field = (args.get("numeric_field") or "").strip() or None
                group_by_field = (args.get("group_by_field") or "").strip() or None
                max_groups = int(args.get("max_groups", 25))
                text = await anyio.to_thread.run_sync(lambda: _summarize_layer_stats(
                    item_id=iid,
                    layer_index=layer_index,
                    where=where,
                    numeric_field=numeric_field,
                    group_by_field=group_by_field,
                    max_groups=max_groups,
                ))
            elif tool_name == "get_map_viewer_url":
                item_id = args.get("item_id")
                if not item_id:
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({"error": "Missing required argument: item_id."}, indent=2))],
                        isError=True,
                    )
                text = await anyio.to_thread.run_sync(_get_map_viewer_url, (args.get("item_id") or "").strip())
            elif tool_name == "export_layer_geojson":
                item_id = args.get("item_id")
                if not item_id:
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({"error": "Missing required argument: item_id."}, indent=2))],
                        isError=True,
                    )
                text = await anyio.to_thread.run_sync(
                    _export_layer_geojson,
                    (args.get("item_id") or "").strip(),
                    int(args.get("layer_index", 0)),
                    (args.get("where") or "1=1").strip(),
                    int(args.get("max_records", 100)),
                )
            elif tool_name == "batch_geocode":
                addresses = args.get("addresses")
                if not isinstance(addresses, list):
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({"error": "Missing or invalid addresses (must be a list of strings)."}, indent=2))],
                        isError=True,
                    )
                text = await anyio.to_thread.run_sync(_batch_geocode, addresses)
            elif tool_name == "share_item":
                item_id = args.get("item_id")
                if not item_id:
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({"error": "Missing required argument: item_id."}, indent=2))],
                        isError=True,
                    )
                group_ids = args.get("group_ids")
                if group_ids is not None and not isinstance(group_ids, list):
                    group_ids = None
                text = await anyio.to_thread.run_sync(
                    _share_item,
                    (args.get("item_id") or "").strip(),
                    [str(g) for g in group_ids] if group_ids else None,
                    bool(args.get("allow_org", False)),
                    bool(args.get("allow_everyone", False)),
                )
            elif tool_name == "enrich":
                loc = (args.get("location") or "").strip()
                if not loc:
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({"error": "Missing required argument: location."}, indent=2))],
                        isError=True,
                    )
                buffer_km = args.get("buffer_km")
                buffer_km = float(buffer_km) if buffer_km is not None else 1.0
                data_collection = (args.get("data_collection") or "").strip() or None
                text = await anyio.to_thread.run_sync(_enrich, loc, buffer_km, data_collection)
            elif tool_name == "find_nearest":
                loc = (args.get("location") or "").strip()
                iid = (args.get("item_id") or "").strip()
                if not loc or not iid:
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({"error": "Missing required arguments: location, item_id."}, indent=2))],
                        isError=True,
                    )
                layer_index = int(args.get("layer_index", 0))
                max_count = int(args.get("max_count", 10))
                max_distance_m = float(args.get("max_distance_m", 50000))
                text = await anyio.to_thread.run_sync(_find_nearest, loc, iid, layer_index, max_count, max_distance_m)
            elif tool_name == "summarize_nearby":
                loc = (args.get("location") or "").strip()
                iid = (args.get("item_id") or "").strip()
                if not loc or not iid:
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({"error": "Missing required arguments: location, item_id."}, indent=2))],
                        isError=True,
                    )
                layer_index = int(args.get("layer_index", 0))
                distance_m = float(args.get("distance_m", 5000))
                where = (args.get("where") or "1=1").strip()
                text = await anyio.to_thread.run_sync(_summarize_nearby, loc, iid, layer_index, distance_m, where)
            elif tool_name == "describe_layer":
                layer_item_id = (args.get("layer_item_id") or "").strip() or None
                layer_url = (args.get("layer_url") or "").strip() or None
                layer_index = int(args.get("layer_index", 0))
                text = await anyio.to_thread.run_sync(
                    lambda: json.dumps(
                        _describe_layer(_get_gis(), layer_item_id=layer_item_id, layer_url=layer_url, layer_index=layer_index),
                        indent=2,
                        default=_json_serial_default,
                    )
                )
            elif tool_name == "sample_features":
                layer_item_id = (args.get("layer_item_id") or "").strip() or None
                layer_url = (args.get("layer_url") or "").strip() or None
                layer_index = int(args.get("layer_index", 0))
                where = (args.get("where") or "1=1").strip()
                out_fields = (args.get("out_fields") or "*").strip()
                limit = int(args.get("limit", 10))
                offset = int(args.get("offset", 0))
                order_by = (args.get("order_by") or "").strip() or None
                return_geometry = bool(args.get("return_geometry", True))
                text = await anyio.to_thread.run_sync(
                    lambda: json.dumps(
                        _sample_features(
                            _get_gis(),
                            layer_item_id=layer_item_id,
                            layer_url=layer_url,
                            layer_index=layer_index,
                            where=where,
                            out_fields=out_fields,
                            limit=limit,
                            offset=offset,
                            order_by=order_by,
                            return_geometry=return_geometry,
                        ),
                        indent=2,
                        default=_json_serial_default,
                    )
                )
            elif tool_name == "distinct_values":
                layer_item_id = (args.get("layer_item_id") or "").strip() or None
                layer_url = (args.get("layer_url") or "").strip() or None
                layer_index = int(args.get("layer_index", 0))
                field = (args.get("field") or "").strip()
                where = (args.get("where") or "1=1").strip()
                max_values = int(args.get("max_values", 25))
                text = await anyio.to_thread.run_sync(
                    lambda: json.dumps(
                        _distinct_values(
                            _get_gis(),
                            layer_item_id=layer_item_id,
                            layer_url=layer_url,
                            layer_index=layer_index,
                            field=field,
                            where=where,
                            max_values=max_values,
                        ),
                        indent=2,
                        default=_json_serial_default,
                    )
                )
            elif tool_name == "field_stats":
                layer_item_id = (args.get("layer_item_id") or "").strip() or None
                layer_url = (args.get("layer_url") or "").strip() or None
                layer_index = int(args.get("layer_index", 0))
                numeric_field = (args.get("numeric_field") or "").strip()
                where = (args.get("where") or "1=1").strip()
                group_by_field = (args.get("group_by_field") or "").strip() or None
                max_groups = int(args.get("max_groups", 25))
                histogram_bins = int(args.get("histogram_bins", 0))
                histogram_sample_size = int(args.get("histogram_sample_size", 500))
                text = await anyio.to_thread.run_sync(
                    lambda: json.dumps(
                        _field_stats(
                            _get_gis(),
                            layer_item_id=layer_item_id,
                            layer_url=layer_url,
                            layer_index=layer_index,
                            numeric_field=numeric_field,
                            where=where,
                            group_by_field=group_by_field,
                            max_groups=max_groups,
                            histogram_bins=histogram_bins,
                            histogram_sample_size=histogram_sample_size,
                        ),
                        indent=2,
                        default=_json_serial_default,
                    )
                )
            elif tool_name == "suggest_symbology":
                layer_item_id = (args.get("layer_item_id") or "").strip() or None
                layer_url = (args.get("layer_url") or "").strip() or None
                layer_index = int(args.get("layer_index", 0))
                preferred_field = (args.get("preferred_field") or "").strip() or None
                goal = (args.get("goal") or "").strip() or None
                where = (args.get("where") or "1=1").strip()
                max_categories = int(args.get("max_categories", 10))
                text = await anyio.to_thread.run_sync(
                    lambda: json.dumps(
                        _suggest_symbology(
                            _get_gis(),
                            layer_item_id=layer_item_id,
                            layer_url=layer_url,
                            layer_index=layer_index,
                            preferred_field=preferred_field,
                            goal=goal,
                            where=where,
                            max_categories=max_categories,
                        ),
                        indent=2,
                        default=_json_serial_default,
                    )
                )
            elif tool_name == "smart_show_layer":
                center = (args.get("center") or "").strip() or None
                zoom = args.get("zoom")
                zoom = int(zoom) if zoom is not None else None
                locations = args.get("locations") if isinstance(args.get("locations"), list) else None
                layer_item_id = (args.get("layer_item_id") or "").strip() or None
                layer_url = (args.get("layer_url") or "").strip() or None
                layer_index = int(args.get("layer_index", 0))
                where = (args.get("where") or "1=1").strip()
                preferred_field = (args.get("preferred_field") or "").strip() or None
                goal = (args.get("goal") or "").strip() or None
                max_categories = int(args.get("max_categories", 10))
                basemap_id = (args.get("basemap_id") or "").strip() or None
                overlays = args.get("overlays") if isinstance(args.get("overlays"), list) else None
                view_mode = (args.get("view_mode") or "").strip() or None
                ground = args.get("ground") if isinstance(args.get("ground"), dict) else None
                terrain = bool(args.get("terrain", False)) if args.get("terrain") is not None else None
                opacity = args.get("opacity")
                opacity = float(opacity) if opacity is not None else None
                text = await anyio.to_thread.run_sync(
                    lambda: _smart_show_layer(
                        center=center,
                        zoom=zoom,
                        locations=locations,
                        basemap_id=basemap_id,
                        overlays=overlays,
                        view_mode=view_mode,
                        ground=ground,
                        terrain=terrain,
                        layer_item_id=layer_item_id,
                        layer_url=layer_url,
                        layer_index=layer_index,
                        where=where,
                        preferred_field=preferred_field,
                        goal=goal,
                        max_categories=max_categories,
                        opacity=opacity,
                    )
                )
            elif tool_name == "auto_map":
                layer_item_id = (args.get("layer_item_id") or "").strip() or None
                layer_url = (args.get("layer_url") or "").strip() or None
                layer_index = int(args.get("layer_index", 0))
                user_intent = (args.get("user_intent") or "").strip() or None
                audience = (args.get("audience") or "").strip() or None
                constraints = args.get("constraints") if isinstance(args.get("constraints"), dict) else None
                where = (args.get("where") or "1=1").strip()
                center = (args.get("center") or "").strip() or None
                zoom = args.get("zoom")
                zoom = int(zoom) if zoom is not None else None
                view_mode = (args.get("view_mode") or "").strip() or None
                basemap_id = (args.get("basemap_id") or "").strip() or None
                terrain = bool(args.get("terrain", False)) if args.get("terrain") is not None else None
                text = await anyio.to_thread.run_sync(
                    lambda: _auto_map(
                        layer_item_id=layer_item_id,
                        layer_url=layer_url,
                        layer_index=layer_index,
                        user_intent=user_intent,
                        audience=audience,
                        constraints=constraints,
                        where=where,
                        center=center,
                        zoom=zoom,
                        view_mode=view_mode,
                        basemap_id=basemap_id,
                        terrain=terrain,
                    )
                )
            elif tool_name == "run_intent":
                intent = (args.get("intent") or "").strip()
                if not intent:
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({"error": "Missing required argument: intent."}, indent=2))],
                        isError=True,
                    )
                text = await anyio.to_thread.run_sync(_run_intent, intent)
            elif tool_name == "open_example_app":
                name = (args.get("name") or "").strip()
                if not name:
                    return types.CallToolResult(
                        content=[types.TextContent(type="text", text=json.dumps({"error": "Missing required argument: name."}, indent=2))],
                        isError=True,
                    )
                initial_state = args.get("initial_state") if isinstance(args.get("initial_state"), dict) else None
                text = json.dumps(
                    {
                        "name": name,
                        "initial_state": initial_state or {},
                        "message": "Opening example app.",
                    },
                    indent=2,
                )
            elif tool_name == "export_map_app":
                map_state = args.get("map_state") if isinstance(args.get("map_state"), dict) else None
                map_state_id = (args.get("map_state_id") or "").strip() or None
                title = (args.get("title") or "").strip() or None
                text = await anyio.to_thread.run_sync(_export_map_app, map_state, map_state_id, title)
            else:
                return types.CallToolResult(
                    content=[types.TextContent(type="text", text=f"Unknown tool: {tool_name}")],
                    isError=True,
                )
            duration_sec = time.monotonic() - t0
            logger.info("tool_call_ok", extra={"tool": tool_name, "duration_sec": round(duration_sec, 3), **_ctx()})
            is_error = False
            try:
                parsed = json.loads(text) if isinstance(text, str) else None
                if isinstance(parsed, dict) and parsed.get("error"):
                    is_error = True
            except Exception:
                # Non-JSON payloads are allowed (e.g., plain strings).
                pass
            return types.CallToolResult(content=[types.TextContent(type="text", text=text)], isError=is_error)
        except Exception as e:
            duration_sec = time.monotonic() - t0
            logger.warning(
                "tool_call_error",
                extra={"tool": tool_name, "duration_sec": round(duration_sec, 3), "error": str(e).split(chr(10))[0], **_ctx()},
            )
            msg = str(e).split("\n")[0] if str(e) else "Tool failed"
            return types.CallToolResult(
                content=[types.TextContent(type="text", text=json.dumps({"error": msg, "hint": "Check inputs and try again.", "request_id": _request_id_var.get()}, indent=2))],
                isError=True,
            )
        finally:
            lim.release()

    return server


# --- Optional Bearer auth middleware ---

def bearer_middleware(app):
    """ASGI middleware: require Authorization Bearer for /mcp when MCP_API_KEY is set."""
    import os

    key = os.environ.get("MCP_API_KEY", "").strip()
    if not key:
        return app

    async def check_bearer(scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await app(scope, receive, send)
            return
        path = scope.get("path", "")
        if not path.startswith("/mcp"):
            await app(scope, receive, send)
            return
        headers = list(scope.get("headers", []))
        auth = next((v for k, v in headers if k == b"authorization"), b"")
        token = None
        if auth and auth.startswith(b"Bearer "):
            token = auth[7:].decode("utf-8", errors="replace").strip()
        if not token or token != key:
            response = JSONResponse(
                {"jsonrpc": "2.0", "id": None, "error": {"code": -32001, "message": "Authorization required"}},
                status_code=401,
            )
            await response(scope, receive, send)
            return
        await app(scope, receive, send)

    return check_bearer


@click.command()
@click.option("--port", default=None, type=int, help="Port (default: 3000 or PORT env)")
@click.option("--host", default="127.0.0.1", help="Host to bind")
@click.option("--log-level", default="INFO", help="Logging level")
@click.option("--env-file", default=None, type=click.Path(exists=True), help="Path to .env (use when started from another directory)")
def main(port: int | None, host: str, log_level: str, env_file: str | None) -> None:
    global _gis
    import os

    # Load .env so ARCGIS_* and PORT are set (last loaded wins)
    from pathlib import Path
    env_paths_tried: list[str] = []
    try:
        from dotenv import load_dotenv
        pkg_root = Path(__file__).resolve().parent.parent
        home = Path.home()
        for path in [home / ".arcgis-mcp-server.env", pkg_root / ".env", Path.cwd() / ".env"]:
            env_paths_tried.append(str(path))
            load_dotenv(path)
        env_file_env = os.environ.get("ARCGIS_ENV_FILE", "").strip()
        if env_file_env:
            env_paths_tried.append(env_file_env)
            load_dotenv(Path(env_file_env))
        if env_file:
            env_paths_tried.append(env_file)
            load_dotenv(Path(env_file))
    except ImportError:
        pass

    # Recreate shared store after env is loaded (picks up ARCGIS_REDIS_URL).
    global _store
    _store = create_store_from_env()

    logging.basicConfig(level=getattr(logging, log_level.upper()), format="%(message)s")
    if port is None:
        port = int(os.environ.get("PORT", "3000"))

    _gis, auth_scheme = get_gis()
    me = _gis.users.me
    if me:
        logger.info("Connected to ArcGIS: %s via %s (user: %s)", _gis.properties.portalName, auth_scheme, me.username)
    else:
        logger.warning(
            "Connected to ArcGIS: %s (%s). Set ARCGIS_URL, ARCGIS_USERNAME, ARCGIS_PASSWORD in .env for authenticated access.",
            _gis.properties.portalName,
            auth_scheme,
        )
        # If no credential env vars were found, log where .env was tried so operators can fix path
        has_creds = (
            os.environ.get("ARCGIS_PROFILE", "").strip()
            or (os.environ.get("ARCGIS_URL", "").strip() and os.environ.get("ARCGIS_USERNAME", "").strip() and os.environ.get("ARCGIS_PASSWORD", "").strip())
            or (os.environ.get("ARCGIS_URL", "").strip() and os.environ.get("ARCGIS_KEY_FILE", "").strip() and os.environ.get("ARCGIS_CERT_FILE", "").strip())
            or (os.environ.get("ARCGIS_USE_PRO", "").strip().lower() in ("1", "true", "yes"))
        )
        if not has_creds and env_paths_tried:
            logger.warning("No ARCGIS_* credentials in environment; .env was tried at: %s", ", ".join(env_paths_tried))

    app = _create_server()
    # Stateless mode: no session tracking; clients like Trimble AI that don't send Mcp-Session-Id work reliably (token stored in _pending_arcgis_token).
    use_stateless = os.environ.get("ARCGIS_MCP_STATELESS", "").strip().lower() in ("1", "true", "yes")
    session_manager = StreamableHTTPSessionManager(app=app, json_response=False, stateless=use_stateless)
    if use_stateless:
        logger.info("MCP running in stateless mode (ARCGIS_MCP_STATELESS=1); token will be stored as pending for clients that do not send session id.")

    async def handle_streamable_http(scope: Scope, receive: Receive, send: Send) -> None:
        await session_manager.handle_request(scope, receive, send)

    def arcgis_token_middleware(app):
        """Set per-request ArcGIS token from X-ArcGIS-Token header or from session store (keyed by Mcp-Session-Id), or from pending (when client doesn't send session id)."""
        async def wrapper(scope: Scope, receive: Receive, send: Send) -> None:
            token, referer, session_id = None, None, None
            request_id = None
            if scope.get("type") == "http":
                headers = {k.decode("latin-1").lower(): v.decode("utf-8", errors="replace") for k, v in scope.get("headers", [])}
                session_id = headers.get("mcp-session-id", "").strip() or None
                request_id = headers.get("x-request-id", "").strip() or None
                token = headers.get("x-arcgis-token", "").strip() or None
                referer = headers.get("x-arcgis-referer", "").strip() or headers.get("referer", "").strip() or None
                if not token and session_id:
                    stored = _store.session_token_get(session_id=session_id)
                    if isinstance(stored, dict):
                        token = stored.get("token")
                        referer = referer or stored.get("referer")
                # If client doesn't send session id, attach pending token to this session when we see one; else use pending for this request.
                # Do not clear pending token so tool tasks (which may run in a different task) can still use it via _get_gis().
                pending = _store.pending_token_get()
                if session_id and isinstance(pending, dict):
                    _store.session_token_set(
                        session_id=session_id,
                        token=str(pending.get("token") or ""),
                        referer=str(pending.get("referer") or referer or "https://www.arcgis.com"),
                        ttl_seconds=_session_token_ttl_seconds(),
                    )
                    token = token or pending.get("token")
                    referer = referer or pending.get("referer")
                if not token and isinstance(pending, dict):
                    token = pending.get("token")
                    referer = referer or pending.get("referer")
            if not request_id:
                request_id = secrets.token_hex(8)
            tok_req = _request_id_var.set(request_id)
            tok_token = _current_session_id_var.set(session_id) if session_id else None
            tok_t = _arcgis_token_var.set(token) if token else None
            ref_t = _arcgis_referer_var.set(referer) if referer else None
            try:
                await app(scope, receive, send)
            finally:
                _request_id_var.reset(tok_req)
                if tok_token is not None:
                    _current_session_id_var.reset(tok_token)
                if tok_t is not None:
                    _arcgis_token_var.reset(tok_t)
                if ref_t is not None:
                    _arcgis_referer_var.reset(ref_t)
        return wrapper

    mcp_app = arcgis_token_middleware(handle_streamable_http)

    @contextlib.asynccontextmanager
    async def lifespan(starlette_app: Starlette) -> AsyncIterator[None]:
        async with session_manager.run():
            logger.info("ArcGIS MCP server started (streamable HTTP at /mcp)")
            try:
                yield
            finally:
                logger.info("Shutting down")

    async def health(_request):
        return PlainTextResponse("OK")

    async def ready(_request):
        """Readiness: portal/route reachable. Returns 200 if GIS can be created and portal is reachable, 503 otherwise."""
        try:
            gis = await anyio.to_thread.run_sync(_get_gis)
            if gis:
                await anyio.to_thread.run_sync(lambda: getattr(gis.properties, "portalName", None))
            return PlainTextResponse("OK")
        except Exception as e:
            logger.debug("readiness_check_failed", extra={"error": str(e)})
            return PlainTextResponse("Not ready", status_code=503)

    # OAuth login/callback for per-user auth (e.g. Okta SSO)
    async def auth_login(request: Request):
        client_id = os.environ.get("ARCGIS_CLIENT_ID", "").strip()
        if not client_id:
            return PlainTextResponse("ARCGIS_CLIENT_ID not set. Add OAuth credentials in ArcGIS and set in .env.", status_code=500)
        portal_url = os.environ.get("ARCGIS_URL", "").strip() or "https://www.arcgis.com"
        base = str(request.base_url).rstrip("/")
        redirect_uri = base + "/auth/callback"
        from urllib.parse import quote
        auth_url = (
            portal_url.rstrip("/") + "/sharing/rest/oauth2/authorize"
            + "?client_id=" + quote(client_id, safe="")
            + "&redirect_uri=" + quote(redirect_uri, safe="")
            + "&response_type=code"
            + "&expiration=20160"
        )
        return RedirectResponse(auth_url)

    async def auth_callback(request: Request):
        code = request.query_params.get("code")
        if not code:
            return PlainTextResponse("Missing code from ArcGIS redirect.", status_code=400)
        client_id = os.environ.get("ARCGIS_CLIENT_ID", "").strip()
        client_secret = os.environ.get("ARCGIS_CLIENT_SECRET", "").strip()
        portal_url = os.environ.get("ARCGIS_URL", "").strip() or "https://www.arcgis.com"
        base = str(request.base_url).rstrip("/")
        redirect_uri = base + "/auth/callback"
        token_url = portal_url.rstrip("/") + "/sharing/rest/oauth2/token"
        import urllib.request
        import urllib.error
        body = {
            "client_id": client_id,
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
        }
        if client_secret:
            body["client_secret"] = client_secret
        import urllib.parse as up
        data = up.urlencode(body).encode("utf-8")
        req = urllib.request.Request(token_url, data=data, method="POST", headers={"Content-Type": "application/x-www-form-urlencoded"})
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                out = json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            body = e.read().decode() if e.fp else ""
            logger.warning("OAuth token exchange failed: %s %s", e.code, body)
            return PlainTextResponse(f"Token exchange failed: {e.code} {body}", status_code=502)
        access_token = out.get("access_token")
        if not access_token:
            return PlainTextResponse("No access_token in response.", status_code=502)
        referer = portal_url
        # One-time code so the user can paste a short string in chat (avoids Azure/content filters blocking the long token)
        code = secrets.token_hex(4).lower()  # 8 chars, e.g. a1b2c3d4
        _store.one_time_code_set(code=code, token=access_token, referer=referer, ttl_seconds=_CODE_TTL_SECONDS)
        html = f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>ArcGIS signed in</title></head><body>
<h1>Signed in to ArcGIS</h1>
<p><strong>Tell the agent:</strong> <code>use ArcGIS code {code}</code></p>
<p><button onclick="navigator.clipboard.writeText('use ArcGIS code {code}')">Copy phrase</button></p>
<p><small>Or say: &quot;use code {code}&quot;. The agent will use <code>set_arcgis_token_with_code</code>. Code expires in 5 minutes.</small></p>
<p><small>You can close this window.</small></p>
</body></html>"""
        return HTMLResponse(html)

    def _decode_map_param(value: str | None) -> dict | list | None:
        """Decode optional base64url-encoded JSON query param for /map. Tolerates truncated endings (e.g. copy-paste)."""
        if not value or not value.strip():
            return None
        raw = urllib.parse.unquote(value.strip())
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

    async def map_viewer(request: Request):
        """Serve standalone map viewer at GET /map; state from query params (center, zoom, markers, route, layer)."""
        center = request.query_params.get("center")
        zoom_raw = request.query_params.get("zoom")
        markers_enc = request.query_params.get("markers")
        route_enc = request.query_params.get("route")
        basemap_q = request.query_params.get("basemap")
        view_q = request.query_params.get("view")
        layer_id = request.query_params.get("layer")
        layer_index_raw = request.query_params.get("layer_index")
        state: dict = {
            "schemaVersion": MAP_STATE_SCHEMA_VERSION,
            "center": None,
            "zoom": 10,
            "bbox": None,
            "markers": [],
            "routeGeometry": None,
            "routeSummary": None,
            "layer": None,
            "basemapId": DEFAULT_BASEMAP_ID,
        }
        if layer_id and (layer_id or "").strip() and not (layer_id or "").strip().startswith("lc_"):
            state["layer"] = {"item_id": (layer_id or "").strip(), "layer_index": 0}
            if layer_index_raw is not None and str(layer_index_raw).strip():
                try:
                    state["layer"]["layer_index"] = int(str(layer_index_raw).strip())
                except ValueError:
                    pass
        if center:
            parts = center.strip().split(",")
            if len(parts) >= 2:
                try:
                    lon, lat = float(parts[0].strip()), float(parts[1].strip())
                    state["center"] = {"longitude": lon, "latitude": lat}
                except ValueError:
                    pass
        if zoom_raw is not None:
            try:
                state["zoom"] = int(zoom_raw.strip())
            except ValueError:
                pass
        if basemap_q and basemap_q.strip():
            state["basemapId"] = basemap_q.strip()
        if view_q and view_q.strip().lower() in ("2d", "3d"):
            state["viewMode"] = view_q.strip().lower()
        decoded_markers = _decode_map_param(markers_enc)
        if isinstance(decoded_markers, list):
            state["markers"] = [m for m in decoded_markers if isinstance(m, dict) and "x" in m and "y" in m]
        # Fallback: if markers decode failed but we have center, show one marker so the map is not empty
        if not state["markers"] and state.get("center") and isinstance(state["center"], dict):
            lon = state["center"].get("longitude")
            lat = state["center"].get("latitude")
            if lon is not None and lat is not None:
                state["markers"] = [{"x": lon, "y": lat}]
        decoded_route = _decode_map_param(route_enc)
        if isinstance(decoded_route, dict):
            state["routeGeometry"] = _normalize_route_geometry(decoded_route) or decoded_route
        summary_min = request.query_params.get("summary_min")
        summary_mi = request.query_params.get("summary_mi")
        if summary_min is not None or summary_mi is not None:
            state["routeSummary"] = {}
            if summary_min is not None:
                try:
                    state["routeSummary"]["total_time_min"] = int(str(summary_min).strip())
                except ValueError:
                    pass
            if summary_mi is not None:
                try:
                    state["routeSummary"]["total_length_mi"] = float(str(summary_mi).strip())
                except ValueError:
                    pass
        html = _map_viewer_html(state)
        return HTMLResponse(html)

    async def map_viewer_short(request: Request):
        """Serve standalone map viewer at GET /map/s/{state_id} (short share link)."""
        state_id = (request.path_params.get("state_id") or "").strip()
        if not state_id:
            return PlainTextResponse("Missing state id", status_code=400)
        state = _get_map_state(state_id)
        if not state:
            return PlainTextResponse("Map state not found (expired?)", status_code=404)
        html = _map_viewer_html(state)
        return HTMLResponse(html)

    async def map_state_create(request: Request):
        """Create a short map state id from JSON (POST /map/state)."""
        try:
            payload = await request.json()
        except Exception:
            return JSONResponse({"error": "Invalid JSON body."}, status_code=400)
        if not isinstance(payload, dict):
            return JSONResponse({"error": "Body must be a JSON object (MapState)."}, status_code=400)
        try:
            state_id = _store_map_state(payload)
        except ValueError as e:
            return JSONResponse({"error": str(e), "hint": "Reduce state size (fewer features/graphics; avoid large styling payloads)."}, status_code=413)
        base = str(request.base_url).rstrip("/")
        return JSONResponse({"state_id": state_id, "url": base + "/map/s/" + urllib.parse.quote(state_id, safe="")})

    routes = [
        Route("/health", health),
        Route("/ready", ready),
        Route("/auth/login", auth_login),
        Route("/auth/callback", auth_callback),
        Route("/map/state", map_state_create, methods=["POST"]),
        Route("/map", map_viewer),
        Route("/map/s/{state_id}", map_viewer_short),
        Mount("/mcp", app=mcp_app),
    ]
    inner_app = Starlette(debug=False, routes=routes, lifespan=lifespan)
    # Rewrite /mcp -> /mcp/ so Mount receives it without 307 redirect (preserves POST body)
    async def redirect_slash_middleware(scope: Scope, receive: Receive, send: Send) -> None:
        if scope.get("type") == "http" and scope.get("path") == "/mcp":
            scope = dict(scope)
            scope["path"] = "/mcp/"
        await inner_app(scope, receive, send)
    starlette_app = CORSMiddleware(
        redirect_slash_middleware,
        allow_origins=["*"],
        allow_methods=["GET", "POST", "DELETE"],
        expose_headers=["Mcp-Session-Id"],
    )
    starlette_app = bearer_middleware(starlette_app)

    import uvicorn
    workers_raw = (os.environ.get("ARCGIS_WORKERS") or os.environ.get("UVICORN_WORKERS") or "").strip()
    try:
        workers = int(workers_raw) if workers_raw else 1
    except ValueError:
        workers = 1
    workers = max(1, min(workers, 16))
    # Multi-worker is safe only when shared state is externalized (Redis).
    if workers > 1 and not (os.environ.get("ARCGIS_REDIS_URL") or "").strip():
        logger.warning("ARCGIS_WORKERS=%s requested but ARCGIS_REDIS_URL is not set; forcing workers=1", workers)
        workers = 1
    uvicorn.run(starlette_app, host=host, port=port, workers=workers)


if __name__ == "__main__":
    main()
