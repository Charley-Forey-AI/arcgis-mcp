from __future__ import annotations
import copy
import json
from arcgis.auth.tools import LazyLoader
from arcgis.auth import EsriSession

_arcgis_gis = LazyLoader("arcgis.gis")
_arcgis_env = LazyLoader("arcgis")
from typing import Any
from functools import lru_cache

__all__ = ["translate"]


# -------------------------------------------------------------------------
@lru_cache(maxsize=100)
def _get_translation_url(gis: _arcgis_gis.GIS) -> str:
    """returns the translation url from the logged in GIS account"""
    # currently a place holder
    prop: dict = dict(gis.properties)
    help_services: dict = prop.get("helperServices", {})
    if "aiUtilityServices" in help_services:
        base_url: str = help_services.get("aiUtilityServices", None)
        translate_util_url: str = base_url.get("url")
        translate_url: str = base_url.get("translateUtility")
        return f"{translate_util_url}{translate_url}"
    return None


# -------------------------------------------------------------------------
@lru_cache(maxsize=255)
def _validate_gis(gis: _arcgis_gis.GIS) -> bool:
    """validates if the GIS object and user meet the criteria"""
    if gis is None:
        return (
            False,
            "The GIS object is None, please provide the GIS object.",
        )
    if gis._is_arcgisonline == False:
        return False, "The GIS object is not ArcGIS Online"
    elif gis.users.me is None:
        return False, "The GIS object is anonymous, please login."

    return True, ""


# -------------------------------------------------------------------------
def translate(
    text: str | list[str],
    to_language: list[str],
    from_language: str | None = "en-us",
    gis: _arcgis_gis.GIS | None = None,
) -> list[str]:
    """
    Translates Text Using the Esri's Translation Service

    ===============     ====================================================================
    **Parameter**        **Description**
    ---------------     --------------------------------------------------------------------
    text                list[str] or str. The text to translate.
    ---------------     --------------------------------------------------------------------
    to_language         list[str]. The list of languages to convert to.
    ---------------     --------------------------------------------------------------------
    from_language       str. The source language.  The default is US English.
    ---------------     --------------------------------------------------------------------
    gis                 Optional GIS. The connection to the organization that has AI assistants enabled.
    ===============     ====================================================================

    :return:

    Dictionary with the translations.

    """
    if gis is None and _arcgis_env.env.active_gis:
        gis = _arcgis_env.env.active_gis

    if gis.org_settings.get("aiAssistantsEnabled", False) == False:
        raise Exception(
            (
                "AI assistants is not available on this Portal."
                " Please contact your administrator."
            )
        )

    state, msg = _validate_gis(gis)
    if state == False:
        raise ValueError(msg)
    service_url: str = _get_translation_url(gis=gis)
    session: EsriSession = gis.session
    if isinstance(text, str):
        text = [{"key": "key:1", "text": text}]
    elif isinstance(text, (list, tuple)):
        text = [{"key": f"key:{idx}", "text": t} for idx, t in enumerate(text)]
    params: dict[str, Any] = {
        "f": "json",
        "token": gis._con.token,
        "to": ",".join(to_language),
        "from": from_language,
        "contents": json.dumps(text),
        "portalUrl": "https://www.arcgis.com",
        "translator": "esri",
    }

    headers = copy.copy(session.headers)
    headers["x-esri-request-source"] = "ArcGIS-API-For-Python"
    resp = session.post(
        url=service_url,
        data=params,
        headers=headers,
        allow_redirects=True,
    )
    resp.raise_for_status()
    return resp.json()
