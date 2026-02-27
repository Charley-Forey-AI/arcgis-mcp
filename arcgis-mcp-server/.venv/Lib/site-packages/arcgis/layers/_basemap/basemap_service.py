from __future__ import annotations
from functools import lru_cache
from arcgis.auth.tools import LazyLoader
from arcgis._impl.common._deprecate import deprecated

_arcgis = LazyLoader("arcgis")


@deprecated(
    deprecated_in="2.4.2",
    removed_in="2.5.0",
    details="Use `basemap_styles_service` property found in BasemapManager class in the `arcgis.map` package.",
)
class BasemapServices:
    """
    The basemap styles service is a ready-to-use location service that serves vector
    and image tiles that represent geographic features around the world. It includes
    styles that represent topographic features, road networks, footpaths, building
    footprints, water features, administrative boundaries, and satellite imagery.
    The styles are returned as JSON based on the Mapbox Style Specification or the ArcGIS
    Web Map Specification. The service also supports displaying localized language place
    labels, places, and worldviews. Custom basemap styles can also be created from the the default styles.

    When you are using a rendered Map instance, you can specify the basemap service to the basemap
    property to apply the style to the map.
    """

    def __init__(self, gis=None) -> None:
        self._gis = gis if gis else _arcgis.env.active_gis
        self._session = self._gis._session
        self._construct_styles_name_to_path

    @property
    def _construct_styles_name_to_path(self):
        """
        Constructs a dictionary of basemap style names to their respective paths.
        """
        url = "https://basemapstyles-api.arcgis.com/arcgis/rest/services/styles/v2/styles/self"
        params = {"f": "json"}
        resp = self._session.get(url, params=params)
        styles_dict = resp.json()["styles"]

        style_names = [style["name"] for style in styles_dict]
        paths = [style["path"] for style in styles_dict]
        self._styles_name_to_path = dict(zip(style_names, paths))

    @property
    def services(self) -> list[str]:
        """
        Returns a list of available basemap styles and their respective path.
        """
        return [
            BasemapService(service_name, service_path, self._gis)
            for service_name, service_path in self._styles_name_to_path.items()
        ]

    @property
    def languages(self):
        """
        Returns a list of supported languages for the basemap styles.
        To see which languages are supported by each service look at the documentation
        found here: https://developers.arcgis.com/rest/basemap-styles/
        """
        url = "https://basemapstyles-api.arcgis.com/arcgis/rest/services/styles/v2/styles/self"
        params = {"f": "json"}
        resp = self._session.get(url, params=params)
        return resp.json()["languages"]

    @property
    def places(self):
        """
        Returns a list of supported places for the basemap styles.
        To see which services support which places look at the documentation
        found here: https://developers.arcgis.com/rest/basemap-styles/
        """
        url = "https://basemapstyles-api.arcgis.com/arcgis/rest/services/styles/v2/styles/self"
        params = {"f": "json"}
        resp = self._session.get(url, params=params)
        return resp.json()["places"]

    @property
    def worldviews(self):
        """
        Returns a list of supported worldviews for the basemap styles.
        To see which services support which worldviews look at the documentation
        found here: https://developers.arcgis.com/rest/basemap-styles/
        """
        url = "https://basemapstyles-api.arcgis.com/arcgis/rest/services/styles/v2/styles/self"
        params = {"f": "json"}
        resp = self._session.get(url, params=params)
        return resp.json()["worldviews"]


@deprecated(
    deprecated_in="2.4.2",
    removed_in="2.5.0",
    details="Use `basemap_styles_service` property found in BasemapManager class in the `arcgis.map` package.",
)
class BasemapService:
    """
    Represents a basemap style service that is available for use in the basemap styles service.
    """

    def __init__(self, service_name: str, service_path: str, gis) -> None:
        self._session = gis._session
        self._service_name = service_name
        self._service_path = service_path
        self._style = self._get_style()

    def __repr__(self) -> str:
        return f"{self._service_name}"

    def _get_style(self) -> dict:
        """
        Returns the style JSON for the specified style name or path.
        """
        url = f"https://basemapstyles-api.arcgis.com/arcgis/rest/services/styles/v2/styles/{self._service_path}"
        params = {"f": "json"}
        resp = self._session.get(url, params=params)
        return resp.json()

    @property
    def style(self) -> dict:
        """
        Returns the style JSON for the specified style name or path.
        """
        return self._style
