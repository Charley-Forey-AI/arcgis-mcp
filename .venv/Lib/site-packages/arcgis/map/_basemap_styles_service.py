from __future__ import annotations
from arcgis.auth.tools import LazyLoader
from enum import Enum
from arcgis._impl.common._utils import _lazy_property

_arcgis = LazyLoader("arcgis")


class BasemapStylesLanguage(Enum):
    GLOBAL = "global"
    LOCAL = "local"
    ARABIC = "ar"
    BOSNIAN = "bs"
    BULGARIAN = "bg"
    CATALAN = "ca"
    CHINESE_HONG_KONG = "zh-HK"
    CHINESE_SIMPLIFIED = "zh-CN"
    CHINESE_TAIWAN = "zh-TW"
    CROATIAN = "hr"
    CZECH = "cs"
    DANISH = "da"
    DUTCH = "nl"
    ENGLISH = "en"
    ESTONIAN = "et"
    FINNISH = "fi"
    FRENCH = "fr"
    GERMAN = "de"
    GREEK = "el"
    HEBREW = "he"
    HUNGARIAN = "hu"
    INDONESIAN = "id"
    ITALIAN = "it"
    JAPANESE = "ja"
    KOREAN = "ko"
    LATVIAN = "lv"
    LITHUANIAN = "lt"
    NORWEGIAN_BOKMAL = "nb"
    NORWEGIAN = "no"
    POLISH = "pl"
    PORTUGUESE_BRAZIL = "pt-BR"
    PORTUGUESE_PORTUGAL = "pt-PT"
    ROMANIAN = "ro"
    RUSSIAN = "ru"
    SERBIAN = "sr"
    SLOVAK = "sk"
    SLOVENIAN = "sl"
    SPANISH = "es"
    SWEDISH = "sv"
    THAI = "th"
    TURKISH = "tr"
    UKRAINIAN = "uk"
    VIETNAMESE = "vi"


class BasemapStylesPlace(Enum):
    ALL = "all"
    ATTRIBUTED = "attributed"
    NONE = "none"


class BasemapStylesWorldview(Enum):
    CHINA = "china"
    INDIA = "india"
    ISRAEL = "israel"
    JAPAN = "japan"
    MOROCCO = "morocco"
    PAKISTAN = "pakistan"
    SOUTH_KOREA = "southKorea"
    UNITED_ARAB_EMIRATES = "unitedArabEmirates"
    UNITED_STATES = "unitedStatesOfAmerica"
    VIETNAM = "vietnam"
    NONE = "none"


class BasemapStyle:
    """
    Represents a single basemap style, providing methods to retrieve and manage
    the style's properties.
    """

    def __init__(
        self,
        name: str,
        path: str,
        language: BasemapStylesLanguage | None = None,
        place: BasemapStylesPlace | None = None,
        worldview: BasemapStylesWorldview | None = None,
        gis=None,
    ) -> None:
        self._gis = gis if gis else _arcgis.env.active_gis
        self._session = self._gis._session
        self._name = name
        self._path = path
        self._language = language or BasemapStylesLanguage.GLOBAL
        self._place = place or BasemapStylesPlace.NONE
        self._worldview = worldview or BasemapStylesWorldview.NONE

    def __repr__(self) -> str:
        """
        Returns a string representation of the BasemapStyle object in an executable format.
        """
        return f"BasemapStyle(name={self._name!r})"

    def __str__(self) -> str:
        """
        Returns a string representation of the BasemapStyle object.
        """
        return f"BasemapStyle: {self._name}"

    @_lazy_property
    def name(self) -> str:
        """
        Returns the name of the basemap style.
        """
        return self._name

    @property
    def language(self) -> str:
        """
        Get or set the *language* of the
        :class:`basemap style <arcgis.map.BasemapStyle>`.

        =============   ================================================
        **Parameter**   **Description**
        -------------   ------------------------------------------------
        language        The :class:`~arcgis.map.BasemapStylesLanguage`
                        value representing the language of the basemap
                        style. If not set, the default behavior is to use
                        the Global Language code usually English.
        =============   ================================================

        .. code-block:: python

            # Usage Example: Set the Basemap style to new language
            # Changes visible when code is run in ArcGIS Notebook

            >>> from arcgis.gis import GIS
            >>> from arcgis.map import BasemapStylesLanguage

            >>> gis = GIS(profile="your_online_profile")

            >>> new_map = gis.map("Paris")
            >>> new_map.basemap.basemap
            >>> # Render map when in an ArcGIS Notebook
            >>> new_map

            >>> styles_service = new_map.basemap.basemap_styles_service

            >>> # List available style names
            >>> styles_service.styles_names

            ['ArcGIS Imagery',
               ...
             'ArcGIS Dark Gray']

            >>> adg_style = styles_service.get_style("ArcGIS Dark Gray")
            >>> adg_style.language = BasemapStylesLanguage.FRENCH

            >>> new_map.basemap.basemap = adg_style
            >>> # Change will be visible in notebook map widget

        """
        return self._language

    @language.setter
    def language(self, language: BasemapStylesLanguage) -> None:
        """
        Sets the language of the basemap style.
        """
        if not isinstance(language, BasemapStylesLanguage):
            raise ValueError(
                "Invalid language. Must be an instance of BasemapStylesLanguage."
            )
        self._language = language

    @property
    def place(self) -> str:
        """
        Get or set the *place* values returned with the
        :class:`basemap style <arcgis.map.BasemapStyle>`. See the
        `places <https://developers.arcgis.com/rest/basemap-styles/arcgis-navigation-style-get/#places>`_
        reference documentation for more information.

        =============   ================================================
        **Parameter**   **Description**
        -------------   ------------------------------------------------
        place           The :class:`~arcgis.map.BasemapStylesPlace` object
                        to use to set the place for thte basemap style.
                        If not set, it defaults to *BasemapStylesPlace.NONE.*
        =============   ================================================

        .. note::
            See code example for :class:`~arcgis.map.BasemapStylesLanguage`
            for coding pattern to set the attribute of the *style*.

        .. code-block:: python

            # Usage Example: Setting *place* attribute of a BasemapStyle

            >>> from arcgis.gis import GIS
            >>> from arcgis.map import BasemapStylesPlace

            >>> gis = GIS(profile="your_online_profile")
            >>> new_map = gis.map("<Location>")

            >>> styles_svc = new_map.basemap.basemap_styles_service
            >>> single_style = styles_svc.get_style("ArcGIS Imagery")

            >>> single_style.place = BasemapStylesPlace.ALL
        """
        return self._place

    @place.setter
    def place(self, place: BasemapStylesPlace) -> None:
        """
        Sets the place of the basemap style.
        """
        if not isinstance(place, BasemapStylesPlace):
            raise ValueError(
                "Invalid place. Must be an instance of BasemapStylesPlace."
            )
        self._place = place

    @property
    def worldview(self) -> str:
        """
        Returns the *worldview* of the
        :class:`basemap style <arcgis.map.BasemapStyle>`. See the
        `Boundary disputes and worldview <https://developers.arcgis.com/rest/basemap-styles/worldview/>`_
        documentation for conceptual information.

        =============   ================================================
        **Parameter**   **Description**
        -------------   ------------------------------------------------
        worldview       The :class:`~arcgis.map.BasemapStylesWorldview`
                        object to set for the basemap style. If not set,
                        it defaults to *BasemapStylesWorldview.NONE*.
                        This will display a standard set of boundary lines
                        and labels as defined by the style, rather than
                        the worldview of a single country.
        =============   ================================================

        .. note::
            See code example for :class:`~arcgis.map.BasemapStylesLanguage`
            for pattern to set the attribute of the style.
        """
        return self._worldview

    @worldview.setter
    def worldview(self, worldview: BasemapStylesWorldview) -> None:
        """
        Sets the worldview of the basemap style.
        """
        if not isinstance(worldview, BasemapStylesWorldview):
            raise ValueError(
                "Invalid worldview. Must be an instance of BasemapStylesWorldview."
            )
        self._worldview = worldview

    def _get_webmap_dict(self) -> dict:
        """
        Returns the webmap dictionary for the basemap style.
        """
        # Get url and params
        url = f"https://basemapstyles-api.arcgis.com/arcgis/rest/services/styles/v2/webmaps/{self._path}"
        params = {"f": "json"}

        # add parameters if they are not the default values
        if self._language != BasemapStylesLanguage.GLOBAL:
            params["language"] = self._language.value
        if self._place != BasemapStylesPlace.NONE:
            params["place"] = self._place.value
        if self._worldview != BasemapStylesWorldview.NONE:
            params["worldview"] = self._worldview.value

        # make the request to get the webmap dictionary
        resp = self._session.get(url, params=params)
        return resp.json()


class BasemapStylesService:
    """
    The BasemapStylesService class provides access to the basemap styles service.
    It allows users to retrieve and manage basemap styles for use in mapping applications.

    YOu can find information about the basemap styles service at: https://developers.arcgis.com/rest/basemap-styles/
    """

    def __init__(self, gis=None) -> None:
        self._gis = gis if gis else _arcgis.env.active_gis
        self._session = self._gis._session
        self._styles_name_to_path = self._construct_styles_name_to_path

    def __repr__(self) -> str:
        """
        Returns a string representation of the BasemapStylesService object.
        """
        return "BasemapStylesService()"

    def __str__(self) -> str:
        """
        Returns a string representation of the BasemapStylesService object.
        """
        return "BasemapStylesService"

    @_lazy_property
    def _construct_styles_name_to_path(self):
        """
        Constructs a dictionary of basemap style names to their respective paths.
        """
        url = "https://basemapstyles-api.arcgis.com/arcgis/rest/services/styles/v2/webmaps/self"
        params = {"f": "json"}
        resp = self._session.get(url, params=params)
        styles_dict = resp.json()["styles"]

        style_names = [
            style["name"] for style in styles_dict if not style.get("deprecated")
        ]
        paths = [style["path"] for style in styles_dict if not style.get("deprecated")]
        return dict(zip(style_names, paths))

    @_lazy_property
    def styles(self) -> list[BasemapStyle]:
        """
        Returns a list of available :class:`~arcgis.map.BasemapStyle`
        objects.
        """
        return [
            BasemapStyle(service_name, service_path, None, None, None, self._gis)
            for service_name, service_path in self._styles_name_to_path.items()
            if "/osm/" not in service_path
        ]

    @_lazy_property
    def styles_names(self) -> list[str]:
        """
        Returns a list of the names of the available
        :class:`basemap styles <arcgis.map.BasemapStyle>`.
        """
        return list(self._styles_name_to_path.keys())

    def get_style(
        self,
        name: str,
        language: BasemapStylesLanguage | None = None,
        place: BasemapStylesPlace | None = None,
        worldview: BasemapStylesWorldview | None = None,
    ) -> BasemapStyle:
        """
        Returns a :class:`~arcgis.map.BasemapStyle` object. This object can be
        used to specify the *language*, *place*, and *worldview* attributes
        for a *style*.

        .. note::
            **Must** be one of the names returned by the *styles_names*
            property of the :class:`~arcgis.map.BasemapStylesService`.

        =============   ================================================
        **Parameter**   **Description**
        -------------   ------------------------------------------------
        name            The name of the *basemap style* to retrieve. You can
                        find the available styles using the `styles_names`
                        property.
        -------------   ------------------------------------------------
        language        The :class:`~arcgis.map.BasemapStylesLanguage` object
                        representing the language of the basemap style. If
                        not set the default behavior is to use the Global
                        Language code, usually English. See
                        `languages <https://developers.arcgis.com/rest/basemap-styles/languages/>`_
                        for more information.
        -------------   ------------------------------------------------
        place           The :class:`~arcgis.map.BasemapStylesPlace` object
                        for the basemap style. If not set, it defaults to
                        BasemapStylesPlace.NONE. See
                        `places <https://developers.arcgis.com/rest/basemap-styles/arcgis-navigation-style-get/#places>`_
                        for more information.
        -------------   ------------------------------------------------
        worldview       The :class:`~arcgis.map.BasemapStylesWorldview`
                        of the basemap style. If not set, it defaults to
                        BasemapStylesWorldview.NONE. This will display a standard
                        set of boundary lines and labels as defined by the style,
                        rather than the specific view of a single country.
                        See `Boundary disputes and worldview <https://developers.arcgis.com/rest/basemap-styles/worldview/>`_
                        for more information.
        =============   ================================================
        """
        if name not in self._styles_name_to_path:
            raise ValueError(f"Basemap style '{name}' not found.")
        return BasemapStyle(
            name,
            self._styles_name_to_path[name],
            language=language,
            place=place,
            worldview=worldview,
            gis=self._gis,
        )
