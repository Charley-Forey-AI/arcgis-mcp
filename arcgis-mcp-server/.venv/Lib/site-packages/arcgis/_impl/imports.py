"""
Utils for importing modules cleanly and determining if they are available from optional namespace packages.
"""

from __future__ import annotations
from functools import lru_cache


@lru_cache(maxsize=255)
def get_arcgis_map_mod(raise_import_error_if_not_installed=False):
    """
    Returns the arcgis.map module from `arcgis-mapping` package, if installed.
    If not installed and raise_import_error_if_not_installed is True, raises ImportError.
    If not installed and raise_import_error_if_not_installed is False, returns None.
    """
    try:
        import arcgis.map as arcgismapping

        return arcgismapping
    except (ImportError, ModuleNotFoundError):
        if raise_import_error_if_not_installed:
            raise ImportError(
                "`arcgis-mapping` is not installed. Use `conda install -c esri arcgis-mapping` or `pip install arcgis-mapping` to install it."
            )
        return None


__all__ = [
    "get_arcgis_map_mod",
]
