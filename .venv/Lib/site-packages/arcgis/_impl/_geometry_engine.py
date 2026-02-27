from __future__ import annotations
import os
from enum import Enum
import logging as _logging
import sys, warnings


class ModuleWithWarnings(sys.__class__):
    """A module that issues a deprecation warning when the `SELECTED_ENGINE` attribute is accessed."""

    def __getattribute__(self, name):

        if name == "SELECTED_ENGINE":
            with warnings.catch_warnings():
                warnings.simplefilter("once")
                warnings.warn(
                    f"{name} is deprecated and will be removed in a future version. please use `SELECTED_IO_ENGINE` or 'SELECTED_GEOMETRY_ENGINE' instead.",
                    DeprecationWarning,
                )

        return super().__getattribute__(name)


class IOEngine(Enum):
    # Available geometry engines
    ARCPY = "arcpy"
    GDAL = "gdal"
    SHAPEFILE = "shapefile"


class GeometryEngine(Enum):
    # Available geometry engines
    ARCPY = "arcpy"
    SHAPELY = "shapely"


class GeometryEngineManager:
    """
    Manages detection and selection of a spatial geometry engine.

    Setting corresponding environment variable will prioritize your preferred engine.
    Environment variables:
    - ARCGIS_IO_ENGINE: for I/O operations (priority order: ARCPY > GDAL > SHAPEFILE)
    - ARCGIS_GEOMETRY_ENGINE(currently has no effect): for geometry operations (priority order: ARCPY > SHAPELY)
    """

    def __init__(self):
        """Initialize and select an engine."""
        self._detect_available_engines()
        self.io_engine = self._select_io_engine()  # Store the selected engine
        self.geometry_engine = (
            self._select_geometry_engine()
        )  # Store the selected engine

    def _detect_available_engines(self):
        """
        Check for installed spatial libraries.
        If we can import, we assume it is available.
        """
        self.available_io_engines = {
            IOEngine.ARCPY: self._is_installed("arcpy"),
            IOEngine.SHAPEFILE: self._is_installed("shapefile"),
            IOEngine.GDAL: self._is_installed("osgeo"),
        }

        self.available_geometry_engines = {
            GeometryEngine.ARCPY: self._is_installed("arcpy"),
            GeometryEngine.SHAPELY: self._is_installed("shapely"),
        }

    def _is_installed(self, module_name):
        """Check if a module is installed."""
        try:
            __import__(module_name)
            return True
        except ImportError:
            return False

    def _select_io_engine(self):
        """Select the best available engine for I/O operations, prioritizing user preference."""
        # Get the preferred engine from the environment variable ARCGIS_IO_ENGINE
        preferred_engine = os.getenv("ARCGIS_IO_ENGINE", "").lower()

        # Check if the preferred engine is in the list of available engines
        if preferred_engine in IOEngine._value2member_map_:
            selected_engine = IOEngine(preferred_engine)
            # If the preferred engine is available, return it
            if self.available_io_engines.get(selected_engine, False):
                return selected_engine  # Use user-specified engine if available

        # Default priority order: arcpy > gdal > shapefile
        # Iterate through the default engines and select the first available one
        for engine in [
            IOEngine.ARCPY,
            IOEngine.GDAL,
            IOEngine.SHAPEFILE,
        ]:
            if self.available_io_engines[engine]:
                return engine  # Return the first available engine from the default priority order

    def _select_geometry_engine(self):
        """Select the best available engine for geometry operations."""
        # Get the preferred engine from the environment variable ARCGIS_GEOMETRY_ENGINE
        preferred_engine = os.getenv("ARCGIS_GEOMETRY_ENGINE", "").lower()
        if len(preferred_engine) > 0:
            warnings.warn(
                "ARCGIS_GEOMETRY_ENGINE environment variable is set but currently has no effect. "
                "Geometry engine selection always follows the default priority order: ARCPY > SHAPELY. "
                "Previously, this variable was used to select the I/O engine, but now it's reserved for selecting geometry engine (not yet implemented). "
                "To select the I/O engine, use the ARCGIS_IO_ENGINE environment variable.",
                DeprecationWarning,
            )

        # Check if the preferred engine is in the list of available engines
        if preferred_engine in GeometryEngine._value2member_map_:
            selected_engine = GeometryEngine(preferred_engine)
            # If the preferred engine is available, return it
            if self.available_geometry_engines.get(selected_engine, False):
                return selected_engine  # Use user-specified engine if available

        # Default priority order: arcpy > shapely
        # Iterate through the default engines and select the first available one
        for engine in [
            GeometryEngine.ARCPY,
            GeometryEngine.SHAPELY,
        ]:
            if self.available_geometry_engines[engine]:
                return engine  # Return the first available engine from the default priority order


# Create a global instance so all modules can import it
ge = GeometryEngineManager()

SELECTED_IO_ENGINE = ge.io_engine
"""Selected I/O engine for IO operations. 
It can be one of the following:
- `IOEngine.ARCPY` 
- `IOEngine.GDAL`
- `IOEngine.SHAPEFILE`

Priority: ARCPY > GDAL > SHAPEFILE
"""

SELECTED_ENGINE = SELECTED_IO_ENGINE
"""Deprecated alias for `SELECTED_IO_ENGINE`."""

_SELECTED_GEOMETRY_ENGINE = ge.geometry_engine
""">Note: `ARCGIS_GEOMETRY_ENGINE` is currently NOT used in engine selection.

Designed for selecting geometry engine for geometry operations.
It can be one of the following:
- `GeometryEngine.ARCPY`
- `GeometryEngine.SHAPELY`

Priority: ARCPY > SHAPELY
"""

HAS_ARCPY = ge.available_io_engines[IOEngine.ARCPY]
HAS_PYSHP = ge.available_io_engines[IOEngine.SHAPEFILE]
HAS_SHAPELY = ge.available_geometry_engines[GeometryEngine.SHAPELY]
HAS_GDAL = ge.available_io_engines[IOEngine.GDAL]

sys.modules[__name__].__class__ = ModuleWithWarnings
