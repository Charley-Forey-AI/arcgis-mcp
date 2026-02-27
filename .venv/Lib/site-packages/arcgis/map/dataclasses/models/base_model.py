from __future__ import annotations

from enum import Enum
from typing import Any
from pydantic import BaseModel as PydanticBaseModel
from pydantic import ConfigDict, field_validator, model_validator
from arcgis.auth.tools import LazyLoader

symbol = LazyLoader("arcgis.map.dataclasses.models.symbols")

common_config = ConfigDict(
    extra="ignore",
    populate_by_name=True,
    use_enum_values=True,
)


class BaseModel(PydanticBaseModel):
    @field_validator("id", mode="before", check_fields=False)
    @classmethod
    def check_id(cls, v):
        # ensure that the id is a string, else make it a str
        if not isinstance(v, str):
            return str(v)
        return v

    @field_validator("item_id", mode="before", check_fields=False)
    @classmethod
    def check_item_id(cls, v):
        # ensure that the id is a string, else make it a str
        if not isinstance(v, str):
            return str(v)
        return v

    @field_validator("style", mode="before", check_fields=False)
    @classmethod
    def check_style(cls, v):
        # ensure that for renderers the style is a string and not Enum instance
        if not isinstance(v, str):
            return v.value
        return v

    def _handle_enums(self, data):
        for key, value in data.items():
            if isinstance(value, Enum):
                data[key] = value.value
            elif isinstance(value, dict):
                data[key] = self._handle_enums(value)
        return data

    def dict(
        self,
        *,
        mode: str = "json",
        include=None,
        exclude=None,
        by_alias: bool = True,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = True,
        round_trip: bool = False,
        warnings: bool = False,
    ) -> dict[str, Any]:
        d = super().model_dump(
            mode=mode,
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
            round_trip=round_trip,
            warnings=warnings,
        )

        return self._handle_enums(d)


class SymbolValidatorMixin(BaseModel):
    model_config = common_config

    # Mapping dictionary for symbol types to classes

    @model_validator(mode="before")
    def validate_symbol_type(cls, values):
        if not isinstance(values, dict):
            values = dict(values)
        # if dataclass, we can skip whole thing
        if isinstance(values, BaseModel):
            return values

        symbol_mapping = {
            "esriSFS": symbol.SimpleFillSymbolEsriSFS,
            "esriSLS": symbol.SimpleLineSymbolEsriSLS,
            "esriSMS": symbol.SimpleMarkerSymbolEsriSMS,
            "esriPMS": symbol.PictureMarkerSymbolEsriPMS,
            "esriTS": symbol.TextSymbolEsriTS,
            "esriPFS": symbol.PictureFillSymbolEsriPFS,
            "LineSymbol3D": symbol.LineSymbol3D,
            "MeshSymbol3D": symbol.MeshSymbol3D,
            "PointSymbol3D": symbol.PointSymbol3D,
            "PolygonSymbol3D": symbol.PolygonSymbol3D,
        }
        symbol_data = values.get("symbol")
        if symbol_data and isinstance(symbol_data, dict):
            symbol_type = symbol_data.get("type")
            symbol_class = symbol_mapping.get(symbol_type)
            if symbol_class:
                values["symbol"] = symbol_class(**symbol_data)
        return values

    @model_validator(mode="before")
    def validate_default_symbol_type(cls, values):
        if not isinstance(values, dict):
            values = dict(values)
        # If dataclass, we can skip whole thing
        if isinstance(values, BaseModel):
            return values
        symbol_mapping = {
            "esriSFS": symbol.SimpleFillSymbolEsriSFS,
            "esriSLS": symbol.SimpleLineSymbolEsriSLS,
            "esriSMS": symbol.SimpleMarkerSymbolEsriSMS,
            "esriPMS": symbol.PictureMarkerSymbolEsriPMS,
            "esriTS": symbol.TextSymbolEsriTS,
            "esriPFS": symbol.PictureFillSymbolEsriPFS,
            "LineSymbol3D": symbol.LineSymbol3D,
            "MeshSymbol3D": symbol.MeshSymbol3D,
            "PointSymbol3D": symbol.PointSymbol3D,
            "PolygonSymbol3D": symbol.PolygonSymbol3D,
        }
        # Handle both instance and dict cases
        symbol_data = None
        if not isinstance(values, dict) and hasattr(values, "default_symbol"):
            symbol_data = values.default_symbol
        if isinstance(values, dict):
            symbol_data = values.get("default_symbol")

        if symbol_data and isinstance(symbol_data, dict):
            symbol_type = symbol_data.get("type")
            symbol_class = symbol_mapping.get(symbol_type)
            if symbol_class:
                values["default_symbol"] = symbol_class(**symbol_data)
        return values
