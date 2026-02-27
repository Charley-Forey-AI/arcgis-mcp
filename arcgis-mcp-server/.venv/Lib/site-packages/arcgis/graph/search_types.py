from __future__ import annotations
from typing import Literal, Any
from pydantic import BaseModel, model_validator, model_serializer, Field


esriNamedTypeCategory = Literal[
    "unspecified",
    "both",
    "both_entity_relationship",
    "relationships",
    "entities",
    "meta_entity_provenance",
]


class SearchAnalyzer(BaseModel):
    name: str = Field(..., description="The search analyzer name.")


class SearchIndexProperties(BaseModel):
    """
    List of properties for search index

    ==============================     ========================================================================
    **Parameter**                       **Description**
    ------------------------------     ------------------------------------------------------------------------
    property_names                      Required List of Strings. Names of properties.
    ==============================     ========================================================================

    .. code-block:: python

        from arcgis.graph import SearchIndexProperties

        SearchIndexProperties(property_names=["name","comment"])

    """

    property_names: list[str] = Field(
        ..., description="The properties in the search index."
    )


class SearchIndex(BaseModel):
    """
    Allows full-text search capability on the graph for a set of properties for each entity or relationship type.
    Search indexes can be accessed in the :class:`~arcgis.graph.data_model_types.GraphDataModel`.

    .. code-block:: python

        data_model = graph.query_data_model()
        data_model.search_indexes

    """

    name: str = Field(..., description="The name of the search index.")
    supported_category: esriNamedTypeCategory = Field(
        ..., description="The supported category of the search index."
    )
    analyzers: list[SearchAnalyzer] = Field(
        ..., description="The list of search analyzers."
    )
    search_properties: dict[str, SearchIndexProperties] = Field(
        ..., description="The search properties, grouped by type name."
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        supported_category: str = "UNSPECIFIED"
        match self.supported_category:
            case "entities":
                supported_category = "Entity"
            case "relationships":
                supported_category = "Relationship"
            case "both":
                supported_category = "Both"
            case "both_entity_relationship":
                supported_category = "Both"
            case "meta_entity_provenance":
                supported_category = "MetaEntityProvenance"
            case _:
                pass
        return {
            "name": self.name,
            "supported_category": supported_category,
            "analyzers": [
                analyzer.model_dump(by_alias=True) for analyzer in self.analyzers
            ],
            "search_properties": {
                key: value.model_dump() for key, value in self.search_properties.items()
            },
        }

    @model_validator(mode="before")  # type: ignore
    @classmethod
    def validate_model(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        if "supported_category" in data:
            match data["supported_category"]:
                case "UNSPECIFIED":
                    data["supported_category"] = "unspecified"
                case "Entity":
                    data["supported_category"] = "entities"
                case "Relationship":
                    data["supported_category"] = "relationships"
                case "Both":
                    data["supported_category"] = "both_entity_relationship"
                case "MetaEntityProvenance":
                    data["supported_category"] = "meta_entity_provenance"
                case _:
                    pass
        return data
