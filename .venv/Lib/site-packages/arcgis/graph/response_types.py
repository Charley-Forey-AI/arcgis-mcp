from __future__ import annotations
from typing import Optional, Any
from pydantic import BaseModel, model_serializer, model_validator, Field
from pydantic.alias_generators import to_camel

from arcgis.graph.data_model_types import EndPoint


class Error(BaseModel):
    error_code: int = Field(..., description="The error code.")
    error_message: str = Field(..., description="The error message.")


class UpdateSearchIndexResponse(BaseModel):
    """
    Response for updating a :class:`~arcgis.graph.search_types.SearchIndex` using
    :meth:`~arcgis.graph.KnowledgeGraph.update_search_index()`.

    .. code-block:: python

        # Example of a successful response
        UpdateSearchIndexResponse(error=None)

        # Example of a response with errors
        UpdateSearchIndexResponse(
            error=Error(
                error_code=112093,
                error_message="The entity or relationship type, 'NotAType', does not exist."
            )
        )

    """

    error: Optional[Error] = Field(
        default=None, description="The error, or None if the operation was successful."
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        if self.error is None:
            return {}
        return {"error": self.error}


class SyncDataModelResult(BaseModel):
    type_name: str = Field(..., description="The type name that was synced.")
    error: Optional[Error] = Field(
        default=None, description="The error, or None if the operation was successful."
    )
    warnings: list[Error] = Field(
        default=[], description="The list of warnings returned for the named type."
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        if self.error is None and not self.warnings:
            return {"typeName": self.type_name}
        elif not self.warnings:
            return {
                "typeName": self.type_name,
                "error": self.error,
            }
        elif self.error is None:
            return {
                "typeName": self.type_name,
                "warnings": self.warnings,
            }
        return {
            "typeName": self.type_name,
            "error": self.error,
            "warnings": self.warnings,
        }

    class Config:
        alias_generator = to_camel
        populate_by_name = True


class SyncDataModelResponse(BaseModel):
    """
    Response for syncing the :class:`~arcgis.graph.data_model_types.GraphDataModel` using
    :meth:`~arcgis.graph.KnowledgeGraph.sync_data_model()`.

    .. code-block:: python

        # Example of a successful response
        SyncDataModelResponse(error=None)

        # Example of a response with errors
        SyncDataModelResponse(
            error=Error(
                error_code=113005,
                error_message="The service's graph data source does not support the operation."
            )
        )

    """

    error: Optional[Error] = Field(
        default=None, description="The error, or None if the operation was successful."
    )
    warnings: list[Error] = Field(
        default=[], description="The list of warnings returned for the operation."
    )
    named_type_sync_results: list[SyncDataModelResult] = Field(
        default=[], description="The list of results for each named type."
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        if (
            self.error is None
            and not self.warnings
            and not self.named_type_sync_results
        ):
            return {}
        elif not self.warnings and not self.named_type_sync_results:
            return {"error": self.error}
        elif self.error is None and not self.named_type_sync_results:
            return {"warnings": self.warnings}
        elif self.error is None and not self.warnings:
            return {"named_type_sync_results": self.named_type_sync_results}
        elif not self.named_type_sync_results:
            return {
                "error": self.error,
                "warnings": self.warnings,
            }
        elif not self.warnings:
            return {
                "error": self.error,
                "named_type_sync_results": self.named_type_sync_results,
            }
        elif self.error is None:
            return {
                "warnings": self.warnings,
                "named_type_sync_results": self.named_type_sync_results,
            }
        return {
            "error": self.error,
            "warnings": self.warnings,
            "named_type_sync_results": self.named_type_sync_results,
        }


class NamedObjectTypeAddResult(BaseModel):
    name: str = Field(..., description="The type name that was added.")
    error: Optional[Error] = Field(
        default=None, description="The error, or None if the operation was successful."
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        if self.error is None:
            return {"name": self.name}
        return {
            "name": self.name,
            "error": self.error,
        }


class NamedObjectTypeAddsResponse(BaseModel):
    """
    Response for adding a named object type to the graph.

    .. code-block:: python

        # Example of a successful response
        NamedObjectTypeAddsResponse(
            error=None,
            entity_add_results=[
                NamedObjectTypeAddResult(
                    name='Vehicle',
                    error=None
                )
            ],
            relationship_add_results=[
                NamedObjectTypeAddResult(
                    name='Drives',
                    error=None
                )
            ]
        )

        # Example of a response with errors
        NamedObjectTypeAddsResponse(
            error=None,
            entity_add_results=[
                NamedObjectTypeAddResult(
                    name='Vehicle',
                    error=Error(
                        error_code=112092,
                        error_message="The entity or relationship type, 'Vehicle', already exists, please provide a new type name."
                    )
                )
            ],
            relationship_add_results=[
                NamedObjectTypeAddResult(
                    name='Drives',
                    error=Error(
                        error_code=112092,
                        error_message="The entity or relationship type, 'Drives', already exists, please provide a new type name."
                    )
                )
            ]
        )

    """

    error: Optional[Error] = Field(
        default=None, description="The error, or None if the operation was successful."
    )
    entity_add_results: list[NamedObjectTypeAddResult] = Field(
        ..., description="The list of results for added entity types."
    )
    relationship_add_results: list[NamedObjectTypeAddResult] = Field(
        ..., description="The list of results for added relationship types."
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        if self.error is None:
            return {
                "entityAddResults": self.entity_add_results,
                "relationshipAddResults": self.relationship_add_results,
            }
        return {
            "error": self.error,
            "entityAddResults": self.entity_add_results,
            "relationshipAddResults": self.relationship_add_results,
        }

    class Config:
        alias_generator = to_camel
        populate_by_name = True


class NamedObjectTypeUpdateResponse(BaseModel):
    """
    Response for updating a named object type in the graph.

    .. code-block:: python

        # Example of a successful response
        NamedObjectTypeUpdateResponse(error=None)

        # Example of a response with errors
        NamedObjectTypeUpdateResponse(
            error=Error(
                error_code=112075,
                error_message='Updating the name of an entity or relationship type is not allowed.'
            )
        )

    """

    error: Optional[Error] = Field(
        default=None, description="The error, or None if the operation was successful."
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        if self.error is None:
            return {}
        return {"error": self.error}


class NamedObjectTypeDeleteResponse(BaseModel):
    """
    Response for deleting a named object type in the graph.

    .. code-block:: python

        # Example of a successful response
        NamedObjectTypeDeleteResponse(error=None)

        # Example of a response with errors
        NamedObjectTypeDeleteResponse(
            error=Error(
                error_code=112020,
                error_message="The entity or relationship type definition, 'Vehicle', was not found."
            )
        )

    """

    error: Optional[Error] = Field(
        default=None, description="The error, or None if the operation was successful."
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        if self.error is None:
            return {}
        return {"error": self.error}


class PropertyAddResult(BaseModel):
    name: str = Field(..., description="The name of the property that was added.")
    error: Optional[Error] = Field(
        default=None, description="The error, or None if the operation was successful."
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        if self.error is None:
            return {"name": self.name}
        return {
            "name": self.name,
            "error": self.error,
        }


class PropertyAddsResponse(BaseModel):
    """
    Response for adding a :class:`~arcgis.graph.data_model_types.GraphProperty` to an :class:`~arcgis.graph.data_model_types.EntityType` or
    :class:`~arcgis.graph.data_model_types.RelationshipType` in the graph.

    .. code-block:: python

        # Example of a successful response
        PropertyAddsResponse(
            error=None,
            property_add_results=[
                PropertyAddResult(
                    name='age',
                    error=None
                )
            ]
        )

        # Example of a response with errors
        PropertyAddsResponse(
            error=None,
            property_add_results=[
                PropertyAddResult(
                    name='age',
                    error=Error(
                        error_code=112043,
                        error_message="Graph property, 'age', already exists in data model."
                    )
                )
            ]
        )

    """

    error: Optional[Error] = Field(
        default=None, description="The error, or None if the operation was successful."
    )
    property_add_results: list[PropertyAddResult] = Field(
        ..., description="The list of results for the added properties."
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        if self.error is None:
            return {"propertyAddResults": self.property_add_results}
        return {
            "error": self.error,
            "propertyAddResults": self.property_add_results,
        }

    class Config:
        alias_generator = to_camel
        populate_by_name = True


class PropertyUpdateResponse(BaseModel):
    """
    Response for updating a :class:`~arcgis.graph.data_model_types.GraphProperty` in the graph.

    .. code-block:: python

        # Example of a successful response
        PropertyUpdateResponse(error=None)

        # Example of a response with errors
        PropertyUpdateResponse(error=Error(error_code=112068, error_message="Graph property, 'current_age', does not exist in data model."))

    """

    error: Optional[Error] = Field(
        default=None, description="The error, or None if the operation was successful."
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        if self.error is None:
            return {}
        return {"error": self.error}


class PropertyDeleteResponse(BaseModel):
    """
    Response for deleting a :class:`~arcgis.graph.data_model_types.GraphProperty` in the graph.

    .. code-block:: python

        # Example of a successful response
        PropertyDeleteResponse(error=None)

        # Example of a response with errors
        PropertyDeleteResponse(error=Error(error_code=112068, error_message="Graph property, 'age', does not exist in data model."))

    """

    error: Optional[Error] = Field(
        default=None, description="The error, or None if the operation was successful."
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        if self.error is None:
            return {}
        return {"error": self.error}


def _to_camel_plus_extra_space(snake: str) -> str:
    return to_camel(snake=snake).strip() + " "


class IndexAddResult(BaseModel):
    name: str = Field(..., description="The name of the added index.")
    error: Optional[Error] = Field(
        default=None, description="The error, or None if the operation was successful."
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        if self.error is None:
            return {"name": self.name}
        return {
            "name": self.name,
            "error": self.error,
        }


class IndexAddsResponse(BaseModel):
    """
    Response for adding a :class:`~arcgis.graph.data_model_types.FieldIndex` to a
    :class:`~arcgis.graph.data_model_types.GraphProperty`.

    .. code-block:: python

        # Example of a successful response
        IndexAddsResponse(
            error=None,
            index_add_results=[
                IndexAddResult(
                    name='name_index',
                    error=None
                )
            ]
        )

        # Example of a response with errors
        IndexAddsResponse(
            error=None,
            index_add_results=[
                IndexAddResult(
                    name='name_index',
                    error=Error(
                        error_code=112047,
                        error_message="Graph Index, 'name_index', already exists."
                    )
                )
            ]
        )

    """

    error: Optional[Error] = Field(
        default=None, description="The error, or None if the operation was successful."
    )
    index_add_results: list[IndexAddResult] = Field(
        ..., description="The list of results for the added indexes."
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        if self.error is None:
            return {"indexAddResults ": self.index_add_results}
        return {
            "error": self.error,
            "indexAddResults ": self.index_add_results,
        }

    class Config:
        alias_generator = _to_camel_plus_extra_space
        populate_by_name = True


class IndexDeleteResult(BaseModel):
    name: str = Field(..., description="The name of the deleted index.")
    error: Optional[Error] = Field(
        default=None, description="The error, or None if the operation was successful."
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        if self.error is None:
            return {"name": self.name}
        return {
            "name": self.name,
            "error": self.error,
        }


class IndexDeletesResponse(BaseModel):
    """
    Response for deleting a :class:`~arcgis.graph.data_model_types.FieldIndex` from a
    :class:`~arcgis.graph.data_model_types.GraphProperty`.

    .. code-block:: python

        # Example of a successful response
        IndexDeletesResponse(
            error=None,
            index_delete_results=[
                IndexDeleteResult(
                    name='name_index',
                    error=None
                )
            ]
        )

        # Example of a response with errors
        IndexDeletesResponse(
            error=None,
            index_delete_results=[
                IndexDeleteResult(
                    name='name_index',
                    error=Error(
                        error_code=112051,
                        error_message="Graph Index, 'name_index', does not exist in the data model."
                    )
                )
            ]
        )

    """

    error: Optional[Error] = Field(
        default=None, description="The error, or None if the operation was successful."
    )
    index_delete_results: list[IndexDeleteResult] = Field(
        ..., description="The list of results for the deleted indexes."
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        if self.error is None:
            return {"indexDeleteResults ": self.index_delete_results}
        return {
            "error": self.error,
            "indexDeleteResults ": self.index_delete_results,
        }

    class Config:
        alias_generator = _to_camel_plus_extra_space
        populate_by_name = True


class ConstraintRuleAddResult(BaseModel):
    name: str = Field(..., description="The name of the added constraint rule.")
    error: Optional[Error] = Field(
        default=None, description="The error, or None if the operation was successful."
    )
    warnings: list[Error] = Field(
        ..., description="The list of warnings from adding the constraint rule."
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        if self.error is None:
            return {
                "name": self.name,
                "warnings": self.warnings,
            }
        return {
            "name": self.name,
            "error": self.error,
            "warnings": self.warnings,
        }


class ConstraintRuleAddsResponse(BaseModel):
    """
    Response for adding a :class:`~arcgis.graph.data_model_types.RelationshipExclusionRule` constraint rule.

    .. code-block:: python

        # Example of successful add result
        ConstraintRuleAddsResponse(
            error=None,
            constraint_rule_add_results=[
                ConstraintRuleAddResult(
                    name='PersonCanOnlyWorkAtCompany',
                    error=None,
                    warnings=[]
                )
            ]
        )

        # Example of a response with errors
        ConstraintRuleAddsResponse(
            error=None,
            constraint_rule_add_results=[
                ConstraintRuleAddResult(
                    name='PersonCanOnlyWorkAtCompany',
                    error=Error(
                        error_code=112237,
                        error_message="Error adding the constraint rule, 'PersonCanOnlyWorkAtCompany', to the data model."
                    ),
                    warnings=[]
                )
            ]
        )

    """

    error: Optional[Error] = Field(
        default=None, description="The error, or None if the operation was successful."
    )
    constraint_rule_add_results: list[ConstraintRuleAddResult] = Field(
        ..., description="The list of results for the added constraint rules."
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        if self.error is None:
            return {"constraint_rule_add_results": self.constraint_rule_add_results}
        return {
            "error": self.error,
            "constraint_rule_add_results": self.constraint_rule_add_results,
        }


class ConstraintRuleUpdateResult(BaseModel):
    name: str = Field(..., description="The name of the updated constraint rule.")
    error: Optional[Error] = Field(
        default=None, description="The error, or None if the operation was successful."
    )
    warnings: list[Error] = Field(
        ..., description="The list of warnings for the updated constraint rules."
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        if self.error is None:
            return {
                "name": self.name,
                "warnings": self.warnings,
            }
        return {
            "name": self.name,
            "error": self.error,
            "warnings": self.warnings,
        }


class ConstraintRuleUpdatesResponse(BaseModel):
    """
    Response for updating a :class:`~arcgis.graph.data_model_types.RelationshipExclusionRule`.

    .. code-block:: python

        # Example of a response without errors
        ConstraintRuleUpdatesResponse(
            error=None,
            constraint_rule_update_results=[
                ConstraintRuleUpdateResult(
                    name='PersonCanOnlyWorkAtCompany',
                    error=None,
                    warnings=[]
                )
            ]
        )

        # Example of a response with errors
        ConstraintRuleUpdatesResponse(
            error=None,
            constraint_rule_update_results=[
                ConstraintRuleUpdateResult(
                    name='PersonCanOnlyWorkAtCompany',
                    error=None,
                    warnings=[
                        Error(
                            error_code=112225,
                            error_message="A relationship with origin entity type 'Employee', relationship type 'WorksFor', and destination entity type 'Park' is explicitly allowed by relationship exclusion rule 'PersonCanOnlyWorkAtCompany', but not allowed by relationship exclusion rule 'PersonCanOnlyWorkAtCompany3'."
                        ),
                        Error(
                            error_code=112244,
                            error_message='The following entity types do not exist in the data model: [Employee, Park].'
                        ),
                        Error(
                            error_code=112245,
                            error_message='The following relationship types do not exist in the data model: [WorksFor].'
                        )
                    ]
                )
            ]
        )

    """

    error: Optional[Error] = Field(
        default=None, description="The error, or None if the operation was successful."
    )
    constraint_rule_update_results: list[ConstraintRuleUpdateResult] = Field(
        ..., description="The list of results from updating the constraint rule."
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        if self.error is None:
            return {
                "constraint_rule_update_results": self.constraint_rule_update_results
            }
        return {
            "error": self.error,
            "constraint_rule_update_results": self.constraint_rule_update_results,
        }


class ConstraintRuleDeleteResult(BaseModel):
    name: str = Field(..., description="The name of the deleted constraint rule.")
    error: Optional[Error] = Field(
        default=None, description="The error, or None if the operation was successful."
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        if self.error is None:
            return {"name": self.name}
        return {
            "name": self.name,
            "error": self.error,
        }


class ConstraintRuleDeletesResponse(BaseModel):
    """
    Response for deleting a :class:`~arcgis.graph.data_model_types.RelationshipExclusionRule`.

    .. code-block:: python

        # Example of a response without errors
        ConstraintRuleDeletesResponse(
            error=None,
            constraint_rule_delete_results=[
                ConstraintRuleDeleteResult(name='PersonCanOnlyWorkAtCompany', error=None)
            ]
        )

        # Example of a response with errors
        ConstraintRuleDeletesResponse(
            error=None,
            constraint_rule_delete_results=[
                ConstraintRuleDeleteResult(
                    name='PersonCanOnlyWorkAtCompany2',
                    error=Error(
                        error_code=112242,
                        error_message="The constraint rule 'PersonCanOnlyWorkAtCompany2' with role 'REGULAR' does not exist in the data model."
                    )
                )
            ]
        )

    """

    error: Optional[Error] = Field(
        default=None, description="The error, or None if the operation was successful."
    )
    constraint_rule_delete_results: list[ConstraintRuleDeleteResult] = Field(
        ..., description="The list of results from deleting the constraint rules."
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        if self.error is None:
            return {
                "constraint_rule_delete_results": self.constraint_rule_delete_results
            }
        return {
            "error": self.error,
            "constraint_rule_delete_results": self.constraint_rule_delete_results,
        }


class EditResult(BaseModel):
    id: Any = Field(..., description="The ID of the edited entity or relationship.")
    error: Optional[Error] = Field(
        default=None, description="The error, or None if the operation was successful."
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        if self.error is None:
            return {"id": self.id}
        return {
            "error": self.error,
            "id": self.id,
        }


class EditResults(BaseModel):
    add_results: list[EditResult] = Field(
        default=[], description="The results from adding entities and relationships."
    )
    update_results: list[EditResult] = Field(
        default=[], description="The results from updating entities and relationships."
    )
    delete_results: list[EditResult] = Field(
        default=[], description="The results from deleting entities and relationships."
    )

    class Config:
        alias_generator = to_camel
        populate_by_name = True


class CascadingRelationshipDelete(BaseModel):
    id: Any = Field(
        ..., description="The ID of the relationship that was cascade deleted."
    )
    origin_id: Any = Field(
        ...,
        description="The origin entity ID for the relationship that was cascade deleted.",
    )
    destination_id: Any = Field(
        ...,
        description="The destination entity ID for the relationship that was cascade deleted.",
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "originId": self.origin_id,
            "destId": self.destination_id,
        }

    @model_validator(mode="before")  # type: ignore
    @classmethod
    def validate_model(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        if "destId" in data:
            data["destinationId"] = data.pop("destId")
        return data

    class Config:
        alias_generator = to_camel
        populate_by_name = True


class RelationshipTypeSchemaChanges(BaseModel):
    new_end_points: list[EndPoint] = Field(
        ..., description="The new end points in the database as a result of the edits."
    )


class CascadingProvenanceDelete(BaseModel):
    id: Any = Field(
        ..., description="The ID of the Provenance entity that was cascade deleted."
    )


class ApplyEditsResponse(BaseModel):
    """
    Response for applying edits to :class:`~arcgis.graph.graph_types.Entity`
    or :class:`~arcgis.graph.graph_types.Relationship` in the graph.

    .. code-block:: python

        # Example of a response without errors
        ApplyEditsResponse(
            error=None,
            edits_result={
                'Person': EditResults(
                    add_results=[EditResult(id=UUID('ab913bcb-1781-4137-9513-b3c942c23bc2'), error=None)],
                    update_results=[],
                    delete_results=[]
                ),
                'Company': EditResults(
                    add_results=[EditResult(id=UUID('26419c98-5521-4611-b2c2-196c35acc06d'), error=None)],
                    update_results=[],
                    delete_results=[]
                ),
                'WorksAt': EditResults(
                    add_results=[EditResult(id=UUID('1da2f6fc-e407-4561-97db-bba4745bd803'), error=None)],
                    update_results=[],
                    delete_results=[]
                )
            },
            cascaded_deletes={},
            relationship_schema_changes={},
            cascaded_provenance_deletes=[]
        )

        # Example of a response with errors
        ApplyEditsResponse(
            error=Error(
                error_code=111188,
                error_message="The destination identifier '{6F445050-0037-4308-8EE4-4B52C83763DC}' was not found."
            ),
            edits_result={},
            cascaded_deletes={},
            relationship_schema_changes={},
            cascaded_provenance_deletes=[]
        )

    """

    error: Optional[Error] = Field(
        default=None, description="The error, or None if the operation was successful."
    )
    edits_result: dict[str, EditResults] = Field(
        ..., description="The edit results, grouped by type name."
    )
    cascaded_deletes: dict[str, list[CascadingRelationshipDelete]] = Field(
        ..., description="The cascade deleted relationships, grouped by type name."
    )
    relationship_schema_changes: dict[str, RelationshipTypeSchemaChanges] = Field(
        ..., description="The relationship type schema changes, grouped by type name."
    )
    cascaded_provenance_deletes: list[CascadingProvenanceDelete] = Field(
        default=[], description="The cascade deleted Provenance entities."
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        if self.error is None:
            return {
                "editsResult": self.edits_result,
                "cascadedDeletes": self.cascaded_deletes,
                "relationshipSchemaChanges": self.relationship_schema_changes,
                "cascadedProvenanceDeletes": self.cascaded_provenance_deletes,
            }
        return {
            "error": self.error,
            "editsResult": self.edits_result,
            "cascadedDeletes": self.cascaded_deletes,
            "relationshipSchemaChanges": self.relationship_schema_changes,
            "cascadedProvenanceDeletes": self.cascaded_provenance_deletes,
        }

    class Config:
        alias_generator = to_camel
        populate_by_name = True
