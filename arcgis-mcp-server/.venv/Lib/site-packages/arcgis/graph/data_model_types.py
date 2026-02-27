from __future__ import annotations
from typing import Literal, Any, Annotated, Union, Optional
from enum import Enum
from pydantic import BaseModel, model_serializer, model_validator, Discriminator, Field
from pydantic.alias_generators import to_camel

from arcgis.graph.search_types import SearchIndex


esriGraphNamedObjectRole = Literal[
    "esriGraphNamedObjectRegular",
    "esriGraphNamedObjectProvenance",
    "esriGraphNamedObjectDocument",
]


esriGraphPropertyRole = Literal[
    "esriGraphPropertyUNSPECIFIED",
    "esriGraphPropertyRegular",
    "esriGraphPropertyDocumentName",
    "esriGraphPropertyDocumentTitle",
    "esriGraphPropertyDocumentUrl",
    "esriGraphPropertyDocumentText",
    "esriGraphPropertyDocumentKeywords",
    "esriGraphPropertyDocumentContentType",
    "esriGraphPropertyDocumentMetadata",
    "esriGraphPropertyDocumentFileExtension",
    "esriGraphPropertyProvenanceInstanceId",
    "esriGraphPropertyProvenanceSourceType",
    "esriGraphPropertyProvenanceSourceName",
    "esriGraphPropertyProvenanceSource",
    "esriGraphPropertyProvenanceComment",
    "esriGraphPropertyProvenanceTypeName",
    "esriGraphPropertyProvenancePropertyName",
]


esriFieldType = Literal[
    "esriFieldTypeSmallInteger",
    "esriFieldTypeInteger",
    "esriFieldTypeSingle",
    "esriFieldTypeDouble",
    "esriFieldTypeString",
    "esriFieldTypeDate",
    "esriFieldTypeDateOnly",
    "esriFieldTypeTimeOnly",
    "esriFieldTypeTimestampOffset",
    "esriFieldTypeOID",
    "esriFieldTypeGeometry",
    "esriFieldTypeBlob",
    "esriFieldTypeRaster",
    "esriFieldTypeGUID",
    "esriFieldTypeGlobalID",
    "esriFieldTypeXML",
    "esriFieldTypeBigInteger",
]


esriGeometryType = Literal[
    "esriGeometryPoint",
    "esriGeometryMultipoint",
    "esriGeometryPolyline",
    "esriGeometryPolygon",
    "esriGeometryEnvelope",
]


class GraphProperty(BaseModel):
    """
    Represents a property of an :class:`~~arcgis.graph.data_model_types.EntityType` or
    :class:`~~arcgis.graph.data_model_types.RelationshipType` in the Knowledge Graph.

    ==================     ===============================================================================================
    **Parameter**           **Description**
    ------------------     -----------------------------------------------------------------------------------------------
    name                    Required String. Name for the property. The default value is "".
    ------------------     -----------------------------------------------------------------------------------------------
    alias                   Optional String. Alias for the property. The default value is "".
    ------------------     -----------------------------------------------------------------------------------------------
    domain                  Optional String. Name of domain for the property. The default value is "".
    ------------------     -----------------------------------------------------------------------------------------------
    field_type              Optional `esriFieldType` string. The default value is "esriFieldTypeString".
    ------------------     -----------------------------------------------------------------------------------------------
    geometry_type           Optional `esriGeometryType` string. The default is None.
    ------------------     -----------------------------------------------------------------------------------------------
    has_z                   Optional Boolean. The default value is False.
    ------------------     -----------------------------------------------------------------------------------------------
    has_m                   Optional Boolean. The default value is False.
    ------------------     -----------------------------------------------------------------------------------------------
    default_value           Optional Any value. The default value is None.
    ------------------     -----------------------------------------------------------------------------------------------
    nullable                Optional Boolean. The default value is True.
    ------------------     -----------------------------------------------------------------------------------------------
    visible                 Optional Boolean. The default value is True.
    ------------------     -----------------------------------------------------------------------------------------------
    editable                Optional Boolean. The default value is True.
    ------------------     -----------------------------------------------------------------------------------------------
    required                Optional Boolean. The default value is False.
    ------------------     -----------------------------------------------------------------------------------------------
    role                    Optional `esriGraphPropertyRole` string. The default value is "esriGraphPropertyRegular".
    ==================     ===============================================================================================

    .. code-block:: python

        from arcgis.graph import GraphProperty

        # Example 1: simple string property
        GraphProperty(name="name", alias="name", required=True)

        # Example 2: geometry property
        GraphProperty(name="shape", field_type="esriFieldTypeGeoemtry", geometry_type="esriGeometryPolygon")

        # Example 3: get GraphProperty information from data model
        data_model = graph.query_data_model()
        data_model.entity_types['Person'].properties['name'].required # accesses the required property on the GraphProperty 'name'.

    """

    name: str = Field(..., description="The name of the property.")
    alias: str = Field(default="", description="The alias of the property.")
    domain: str = Field(default="", description="The domain of the property.")
    field_type: esriFieldType = Field(
        default="esriFieldTypeString", description="The type of the property."
    )
    geometry_type: Optional[esriGeometryType] = Field(
        default=None,
        description="The geometry type, or None if not a geometry property.",
    )
    has_z: bool = Field(default=False, description="Whether the geometry has Z values.")
    has_m: bool = Field(default=False, description="Whether the geometry has M values.")
    default_value: Any = Field(
        default=None,
        description="The default property value, or None if there is no default.",
    )
    nullable: bool = Field(
        default=True, description="Whether the property is nullable."
    )
    visible: bool = Field(default=True, description="Whether the property is visible.")
    editable: bool = Field(
        default=True, description="Whether the property is editable."
    )
    required: bool = Field(
        default=False, description="Whether the property is required."
    )
    is_system_maintained: bool = Field(
        default=False, description="Whether the property is system maintained."
    )
    role: esriGraphPropertyRole = Field(
        default="esriGraphPropertyRegular", description="The role of the property."
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        model: dict[str, Any] = {
            "name": self.name,
            "alias": self.alias,
            "domain": self.domain,
            "fieldType": self.field_type,
            "hasZ": self.has_z,
            "hasM": self.has_m,
            "defaultValue": self.default_value,
            "nullable": self.nullable,
            "visible": self.visible,
            "editable": self.editable,
            "required": self.required,
            "isSystemMaintained": self.is_system_maintained,
            "role": self.role,
        }
        if not self.geometry_type:
            return model
        model["geometryType"] = self.geometry_type
        return model

    @model_validator(mode="before")  # type: ignore
    @classmethod
    def validate_model(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        if (
            "geometry_type" in data
            and "field_type" in data
            and data["field_type"] != "esriFieldTypeGeometry"
        ):
            data.pop("geometry_type")
        return data

    class Config:
        alias_generator = to_camel
        populate_by_name = True


class GraphPropertyMask(BaseModel):
    """
    Allows users to define which settings should be updated for a :class:`~~arcgis.graph.data_model_types.GraphProperty`
    during a :meth:`~arcgis.graph.KnowledgeGraph.graph_property_update()`.

    =====================   ===============================================================================================
    **Parameter**           **Description**
    ---------------------   -----------------------------------------------------------------------------------------------
    update_name             Optional Boolean. The default value is False.
    ---------------------   -----------------------------------------------------------------------------------------------
    update_alias            Optional Boolean. The default value is False.
    ---------------------   -----------------------------------------------------------------------------------------------
    update_domain           Optional Boolean. The default value is False.
    ---------------------   -----------------------------------------------------------------------------------------------
    update_field_type       Optional Boolean. The default value is False.
    ---------------------   -----------------------------------------------------------------------------------------------
    update_geometry_type    Optional Boolean. The default value is False.
    ---------------------   -----------------------------------------------------------------------------------------------
    update_has_z            Optional Boolean. The default value is False.
    ---------------------   -----------------------------------------------------------------------------------------------
    update_has_m            Optional Boolean. The default value is False.
    ---------------------   -----------------------------------------------------------------------------------------------
    update_default_value    Optional Boolean. The default value is False.
    ---------------------   -----------------------------------------------------------------------------------------------
    update_nullable         Optional Boolean. The default value is False.
    ---------------------   -----------------------------------------------------------------------------------------------
    update_visible          Optional Boolean. The default value is False.
    ---------------------   -----------------------------------------------------------------------------------------------
    update_editable         Optional Boolean. The default value is False.
    ---------------------   -----------------------------------------------------------------------------------------------
    update_required         Optional Boolean. The default value is False.
    =====================   ===============================================================================================

    .. code-block:: python

        from arcgis.graph import GraphPropertyMask

        GraphPropertyMask(update_name=True, update_visible=True, update_editable=True)

    """

    update_name: bool = Field(
        default=False, description="Whether to update the property name."
    )
    update_alias: bool = Field(
        default=False, description="Whether to update the property alias."
    )
    update_field_type: bool = Field(
        default=False, description="Whether to update the property field type."
    )
    update_geometry_type: bool = Field(
        default=False, description="Whether to update the property geometry type."
    )
    update_has_m: bool = Field(
        default=False, description="Whether to update if the property has M values."
    )
    update_has_z: bool = Field(
        default=False, description="Whether to update if the property has Z values."
    )
    update_default_value: bool = Field(
        default=False, description="Whether to update the property default value."
    )
    update_nullable: bool = Field(
        default=False, description="Whether to update if the property is nullable."
    )
    update_editable: bool = Field(
        default=False, description="Whether to update if the property is editable."
    )
    update_visible: bool = Field(
        default=False, description="Whether to update if the property is visible."
    )
    update_required: bool = Field(
        default=False, description="Whether to update if the property is required."
    )
    update_domain: bool = Field(
        default=False, description="Whether to update the property domain."
    )


class FieldIndex(BaseModel):
    """
    Represents a field index to be used in
    :meth:`~arcgis.graph.KnowledgeGraph.graph_property_index_adds()`.

    ==================     ===============================================================================================
    **Parameter**           **Description**
    ------------------     -----------------------------------------------------------------------------------------------
    name                    Required String. Name for the field index.
    ------------------     -----------------------------------------------------------------------------------------------
    is_ascending            Required Boolean.
    ------------------     -----------------------------------------------------------------------------------------------
    is_unique               Required Boolean.
    ------------------     -----------------------------------------------------------------------------------------------
    fields                  Required List(String). List of field names for the index.
    ==================     ===============================================================================================

    .. code-block:: python

        from arcgis.graph import FieldIndex

        # Example 1: create a FieldIndex to use
        FieldIndex(name="name_index", is_ascending=True, is_unique=False, fields=["name"])

        # Example 2: access FieldIndex in the data model
        data_model = graph.query_data_model()
        data_model.entity_types["Person"].field_indexes['name_index'].fields

    """

    name: str = Field(..., description="The index name.")
    is_ascending: bool = Field(..., description="Whether the index is ascending.")
    is_unique: bool = Field(..., description="Whether the index is unique.")
    fields: list[str] = Field(
        ..., description="The list of fields participating in the index."
    )

    class Config:
        alias_generator = to_camel
        populate_by_name = True


class NamedObjectType(BaseModel):
    name: str = Field(..., description="The name of the entity or relationship type.")
    alias: str = Field(
        default="", description="The alias of the entity or relationship type."
    )
    role: esriGraphNamedObjectRole = Field(
        default="esriGraphNamedObjectRegular",
        description="The role of the entity or relationship type.",
    )
    strict: bool = Field(
        default=False,
        description="If True, the type can only be edited by admins or owners.",
    )
    properties: list[GraphProperty] = Field(
        default=[], description="The properties of the entity or relationship type."
    )
    field_indexes: list[FieldIndex] = Field(
        default=[], description="The indexes of the entity or relationship type."
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "alias": self.alias,
            "role": self.role,
            "strict": self.strict,
            "properties": {prop.name: prop for prop in self.properties},
            "field_indexes": {idx.name: idx for idx in self.field_indexes},
        }

    @model_validator(mode="before")  # type: ignore
    @classmethod
    def validate_model(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        if "properties" in data and isinstance(data["properties"], dict):
            data["properties"] = data["properties"].values()
        if "field_indexes" in data and isinstance(data["field_indexes"], dict):
            data["field_indexes"] = data["field_indexes"].values()
        return data


class EntityType(NamedObjectType):
    """
    Represents an entity named object type for a Knowledge Graph.

    ==================     ===============================================================================================
    **Parameter**           **Description**
    ------------------     -----------------------------------------------------------------------------------------------
    name                    Required String. Name for the Entity Type.
    ------------------     -----------------------------------------------------------------------------------------------
    alias                   Optional String. The default value is "".
    ------------------     -----------------------------------------------------------------------------------------------
    role                    Optional `esriGraphNamedObjectRole` String. The default value is "esriGraphNamedObjectRegular".
    ------------------     -----------------------------------------------------------------------------------------------
    strict                  Optional Boolean. The default value is False.
    ------------------     -----------------------------------------------------------------------------------------------
    properties              Optional Dict(String, :class:`~~arcgis.graph.data_model_types.GraphProperty`). The default value is {}.
    ------------------     -----------------------------------------------------------------------------------------------
    field_indexes           Optional Dict(String, :class:`~~arcgis.graph.data_model_types.FieldIndex`). The default value is {}.
    ==================     ===============================================================================================

    .. code-block:: python

        from arcgis.graph import EntityType

        # Example 1: Define an entity type
        EntityType(
            name="Person",
            properties={"name": GraphProperty(name="name")},
            field_indexes={"name_index": FieldIndex(name="name_index", is_ascending=True, is_unique=False, fields=["name"])}
        )

        # Example 2: Access information about an entity type from the data model
        data_model = graph.query_data_model()
        data_model.entity_types['Person'].properties

    """

    ...


class EndPoint(BaseModel):
    """
    Represents a set of origin and destination entity types for a relationship type

    ========================     ===============================================================================================
    **Parameter**                 **Description**
    ------------------------     -----------------------------------------------------------------------------------------------
    origin_enity_type             Required String. Name of origin entity type.
    ------------------------     -----------------------------------------------------------------------------------------------
    destination_entity_type       Required String. Name of destination entity type.
    ========================     ===============================================================================================

    .. code-block:: python

        from arcgis.graph import EndPoint

        EndPoint("Person", "Company")
    """

    origin_entity_type: str = Field(
        ..., description="The origin entity type for the relationship type."
    )
    destination_entity_type: str = Field(
        ..., description="The destination entity type for the relationship type."
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        return {
            "origin_entity_type": self.origin_entity_type,
            "dest_entity_type": self.destination_entity_type,
        }

    @model_validator(mode="before")  # type: ignore
    @classmethod
    def validate_model(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        if "dest_entity_type" in data:
            data["destination_entity_type"] = data.pop("dest_entity_type")
        return data


class RelationshipType(NamedObjectType):
    """
    Represents a relationship named object type for a Knowledge Graph.

    ==================     ===============================================================================================
    **Parameter**           **Description**
    ------------------     -----------------------------------------------------------------------------------------------
    name                    Required String. Name for the Entity Type.
    ------------------     -----------------------------------------------------------------------------------------------
    alias                   Optional String. The default value is "".
    ------------------     -----------------------------------------------------------------------------------------------
    role                    Optional `esriGraphNamedObjectRole` String. The default value is "esriGraphNamedObjectRegular".
    ------------------     -----------------------------------------------------------------------------------------------
    strict                  Optional Boolean. The default value is False.
    ------------------     -----------------------------------------------------------------------------------------------
    properties              Optional Dict(String, :class:`~arcgis.graph.data_model_types.GraphProperty`). The default value is {}.
    ------------------     -----------------------------------------------------------------------------------------------
    field_indexes           Optional Dict(String, :class:`~arcgis.graph.data_model_types.FieldIndex`). The default value is {}.
    ------------------     -----------------------------------------------------------------------------------------------
    end_points              Optional List[:class:`~arcgis.graph.data_model_types.EndPoint`]. The default value is [].
    ==================     ===============================================================================================

    .. code-block:: python

        from arcgis.graph import RelationshipType

        # Example 1: Define a relationship type
        RelationshipType(
            name="WorksAt",
            properties={"name": GraphProperty(name="name")},
            field_indexes={"name_index": FieldIndex(name="name_index", is_ascending=True, is_unique=False, fields=["name"])}
        )

        # Example 2: Access a relationship type from the data model
        data_model = graph.query_data_model()
        data_model.relationship_types['WorksAt'].properties

    """

    observed_end_points: list[EndPoint] = Field(
        default=[],
        description="The observed origin and destination entity type pairs in the database for the relationship type.",
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "alias": self.alias,
            "role": self.role,
            "strict": self.strict,
            "properties": {prop.name: prop for prop in self.properties},
            "field_indexes": {idx.name: idx for idx in self.field_indexes},
            "end_points": self.observed_end_points,
        }

    @model_validator(mode="before")  # type: ignore
    @classmethod
    def validate_model(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        if "properties" in data and isinstance(data["properties"], dict):
            data["properties"] = data["properties"].values()
        if "field_indexes" in data and isinstance(data["field_indexes"], dict):
            data["field_indexes"] = data["field_indexes"].values()
        if "end_points" in data:
            data["observed_end_points"] = data.pop("end_points")
        return data


class NamedObjectTypeMask(BaseModel):
    """
    Allows user to define what should be updated when performing a
    :meth:`~arcgis.graph.KnowledgeGraph.named_object_type_update()`.

    ==================     ===============================================================================================
    **Parameter**           **Description**
    ------------------     -----------------------------------------------------------------------------------------------
    update_name             Optional Boolean. The default value is False.
    ------------------     -----------------------------------------------------------------------------------------------
    update_alias            Optional Boolean. The default value is False.
    ------------------     -----------------------------------------------------------------------------------------------
    update_role             Optional Boolean. The default value is False.
    ------------------     -----------------------------------------------------------------------------------------------
    update_strict           Optional Boolean. The default value is False.
    ==================     ===============================================================================================

    .. code-block:: python

        from arcgis.graph import NamedObjectTypeMask

        NamedObjectTypeMask(update_name=True, update_alias=True, update_strict=True)

    """

    update_name: bool = Field(
        default=False,
        description="Whether to update the name in the entity or relationship type.",
    )
    update_alias: bool = Field(
        default=False,
        description="Whether to update the alias in the entity or relationship type.",
    )
    update_role: bool = Field(
        default=False,
        description="Whether to update the role in the entity or relationship type.",
    )
    update_strict: bool = Field(
        default=False,
        description="Whether to update if the entity or relationship type is strict.",
    )


esriGraphConstraintRuleRole = Literal[
    "esriGraphConstraintRuleRoleUNSPECIFIED",
    "esriGraphConstraintRuleRoleRegular",
    "esriGraphConstraintRuleRoleHasDocument",
    "esriGraphConstraintRuleRoleNoProvenanceOrigin",
    "esriGraphConstraintRuleRoleNoProvenanceDestination",
]


class ConstraintRule(BaseModel):
    """
    Represents an constraint rule to define how data can be created in
    the knowledge graph.

    ========================     ===============================================================================================
    **Parameter**                 **Description**
    ------------------------     -----------------------------------------------------------------------------------------------
    name                          Required String. Name for the exclusion rule.
    ------------------------     -----------------------------------------------------------------------------------------------
    alias                         Optional String. The default value is "".
    ------------------------     -----------------------------------------------------------------------------------------------
    disabled                      Optional Boolean. The default value is False.
    ------------------------     -----------------------------------------------------------------------------------------------
    role                          Optional `esriGraphConstraintRuleRole`. The default value is "esriGraphConstraintRuleRoleRegular"..
    ========================     ===============================================================================================

    .. code-block:: python

        from arcgis.graph import ConstraintRule

        # Example 1: Update a constraint rule
        person_company_constraint = ConstraintRule(name="PersonCanWorkForCompanyOrPark", alias="Person Can Work For Company or Park")
        graph.constraint_rule_updates(
            rules=[
                RelationshipExclusionRuleUpdate(
                    rule_name="OnlyPersonCanWorkForCompany",
                    mask=ConstraintRuleMask(update_name=True, update_alias=True),
                    constraint_rule=person_company_constraint
                )
            ],
            as_dict=False
        )

        # Example 2: Access an exclusion rule from the data model
        data_model = graph.query_data_model()
        data_model.constraint_rules

    """

    name: str = Field(..., description="The constraint rule name.")
    alias: str = Field(default="", description="The constraint rule alias.")
    disabled: bool = Field(
        default=False, description="Whether the constraint rule is disabled."
    )
    role: esriGraphConstraintRuleRole = Field(
        default="esriGraphConstraintRuleRoleRegular",
        description="The constraint rule role.",
    )
    type: Literal["esriGraphConstraintRuleTypeUNSPECIFIED"] = Field(
        default="esriGraphConstraintRuleTypeUNSPECIFIED",
        description="The constraint rule type.",
    )


class TypeOfSet(Enum):
    SET = 0
    SET_COMPLEMENT = 1


class SetOfNamedTypes(BaseModel):
    """
    Allows users to define the set of named types for a :class:`~arcgis.graph.data_model_types.RelationshipExclusionRule`.

    Defining a `set` will exclude the set of named type names from being created in the graph
    once the exclusion rule is applied.
    Defining a `set_complement` will exclude anything other than the set of named type names
    from being created in the graph once the exclusion rule is applied.

    ==================     ===============================================================================================
    **Parameter**           **Description**
    ------------------     -----------------------------------------------------------------------------------------------
    set                     Optional List of Strings. The default value is [].
    ------------------     -----------------------------------------------------------------------------------------------
    set_complement          Optional List of Strings. The default value is [].
    ==================     ===============================================================================================

    .. code-block:: python

        from arcgis.graph import SetOfNamedTypes

        # Example 1: set
        SetOfNamedTypes(set=["Person","Animal"])

        # Example 2: set_complement
        SetOfNamedTypes(set_complement=["Company"])

    """

    set: list[str] = Field(default=[], description="The set of types.")
    set_complement: list[str] = Field(
        default=[], description="The complement of the set of types."
    )

    def type_of_set(self) -> TypeOfSet:
        if self.set and not self.set_complement:
            return TypeOfSet.SET
        else:
            return TypeOfSet.SET_COMPLEMENT

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        match self.type_of_set():
            case TypeOfSet.SET:
                return {
                    "set": self.set,
                }
            case TypeOfSet.SET_COMPLEMENT:
                return {
                    "set_complement": self.set_complement,
                }
            case _:
                return {}


class RelationshipExclusionRule(ConstraintRule):
    """
    Represents an exclusion rule to define how relationships can be created in
    the knowledge graph between defined entity types.

    ========================     ===============================================================================================
    **Parameter**                 **Description**
    ------------------------     -----------------------------------------------------------------------------------------------
    name                          Required String. Name for the exclusion rule.
    ------------------------     -----------------------------------------------------------------------------------------------
    alias                         Optional String. The default value is "".
    ------------------------     -----------------------------------------------------------------------------------------------
    disabled                      Optional Boolean. The default value is False.
    ------------------------     -----------------------------------------------------------------------------------------------
    role                          Optional `esriGraphConstraintRuleRole`. The default value is "esriGraphConstraintRuleRoleRegular".
    ------------------------     -----------------------------------------------------------------------------------------------
    origin_entity_types           Required :class:`~SetOfNamedTypes`.
    ------------------------     -----------------------------------------------------------------------------------------------
    relationship_types            Required :class:`~SetOfNamedTypes`.
    ------------------------     -----------------------------------------------------------------------------------------------
    destination_entity_types      Required :class:`~SetOfNamedTypes`.
    ========================     ===============================================================================================

    .. code-block:: python

        from arcgis.graph import RelationshipExclusionRule, SetOfNamedTypes

        # Example 1: Define an exclusion rule
        RelationshipExclusionRule(
            name="OnlyPersonCanWorkAtCompany",
            origin_entity_types=SetOfNamedTypes(set_complement=["Person"]),
            relationship_types=SetOfNamedTypes(set=["WorksAt"]),
            destination_entity_types=SetOfNamedTypes(set=["Company"])
        )

        # Example 2: Access an exclusion rule from the data model
        data_model = graph.query_data_model()
        data_model.constraint_rules

    """

    type: Literal["esriGraphRelationshipExclusionRuleType"] = Field(default="esriGraphRelationshipExclusionRuleType", description="The constraint rule type.")  # type: ignore
    origin_entity_types: SetOfNamedTypes = Field(
        ..., description="The origin entity types in the rule."
    )
    relationship_types: SetOfNamedTypes = Field(
        ..., description="The relationship types in the rule."
    )
    destination_entity_types: SetOfNamedTypes = Field(
        ..., description="The destination entity types in the rule."
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "alias": self.alias,
            "disabled": self.disabled,
            "role": self.role,
            "type": self.type,
            "relationship_exclusion_rule": {
                "origin_entity_types": self.origin_entity_types.model_dump(),
                "relationship_types": self.relationship_types.model_dump(),
                "destination_entity_types": self.destination_entity_types.model_dump(),
            },
        }

    @model_validator(mode="before")  # type: ignore
    @classmethod
    def validate_model(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        if "relationship_exclusion_rule" in data:
            data["origin_entity_types"] = data["relationship_exclusion_rule"][
                "origin_entity_types"
            ]
            data["relationship_types"] = data["relationship_exclusion_rule"][
                "relationship_types"
            ]
            data["destination_entity_types"] = data["relationship_exclusion_rule"][
                "destination_entity_types"
            ]
            data.pop("relationship_exclusion_rule")
        return data


class ConstraintRuleMask(BaseModel):
    """
    Allows users to define which settings to update for a contraint rule

    ==================     ===============================================================================================
    **Parameter**           **Description**
    ------------------     -----------------------------------------------------------------------------------------------
    update_name             Optional Boolean. The default value is False.
    ------------------     -----------------------------------------------------------------------------------------------
    update_alias            Optional Boolean. The default value is False.
    ------------------     -----------------------------------------------------------------------------------------------
    update_disabled         Optional Boolean. The default value is False.
    ==================     ===============================================================================================

    .. code-block:: python

        from arcgis.graph import ConstraintRuleMask

        ConstraintRuleMask(update_name=True, update_alias=True, update_disabled=True)

    """

    update_name: bool = Field(
        default=False, description="Whether to update the name in the constraint rule."
    )
    update_alias: bool = Field(
        default=False, description="Whether to update the alias in the constraint rule."
    )
    update_disabled: bool = Field(
        default=False,
        description="Whether to update if the constraint rule is disabled.",
    )


class ConstraintRuleUpdate(BaseModel):
    rule_name: str = Field(
        ..., description="The name of the constraint rule to update."
    )
    mask: ConstraintRuleMask = Field(
        ...,
        description="The mask indicating the properties in the constraint rule to update.",
    )
    constraint_rule: ConstraintRule = Field(
        ...,
        description="The constraint rule properties to update, as determined by the mask.",
    )


class UpdateSetOfNamedTypes(BaseModel):
    """
    Allows a user to define named types to add or remove from a :class:`~arcgis.graph.data_model_types.RelationshipExclusionRule`.

    ==============================     ===============================================================================================
    **Parameter**                       **Description**
    ------------------------------     -----------------------------------------------------------------------------------------------
    add_named_types                     Required List of Strings. Names of types to add to rule.
    ------------------------------     -----------------------------------------------------------------------------------------------
    remove_named_types                  Required List of Strings. Names of types to remove from rule.
    ==============================     ===============================================================================================

    .. code-block:: python

        from arcgis.graph import UpdateSetOfNamedTypes

        UpdateSetOfNamedTypes(add_named_types=["Individual"], remove_named_types=["Person"])

    """

    add_named_types: list[str] = Field(
        ..., description="The named types to add to the set."
    )
    remove_named_types: list[str] = Field(
        ..., description="The named types to remove from the set."
    )


class RelationshipExclusionRuleUpdate(ConstraintRuleUpdate):
    """
    Allows a user to provide information for updating a relationship exclusion rule

    ===============================   ===============================================================================================
    **Parameter**                     **Description**
    -------------------------------   -----------------------------------------------------------------------------------------------
    rule_name                         Required String. Name of the constraint rule to update
    -------------------------------   -----------------------------------------------------------------------------------------------
    mask                              Required :class:`~arcgis.graph.data_model_types.ConstraintRuleMask`.
    -------------------------------   -----------------------------------------------------------------------------------------------
    constraint_rule                   Required :class:`~arcgis.graph.data_model_types.RelationshipExclusionRule`.
    -------------------------------   -----------------------------------------------------------------------------------------------
    update_origin_entity_types        Required :class:`~arcgis.graph.data_model_types.UpdateSetOfNamedTypes`.
    -------------------------------   -----------------------------------------------------------------------------------------------
    update_relationship_types         Required :class:`~arcgis.graph.data_model_types.UpdateSetOfNamedTypes`.
    -------------------------------   -----------------------------------------------------------------------------------------------
    update_destination_entity_types   Required :class:`~arcgis.graph.data_model_types.UpdateSetOfNamedTypes`.
    ===============================   ===============================================================================================

    .. code-block:: python

        from arcgis.graph import RelationshipExclusionRuleUpdate, RelationshipExclusionRule, ConstraintRuleMask

        RelationshipExclusionRuleUpdate(
            rule_name="OnlyPersonCanWorkForCompany",
            mask=ConstraintRuleMask(update_name=True, update_alias=True),
            constraint_rule=RelationshipExclusionRule(
                name="PersonCanOnlyWorkAtCompany",
                alais="Person Works At Company",
                origin_entity_types=SetOfNamedTypes(set_complement=["Person"]),
                relationship_types=SetOfNamedTypes(set=["WorksAt"]),
                destination_entity_types=SetOfNamedTypes(set=["Company"])
            ),
            update_origin_entity_types=UpdateSetOfNamedTypes(add_named_types=["Employee"],remove_named_types=[]),
            update_relationship_types=UpdateSetOfNamedTypes(add_named_types=["WorksFor"],remove_named_types=["WorksAt"]),
            update_destination_entity_types=UpdateSetOfNamedTypes(add_named_types=["Park"],remove_named_types=[])
        )

    """

    update_origin_entity_types: UpdateSetOfNamedTypes = Field(
        ..., description="Updates to the set of origin entity types."
    )
    update_relationship_types: UpdateSetOfNamedTypes = Field(
        ..., description="Updates to the set of relationship types."
    )
    update_destination_entity_types: UpdateSetOfNamedTypes = Field(
        ..., description="Updates to the set of destination entity types."
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        return {
            "rule_name": self.rule_name,
            "mask": self.mask,
            "constraint_rule": self.constraint_rule.model_dump(),
            "relationship_exclusion_rule_update": {
                "update_origin_entity_types": self.update_origin_entity_types.model_dump(),
                "update_relationship_types": self.update_relationship_types.model_dump(),
                "update_destination_entity_types": self.update_destination_entity_types.model_dump(),
            },
        }


class IdentifierMappingInfo(BaseModel):
    identifier_info_type: Literal["esriIdentifierInfoTypeUNSPECIFIED"] = Field(
        default="esriIdentifierInfoTypeUNSPECIFIED",
        description="The identifier info type.",
    )


class DatabaseNativeIdentifier(IdentifierMappingInfo):
    identifier_info_type: Literal["esriIdentifierInfoTypeDatabaseNative"] = Field(default="esriIdentifierInfoTypeDatabaseNative", description="The identifier info type.")  # type: ignore

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        if isinstance(self, UniformPropertyIdentifier):
            return self.ser_model()
        return {
            "identifier_info_type": self.identifier_info_type,
            "native_identifier": "",
        }


class UniformPropertyIdentifier(IdentifierMappingInfo):
    identifier_info_type: Literal["esriIdentifierInfoTypeUniformProperty"] = Field(default="esriIdentifierInfoTypeUniformProperty", description="The identifier info type.")  # type: ignore
    identifier_property_name: str = Field(
        ..., description="The identifier property name."
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        if isinstance(self, DatabaseNativeIdentifier):
            return self.ser_model()
        return {
            "identifier_info_type": self.identifier_info_type,
            "uniform_property": {
                "identifier_property_name": self.identifier_property_name
            },
        }

    @model_validator(mode="before")  # type: ignore
    @classmethod
    def validate_model(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        if "uniform_property" in data:
            data["identifier_property_name"] = data["uniform_property"][
                "identifier_property_name"
            ]
            data.pop("uniform_property")
        return data


esriUUIDMethodHint = Literal[
    "esriMethodHintUNSPECIFIED",
    "esriUUIDESRI",
    "esriUUIDRFC4122",
]


class IdentifierGenerationInfo(BaseModel):
    uuid_method_hint: esriUUIDMethodHint = Field(
        ..., description="Indicates the format in which UUIDs are generated."
    )


class IdentifierInfo(BaseModel):
    identifier_mapping_info: Annotated[
        Union[
            IdentifierMappingInfo, DatabaseNativeIdentifier, UniformPropertyIdentifier
        ],
        Discriminator(discriminator="identifier_info_type"),
    ] = Field(
        ...,
        description="Contains information about the unique identifier in the database.",
    )
    identifier_generation_info: IdentifierGenerationInfo = Field(
        ...,
        description="Contains information about the how unique identifier is generated.",
    )

    @model_validator(mode="before")  # type: ignore
    @classmethod
    def validate_model(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        if "identifier_mapping_info" in data:
            if isinstance(data["identifier_mapping_info"], dict):
                if "native_identifier" in data["identifier_mapping_info"]:
                    data["identifier_mapping_info"] = (
                        DatabaseNativeIdentifier.model_validate(
                            data["identifier_mapping_info"]
                        )
                    )
                elif "uniform_property" in data["identifier_mapping_info"]:
                    data["identifier_mapping_info"] = (
                        UniformPropertyIdentifier.model_validate(
                            data["identifier_mapping_info"]
                        )
                    )
        return data


class SourceTypeValueBehavior(BaseModel):
    behavior: str = Field(
        ..., description="Valid values are `String`, `URL` and `Document`."
    )
    value: str = Field(
        ...,
        description="User-defined value for valid source type in a provenance record.",
    )


class ProvenanceSourceTypeValues(BaseModel):
    value_behavior_array: list[SourceTypeValueBehavior] = Field(
        default=[], description="Valid source type values for provenance records."
    )


class GraphDataModel(BaseModel):
    """
    Allows users to access information about the knowledge graph's data model.

    .. code-block:: python

        from arcgis.gis import GIS
        from arcgis.graph import KnowledgeGraph, GraphDataModel

        graph = KnowledgeGraph("URL to Knowledge Graph Service", gis=GIS("home"))
        data_model = graph.query_data_model()

        # Access the timestamp the data model was last update. The response will be an integer timestamp.
        data_model.data_model_timestamp

        # Access the spatial reference. The response will be a dictionary containing the spatial reference information.
        data_model.spatial_reference

        # Access entity types. The response will be a dictionary of string names and EntityType objects.
        data_model.entity_types

        # Access relationship types. The response will be a dictionary of string names and RelationshipType objects.
        data_model.relationship_types

        # Access meta entity types (example: Provenance). The response will be a dictionary of string names and EntityType objects.
        data_model.meta_entity_types

        # Access the boolean value of whether the data model is strict
        data_model.strict

        # Access the object id property name. The response will be a string.
        data_model.objectid_property

        # Access the global id property name. The response will be a string.
        data_model.globalid_property

        # Access the boolean value of whether the knowledge graph is ArcGIS managed.
        data_model.arcgis_managed

        # Access the identifier info. The response will be a IdentifierInfo object.
        data_model.identifier_info

        # Access the search indexes. The response will be a list of SearchIndex objects.
        data_model.search_indexes

        # Access the provenance source type values
        data_model.provenance_source_type_values

        # Access the constraint rules
        data_model.constraint_rules

    """

    data_model_timestamp: int = Field(
        ..., description="The timestamp of the last change to the data model."
    )
    spatial_reference: dict[str, Any] = Field(
        ..., description="The spatial reference of the Knowledge Graph."
    )
    entity_types: list[EntityType] = Field(
        default=[], description="The list of entity types in the data model."
    )
    relationship_types: list[RelationshipType] = Field(
        default=[], description="The list of relationship types in the data model."
    )
    meta_entity_types: list[EntityType] = Field(
        default=[], description="The list of meta entity types in the data model."
    )
    strict: bool = Field(
        ...,
        description="If True, the data model can only be edited by admins or owners.",
    )
    objectid_property: str = Field(
        ..., description="The name of the ObjectID property."
    )
    globalid_property: str = Field(
        ..., description="The name of the GlobalID property."
    )
    arcgis_managed: bool = Field(
        ..., description="Whether the Knowledge Graph is ArcGIS managed."
    )
    identifier_info: IdentifierInfo = Field(
        ..., description="Contains information about the unique identifier."
    )
    search_indexes: list[SearchIndex] = Field(
        default=[], description="The list of search indexes."
    )
    provenance_source_type_values: ProvenanceSourceTypeValues = Field(
        default=ProvenanceSourceTypeValues(),
        description="The Provenance source type values.",
    )
    constraint_rules: list[
        Annotated[
            Union[ConstraintRule, RelationshipExclusionRule],
            Discriminator(discriminator="type"),
        ],
    ] = Field(default=[], description="The list of constraint rules.")

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        return {
            "data_model_timestamp": self.data_model_timestamp,
            "spatial_reference": self.spatial_reference,
            "entity_types": {
                entity_type.name: entity_type for entity_type in self.entity_types
            },
            "relationship_types": {
                relationship_type.name: relationship_type
                for relationship_type in self.relationship_types
            },
            "meta_entity_types": {
                meta_entity_type.name: meta_entity_type
                for meta_entity_type in self.meta_entity_types
            },
            "strict": self.strict,
            "objectid_property": self.objectid_property,
            "globalid_property": self.globalid_property,
            "arcgis_managed": self.arcgis_managed,
            "identifier_info": self.identifier_info,
            "search_indexes": {idx.name: idx for idx in self.search_indexes},
            "provenance_source_type_values": self.provenance_source_type_values,
            "constraint_rules": {rule.name: rule for rule in self.constraint_rules},
        }

    @model_validator(mode="before")  # type: ignore
    @classmethod
    def validate_model(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        if "entity_types" in data and isinstance(data["entity_types"], dict):
            data["entity_types"] = data["entity_types"].values()
        if "relationship_types" in data and isinstance(
            data["relationship_types"], dict
        ):
            data["relationship_types"] = data["relationship_types"].values()
        if "meta_entity_types" in data and isinstance(data["meta_entity_types"], dict):
            data["meta_entity_types"] = data["meta_entity_types"].values()
        if "search_indexes" in data and isinstance(data["search_indexes"], dict):
            data["search_indexes"] = data["search_indexes"].values()
        if "constraint_rules" in data and isinstance(data["constraint_rules"], dict):
            for value in data["constraint_rules"].values():
                if "relationship_exclusion_rule" in value:
                    value = RelationshipExclusionRule.model_validate(value)
            data["constraint_rules"] = data["constraint_rules"].values()
        return data
