from __future__ import annotations
from typing import Optional, Any, Union
from pydantic import BaseModel, model_serializer, model_validator, Field
from pydantic.alias_generators import to_camel

from arcgis.geometry import Geometry


def _client_core_to_python_value(client_core_value: Any) -> Any:
    if isinstance(client_core_value, list):
        transformed_list: list[Any] = []
        for val in client_core_value:
            transformed_list.append(_client_core_to_python_value(client_core_value=val))
        return transformed_list
    if isinstance(client_core_value, dict):
        if "_objectType" not in client_core_value:
            return None
        match client_core_value["_objectType"]:
            case "geometry":
                client_core_value.pop("_objectType")
                return Geometry(client_core_value)
            case "object":
                return GraphObject.model_validate(client_core_value)
            case "entity":
                return Entity.model_validate(client_core_value)
            case "relationship":
                return Relationship.model_validate(client_core_value)
            case "path":
                return Path.model_validate(client_core_value)
            case _:
                return None
    return client_core_value


def _python_to_client_core_value(python_value: Any) -> Any:
    if isinstance(python_value, Geometry):
        copy_dict: dict[str, Any] = python_value.copy()
        copy_dict["_objectType"] = "geometry"
        return copy_dict
    if isinstance(python_value, BaseModel):
        return python_value.model_dump(by_alias=True)
    if isinstance(python_value, list):
        return [_python_to_client_core_value(val) for val in python_value]
    return python_value


class GraphObject(BaseModel):
    properties: dict[str, Any] = Field(
        default={},
        description="The property values, keyed by property name, in the graph object.",
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        return {
            "_objectType": "object",
            "_properties": {
                key: _python_to_client_core_value(value)
                for key, value in self.properties.items()
            },
        }

    @model_validator(mode="before")  # type: ignore
    @classmethod
    def validate_model(cls, data: Any) -> Any:
        if not isinstance(data, dict) or "_objectType" not in data:
            return data
        assert data["_objectType"] == "object"
        data.pop("_objectType")
        assert isinstance(data["_properties"], dict)
        for value in data["_properties"].values():
            value = _client_core_to_python_value(client_core_value=value)
        data["properties"] = data.pop("_properties")
        return data


class NamedObject(GraphObject):
    type_name: str = Field(..., description="The entity or relationship type name.")
    id: Optional[Any] = Field(
        default=None,
        description="The unique identifier for the entity or relationship.",
    )


class Entity(NamedObject):
    """
    Represents an entity instance in the knowledge graph

    ==============================     =======================================================================
    **Parameter**                       **Description**
    ------------------------------     -----------------------------------------------------------------------
    type_name                           Required String. Name of the :class:`~arcgis.graph.data_model_types.EntityType`
    ------------------------------     -----------------------------------------------------------------------
    id                                  Optional UUID. The default value is None. If not provided, an id will
                                        be assigned to the entity when it is created.
    ------------------------------     -----------------------------------------------------------------------
    properties                          Required Dictionary of Strings and Any values. String is the property
                                        name and Any value is the value for that property.
    ==============================     =======================================================================

    .. code-block:: python

        from arcgis.graph import Entity

        # Example 1: Define an entity
        Entity(
            type_name="Company",
            properties={"name":"Esri"}
        )

        # Example 2: Access an entity in a query response
        query_result = graph.query("MATCH (n) RETURN n")
        next(query_result)[0].properties

    """

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        model_dict: dict[str, Any] = {
            "_objectType": "entity",
            "_typeName": self.type_name,
            "_properties": {
                key: _python_to_client_core_value(value)
                for key, value in self.properties.items()
            },
        }
        if self.id is not None:
            model_dict["_id"] = self.id
        return model_dict

    @model_validator(mode="before")  # type: ignore
    @classmethod
    def validate_model(cls, data: Any) -> Any:
        if not isinstance(data, dict) or "_objectType" not in data:
            return data
        assert data["_objectType"] == "entity"
        data.pop("_objectType")
        data["type_name"] = data.pop("_typeName")
        assert isinstance(data["_properties"], dict)
        for value in data["_properties"].values():
            value = _client_core_to_python_value(client_core_value=value)
        data["properties"] = data.pop("_properties")
        if "_id" in data:
            data["id"] = data.pop("_id")
        return data


class Relationship(NamedObject):
    """
    Represents a relationship instance in the knowledge graph

    ==============================     =======================================================================
    **Parameter**                       **Description**
    ------------------------------     -----------------------------------------------------------------------
    type_name                           Required String. Name of the :class:`~arcgis.graph.data_model_types.EntityType`
    ------------------------------     -----------------------------------------------------------------------
    id                                  Optional UUID or String. The default value is None. If not provided, an id will
                                        be assigned to the entity when it is created.
    ------------------------------     -----------------------------------------------------------------------
    origin_entity_id                    Required UUID or String. The id of the origin :class:`~arcgis.graph.graph_types.Entity` in the graph.
    ------------------------------     -----------------------------------------------------------------------
    destiation_entity_id                Required UUID or String. The id of the destination :class:`~arcgis.graph.graph_types.Entity` in the graph.
    ------------------------------     -----------------------------------------------------------------------
    properties                          Optional Dictionary of Strings and Any values. String is the property
                                        name and Any value is the value for that property.
    ==============================     =======================================================================

    .. code-block:: python

        from arcgis.graph import Relationship
        from datetime import datetime
        from uuid import UUID

        # Example 1: Define a relationship
        Relationship(
            type_name="WorksAt",
            origin_entity_id=UUID("488bd414-3afd-4547-89aa-4adbbdac0a8d"),
            destination_entity_id=UUID("783bd422-3hfp-45c7-87aa-8adbbdac0a3d"),
            properties={"start_date":datetime.fromtimestamp(1578247200000)}
        )

        # Example 2: Access an relationship in a query response
        query_result = graph.query("MATCH ()-[n]-() RETURN n")
        next(query_result)[0].properties

    """

    origin_entity_id: Any = Field(
        ..., description="The unique identifier of the relationship's origin entity."
    )
    destination_entity_id: Any = Field(
        ...,
        description="The unique identifier of the relationship's destination entity.",
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        model_dict: dict[str, Any] = {
            "_objectType": "relationship",
            "_typeName": self.type_name,
            "_properties": {
                key: _python_to_client_core_value(value)
                for key, value in self.properties.items()
            },
            "_originEntityId": self.origin_entity_id,
            "_destinationEntityId": self.destination_entity_id,
        }
        if self.id is not None:
            model_dict["_id"] = self.id
        return model_dict

    @model_validator(mode="before")  # type: ignore
    @classmethod
    def validate_model(cls, data: Any) -> Any:
        if not isinstance(data, dict) or "_objectType" not in data:
            return data
        assert data["_objectType"] == "relationship"
        data.pop("_objectType")
        data["type_name"] = data.pop("_typeName")
        assert isinstance(data["_properties"], dict)
        for value in data["_properties"].values():
            value = _client_core_to_python_value(client_core_value=value)
        data["properties"] = data.pop("_properties")
        data["origin_entity_id"] = data.pop("_originEntityId")
        data["destination_entity_id"] = data.pop("_destinationEntityId")
        if "_id" in data:
            data["id"] = data.pop("_id")
        return data


class Path(BaseModel):
    """
    A list of :class:`~arcgis.graph.graph_types.Entity` and :class:`~arcgis.graph.graph_types.Relationship`
    objects required to traverse a graph from one entity to another.

    .. code-block:: python

        graph.query("MATCH path=()-[]-() RETURN path LIMIT 1")
        path = list(result)[0][0]

        path.path[0] # first entity in path
        path.path[1] # first relationship in path
        path.path[-1] # last entity in path

    """

    path: list[Union[Entity, Relationship]] = Field(
        ..., description="The list of entities and relationships in the path."
    )

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        return {
            "_objectType": "path",
            "_path": [named_object.model_dump() for named_object in self.path],
        }

    @model_validator(mode="before")  # type: ignore
    @classmethod
    def validate_model(cls, data: Any) -> Any:
        if not isinstance(data, dict) or "_objectType" not in data:
            return data
        assert data["_objectType"] == "path"
        data.pop("_objectType")
        path: Any = data.pop("_path")
        if isinstance(path, list):
            new_path: list[Union[Entity, Relationship]] = []
            for named_object in path:
                if isinstance(named_object, dict):
                    match named_object["_objectType"]:
                        case "entity":
                            new_path.append(Entity.model_validate(named_object))
                        case "relationship":
                            new_path.append(Relationship.model_validate(named_object))
                        case _:
                            raise ValueError(
                                "Path must contain only entities or relationships!"
                            )
                elif isinstance(named_object, (Entity, Relationship)):
                    new_path.append(named_object)
                else:
                    raise ValueError(
                        "Path must contain only entities or relationships!"
                    )
            path = new_path
        data["path"] = path
        return data


class NamedObjectDelete(BaseModel):
    type_name: str = Field(
        ..., description="The type name of the entity or relationship to delete."
    )
    ids: list[Any] = Field(
        ..., description="The IDs of the entities or relationships to delete."
    )


class EntityDelete(NamedObjectDelete):
    """
    Allows a user to define which entities to delete from a
    :class:`~arcgis.graph.data_model_types.EntityType`.

    ==============================     =======================================================================
    **Parameter**                       **Description**
    ------------------------------     -----------------------------------------------------------------------
    type_name                           Required String. Name of the :class:`~arcgis.graph.data_model_types.EntityType`
    ------------------------------     -----------------------------------------------------------------------
    ids                                 Required List of UUID or Strings. Ids of the entities to delete.
    ==============================     =======================================================================

    .. code-block:: python

        from arcgis.graph import EntityDelete

        # Example 1: Provide entity id values manually
        EntityDelete(type_name="Person", ids=[UUID("783bd422-3hfp-45c7-87aa-8adbbdac0a3d")])

        # Example 2: Delete from results of a query
        results = graph.query("MATCH (n:Person) WHERE n.name CONTAINS "delete" RETURN n.globalid")
        EntityDelete(type_name="Person", ids=list(results)[0])

    """

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        return {
            "_objectType": "entity",
            "_typeName": self.type_name,
            "_ids": self.ids,
        }

    @model_validator(mode="before")  # type: ignore
    @classmethod
    def validate_model(cls, data: Any) -> Any:
        if not isinstance(data, dict) or "_objectType" not in data:
            return data
        assert data["_objectType"] == "entity"
        data.pop("_objectType")
        data["type_name"] = data.pop("_typeName")
        data["ids"] = data.pop("_ids")
        return data


class RelationshipDelete(NamedObjectDelete):
    """
    Allows a user to define which relationships to delete from a :class:`~arcgis.graph.data_model_types.RelationshipType`.

    ==============================     =======================================================================
    **Parameter**                       **Description**
    ------------------------------     -----------------------------------------------------------------------
    type_name                           Required String. Name of the :class:`~arcgis.graph.data_model_types.RelationshipType`
    ------------------------------     -----------------------------------------------------------------------
    ids                                 Required List of UUID or Strings. Ids of the relationships to delete.
    ==============================     =======================================================================

    .. code-block:: python

        from arcgis.graph import RelationshipDelete

        # Example 1: Provide relationship id values manually
        RelationshipDelete(type_name="WorksAt", ids=[UUID("783bd422-3hfp-45c7-87aa-8adbbdac0a3d")])

        # Example 2: Delete from results of a query
        results = graph.query("MATCH ()-[n:WorksAt]-() WHERE n.name CONTAINS "delete" RETURN n.globalid")
        RelationshipDelete(type_name="WorksAt", ids=list(results)[0])

    """

    @model_serializer
    def ser_model(self) -> dict[str, Any]:
        return {
            "_objectType": "relationship",
            "_typeName": self.type_name,
            "_ids": self.ids,
        }

    @model_validator(mode="before")  # type: ignore
    @classmethod
    def validate_model(cls, data: Any) -> Any:
        if not isinstance(data, dict) or "_objectType" not in data:
            return data
        assert data["_objectType"] == "relationship"
        data.pop("_objectType")
        data["type_name"] = data.pop("_typeName")
        data["ids"] = data.pop("_ids")
        return data


class Transform(BaseModel):
    """
    Allows a user to specify custom quantization parameters for input geometry,
    which dictate how geometries are compressed and transferred to the server.

    ==============================     ==============================
    **Parameter**                       **Description**
    ------------------------------     ------------------------------
    xy_resolution                       Required float.
    ------------------------------     ------------------------------
    x_false_origin                      Required float.
    ------------------------------     ------------------------------
    y_false_origin                      Required float.
    ------------------------------     ------------------------------
    z_resolution                        Required float.
    ------------------------------     ------------------------------
    z_false_origin                      Required float.
    ------------------------------     ------------------------------
    m_resolution                        Required float.
    ------------------------------     ------------------------------
    m_false_origin                      Required float.
    ==============================     ==============================

    """

    xy_resolution: float = Field(..., description="The XY resolution.")
    x_false_origin: float = Field(..., description="The X false origin.")
    y_false_origin: float = Field(..., description="The Y false origin.")
    z_resolution: float = Field(..., description="The Z resolution.")
    z_false_origin: float = Field(..., description="The Z false origin.")
    m_resolution: float = Field(..., description="The M resolution.")
    m_false_origin: float = Field(..., description="The M false origin.")

    class Config:
        alias_generator = to_camel
        populate_by_name = True
