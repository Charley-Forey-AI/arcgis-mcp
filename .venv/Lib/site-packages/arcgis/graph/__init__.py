"""
The ``arcgis.graph`` module contains classes and functions for working with ArcGIS Knowledge graphs. The available
functions allow for searching and querying the graph data, and viewing the data model of the graph database.

Knowledge graphs consist of entities and the relationships between them, each of which can contain properties which describe
their attributes. The data model of the knowledge graph can show you which entities, relationships, and properties are in
your database, along with other information about the graph. Performing a search or openCypher query on the graph will
return results from the database based on the search or query terms provided.

.. note::
    Applications based on ArcGIS API for Python version 2.0.1 can only communicate with knowledge graphs in an ArcGIS Enterprise
    10.9.1 or 11.0 deployment. ArcGIS Enterprise 11.1 includes breaking changes for knowledge graphs. Only applications based on
    ArcGIS API for Python version 2.1.0 or later will be able to communicate with knowledge graphs in an Enterprise 11.1 deployment.
    See the ArcGIS Enterprise Knowledge Server documentation for more details.
"""

from arcgis.graph._service import KnowledgeGraph

from arcgis.graph.data_model_types import (
    esriGraphNamedObjectRole,
    esriGraphPropertyRole,
    esriFieldType,
    esriGeometryType,
    FieldIndex,
    EntityType,
    EndPoint,
    RelationshipType,
    NamedObjectTypeMask,
    GraphProperty,
    GraphPropertyMask,
    ConstraintRule,
    TypeOfSet,
    SetOfNamedTypes,
    RelationshipExclusionRule,
    ConstraintRuleUpdate,
    ConstraintRuleMask,
    UpdateSetOfNamedTypes,
    RelationshipExclusionRuleUpdate,
    DatabaseNativeIdentifier,
    UniformPropertyIdentifier,
    IdentifierMappingInfo,
    esriUUIDMethodHint,
    IdentifierGenerationInfo,
    IdentifierInfo,
    SourceTypeValueBehavior,
    ProvenanceSourceTypeValues,
    GraphDataModel,
    esriGraphConstraintRuleRole,
)
from arcgis.graph.graph_types import (
    GraphObject,
    Entity,
    Relationship,
    Path,
    EntityDelete,
    RelationshipDelete,
    Transform,
)
from arcgis.graph.search_types import (
    SearchIndexProperties,
    esriNamedTypeCategory,
    SearchAnalyzer,
    SearchIndex,
)
from arcgis.graph.response_types import (
    Error,
    UpdateSearchIndexResponse,
    SyncDataModelResult,
    SyncDataModelResponse,
    NamedObjectTypeAddResult,
    NamedObjectTypeAddsResponse,
    NamedObjectTypeUpdateResponse,
    NamedObjectTypeDeleteResponse,
    PropertyAddResult,
    PropertyAddsResponse,
    PropertyUpdateResponse,
    PropertyDeleteResponse,
    PropertyAddResult,
    IndexAddsResponse,
    IndexDeleteResult,
    IndexDeletesResponse,
    ConstraintRuleAddResult,
    ConstraintRuleAddsResponse,
    ConstraintRuleUpdateResult,
    ConstraintRuleUpdatesResponse,
    ConstraintRuleDeleteResult,
    ConstraintRuleDeletesResponse,
    EditResult,
    EditResults,
    CascadingRelationshipDelete,
    RelationshipTypeSchemaChanges,
    CascadingProvenanceDelete,
    ApplyEditsResponse,
)
