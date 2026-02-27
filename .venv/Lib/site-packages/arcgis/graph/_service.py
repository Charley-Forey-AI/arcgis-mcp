from __future__ import annotations
from arcgis.auth.tools import LazyLoader
from arcgis.geometry import Geometry
from arcgis.gis._impl._util import _get_item_url
from arcgis._impl.common._deprecate import deprecated

import warnings

try:
    import arcgis.graph._arcgisknowledge as _kgparser

    HAS_KG = True
except ImportError as e:
    HAS_KG = False
_isd = LazyLoader("arcgis._impl.common._isd")
from typing import List, Any, Sequence, Generator, Union, Optional
from arcgis.graph.data_model_types import (
    FieldIndex,
    EntityType,
    RelationshipType,
    NamedObjectTypeMask,
    GraphProperty,
    GraphPropertyMask,
    ConstraintRule,
    ConstraintRuleUpdate,
    GraphDataModel,
)
from arcgis.graph.graph_types import (
    Entity,
    Relationship,
    EntityDelete,
    RelationshipDelete,
    Transform,
    _client_core_to_python_value,
    _python_to_client_core_value,
)
from arcgis.graph.search_types import (
    SearchIndexProperties,
    esriNamedTypeCategory,
)
from arcgis.graph.response_types import (
    UpdateSearchIndexResponse,
    SyncDataModelResponse,
    NamedObjectTypeAddsResponse,
    NamedObjectTypeUpdateResponse,
    NamedObjectTypeDeleteResponse,
    PropertyAddsResponse,
    PropertyUpdateResponse,
    PropertyDeleteResponse,
    IndexAddsResponse,
    IndexDeletesResponse,
    ConstraintRuleAddsResponse,
    ConstraintRuleUpdatesResponse,
    ConstraintRuleDeletesResponse,
    ApplyEditsResponse,
)


AS_DICT_DEPRECATION_WARNING: str = (
    "In the future, the as_dict parameter will be removed, and the behavior will be as though as_dict is False. Setting as_dict to False is recommended."
)


class KnowledgeGraph:
    """
    Provides access to the Knowledge Graph service data model and properties, as well as
    methods to search and query the graph.

    ==================     ====================================================================
    **Parameter**           **Description**
    ------------------     --------------------------------------------------------------------
    url                    Knowledge Graph service URL
    ------------------     --------------------------------------------------------------------
    gis                    an authenticated :class:`~arcgis.gis.GIS` object.
    ==================     ====================================================================

    .. code-block:: python

        # Connect to a Knowledge Graph service:

        gis = GIS(url="url",username="username",password="password")

        knowledge_graph = KnowledgeGraph(url, gis=gis)

    """

    _gis = None
    _url = None
    _properties = None

    def __init__(self, url: str, *, gis=None):
        """initializer"""
        self._url = url
        self._gis = gis

    def _validate_import(self):
        if HAS_KG == False:
            raise ImportError(
                "An error occurred with importing the Knowledge Graph libraries. Please ensure you "
                "are using Python 3.9, 3.10 or 3.11 on Windows or Linux platforms."
            )

    def _getInputQuantParams(self, inputQuantParams: dict):
        clientCoreQuantParams = _kgparser.InputQuantizationParameters()
        clientCoreQuantParams.xy_resolution = inputQuantParams["xyResolution"]
        clientCoreQuantParams.x_false_origin = inputQuantParams["xFalseOrigin"]
        clientCoreQuantParams.y_false_origin = inputQuantParams["yFalseOrigin"]
        clientCoreQuantParams.z_resolution = inputQuantParams["zResolution"]
        clientCoreQuantParams.z_false_origin = inputQuantParams["zFalseOrigin"]
        clientCoreQuantParams.m_resolution = inputQuantParams["mResolution"]
        clientCoreQuantParams.m_false_origin = inputQuantParams["mFalseOrigin"]
        return clientCoreQuantParams

    @classmethod
    def fromitem(cls, item):
        """Returns the Knowledge Graph service from an Item"""
        if item.type != "Knowledge Graph":
            raise ValueError(
                "Invalid item type, please provide a 'Knowledge Graph' item."
            )
        if item._gis._use_private_url_only:
            url: str = _get_item_url(item=item)
        else:
            url: str = item.url
        return cls(url=url, gis=item._gis)

    @property
    def properties(self) -> _isd.InsensitiveDict:
        """Returns the properties of the Knowledge Graph service"""
        if self._properties is None:
            resp = self._gis._con.get(self._url, {"f": "json"})
            self._properties = _isd.InsensitiveDict(resp)
        return self._properties

    def _validate_response(self, response):
        if response.status_code != 200:
            response.raise_for_status()
        headers = response.headers
        if (
            "Content-Type" not in headers
            or headers["Content-Type"] != "application/x-protobuf"
        ):
            err_message = (
                "Improper response type from server. See error below.\n"
                + response.content.decode()
            )
            raise Exception(err_message)

    def search(
        self, search: str, category: str = "both", as_dict: bool = True
    ) -> List[Sequence[Any]]:
        """
        Allows for the searching of the properties of entities,
        relationships, or both in the graph using a full-text index.

        `Learn more about searching a knowledge graph <https://developers.arcgis.com/rest/services-reference/enterprise/kgs-graph-search.htm>`_

        ================    ===============================================================
        **Parameter**        **Description**
        ----------------    ---------------------------------------------------------------
        search              Required String. The search to perform on the Knowledge Graph.
        ----------------    ---------------------------------------------------------------
        category            Optional String.  The category is the location of the full
                            text search.  This can be isolated to either the `entities` or
                            the `relationships`.  The default is to look in `both`.

                            The allowed values are: both, entities, relationships,
                            both_entity_relationship, and meta_entity_provenance. Both and
                            both_entity_relationship are functionally the same.
        ----------------    ---------------------------------------------------------------
        as_dict             Optional Boolean. Determines whether the result is returned as
                            a dictionary or an object. The default is True. False is recommended.
        ================    ===============================================================

        .. note::
            Check the `service definition for the Knowledge Graph service <https://developers.arcgis.com/rest/services-reference/enterprise/kgs-hosted-server.htm>`_
            for valid values of category. Not all services support both and both_entity_relationship.

        .. code-block:: python

            #Perform a search on the knowledge graph
            for search_result in knowledge_graph.search("cat", as_dict=False):
                print(search_result)

            # Perform a search on only entities in the knowledge graph
            for searchentities_result in knowledge_graph.search("cat", "entities", as_dict=False):
            print(searchentities_result)

        :return: `Generator[Sequence[Any], None, None]`

        """
        if as_dict:
            warnings.warn(
                message=AS_DICT_DEPRECATION_WARNING,
                category=DeprecationWarning,
                stacklevel=2,
            )
        return list(self._search(search=search, category=category, as_dict=as_dict))

    def _search(
        self, search: str, category: str, as_dict: bool
    ) -> Generator[Sequence[Any], None, None]:
        url = self._url + "/graph/search"
        cat_lu = {
            "both": _kgparser.esriNamedTypeCategory.both,
            "both_entity_relationship": _kgparser.esriNamedTypeCategory.both_entity_relationship,
            "relationships": _kgparser.esriNamedTypeCategory.relationship,
            "entities": _kgparser.esriNamedTypeCategory.entity,
            "meta_entity_provenance": _kgparser.esriNamedTypeCategory.meta_entity_provenance,
        }
        assert str(category).lower() in cat_lu.keys()
        r_enc = _kgparser.GraphSearchRequestEncoder()
        r_enc.search_query = search
        r_enc.return_geometry = True
        r_enc.max_num_results = self.properties["maxRecordCount"]
        r_enc.type_category_filter = cat_lu[category.lower()]
        r_enc.encode()
        error = r_enc.get_encoding_result().error
        if error.error_code != 0:
            raise Exception(error.error_message)
        query_dec = _kgparser.GraphQueryDecoder()

        session = self._gis._con._session
        response = session.post(
            url=url,
            params={"f": "pbf"},
            data=r_enc.get_encoding_result().byte_buffer,
            stream=True,
            headers={"Content-Type": "application/octet-stream"},
        )

        self._validate_response(response)
        query_dec = _kgparser.GraphQueryDecoder()
        query_dec.data_model = self._datamodel
        for chunk in response.iter_content(8192):
            did_push = query_dec.push_buffer(chunk)
            while query_dec.next_row():
                row = query_dec.get_current_row()
                yield (
                    row
                    if as_dict
                    else _client_core_to_python_value(client_core_value=row)
                )
        if query_dec.has_error():
            raise Exception(query_dec.error.error_message)

    def update_search_index(
        self,
        adds: dict[str, Union[dict, SearchIndexProperties]] = {},
        deletes: dict[str, Union[dict, SearchIndexProperties]] = {},
        as_dict: bool = True,
    ) -> Union[dict, UpdateSearchIndexResponse]:
        """
        Allows users to add or delete :class:`~arcgis.graph.search_types.SearchIndexProperties` for different
        :class:`~arcgis.graph.data_model_types.EntityType` and :class:`~arcgis.graph.data_model_types.RelationshipType`
        from the :class:`~arcgis.graph.data_model_types.GraphDataModel`. Can only be existent properties for a given
        entity/relationship type.

        =========================   ===============================================================
        **Parameter**                **Description**
        -------------------------   ---------------------------------------------------------------
        adds                        Optional dict. See below for structure. The properties to add
                                    to the search index, specified by entity/relationship.
        -------------------------   ---------------------------------------------------------------
        deletes                     Optional dict. See below for structure. The properties to
                                    delete from the search index, specified by entity/relationship.
        -------------------------    ---------------------------------------------------------------
        as_dict                     Optional Boolean. Determines whether the result is returned as
                                    a dictionary or an object. The default is True. False is recommended.
        =========================   ===============================================================

        .. code-block:: python

            from arcgis.graph import SearchIndexProperties

            graph.update_search_index(
                adds={"Person": SearchIndexProperties(property_names=["name"])},
                as_dict=False
            )

        :return: :class:`~arcgis.graph.response_types.UpdateSearchIndexResponse`

        """
        if as_dict:
            warnings.warn(
                message=AS_DICT_DEPRECATION_WARNING,
                category=DeprecationWarning,
                stacklevel=2,
            )

        raw_adds: dict[str, Any] = {}
        for type_name, search_index_properties in adds.items():
            if isinstance(search_index_properties, SearchIndexProperties):
                raw_adds[type_name] = search_index_properties.model_dump(by_alias=True)
            else:
                warnings.warn(
                    message="Dictionary values of type dict for adds is deprecated. Please migrate to SearchIndexProperties.",
                    category=DeprecationWarning,
                    stacklevel=2,
                )
                raw_adds[type_name] = search_index_properties
        raw_deletes: dict[str, Any] = {}
        for type_name, search_index_properties in deletes.items():
            if isinstance(search_index_properties, SearchIndexProperties):
                raw_deletes[type_name] = search_index_properties.model_dump(
                    by_alias=True
                )
            else:
                warnings.warn(
                    message="Dictionary values of type dict for deletes is deprecated. Please migrate to SearchIndexProperties.",
                    category=DeprecationWarning,
                    stacklevel=2,
                )
                raw_deletes[type_name] = search_index_properties

        self._validate_import()
        url = self._url + "/dataModel/searchIndex/update"
        params = {
            "f": "pbf",
            "token": self._gis._con.token,
        }
        headers = {"Content-Type": "application/octet-stream"}

        enc = _kgparser.GraphUpdateSearchIndexRequestEncoder()
        if raw_adds:
            enc.insert_add_search_property(raw_adds)
        if raw_deletes:
            enc.insert_delete_search_property(raw_deletes)

        enc.encode()
        enc_result = enc.get_encoding_result()
        error = enc_result.error
        if error.error_code != 0:
            raise Exception(error.error_message)

        session = self._gis._con._session
        response = session.post(
            url=url,
            params=params,
            data=enc_result.byte_buffer,
            stream=True,
            headers=headers,
        )

        self._validate_response(response)
        content = response.content
        dec = _kgparser.GraphUpdateSearchIndexResponseDecoder()
        dec.decode(content)

        results = dec.get_results()
        return results if as_dict else UpdateSearchIndexResponse.model_validate(results)

    @deprecated(
        deprecated_in="2.4.1",
        removed_in="3.0.0",
        details="Use query_streaming instead.",
    )
    def query(self, query: str) -> List[dict]:
        """
        Queries the Knowledge Graph using openCypher

        `Learn more about querying a knowledge graph <https://developers.arcgis.com/rest/services-reference/enterprise/kgs-graph-query.htm>`_

        ================    ===============================================================
        **Parameter**        **Description**
        ----------------    ---------------------------------------------------------------
        query               Required String. Allows you to return the entities and
                            relationships in a graph, as well as the properties of those
                            entities and relationships, by providing an openCypher query.
        ================    ===============================================================

        .. code-block:: python

            # Perform an openCypher query on the knowledge graph
            query_result = knowledge_graph.query("MATCH path = (n)-[r]-(n2) RETURN path LIMIT 5")


        :return: `List[list]`

        """
        self._validate_import()
        url = f"{self._url}/graph/query"
        params = {
            "f": "pbf",
            "token": self._gis._con.token,
            "openCypherQuery": query,
        }

        data = self._gis._con.get(url, params, return_raw_response=True, try_json=False)
        self._validate_response(data)
        buffer_dm = data.content
        gqd = _kgparser.GraphQueryDecoder()
        gqd.push_buffer(buffer_dm)
        gqd.data_model = self._datamodel
        rows = []
        while gqd.next_row():
            r = gqd.get_current_row()
            rows.append(r)
        if gqd.has_error():
            if gqd.error.error_code == 111098:
                raise ValueError(gqd.error.error_message)
            else:
                raise RuntimeError(gqd.error.error_message)
        return rows

    @staticmethod
    def _convert_to_proper_representation(python_value: Any) -> Any:
        if isinstance(python_value, Geometry):
            copy_dict: dict[str, Any] = python_value.copy()
            copy_dict["_objectType"] = "geometry"
            return copy_dict
        if isinstance(python_value, dict):
            if "_objectType" in python_value:
                return python_value
            return {
                "_objectType": "object",
                "_properties": {
                    key: KnowledgeGraph._convert_to_proper_representation(value)
                    for key, value in python_value.items()
                },
            }
        if isinstance(python_value, list):
            return [
                KnowledgeGraph._convert_to_proper_representation(val)
                for val in python_value
            ]
        return python_value

    def query_streaming(
        self,
        query: str,
        input_transform: Optional[Union[dict[str, Any], Transform]] = None,
        bind_param: dict[str, Any] = {},
        include_provenance: bool = False,
        as_dict: bool = True,
    ) -> Generator[Sequence[Any], None, None]:
        """
        Query the graph using an openCypher query. Allows for more customization than the base
        :class:`~arcgis.graph.KnowledgeGraph.query()` function. Creates a generator of the query
        results, from which users can access each row or add them to a list.
        See below for example usage.

        ===================    ===============================================================
        **Parameter**           **Description**
        -------------------    ---------------------------------------------------------------
        query                  Required String. Allows you to return the entities and
                               relationships in a graph, as well as the properties of those
                               entities and relationships, by providing an openCypher query.
        -------------------    ---------------------------------------------------------------
        input_transform        Optional dict or Transform. Allows a user to specify custom quantization
                               parameters for input geometry, which dictate how geometries are
                               compressed and transferred to the server. Defaults to lossless
                               WGS84 quantization.
        -------------------    ---------------------------------------------------------------
        bind_param             Optional dict. The bind parameters used to filter
                               query results. Key of each pair is the string name for it,
                               which is how the parameter can be referenced in the query. The
                               value can be any "primitive" type value that may be found as
                               an attribute of an entity or relationship (e.g., string,
                               double, boolean, etc.), a list, an anonymous object (a dict),
                               or a geometry.

                               Anonymous objects and geometries can be passed
                               in as either their normal Python forms, or following the
                               format found in Knowledge Graph entries (containing an
                               "_objectType" key, and "_properties" for anonymous objects).

                               Note: Including bind parameters not used in the query will
                               cause queries to yield nothing on ArangoDB based services,
                               while Neo4j based services will still produce results.
        -------------------    ---------------------------------------------------------------
        include_provenance     Optional boolean. When `True`, provenance entities (metadata)
                               will be included in the query results. Defaults to `False`.
        ===================    ===============================================================

        .. code-block:: python

            # Get a list of all query results
            query_gen = knowledge_graph.query_streaming("MATCH path = (n)-[r]-(n2) RETURN path LIMIT 5")
            results = list(gen)

            # Grab one result at a time
            query_gen = knowledge_graph.query_streaming("MATCH path = (n)-[r]-(n2) RETURN path LIMIT 5")
            first_result = next(query_gen)
            second_result = next(query_gen)

        :return: `Generator[Sequence[Any], None, None]`
        """
        if as_dict:
            warnings.warn(
                message=AS_DICT_DEPRECATION_WARNING,
                category=DeprecationWarning,
                stacklevel=2,
            )

        raw_input_transform: Optional[dict[str, Any]] = None
        if isinstance(input_transform, Transform):
            raw_input_transform = input_transform.model_dump(by_alias=True)
        elif input_transform:
            warnings.warn(
                message="Input transform of type dict is deprecated. Please migrate to Transform.",
                category=DeprecationWarning,
                stacklevel=2,
            )
            raw_input_transform = input_transform
        raw_bind_param: dict[str, Any] = {
            key: _python_to_client_core_value(value)
            for key, value in bind_param.items()
        }

        self._validate_import()
        url = f"{self._url}/graph/query"
        params = {
            "f": "pbf",
            "token": self._gis._con.token,
        }
        headers = {"Content-Type": "application/octet-stream"}

        # initialize encoder
        r_enc = _kgparser.GraphQueryRequestEncoder()
        r_enc.open_cypher_query = query

        # set quant params
        if raw_input_transform:
            quant_params = self._getInputQuantParams(raw_input_transform)
        else:
            quant_params = _kgparser.InputQuantizationParameters.WGS84_lossless()
        r_enc.input_quantization_parameters = quant_params

        # set bind parameters
        if raw_bind_param:
            for k, v in raw_bind_param.items():
                converted: Any = KnowledgeGraph._convert_to_proper_representation(v)
                if isinstance(converted, (dict, list)):
                    r_enc.set_param_key_value(k, _kgparser.from_value_object(converted))
                else:
                    r_enc.set_param_key_value(k, v)

        # set provenance behavior
        if include_provenance == True:
            r_enc.provenance_behavior = _kgparser.ProvenanceBehavior.include
        else:
            r_enc.provenance_behavior = _kgparser.ProvenanceBehavior.exclude

        r_enc.encode()
        error = r_enc.get_encoding_result().error
        if error.error_code != 0:
            raise Exception(error.error_message)
        query_dec = _kgparser.GraphQueryDecoder()

        session = self._gis._con._session
        response = session.post(
            url=url,
            params=params,
            data=r_enc.get_encoding_result().byte_buffer,
            stream=True,
            headers=headers,
        )

        self._validate_response(response)

        for chunk in response.iter_content(8192):
            did_push = query_dec.push_buffer(chunk)
            while query_dec.next_row():
                row = query_dec.get_current_row()
                yield (
                    row
                    if as_dict
                    else _client_core_to_python_value(client_core_value=row)
                )
            if query_dec.has_error():
                if query_dec.error.error_code == 111098:
                    raise ValueError(query_dec.error.error_message)
                else:
                    raise RuntimeError(query_dec.error.error_message)

    @property
    def _datamodel(self) -> object:
        """
        Returns the datamodel for the Knowledge Graph service
        """
        self._validate_import()
        url = f"{self._url}/dataModel/queryDataModel"
        params = {
            "f": "pbf",
        }
        r_dm = self._gis._con.get(
            url, params=params, return_raw_response=True, try_json=False
        )
        self._validate_response(r_dm)
        buffer_dm = r_dm.content
        dm = _kgparser.decode_data_model_from_protocol_buffer(buffer_dm)
        return dm

    @property
    def datamodel(self) -> dict:
        """
        Returns the datamodel for the Knowledge Graph service

        :return: dict
        """
        self._validate_import()
        url = f"{self._url}/dataModel/queryDataModel"
        params = {
            "f": "pbf",
        }
        r_dm = self._gis._con.get(
            url, params=params, return_raw_response=True, try_json=False
        )
        self._validate_response(r_dm)
        buffer_dm = r_dm.content
        dm = _kgparser.decode_data_model_from_protocol_buffer(buffer_dm)
        return dm.to_value_object()

    def query_data_model(self, as_dict: bool = True) -> Union[dict, GraphDataModel]:
        """
        Returns the datamodel for the Knowledge Graph service

        ===================    ===============================================================
        **Parameter**           **Description**
        -------------------    ---------------------------------------------------------------
        as_dict                Optional Boolean. Determines whether the result is returned as
                               a dictionary or an object. The default is True. False is recommended.
        ===================    ===============================================================

        .. code-block:: python

            # Query knowledge graph data model
            knowledge_graph.query_data_model(as_dict=False)

        :return: :class:`~arcgis.graph.data_model_types.GraphDataModel`
        """
        if as_dict:
            warnings.warn(
                message=AS_DICT_DEPRECATION_WARNING,
                category=DeprecationWarning,
                stacklevel=2,
            )
        return (
            self.datamodel if as_dict else GraphDataModel.model_validate(self.datamodel)
        )

    def sync_data_model(
        self, as_dict: bool = True
    ) -> Union[dict, SyncDataModelResponse]:
        """
        Synchronizes the Knowledge Graph Service's data model with any changes made
        in the database. Will return any errors or warnings from the sync.

        ===================    ===============================================================
        **Parameter**           **Description**
        -------------------    ---------------------------------------------------------------
        as_dict                Optional Boolean. Determines whether the result is returned as
                               a dictionary or an object. The default is True. False is recommended.
        ===================    ===============================================================

        .. code-block:: python

            # Synchronize the data model
            sync_result = knowledge_graph.sync_data_model(as_dict=False)

        :return: :class:`~arcgis.graph.response_types.SyncDataModelResponse`

        """
        if as_dict:
            warnings.warn(
                message=AS_DICT_DEPRECATION_WARNING,
                category=DeprecationWarning,
                stacklevel=2,
            )
        url = self._url + "/dataModel/syncDataModel"
        session = self._gis._con._session
        params = {
            "f": "pbf",
            "token": self._gis._con.token,
        }
        headers = {"Content-Type": "application/octet-stream"}
        response = session.post(url=url, params=params, headers=headers, stream=True)
        self._validate_response(response)
        sync_response = response.content
        dec = _kgparser.SyncDataModelResponseDecoder()
        dec.decode(sync_response)
        results = dec.get_results()
        return results if as_dict else SyncDataModelResponse.model_validate(results)

    def apply_edits(
        self,
        adds: Sequence[Union[dict[str, Any], Entity, Relationship]] = [],
        updates: Sequence[Union[dict[str, Any], Entity, Relationship]] = [],
        deletes: Sequence[Union[dict[str, Any], EntityDelete, RelationshipDelete]] = [],
        input_transform: Optional[Union[dict[str, Any], Transform]] = None,
        cascade_delete: bool = False,
        cascade_delete_provenance: bool = False,
        as_dict: bool = True,
    ) -> Union[dict, ApplyEditsResponse]:
        """
        Allows users to add, update, and delete :class:`~arcgis.graph.graph_types.Entity` and
        :class:`~arcgis.graph.graph_types.Relationship`.

        .. note::
            objectid values are not supported in dictionaries for apply_edits

        =========================   ===============================================================
        **Parameter**                **Description**
        -------------------------   ---------------------------------------------------------------
        adds                        Optional list of :class:`~arcgis.graph.graph_types.Entity` or
                                    :class:`~arcgis.graph.graph_types.Relationship`. The list of
                                    objects to add to the graph, represented in dictionary format.
        -------------------------   ---------------------------------------------------------------
        updates                     Optional list of :class:`~arcgis.graph.graph_types.Entity` or
                                    :class:`~arcgis.graph.graph_types.Relationship`. The list of
                                    existent graph objects that are to be updated, represented
                                    in dictionary format.
        -------------------------   ---------------------------------------------------------------
        deletes                     Optional list of :class:`~arcgis.graph.graph_types.EntityDelete` or
                                    :class:`~arcgis.graph.graph_types.RelationshipDelete`. The list
                                    of existent objects to remove from the graph, represented in
                                    dictionary format.
        -------------------------   ---------------------------------------------------------------
        input_transform             Optional :class:`~arcgis.graph.graph_types.Transform`.
                                    Allows a user to specify custom quantization parameters for input
                                    geometry, which dictate how geometries are compressed and
                                    transferred to the server. Defaults to lossless WGS84 quantization.
        -------------------------   ---------------------------------------------------------------
        cascade_delete              Optional boolean. When `True`, relationships connected to
                                    entities that are being deleted will automatically be deleted
                                    as well. When `False`, these relationships must be deleted
                                    manually first. Defaults to `False`.
        -------------------------   ---------------------------------------------------------------
        cascade_delete_provenance   Optional boolean. When `True`, deleting entities/relationships
                                    or setting their property values to null will result in
                                    automatic deletion of associated provenance records. When
                                    `False`, `apply_edits()` will fail if there are provenance
                                    records connected to entities/relationships intended for
                                    deletion or having their properties set to null.
        -------------------------   ---------------------------------------------------------------
        as_dict                     Optional Boolean. Determines whether the result is returned as
                                    a dictionary or an object. The default is True. False is recommended.
        =========================   ===============================================================

        .. code-block:: python

            from arcgis.graph import Entity, Relationship

            add_entity = Entity(type_name="Company", properties={"name": "Esri"})
            delete_relationship = DeleteRelationship(type_name="WorksAt", ids=[UUID("783bd422-3hfp-45c7-87aa-8adbbdac0a3d")])

            graph.apply_edits(adds=[add_entity], deletes=[delete_relationship], as_dict=False)

        :return: :class:`~arcgis.graph.response_types.ApplyEditsResponse`

        """
        if as_dict:
            warnings.warn(
                message=AS_DICT_DEPRECATION_WARNING,
                category=DeprecationWarning,
                stacklevel=2,
            )

        raw_adds: list[dict[str, Any]] = []
        for named_object in adds:
            if isinstance(named_object, (Entity, Relationship)):
                raw_adds.append(named_object.model_dump(by_alias=True))
            else:
                warnings.warn(
                    message="List value of type dict for adds is deprecated. Please migrate to Entity or Relationship.",
                    category=DeprecationWarning,
                    stacklevel=2,
                )
                raw_adds.append(named_object)
        raw_updates: list[dict[str, Any]] = []
        for named_object in updates:
            if isinstance(named_object, (Entity, Relationship)):
                raw_updates.append(named_object.model_dump(by_alias=True))
            else:
                warnings.warn(
                    message="List value of type dict for updates is deprecated. Please migrate to Entity or Relationship.",
                    category=DeprecationWarning,
                    stacklevel=2,
                )
                raw_updates.append(named_object)
        raw_deletes: list[dict[str, Any]] = []
        for named_object_delete in deletes:
            if isinstance(named_object_delete, (EntityDelete, RelationshipDelete)):
                raw_deletes.append(named_object_delete.model_dump(by_alias=True))
            else:
                warnings.warn(
                    message="List value of type dict for deletes is deprecated. Please migrate to EntityDelete or RelationshipDelete.",
                    category=DeprecationWarning,
                    stacklevel=2,
                )
                raw_deletes.append(named_object_delete)
        raw_input_transform: Optional[dict[str, Any]] = None
        if isinstance(input_transform, Transform):
            raw_input_transform = input_transform.model_dump(by_alias=True)
        elif input_transform:
            warnings.warn(
                message="Input transform of type dict is deprecated. Please migrate to Transform.",
                category=DeprecationWarning,
                stacklevel=2,
            )
            raw_input_transform = input_transform

        url = self._url + "/graph/applyEdits"

        if raw_input_transform:
            quant_params = self._getInputQuantParams(raw_input_transform)
        else:
            quant_params = _kgparser.InputQuantizationParameters.WGS84_lossless()

        # now, make our encoder, and specify the edits to it
        enc = _kgparser.GraphApplyEditsEncoder(
            self._datamodel.spatial_reference,
            quant_params,
        )

        for edit in raw_adds:
            enc.add(edit)
        for edit in raw_updates:
            enc.update(edit)
        for edit in raw_deletes:
            enc.delete_from_ids(edit)
        enc.cascade_delete = cascade_delete
        enc.cascade_delete_provenance = cascade_delete_provenance

        # encode and prepare for the post request
        enc.encode()
        res = enc.get_encoding_result()

        if res.error.error_code != 0:
            raise Exception(res.error.error_message)

        pbf_params = {
            "f": "pbf",
            "token": self._gis._con.token,
        }
        headers = {"Content-Type": "application/octet-stream"}

        # post and decode the response
        session = self._gis._con._session
        request_response = session.post(
            url,
            params=pbf_params,
            headers=headers,
            data=res.byte_buffer,
            stream=True,
        )

        self._validate_response(request_response)
        apply_edits_response = request_response.content

        dec = _kgparser.GraphApplyEditsDecoder()
        dec.decode(apply_edits_response)
        results_dict = dec.get_results()

        return (
            results_dict if as_dict else ApplyEditsResponse.model_validate(results_dict)
        )

    def named_object_type_adds(
        self,
        entity_types: Sequence[Union[dict[str, Any], EntityType]] = [],
        relationship_types: Sequence[Union[dict[str, Any], RelationshipType]] = [],
        as_dict: bool = True,
    ) -> Union[dict, NamedObjectTypeAddsResponse]:
        """
        Adds :class:`~arcgis.graph.data_model_types.EntityType` and :class:`~arcgis.graph.data_model_types.RelationshipType`
        to the data model

        `Learn more about adding named types to a knowledge graph <https://developers.arcgis.com/rest/services-reference/enterprise/kgs-datamodel-edit-namedtypes-add.htm>`_

        ==================  ===============================================================
        **Parameter**        **Description**
        ------------------  ---------------------------------------------------------------
        entity_types        Optional list of EntityTypes. The list of entity types
                            to add to the data model, represented in dictionary format.
        ------------------  ---------------------------------------------------------------
        relationship_types  Optional list of RelationshipTypes. The list of
                            relationship types to add to the data model, represented in
                            dictionary format.
        ------------------  ---------------------------------------------------------------
        as_dict             Optional Boolean. Determines whether the result is returned as
                            a dictionary or an object. The default is True. False is recommended.
        ==================  ===============================================================

        .. code-block:: python

            from arcgis.graph import EntityType, RelationshipType, GraphProperty

            entity_type_add = EntityType(name="Vehicle", properties={"make": GraphProperty(name="make")})
            relationship_type_add = RelationshipType(name="Drives")

            graph.named_object_type_adds(entity_types=[entity_type_add], relationship_types=[relationship_type_add], as_dict=False)

        :return: :class:`~arcgis.graph.response_types.NamedObjectTypeAddsResponse`

        """
        if as_dict:
            warnings.warn(
                message=AS_DICT_DEPRECATION_WARNING,
                category=DeprecationWarning,
                stacklevel=2,
            )

        raw_entity_types: list[dict[str, Any]] = []
        for entity_type in entity_types:
            if isinstance(entity_type, EntityType):
                raw_entity_types.append(entity_type.model_dump(by_alias=True))
            else:
                warnings.warn(
                    message="List value of type dict for entity_types is deprecated. Please migrate to EntityType.",
                    category=DeprecationWarning,
                    stacklevel=2,
                )
                raw_entity_types.append(entity_type)
        raw_relationship_types: list[dict[str, Any]] = []
        for relationship_type in relationship_types:
            if isinstance(relationship_type, RelationshipType):
                raw_relationship_types.append(
                    relationship_type.model_dump(by_alias=True)
                )
            else:
                warnings.warn(
                    message="List value of type dict for relationship_types is deprecated. Please migrate to RelationshipType.",
                    category=DeprecationWarning,
                    stacklevel=2,
                )
                raw_relationship_types.append(relationship_type)

        self._validate_import()
        url = f"{self._url}/dataModel/edit/namedTypes/add"

        r_enc = _kgparser.GraphNamedObjectTypeAddsRequestEncoder()
        for entity_type in raw_entity_types:
            r_enc.add_entity_type(entity_type)
        for relationship_type in raw_relationship_types:
            r_enc.add_relationship_type(relationship_type)

        r_enc.encode()
        error = r_enc.get_encoding_result().error
        if error.error_code != 0:
            raise Exception(error.error_message)
        r_dec = _kgparser.GraphNamedObjectTypeAddsResponseDecoder()

        session = self._gis._con._session
        response = session.post(
            url=url,
            params={"f": "pbf"},
            data=r_enc.get_encoding_result().byte_buffer,
            stream=True,
            headers={"Content-Type": "application/octet-stream"},
        )

        self._validate_response(response)
        r_response = response.content

        r_dec.decode(r_response)
        results_dict = r_dec.get_results()

        return (
            results_dict
            if as_dict
            else NamedObjectTypeAddsResponse.model_validate(results_dict)
        )

    def named_object_type_update(
        self,
        type_name: str,
        named_type_update: Union[dict[str, Any], EntityType, RelationshipType],
        mask: Union[dict[str, Any], NamedObjectTypeMask],
        as_dict: bool = True,
    ) -> Union[dict, NamedObjectTypeUpdateResponse]:
        """
        Updates an :class:`~arcgis.graph.data_model_types.EntityType` or :class:`~arcgis.graph.data_model_types.RelationshipType` in the data model

        `Learn more about updating named types in a knowledge graph <https://developers.arcgis.com/rest/services-reference/enterprise/kgs-datamodel-edit-namedtypes-type-update.htm>`_

        =================   ===============================================================
        **Parameter**        **Description**
        -----------------   ---------------------------------------------------------------
        type_name           Required string. The named type to be updated.
        -----------------   ---------------------------------------------------------------
        named_type_update   Required Union[:class:`~arcgis.graph.data_model_types.EntityType`,
                            :class:`~arcgis.graph.data_model_types.RelationshipType`]. The entity or
                            relationship type to be updated.
        -----------------   ---------------------------------------------------------------
        mask                Required :class:`~arcgis.graph.data_model_types.NamedObjectTypeMask`.
                            The properties of the named type to be updated.
        -----------------   ---------------------------------------------------------------
        as_dict             Optional Boolean. Determines whether the result is returned as
                            a dictionary or an object. The default is True. False is recommended.
        =================   ===============================================================

        .. code-block:: python

            from arcgis.graph import EntityType, NamedObjectTypeMask

            type_update = EntityType(name="Vehicle", alias="Car")

            graph.named_object_type_update(
                type_name="Vehicle",
                named_type_update=type_update,
                mask=NamedObjectTypeMask(update_alias=True),
                as_dict=False
            )

        :return: :class:`~arcgis.graph.response_types.NamedObjectTypeUpdateResponse`

        """
        if as_dict:
            warnings.warn(
                message=AS_DICT_DEPRECATION_WARNING,
                category=DeprecationWarning,
                stacklevel=2,
            )

        if not isinstance(named_type_update, (EntityType, RelationshipType)):
            warnings.warn(
                message="Type dict is deprecated for named_type_update. Please migrate to EntityType or RelationshipType.",
                category=DeprecationWarning,
                stacklevel=2,
            )
        raw_named_type_update: dict[str, Any] = (
            named_type_update.model_dump(by_alias=True)
            if isinstance(named_type_update, (EntityType, RelationshipType))
            else named_type_update
        )
        if not isinstance(mask, NamedObjectTypeMask):
            warnings.warn(
                message="Type dict is deprecated for mask. Please migrate to NamedObjectTypeMask.",
                category=DeprecationWarning,
                stacklevel=2,
            )
        raw_mask: dict[str, Any] = (
            mask.model_dump(by_alias=True)
            if isinstance(mask, NamedObjectTypeMask)
            else mask
        )

        self._validate_import()
        url = f"{self._url}/dataModel/edit/namedTypes/{type_name}/update"

        data_model = self._datamodel
        entity_type = data_model.query_entity_type(type_name)
        relationship_type = data_model.query_relationship_type(type_name)

        r_enc = _kgparser.GraphNamedObjectTypeUpdateRequestEncoder()
        if entity_type is not None:
            r_enc.update_entity_type(raw_named_type_update, raw_mask)
        elif relationship_type is not None:
            r_enc.update_relationship_type(raw_named_type_update, raw_mask)

        r_enc.encode()
        error = r_enc.get_encoding_result().error
        if error.error_code != 0:
            raise Exception(error.error_message)
        r_dec = _kgparser.GraphNamedObjectTypeUpdateResponseDecoder()

        session = self._gis._con._session
        response = session.post(
            url=url,
            params={"f": "pbf"},
            data=r_enc.get_encoding_result().byte_buffer,
            stream=True,
            headers={"Content-Type": "application/octet-stream"},
        )

        self._validate_response(response)
        r_response = response.content

        r_dec.decode(r_response)
        results_dict = r_dec.get_results()

        return (
            results_dict
            if as_dict
            else NamedObjectTypeUpdateResponse.model_validate(results_dict)
        )

    def named_object_type_delete(
        self, type_name: str, as_dict: bool = True
    ) -> Union[dict, NamedObjectTypeDeleteResponse]:
        """
        Deletes an :class:`~arcgis.graph.data_model_types.EntityType` or :class:`~arcgis.graph.data_model_types.RelationshipType` in the data model

        `Learn more about deleting named types in a knowledge graph <https://developers.arcgis.com/rest/services-reference/enterprise/kgs-datamodel-edit-namedtypes-type-delete.htm>`_

        ================    ===============================================================
        **Parameter**        **Description**
        ----------------    ---------------------------------------------------------------
        type_name           Required string. The named type to be deleted.
        ----------------    ---------------------------------------------------------------
        as_dict             Optional Boolean. Determines whether the result is returned as
                            a dictionary or an object. The default is True. False is recommended.
        ================    ===============================================================

        .. code-block:: python

            # Delete a named type in the data model
            delete_result = graph.named_object_type_delete("Person")

        :return: :class:`~arcgis.graph.response_types.NamedObjectTypeDeleteResponse`

        """
        if as_dict:
            warnings.warn(
                message=AS_DICT_DEPRECATION_WARNING,
                category=DeprecationWarning,
                stacklevel=2,
            )
        self._validate_import()
        url = f"{self._url}/dataModel/edit/namedTypes/{type_name}/delete"

        r_dec = _kgparser.GraphNamedObjectTypeUpdateResponseDecoder()

        session = self._gis._con._session
        response = session.post(
            url=url,
            params={"f": "pbf"},
            stream=True,
            headers={"Content-Type": "application/octet-stream"},
        )

        self._validate_response(response)
        r_response = response.content

        r_dec.decode(r_response)
        results_dict = r_dec.get_results()

        return (
            results_dict
            if as_dict
            else NamedObjectTypeDeleteResponse.model_validate(results_dict)
        )

    def graph_property_adds(
        self,
        type_name: str,
        graph_properties: Sequence[Union[dict[str, Any], GraphProperty]],
        as_dict: bool = True,
    ) -> Union[dict, PropertyAddsResponse]:
        """
        Adds set of :class:`~arcgis.graph.data_model_types.GraphProperty` to a named type in the data model

        `Learn more about adding properties in a knowledge graph <https://developers.arcgis.com/rest/services-reference/enterprise/kgs-datamodel-edit-namedtypes-type-fields-add.htm>`_

        ================    ===============================================================
        **Parameter**        **Description**
        ----------------    ---------------------------------------------------------------
        type_name           Required string. The entity or relationship type to which the
                            properties will be added.
        ----------------    ---------------------------------------------------------------
        graph_properties    Required Sequence of :class:`~arcgis.graph.data_model_types.GraphProperty`.
                            The Sequence of properties to add to the named type.
        ----------------    ---------------------------------------------------------------
        as_dict             Optional Boolean. Determines whether the result is returned as
                            a dictionary or an object. The default is True. False is recommended.
        ================    ===============================================================

        .. code-block:: python

            from arcgis.graph import GraphProperty

            graph.graph_property_adds(type_name="Vehicle", graph_properties=[GraphProperty(name="year")])

        :return: :class:`~arcgis.graph.response_types.PropertyAddsResponse`

        """
        if as_dict:
            warnings.warn(
                message=AS_DICT_DEPRECATION_WARNING,
                category=DeprecationWarning,
                stacklevel=2,
            )

        raw_graph_properties: list[dict[str, Any]] = []
        for graph_property in graph_properties:
            if isinstance(graph_property, GraphProperty):
                raw_graph_properties.append(graph_property.model_dump(by_alias=True))
            else:
                warnings.warn(
                    message="List values of type dict for graph_properties is deprecated. Please migrate to GraphProperty.",
                    category=DeprecationWarning,
                    stacklevel=2,
                )
                raw_graph_properties.append(graph_property)

        self._validate_import()
        url = f"{self._url}/dataModel/edit/namedTypes/{type_name}/fields/add"

        r_enc = _kgparser.GraphPropertyAddsRequestEncoder()
        for prop in raw_graph_properties:
            r_enc.add_property(prop)

        r_enc.encode()
        error = r_enc.get_encoding_result().error
        if error.error_code != 0:
            raise Exception(error.error_message)
        r_dec = _kgparser.GraphPropertyAddsResponseDecoder()

        session = self._gis._con._session
        response = session.post(
            url=url,
            params={"f": "pbf"},
            data=r_enc.get_encoding_result().byte_buffer,
            stream=True,
            headers={"Content-Type": "application/octet-stream"},
        )

        self._validate_response(response)
        r_response = response.content

        r_dec.decode(r_response)
        results_dict = r_dec.get_results()

        return (
            results_dict
            if as_dict
            else PropertyAddsResponse.model_validate(results_dict)
        )

    def graph_property_update(
        self,
        type_name: str,
        property_name: str,
        graph_property: Union[dict[str, Any], GraphProperty],
        mask: Union[dict[str, Any], GraphPropertyMask],
        as_dict: bool = True,
    ) -> Union[dict, PropertyUpdateResponse]:
        """
        Updates a :class:`~arcgis.graph.data_model_types.GraphProperty` for a named type in the data model

        `Learn more about updating properties in a knowledge graph <https://developers.arcgis.com/rest/services-reference/enterprise/kgs-datamodel-edit-namedtypes-type-fields-update.htm>`_

        ================    ===============================================================
        **Parameter**        **Description**
        ----------------    ---------------------------------------------------------------
        type_name           Required string. The entity or relationship type containing
                            the property to be updated.
        ----------------    ---------------------------------------------------------------
        property_name       Required string. The property to be updated.
        ----------------    ---------------------------------------------------------------
        graph_property      Required :class:`~arcgis.graph.data_model_types.GraphProperty`.
                            The graph property to be updated.
        ----------------    ---------------------------------------------------------------
        mask                Required :class:`~arcgis.graph.data_model_types.GraphPropertyMask`.
                            The properties of the field to be updated.
        ----------------    ---------------------------------------------------------------
        as_dict             Optional Boolean. Determines whether the result is returned as
                            a dictionary or an object. The default is True. False is recommended.
        ================    ===============================================================

        .. code-block:: python

            from arcgis.graph import GraphProperty, GraphPropertyMask

            graph.graph_property_update(
                type_name="Vehicle",
                property_name="year",
                graph_property=GraphProperty(name="year", alias="year_made"),
                mask=GraphPropertyMask(update_alias=True),
                as_dict=False
            )

        :return: :class:`~arcgis.graph.response_types.PropertyUpdateResponse`

        """
        if as_dict:
            warnings.warn(
                message=AS_DICT_DEPRECATION_WARNING,
                category=DeprecationWarning,
                stacklevel=2,
            )

        if not isinstance(graph_property, GraphProperty):
            warnings.warn(
                message="Type dict for graph_property is deprecated. Please migrate to GraphProperty.",
                category=DeprecationWarning,
                stacklevel=2,
            )
        raw_graph_property: dict[str, Any] = (
            graph_property.model_dump(by_alias=True)
            if isinstance(graph_property, GraphProperty)
            else graph_property
        )
        if not isinstance(mask, GraphPropertyMask):
            warnings.warn(
                message="Type dict for mask is deprecated. Please migrate to GraphPropertyMask.",
                category=DeprecationWarning,
                stacklevel=2,
            )
        raw_mask: dict[str, Any] = (
            mask.model_dump(by_alias=True)
            if isinstance(mask, GraphPropertyMask)
            else mask
        )

        self._validate_import()
        url = f"{self._url}/dataModel/edit/namedTypes/{type_name}/fields/update"

        r_enc = _kgparser.GraphPropertyUpdateRequestEncoder()
        r_enc.update_property(raw_graph_property, raw_mask)
        r_enc.name = property_name

        r_enc.encode()
        error = r_enc.get_encoding_result().error
        if error.error_code != 0:
            raise Exception(error.error_message)
        r_dec = _kgparser.GraphPropertyUpdateResponseDecoder()

        session = self._gis._con._session
        response = session.post(
            url=url,
            params={"f": "pbf"},
            data=r_enc.get_encoding_result().byte_buffer,
            stream=True,
            headers={"Content-Type": "application/octet-stream"},
        )

        self._validate_response(response)
        r_response = response.content

        r_dec.decode(r_response)
        results_dict = r_dec.get_results()

        return (
            results_dict
            if as_dict
            else PropertyUpdateResponse.model_validate(results_dict)
        )

    def graph_property_delete(
        self, type_name: str, property_name: str, as_dict: bool = True
    ) -> Union[dict, PropertyDeleteResponse]:
        """
        Delete a :class:`~arcgis.graph.data_model_types.GraphProperty` for a named type in the data model

        `Learn more about deleting properties in a knowledge graph <https://developers.arcgis.com/rest/services-reference/enterprise/kgs-datamodel-edit-namedtypes-type-fields-delete.htm>`_

        ================    ===============================================================
        **Parameter**        **Description**
        ----------------    ---------------------------------------------------------------
        type_name           Required string. The entity or relationship type containing
                            the property to be deleted.
        ----------------    ---------------------------------------------------------------
        property_name       Required string. The property to be deleted.
        ----------------    ---------------------------------------------------------------
        as_dict             Optional Boolean. Determines whether the result is returned as
                            a dictionary or an object. The default is True. False is recommended.
        ================    ===============================================================

        .. code-block:: python

            # Delete a named type's property in the data model
            delete_result = knowledge_graph.graph_property_delete("Person", "Address", as_dict=False)


        :return: :class:`~arcgis.graph.response_types.PropertyDeleteResponse`

        """
        if as_dict:
            warnings.warn(
                message=AS_DICT_DEPRECATION_WARNING,
                category=DeprecationWarning,
                stacklevel=2,
            )
        self._validate_import()
        url = f"{self._url}/dataModel/edit/namedTypes/{type_name}/fields/delete"

        r_enc = _kgparser.GraphPropertyDeleteRequestEncoder()
        r_enc.name = property_name

        r_enc.encode()
        error = r_enc.get_encoding_result().error
        if error.error_code != 0:
            raise Exception(error.error_message)
        r_dec = _kgparser.GraphPropertyDeleteResponseDecoder()

        session = self._gis._con._session
        response = session.post(
            url=url,
            params={"f": "pbf"},
            data=r_enc.get_encoding_result().byte_buffer,
            stream=True,
            headers={"Content-Type": "application/octet-stream"},
        )

        self._validate_response(response)
        r_response = response.content

        r_dec.decode(r_response)
        results_dict = r_dec.get_results()

        return (
            results_dict
            if as_dict
            else PropertyDeleteResponse.model_validate(results_dict)
        )

    def graph_property_index_adds(
        self,
        type_name: str,
        field_indexes: Sequence[Union[dict[str, Any], FieldIndex]],
        as_dict: bool = True,
    ) -> Union[dict, IndexAddsResponse]:
        """
        Adds one or more :class:`~arcgis.graph.data_model_types.FieldIndex` to a field or multiple fields
        associated with a named type in the data model.

        `Learn more about adding graph property indexes in a knowledge graph <https://developers.arcgis.com/rest/services-reference/enterprise/kgs-datamodel-edit-namedtypes-type-indexes-add.htm>`_

        ================    ===============================================================
        **Parameter**        **Description**
        ----------------    ---------------------------------------------------------------
        type_name           Required string. The entity or relationship type to add the
                            indexes to.
        ----------------    ---------------------------------------------------------------
        field_indexes       Required list of dicts. The indexes to add for the type.
                            See below for an example of the structure.
        ----------------    ---------------------------------------------------------------
        as_dict             Optional Boolean. Determines whether the result is returned as
                            a dictionary or an object. The default is True. False is recommended.
        ================    ===============================================================

        .. code-block:: python

            from arcgis.graph import FieldIndex

            graph.graph_property_index_adds(
                type_name="Person",
                field_indexes=[FieldIndex(name="name_index", is_ascending=True, is_unique=False, fields=["name"])],
                as_dict=False
            )

        :return: :class:`~arcgis.graph.response_types.IndexAddsResponse`

        """
        if as_dict:
            warnings.warn(
                message=AS_DICT_DEPRECATION_WARNING,
                category=DeprecationWarning,
                stacklevel=2,
            )

        raw_field_indexes: list[dict[str, Any]] = []
        for field_index in field_indexes:
            if isinstance(field_index, FieldIndex):
                raw_field_indexes.append(field_index.model_dump(by_alias=True))
            else:
                warnings.warn(
                    message="List value of type dict for field_indexes is deprecated. Please migrate to FieldIndex.",
                    category=DeprecationWarning,
                    stacklevel=2,
                )
                raw_field_indexes.append(field_index)

        self._validate_import()
        url = self._url + "/dataModel/edit/namedTypes/" + type_name + "/indexes/add"
        params = {
            "f": "pbf",
            "token": self._gis._con.token,
        }
        headers = {"Content-Type": "application/octet-stream"}

        enc = _kgparser.GraphIndexAddsRequestEncoder()
        enc.add_field_indexes(raw_field_indexes)
        enc.encode()
        enc_result = enc.get_encoding_result()
        error = enc_result.error
        if error.error_code != 0:
            raise Exception(error.error_message)

        session = self._gis._con._session
        response = session.post(
            url=url,
            params=params,
            data=enc_result.byte_buffer,
            stream=True,
            headers=headers,
        )

        self._validate_response(response)
        response_content = response.content
        dec = _kgparser.GraphIndexAddsResponseDecoder()
        dec.decode(response_content)

        results_dict = dec.get_results()
        return (
            results_dict if as_dict else IndexAddsResponse.model_validate(results_dict)
        )

    def graph_property_index_deletes(
        self, type_name: str, field_indexes: Sequence[str], as_dict: bool = True
    ) -> Union[dict, IndexDeletesResponse]:
        """
        Deletes one or more :class:`~arcgis.graph.data_model_types.FieldIndex` from fields
        associated with a named type in the data model.

        `Learn more about deleting graph property indexes from a knowledge graph <https://developers.arcgis.com/rest/services-reference/enterprise/kgs-datamodel-edit-namedtypes-type-indexes-delete.htm>`_

        ================    ===============================================================
        **Parameter**        **Description**
        ----------------    ---------------------------------------------------------------
        type_name           Required string. The entity or relationship type to delete the
                            field indexes from.
        ----------------    ---------------------------------------------------------------
        field_indexes       Required Sequence of strings. The field indexes to delete from the
                            type.
        ----------------    ---------------------------------------------------------------
        as_dict             Optional Boolean. Determines whether the result is returned as
                            a dictionary or an object. The default is True. False is recommended.
        ================    ===============================================================

        .. code-block:: python

            # Delete field indexes from a Knowledge Graph type
            delete_result = graph.graph_property_index_deletes("Project", ["title"], as_dict=False)


        :return: :class:`~arcgis.graph.response_types.IndexDeletesResponse`

        """
        if as_dict:
            warnings.warn(
                message=AS_DICT_DEPRECATION_WARNING,
                category=DeprecationWarning,
                stacklevel=2,
            )

        indexes: list[str] = [index for index in field_indexes]

        self._validate_import()
        url = self._url + "/dataModel/edit/namedTypes/" + type_name + "/indexes/delete"
        params = {
            "f": "pbf",
            "token": self._gis._con.token,
        }
        headers = {"Content-Type": "application/octet-stream"}

        enc = _kgparser.GraphIndexDeleteRequestEncoder()
        enc.add_field_index_names(indexes)
        enc.encode()
        enc_result = enc.get_encoding_result()
        error = enc_result.error
        if error.error_code != 0:
            raise Exception(error.error_message)

        session = self._gis._con._session
        response = session.post(
            url=url,
            params=params,
            data=enc_result.byte_buffer,
            stream=True,
            headers=headers,
        )

        self._validate_response(response)
        response_content = response.content
        dec = _kgparser.GraphIndexDeleteResponseDecoder()
        dec.decode(response_content)

        results_dict = dec.get_results()
        return (
            results_dict
            if as_dict
            else IndexDeletesResponse.model_validate(results_dict)
        )

    def constraint_rule_adds(
        self,
        rules: Sequence[Union[dict[str, Any], ConstraintRule]],
        as_dict: bool = True,
    ) -> Union[dict, ConstraintRuleAddsResponse]:
        """
        Adds constraint rules for entities & relationships to the data model.
        :class:`~arcgis.graph.data_model_types.RelationshipExclusionRule` is a constraint rule.

        ================    ===============================================================
        **Parameter**        **Description**
        ----------------    ---------------------------------------------------------------
        rules               Required Sequence of :class:`~arcgis.graph.data_model_types.RelationshipExclusionRule`.
                            Defines the constraint rules to be added.
        ----------------    ---------------------------------------------------------------
        as_dict             Optional Boolean. Determines whether the result is returned as
                            a dictionary or an object. The default is True. False is recommended.
        ================    ===============================================================

        .. code-block:: python

            from arcgis.graph import RelationshipExclusionRule

            graph.constraint_rule_adds(
                rules=[
                    RelationshipExclusionRule(
                        name="OnlyPersonCanWorkAtCompany",
                        origin_entity_types=SetOfNamedTypes(set_complement=["Person"]),
                        relationship_types=SetOfNamedTypes(set=["WorksAt"]),
                        destination_entity_types=SetOfNamedTypes(set=["Company"])
                    )
                ],
                as_dict=False
            )

        :return: :class:`~arcgis.graph.response_types.ConstraintRuleAddsResponse`

        """
        if as_dict:
            warnings.warn(
                message=AS_DICT_DEPRECATION_WARNING,
                category=DeprecationWarning,
                stacklevel=2,
            )

        raw_rules: list[dict[str, Any]] = []
        for rule in rules:
            if isinstance(rule, ConstraintRule):
                raw_rules.append(rule.model_dump(by_alias=True))
            else:
                warnings.warn(
                    message="List value of type dict for rules is deprecated. Please migrate to ConstraintRule.",
                    category=DeprecationWarning,
                    stacklevel=2,
                )
                raw_rules.append(rule)

        self._validate_import()
        split_url = self._url.split("/rest/")
        url = (
            split_url[0]
            + "/rest/admin/"
            + split_url[1]
            + "/dataModel/constraintRules/add"
        )
        params = {
            "f": "pbf",
            "token": self._gis._con.token,
        }
        headers = {"Content-Type": "application/octet-stream"}

        enc = _kgparser.GraphAddConstraintRulesEncoder()
        for rule in raw_rules:
            enc.add_constraint_rule(rule)
        enc.encode()
        enc_result = enc.get_encoding_result()
        error = enc_result.error
        if error.error_code != 0:
            raise Exception(error.error_message)

        session = self._gis._con._session
        response = session.post(
            url=url,
            params=params,
            data=enc_result.byte_buffer,
            stream=True,
            headers=headers,
        )

        self._validate_response(response)
        response_content = response.content
        dec = _kgparser.GraphAddConstraintRulesDecoder()
        dec.decode(response_content)

        results_dict = dec.get_results()
        return (
            results_dict
            if as_dict
            else ConstraintRuleAddsResponse.model_validate(results_dict)
        )

    def constraint_rule_updates(
        self,
        rules: Sequence[Union[dict[str, Any], ConstraintRuleUpdate]],
        as_dict: bool = True,
    ) -> Union[dict, ConstraintRuleUpdatesResponse]:
        """
        Update :class:`~arcgis.graph.data_model_types.ConstraintRule` for entities & relationships in the data model.
        :class:`~arcgis.graph.data_model_types.RelationshipExclusionRule` is a type of constraint rule.

        ================    ===============================================================
        **Parameter**        **Description**
        ----------------    ---------------------------------------------------------------
        rules               Required Sequence of :class:`~arcgis.graph.data_model_types.RelationshipExclusionRuleUpdate`.
                            Defines the constraint rules to be updated.
        ----------------    ---------------------------------------------------------------
        as_dict             Optional Boolean. Determines whether the result is returned as
                            a dictionary or an object. The default is True. False is recommended.
        ================    ===============================================================

        .. code-block:: python

            from arcgis.graph import RelationshipExclusionRuleUpdate, ConstraintRule, ConstraintRuleMask, UpdateSetOfNamedTypes

            graph.constraint_rule_updates(
                rules=[
                    RelationshipExclusionRuleUpdate(
                        rule_name="OnlyPersonCanWorkForCompany",
                        mask=ConstraintRuleMask(update_name=True, update_alias=True),
                        constraint_rule=ConstraintRule(
                            name="PersonCanWorkForCompanyOrPark",
                            alias="Person Can Work For Company or Park"
                        )
                    )
                ],
                as_dict=False
            )

        :return: :class:`~arcgis.graph.response_types.ConstraintRuleUpdatesResponse`

        """
        if as_dict:
            warnings.warn(
                message=AS_DICT_DEPRECATION_WARNING,
                category=DeprecationWarning,
                stacklevel=2,
            )

        raw_rules: list[dict[str, Any]] = []
        for rule in rules:
            if isinstance(rule, ConstraintRuleUpdate):
                raw_rules.append(rule.model_dump(by_alias=True))
            else:
                warnings.warn(
                    message="List value of type dict for rules is deprecated. Please migrate to ConstraintRuleUpdate.",
                    category=DeprecationWarning,
                    stacklevel=2,
                )
                raw_rules.append(rule)

        self._validate_import()
        split_url = self._url.split("/rest/")
        url = (
            split_url[0]
            + "/rest/admin/"
            + split_url[1]
            + "/dataModel/constraintRules/update"
        )
        params = {
            "f": "pbf",
            "token": self._gis._con.token,
        }
        headers = {"Content-Type": "application/octet-stream"}

        enc = _kgparser.GraphUpdateConstraintRulesEncoder()
        for rule in raw_rules:
            enc.add_constraint_rule_update(rule)
        enc.encode()
        enc_result = enc.get_encoding_result()
        error = enc_result.error
        if error.error_code != 0:
            raise Exception(error.error_message)

        session = self._gis._con._session
        response = session.post(
            url=url,
            params=params,
            data=enc_result.byte_buffer,
            stream=True,
            headers=headers,
        )

        self._validate_response(response)
        response_content = response.content
        dec = _kgparser.GraphUpdateConstraintRulesDecoder()
        dec.decode(response_content)

        results_dict = dec.get_results()
        return (
            results_dict
            if as_dict
            else ConstraintRuleUpdatesResponse.model_validate(results_dict)
        )

    def constraint_rule_deletes(
        self, rule_names: Sequence[str], as_dict: bool = True
    ) -> Union[dict, ConstraintRuleDeletesResponse]:
        """
        Deletes existing constraint rules for entities & relationships from the data model.
        :class:`~arcgis.graph.data_model_types.RelationshipExclusionRule` is a constraint rule.

        ================    ===============================================================
        **Parameter**        **Description**
        ----------------    ---------------------------------------------------------------
        rule_names          Required Sequence of strings. The names of the constraint rules to
                            be deleted, as defined in a rule's 'name' attribute.
        ----------------    ---------------------------------------------------------------
        as_dict             Optional Boolean. Determines whether the result is returned as
                            a dictionary or an object. The default is True. False is recommended.
        ================    ===============================================================

        .. code-block:: python

            # Delete a constraint rule from the Knowledge Graph's data model.
            graph.constraint_rule_deletes(["constraint_rule_1"], as_dict=False)


        :return: :class:`~arcgis.graph.response_types.ConstraintRuleDeletesResponse`

        """
        if as_dict:
            warnings.warn(
                message=AS_DICT_DEPRECATION_WARNING,
                category=DeprecationWarning,
                stacklevel=2,
            )

        rules: list[str] = [rule_name for rule_name in rule_names]

        self._validate_import()
        split_url = self._url.split("/rest/")
        url = (
            split_url[0]
            + "/rest/admin/"
            + split_url[1]
            + "/dataModel/constraintRules/delete"
        )
        params = {
            "f": "pbf",
            "token": self._gis._con.token,
        }
        headers = {"Content-Type": "application/octet-stream"}

        enc = _kgparser.GraphDeleteConstraintRulesEncoder()
        enc.add_constraint_rule_names(rules)
        enc.encode()
        enc_result = enc.get_encoding_result()
        error = enc_result.error
        if error.error_code != 0:
            raise Exception(error.error_message)

        session = self._gis._con._session
        response = session.post(
            url=url,
            params=params,
            data=enc_result.byte_buffer,
            stream=True,
            headers=headers,
        )

        self._validate_response(response)
        response_content = response.content
        dec = _kgparser.GraphDeleteConstraintRulesDecoder()
        dec.decode(response_content)

        results_dict = dec.get_results()
        return (
            results_dict
            if as_dict
            else ConstraintRuleDeletesResponse.model_validate(results_dict)
        )
