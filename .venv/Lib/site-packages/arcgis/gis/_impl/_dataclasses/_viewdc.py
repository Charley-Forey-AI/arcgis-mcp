from __future__ import annotations
import os
import json
from arcgis.auth.tools._lazy import LazyLoader
from dataclasses import dataclass, field
from enum import Enum
from arcgis.gis._impl._dataclasses._sfilters import SpatialFilter
from dataclasses import asdict


def _custom_asdict_factory(data):
    def convert_value(obj):
        if isinstance(obj, Enum):
            return obj.value
        return obj

    return dict(
        (k, convert_value(v)) for k, v in data if not k in ["_dict_data", "layer"]
    )


arcgis = LazyLoader("arcgis")

__all__ = [
    "JoinType",
    "StatisticType",
    "PrimaryTableView",
    "RelatedTable",
    "SourceLayerField",
    "ViewLayerDefParameter",
]


###########################################################################
class JoinType(Enum):
    INNER: str = "INNER"
    LEFT: str = "LEFT"
    RIGHT: str = "RIGHT"
    FULL: str = "FULL"


###########################################################################
class StatisticType(Enum):
    """
    The statistic/aggregation function to be applied on the field.
    """

    SUM: str = "SUM"
    AVG: str = "AVG"
    MIN: str = "MIN"
    MAX: str = "MAX"
    STDDEV: str = "STDDEV"


###########################################################################
@dataclass
class SourceLayerField:
    """
    The `SourceLayerField` property defines how view fields reference fields
    from the source layer. It can reference a source field directly, create
    a statistic from it, or can be defined as an expression that uses the
    source layer fields.

    ==================  ===============================================================================
    **Parameter**        **Description**
    ------------------  -------------------------------------------------------------------------------
    name                Required String. The field name in the new view. The name of the field can be different from the source field name.
    ------------------  -------------------------------------------------------------------------------
    alias               Optional String. The field alias in the new view.
    ------------------  -------------------------------------------------------------------------------
    source              Optional String. The source layer field name that is referenced directly or utilized inside the statistic operation.
    ------------------  -------------------------------------------------------------------------------
    statistic_type      Optional StatisticType. The statistic/aggregation function to be applied on the field.
    ==================  ===============================================================================
    """

    name: str
    alias: str | None = None
    source: str | None = None
    statistic_type: StatisticType | None = None
    field_type: str | None = None
    _dict_data: dict | None = field(init=False)

    def __str__(self) -> str:
        return "<SourceLayerField>"

    def __repr__(self) -> str:
        return "<SourceLayerField>"

    def __post_init__(self):
        self._dict_data = self._create_dict()

    def to_json(self) -> str:
        return json.dumps(asdict(self, dict_factory=_custom_asdict_factory))

    def _create_dict(self) -> dict:
        data = {
            "name": self.name,
        }
        if self.alias:
            data["alias"] = self.alias
        if self.source:
            data["source"] = self.source
        if self.statistic_type:
            data["statisticType"] = self.statistic_type.value
        if self.field_type:
            data["type"] = self.field_type

        self._dict_data = data
        return data


###########################################################################
@dataclass
class RelatedTable:
    """
    Related tables define the tables joined to the primary table. Related
    tables are the same in the way they define fields, but differ insofar
    as they contain join information. Related table objects should be
    listed in the relatedTables property of the table.
    """

    name: str
    source_service_name: str
    join_type: JoinType
    parent_key_fields: list[str]
    key_fields: list[str]
    source_layer_id: int | None = None
    filter: str | None = None
    top_filter: str | None = None
    source_fields: list[SourceLayerField] | None = None

    def __str__(self) -> str:
        return "<RelatedTable>"

    def __repr__(self) -> str:
        return "<RelatedTable>"

    def __post_init__(self):
        self._dict_data = self._create_dict()

    def to_json(self) -> str:
        return json.dumps(asdict(self, dict_factory=_custom_asdict_factory))

    def _create_dict(self) -> dict:
        data = {}
        data["name"] = self.name
        data["sourceServiceName"] = self.source_service_name
        data["type"] = self.join_type.value
        data["parentKeyFields"] = self.parent_key_fields
        data["keyFields"] = self.key_fields
        if isinstance(self.source_layer_id, int):
            data["sourceLayerId"] = self.source_layer_id
        if self.filter:
            data["filter"] = self.filter
        if self.top_filter:
            data["topFilter"] = self.top_filter
        if self.source_fields:
            data["sourceLayerFields"] = [
                fld._create_dict()
                for fld in self.source_fields
                if isinstance(fld, SourceLayerField)
            ]
        self._dict_data = data
        return data


###########################################################################
@dataclass
class PrimaryTableView:
    """
    Related tables define the tables joined to the primary table. Related
    tables are the same in the way they define fields, but differ insofar
    as they contain join information.

    =====================  ===============================================================================
    **Parameter**           **Description**
    ---------------------  -------------------------------------------------------------------------------
    source_service_name    Required str. The underlying source service name of the joined service.
    ---------------------  -------------------------------------------------------------------------------
    source_fields          Required list[SourceLayerField]. The list of fields in the newly created view of the joined layer.
    ---------------------  -------------------------------------------------------------------------------
    name                   Optional str. An alias sometimes used to reference this table.
    ---------------------  -------------------------------------------------------------------------------
    source_id              Optional int. The underlying source layer ID of the joined layer. Defaults to the first layer or table.
    ---------------------  -------------------------------------------------------------------------------
    filter                 Optional str. Filters layer data based on non-aggregated fields. The filter needs to reference the source layer field names.
    ---------------------  -------------------------------------------------------------------------------
    top_filter             Optional str. A topFilter on the table results.
    ---------------------  -------------------------------------------------------------------------------
    related_tables         Optional list[RelatedTable]. The definition of the related tables.
    ---------------------  -------------------------------------------------------------------------------
    having_filter          Optional str. Filters data based on aggregated fields. The filter needs to reference field names defined in the view rather than the source field names.
    ---------------------  -------------------------------------------------------------------------------
    group_by               Optional str. Fields for aggregation. Needs to reference field names defined in the view and not the source field names.
    =====================  ===============================================================================
    """

    # table: str
    layer: arcgis.features.FeatureLayer
    source_service_name: str
    source_fields: list[SourceLayerField]
    related_tables: list[RelatedTable]
    name: str | None = None
    source_id: int | None = None
    filter: str | None = None
    top_filter: str | None = None

    having_filter: str | None = None
    group_by: str | None = None
    _dict_data: dict | None = field(init=False)

    def __str__(self) -> str:
        return "<PrimaryTable>"

    def __repr__(self) -> str:
        return "<PrimaryTable>"

    def __post_init__(self):
        self._dict_data = self._create_dict()

    def to_json(self) -> str:
        return json.dumps(asdict(self, dict_factory=_custom_asdict_factory))

    def _create_dict(self) -> dict:
        from arcgis.features import FeatureLayer

        data = {}
        data["name"] = self.name
        data["sourceServiceName"] = self.source_service_name
        if isinstance(self.source_id, int):
            data["sourceLayerId"] = self.source_id
        elif self.layer:
            data["sourceLayerId"] = int(os.path.basename(self.layer._url))
        if self.name:
            data["name"] = self.name
        if self.source_id:
            data["sourceLayerId"] = self.source_id
        if self.filter:
            data["filter"] = self.filter
        if self.top_filter:
            data["topFilter"] = self.top_filter
        if self.related_tables and isinstance(self.related_tables, list):
            data["relatedTables"] = [
                fld._create_dict()
                for fld in self.related_tables
                if isinstance(fld, RelatedTable)
            ]
        if self.having_filter:
            data["havingFilter"] = self.having_filter
        if self.group_by:
            data["groupBy"] = self.group_by
        if self.source_fields:
            data["sourceLayerFields"] = [
                fld._create_dict()
                for fld in self.source_fields
                if isinstance(fld, SourceLayerField)
            ]
        self._dict_data = data
        return data


###########################################################################
@dataclass
class ViewLayerDefParameter:
    """
    When updating views, an *ViewLayerDefParameter* object can be provided
    to change the view definition.  Objects of this class can be created
    directly, obtained from the :meth:`~arcgis.gis.ViewManager.get_definitions`
    method of the :class:`~arcgis.gis.ViewManager` class, or accessed using the
    :meth:`~arcgis.gis._impl._dataclasses.ViewLayerDefParameter.from_layer`
    method.

    .. code-block:: python

        # Usage Example 1: Initializing an object directly
        >>> from arcgis.gis import GIS, ViewLayerDefParameter
        >>> from arcgis.gis import SpatialFilter, SpatialRelationship
        >>> from arcgis.geometry import Envelope, SpatialReference

        >>> gis = GIS(profile="your_organization_profile")

        >>> filter_geom = Envelope(
              iterable={
                            "xmin": -10571116.801669,
                            "ymin": 3877172.713480,
                            "xmax": -9897056.867736,
                            "ymax": 4391087.518527,
                            "spatialReference": {
                              "wkid": 102100,
                              "latestWkid": 3857
                            }
                        }
            )
        >>> spat_filter = SpatialFilter(
                goemetry= filter_geom,
                spatial_rel= SpatialRelationship.CROSSES,
                sr= SpatialReference({"latestWkid": 3857, "wkid": 102100})
            )
        >>> vw_def = ViewLayerDefParameter(
               layer= view_item.layers[0],
               query_definition= "unit_field IS NOT NULL",
               spatial_filter= spat_filter
               fields= [
                    {"name": "OBJECTID", "visible": True},
                    {"name": "unit_field", "visible": True},
                    {"name": "create_date", "visible": False},
                    {"name": "system_id", "visible": False}
                ]
            )

        # Usage Example 2: Accessing from a Feature Layer view
        >>> from arcgis.gis import GIS
        >>> gis = GIS(profile="your_organization_profile")

        >>> view_item = gis.content.get("<item_id_for_view>")
        >>> vw_mgr = view_item.view_manager

        >>> vw_defs = vw_mgr.get_definitions(view_item)
        >>> vw_def - vw_defs[0]
        <ViewLayerDefParameter>

        >>> vw_def.as_json()
        {'viewLayerDefinition': {'filter': {'geometry': {'rings': [[[-9982417.919074,4370975.02546],
                                                            [-9982417.919074,4769966.75848],
                                                            [-8954750.737665,4769966.75848],
                                                            [-8954750.737665,4370975.02546],
                                                            [-9982417.919074,4370975.02546]]],
                                                 'spatialReference': {'latestWkid': 3857,'wkid': 102100}},
                                    'geometryType': 'esriGeometryPolygon',
                                    'spatialRel': 'esriSpatialRelIntersects',
                                    'inSR': {'latestWkid': 3857,
                                             'wkid': 102100}}},
         'fields': [{'name': 'objectid', 'visible': True},
                    ...
                    {'name': 'feature_code', 'visible': True}]
        }
    """

    layer: arcgis.features.FeatureLayer
    """
    A :class:`~arcgis.features.FeatureLayer` object.
    """
    query_definition: str | None = None
    """
    A string representing a where clause to filter records in the view.
    """
    spatial_filter: SpatialFilter | None = None
    """
    A :class:`~arcgis.gis._impl._dataclasses.SpatialFilter` object.
    """
    fields: list[dict] | None = None
    """
    A list of dictionaries indicating whether a specific field is to be visible
    in the view layer. Each dictionary must contain a *name* and *visible* key.
    If this property is not defined, all fields will be visible.
    
    .. code-block:: python
    
        # Format for dictionary to define field visibility in a view
        >>> view_lyr_def = ViewLayerDefParameter(
                    ...
                    fields=[
                        {"name": <field_name>, "visible": True|False},
                        ...
                        {"name": <field2_name>}, "visible": True|False}  
                    ]
            )
    """
    _dict_data: dict | None = field(init=False)

    def __str__(self) -> str:
        return "<ViewLayerDefParameter>"

    def __repr__(self) -> str:
        return "<ViewLayerDefParameter>"

    def __post_init__(self):
        self._dict_data = self._create_dict()

    def _create_dict(self) -> dict:
        data = {}
        if self.query_definition:
            data["viewDefinitionQuery"] = self.query_definition
        if self.spatial_filter:
            spatial_filter: dict = self.spatial_filter.as_json()
            sfilter: dict = {
                "operator": spatial_filter.get(
                    "spatialRel", "esriSpatialRelIntersects"
                ),
                "value": {
                    "geometryType": spatial_filter.get("geometryType"),
                    "geometry": spatial_filter.get("geometry"),
                },
            }
            data["viewLayerDefinition"] = {"filter": sfilter}
        if self.fields:
            data["fields"] = self.fields
        self._dict_data = data
        return data

    @classmethod
    def fromlayer(
        self,
        layer: (
            arcgis.features.managers.FeatureLayerManager | arcgis.features.FeatureLayer
        ),
    ) -> "ViewLayerDefParameter":
        """Creates a view layer definition parameter object from a feature layer
        or feature layer manager.

        =================     ==================================================
        **Parameter**         **Description**
        -----------------     --------------------------------------------------
        layer                 Required :class:`~arcgis.features.FeatureLayer` or
                              :class:`~arcgis.features.managers.FeatureLayerManager`
                              object.
        =================     ==================================================
        """
        from arcgis.features.managers import FeatureLayerManager
        from arcgis.features import FeatureLayer, Table

        if isinstance(layer, (FeatureLayer, Table)):
            fl = layer
            adminlayer = layer.manager
        else:
            adminlayer = layer
            fl = FeatureLayer(
                url=adminlayer.url.replace(r"/admin/", r"/"), gis=layer._gis
            )
        props = dict(adminlayer.properties)
        gfilter = (
            props.get("adminLayerInfo", {})
            .get("viewLayerDefinition", {})
            .get("table", {})
            .get("filter", None)
        )
        if gfilter:
            from arcgis.gis._impl._dataclasses import (
                SpatialRelationship,
                SpatialFilter,
            )

            g = arcgis.geometry.Geometry(gfilter["value"]["geometry"])
            sf = SpatialFilter(
                geometry=g,
                sr=g["spatialReference"],
                spatial_rel=SpatialRelationship._value2member_map_[gfilter["operator"]],
            )
        else:
            sf = None
        fields = [
            {"name": fld["name"], "visible": fld.get("visible", True)}
            for fld in props["fields"]
        ]
        return ViewLayerDefParameter(
            layer=fl,
            query_definition=props.get("viewDefinitionQuery", None),
            spatial_filter=sf,
            fields=fields,
        )

    def as_json(self) -> dict:
        """returns the view as a dictionary"""
        return self._create_dict()
