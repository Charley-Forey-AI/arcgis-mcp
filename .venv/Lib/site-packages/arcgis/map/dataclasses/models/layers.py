from __future__ import annotations  # Enables postponed evaluation of type hints
from .base_model import BaseModel, common_config
from pydantic import Field, constr, confloat, conint
from typing import Any, Literal, ForwardRef
from ..enums.layers import (
    BlendMode,
    ListMode,
    FeatureCollectionType,
    ImageFormat,
    NoDataInterpretation,
    PixelType,
    Interpolation,
    GraphType,
    Download,
    Sync,
    VisibilityMode,
    ColumnDelimiter,
    BingLayerType,
)
from .popups import PopupInfo
from .forms import FormInfo
from .geometry import Extent
from .layer_definition import (
    AttributeTableInfo,
    LayerDefinition,
    EffectFunctions,
    EffectFunctions1,
    EffectFunctions2,
    EffectFunctions3,
    EffectFunctions4,
    RasterLayerDefinition,
    ScaleDependentEffect,
    FeatureEffect,
    LocationInfo,
    DefinitionEditor,
    Group,
    FeatureSet,
    FieldModel,
    MosaicRule,
    MultidimensionalSubset,
    RenderingRule,
    ExpressionInfo,
    TelemetryDisplay,
    ThematicGroup,
    OrientedImageryProperties,
    DimensionalDefinition,
    ExclusionArea,
    VideoLayerDrawingInfo,
    WebFeatureServiceInfo,
    Georeference,
    TileInfo,
    WebMapTileServiceInfo,
    BuildingSceneLayerFilter,
    LineOfSightObserver,
    LineOfSightTarget,
    VoxelLayerDefinition,
    DimensionSimpleStyle,
    LengthDimension,
    WCSInfo,
    Viewshed,
)
from .symbols import (
    SimpleLineSymbolEsriSLS,
    PictureMarkerSymbolEsriPMS,
    SimpleFillSymbolEsriSFS,
)

GroupLayerRef = ForwardRef("GroupLayer")


class WCSLayer(BaseModel):
    """
    OGC Web Coverage Service (WCS) is a dynamic image service that follows the specifications of OGC.
    """

    band_ids: list[int] | None = Field(
        None,
        alias="bandIds",
        description="The band IDs to display in the WCS layer.",
    )
    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    custom_parameters: dict[constr(pattern=r".*"), str] | None = Field(
        None,
        alias="customParameters",
        description="A sequence of custom parameters appended to the URL of all requests related to a layer.",
    )
    disable_popup: bool | None = Field(
        False,
        alias="disablePopup",
        description="Indicates whether to allow a client to ignore popups defined by the service item.",
    )
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
        title="Effect",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    interpolation: Interpolation | None = Field(
        None,
        description="The interpolation method for the WCS layer.",
    )
    is_reference: bool | None = Field(
        None,
        alias="isReference",
        description="Applicable if used as a baseMapLayer. A boolean value indicating whether or not the baseMapLayer draws on top (true) of other layers, including operationalLayers , or below (false).",
    )
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="Optional string containing the item ID if it's registered on ArcGIS Online or your organization's portal.",
    )
    layer_definition: RasterLayerDefinition | None = Field(
        None,
        alias="layerDefinition",
        description="A layerDefinition object defining the attribute schema and drawing information for the layer.",
    )
    layer_type: Literal["WCS"] = Field(
        "WCS",
        alias="layerType",
        description="String indicating the layer type.",
    )
    max_scale: float | None = Field(
        None,
        alias="maxScale",
        description="A number representing the maximum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    min_scale: float | None = Field(
        None,
        alias="minScale",
        description="A number representing the minimum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    multidimensional_definition: DimensionalDefinition | None = Field(
        None,
        alias="multidimensionalDefinition",
        description="The multidimensional definition for the WCS layer.",
    )
    multidimensional_subset: MultidimensionalSubset | None = Field(
        None,
        alias="multidimensionalSubset",
        description="The multidimensional subset for the WCS layer.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    popup_info: PopupInfo | None = Field(
        None,
        alias="popupInfo",
        description="A popupInfo object defining the content of popup windows when you click or query a feature.",
    )
    refresh_interval: int | None = Field(
        0,
        alias="refreshInterval",
        description="Refresh interval of the layer in minutes. Non-zero value indicates automatic layer refresh at the specified interval. Value of 0 indicates auto refresh is not enabled.",
    )
    show_legend: bool | None = Field(
        True,
        alias="showLegend",
        description="Boolean value indicating whether to display the layer in the legend. Default value is `true`.",
    )
    time_animation: bool | None = Field(
        True,
        alias="timeAnimation",
        description="This property is applicable to layers that support time. If 'true', timeAnimation is enabled.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    url: str = Field(
        ...,
        description="The URL to the layer. If the layer is not from a web service but rather a feature collection, then the url property is omitted.",
    )
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )
    visibility_time_extent: list[int | None] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Represents time extent that will control when a layer should be visible based on webmap's current time. Visibility time extent only affects the layer visibility and will not filter the data.",
    )
    wcs_info: WCSInfo = Field(
        ...,
        alias="wcsInfo",
        description="The WCS information for the WCS layer.",
    )


class WMSSubLayer(BaseModel):
    """
    A layer object may allow overrides on popup content and drawing behavior for individual layers of a web service.
    """

    model_config = common_config
    legend_url: str | None = Field(
        None,
        alias="legendUrl",
        description="A string URL to a legend graphic for the layer.",
    )
    name: str | None = Field(
        None, description="A string containing a unique name for the layer."
    )
    queryable: bool | None = Field(
        None,
        description="Boolean specifying whether a layer is queryable or not.",
    )
    show_popup: bool | None = Field(
        None,
        alias="showPopup",
        description="Boolean specifying whether to display popup or not.",
    )
    time_animation: bool | None = Field(
        True,
        alias="timeAnimation",
        description="Boolean specifying whether to animate time or not.",
    )
    title: str | None = Field(
        None,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )


class AnnotationLayer(BaseModel):
    """
    Annotation layers can be created by referencing a layer from a feature service. Annotation layers honor any feature templates configured in the source document.
    """

    model_config = common_config
    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    custom_parameters: dict[constr(pattern=r".*"), str] | None = Field(
        None,
        alias="customParameters",
        description="A sequence of custom parameters appended to the URL of all requests related to a layer.",
        title="customParameters",
    )
    disable_popup: bool | None = Field(
        False,
        alias="disablePopup",
        description="Indicates whether to allow a client to ignore popups defined by the service item.",
    )
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
        title="Effect",
    )
    enable_editing: bool | None = Field(
        False,
        alias="enableEditing",
        description="Indicates whether a client can add, remove or update features in the layer.",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    layer_definition: LayerDefinition | None = Field(
        None,
        alias="layerDefinition",
        description="A layerDefinition object defining the attribute schema and drawing information for the layer.",
    )
    layer_type: Literal["ArcGISAnnotationLayer"] = Field(
        "ArcGISAnnotationLayer",
        alias="layerType",
        description="String indicating the layer type.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    popup_info: PopupInfo | None = Field(
        None,
        alias="popupInfo",
        description="A popupInfo object defining the content of popup windows when you click or query a feature.",
    )
    refresh_interval: float | None = Field(
        0,
        alias="refreshInterval",
        description="Refresh interval of the layer in minutes. Non-zero value indicates automatic layer refresh at the specified interval. Value of 0 indicates auto refresh is not enabled.",
    )
    time_animation: bool | None = Field(
        True,
        alias="timeAnimation",
        description="Indicates whether to disable time animation if the layer supports it.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    url: str | None = Field(
        None,
        description="The URL to the layer. If the layer is not from a web service but rather a feature collection, then the url property is omitted.",
    )
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )
    visible_layers: list[int] | None = Field(
        None,
        alias="visibleLayers",
        description="An array of sublayer ids that should appear visible. These ids refer to subsets of the Annotation features in the AnnotationLayer, identified by their AnnotationClassId.",
    )


class CSVLayer(BaseModel):
    """
    The CSV layer type references a CSV or TXT file from a publicly-accessible web server. It then dynamically loads into the map at run time. The CSV layer will maintain a reference to the CSV resource.
    """

    model_config = common_config

    attribute_table_info: AttributeTableInfo | None = Field(
        None,
        alias="attributeTableInfo",
        description="An `attributeTableInfo` object defining how the data will be presented in tabular format.",
    )
    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    column_delimiter: ColumnDelimiter | None = Field(
        None,
        alias="columnDelimiter",
        description="A string defining the character used to separate columns in a CSV file.",
    )
    custom_parameters: dict[constr(pattern=r".*"), str] | None = Field(
        None,
        alias="customParameters",
        description="A sequence of custom parameters appended to the URL of all requests related to a layer.",
        title="customParameters",
    )
    disable_popup: bool | None = Field(
        False,
        alias="disablePopup",
        description="Indicates whether to allow a client to ignore popups defined by the service item.",
    )
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
        title="Effect",
    )
    feature_effect: FeatureEffect | None = Field(None, alias="featureEffect")
    id: str = Field(..., description="A unique identifying string for the layer.")
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="Optional string containing the item ID if it's registered on ArcGIS Online or your organization's portal.",
    )
    layer_definition: LayerDefinition | None = Field(
        None,
        alias="layerDefinition",
        description="A layerDefinition object defining the attribute schema and drawing information for the layer.",
    )
    layer_type: Literal["CSV"] = Field(
        "CSV",
        alias="layerType",
        description="String indicating the layer type.",
    )
    list_mode: ListMode | None = Field(
        ListMode.show,
        alias="listMode",
        description="To show or hide the layer in the layer list",
    )
    location_info: LocationInfo | None = Field(
        None,
        alias="locationInfo",
        description="A locationInfo object defining how location information will be retrieved from a CSV file.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    popup_info: PopupInfo | None = Field(
        None,
        alias="popupInfo",
        description="A popupInfo object defining the content of popup windows when you click or query a feature.",
    )
    refresh_interval: float | None = Field(
        0,
        alias="refreshInterval",
        description="Refresh interval of the layer in minutes. Non-zero value indicates automatic layer refresh at the specified interval. Value of 0 indicates auto refresh is not enabled.",
    )
    screen_size_perspective: bool | None = Field(
        True,
        alias="screenSizePerspective",
        description="Apply [perspective scaling](https://developers.arcgis.com/javascript/latest/api-reference/esri-layers-FeatureLayer.html#screenSizePerspectiveEnabled) to screen-size symbols.",
    )
    show_labels: bool | None = Field(
        False,
        alias="showLabels",
        description="Labels will display if this property is set to `true` and the layer also has a [labelingInfo](labelingInfo.md) property associated with it.",
    )
    show_legend: bool | None = Field(
        True,
        alias="showLegend",
        description="Boolean value indicating whether to display the layer in the legend. Default value is `true`.",
    )
    time_animation: bool | None = Field(
        None,
        alias="timeAnimation",
        description="Indicates whether to enable time animation for the layer.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    url: str = Field(..., description="The URL to the layer.")
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )
    visibility_time_extent: list[int | None] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Represents time extent that will control when a layer should be visible based on webmap's current time. Visibility time extent only affects the layer visibility and will not filter the data.",
        title="timeExtent",
    )


class DimensionLayer(BaseModel):
    """
    Dimension layers can be created by referencing a layer from a feature service. Dimension layers honor any feature templates configured in the source document.
    """

    model_config = common_config
    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    custom_parameters: dict[constr(pattern=r".*"), str] | None = Field(
        None,
        alias="customParameters",
        description="A sequence of custom parameters appended to the URL of all requests related to a layer.",
        title="customParameters",
    )
    dimensions: list[LengthDimension] | None = Field(
        None,
        description="A collection of dimension objects embedded in the layer.",
    )
    disable_popup: bool | None = Field(
        False,
        alias="disablePopup",
        description="Indicates whether to allow a client to ignore popups defined by the service item.",
    )
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
        title="Effect",
    )
    enable_editing: bool | None = Field(
        False,
        alias="enableEditing",
        description="Indicates whether a client can add, remove or update features in the layer.",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    layer_definition: LayerDefinition | None = Field(
        None,
        alias="layerDefinition",
        description="A layerDefinition object defining the attribute schema and drawing information for the layer.",
    )
    layer_type: Literal["ArcGISDimensionLayer"] = Field(
        "ArcGISDimensionLayer",
        alias="layerType",
        description="String indicating the layer type.",
    )
    list_mode: ListMode | None = Field(
        None,
        alias="listMode",
        description="To show or hide the layer in the layer list.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    popup_info: PopupInfo | None = Field(
        None,
        alias="popupInfo",
        description="A popupInfo object defining the content of popup windows when you click or query a feature.",
    )
    refresh_interval: float | None = Field(
        0,
        alias="refreshInterval",
        description="Refresh interval of the layer in minutes. Non-zero value indicates automatic layer refresh at the specified interval. Value of 0 indicates auto refresh is not enabled.",
    )
    style: DimensionSimpleStyle | None = Field(
        None,
        description="Specification of how dimensions and their labels are displayed.",
    )
    time_animation: bool | None = Field(
        True,
        alias="timeAnimation",
        description="Indicates whether to disable time animation if the layer supports it.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    url: str | None = Field(
        None,
        description="The URL to the layer. If the layer is not from a web service but rather a feature collection, then the url property is omitted.",
    )
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )
    visibility_time_extent: list[int | None] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Represents time extent that will control when a layer should be visible based on webmap's current time. Visibility time extent only affects the layer visibility and will not filter the data.",
        title="timeExtent",
    )


class Layer(BaseModel):
    """
    A layer object may allow overrides on popup content and drawing behavior for individual layers of a web service. This object also contains geographic features and their attributes when used in a feature collection.
    """

    model_config = common_config
    attribute_table_info: AttributeTableInfo | None = Field(
        None,
        alias="attributeTableInfo",
        description="An `attributeTableInfo` object defining how the data will be presented in tabular format.",
    )
    default_visibility: bool | None = Field(
        None,
        alias="defaultVisibility",
        description="Default visibility of the layers in the map service.",
    )
    definition_editor: DefinitionEditor | None = Field(
        None,
        alias="definitionEditor",
        description="An object that provides interactive filters.",
    )
    disable_popup: bool | None = Field(
        False,
        alias="disablePopup",
        description="Indicates whether to allow a client to ignore the popups defined on the layer. The popupInfo object could be saved in the map or item.",
    )
    feature_set: FeatureSet | None = Field(
        None,
        alias="featureSet",
        description="A featureSet object containing the geometry and attributes of the features in the layer.",
    )
    field: FieldModel | None = Field(
        None,
        description="Information about each field in a layer. Used with feature collections.",
    )
    id: int | str | None = Field(
        None, description="A unique identifying string or integer for the layer."
    )
    layer_definition: LayerDefinition | None = Field(
        None,
        alias="layerDefinition",
        description="The layerDefinition object defines the attribute schema and drawing information for the layer.",
    )
    layer_item_id: str | None = Field(
        None,
        alias="layerItemId",
        description="The associated query layer's itemId. Only available when there is a `layerUrl`.  You will see this if [popups are configured](https://doc.arcgis.com/en/arcgis-online/manage-data/publish-tiles-from-features.htm) on it.",
    )
    layer_url: str | None = Field(
        None,
        alias="layerUrl",
        description="A URL to a service that should be used for all queries against the layer.",
    )
    list_mode: ListMode | None = Field(
        None,
        alias="listMode",
        description="To show or hide the sublayer in the layer list. If the layer has sublayers, selecting `hide-children` will hide them in the layer list.",
    )
    max_scale: float | None = Field(
        None,
        alias="maxScale",
        description="A number representing the maximum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    min_scale: float | None = Field(
        None,
        alias="minScale",
        description="A number representing the minimum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    name: str | None = Field(None, description="The name of the layer.")
    next_object_id: int | None = Field(
        None,
        alias="nextObjectId",
        description="Iterates within a featureset. Number objectId value is incremented 1 based on last Object ID defined for the feature in a featureset. Used with feature collections.",
    )
    parent_layer_id: int | None = Field(
        None,
        alias="parentLayerId",
        description="If working with nested layers, this is the numeric value indicating the layer id of the next layer (parent) directly above the current referenced layer.",
    )
    popup_info: PopupInfo | None = Field(
        None,
        alias="popupInfo",
        description="A popupInfo object defining the popup window content for the layer.",
    )
    show_legend: bool | None = Field(
        True,
        alias="showLegend",
        description="Boolean value indicating whether to display the layer in the legend. Default value is `true`.",
    )
    sub_layer: int | None = Field(
        None,
        alias="subLayer",
        description="Integer value indicating the layer id.",
    )
    sub_layer_ids: list[int] | None = Field(
        None,
        alias="subLayerIds",
        description="If the layer is a parent layer, it will have one or more sub layers included in an array.",
    )
    title: str | None = Field(
        None,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )


class FeatureCollection(BaseModel):
    """
    An object defining a layer of features whose geometry and attributes will be stored directly within the web map. This is used when features are referenced by the client and no url is being used. Feature Collection can be generated from shapefiles, CSVs, GPX files, or map notes. Map notes allows you to add your own data directly to a map. With a map notes layer, you use features to symbolize something you want to show on your map. You can also add descriptive information that appears in pop-ups when the feature is clicked. Map notes are stored directly within the web map and not as a separate item.
    """

    model_config = common_config
    group_id_field: str | None = Field(
        None,
        alias="groupIdField",
        description="The name of the attribute field of features in the feature collection that contains group identifier. The identifier will be one of those specified in `groups`.",
    )
    groups: list[Group] | None = Field(
        None,
        description="Specifies the type of groups available in the feature collection.",
    )
    layers: list[Layer] | None = Field(
        None,
        description="An array of layer objects defining the styling, geometry, and attribute information for the features.",
    )
    show_legend: bool | None = Field(
        True,
        alias="showLegend",
        description="Boolean value indicating whether to display the layer in the legend. Default value is `true`.",
    )


class FeatureLayer(BaseModel):
    """
    Feature layers can be created by referencing a layer from either a map service or a feature service or by specifying a [feature collection](featureCollection.md) object. Use a map service if you just want to retrieve geometries and attributes from the server and symbolize them yourself. Use a feature service if you want to take advantage of symbols from the service's source map document. Also, use a feature service if you plan on doing editing with the feature layer. Feature layers honor any feature templates configured in the source map document. Feature collection objects are used to create a feature layer based on the supplied definition.
    """

    model_config = common_config

    attribute_table_info: AttributeTableInfo | None = Field(
        None,
        alias="attributeTableInfo",
        description="An `attributeTableInfo` object defining how the data will be presented in tabular format.",
    )
    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    capabilities: str | None = Field(
        None,
        description="A comma-separated string indicating whether feature editing is allowed. Lack of `Editing` in this string indicates editing is not allowed. Allowed values: `Query`, `Query,Sync`. This property is superseded by `layerDefinition.capabilities`.",
    )
    charts: list[dict[str, Any]] | None = Field(
        None,
        description="An array of chart items of type WebChart available on the feature layer.",
    )
    custom_parameters: dict[constr(pattern=r".*"), str] | None = Field(
        None,
        alias="customParameters",
        description="A sequence of custom parameters appended to the URL of all requests related to a layer.",
        title="customParameters",
    )
    definition_editor: DefinitionEditor | None = Field(
        None,
        alias="definitionEditor",
        description="Stores interactive filters.",
    )
    disable_popup: bool | None = Field(
        False,
        alias="disablePopup",
        description="Indicates whether to allow a client to ignore popups defined by the service item.",
    )
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
        title="Effect",
    )
    feature_collection: FeatureCollection | None = Field(
        None,
        alias="featureCollection",
        description="A `featureCollection` object defining a layer of features whose geometry and attributes are either stored directly within the web map or with an item. Feature Collections can be created from CSVs, shapefiles, GPX, or map notes.",
    )
    feature_collection_type: FeatureCollectionType | None = Field(
        None,
        alias="featureCollectionType",
        description="Indicates the type of features in the feature collection. If `featureCollectionType` is missing, it means the feature collection is a regular single-layer or multi-layer feature collection.",
    )
    feature_effect: FeatureEffect | None = Field(
        None,
        alias="featureEffect",
        description="Feature Effect emphasizes or deemphasizes features that satisfy a filter using graphical effects.",
    )
    form_info: FormInfo | None = Field(
        None,
        alias="formInfo",
        description="A formInfo object defining the content of the form when you are editing a feature.",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="Optional string containing the item ID of the service if it's registered on ArcGIS Online or your organization's portal.",
    )
    layer_definition: LayerDefinition | None = Field(
        None,
        alias="layerDefinition",
        description="A layerDefinition object defining the attribute schema and drawing information for the layer.",
    )
    layer_type: Literal["ArcGISFeatureLayer"] = Field(
        "ArcGISFeatureLayer",
        alias="layerType",
        description="String indicating the layer type.",
    )
    list_mode: ListMode | None = Field(
        ListMode.show,
        alias="listMode",
        description="To show or hide layers in the layer list",
    )
    mode: conint(ge=0, le=2) | None = Field(
        None,
        description="0 is snapshot mode. 1 is on-demand mode. 2 is selection-only mode. Used with ArcGIS feature services and individual layers in ArcGIS map services.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    path: constr(pattern=r"^file:.+\.geodatabase\?itemId=\d+$") | None = Field(
        None,
        description="For offline data, a path to a geodatabase file. A URI format is used starting with `file:` followed by a file system path with a `.geodatabase` extension. A query parameter `itemId` must also be present specifying the ID of a table in the geodatabase's `GDB_ServiceItems` table. A relative path must be from the file which defines the layer. For example `file:../p20/northamerica.geodatabase?itemId=1`.",
    )
    popup_info: PopupInfo | None = Field(
        None,
        alias="popupInfo",
        description="A popupInfo object defining the content of popup window when you click a feature on the map.",
    )
    refresh_interval: float | None = Field(
        0,
        alias="refreshInterval",
        description="Refresh interval of the layer in minutes. Non-zero value indicates automatic layer refresh at the specified interval. Value of 0 indicates auto refresh is not enabled.",
    )
    screen_size_perspective: bool | None = Field(
        True,
        alias="screenSizePerspective",
        description="Apply [perspective scaling](https://developers.arcgis.com/javascript/latest/api-reference/esri-layers-FeatureLayer.html#screenSizePerspectiveEnabled) to screen-size symbols.",
    )
    show_labels: bool | None = Field(
        False,
        alias="showLabels",
        description="Labels will display if this property is set to `true` and the layer also has a [labelingInfo](labelingInfo.md) property associated with it. This property can get stored in the web map config and in the item/data.",
    )
    show_legend: bool | None = Field(
        True,
        alias="showLegend",
        description="Boolean value indicating whether to display the layer in the legend. Default value is `true`.",
    )
    subtype_code: int | None = Field(
        None,
        alias="subtypeCode",
        description="The feature subtype code identifying the layer. Used with SubtypeGroupLayers.",
    )
    time_animation: bool | None = Field(
        True,
        alias="timeAnimation",
        description="Indicates whether to disable time animation if the layer supports it.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    url: str | None = Field(
        None,
        description="The URL to the layer. If the layer is not from a web service but rather a feature collection, then the url property is omitted.",
    )
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )
    visibility_time_extent: list[int | None] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Represents time extent that will control when a layer should be visible based on webmap's current time. Visibility time extent only affects the layer visibility and will not filter the data.",
        title="timeExtent",
    )
    visible_layers: list[int] | None = Field(
        None,
        alias="visibleLayers",
        description="An array of sublayer ids that should appear visible. Used with feature layers that are based on feature collections.",
    )


class GeoJSONLayer(BaseModel):
    """
    The GeoJSON layer type references a GeoJSON file from a publicly-accessible web server. It then dynamically loads into the map at run time. The GeoJSON layer will maintain a reference to the GeoJSON resource.
    """

    model_config = common_config

    attribute_table_info: AttributeTableInfo | None = Field(
        None,
        alias="attributeTableInfo",
        description="An `attributeTableInfo` object defining how the data will be presented in tabular format.",
    )
    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    custom_parameters: dict[constr(pattern=r".*"), str] | None = Field(
        None,
        alias="customParameters",
        description="A sequence of custom parameters appended to the URL of all requests related to a layer.",
        title="customParameters",
    )
    disable_popup: bool | None = Field(
        False,
        alias="disablePopup",
        description="Indicates whether to allow a client to ignore popups defined by the service item.",
    )
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
        title="Effect",
    )
    feature_effect: FeatureEffect | None = Field(None, alias="featureEffect")
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="Optional string containing the item ID if it's registered on ArcGIS Online or your organization's portal.",
    )
    layer_definition: LayerDefinition | None = Field(
        None,
        alias="layerDefinition",
        description="Additional properties that define drawing information and other configurations for the layer.",
    )
    layer_type: Literal["GeoJSON"] = Field(
        "GeoJSON",
        alias="layerType",
        description="String indicating the layer type.",
    )
    list_mode: ListMode | None = Field(
        ListMode.show,
        alias="listMode",
        description="To show or hide layers in the layer list",
    )
    max_scale: confloat(ge=0.0) | None = Field(
        None,
        alias="maxScale",
        description="A number representing the maximum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    min_scale: confloat(ge=0.0) | None = Field(
        None,
        alias="minScale",
        description="A number representing the minimum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    popup_info: PopupInfo | None = Field(
        None,
        alias="popupInfo",
        description="A popupInfo object defining the content of popup windows when you click or query a feature.",
    )
    refresh_interval: float | None = Field(
        0,
        alias="refreshInterval",
        description="Refresh interval of the layer in minutes. Non-zero value indicates automatic layer refresh at the specified interval. Value of 0 indicates auto refresh is not enabled.",
    )
    screen_size_perspective: bool | None = Field(
        True,
        alias="screenSizePerspective",
        description="Apply [perspective scaling](https://developers.arcgis.com/javascript/latest/api-reference/esri-layers-FeatureLayer.html#screenSizePerspectiveEnabled) to screen-size symbols.",
    )
    show_labels: bool | None = Field(
        False,
        alias="showLabels",
        description="Labels will display if this property is set to `true` and the layer also has a [labelingInfo](labelingInfo.md) property associated with it.",
    )
    show_legend: bool | None = Field(
        True,
        alias="showLegend",
        description="Boolean value indicating whether to display the layer in the legend. Default value is `true`.",
    )
    time_animation: bool | None = Field(
        False,
        alias="timeAnimation",
        description="Indicates whether to disable time animation if the layer supports it.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    url: str = Field(..., description="The URL to the layer.")
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )
    visibility_time_extent: list[int | None] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Represents time extent that will control when a layer should be visible based on webmap's current time. Visibility time extent only affects the layer visibility and will not filter the data.",
        title="timeExtent",
    )


class ImageServiceLayer(BaseModel):
    """
    An image service provides access to raster data through a web service. Multiple rasters can be served as one image service through mosaic dataset technology, dynamically processed and mosaicked on the fly. An image service supports accessing both the mosaicked image and its catalog, as well as individual rasters in the catalog. Also, image services can be cached (tiled) or uncached (dynamic). This object specifically details properties within uncached image services.
    """

    model_config = common_config
    band_ids: list[int] | None = Field(
        None,
        alias="bandIds",
        description="An array of bandIds that are visible, can specify bands to export or rearrange band order(from image service).",
    )
    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    compression_quality: confloat(ge=0.0, le=100.0) | None = Field(
        None,
        alias="compressionQuality",
        description="Controls how much loss the image will be subjected to by the compression algorithm (from image service).",
    )
    custom_parameters: dict[constr(pattern=r".*"), str] | None = Field(
        None,
        alias="customParameters",
        description="A sequence of custom parameters appended to the URL of all requests related to a layer.",
        title="customParameters",
    )
    definition_editor: DefinitionEditor | None = Field(
        None,
        alias="definitionEditor",
        description="Stores interactive filters.",
    )
    disable_popup: bool | None = Field(
        False,
        alias="disablePopup",
        description="Boolean property indicating whether to ignore popups defined by the service item.",
    )
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
        title="Effect",
    )
    format: ImageFormat | None = Field(
        ImageFormat.png,
        validate_default=True,
        description="String value representing image format.",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    interpolation: Interpolation | None = Field(
        None, description="The algorithm used for interpolation."
    )
    is_reference: bool | None = Field(
        None,
        alias="isReference",
        description="This is applicable if used as a baseMapLayer. A boolean value indicating whether or not the baseMapLayer draws on top (true) of other layers, including operationalLayers , or below (false).",
    )
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="Optional string containing the item ID of the service if it's registered on ArcGIS Online or your organization's portal.",
    )
    layer_definition: RasterLayerDefinition | None = Field(
        None, alias="layerDefinition"
    )
    layer_type: Literal["ArcGISImageServiceLayer"] = Field(
        "ArcGISImageServiceLayer",
        alias="layerType",
        description="String indicating the layer type.",
    )
    max_scale: confloat(ge=0.0) | None = Field(
        None,
        alias="maxScale",
        description="A number representing the maximum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    min_scale: confloat(ge=0.0) | None = Field(
        None,
        alias="minScale",
        description="A number representing the minimum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    mosaic_rule: MosaicRule | None = Field(
        None,
        alias="mosaicRule",
        description="Specifies the mosaic rule when defining how individual images should be mosaicked.",
    )
    multidimensional_subset: MultidimensionalSubset | None = Field(
        None,
        alias="multidimensionalSubset",
        description="An object representing a subset from multidimensional data. The data is based on slices or ranges in one or more dimensions in [mosaicRule.description](mosaicRule.md). When the [multidimensionalSubset](multidimensionalSubset.md) is defined then the [mosaicRule.multidimensionalDefinition](mosaicRule.md) must be within the defined multidimensionalSubset, otherwise nothing will be displayed.",
    )
    no_data: int | None = Field(
        None,
        alias="noData",
        description="The pixel value that represents no information.",
    )
    no_data_interpretation: NoDataInterpretation | None = Field(
        NoDataInterpretation.esri_no_data_match_any,
        alias="noDataInterpretation",
        description="A string value of interpretation of noData setting. Default is 'esriNoDataMatchAny' when noData is a number, and 'esriNoDataMatchAll' when noData is an array.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    pixel_type: PixelType | None = Field(
        None,
        alias="pixelType",
        description="Pertains to the type of values stored in the raster, such as signed integer, unsigned integer, or floating point.",
    )
    popup_info: PopupInfo | None = Field(
        None,
        alias="popupInfo",
        description="A popupInfo object defining the content of popup windows when you click or query a feature.",
    )
    refresh_interval: float | None = Field(
        0,
        alias="refreshInterval",
        description="Refresh interval of the layer in minutes. Non-zero value indicates automatic layer refresh at the specified interval. Value of 0 indicates auto refresh is not enabled.",
    )
    rendering_rule: RenderingRule | None = Field(
        None,
        alias="renderingRule",
        description="Specifies the rendering rule for how the requested image should be rendered.",
    )
    show_legend: bool | None = Field(
        True,
        alias="showLegend",
        description="Boolean value indicating whether to display the layer in the legend. Default value is `true`.",
    )
    time_animation: bool | None = Field(
        True,
        alias="timeAnimation",
        description="This property is applicable to layers that support time. If 'true', timeAnimation is enabled.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    url: str = Field(..., description="The URL to the layer.")
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )
    visibility_time_extent: list[int | None] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Represents time extent that will control when a layer should be visible based on webmap's current time. Visibility time extent only affects the layer visibility and will not filter the data.",
        title="timeExtent",
    )


class ImageServiceVectorLayer(BaseModel):
    """
    The imageServiceVectorLayer displays pixel values as vectors. To do this, the image service layer must be a two-band raster in which one band holds magnitude values and one band holds direction values. The imageServiceVectorLayer also supports time-enabled data.
    """

    model_config = common_config
    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    custom_parameters: dict[constr(pattern=r".*"), str] | None = Field(
        None,
        alias="customParameters",
        description="A sequence of custom parameters appended to the URL of all requests related to a layer.",
        title="customParameters",
    )
    definition_editor: DefinitionEditor | None = Field(
        None,
        alias="definitionEditor",
        description="Stores interactive filters.",
    )
    disable_popup: bool | None = Field(
        False,
        alias="disablePopup",
        description="Boolean property indicating whether to ignore popups defined by the service item.",
    )
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
        title="Effect",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    is_reference: bool | None = Field(
        None,
        alias="isReference",
        description="This is applicable if used as a baseMapLayer. A boolean value indicating whether or not the baseMapLayer draws on top (true) of other layers, including operationalLayers , or below (false).",
    )
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="A string containing the item ID of the service if it's registered with ArcGIS Online or your organization's portal.",
    )
    layer_definition: RasterLayerDefinition | None = Field(
        None, alias="layerDefinition"
    )
    layer_type: Literal["ArcGISImageServiceVectorLayer"] = Field(
        "ArcGISImageServiceVectorLayer",
        alias="layerType",
        description="String indicating the layer type.",
    )
    max_scale: confloat(ge=0.0) | None = Field(
        None,
        alias="maxScale",
        description="A number representing the maximum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    min_scale: confloat(ge=0.0) | None = Field(
        None,
        alias="minScale",
        description="A number representing the minimum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    mosaic_rule: MosaicRule | None = Field(
        None,
        alias="mosaicRule",
        description="Specifies the mosaic rule when defining how individual images should be mosaicked.",
    )
    multidimensional_subset: MultidimensionalSubset | None = Field(
        None,
        alias="multidimensionalSubset",
        description="An object representing a subset from multidimensional data. The data is based on slices or ranges in one or more dimensions in [mosaicRule.description](mosaicRule.md). When the [multidimensionalSubset](multidimensionalSubset.md) is defined then the [mosaicRule.multidimensionalDefinition](mosaicRule.md) must be within the defined multidimensionalSubset, otherwise nothing will be displayed.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    popup_info: PopupInfo | None = Field(
        None,
        alias="popupInfo",
        description="A popupInfo object defining the content of popup windows when you click or query a feature.",
    )
    show_legend: bool | None = Field(
        True,
        alias="showLegend",
        description="Boolean value indicating whether to display legend for this layer. Default value is 'true'.",
    )
    symbol_tile_size: float | None = Field(
        None,
        alias="symbolTileSize",
        description="Number describing the size of the tile.",
    )
    time_animation: bool | None = Field(
        True,
        alias="timeAnimation",
        description="This property is applicable to layers that support time. If 'true', timeAnimation is enabled.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    url: str = Field(..., description="The URL to the layer.")
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )
    visibility_time_extent: list[int | None] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Represents time extent that will control when a layer should be visible based on webmap's current time. Visibility time extent only affects the layer visibility and will not filter the data.",
        title="timeExtent",
    )


class MapServiceLayer(BaseModel):
    """
    ArcGIS web maps are designed to work with web services hosted on ArcGIS Server and ArcGIS Online, in addition to other types of servers. Map services can be cached (tiled) or uncached (dynamic). This object specifically details properties within uncached map services.
    """

    model_config = common_config
    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    custom_parameters: dict[constr(pattern=r".*"), str] | None = Field(
        None,
        alias="customParameters",
        description="A sequence of custom parameters appended to the URL of all requests related to a layer.",
        title="customParameters",
    )
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
        title="Effect",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    is_reference: bool | None = Field(
        None,
        alias="isReference",
        description="This is applicable if used as a baseMapLayer. A boolean value indicating whether or not the baseMapLayer draws on top (true) of other layers, including operationalLayers , or below (false).",
    )
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="Optional string containing the item ID of the service if it's registered on ArcGIS Online or your organization's portal.",
    )
    layers: list[Layer] | None = Field(
        None,
        description="An array of layer objects defining the styling, geometry, and attribute information for the features.",
    )
    layer_type: Literal["ArcGISMapServiceLayer"] = Field(
        "ArcGISMapServiceLayer",
        alias="layerType",
        description="String indicating the layer type.",
    )
    list_mode: ListMode | None = Field(
        ListMode.show,
        alias="listMode",
        description="To show or hide the sublayer in the layer list. If the layer has sublayers, selecting `hide-children` will hide them in the layer list.",
    )
    max_scale: confloat(ge=0.0) | None = Field(
        None,
        alias="maxScale",
        description="A number representing the maximum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    min_scale: confloat(ge=0.0) | None = Field(
        None,
        alias="minScale",
        description="A number representing the minimum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    refresh_interval: float | None = Field(
        0,
        alias="refreshInterval",
        description="Refresh interval of the layer in minutes. Non-zero value indicates automatic layer refresh at the specified interval. Value of 0 indicates auto refresh is not enabled.",
    )
    show_legend: bool | None = Field(
        None,
        alias="showLegend",
        description="Boolean value indicating whether to display the layer in the legend. Default value is `true`.",
    )
    thematic_group: ThematicGroup | None = Field(
        None,
        alias="thematicGroup",
        description="(Optional) A thematicGroup object used in [ArcGISMapServiceLayer layers](mapServiceLayer.md).",
    )
    time_animation: bool | None = Field(
        True,
        alias="timeAnimation",
        description="This property is applicable to layers that support time. If 'true', timeAnimation is enabled.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    url: str = Field(..., description="The URL to the layer.")
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )
    visibility_time_extent: list[int | None] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Represents time extent that will control when a layer should be visible based on webmap's current time. Visibility time extent only affects the layer visibility and will not filter the data.",
        title="timeExtent",
    )
    visible_layers: list[int] | None = Field(
        None,
        alias="visibleLayers",
        description="An array of sublayer ids that should appear visible. Used with map service layers that are not tiled.",
    )


class OGCFeatureLayer(BaseModel):
    """
    OGC Feature Layer is a dynamic feature service that follows the specifications of OGC API - Features.
    """

    model_config = common_config
    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    collection_id: str | None = Field(
        None,
        alias="collectionId",
        description="A unique identifying string for a feature collection.",
    )
    custom_parameters: dict[constr(pattern=r".*"), str] | None = Field(
        None,
        alias="customParameters",
        description="A sequence of parameters used to append custom or vendor specific parameters to all OGC API - Features requests.",
    )
    disable_popup: bool | None = Field(
        False,
        alias="disablePopup",
        description="Indicates whether popup is enabled or not.",
    )
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
        title="Effect",
    )
    feature_effect: FeatureEffect | None = Field(None, alias="featureEffect")
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="Unique string value indicating an item registered in ArcGIS Online or your organization's portal.",
    )
    layer_definition: LayerDefinition | None = Field(None, alias="layerDefinition")
    layer_type: Literal["OGCFeatureLayer"] = Field(
        "OGCFeatureLayer",
        alias="layerType",
        description="String indicating the layer type.",
    )
    max_scale: confloat(ge=0.0) | None = Field(
        0,
        alias="maxScale",
        description="A number representing the maximum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    min_scale: confloat(ge=0.0) | None = Field(
        0,
        alias="minScale",
        description="A number representing the minimum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    popup_info: PopupInfo | None = Field(
        None,
        alias="popupInfo",
        description="A popupInfo object defining the content of popup windows when you click or query a feature.",
    )
    refresh_interval: float | None = Field(
        None,
        alias="refreshInterval",
        description="Refresh interval of the layer in minutes. Non-zero value indicates automatic layer refresh at the specified interval. Value of 0 indicates auto refresh is not enabled.",
    )
    screen_size_perspective: bool | None = Field(
        True,
        alias="screenSizePerspective",
        description="Apply [perspective scaling](https://developers.arcgis.com/javascript/latest/api-reference/esri-layers-FeatureLayer.html#screenSizePerspectiveEnabled) to screen-size symbols.",
    )
    show_labels: bool | None = Field(
        False,
        alias="showLabels",
        description="Boolean value indicating whether to display labels for this layer.",
    )
    show_legend: bool | None = Field(
        True,
        alias="showLegend",
        description="Boolean value indicating whether to display the layer in the legend. Default value is `true`.",
    )
    time_animation: bool | None = Field(
        True,
        alias="timeAnimation",
        description="This property is applicable to layers that support time. If 'true', timeAnimation is enabled.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    url: str = Field(
        ...,
        description="The URL of the OGC API Features service landing page.",
    )
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )
    visibility_time_extent: list[int | None] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Represents time extent that will control when a layer should be visible based on webmap's current time. Visibility time extent only affects the layer visibility and will not filter the data.",
        title="timeExtent",
    )


class OrientedImageryLayer(BaseModel):
    """
    An Oriented Imagery layer is an extended point feature layer with additional properties that support the oriented imagery workflow. It is defined by camera locations (features) and has a geometry that allows it to be rendered in either a 2D MapView or 3D SceneView as a graphic with spatial context.
    """

    model_config = common_config
    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    capabilities: str | None = Field(
        None,
        description="A comma-separated string listing which editing operations are allowed on an editable feature service.",
    )
    charts: list[dict[str, Any]] | None = Field(
        None,
        description="An array of chart items of type WebChart available on the feature layer.",
    )
    custom_parameters: dict[constr(pattern=r".*"), str] | None = Field(
        None,
        alias="customParameters",
        description="A sequence of custom parameters appended to the URL of all requests related to a layer.",
        title="customParameters",
    )
    definition_editor: DefinitionEditor | None = Field(
        None,
        alias="definitionEditor",
        description="Stores interactive filters.",
    )
    disable_popup: bool | None = Field(
        False,
        alias="disablePopup",
        description="Indicates whether to allow a client to ignore popups defined by the service item.",
    )
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
        title="Effect",
    )
    feature_effect: FeatureEffect | None = Field(None, alias="featureEffect")
    form_info: FormInfo | None = Field(
        None,
        alias="formInfo",
        description="A formInfo object defining the content of the form when you are editing a feature.",
    )
    id: str = Field(..., description="A unique identifying string for the layer.")
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="Optional string containing the item ID of the service if it's registered on ArcGIS Online or your organization's portal.",
    )
    layer_definition: LayerDefinition | None = Field(
        None,
        alias="layerDefinition",
        description="A layerDefinition object defining the attribute schema and drawing information for the layer.",
    )
    layer_type: Literal["OrientedImageryLayer"] = Field(
        "OrientedImageryLayer",
        alias="layerType",
        description="String indicating the layer type.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    oriented_imagery_properties: OrientedImageryProperties | None = Field(
        None,
        alias="orientedImageryProperties",
        description="Object containing information about the chosen Oriented Imagery layer and schema.",
    )
    popup_info: PopupInfo | None = Field(
        None,
        alias="popupInfo",
        description="A popupInfo object defining the content of popup window when you click a feature on the map.",
    )
    refresh_interval: float | None = Field(
        0,
        alias="refreshInterval",
        description="Refresh interval of the layer in minutes. Non-zero value indicates automatic layer refresh at the specified interval. Value of 0 indicates auto refresh is not enabled.",
    )
    show_labels: bool | None = Field(
        False,
        alias="showLabels",
        description="Labels will display if this property is set to `true` and the layer also has a [labelingInfo](labelingInfo.md) property associated with it. This property can get stored in the web map config and in the item/data.",
    )
    show_legend: bool | None = Field(
        True,
        alias="showLegend",
        description="Boolean value indicating whether to display the layer in the legend. Default value is `true`.",
    )
    time_animation: bool | None = Field(
        True,
        alias="timeAnimation",
        description="Indicates whether to disable time animation if the layer supports it.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    url: str | None = Field(None, description="The URL to the layer.")
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )
    visibility_time_extent: list[int | None] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Represents time extent that will control when a layer should be visible based on webmap's current time. Visibility time extent only affects the layer visibility and will not filter the data.",
        title="timeExtent",
    )


class StreamLayer(BaseModel):
    """
    Properties specific to the ArcGISStreamLayer layer type.
    """

    model_config = common_config
    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    custom_parameters: dict[constr(pattern=r".*"), str] | None = Field(
        None,
        alias="customParameters",
        description="A sequence of custom parameters appended to the URL of all requests related to a layer.",
        title="customParameters",
    )
    definition_editor: DefinitionEditor | None = Field(
        None,
        alias="definitionEditor",
        description="Stores interactive filters.",
    )
    disable_popup: bool | None = Field(
        False,
        alias="disablePopup",
        description="Indicates whether to ignore popups defined by the service item.",
    )
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
        title="Effect",
    )
    feature_effect: FeatureEffect | None = Field(None, alias="featureEffect")
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="Optional string containing the item ID of the service if it's registered on ArcGIS Online or your organization's portal.",
    )
    layer_definition: LayerDefinition | None = Field(None, alias="layerDefinition")
    layer_type: Literal["ArcGISStreamLayer"] = Field(
        "ArcGISStreamLayer",
        alias="layerType",
        description="String indicating the layer type.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    popup_info: PopupInfo | None = Field(
        None,
        alias="popupInfo",
        description="A popupInfo object defining the content of pop-up windows when you click or query a feature.",
    )
    show_labels: bool | None = Field(
        False,
        alias="showLabels",
        description="Labels will display if this property is set to `true` and the layer also has a [labelingInfo](labelingInfo.md) property associated with it. This property can get stored in the web map config and in the item/data.",
    )
    show_legend: bool | None = Field(
        True,
        alias="showLegend",
        description="Boolean value indicating whether to display the layer in the legend. Default value is `true`.",
    )
    time_animation: bool | None = Field(
        True,
        alias="timeAnimation",
        description="Indicates whether to disable time animation if the layer supports it.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    url: str = Field(..., description="URL to the ArcGIS Server Stream Service.")
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )
    visibility_time_extent: list[int | None] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Represents time extent that will control when a layer should be visible based on webmap's current time. Visibility time extent only affects the layer visibility and will not filter the data.",
        title="timeExtent",
    )


class TiledImageServiceLayer(BaseModel):
    """
    An ArcGIS Tiled Image Service layer displays map content from an ArcGIS Server Image service that has been cached (tiled).
    """

    model_config = common_config
    band_ids: list[int] | None = Field(
        None,
        alias="bandIds",
        description="The band selection for a multispectral dataset",
    )
    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    custom_parameters: dict[constr(pattern=r".*"), str] | None = Field(
        None,
        alias="customParameters",
        description="A sequence of custom parameters appended to the URL of all requests related to a layer.",
        title="customParameters",
    )
    disable_popup: bool | None = Field(
        False,
        alias="disablePopup",
        description="Indicates whether to allow a client to ignore popups defined by the service item.",
    )
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
        title="Effect",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    interpolation: Interpolation | None = Field(
        None, description="String indicating the interpolation type."
    )
    is_reference: bool | None = Field(
        None,
        alias="isReference",
        description="Applicable if used as a baseMapLayer. A boolean value indicating whether or not the baseMapLayer draws on top (true) of other layers, including operationalLayers , or below (false).",
    )
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="Optional string containing the item ID of the service if it's registered on ArcGIS Online or your organization's portal.",
    )
    layer_definition: RasterLayerDefinition | None = Field(
        None, alias="layerDefinition"
    )
    layer_type: Literal["ArcGISTiledImageServiceLayer"] = Field(
        "ArcGISTiledImageServiceLayer",
        alias="layerType",
        description="String indicating the layer type.",
    )
    list_mode: ListMode | None = Field(
        ListMode.show,
        alias="listMode",
        description="To show or hide the sublayer in the layer list.",
    )
    max_scale: confloat(ge=0.0) | None = Field(
        None,
        alias="maxScale",
        description="A number representing the maximum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    min_scale: confloat(ge=0.0) | None = Field(
        None,
        alias="minScale",
        description="A number representing the minimum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    multidimensional_definition: list[DimensionalDefinition] | None = Field(
        None,
        alias="multidimensionalDefinition",
        description="An array of objects representing a slice from multidimensional data. The dimensional definitions in this array are used to filter display data based on slices in one or more dimensions.",
    )
    multidimensional_subset: MultidimensionalSubset | None = Field(
        None,
        alias="multidimensionalSubset",
        description="An object representing a subset from multidimensional data. This includes subsets of both variables and dimensions. When the multidimensionalSubset is defined on a tiled image service layer, the layer's [multidimensionalDefinition](multidimensionalDefinition.md) must be within the defined multidimensionalSubset, otherwise nothing will be displayed.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    popup_info: PopupInfo | None = Field(
        None,
        alias="popupInfo",
        description="A popupInfo object defining the content of popup windows when you click or query a feature.",
    )
    refresh_interval: float | None = Field(
        0,
        alias="refreshInterval",
        description="Refresh interval of the layer in minutes. Non-zero value indicates automatic layer refresh at the specified interval. Value of 0 indicates auto refresh is not enabled.",
    )
    rendering_rule: RenderingRule | None = Field(
        None,
        alias="renderingRule",
        description="Specifies the rendering rule for how the requested image should be rendered.",
    )
    show_legend: bool | None = Field(
        True,
        alias="showLegend",
        description="Boolean value indicating whether to display the layer in the legend. Default value is `true`.",
    )
    time_animation: bool | None = Field(
        True,
        alias="timeAnimation",
        description="This property is applicable to layers that support time. If 'true', timeAnimation is enabled.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    url: str = Field(..., description="URL to the ArcGIS Server Image Service.")
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )
    visibility_time_extent: list[int | None] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Represents time extent that will control when a layer should be visible based on webmap's current time. Visibility time extent only affects the layer visibility and will not filter the data.",
        title="timeExtent",
    )


class TiledMapServiceLayer(BaseModel):
    """
    An ArcGIS Tiled Map Service layer displays map content from an ArcGIS Server Map service that has been cached (tiled).
    """

    model_config = common_config
    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    custom_parameters: dict[constr(pattern=r".*"), str] | None = Field(
        None,
        alias="customParameters",
        description="A sequence of custom parameters appended to the URL of all requests related to a layer.",
        title="customParameters",
    )
    display_levels: int | None = Field(
        None,
        alias="displayLevels",
        description="NOTE: Applicable if used as a baseMapLayer. Integer value(s) indicating the display levels of the basemap layer. Only applicable for TiledMapService layers. All tiled map service layers should share the same tiling scheme. This property cannot be set via the Map Viewer UI.",
    )
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
        title="Effect",
    )
    exclusion_areas: list[ExclusionArea] | None = Field(
        None,
        alias="exclusionAreas",
        description="NOTE: Applicable if used as a baseMapLayer.  An array of exclusionArea objects defining the layer exclusions.",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    is_reference: bool | None = Field(
        None,
        alias="isReference",
        description="This property is applicable if used as a baseMapLayer. A boolean value indicating whether or not the baseMapLayer draws on top (true) of other layers, including operationalLayers , or below (false).",
    )
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="Optional string containing the item ID of the service if it's registered on ArcGIS Online or your organization's portal.",
    )
    layers: list[Layer] | None = Field(
        None,
        description="An array of layer objects defining a URL for queries and the popup window content.",
    )
    layer_type: Literal["ArcGISTiledMapServiceLayer"] = Field(
        "ArcGISTiledMapServiceLayer",
        alias="layerType",
        description="String indicating the layer type.",
    )
    list_mode: ListMode | None = Field(
        ListMode.show,
        alias="listMode",
        description="To show or hide the sublayer in the layer list. If the layer has sublayers, selecting `hide-children` will hide them in the layer list.",
    )
    max_scale: confloat(ge=0.0) | None = Field(
        None,
        alias="maxScale",
        description="A number representing the maximum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    min_scale: confloat(ge=0.0) | None = Field(
        None,
        alias="minScale",
        description="A number representing the minimum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    refresh_interval: float | None = Field(
        0,
        alias="refreshInterval",
        description="Refresh interval of the layer in minutes. Non-zero value indicates automatic layer refresh at the specified interval. Value of 0 indicates auto refresh is not enabled.",
    )
    show_legend: bool | None = Field(
        None,
        alias="showLegend",
        description="Boolean value indicating whether to display the map service layer in the legend.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents. If this is not included, a title is derived from the service.",
    )
    url: str = Field(..., description="URL to the ArcGIS Server tiled Map Service")
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the map.",
    )
    visibility_time_extent: list[int | None] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Represents time extent that will control when a layer should be visible based on webmap's current time. Visibility time extent only affects the layer visibility and will not filter the data.",
        title="timeExtent",
    )


class WFSLayer(BaseModel):
    """
    OGC Web Feature Service (WFS) is a dynamic feature service that follows the specifications of OGC.
    """

    model_config = common_config
    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    disable_popup: bool | None = Field(
        False,
        alias="disablePopup",
        description="Indicates whether to allow a client to ignore popups defined by the service item.",
    )
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
        title="Effect",
    )
    feature_effect: FeatureEffect | None = Field(None, alias="featureEffect")
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="Unique string value indicating an item registered in ArcGIS Online or your organization's portal.",
    )
    layer_definition: LayerDefinition | None = Field(None, alias="layerDefinition")
    layer_type: Literal["WFS"] = Field(
        "WFS",
        alias="layerType",
        description="String indicating the layer type.",
    )
    list_mode: ListMode | None = Field(
        ListMode.show,
        alias="listMode",
        description="To show or hide the sublayer in the layer list. If the layer has sublayers, selecting `hide-children` will hide them in the layer list.",
    )
    max_scale: confloat(ge=0.0) | None = Field(
        None,
        alias="maxScale",
        description="A number representing the maximum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    min_scale: confloat(ge=0.0) | None = Field(
        None,
        alias="minScale",
        description="A number representing the minimum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    mode: conint(ge=0, le=1) | None = Field(
        None,
        description="Number where 0 means 'snapshot' and 1 means 'ondemand'.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    popup_info: PopupInfo | None = Field(
        None,
        alias="popupInfo",
        description="A popupInfo object defining the content of popup windows when you click or query a feature.",
    )
    refresh_interval: float | None = Field(
        0,
        alias="refreshInterval",
        description="Refresh interval of the layer in minutes. Non-zero value indicates automatic layer refresh at the specified interval. Value of 0 indicates auto refresh is not enabled.",
    )
    screen_size_perspective: bool | None = Field(
        True,
        alias="screenSizePerspective",
        description="Apply [perspective scaling](https://developers.arcgis.com/javascript/latest/api-reference/esri-layers-FeatureLayer.html#screenSizePerspectiveEnabled) to screen-size symbols.",
    )
    show_labels: bool | None = Field(
        False,
        alias="showLabels",
        description="Labels will display if this property is set to `true` and the layer also has a [labelingInfo](labelingInfo.md) property associated with it. This property can get stored in the web map config and in the item/data.",
    )
    show_legend: bool | None = Field(
        True,
        alias="showLegend",
        description="Boolean value indicating whether to display the layer in the legend. Default value is `true`.",
    )
    time_animation: bool | None = Field(
        True,
        alias="timeAnimation",
        description="This property is applicable to layers that support time. If 'true', timeAnimation is enabled.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    url: str = Field(
        ...,
        description="The URL to the layer. If the layer is not from a web service but rather a feature collection, than the url property is omitted.",
    )
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )
    visibility_time_extent: list[int | None] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Represents time extent that will control when a layer should be visible based on webmap's current time. Visibility time extent only affects the layer visibility and will not filter the data.",
        title="timeExtent",
    )
    wfs_info: WebFeatureServiceInfo | None = Field(
        None,
        alias="wfsInfo",
        description="Object that defines and provides information about layers in a WFS service.",
    )


class Table(BaseModel):
    """
    A table is a non-spatial dataset in a feature service or map service. A table can be created by specifying a URL to a table resource in a feature service or map service, with optional reference to a portal item that contains additional property overrides.
    """

    model_config = common_config
    attribute_table_info: AttributeTableInfo | None = Field(
        None,
        alias="attributeTableInfo",
        description="An `attributeTableInfo` object defining how the data will be presented in tabular format.",
    )
    capabilities: str | None = Field(
        None,
        description="A comma-separated string indicating whether feature editing is allowed. Allowed values: `Query`, `Query,Sync`. Lack of `Editing` in this string indicates editing is not allowed. This property is superceded by `layerDefinition.capabilities`.",
    )
    charts: list[dict[str, Any]] | None = Field(
        None,
        description="A list of chart objects defining the charts to be displayed for the table.",
    )
    custom_parameters: dict[constr(pattern=r".*"), str] | None = Field(
        None,
        alias="customParameters",
        description="A sequence of custom parameters appended to the URL of all requests related to a layer.",
        title="customParameters",
    )
    definition_editor: DefinitionEditor | None = Field(
        None,
        alias="definitionEditor",
        description="Object indicating the definitionEditor used as a table's interactive filter.",
    )
    disable_popup: bool | None = Field(
        False,
        alias="disablePopup",
        description="Indicates whether to allow a client to ignore popups defined by the service item.",
    )
    form_info: FormInfo | None = Field(
        None,
        alias="formInfo",
        description="A formInfo object defining the content of the form when you are editing a record.",
    )
    id: str | None = Field(None, description="Unique string identifier for the table.")
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="Optional string containing the item ID of a portal item registered on ArcGIS Online or your organization's portal.",
    )
    layer_definition: LayerDefinition | None = Field(
        None,
        alias="layerDefinition",
        description="A layerDefinition object defining the attribute schema and other related information specific to the table.",
    )
    popup_info: PopupInfo | None = Field(
        None,
        alias="popupInfo",
        description="An object defining the content of popup windows when you query a record and the sort option for child related records.",
    )
    refresh_interval: float | None = Field(
        0,
        alias="refreshInterval",
        description="Refresh interval of the table in minutes. Non-zero value indicates automatic table refresh at the specified interval. Value of 0 indicates auto refresh is not enabled.",
    )
    title: str = Field(..., description="String value for the title of the table.")
    url: str = Field(
        ...,
        description="String value indicating the URL reference of the hosted table.",
    )


class FootprintLayer(BaseModel):
    """
    A footprint layer represents polygon features representing footprints. It has its own display and editing properties, and can exist only as a child of a catalog layer.
    """

    model_config = common_config

    attribute_table_info: AttributeTableInfo | None = Field(
        None,
        alias="attributeTableInfo",
        description="An `attributeTableInfo` object defining how the data will be presented in tabular format.",
    )
    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    charts: list[dict[str, Any]] | None = Field(
        None,
        description="An array of chart items of type WebChart available on the footprint layer.",
    )
    disable_popup: bool | None = Field(
        False,
        alias="disablePopup",
        description="Indicates whether a client should ignore popups defined in this layer",
    )
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
        title="Effect",
    )
    enable_editing: bool | None = Field(
        True,
        alias="enableEditing",
        description="Indicates whether a client should allow feature editing for this layer. Applicable only if the layer has editing capability.",
    )
    feature_effect: FeatureEffect | None = Field(None, alias="featureEffect")
    form_info: FormInfo | None = Field(
        None,
        alias="formInfo",
        description="A formInfo object defining the content of the form when you are editing a feature.",
    )
    id: str = Field(..., description="A unique identifying string for the layer.")
    layer_definition: LayerDefinition | None = Field(
        None,
        alias="layerDefinition",
        description="A layerDefinition object defining the attribute schema and drawing information for the layer.",
    )
    list_mode: ListMode | None = Field(
        ListMode.show,
        alias="listMode",
        description="To show or hide the footprint layer in the layer list",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    popup_info: PopupInfo | None = Field(
        None,
        alias="popupInfo",
        description="A popupInfo object defining the content of popup window when you click a feature on the map.",
    )
    show_labels: bool | None = Field(
        False,
        alias="showLabels",
        description="Labels will display if this property is set to `true` and the layer also has a [labelingInfo](labelingInfo.md) property associated with it. This property can get stored in the web map config and in the item/data.",
    )
    show_legend: bool | None = Field(
        True,
        alias="showLegend",
        description="Boolean value indicating whether to display the layer in the legend. Default value is `true`.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )


class KnowledgeGraphSubLayer(BaseModel):
    """
    Knowledge graph sub layer.
    """

    model_config = common_config
    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    charts: list[dict[str, Any]] | None = Field(
        None,
        description="An array of chart items of type WebChart available on the KnowledgeGraphSubLayer.",
    )
    disable_popup: bool | None = Field(
        False,
        alias="disablePopup",
        description="Indicates whether to allow a client to ignore popups defined by the service item.",
    )
    display_expression_info: ExpressionInfo | None = Field(
        None,
        alias="displayExpressionInfo",
        description="Object defining Arcade expression that will return a display name used for listing entities or relationships. This Arcade expression profile expects the returnType to be always a string.",
    )
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
        title="Effect",
    )
    feature_effect: FeatureEffect | None = Field(None, alias="featureEffect")
    graph_type: GraphType = Field(
        ...,
        alias="graphType",
        description="Indicates the type of graph object.",
    )
    graph_type_name: str = Field(
        ...,
        alias="graphTypeName",
        description="A unique identifying knowledge graph entity or relationship type name.",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    layer_definition: LayerDefinition | None = Field(
        None,
        alias="layerDefinition",
        description="A layerDefinition object defining the attribute schema and drawing information for the layer.",
    )
    layer_type: Literal["KnowledgeGraphSubLayer"] = Field(
        "KnowledgeGraphSubLayer",
        alias="layerType",
        description="String indicating the layer type.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    popup_info: PopupInfo | None = Field(
        None,
        alias="popupInfo",
        description="A popupInfo object defining the content of popup window when you click a feature on the map.",
    )
    refresh_interval: float | None = Field(
        0,
        alias="refreshInterval",
        description="Refresh interval of the layer in minutes. Non-zero value indicates automatic layer refresh at the specified interval. Value of 0 indicates auto refresh is not enabled.",
    )
    show_labels: bool | None = Field(
        False,
        alias="showLabels",
        description="Labels will display if this property is set to `true` and the layer also has a [labelingInfo](labelingInfo.md) property associated with it. This property can get stored in the web map config and in the item/data.",
    )
    show_legend: bool | None = Field(
        True,
        alias="showLegend",
        description="Boolean value indicating whether to display the layer in the legend. Default value is `true`.",
    )
    time_animation: bool | None = Field(
        True,
        alias="timeAnimation",
        description="Indicates whether to disable time animation if the layer supports it.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )


class KnowledgeGraphSubTable(BaseModel):
    """
    A table is a non-spatial dataset in a Knowledge Graph Server. A table can be created by specifying a graph type name in a Knowledge Graph Server.
    """

    model_config = common_config
    display_expression_info: ExpressionInfo | None = Field(
        None,
        alias="displayExpressionInfo",
        description="Object defining Arcade expression that will return a display name used for listing entities or relationships. This Arcade expression profile expects the returnType to be always a string.",
    )
    graph_type: GraphType = Field(
        ...,
        alias="graphType",
        description="Indicates the type of graph object.",
    )
    graph_type_name: str = Field(
        ...,
        alias="graphTypeName",
        description="A unique identifying knowledge graph entity type or relationship name.",
    )
    id: str | None = Field(None, description="Unique string identifier for the table.")
    layer_definition: LayerDefinition | None = Field(
        None,
        alias="layerDefinition",
        description="A layerDefinition object defining the attribute schema and drawing information for the layer.",
    )
    layer_type: Literal["KnowledgeGraphSubTable"] = Field(
        "KnowledgeGraphSubTable",
        alias="layerType",
        description="String indicating the table type.",
    )
    popup_info: PopupInfo | None = Field(
        None,
        alias="popupInfo",
        description="An object defining the content of popup windows when you query a record and the sort option for child related records.",
    )
    refresh_interval: float | None = Field(
        0,
        alias="refreshInterval",
        description="Refresh interval of the table in minutes. Non-zero value indicates automatic table refresh at the specified interval. Value of 0 indicates auto refresh is not enabled.",
    )
    title: str = Field(..., description="String value for the title of the table.")


class KnowledgeGraphLayer(BaseModel):
    """
    Knowledge graph layer can be created by referencing a Knowledge Graph Service. A Knowledge graph layer is a group layer with collections of feature layers and tables representing geospatial and non-geospatial entity and relationship types.
    """

    model_config = common_config
    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    custom_parameters: dict[constr(pattern=r".*"), str] | None = Field(
        None,
        alias="customParameters",
        description="A sequence of custom parameters appended to the URL of all requests related to a layer.",
        title="customParameters",
    )
    definition_set_map: str | None = Field(
        None,
        alias="definitionSetMap",
        description="A uri pointing to a resource containing graphTypeName to object identifier set dictionary persisted as pbf. The object identifier set for the corresponding graphTypeName will be used to filter features in each sub layer or table. If the graphTypeName is missing, all features are included.  Only graphTypeNames of sub layers and sub tables included in this Knowledge Graph Layer are relevant. Missing definitionSetMap is interpreted as an empty definitionSetMap, which implies all features are included for each relevant sub layer or table.",
    )
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
        title="Effect",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="Optional string containing the item ID of the service if it's registered on ArcGIS Online or your organization's portal.",
    )
    layers: list[KnowledgeGraphSubLayer] | None = Field(
        None,
        description="An array of Knowledge Graph sub layers, each representing graph entity type or relationship in knowledge graph server.",
    )
    layer_type: Literal["KnowledgeGraphLayer"] = Field(
        "KnowledgeGraphLayer",
        alias="layerType",
        description="String indicating the layer type.",
    )
    max_scale: confloat(ge=0.0) | None = Field(
        0,
        alias="maxScale",
        description="A number representing the maximum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    min_scale: confloat(ge=0.0) | None = Field(
        0,
        alias="minScale",
        description="A number representing the minimum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    refresh_interval: float | None = Field(
        0,
        alias="refreshInterval",
        description="Refresh interval of the layer in minutes. Non-zero value indicates automatic layer refresh at the specified interval. Value of 0 indicates auto refresh is not enabled.",
    )
    tables: list[KnowledgeGraphSubTable] | None = Field(
        None,
        description="An array of Knowledge Graph sub tables, each representing non spatial entity type or relationship in knowledge graph server.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    url: str = Field(..., description="The URL to the KnowledgeGraphServer.")
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )
    visibility_time_extent: list[int | None] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Represents time extent that will control when a layer should be visible based on webmap's current time. Visibility time extent only affects the layer visibility and will not filter the data.",
        title="timeExtent",
    )


class SubtypeLayer(BaseModel):
    """
    A subtype layer represents a subtype defined in a feature service layer. It has its own display and editing properties, and can exist only as a child of a subtype group layer.
    """

    model_config = common_config
    attribute_table_info: AttributeTableInfo | None = Field(
        None,
        alias="AttributeTableInfo",
        description="An `attributeTableInfo` object defining how the data will be presented in tabular format.",
    )
    charts: list[dict[str, Any]] | None = Field(
        None,
        description="An array of chart items of type WebChart available on the subtype layer.",
    )
    disable_popup: bool | None = Field(
        False,
        alias="disablePopup",
        description="Indicates whether a client should ignore popups defined in this layer",
    )
    enable_editing: bool | None = Field(
        True,
        alias="enableEditing",
        description="Indicates whether a client should allow feature editing for this layer. Applicable only if the layer has editing capability.",
    )
    form_info: FormInfo | None = Field(
        None,
        alias="formInfo",
        description="A formInfo object defining the content of the form when you are editing a feature.",
    )
    id: str = Field(..., description="A unique identifying string for the layer.")
    layer_definition: LayerDefinition | None = Field(
        None,
        alias="layerDefinition",
        description="A layerDefinition object defining the attribute schema and drawing information for the layer.",
    )
    layer_type: Literal["ArcGISFeatureLayer"] = Field(
        "ArcGISFeatureLayer",
        alias="layerType",
        description="String indicating the layer type.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    popup_info: PopupInfo | None = Field(
        None,
        alias="popupInfo",
        description="A popupInfo object defining the content of popup window when you click a feature on the map.",
    )
    show_labels: bool | None = Field(
        False,
        alias="showLabels",
        description="Labels will display if this property is set to `true` and the layer also has a [labelingInfo](labelingInfo.md) property associated with it. This property can get stored in the web map config and in the item/data.",
    )
    show_legend: bool | None = Field(
        True,
        alias="showLegend",
        description="Boolean value indicating whether to display the layer in the legend. Default value is `true`.",
    )
    subtype_code: int = Field(
        ...,
        alias="subtypeCode",
        description="The feature subtype code identifying the layer. Used with SubtypeGroupLayers.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )


class SubtypeTable(BaseModel):
    """
    A subtype table represents a subtype defined in a feature service table. It has its own display and editing properties, and can exist only as a child of a subtype group table.
    """

    model_config = common_config
    attribute_table_info: AttributeTableInfo | None = Field(
        None,
        alias="attributeTableInfo",
        description="An `attributeTableInfo` object defining how the data will be presented in tabular format.",
    )
    charts: list[dict[str, Any]] | None = Field(
        None,
        description="An array of chart items of type WebChart available on the subtype table.",
    )
    disable_popup: bool | None = Field(
        False,
        alias="disablePopup",
        description="Indicates whether a client should ignore popups defined in this table",
    )
    enable_editing: bool | None = Field(
        True,
        alias="enableEditing",
        description="Indicates whether a client should allow feature editing for this table. Applicable only if the table has editing capability.",
    )
    form_info: FormInfo | None = Field(
        None,
        alias="formInfo",
        description="A formInfo object defining the content of the form when you are editing a feature.",
    )
    id: str = Field(..., description="A unique identifying string for the table.")
    layer_definition: LayerDefinition | None = Field(
        None,
        alias="layerDefinition",
        description="A layerDefinition object defining the attribute schema and other related information specific to the table.",
    )
    popup_info: PopupInfo | None = Field(
        None,
        alias="popupInfo",
        description="A popupInfo object defining the content of popup window when you click a feature on the map.",
    )
    subtype_code: int = Field(
        ...,
        alias="subtypeCode",
        description="The feature subtype code identifying the table. Used with Subtype Group Tables.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the table that can be used in a table of contents.",
    )


class SubtypeGroupTable(BaseModel):
    """
    Subtype group tables can be created by referencing a table from a feature service that contains subtypes.  Each subtype in the feature service table can be a separate table in the subtype group table, and be given its own display and editing properties.<br><br>With respect to layer blending, subtype group layers follow the same rules as a typical [group layers](groupLayer.md).
    """

    model_config = common_config
    custom_parameters: dict[constr(pattern=r".*"), str] | None = Field(
        None,
        alias="customParameters",
        description="A sequence of custom parameters appended to the URL of all requests related to a layer.",
        title="customParameters",
    )
    id: str | None = Field(None, description="Unique string identifier for the table.")
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="Optional string containing the item ID of a portal item registered on ArcGIS Online or your organization's portal.",
    )
    layer_definition: LayerDefinition | None = Field(
        None,
        alias="layerDefinition",
        description="A layerDefinition object defining the definition of the group table that apply to all sub tables.",
    )
    layer_type: Literal["SubtypeGroupTable"] = Field(
        "SubtypeGroupTable",
        alias="layerType",
        description="String indicating the table type.",
    )
    refresh_interval: float | None = Field(
        0,
        alias="refreshInterval",
        description="Refresh interval of the table in minutes. Non-zero value indicates automatic table refresh at the specified interval. Value of 0 indicates auto refresh is not enabled.",
    )
    tables: list[SubtypeTable] | None = Field(
        None,
        description="An array of objects representing non-spatial datasets used in the web map.",
    )
    title: str = Field(..., description="String value for the title of the table.")
    url: str = Field(
        ...,
        description="String value indicating the URL reference of the hosted table.",
    )


class SubtypeGroupLayer(BaseModel):
    """
    Subtype group layers can be created by referencing a layer from a feature service that contains subtypes.  Each subtype in the feature service layer can be a separate layer in the subtype group layer, and be given its own display and editing properties.<br><br>With respect to layer blending, subtype group layers follow the same rules as [group layers](groupLayer.md).
    """

    model_config = common_config
    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    custom_parameters: dict[constr(pattern=r".*"), str] | None = Field(
        None,
        alias="customParameters",
        description="A sequence of custom parameters appended to the URL of all requests related to a layer.",
        title="customParameters",
    )
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
        title="Effect",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="Optional string containing the item ID of the service if it's registered on ArcGIS Online or your organization's portal.",
    )
    layer_definition: LayerDefinition | None = Field(
        None,
        alias="layerDefinition",
        description="A layerDefinition object defining the attribute schema and drawing information for the layer.",
    )
    layers: list[SubtypeLayer] | None = Field(
        None,
        description="An array of subtype layers, each describing the properties for a subtype in the feature service layer.",
    )
    layer_type: Literal["SubtypeGroupLayer"] = Field(
        "SubtypeGroupLayer",
        alias="layerType",
        description="String indicating the layer type.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    refresh_interval: float | None = Field(
        0,
        alias="refreshInterval",
        description="Refresh interval of the layer in minutes. Non-zero value indicates automatic layer refresh at the specified interval. Value of 0 indicates auto refresh is not enabled.",
    )
    time_animation: bool | None = Field(
        True,
        alias="timeAnimation",
        description="Indicates whether to disable time animation if the layer supports it.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    url: str | None = Field(
        None,
        description="The URL to the layer. If the layer is not from a web service but rather a feature collection, then the url property is omitted.",
    )
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )
    visibility_time_extent: list[int | None] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Represents time extent that will control when a layer should be visible based on webmap's current time. Visibility time extent only affects the layer visibility and will not filter the data.",
        title="timeExtent",
    )


class CatalogDynamicGroupLayer(BaseModel):
    """
    A dynamic group layer that is used in Catalog Layer that allows
    """

    model_config = common_config
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
        title="Effect",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    list_mode: ListMode | None = Field(
        ListMode.show,
        alias="listMode",
        description="To show or hide the group layer in the layer list",
    )
    maximum_visible_sublayers: int | None = Field(
        10,
        alias="maximumVisibleSublayers",
        description=" Gets or sets upper bound for number of layers in view for the dynamic group layer.",
    )
    max_scale: confloat(ge=0.0) | None = Field(
        0,
        alias="maxScale",
        description="A number representing the maximum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    min_scale: confloat(ge=0.0) | None = Field(
        0,
        alias="minScale",
        description="A number representing the minimum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    show_legend: bool = Field(
        True,
        alias="showLegend",
        description="Boolean value indicating whether to display the legends for sub layers that are dynamically loaded. Default value is `true`.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )


class CatalogLayer(BaseModel):
    """
    Catalog layers can be created by referencing a feature service layer where the type is a 'Catalog Layer'. Catalog Layer helps visually explore the footprints of disperate layer types stored as references along with footprint in shape field and other attributes relavent to each layer reference.
    """

    model_config = common_config
    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    custom_parameters: dict[constr(pattern=r".*"), str] | None = Field(
        None,
        alias="customParameters",
        description="A sequence of custom parameters appended to the URL of all requests related to a layer.",
        title="customParameters",
    )
    dynamic_group_layer: CatalogDynamicGroupLayer | None = Field(
        None,
        alias="dynamicGroupLayer",
        description="Object representing the dynamic group layer that loads layers selectively based on the current map extent, their scale visibility, time and range.",
    )
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
        title="Effect",
    )
    footprint_layer: FootprintLayer | None = Field(
        None,
        alias="footprintLayer",
        description="Object representing the dynamic group layer to visualize the catalog layer items as a polygon feature layer.",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="Optional string containing the item ID of the service if it's registered on ArcGIS Online or your organization's portal.",
    )
    layer_definition: LayerDefinition | None = Field(
        None,
        alias="layerDefinition",
        description="A layerDefinition object defining the attribute schema and drawing information for the layer.",
    )
    layer_type: Literal["CatalogLayer"] = Field(
        "CatalogLayer",
        alias="layerType",
        description="String indicating the layer type.",
    )
    list_mode: ListMode | None = Field(
        ListMode.show,
        alias="listMode",
        description="To show or hide the group layer in the layer list",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    refresh_interval: float | None = Field(
        0,
        alias="refreshInterval",
        description="Refresh interval of the layer in minutes. Non-zero value indicates automatic layer refresh at the specified interval. Value of 0 indicates auto refresh is not enabled.",
    )
    show_legend: bool = Field(
        True,
        alias="showLegend",
        description="Boolean value indicating whether to display the layer in the legend. Default value is `true`.",
    )
    time_animation: bool | None = Field(
        True,
        alias="timeAnimation",
        description="Indicates whether to disable time animation if the layer supports it.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    url: str | None = Field(None, description="The URL to the layer.")
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )
    visibility_time_extent: list[int | None] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Represents time extent that will control when a layer should be visible based on webmap's current time. Visibility time extent only affects the layer visibility and will not filter the data.",
        title="timeExtent",
    )


class GeoRSSLayer(BaseModel):
    """
    GeoRSS feeds may contain any combination of points, lines, and polygons. Web clients use a GeoRSS to JSON request service. This service returns one to many feature collections with different geometry types. The returned JSON specifies the point, lines, and polygons symbols used to display the features in that layer.
    """

    model_config = common_config
    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
        title="Effect",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    layer_type: Literal["GeoRSS"] = Field(
        "GeoRSS",
        alias="layerType",
        description="String indicating the layer type.",
    )
    line_symbol: SimpleLineSymbolEsriSLS | None = Field(
        None,
        alias="lineSymbol",
        description="Defined by the GeoRSS to JSON request service. If the GeoRSS feed does not have lines, this property is not added to the layer JSON.",
    )
    max_scale: confloat(ge=0.0) | None = Field(
        None,
        alias="maxScale",
        description="A number representing the maximum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    min_scale: confloat(ge=0.0) | None = Field(
        None,
        alias="minScale",
        description="A number representing the minimum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    point_symbol: PictureMarkerSymbolEsriPMS | None = Field(
        None,
        alias="pointSymbol",
        description="Defined by the GeoRSS to JSON request service. If the GeoRSS feed does not have points, this property is not added to the layer JSON.",
    )
    polygon_symbol: SimpleFillSymbolEsriSFS | None = Field(
        None,
        alias="polygonSymbol",
        description="Defined by the GeoRSS to JSON request service. If the GeoRSS feed does not have polygons, this property is not added to the layer JSON.",
    )
    refresh_interval: float | None = Field(
        0,
        alias="refreshInterval",
        description="Refresh interval of the layer in minutes. Non-zero value indicates automatic layer refresh at the specified interval. Value of 0 indicates auto refresh is not enabled.",
    )
    show_legend: bool | None = Field(
        True,
        alias="showLegend",
        description="Boolean value indicating whether to display the layer in the legend. Default value is `true`.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    url: str = Field(..., description="The URL to the layer.")
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )
    visibility_time_extent: list[int | None] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Represents time extent that will control when a layer should be visible based on webmap's current time. Visibility time extent only affects the layer visibility and will not filter the data.",
        title="timeExtent",
    )


class KMLLayer(BaseModel):
    """
    Keyhole Markup Language (KML) is an XML-based format for storing geographic data and associated content and is an official Open Geospatial Consortium (OGC) standard. KML is a common format for sharing geographic data with non-GIS users as it can be easily delivered on the Internet.
    """

    model_config = common_config
    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
        title="Effect",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="Optional string containing the item ID if it's registered on ArcGIS Online or your organization's portal.",
    )
    layer_type: Literal["KML"] = Field(
        "KML",
        alias="layerType",
        description="String indicating the layer type.",
    )
    max_scale: confloat(ge=0.0) | None = Field(
        None,
        alias="maxScale",
        description="A number representing the maximum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    min_scale: confloat(ge=0.0) | None = Field(
        None,
        alias="minScale",
        description="A number representing the minimum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    path: constr(pattern=r"^file:.+\.km[lz]$") = Field(
        None,
        description="For offline data, a path to a KML file. A URI format is used, starting with `file:` followed by a file system path with an extension of `.kml` or `.kmz`.  A relative path must be from the file which defines the layer. For example `file:../commondata/kml/paris.kml`.",
    )
    refresh_interval: float | None = Field(
        0,
        alias="refreshInterval",
        description="Refresh interval of the layer in minutes. Non-zero value indicates automatic layer refresh at the specified interval. Value of 0 indicates auto refresh is not enabled.",
    )
    show_legend: bool | None = Field(
        True,
        alias="showLegend",
        description="Boolean value indicating whether to display the layer in the legend. Default value is `true`.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    url: str = Field(None, description="The URL to the layer.")
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )
    visibility_time_extent: list[int | None] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Represents time extent that will control when a layer should be visible based on webmap's current time. Visibility time extent only affects the layer visibility and will not filter the data.",
        title="timeExtent",
    )
    visible_folders: list[int] | None = Field(
        None,
        alias="visibleFolders",
        description="Array of numeric IDs of folders that will be made visible.",
    )


class MediaLayer(BaseModel):
    """
    Media layer displays one media on the map positioned by 4 control points.
    """

    model_config = common_config
    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
        title="Effect",
    )
    georeference: Georeference | None = None
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="Optional string containing the item ID of the `Media Layer` item registered on ArcGIS Online or your organization's portal.",
    )
    layer_type: Literal["MediaLayer"] = Field(
        "MediaLayer",
        alias="layerType",
        description="String indicating the layer type.",
    )
    list_mode: ListMode | None = Field(
        None,
        alias="listMode",
        description="To show or hide the layer in the layer list.",
    )
    max_scale: confloat(ge=0.0) | None = Field(
        0,
        alias="maxScale",
        description="A number representing the maximum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    media_type: Literal["image", "video"] = Field(
        "image",
        alias="mediaType",
        description="Indicates the type of media that the `url` points to. The currently supported media types are `image` and `video`. Images must be in JPEG or PNG format. For supported video formats, please refer to the [MDN video codec guide](https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Video_codecs). `mediaType` is applicable only when the `url` is defined.",
    )
    min_scale: confloat(ge=0.0) | None = Field(
        0,
        alias="minScale",
        description="A number representing the minimum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    url: str | None = Field(
        None,
        description="URL to the media file. It is not applicable when `itemId` is defined. The url is relative when the media file is stored as an item resource. Relative urls are only applicable for `image` media types. `video` media types only support external video urls at this time. See related `mediaType` property.",
    )
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )
    visibility_time_extent: list[int | None] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Represents time extent that will control when a layer should be visible based on webmap's current time. Visibility time extent only affects the layer visibility and will not filter the data.",
        title="timeExtent",
    )


class VectorTileLayer(BaseModel):
    """
    A vector tile layer references a set of web-accessible vector tiles and the corresponding style for how those tiles should be drawn.
    """

    model_config = common_config
    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    full_extent: Extent | dict | None = Field(
        None,
        alias="fullExtent",
        description="An extent object representing the full extent envelope for the layer.",
    )
    custom_parameters: dict[constr(pattern=r".*"), str] | None = Field(
        None,
        alias="customParameters",
        description="A sequence of custom parameters appended to the URL of all requests related to a layer.",
        title="customParameters",
    )
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
        title="Effect",
    )
    id: str | None = Field(
        None,
        description="A string containing the item ID of the service if it's registered on ArcGIS Online or your organization's portal.",
    )
    is_reference: bool | None = Field(
        None,
        alias="isReference",
        description="This property is applicable if used as a baseMapLayer. A boolean value indicating whether or not the baseMapLayer draws on top (true) of other layers, including operationalLayers, or below (false).",
    )
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="Optional string containing the item ID of the service if it's registered on ArcGIS Online or your organization's portal.",
    )
    layer_type: Literal["VectorTileLayer"] = Field(
        "VectorTileLayer",
        alias="layerType",
        description="String indicating the layer type.",
    )
    list_mode: ListMode | None = Field(
        None,
        alias="listMode",
        description="To show or hide the layer in the layer list.",
    )
    max_scale: float | None = Field(
        None,
        alias="maxScale",
        description="A number representing the maximum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    min_scale: float | None = Field(
        None,
        alias="minScale",
        description="A number representing the minimum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    path: constr(pattern=r"^file:.+\.vtpk$") | None = Field(
        None,
        description="For offline data, a path to a vector tile layer package file. A URI format is used, starting with `file:` followed by a file system path with an extension of `.vtpk`. A relative path must be from the file which defines the layer. For example `file:../p20/northamerica.vtpk`.",
    )
    style_url: str | None = Field(
        None,
        alias="styleUrl",
        description="A url to a JSON file containing the stylesheet information used to render the layer. You may also pass an object containing the stylesheet information identical to the JSON file.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    url: str | None = Field(None, description="The URL to the vector tiled layer.")
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )
    visibility_time_extent: list[int | None] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Represents time extent that will control when a layer should be visible based on webmap's current time. Visibility time extent only affects the layer visibility and will not filter the data.",
        title="timeExtent",
    )


class WebTiledLayer(BaseModel):
    """
    A tile layer is a derived from a set of web-accessible tiles which reside on a server. The tiles are accessed by a direct URL request from the web browser. Because the tiles in a tile layer are not available as a service, they must be in a specific format for a web app such as the ArcGIS.com map viewer to display the layer on a map.
    """

    model_config = common_config
    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    copyright: str | None = Field(
        None,
        description="Attribution to the Web Tiled Layer provider. It is displayed in the attribution on the web map. Input required by the user when the layer is added to the web map.",
    )
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
        title="Effect",
    )
    full_extent: Extent | dict | None = Field(
        None,
        alias="fullExtent",
        description="An extent object representing the full extent envelope for the layer.",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    is_reference: bool | None = Field(
        None,
        alias="isReference",
        description="This is applicable if used as a baseMapLayer. A boolean value indicating whether or not the baseMapLayer draws on top (true) of other layers, including operationalLayers , or below (false).",
    )
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="Unique string value indicating an item registered in ArcGIS Online or your organization's portal.",
    )
    layer_type: Literal["WebTiledLayer"] = Field(
        "WebTiledLayer",
        alias="layerType",
        description="String indicating the layer type.",
    )
    list_mode: ListMode | None = Field(
        ListMode.show,
        alias="listMode",
        description="To show or hide the layer in the layer list",
    )
    max_scale: confloat(ge=0.0) | None = Field(
        None,
        alias="maxScale",
        description="A number representing the maximum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    min_scale: confloat(ge=0.0) | None = Field(
        None,
        alias="minScale",
        description="A number representing the minimum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    refresh_interval: float | None = Field(
        0,
        alias="refreshInterval",
        description="Refresh interval of the layer in minutes. Non-zero value indicates automatic layer refresh at the specified interval. Value of 0 indicates auto refresh is not enabled.",
    )
    sub_domains: list[str] | None = Field(
        None,
        alias="subDomains",
        description="If subdomains are detected, they must be specified. The map viewer detects if the Web Tiled Layer has subdomains by parsing the templateURL value for {subDomain}.",
    )
    template_url: str | None = Field(
        None,
        alias="templateUrl",
        description="URL to the Web Tiled Layer. Input required by the user when the layer is added to the web map. The template URL contains a parameterized URL. The URL can contain the following templated parameters: 'level', 'col', 'row', and 'subDomain'.",
    )
    tile_info: TileInfo | None = Field(
        None,
        alias="tileInfo",
        description="Contains the spatial reference and the tiling scheme of the layer. Typically retrieved from a WMTS OGC Web Service. If missing the layer must be in the WGS 1984 Web Mercator (Auxiliary Sphere) tiling scheme.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )
    visibility_time_extent: list[int | None] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Represents time extent that will control when a layer should be visible based on webmap's current time. Visibility time extent only affects the layer visibility and will not filter the data.",
        title="timeExtent",
    )
    wmts_info: WebMapTileServiceInfo | None = Field(
        None,
        alias="wmtsInfo",
        description="Object containing information about the chosen WMTS service layer and tiling schema.",
    )


class WMSLayer(BaseModel):
    """
    A layer consuming a Web Map Service (WMS). The WMS specification is an international specification for serving and consuming dynamic maps on the web.
    """

    model_config = common_config
    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    copyright: str | None = Field(
        None,
        description="A string containing copyright and access information for a WMS layer. This is copied from the capabilities document exposed by the WMS service.",
    )
    custom_layer_parameters: dict[constr(pattern=r".*"), str] | None = Field(
        None,
        alias="customLayerParameters",
        description="A sequence of custom parameters to WMS layer requests. These parameters are applied to `GetMap` and `GetFeatureInfo` requests. The `customLayerParameters` property takes precedence if `customParameters` is also present.",
    )
    custom_parameters: dict[constr(pattern=r".*"), str] | None = Field(
        None,
        alias="customParameters",
        description="A sequence of custom parameters to all WMS requests. These parameters are applied to `GetCapabilities`, `GetMap`, and `GetFeatureinfo` requests. If used with the `customLayerParameters` property, `customParameters` will not take precedence.",
    )
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
        title="Effect",
    )
    extent: list[list[Any]] | None = Field(
        None,
        description="The minimum bounding rectangle, in decimal degrees, of the area covered by the layer as specified in the capabilities.",
        max_length=2,
        min_length=2,
    )
    feature_info_format: str | None = Field(
        None,
        alias="featureInfoFormat",
        description="Format of the feature, e.g.`text/plain`",
    )
    feature_info_url: str | None = Field(
        None,
        alias="featureInfoUrl",
        description="The URL for the WMS GetFeatureInfo call.",
    )
    format: ImageFormat | None = Field(
        ImageFormat.png,
        validate_default=True,
        description="A string containing the image format to be requested from a WMS service. Default is `png`.",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    is_reference: bool | None = Field(
        None,
        alias="isReference",
        description="This is applicable if used as a baseMapLayer. A boolean value indicating whether or not the baseMapLayer draws on top (true) of other layers, including operationalLayers , or below (false).",
    )
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="Unique string value indicating an item registered in ArcGIS Online or your organization's portal.",
    )
    layers: list[WMSSubLayer] | None = Field(
        None, description="An array of layer objects from the WMS service."
    )
    layer_type: Literal["WMS"] = Field(
        "WMS",
        alias="layerType",
        description="String indicating the layer type.",
    )
    list_mode: ListMode | None = Field(
        ListMode.show,
        alias="listMode",
        description="To show or hide the layer in the layer list",
    )
    map_url: str | None = Field(
        None,
        alias="mapUrl",
        description="A string containing the URL of the WMS map. When using a WMS layer, you should also supply the url property. `mapUrl` is the URL returned by the capabilities to be used for the getMap requests.",
    )
    max_height: float | None = Field(
        None,
        alias="maxHeight",
        description="A number defining the maximum height, in pixels, that should be requested from the service.",
    )
    max_scale: confloat(ge=0.0) | None = Field(
        None,
        alias="maxScale",
        description="A number representing the maximum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    max_width: float | None = Field(
        None,
        alias="maxWidth",
        description="A number defining the maximum width, in pixels, that should be requested from the service.",
    )
    min_scale: confloat(ge=0.0) | None = Field(
        None,
        alias="minScale",
        description="A number representing the minimum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    refresh_interval: float | None = Field(
        0,
        alias="refreshInterval",
        description="Refresh interval of the layer in minutes. Non-zero value indicates automatic layer refresh at the specified interval. Value of 0 indicates auto refresh is not enabled.",
    )
    show_legend: bool | None = Field(
        None,
        alias="showLegend",
        description="Boolean value indicating whether to display the layer in the legend. Default value is `true`.",
    )
    spatial_references: list[int] | None = Field(
        None,
        alias="spatialReferences",
        description="An array of numbers containing well-known IDs for spatial references supported by the service.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    time_animation: bool | None = Field(
        None,
        alias="timeAnimation",
        description="Boolean value indicating whether the layer supports time animation.",
    )
    url: str | None = Field(
        None,
        description="The URL to the WMS service (`getCapabilities` URL).",
    )
    version: str | None = Field(
        None,
        description="A string containing the version number of the service.",
    )
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )
    visibility_time_extent: list[int | None] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Represents time extent that will control when a layer should be visible based on webmap's current time. Visibility time extent only affects the layer visibility and will not filter the data.",
        title="timeExtent",
    )
    visible_layers: list[str] | None = Field(
        None,
        alias="visibleLayers",
        description="An array of layers that should appear visible. The array contains the names of the visible layers.",
    )


class VideoLayer(BaseModel):
    """
    Video Layer provides video content from on-demand and livestream feeds from an ArcGIS Video server.
    """

    model_config = common_config
    autoplay: bool | None = Field(
        False,
        description="Boolean value indicating whether the video should start playing automatically when the layer is added to the map.",
    )
    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    drawing_info: VideoLayerDrawingInfo | None = Field(
        None,
        alias="drawingInfo",
        description="Drawing information for the video layer, including the video symbol and other rendering properties.",
    )
    effect: (
        EffectFunctions
        | EffectFunctions1
        | EffectFunctions2
        | EffectFunctions3
        | EffectFunctions4
    ) | None = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
    )
    frame_effect: (
        EffectFunctions
        | EffectFunctions1
        | EffectFunctions2
        | EffectFunctions3
        | EffectFunctions4
    ) | None = Field(
        None,
        description="Frame effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
    )
    frame_opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1.0,
        description="The opacity of the video layer frame, where 0 is fully transparent and 1 is fully opaque.",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    item_id: str | None = Field(
        None, description="A unique identifying string for the item."
    )
    layer_type: Literal["ArcGISVideoLayer"] = Field(
        "ArcGISVideoLayer",
        alias="layerType",
        description="String indicating the layer type.",
    )
    max_scale: confloat(ge=0.0) | None = Field(
        None,
        alias="maxScale",
        description="A number representing the maximum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    min_scale: confloat(ge=0.0) | None = Field(
        None,
        alias="minScale",
        description="A number representing the minimum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    muted: bool | None = Field(
        True,
        description="Boolean value indicating whether the video layer is muted. Default is `true`.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1.0,
        description="The opacity of the video layer, where 0 is fully transparent and 1 is fully opaque.",
    )
    start: int | None = Field(
        0,
        description="The playback start time in seconds since teh beginning of the video. The default is 0.",
    )
    telemetry_display: TelemetryDisplay | None = Field(
        None,
        alias="telemetryDisplay",
        description="Telemetry display settings for the video layer, including telemetry symbols and rendering properties.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    url: str = Field(
        ...,
        description="The URL to the video content, which can be a direct link to a video file or a streaming endpoint.",
    )
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )
    visibility_time_extent: list[int | None] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Represents time extent that will control when a layer should be visible based on webmap's current time. Visibility time extent only affects the layer visibility and will not filter the data.",
    )


class BingLayer(BaseModel):
    """
    Indicates if working with Microsoft Bing layers. There are three layer types associated with Bing Layers: BingMapsAerial, BingMapsRoad, and BingMapsHybrid.
    """

    model_config = common_config
    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
        title="Effect",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    layer_type: BingLayerType = Field(
        ...,
        alias="layerType",
        description="String indicating the layer type.",
    )
    max_scale: conint(ge=0) | None = Field(
        None,
        alias="maxScale",
        description="Integer property used to determine the maximum scale at which the layer is displayed.",
    )
    min_scale: conint(ge=0) | None = Field(
        None,
        alias="minScale",
        description="Integer property used to determine the minimum scale at which the layer is displayed.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    portal_url: str | None = Field(
        None,
        alias="portalUrl",
        description="A string value representing the URL to the Portal/organization Self resource. Calls should be made to this property to retrieve the Bing key. If the key is not made accessible to the public or if `canShareBingPublic` is false, any web maps using Bing layers will not work.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in  a table of contents.",
    )
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )
    visibility_time_extent: list[int | None] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Represents time extent that will control when a layer should be visible based on webmap's current time. Visibility time extent only affects the layer visibility and will not filter the data.",
        title="timeExtent",
    )


class OpenStreetMapLayer(BaseModel):
    """
    Allows use of OpenStreetMap data for use in basemaps only.
    """

    model_config = common_config
    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
        title="Effect",
    )
    id: str | None = Field(
        "defaultBasemap",
        description="A unique identifying string for the layer.",
    )
    layer_type: Literal["OpenStreetMap"] = Field(
        "OpenStreetMap",
        alias="layerType",
        description="String indicating the layer type.",
    )
    list_mode: ListMode | None = Field(
        ListMode.show,
        alias="listMode",
        description="To show or hide layers in the layer list",
    )
    max_scale: confloat(ge=0.0) | None = Field(
        None,
        alias="maxScale",
        description="A numeric property used to determine the maximum scale at which the layer is displayed.",
    )
    min_scale: confloat(ge=0.0) | None = Field(
        None,
        alias="minScale",
        description="A numeric property used to determine the minimum scale at which the layer is displayed.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    title: str = Field(..., description="String title for the basemap layer.")
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )
    visibility_time_extent: list[int | None] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Represents time extent that will control when a layer should be visible based on webmap's current time. Visibility time extent only affects the layer visibility and will not filter the data.",
        title="timeExtent",
    )


class ConnectedOnlineLayer(BaseModel):
    """
    This object indicates that the referenced layer should be included in an offline map using a connection to the original online service. Note that the resulting Mobile Map Package will require a network connection and may need to handle authentication
    """

    model_config = common_config

    id: str | None = Field(
        None,
        description="Identifies which layer or table in the web map is to remain online by specifying the value of its unique id property.",
    )


class EditableLayers(BaseModel):
    """
    Object detailing the available offline editing options.
    """

    model_config = common_config
    download: Download | None = Field(
        Download.features_and_attachments,
        validate_default=True,
        description="When editing layers, the edits are always sent to the server. This string value indicates which data is retrieved. For example, `none` indicates that only the schema is written since neither the features nor attachments are retrieved. For a full sync without downloading attachments, indicate `features`. Lastly, the default behavior is to have a full sync using `featuresAndAttachments` where both features and attachments are retrieved.",
    )
    sync: Sync | None = Field(
        Sync.sync_features_and_attachments,
        validate_default=True,
        description="This string value indicates how the data is synced.",
    )


class IntegratedMesh3DTilesLayer(BaseModel):
    """
    An integrated mesh layer with OGC 3D Tiles as the data source. This layer type can represent built and natural 3D features, such as building walls, trees, valleys, and cliffs, and can include realistic textures and elevation information.
    """

    model_config = common_config
    custom_parameters: dict[constr(pattern=r".*"), str] | None = Field(
        None,
        alias="customParameters",
        description="A sequence of custom parameters appended to the URL of all requests related to a layer.",
        title="customParameters",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="Optional string containing the item ID of the service if it's registered on ArcGIS Online or your organization's portal.",
    )
    layer_definition: LayerDefinition | None = Field(
        None,
        alias="layerDefinition",
        description="A layerDefinition object defining the attribute schema and drawing information for the layer.",
    )
    layer_type: Literal["IntegratedMesh3DTilesLayer"] = Field(
        "IntegratedMesh3DTilesLayer",
        alias="layerType",
        description="String indicating the layer type.",
    )
    list_mode: ListMode | None = Field(
        ListMode.show,
        alias="listMode",
        description="To show or hide layers in the layer list",
    )
    modifications: str | None = Field(
        None,
        description="URL to modifications json file, typically stored in `ITEM/resources`.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    path: constr(pattern=r"^file:.+\.3tz$") | None = Field(
        None,
        description="For offline data, a path to integrated mesh data in a 3D Tiles Archive Format file. A URI format is used, starting with `file:` followed by a file system path with an extension of `.3tz`. A relative path must be from the file which defines the layer. For example `file:../p20/edinburgh.3tz`.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents. If this is not included, a title is derived from the service.",
    )
    url: str = Field(
        None,
        description="The URL to the tileset JSON file for the OGC 3D Tiles layer.",
    )
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web scene.",
    )
    visibility_time_extent: list[int | None] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Represents time extent that will control when a layer should be visible based on webscene's current time. Visibility time extent only affects the layer visibility and will not filter the data.  ",
    )


class BuildingSceneSublayer(BaseModel):
    """
    The BuildingSceneLayer sublayer is a part of a building scene layer.
    """

    model_config = common_config
    disable_popup: bool | None = Field(
        None,
        alias="disablePopup",
        description="disablePopups allows a client to ignore popups defined by the service item.",
    )
    id: int = Field(
        ...,
        description="Identifies the sublayer inside the building scene layer.",
    )
    layer_definition: LayerDefinition | None = Field(
        None,
        alias="layerDefinition",
        description="Additional properties that can define drawing information and a definition expression for the sublayer.",
    )
    list_mode: ListMode | None = Field(
        ListMode.show,
        alias="listMode",
        description="To show or hide the sublayer in the layer list.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the sublayer on the client side, where 0 is full transparency and 1 is no transparency. This is multiplied with the opacity of the containing layers.",
    )
    popup_info: PopupInfo | None = Field(
        None,
        alias="popupInfo",
        description="A popupInfo object defining the content of pop-up windows when you click or query a feature.",
    )
    title: str | None = Field(
        None,
        description="A user-friendly string title for the sublayer that can be used in a table of contents.",
    )
    visibility: bool | None = Field(
        None,
        description="Boolean property determining whether the sublayer is initially visible in the web scene",
    )


class BuildingSceneLayer(BaseModel):
    """
    The BuildingSceneLayer is a layer type designed for on-demand streaming and displaying building data.
    """

    model_config = common_config
    active_filter_id: str | None = Field(
        None,
        alias="activeFilterId",
        description="specifies the id of the currently active filter",
    )
    custom_parameters: dict[constr(pattern=r".*"), str] | None = Field(
        None,
        alias="customParameters",
        description="A sequence of custom parameters appended to the URL of all requests related to a layer.",
        title="customParameters",
    )
    filters: list[BuildingSceneLayerFilter] | None = Field(
        None,
        description="A list of filters available for this layer. Overrides filters defined on the service.",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="Optional string containing the item ID of the service if it's registered on ArcGIS Online or your organization's portal.",
    )
    layer_definition: LayerDefinition | None = Field(
        None,
        alias="layerDefinition",
        description="Additional properties that can define an elevation offset for the layer.",
    )
    layer_type: Literal["BuildingSceneLayer"] = Field(
        "BuildingSceneLayer",
        alias="layerType",
        description="String indicating the layer type.",
    )
    list_mode: ListMode | None = Field(
        ListMode.show,
        alias="listMode",
        description="To show or hide the sublayer in the layer list. If the layer has sublayers, selecting `hide-children` will hide them in the layer list.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    show_legend: bool | None = Field(
        True,
        alias="showLegend",
        description="Boolean value indicating whether to display the layer in the legend. Default value is `true`.",
    )
    sublayers: list[BuildingSceneSublayer] | None = Field(
        None,
        description="An array of objects specifying overrides for building scene layer sublayers",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    url: str = Field(..., description="The URL to the service.")
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web scene",
    )
    visibility_time_extent: list[int | None] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Represents time extent that will control when a layer should be visible based on webscene's current time. Visibility time extent only affects the layer visibility and will not filter the data.  ",
        title="timeExtent",
    )


class IntegratedMeshLayer(BaseModel):
    """
    An integrated mesh can represent built and natural 3D features, such as building walls, trees, valleys, and cliffs, with realistic textures and includes elevation information.
    """

    model_config = common_config
    custom_parameters: dict[constr(pattern=r".*"), str] | None = Field(
        None,
        alias="customParameters",
        description="A sequence of custom parameters appended to the URL of all requests related to a layer.",
        title="customParameters",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="Optional string containing the item ID of the service if it's registered on ArcGIS Online or your organization's portal.",
    )
    layer_definition: LayerDefinition | None = Field(
        None,
        alias="layerDefinition",
        description="A layerDefinition object defining the attribute schema and drawing information for the layer.",
    )
    layer_type: Literal["IntegratedMeshLayer"] = Field(
        "IntegratedMeshLayer",
        alias="layerType",
        description="String indicating the layer type.",
    )
    list_mode: ListMode | None = Field(
        ListMode.show,
        alias="listMode",
        description="To show or hide layers in the layer list",
    )
    modifications: str | None = Field(
        None,
        description="URL to modifications json file, typically stored in `ITEM/resources`. Content of the file follows the $ref:[Modifications schema](modifications_schema.json).",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    path: constr(pattern=r"^file:.+\.slpk$") | None = Field(
        None,
        description="For offline data, a path to integrated mesh data in a scene layer package file. A URI format is used, starting with `file:` followed by a file system path with an extension of `.slpk`. A relative path must be from the file which defines the layer. For example `file:../p20/edinburgh.slpk`.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents. If this is not included, a title is derived from the service.",
    )
    url: str = Field(
        None,
        description="The URL to the layer. If the layer is not from a web service but rather a feature collection, then the url property is omitted.",
    )
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web scene.",
    )
    visibility_time_extent: list[int | None] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Represents time extent that will control when a layer should be visible based on webscene's current time. Visibility time extent only affects the layer visibility and will not filter the data.  ",
    )


class PointCloudLayer(BaseModel):
    """
    Point cloud data is post-processed spatially organized lidar data that consists of large collections of 3D points. Elevations for the ground, buildings, forest canopy, highway overpasses, and anything else encountered during the lidar survey make up the point cloud data. Point cloud layers allow for fast visualisation of point cloud data in the browser.
    """

    model_config = common_config
    custom_parameters: dict[constr(pattern=r".*"), str] | None = Field(
        None,
        alias="customParameters",
        description="A sequence of custom parameters appended to the URL of all requests related to a layer.",
        title="customParameters",
    )
    disable_popup: bool | None = Field(
        None,
        alias="disablePopup",
        description="disablePopups allows a client to ignore popups defined by the service item.",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the layer"
    )
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="Optional string containing the item ID of the service if it's registered on ArcGIS Online or your organization's portal",
    )
    layer_definition: LayerDefinition | None = Field(
        None,
        alias="layerDefinition",
        description="A layerDefinition object defining the attribute schema and drawing information for the layer.",
    )
    layer_type: Literal["PointCloudLayer"] = Field(
        "PointCloudLayer",
        alias="layerType",
        description="String indicating the layer type",
    )
    list_mode: ListMode | None = Field(
        ListMode.show,
        alias="listMode",
        description="To show or hide the layer in the layer list",
    )
    path: constr(pattern=r"^file:.+\.slpk$") | None = Field(
        None,
        description="For offline data, a path to point cloud layer data in a scene layer package file. A URI format is used, starting with `file:` followed by a file system path with an extension of `.slpk`. A relative path must be from the file which defines the layer. For example `file:../p20/zurich.slpk`.",
    )
    popup_info: PopupInfo | None = Field(
        None,
        alias="popupInfo",
        description="A popupInfo object defining the content of pop-up windows when you click a point.",
    )
    show_legend: bool | None = Field(
        True,
        alias="showLegend",
        description="Boolean value indicating whether to display the layer in the legend. Default value is `true`.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents. If this is not included, a title is derived from the service",
    )
    url: str = Field(None, description="The URL to the layer.")
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible",
    )
    visibility_time_extent: list[int | None] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Represents time extent that will control when a layer should be visible based on webscene's current time. Visibility time extent only affects the layer visibility and will not filter the data.  ",
    )


class SceneLayer(BaseModel):
    """
    The SceneLayer is a layer type designed for on-demand streaming and displaying large amounts of data in a SceneView. SceneLayers support two geometry types: Point and 3D Objects (e.g. buildings). The SceneLayer displays data published to a Scene Service. Scene Services can hold large volumes of features in an open format that is suitable for web streaming. The SceneLayer loads these features progressively, starting from coarse representations and refines them to higher detail as necessary for close-up views.
    """

    model_config = common_config
    attribute_table_info: AttributeTableInfo | None = Field(
        None,
        alias="attributeTableInfo",
        description="Information about how the data will be presented in tabular format.",
    )
    custom_parameters: dict[constr(pattern=r".*"), str] | None = Field(
        None,
        alias="customParameters",
        description="A sequence of custom parameters appended to the URL of all requests related to a layer.",
        title="customParameters",
    )
    disable_popup: bool | None = Field(
        None,
        alias="disablePopup",
        description="disablePopups allows a client to ignore popups defined by the service item.",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="Optional string containing the item ID of the service if it's registered on ArcGIS Online or your organization's portal.",
    )
    layer_definition: LayerDefinition | None = Field(
        None,
        alias="layerDefinition",
        description="A layerDefinition object defining the attribute schema and drawing information for the layer.",
    )
    layer_type: Literal["ArcGISSceneServiceLayer"] = Field(
        "ArcGISSceneServiceLayer",
        alias="layerType",
        description="String indicating the layer type.",
    )
    list_mode: ListMode | None = Field(
        ListMode.show,
        alias="listMode",
        description="To show or hide layers in the layer list",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    path: constr(pattern=r"^file:.+\.slpk$") | None = Field(
        None,
        description="For offline data, a path to a scene layer package file. A URI format is used, starting with `file:` followed by a file system path with an extension of `.slpk`. A relative path must be from the file which defines the layer. For example `file:../p20/northamerica.slpk`.",
    )
    popup_info: PopupInfo | None = Field(
        None,
        alias="popupInfo",
        description="A popupInfo object defining the content of pop-up windows when you click or query a feature.",
    )
    screen_size_perspective: bool | None = Field(
        True,
        alias="screenSizePerspective",
        description="Apply [perspective scaling](https://developers.arcgis.com/javascript/latest/api-reference/esri-layers-FeatureLayer.html#screenSizePerspectiveEnabled) to screen-size symbols.",
    )
    show_labels: bool | None = Field(
        False,
        alias="showLabels",
        description="If the layer has a labelingInfo property then labels show on the scene only if the showLabels property it true.",
    )
    show_legend: bool | None = Field(
        True,
        alias="showLegend",
        description="Boolean value indicating whether to display the layer in the legend. Default value is `true`.",
    )
    time_animation: bool | None = Field(
        True,
        alias="timeAnimation",
        description="Indicates whether to enable time animation for the layer.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    url: str = Field(..., description="The URL to the service.")
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web scene",
    )
    visibility_time_extent: list[int | None] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Represents time extent that will control when a layer should be visible based on webscene's current time. Visibility time extent only affects the layer visibility and will not filter the data.  ",
    )


class VoxelLayer(BaseModel):
    """
    A voxel layer represents multidimensional spatial and temporal information in a 3D volumetric visualization. You can analyze the volume by slicing, creating sections or isosurfaces. For example, you can visualize atmospheric or oceanic data, a geological underground model, or space-time cubes as voxel layers.
    """

    custom_parameters: dict[constr(pattern=r".*"), str] | None = Field(
        None,
        alias="customParameters",
        description="A sequence of custom parameters appended to the URL of all requests related to a layer.",
        title="customParameters",
    )
    disable_popup: bool | None = Field(
        None,
        alias="disablePopup",
        description="disablePopups allows a client to ignore popups defined by the service item.",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="Optional string containing the item ID of the service if it's registered on ArcGIS Online or your organization's portal.",
    )
    layer_definition: VoxelLayerDefinition | None = Field(
        None,
        alias="layerDefinition",
        description="Additional properties that define drawing of a voxel layer.",
    )
    layer_type: Literal["Voxel"] = Field(
        "Voxel",
        alias="layerType",
        description="String indicating the layer type.",
    )
    list_mode: ListMode | None = Field(
        ListMode.show,
        alias="listMode",
        description="To show or hide the layer in the layer list.",
    )
    popup_info: PopupInfo | None = Field(
        None,
        alias="popupInfo",
        description="A popupInfo object defining the content of pop-up windows when you click or query a feature.",
    )
    show_legend: bool | None = Field(
        True,
        alias="showLegend",
        description="Boolean value indicating whether to display the layer in the legend. Default value is `true`.",
    )
    time_animation: bool | None = Field(
        True,
        alias="timeAnimation",
        description="Boolean value indicating whether to enable time animation.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents. If this is not included, a title is derived from the service.",
    )
    url: str = Field(..., description="The URL to the layer.")
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible",
    )
    visibility_time_extent: list[int] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Time extent for the visibility of the layer.",
    )


class RasterDataLayer(BaseModel):
    """
    A layer for displaying raster data. This layer only applies to offline data.
    """

    model_config = common_config
    copyright: str | None = Field(
        None,
        description="Attribution to the Raster Data Layer provider. It is displayed in the attribution on the scene. Input required by the user when the layer is added to the scene.",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    layer_type: Literal["RasterDataLayer"] = Field(
        "RasterDataLayer",
        alias="layerType",
        description="String indicating the layer type.",
    )
    list_mode: ListMode | None = Field(
        ListMode.show,
        alias="listMode",
        description="To show or hide the sublayer in the layer list.",
    )
    max_scale: confloat(ge=0.0) | None = Field(
        None,
        alias="maxScale",
        description="A number representing the maximum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    min_scale: confloat(ge=0.0) | None = Field(
        None,
        alias="minScale",
        description="A number representing the minimum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    path: constr(pattern=r"^file:.+$") = Field(
        ...,
        description="For offline data, a path to a raster data file. A URI format is used, starting with `file:` followed by a file system path. A relative path must be from the file which defines the layer. For example `file:../commondata/raster_data/beijing.tif`.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the scene.",
    )


class LineOfSightLayer(BaseModel):
    """
    The LineOfSight layer is a layer for storing line of sight analyses in web scenes.
    """

    model_config = common_config
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    layer_type: Literal["LineOfSightLayer"] = Field(
        "LineOfSightLayer",
        alias="layerType",
        description="String indicating the layer type.",
    )
    list_mode: ListMode | None = Field(
        None,
        alias="listMode",
        description="To show or hide the layer in the layer list.",
    )
    observer: LineOfSightObserver
    targets: list[LineOfSightTarget] = Field(
        ...,
        description="A Collection of LineOfSight target objects used for visibility analysis from observer position.",
    )
    title: str = Field(
        ...,
        description="A human readable string title for the layer that can be used in a table of contents.",
    )
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is visible in the scene.",
    )
    visibility_time_extent: list[int] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Time extent for the visibility of the layer.",
    )


class GroupLayer(BaseModel):
    """
    GroupLayer provides the ability to organize several sublayers into one common layer. Suppose there are several FeatureLayers that all represent water features in different dimensions. For example, wells (points), streams (lines), and lakes (polygons). The GroupLayer provides the functionality to treat them as one layer called Water Features even though they are stored as separate feature layers.<br><br>With respect to layer blending, sublayers of a group layer are blended together in isolation, separate from layers outside that group layer. When `blendMode` is specified for a group layer, the group's collective content is blended with the layer underneath.<br><br>With respect to scale visibility, sublayers of a group layer will be visible only within the scale range defined for the group layer. A sublayer may further restrict itself to a narrow scale range. In other words, a sublayer will be visible only when the current map scale intersects the scale range of that sublayer as well as the scale range of all its parent group layers.
    """

    model_config = common_config
    blend_mode: BlendMode | None = Field(None, alias="blendMode")
    effect: (
        list[
            EffectFunctions
            | EffectFunctions1
            | EffectFunctions2
            | EffectFunctions3
            | EffectFunctions4
        ]
        | list[ScaleDependentEffect]
        | None
    ) = Field(
        None,
        description="Effect provides various filter functions to achieve different visual effects similar to how image filters (photo apps) work.",
        title="Effect",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="Optional string containing the item ID of the group layer if it's registered on ArcGIS Online or your organization's portal.",
    )
    layers: (
        list[
            AnnotationLayer
            | BuildingSceneLayer
            | CatalogLayer
            | CSVLayer
            | DimensionLayer
            | FeatureLayer
            | GeoJSONLayer
            | GeoRSSLayer
            | GroupLayerRef  # forward reference group layer
            | ImageServiceLayer
            | ImageServiceVectorLayer
            | KMLLayer
            | KnowledgeGraphLayer
            | LineOfSightLayer
            | MapServiceLayer
            | OGCFeatureLayer
            | RasterDataLayer
            | OrientedImageryLayer
            | StreamLayer
            | SubtypeGroupLayer
            | TiledImageServiceLayer
            | TiledMapServiceLayer
            | VectorTileLayer
            | WebTiledLayer
            | WFSLayer
            | WMSLayer
            | MediaLayer
            | VoxelLayer
            | IntegratedMesh3DTilesLayer
            | IntegratedMeshLayer
            | PointCloudLayer
            | SceneLayer
        ]
        | None
    ) = Field(None, description="List of child operationalLayers")
    layer_type: Literal["GroupLayer"] = Field(
        "GroupLayer",
        alias="layerType",
        description="String indicating the layer type.",
    )
    list_mode: ListMode | None = Field(
        ListMode.show,
        alias="listMode",
        description="To show or hide the group layer in the layer list",
    )
    max_scale: confloat(ge=0.0) | None = Field(
        0,
        alias="maxScale",
        description="A number representing the maximum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    min_scale: confloat(ge=0.0) | None = Field(
        0,
        alias="minScale",
        description="A number representing the minimum scale at which the layer will be visible. The number is the scale's denominator.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents.",
    )
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web map.",
    )
    visibility_mode: VisibilityMode | None = Field(
        VisibilityMode.independent,
        validate_default=True,
        alias="visibilityMode",
        description="Defines how visibility of sub layers is affected. If set to 'exclusive', clients should ensure only one sublayer is visible at a time. If set to 'independent', clients should allow visibility to be set independently for each sublayer. 'independent' is default.'",
    )
    visibility_time_extent: list[int | None] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Represents time extent that will control when a layer should be visible based on webmap's current time. Visibility time extent only affects the layer visibility and will not filter the data.",
        title="timeExtent",
    )


class TiledElevationLayer(BaseModel):
    """
    Tiled Elevation Layer is a tile layer used for rendering elevation.
    """

    model_config = common_config

    id: str | None = Field(
        None, description="A unique identifying string for the layer"
    )
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="Optional string containing the item ID of the service if it's registered on ArcGIS Online or your organization's portal.",
    )
    layer_type: Literal["ArcGISTiledElevationServiceLayer"] = Field(
        "ArcGISTiledElevationServiceLayer",
        alias="layerType",
        description="String indicating the layer type",
    )
    list_mode: ListMode | None = Field(
        ListMode.show,
        alias="listMode",
        description="To show or hide the elevation layer in the layer list",
    )
    path: constr(pattern=r"^file:.+\.tpkx?$") | None = Field(
        None,
        description="For offline data, a path to a tile package containing elevation data. A URI format is used, starting with `file:` followed by a file system path with an extension of `.tpk` or `.tpkx`. A relative path must be from the file which defines the layer. For example `file:../p20/portland.tpk`.",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents. If this is not included, a title is derived from the service.",
    )
    url: str = Field(
        ...,
        description="The URL to the layer.If the layer is not from a web service but rather a feature collection, then the url property is omitted",
    )
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web scene.",
    )
    visibility_time_extent: list[str] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Array of two strings representing the start and end time of the visibility of the layer.",
    )


class RasterDataElevationLayer(BaseModel):
    """
    RasterDataElevationLayer is a single-band raster layer used for rendering elevation.
    """

    model_config = common_config
    id: str | None = Field(
        None, description="A unique identifying string for the layer"
    )
    layer_type: Literal["RasterDataElevationLayer"] = Field(
        "RasterDataElevationLayer",
        alias="layerType",
        description="String indicating the layer type",
    )
    list_mode: ListMode | None = Field(
        ListMode.show,
        alias="listMode",
        description="To show or hide the elevation layer in the layer list",
    )
    path: constr(pattern=r"^file:.+$") = Field(
        ...,
        description="For offline data, a path to an ArcGIS Runtime supported raster data file. A URI format is used, starting with file: followed by a file system path. A relative path must be from the file which defines the layer. For example `file:../commondata/raster_data/beijing.tif`",
    )
    title: str = Field(
        ...,
        description="A user-friendly string title for the layer that can be used in a table of contents. If this is not included, a title is derived from the service.",
    )
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is initially visible in the web scene.",
    )


class FacilityLayer(BaseModel):
    """
    Defines the layer and field properties for the Facility layer used for floor filtering.
    """

    model_config = common_config
    facility_id_field: str = Field(
        ...,
        alias="facilityIdField",
        description="The name of the attribute field that contains a facility feature's unique identifier.",
    )
    layer_id: str = Field(
        ...,
        alias="layerId",
        description="A layer ID that refers to an operational layer in the map. The layer provides access to Facility features to use for floor filtering.",
    )
    name_field: str = Field(
        ...,
        alias="nameField",
        description="The name of the attribute field that contains a facility feature's name.",
    )
    site_id_field: str | None = Field(
        None,
        alias="siteIdField",
        description="The name of the attribute field that contains a facility feature's site identifier (a foreign key to the Site layer).",
    )
    sub_layer_id: int | None = Field(
        None,
        alias="subLayerId",
        description="The numeric ID of a map service sublayer. This is required only when the layerId property refers to a map service layer.",
    )


class LevelLayer(BaseModel):
    """
    Defines the layer and field properties for the Level layer used for floor filtering.
    """

    model_config = common_config
    facility_id_field: str = Field(
        ...,
        alias="facilityIdField",
        description="The name of the attribute field that contains a level feature's facility identifier (a foreign key to the Facility layer).",
    )
    layer_id: str = Field(
        ...,
        alias="layerId",
        description="A layer ID that refers to an operational layer in the map. The layer provides access to Level features to use for floor filtering.",
    )
    level_id_field: str = Field(
        ...,
        alias="levelIdField",
        description="The name of the attribute field that contains a level feature's unique identifier.",
    )
    level_number_field: str = Field(
        ...,
        alias="levelNumberField",
        description="The name of the attribute field that contains a level feature's level number specific to its facility.",
    )
    long_name_field: str = Field(
        ...,
        alias="longNameField",
        description="The name of the attribute field that contains a level feature's long name.",
    )
    short_name_field: str = Field(
        ...,
        alias="shortNameField",
        description="The name of the attribute field that contains a level feature's short name.",
    )
    sub_layer_id: int | None = Field(
        None,
        alias="subLayerId",
        description="The numeric ID of a map service sublayer. This is required only when the layerId property refers to a map service layer.",
    )
    vertical_order_field: str = Field(
        ...,
        alias="verticalOrderField",
        description="The name of the attribute field that contains a level feature's vertical order. The vertical order defines the order of display in the floor filter widget, and it also references the floor levels of an Indoor Positioning System.",
    )


class SiteLayer(BaseModel):
    """
    Defines the layer and field properties for the Site layer used for floor filtering.
    """

    model_config = common_config
    layer_id: str = Field(
        ...,
        alias="layerId",
        description="A layer ID that refers to an operational layer in the map. The layer provides access to Site features to use for floor filtering.",
    )
    name_field: str = Field(
        ...,
        alias="nameField",
        description="The name of the attribute field that contains a site feature's name.",
    )
    site_id_field: str = Field(
        ...,
        alias="siteIdField",
        description="The name of the attribute field that contains a site feature's unique identifier.",
    )
    sub_layer_id: int | None = Field(
        None,
        alias="subLayerId",
        description="The numeric ID of a map service sublayer. This is required only when the layerId property refers to a map service layer.",
    )


class ReadonlyLayers(BaseModel):
    """
    Read-only layers as the features are always retrieved from the server.
    """

    model_config = common_config
    download_attachments: bool | None = Field(
        True,
        alias="downloadAttachments",
        description="Indicates whether to include attachments with the read-only data.",
    )


class ViewshedLayer(BaseModel):
    """
    The ViewshedLayer is a layer for storing viewshed analyses in web scenes.
    """

    model_config = common_config
    id: str = Field(..., description="A unique identifying string for the layer.")
    layer_type: Literal["ViewshedLayer"] = Field(
        "ViewshedLayer",
        alias="layerType",
        description="String indicating the layer type.",
    )
    list_mode: ListMode | None = Field(
        None,
        alias="listMode",
        description="To show or hide the layer in the layer list.",
    )
    title: str = Field(
        ...,
        description="A human readable string title for the layer that can be used in a table of contents.",
    )
    viewsheds: list[Viewshed] = Field(
        ...,
        description="A Collection of Viewshed embedded in the layer.",
    )
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is visible in the scene.",
    )
    visibility_time_extent: list[int] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Time extent for the visibility of the layer.",
    )


class Object3DTilesLayer(BaseModel):
    """
    A 3D object layer with OGC 3D Tiles as the data source. This layer type is mainly used for displaying 3D features like buildings and other elements of a city scene.
    """

    model_config = common_config
    custom_parameters: dict[constr(pattern=r".*"), str] | None = Field(
        None,
        alias="customParameters",
        description="A sequence of custom parameters appended to the URL of all requests related to a layer.",
    )
    disable_popup: bool | None = Field(
        None,
        alias="disablePopup",
        description="disablePopups allows a client to ignore popups defined by the service item.",
    )
    id: str | None = Field(
        None, description="A unique identifying string for the layer."
    )
    item_id: str | None = Field(
        None,
        alias="itemId",
        description="Optional string containing the item ID of the service if it's registered on ArcGIS Online or your organization's portal.",
    )
    layer_definition: LayerDefinition | None = Field(
        None,
        alias="layerDefinition",
        description="A layerDefinition object defining the attribute schema and drawing information for the layer.",
    )
    layer_type: Literal["Object3DTilesLayer"] = Field(
        "Object3DTilesLayer",
        alias="layerType",
        description="String indicating the layer type.",
    )
    list_mode: ListMode | None = Field(
        None,
        alias="listMode",
        description="To show or hide the layer in the layer list.",
    )
    opacity: confloat(ge=0.0, le=1.0) | None = Field(
        1,
        description="The degree of transparency applied to the layer on the client side, where 0 is full transparency and 1 is no transparency.",
    )
    path: constr(pattern=r"^file:.+\.3dtiles$") | None = Field(
        None,
        description="For offline data, a path to a 3D Tiles layer data in a scene layer package file. A URI format is used, starting with `file:` followed by a file system path with an extension of `.3dtiles`. A relative path must be from the file which defines the layer. For example `file:../p20/zurich.3dtiles`.",
    )
    title: str = Field(
        ...,
        description="A human readable string title for the layer that can be used in a table of contents.",
    )
    url: str | None = Field(None, description="The URL to the service.")
    visibility: bool | None = Field(
        True,
        description="Boolean property determining whether the layer is visible in the scene.",
    )
    visibility_time_extent: list[int] | None = Field(
        None,
        alias="visibilityTimeExtent",
        description="Time extent for the visibility of the layer.",
    )


AnnotationLayer.model_rebuild()
CatalogLayer.model_rebuild()
CatalogDynamicGroupLayer.model_rebuild()
CSVLayer.model_rebuild()
DimensionLayer.model_rebuild()
FeatureLayer.model_rebuild()
GeoJSONLayer.model_rebuild()
ImageServiceLayer.model_rebuild()
ImageServiceVectorLayer.model_rebuild()
KnowledgeGraphLayer.model_rebuild()
MapServiceLayer.model_rebuild()
OGCFeatureLayer.model_rebuild()
OrientedImageryLayer.model_rebuild()
StreamLayer.model_rebuild()
SubtypeGroupLayer.model_rebuild()
TiledImageServiceLayer.model_rebuild()
TiledMapServiceLayer.model_rebuild()
WFSLayer.model_rebuild()
SubtypeGroupTable.model_rebuild()
Table.model_rebuild()
LayerDefinition.model_rebuild()
FootprintLayer.model_rebuild()
FeatureCollection.model_rebuild()
KnowledgeGraphSubLayer.model_rebuild()
KnowledgeGraphSubTable.model_rebuild()
SubtypeLayer.model_rebuild()
SubtypeTable.model_rebuild()
IntegratedMesh3DTilesLayer.model_rebuild()
IntegratedMeshLayer.model_rebuild()
BuildingSceneLayer.model_rebuild()
BuildingSceneSublayer.model_rebuild()
PointCloudLayer.model_rebuild()
SceneLayer.model_rebuild()
WCSLayer.model_rebuild()
ViewshedLayer.model_rebuild()
