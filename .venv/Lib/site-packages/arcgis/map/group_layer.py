from __future__ import annotations

from typing import Any

import pandas as pd
from arcgis.auth.tools import LazyLoader

arcgismapping = LazyLoader("arcgis.map")
features = LazyLoader("arcgis.features")
arcgis_layers = LazyLoader("arcgis.layers")
popups = LazyLoader("arcgis.map.popups")
renderers = LazyLoader("arcgis.map.renderers")
forms = LazyLoader("arcgis.map.forms")
_models = LazyLoader("arcgis.map.dataclasses.models")


class BaseGroup:
    """
    Base class for GroupLayer, SubtypeGroupTable, and SubtypeGroupLayer.

    This is a facade over the underlying pydantic WebMap/WebScene dataclasses.
    """

    def __init__(
        self,
        group_layer: _models.Layer,
        map: arcgismapping.Map | arcgismapping.Scene,
        parent: BaseGroup | arcgismapping.Map | arcgismapping.Scene,
    ) -> None:
        # The underlying pydantic layer dataclass backing this group.
        self._group_layer: _models.Layer = group_layer

        # Parent: either another BaseGroup (GroupLayer/SubtypeGroupLayer/SubtypeGroupTable)
        # or the Map/Scene instance.
        self._parent: BaseGroup | arcgismapping.Map | arcgismapping.Scene = parent

        # Associated Map/Scene widget.
        self._source: arcgismapping.Map | arcgismapping.Scene = map

        # Webmap/Webscene definition reference.
        if isinstance(map, arcgismapping.Map):
            self._is_map: bool = True
            self._definition: _models.Webmap = map._webmap
        elif isinstance(map, arcgismapping.Scene):
            self._is_map: bool = False
            self._definition: _models.Webscene = map._webscene
        else:
            raise ValueError("Invalid widget type")

        # Cache to keep stable wrapper instances for child layers/tables.
        # Mapping: child_pydantic_layer.id -> python wrapper instance
        self._layer_cache: dict[str, Any] = {}

    def __str__(self) -> str:
        return f"GroupLayer(layer={self._group_layer.title})"

    def __repr__(self) -> str:
        return f"GroupLayer(layer={self._group_layer.title})"

    def _update_source(self) -> None:
        """
        Update the source webmap or webscene.
        """
        self._source._update_source()

    # ------------------------------------------------------------------
    # Internal access to children (pydantic dataclasses)
    # ------------------------------------------------------------------
    @property
    def _children(self) -> list:
        """
        Get the list of children in the group layer/table.

        This returns the *pydantic dataclasses* that live inside the
        WebMap/WebScene definition (layers or tables).
        """
        if isinstance(self._group_layer, _models.SubtypeGroupTable):
            # subtype group table -> tables
            return self._group_layer.tables or []
        else:
            # group layer or subtype group layer -> layers
            return self._group_layer.layers or []

    @property
    def title(self) -> str:
        """
        Get the title of the group layer.
        """
        return self._group_layer.title

    @title.setter
    def title(self, value: str) -> None:
        """
        Set the title of the group layer.
        """
        self._group_layer.title = value
        if getattr(self._source, "legend", None) and self._source.legend.enabled:
            # Update so it reflects in the legend and such widgets
            self._source._update_source()

    # ------------------------------------------------------------------
    # Manager helpers (Popup, Renderer, Layer Visibility, Form)
    # These are PUBLIC on GroupLayer and must keep behavior.
    # They operate on the underlying pydantic child dataclass.
    # ------------------------------------------------------------------
    def popup(self, index: int) -> popups.PopupManager:
        """
        Get an instance of the PopupManager class for the layer specified.
        Specify the layer through it's position in the list of layers.
        The list of layers can be accessed with the `layers` property.

        ==================      =====================================================================
        **Parameter**           **Description**
        ------------------      ---------------------------------------------------------------------
        index                   Required integer specifying the layer who's popup to get. Layer has to be
                                in list of layers of the Group Layer instance. You can get the list of layers
                                by calling the `layers` property on your Group Layer instance.
        ==================      =====================================================================

        :return: PopupManager instance for the layer specified.
        """
        # pydantic child layer/table
        layer = self._children[index]
        # PopupManager is designed to work with the pydantic dataclass directly.
        if hasattr(layer, "popup_info"):
            return popups.PopupManager(layer=layer, source=self)
        else:
            raise ValueError("The layer type does not support popups.")

    def renderer(self, index: int) -> renderers.RendererManager:
        """
        Get an instance of the RendererManager class for the layer specified.
        Specify the layer through it's position in the list of layers.
        The list of layers can be accessed with the `layers` property.

        ==================      =====================================================================
        **Parameter**           **Description**
        ------------------      ---------------------------------------------------------------------
        index                   Required integer specifying the layer who's renderer to get. Layer has to be
                                in list of layers of the Group Layer instance. You can get the list of layers
                                by calling the `layers` property on your Group Layer instance.
        ==================      =====================================================================

        :return: RendererManager instance for the layer specified.
        """
        # RendererManager is also designed around the pydantic dataclass.
        return renderers.RendererManager(layer=self._children[index], source=self)

    def layer_visibility(self, index: int) -> arcgismapping.map_widget.LayerVisibility:
        """
        Get an instance of the LayerVisibility class for the layer specified.
        """
        # LayerVisibility works against the underlying dataclass (as currently implemented).
        return arcgismapping.map_widget.LayerVisibility(self._children[index])

    def form(self, index: int) -> forms.FormInfo | None:
        # For backwards compatibility: return a *copy* of the FormInfo.
        form_info = getattr(self._children[index], "form_info", None)
        if form_info is None:
            return None
        else:
            return forms.FormInfo(**form_info.dict())

    # ------------------------------------------------------------------
    # Reposition and update logic (shared across group-like classes)
    # ------------------------------------------------------------------
    def reposition(self, current_index: int, new_index: int) -> None:
        """
        Reorder a child (layer/table) inside this group.
        """
        children = self._children
        # Just mutate the underlying pydantic list:
        item = children.pop(current_index)
        children.insert(new_index, item)

        # Reflect changes in widget/webmap.
        self._update_source()

    def update_layer(
        self,
        index,
        labeling_info=None,
        renderer=None,
        scale_symbols=None,
        transparency=None,
        options=None,
        form=None,
    ) -> None:
        """
        This method can be used to update certain properties found in a layer within a group layer in your map.

        ==================      =====================================================================
        **Parameter**           **Description**
        ------------------      ---------------------------------------------------------------------
        index                   Required integer specifying the index for the layer you want to update.
                                To see a list of layers use the layers property. This cannot be a group layer.
                                To update a layer in a GroupLayer, use the `update_layer` method in the group layer class.
        ------------------      ---------------------------------------------------------------------
        labeling_info           Optional list of dictionaries. Defines the properties used for labeling the layer.
        ------------------      ---------------------------------------------------------------------
        renderer                Optional renderer instance for the layer.
        ------------------      ---------------------------------------------------------------------
        scale_symbols           Optional bool. Indicates whether symbols should stay the same size in
                                screen units as you zoom in.
        ------------------      ---------------------------------------------------------------------
        transparency            Optional int. 0 (no transparency) to 100 (completely transparent).
        ------------------      ---------------------------------------------------------------------
        options                 Optional dict of additional layer property edits.
        ------------------      ---------------------------------------------------------------------
        form                    Optional FormInfo dataclass.
        ==================      =====================================================================
        """
        # Python wrapper for type-checking and user-facing expectations.
        # (GroupLayer override below ensures this remains public there.)
        from_group = isinstance(self, GroupLayer) or isinstance(self, SubtypeGroupLayer)

        # For groups, we infer python wrapper objects for children via .layers;
        # for SubtypeGroupTable this method is generally not used.
        layer_wrapper = None
        if from_group:
            # Avoid circular imports: layers are resolved via the map helper.
            layer_wrapper = self.layers[index]

        # Underlying pydantic child dataclass.
        # For grouping types this is _group_layer.layers[index].
        # For subtype tables, this would be tables[index] (if ever used).
        pydantic_child = self._children[index]

        # Error check: do not allow editing a GroupLayer child.
        if isinstance(layer_wrapper, GroupLayer):
            raise ValueError(
                "The layer cannot be of type Group Layer. "
                "Use the `update_layer` method found in the Group Layer class."
            )

        # Restrict editable layer types (FeatureCollection, FeatureLayer, GeoJSON, CSV).
        if layer_wrapper is not None and not (
            isinstance(layer_wrapper, features.FeatureCollection)
            or isinstance(layer_wrapper, features.FeatureLayer)
            or isinstance(layer_wrapper, arcgis_layers.GeoJSONLayer)
            or isinstance(layer_wrapper, arcgis_layers.CSVLayer)
        ):
            raise ValueError(
                "Only Feature Collections, Feature Layers, GeoJSON Layers, and CSV Layers can be edited."
            )

        # Only update drawing info if needed
        if (
            renderer
            or scale_symbols is not None
            or labeling_info
            or transparency is not None
        ):
            # Find this group layer in the operational_layers in the WebMap/WebScene.
            group_layer_index = next(
                (
                    i
                    for i, op_layer in enumerate(self._definition.operational_layers)
                    if op_layer.id == self._group_layer.id
                ),
                None,
            )
            if group_layer_index is None:
                raise ValueError(
                    "Could not locate the group layer in the WebMap definition."
                )

            # The pydantic layer inside the group in the WebMap definition.
            wm_child = self._definition.operational_layers[group_layer_index].layers[
                index
            ]

            # Retrieve the existing drawing info from the WebMap-side dataclass.
            existing_drawing_info = (
                wm_child.dict().get("layerDefinition", {}).get("drawingInfo", {})
            )

            # Update the drawing info dict with new properties.
            if renderer is not None:
                existing_drawing_info["renderer"] = renderer
            if scale_symbols in (True, False):
                existing_drawing_info["scale_symbols"] = scale_symbols
            if labeling_info:
                existing_drawing_info["labeling_info"] = labeling_info
            if transparency is not None:
                existing_drawing_info["transparency"] = transparency

            # Assign the updated drawing info back to the WebMap definition.
            if wm_child.layer_definition is None:
                # Create a new LayerDefinition if it doesn't exist.
                wm_child.layer_definition = _models.LayerDefinition(
                    drawing_info=_models.DrawingInfo(**existing_drawing_info)
                )
            else:
                wm_child.layer_definition.drawing_info = _models.DrawingInfo(
                    **existing_drawing_info
                )

        # Only update the layer definition if options are provided
        if options:
            # handle layerDefinition separately (WebMap-spec key)
            if "layerDefinition" in options:
                # get existing layer definition from the WebMap-side child
                group_layer_index = next(
                    (
                        i
                        for i, op_layer in enumerate(
                            self._definition.operational_layers
                        )
                        if op_layer.id == self._group_layer.id
                    ),
                    None,
                )
                if group_layer_index is None:
                    raise ValueError(
                        "Could not locate the group layer in the WebMap definition."
                    )

                wm_child = self._definition.operational_layers[
                    group_layer_index
                ].layers[index]

                existing_layer_definition = (
                    wm_child.layer_definition or _models.LayerDefinition()
                )
                # We assume LayerDefinition is a pydantic model; use its model_copy
                # to apply dict updates.
                ld_dict = existing_layer_definition.dict(by_alias=True)
                ld_dict.update(options["layerDefinition"])
                wm_child.layer_definition = _models.LayerDefinition(**ld_dict)

                # remove from options so we don't set it again
                del options["layerDefinition"]

            # make the edits straight in the WebMap group child definition
            group_layer_index = next(
                (
                    i
                    for i, op_layer in enumerate(self._definition.operational_layers)
                    if op_layer.id == self._group_layer.id
                ),
                None,
            )
            if group_layer_index is None:
                raise ValueError(
                    "Could not locate the group layer in the WebMap definition."
                )

            wm_child = self._definition.operational_layers[group_layer_index].layers[
                index
            ]

            # if an options dictionary was passed in, set the available attributes
            for key, value in options.items():
                # convert camelCase to snake_case for dataclass attributes
                snake_key = "".join(
                    ["_" + c.lower() if c.isupper() else c for c in key]
                ).lstrip("_")
                if hasattr(wm_child, snake_key):
                    setattr(wm_child, snake_key, value)

        # Update the webmap dict on the widget so layer changes are reflected
        self._update_source()

        # Update form info if provided
        if form is not None:
            # Normalize to a FormInfo dataclass instance
            form_info = form
            if hasattr(form, "dict"):
                form_info = forms.FormInfo(**form.dict())
            # Update the form on the underlying pydantic child
            setattr(pydantic_child, "form_info", form_info)


class GroupLayer(BaseGroup):
    """
    The Group Layer class provides the ability to organize several sublayers into one
    common layer. Suppose there are several FeatureLayers that all represent
    water features in different dimensions.

    .. note:: This class should not be created by a user but rather accessed through
        indexing using the `layers` property on a Map instance.
    """

    def __init__(
        self,
        group_layer: _models.Layer,
        map: arcgismapping.Map | arcgismapping.Scene,
        parent: BaseGroup | arcgismapping.Map | arcgismapping.Scene,
    ) -> None:
        super().__init__(group_layer, map, parent)

    def __str__(self) -> str:
        return f"GroupLayer(title={self.title})"

    def __repr__(self) -> str:
        return f"GroupLayer(title={self.title})"

    # ------------------------------------------------------------------
    # PUBLIC API: layers
    # Returns python wrapper layer instances, inferred from pydantic children.
    # ------------------------------------------------------------------
    @property
    def layers(self) -> list:
        """
        Get the list of layers in the group layer.

        Returns the user-facing Python API layer objects:
        - FeatureLayer, MapImageLayer, VectorTileLayer, GroupLayer, etc.
        """
        children_wrappers: list[Any] = []

        for child_dc in self._children:
            layer_id = getattr(child_dc, "id", None)
            wrapper = None

            # Reuse cached wrapper if available (stable identity).
            if layer_id and layer_id in self._layer_cache:
                wrapper = self._layer_cache[layer_id]
                # Ensure the parent is always correct
                wrapper._parent = self  # type: ignore[attr-defined]
            else:
                # If the layer is a group, create a new GroupLayer wrapper.
                if getattr(child_dc, "layer_type", None) in ("group", "GroupLayer"):
                    wrapper = GroupLayer(child_dc, self._source, parent=self)
                else:
                    # Otherwise, infer the appropriate layer wrapper type.
                    wrapper = self._source._helper._infer_layer(child_dc)

                if layer_id:
                    self._layer_cache[layer_id] = wrapper

            children_wrappers.append(wrapper)

        return children_wrappers

    # ------------------------------------------------------------------
    # PUBLIC API: move, ungroup, remove_layer
    # These now operate on the pydantic tree only, so Map/Scene rebuilds
    # wrappers from the updated WebMap/WebScene definition.
    # ------------------------------------------------------------------
    def move(self, index: int, group: GroupLayer | None = None) -> None:
        """
        Move a layer from its group into another GroupLayer or into 'No Group'
        which means it will be moved to the main Map's layers.

        You can use this method on a GroupLayer and the entire GroupLayer will be
        added to another group or be added to the main Map's layers.

        ==================      =====================================================================
        **Parameter**           **Description**
        ------------------      ---------------------------------------------------------------------
        index                   Required int. The index of the layer you want to move. You can see the
                                list of layers by calling the `layers` property on your Group Layer instance.
        ------------------      ---------------------------------------------------------------------
        group                   Optional GroupLayer. The group layer you want to move the layer to.
                                If you want to move the layer to the main Map's layers, pass in None.
        ==================      =====================================================================
        """
        children = self._children
        try:
            child_dc = children.pop(index)
        except IndexError:
            raise IndexError("Layer index out of range.")

        # Move to main Map/Scene operational_layers if group is None.
        if group is None:
            # Top-level operational layers in the WebMap/WebScene.
            self._definition.operational_layers.append(child_dc)
        else:
            # Ensure we are moving within the same Map/Scene.
            if group._source is not self._source:
                raise ValueError(
                    "Cannot move a layer to a group on a different map or scene."
                )
            # Append to the target group's pydantic children.
            target_children = group._children
            target_children.append(child_dc)

        # Reflect changes.
        self._update_source()

    def ungroup(self) -> None:
        """
        Un-group a GroupLayer. This will send the layer's children to the parent's layers.

        - If the parent is Map, then all the layers in the GroupLayer will be sent to the Map's
          operational_layers and the GroupLayer removed.
        - If the parent is another GroupLayer, then all the layers in the GroupLayer will be
          sent to the parent's layers and the GroupLayer removed.
        """
        children = list(self._children)  # snapshot

        if isinstance(self._parent, arcgismapping.Map):
            # Parent is top-level Map: operate on WebMap.operational_layers.
            op_layers = self._definition.operational_layers
            # Find this group layer in operational_layers.
            group_index = next(
                (i for i, lyr in enumerate(op_layers) if lyr is self._group_layer),
                None,
            )
            if group_index is None:
                raise ValueError("GroupLayer not found in WebMap operational_layers.")

            # Remove the group layer and insert its children in its place.
            op_layers.pop(group_index)
            for child in reversed(children):
                op_layers.insert(group_index, child)

        elif isinstance(self._parent, GroupLayer):
            # Parent is another GroupLayer: operate on parent._group_layer.layers.
            parent_dc = self._parent._group_layer
            parent_children = parent_dc.layers or []
            group_index = next(
                (
                    i
                    for i, lyr in enumerate(parent_children)
                    if lyr is self._group_layer
                ),
                None,
            )
            if group_index is None:
                raise ValueError("GroupLayer not found in parent's layers.")

            parent_children.pop(group_index)
            for child in reversed(children):
                parent_children.insert(group_index, child)
        else:
            # Parent is some other BaseGroup or unsupported parent type.
            raise ValueError(
                "Ungroup operation is only supported for Map and GroupLayer parents."
            )

        # Clear this group's own children for consistency (though the dataclass
        # is effectively removed from the hierarchy).
        self._group_layer.layers = []

        # Reflect changes.
        self._update_source()

    def remove_layer(self, index: int) -> None:
        """
        Remove a layer from the GroupLayer's layers by index.

        ==================      =====================================================================
        **Parameter**           **Description**
        ------------------      ---------------------------------------------------------------------
        index                   Required int. The index of the layer you want to remove.
                                You can see the list of layers by calling the `layers` property on your
                                Group Layer instance.
        ==================      =====================================================================
        """
        children = self._children
        try:
            children.pop(index)
        except IndexError:
            raise IndexError("Layer index out of range.")

        self._update_source()


class SubtypeGroupTable(BaseGroup):
    """
    A SubtypeGroupTable represents a **single feature service table that defines
    multiple subtypes**, each of which is exposed in the WebMap as its own logical
    table with a title and editing/display properties.

    Subtype tables (instances of `SubtypeTable`) are **not independent service tables**
    and do **not** have their own URLs. All subtype tables:

        - share the same parent table’s URL
        - belong to the same dataset
        - use the same REST endpoint
        - cannot be queried or accessed separately from the parent table

    For this reason, the Python API does **not** expose a `.tables` property on
    `SubtypeGroupTable`. Instead, you can inspect subtype table information using
    :func:`table_info`, which provides a structured summary suitable for display in
    notebooks.

    While subtype tables cannot be manipulated as full tables, you can still manage
    subtype-specific configuration such as popups and forms using the manager methods
    inherited from the base group classes.

    **Key Concept:**
        A SubtypeGroupTable does *not* contain multiple standalone tables—it contains
        multiple subtype definitions stored under **one** feature service table.

    **Example**
        >>> table = m.content.tables[0]
        >>> table.table_info
          Layer Index Layer Title Layer Type
        0            0        Soil      Table
        1            1   Bedrock      Table

    This class is intended to be accessed through the `tables` collection on a Map or
    GroupLayer. Users should not instantiate it directly.
    """

    def __init__(
        self,
        group_layer: _models.SubtypeGroupTable,
        map: arcgismapping.Map | arcgismapping.Scene,
        parent: BaseGroup | arcgismapping.Map | arcgismapping.Scene,
    ) -> None:
        super().__init__(group_layer, map, parent)

    def __str__(self) -> str:
        return f"SubtypeGroupTable(title={self.title})"

    def __repr__(self) -> str:
        return f"SubtypeGroupTable(title={self.title})"

    @property
    def table_info(self) -> pd.DataFrame:
        """
        Get a DataFrame with information about the tables in this subtype group table.
        """
        table_info: list[list[Any]] = []
        for i, tbl_dc in enumerate(self._children):
            title = getattr(tbl_dc, "title", None)
            table_info.append([i, title, "Table"])
        return pd.DataFrame(
            table_info, columns=["Layer Index", "Layer Title", "Layer Type"]
        )


class SubtypeGroupLayer(BaseGroup):
    """
    A SubtypeGroupLayer represents a **single feature service layer that contains multiple
    subtypes**, each of which can be displayed with its own title, renderer, popup
    configuration, form, and editing properties.

    Unlike a regular `GroupLayer`, the sublayers inside a `SubtypeGroupLayer`
    (instances of `SubtypeLayer`) are **not separate service layers**, do **not** have
    their own REST URLs, and cannot be queried or managed independently on the service.
    All subtype layers share:

    - the same URL (the parent feature layer’s URL)
    - the same dataset
    - the same REST endpoint
    - the same feature service layer ID

    Because subtype layers are not standalone layers, the Python API **does not expose
    them as a `.layers` property**. Instead, you can inspect them using the
    :func:`layer_info` property of `SubtypeGroupLayer` or manage their individual
    display properties using the appropriate manager classes:

        - `popup(index)`
        - `renderer(index)`
        - `form(index)`
        - `layer_visibility(index)`

    These managers allow modifying the popup, renderer, form, and visibility settings
    that are stored in the WebMap specification for each subtype, without implying that
    subtype layers can be manipulated like normal map layers.

    **Key Concept:**
        A SubtypeGroupLayer *looks* like a group of layers in the Map Viewer,
        but technically it represents **one layer with multiple subtype definitions**.

    **Example**
        >>> layer = m.content.layers[0]
        >>> layer.layer_info
          Layer Index Layer Title            Layer Type
        0            0  Washington  ArcGIS Feature Layer
        1            1      Oregon  ArcGIS Feature Layer
        2            2  California  ArcGIS Feature Layer

    This class is intended to be accessed through the `layers` property of a Map or
    the `layers` property of a GroupLayer. Users should not instantiate it directly.
    """

    def __init__(
        self,
        group_layer: _models.SubtypeGroupLayer,
        map: arcgismapping.Map | arcgismapping.Scene,
        parent: BaseGroup | arcgismapping.Map | arcgismapping.Scene,
    ) -> None:
        super().__init__(group_layer, map, parent)

    def __str__(self) -> str:
        return f"SubtypeGroupLayer(title={self.title})"

    def __repr__(self) -> str:
        return f"SubtypeGroupLayer(title={self.title})"

    # ------------------------------------------------------------------
    # PUBLIC API: layers
    # Returns python wrapper layer instances, inferred from pydantic children.
    # ------------------------------------------------------------------
    @property
    def layer_info(self) -> pd.DataFrame:
        """
        Get a DataFrame with information about the layers in this subtype group layer.
        """
        layer_info: list[list[Any]] = []
        for i, lyr_dc in enumerate(self._children):
            title = getattr(lyr_dc, "title", None)
            layer_info.append([i, title, "ArcGIS Feature Layer"])
        return pd.DataFrame(
            layer_info, columns=["Layer Index", "Layer Title", "Layer Type"]
        )
