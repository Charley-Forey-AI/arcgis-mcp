from __future__ import annotations
from arcgis.map.dataclasses.models.popups import (
    Format,
    FieldInfo,
    LayerOptions,
    Value,
    MediaInfo,
    PopupElementAttachments,
    PopupExpressionInfo,
    PopupElementFields,
    PopupElementMedia,
    OrderByField,
    PopupElementRelationship,
    PopupElementText,
    RelatedRecordsInfo,
    PopupElementExpression,
    PopupElementUtilityNetworkAssociations,
    PopupInfo,
    AssociationType,
)
from arcgis.map.dataclasses.enums.popups import (
    ArcadeReturnType,
    StringFieldOption,
    MediaType,
    AttachmentDisplayType,
    Order,
    AssociationTypes,
)
from arcgis.map.dataclasses.models.layers import Table
from typing import Any
from arcgis.auth.tools import LazyLoader

_map = LazyLoader("arcgis.map.map_widget")
_scene = LazyLoader("arcgis.map.scene_widget")
_group = LazyLoader("arcgis.map.group_layer")

__all__ = [
    "Format",
    "FieldInfo",
    "LayerOptions",
    "Value",
    "MediaInfo",
    "PopupElementAttachments",
    "PopupExpressionInfo",
    "PopupElementFields",
    "PopupElementMedia",
    "OrderByField",
    "PopupElementRelationship",
    "PopupElementText",
    "RelatedRecordsInfo",
    "PopupElementExpression",
    "PopupElementUtilityNetworkAssociations",
    "PopupInfo",
    "ArcadeReturnType",
    "StringFieldOption",
    "MediaType",
    "AttachmentDisplayType",
    "Order",
    "AssociationType",
    "AssociationTypes",
]


class PopupManager:
    """
    A class that defines the popup found on a layer.
    Through this class you can edit the popup and get information on it.


    .. note::
        This class should not be created by a user but rather called through the `popup` method on
        a MapContent or GroupLayer instance.
    """

    def __init__(
        self,
        **kwargs: dict[str, Any],
    ) -> None:
        # The pydantic layer or table, this hooks it to the main webmap and tracks changes made
        self._layer = kwargs.pop("layer")
        self._parent = kwargs.pop("source")  # need to know where to find the layer
        self._source = (
            self._parent._source
        )  # need to know to update the correct dataclass
        self._is_table = isinstance(self._layer, Table)

    def __str__(self) -> str:
        return "PopupManager for: " + self._layer.title

    def __repr__(self) -> str:
        return f"PopupManager(layer={self._layer.title})"

    @property
    def info(self) -> PopupInfo | None:
        """
        Return the popup info for your layer. If no popup info
        is present then the value is None.

        Set the popup info for your layer.

        =====================       ===================================================================
        **Parameter**                **Definition**
        ---------------------       -------------------------------------------------------------------
        value                       Required PopupInfo object. The new popup info for the layer.
        =====================       ===================================================================
        """
        if self._layer.popup_info:
            # Pass into class here so if users want to edit it will show correct version
            return PopupInfo(**self._layer.popup_info.dict())
        else:
            return None

    @info.setter
    def info(self, info):
        if isinstance(self._parent, _map.MapContent):

            if self._is_table:
                # Update the main webmap dataclass in the MapContent
                for i, lyr in enumerate(self._source._webmap.tables):
                    if lyr == self._layer:
                        self._source._webmap.tables[i].popup_info = info
            else:
                # Update the main webmap dataclass in the MapContent
                for i, lyr in enumerate(self._source._webmap.operational_layers):
                    if lyr == self._layer:
                        self._source._webmap.operational_layers[i].popup_info = info
        elif isinstance(self._parent, _scene.SceneContent):
            if self._is_table:
                # Update the main webmap dataclass in the MapContent
                for i, lyr in enumerate(self._source._webscene.tables):
                    if lyr == self._layer:
                        self._source._webscene.tables[i].popup_info = info
            else:
                # Update the main webmap dataclass in the MapContent
                for i, lyr in enumerate(self._source._webscene.operational_layers):
                    if lyr == self._layer:
                        self._source._webscene.operational_layers[i].popup_info = info
        elif isinstance(self._parent, _group.BaseGroup):
            # Update the layer in the group layer
            for i, lyr in enumerate(self._parent._children):
                if lyr == self._children:
                    self._parent._children[i].popup_info = info
        self._layer.popup_info = info
        self._source._update_source()

    @property
    def title(self) -> str | None:
        """
        The title of the popup. If no title is present then the value is None.

        Set the title of the popup in the edit method.
        """
        if self._layer.popup_info:
            return self._layer.popup_info.title
        else:
            return None

    @property
    def disable_popup(self) -> bool:
        """
        Determine whether the popup is enabled for the layer, meaning it is visible when the map is rendered.

        Set whether the popup is enabled for the layer.

        =====================       ===================================================================
        **Parameter**                **Definition**
        ---------------------       -------------------------------------------------------------------
        value                       Required bool. Whether the popup is enabled for the layer. If True, the
                                    popup is not visible when the map is rendered.
        =====================       ===================================================================
        """
        return self._layer.disable_popup

    @disable_popup.setter
    def disable_popup(self, value: bool):
        self._layer.disable_popup = value
        self._source._update_source()

    def edit(
        self,
        title: str | None = None,
        description: str | None = None,
        expression_infos: list[PopupExpressionInfo] | None = None,
        field_infos: list[FieldInfo] | None = None,
        layer_options: LayerOptions | None = None,
        media_infos: list[MediaInfo] | None = None,
        popup_elements: (
            list[
                PopupElementAttachments
                | PopupElementExpression
                | PopupElementFields
                | PopupElementMedia
                | PopupElementRelationship
                | PopupElementText
            ]
            | None
        ) = None,
        show_attachments: bool | None = None,
    ) -> bool:
        """
        Edit the properties of the popup. If no popup info exists then it will create a popup for the layer.
        To remove any existing items from the popup, pass in an empty instance of the parameter. For example to
        remove the title, pass an empty string or to remove the field_infos pass an empty list. If the parameter
        is set to None then nothing will change for that parameter.

        .. note::
            Passing any of the parameters will update completely the information for that parameter.
            For example, if you pass a list of FieldInfo objects, it will replace the existing
            field_infos with the new list. If you want to keep existing field infos and add new ones,
            you will need to retrieve the existing field infos first, modify them, and then pass the modified list.

        =====================       ===================================================================
        **Parameter**                **Definition**
        ---------------------       -------------------------------------------------------------------
        title                       Optional string. Appears at the top of the popup window as a title.
        ---------------------       -------------------------------------------------------------------
        description                 Optional string. Appears in the body of the popup window as a description.
        ---------------------       -------------------------------------------------------------------
        expression_infos            Optional list of PopupExpressionInfo objects. List of Arcade expressions added to the pop-up.

                                    .. note::
                                        When providing expression infos, associated field infos must also be provided.
                                        If you do not provide field infos, the service will
                                        automatically generate field infos for the expressions with the same
                                        name, visibility set to True, and editable set to True.
                                        This will add to field infos and not replace them.
        ---------------------       -------------------------------------------------------------------
        field_infos                 Optional list of FieldInfo objects. Array of fieldInfo information properties.
                                    This information is provided by the service layer definition.
                                    When the description uses name/value pairs, the order of the array
                                    is how the fields display in the editable Map Viewer popup and the
                                    resulting popup. It is also possible to specify HTML-formatted content.
        ---------------------       -------------------------------------------------------------------
        layer_options               Optional LayerOptions class.
        ---------------------       -------------------------------------------------------------------
        media_infos                 Optional list of MediaInfo objects. Array of various mediaInfo to display.
        ---------------------       -------------------------------------------------------------------
        popup_elements              Optional list of PopupElement objects. An array of popupElement objects
                                    that represent an ordered list of popup elements.
        ---------------------       -------------------------------------------------------------------
        show_attachments            Optional bool. Indicates whether attachments will be loaded for
                                    feature layers that have attachments.
        =====================       ===================================================================
        """
        # Check if popup exists
        if self._layer.popup_info is None:
            # create empty popup info
            self._layer.popup_info = PopupInfo()

        # Add any edits made to the popup
        if title is not None:
            self._layer.popup_info.title = title
        if description is not None:
            self._layer.popup_info.description = description

        # Handle field infos first!
        if field_infos is not None:
            self._layer.popup_info.field_infos = field_infos

        if expression_infos is not None:
            # update the expression infos
            self._layer.popup_info.expression_infos = expression_infos

            # We need to see if the user has provided field infos.
            # If not, we will generate them based on the expressions provided.
            # creating a new list since the user provided one was already added
            new_field_infos = []
            if field_infos is None:
                # Generate field infos based on the expressions
                new_field_infos = [
                    FieldInfo(
                        field_name=f"expression/{expr.name}",
                        visible=True,
                        is_editable=True,
                    )
                    for expr in expression_infos
                ]
            else:
                # If field_infos are provided, we need to ensure the expression name match the field name, if not, add them.
                for expr in expression_infos:
                    field_name = f"expression/{expr.name}"
                    if not any(fi.field_name == field_name for fi in field_infos):
                        new_field_infos.append(
                            FieldInfo(
                                field_name=field_name,
                                visible=True,
                                is_editable=True,
                            )
                        )
            if self._layer.popup_info.field_infos is None:
                self._layer.popup_info.field_infos = new_field_infos
            else:
                # If field_infos already exist, we will append the new ones
                self._layer.popup_info.field_infos.extend(new_field_infos)

        if layer_options is not None:
            self._layer.popup_info.layer_options = layer_options

        if media_infos is not None:
            self._layer.popup_info.media_infos = media_infos

        if popup_elements is not None:
            # there are various popup elements that can be added
            self._layer.popup_info.popup_elements = popup_elements

        if show_attachments is not None:
            self._layer.popup_info.show_attachments = show_attachments

        self._source._update_source()

        return True
