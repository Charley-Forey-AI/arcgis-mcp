from __future__ import annotations

import uuid
from arcgis.auth.tools import LazyLoader
import re
import os
import copy

arcgis = LazyLoader("arcgis")
content = LazyLoader("arcgis.apps.storymap.story_content")
storymap = LazyLoader("arcgis.apps.storymap.story")
json = LazyLoader("json")
time = LazyLoader("time")
utils = LazyLoader("arcgis.apps.storymap._utils")
pd = LazyLoader("pandas")


###############################################################################################################
class Collection(object):
    """

    ===============     ====================================================================
    **Parameter**        **Description**
    ---------------     --------------------------------------------------------------------
    item                Optional String or Item. The string for an item id or an item of type
                        'StoryMap Collection'. If no item is passed, a new story is created and saved to
                        your active portal.
    ---------------     --------------------------------------------------------------------
    gis                 Optional instance of :class:`~arcgis.gis.GIS` . If none provided the active gis is used.
    ===============     ====================================================================
    """

    _properties = None
    _gis = None
    _itemid = None
    _item = None
    _resources = None

    def __init__(
        self,
        item: arcgis.gis.Item | str | None = None,
        gis: arcgis.gis.GIS | None = None,
    ) -> None:
        # Section: Set up gis
        if gis is None:
            # If no gis, find active env
            gis = arcgis.env.active_gis
            self._gis = gis
        else:
            self._gis = gis
        if gis is None or gis._portal.is_logged_in is False:
            # Check to see if user is authenticated
            raise Exception("Must be logged into a Portal Account")

        # Section: Set up existing story
        if item and isinstance(item, str):
            # Get item using the item id
            item = gis.content.get(item)
            if item is None:
                # Error with storymap in current gis
                raise ValueError(
                    "Cannot find storymap collection associated with this item id in your portal. Please check it is correct."
                )
        if (
            item
            and isinstance(item, arcgis.gis.Item)
            and item.type == "StoryMap"
            and "storymapcollection" in item.typeKeywords
        ):
            # Set item properties from existing item
            self._item = item
            self._itemid = self._item.itemid
            self._resources = self._item.resources.list()
            # Create existing story
            self._create_existing_collection()
        elif (
            item
            and isinstance(item, arcgis.gis.Item)
            and "storymapcollection" not in item.typeKeywords
        ):
            # Throw error if item is not of type Story Map
            raise ValueError("Item is not a StoryMap Collection")
        else:
            # If no item was provided create a new story map
            self._create_new_collection()
        # Get the story url
        self._url = self._get_url()

    # ----------------------------------------------------------------------
    def _create_existing_collection(self) -> None:
        # Get properties from most recent resource file.
        # Can have multiple drafts so need to account for this.
        # Draft file will be of form: draft_{13 digit timestamp}.json or draft.json
        saved_drafts = []
        for resource in self._resources:
            for key, val in resource.items():
                # Find all drafts in the resources and add to a list
                if key == "resource" and (
                    re.match("draft_[0-9]{13}.json", val) or re.match("draft.json", val)
                ):
                    saved_drafts.append(val)
        # Find the correct draft to use
        if len(saved_drafts) == 1:
            # Only one draft saved
            # Open JSON draft file for properties
            data = self._item.resources.get(saved_drafts[0], try_json=True)
            self._properties = data
        elif len(saved_drafts) > 1:
            # Multiple drafts saved
            # Remove draft.json because oldest one
            if "draft.json" in saved_drafts:
                idx = saved_drafts.index("draft.json")
                del saved_drafts[idx]
            # check remaining to find most recent
            start = saved_drafts[0][6:19]  # get only timestamp
            current = saved_drafts[0]
            for draft in saved_drafts:
                compare = draft[6:19]
                if start < compare:
                    start = compare
                    current = draft
            # Open most recent JSON draft file for properties
            data = self._item.resources.get(current, try_json=True)
            self._properties = data
        else:
            # Collection has no draft json so look for published json
            data = self._item.resources.get("published_data.json", try_json=True)
            self._properties = data

    # ----------------------------------------------------------------------
    def _create_new_collection(self) -> None:
        # Get template from _util module
        template = copy.deepcopy(utils._TEMPLATES["collection"])
        # Add correct by-line and locale
        template["nodes"]["n-U3Ou63"]["data"]["byline"] = self._gis._username

        # Create unique collection node id
        collection_node = "n-" + uuid.uuid4().hex[0:6]
        template["root"] = collection_node
        template["nodes"][collection_node] = template["nodes"]["n-vCW523"]
        del template["nodes"]["n-vCW523"]
        # Set properties for the collection
        self._properties = template
        # Create text for resource call
        text = json.dumps(template)
        # Create a temporary title
        title = "Collection via Python %s" % uuid.uuid4().hex[:10]
        # Create draft resource name
        draft = "draft_" + str(int(time.time() * 1000)) + ".json"
        # Will be posted as a draft
        br_version = self._gis._con.get("https://storymaps.arcgis.com/version")[
            "version"
        ]
        keywords = ",".join(
            [
                "arcgis-storymaps",
                "smdraftresourceid:" + draft,
                "smversiondraft:" + br_version,
                "StoryMap",
                "storymapcollection",
                "Web Application",
                "smstatusdraft",
            ]
        )
        # Get default thumbnail for a new item
        thumbnail = self._get_thumbnail()
        # Set the item properties dict to add new item to active gis
        item_properties = {
            "title": title,
            "typeKeywords": keywords,
            "type": "StoryMap",
        }
        # Add item to active gis and set properties
        folder = self._gis.content.folders.get()
        if thumbnail:
            item_properties["thumbnail"] = thumbnail
        item = folder.add(item_properties=item_properties).result()
        # Assign to story properties
        self._item = item
        self._itemid = item.itemid
        # Make a resource call with the template to create json draft needed
        utils._add_resource(self, resource_name=draft, text=text, access="private")
        # Assign resources to item
        self._resources = self._item.resources.list()

    # ----------------------------------------------------------------------
    def _repr_html_(self) -> str:
        """
        HTML Representation for IPython Notebook
        """
        return self._item._repr_html_()

    # ----------------------------------------------------------------------
    def __str__(self) -> str:
        """Return the url of the storymap"""
        return self._url

    # ----------------------------------------------------------------------
    def __repr__(self) -> str:
        return self.__str__()

    # ----------------------------------------------------------------------
    def _refresh(self) -> None:
        """Load the latest data from the item"""
        if self._item:
            self._properties = json.loads(self._item.get_data())

    # ----------------------------------------------------------------------
    def _get_url(self) -> str:
        """
        Private method to determine what the story url is. This is used to publish
        and have the correct path set.
        """
        if self._gis._is_agol:
            # Online
            self._url = (
                "https://storymaps.arcgis.com/collections/{collectionid}".format(
                    collectionid=self._itemid
                )
            )
        else:
            # Enterprise
            self._url = (
                "https://{portal}/apps/storymaps/collections/{collectionid}".format(
                    portal=self._gis.url, collectionid=self._itemid
                )
            )
        return self._url

    # ----------------------------------------------------------------------
    def _get_thumbnail(self) -> str:
        """
        Private method to get the default thumbnail path dependent on whether the
        user is Online or on Enterprise.
        """
        return utils._get_thumbnail(self._gis)

    # ----------------------------------------------------------------------
    def show(self, width: int | None = None, height: int | None = None) -> object:
        """
        Show a preview of the collection. The default is a width of 700 and height of 300.

        ===============     ====================================================================
        **Parameter**       **Description**
        ---------------     --------------------------------------------------------------------
        width               Optional integer. The desired width to show the preview.
        ---------------     --------------------------------------------------------------------
        height              Optional integer. The desired height to show the preview.
        ===============     ====================================================================

        :return:
            An Iframe display of the collection if possible, else the item url is returned to be
            clicked on.
        """
        return utils.show(self._item, width, height)

    # ----------------------------------------------------------------------
    def get_theme(self) -> str:
        """
        Get the theme name or the theme item that is used in the collection.

        return: The theme name or the theme item item_id.
        """
        return utils.get_theme(self)

    # ----------------------------------------------------------------------
    def theme(self, theme: storymap.Themes | str | None = None) -> bool:
        """
        Each collection has a theme node in its resources. This method can be used to change the theme.
        To add a custom theme to your story, pass in the item_id for the item of type Story Map Theme.

        ===============     ====================================================================
        **Parameter**        **Description**
        ---------------     --------------------------------------------------------------------
        theme               Required Themes Style or custom theme item id.
                            The theme to set on the collection.

                            Values: `SUMMIT` | `TIDAL` | `MESA` | `RIDGELINE` | `SLATE` | `OBSIDIAN` | `<item_id>`
        ===============     ====================================================================

        .. code-block:: python

            >>> from arcgis.apps.storymap import Themes, Collection

            >>> collection = Collection()
            >>> collection.theme(Themes.TIDAL)
        """
        theme = theme or storymap.Themes.SUMMIT
        # call method to update theme
        utils.theme(self, theme)
        return True

    # ----------------------------------------------------------------------
    def save(
        self,
        title: str | None = None,
        tags: list | None = None,
        access: str | None = None,
        publish: bool = False,
        make_copyable: bool | None = None,
        no_seo: bool | None = None,
    ) -> object:
        """
        This method will save your Story Map to your active GIS. The story will be saved
        with unpublished changes unless `publish` parameter is specified to True.

        The title only needs to be specified if a change is wanted, otherwise existing title
        is used.

        .. warning::
            Publishing your story through the Python API means it will not go through the Story Map
            issue checker. It is recommended to publish through the Story Maps builder if you
            want your story to go through the issue checker.

        .. warning::
            Changes to the published story may not be visible for up to one hour. You can open
            the story in the story builder to force changes to appear immediately and perform
            other optimizations, such as updating the story's social/SEO metadata.

        ===============     ====================================================================
        **Parameter**        **Description**
        ---------------     --------------------------------------------------------------------
        title               Optional string. The title of the StoryMap.
        ---------------     --------------------------------------------------------------------
        tags                Optional string. The tags of the StoryMap.
        ---------------     --------------------------------------------------------------------
        access              Optional string. The access of the StoryMap. If none is specified, the
                            current access is kept. This is used when `publish` parameter is set
                            to True.

                            Values: `private` | `org` | `public`
        ---------------     --------------------------------------------------------------------
        publish             Optional boolean. If True, the story is saved and also published.
                            Default is false so story is saved with unpublished changes.
        ---------------     --------------------------------------------------------------------
        make_copyable       Optional boolean. If True, the story is saved as copyable for users.
        ---------------     --------------------------------------------------------------------
        no_seo              Optional boolean. If True, the story is saved without SEO metadata.
        ===============     ====================================================================


        :return: The Item that was saved to your active GIS.

        """
        # call the save method in common utils module
        return utils.save(self, title, tags, access, publish, make_copyable, no_seo)

    # ----------------------------------------------------------------------
    def delete_collection(self) -> bool:
        """
        Deletes the collection item.
        """
        # deletes the item
        return utils.delete_item(self)

    # ----------------------------------------------------------------------
    def _get_content_type(self, obj) -> str:
        """
        Determines the content type from a given object.
        - If it's a known class instance, returns its type name.
        - If it's a Portal Item, returns item.type.
        - If it's a file-item dict, returns 'file-item'.
        """
        if isinstance(obj, content.Cover):
            return "Cover"
        elif isinstance(obj, content.CollectionNavigation):
            return "Collection Navigation"
        elif isinstance(obj, content.Image):
            return "Image"
        elif hasattr(obj, "type"):
            # Assuming this is an Item instance
            return obj.type
        elif isinstance(obj, dict) and obj.get("type") == "file-item":
            return "file-item"
        else:
            return "Unknown"

    def _iterate_content(self) -> object:
        """
        Internal generator method to iterate through the content of the collection.
        """
        root_node = self._properties["root"]
        ui_node = self._properties["nodes"][root_node]["children"][0]
        ui = self._properties["nodes"][ui_node]

        # children: no visibility info, assume True
        for child in ui["children"]:
            node = utils._assign_node_class(self, child)
            yield {
                "type": self._get_content_type(node),
                "instance": node,
                "visibility": True,
            }

        # items: visibility info might be present in 'isHidden'
        for item in ui["data"]["items"]:
            visible = not item.get("isHidden", False)  # defaults to True if key missing

            if "nodeId" in item:
                node = utils._assign_node_class(self, item["nodeId"])
                yield {
                    "type": self._get_content_type(node),
                    "instance": node,
                    "visibility": visible,
                }
            elif "resourceId" in item:
                resource = self._properties["resources"][item["resourceId"]]
                if resource["type"] == "file-item":
                    yield {
                        "type": "file-item",
                        "instance": resource["data"]["name"],
                        "visibility": visible,
                    }
                elif resource["type"] == "portal-item":
                    portal_item = self._gis.content.get(resource["data"]["itemId"])
                    yield {
                        "type": self._get_content_type(portal_item),
                        "instance": portal_item,
                        "visibility": visible,
                    }

    # ----------------------------------------------------------------------
    @property
    def content(self) -> list:
        """
        Returns the content of the collection. This includes the cover and navigation.
        """
        return [item["instance"] for item in self._iterate_content()]

    # ----------------------------------------------------------------------
    @property
    def content_info(self) -> object:
        """
        Returns the content as a table with the following columns:
        - Index
        - Type
        - Content class instance
        - Visibility

        : return: A dataframe with the content information.
        """
        data = []
        for entry in self._iterate_content():
            # Skip the first two entries (cover and navigation) since we do not want them in the table
            if entry["type"] in ["Cover", "Collection Navigation"]:
                continue
            data.append(
                {
                    "Type": entry["type"],
                    "Instance": entry["instance"],
                    "Visibility": entry["visibility"],
                }
            )

        return pd.DataFrame(data)

    # ----------------------------------------------------------------------
    def update_content_info(
        self,
        index: int | list[int],
        custom_title: str | list[str] | None = None,
        visible: bool | None = None,
    ) -> object:
        """
        Update the content item in the collection.

        .. note::
            Not all content types support visibility updates. For example, the cover and navigation
            items do not have visibility properties. This method is primarily for items within the collection.

        ===============     ====================================================================
        **Parameter**        **Description**
        ---------------     --------------------------------------------------------------------
        index               Required integer or list of integers. The index position(s) of the item to update.
        ---------------     --------------------------------------------------------------------
        custom_title        Optional string or list of strings. The custom title to set for the item.
        ---------------     --------------------------------------------------------------------
        visible             Required boolean. If True, the item is visible. If False, the item is hidden.
                            If a list of indices is passed, all items will be set to the same specified visibility.
        ===============     ====================================================================

        :return: DataFrame of content information with the updated changes.
        """
        if not isinstance(index, list):
            index = [index]
        root_node = self._properties["root"]
        ui_node = self._properties["nodes"][root_node]["children"][0]
        ui = self._properties["nodes"][ui_node]
        items = ui["data"]["items"]

        # Iterate through the items in the collection-ui node
        for i, _ in enumerate(items):
            if i in index:
                if visible is not None:
                    # Update the visibility of the item
                    self._properties["nodes"][ui_node]["data"]["items"][i][
                        "isHidden"
                    ] = not visible
                if custom_title is not None:
                    # Update the custom title of the item
                    self._properties["nodes"][ui_node]["data"]["items"][i][
                        "customTitle"
                    ] = custom_title
        return self.content_info

    # ----------------------------------------------------------------------
    def remove(self, index: int) -> bool:
        """
        Remove an item from the collection. Specify this item with the index position
        of the item in the collection. The list of items in the collection can be found
        by using the `content` property. The index position is the position of the item
        in the list.

        ===============     ====================================================================
        **Parameter**        **Description**
        ---------------     --------------------------------------------------------------------
        index               Required integer. The index position of the item to remove.
        ===============     ====================================================================

        :return: True if the item was removed successfully.
        """
        # Get the list of content and find what the item is
        item = self.content[index]

        # If the item is a node, remove the node from the collection
        if isinstance(item, (content.Video, content.Image, content.Embed)):
            # Remove the node from the collection by using the class method `delete`
            item.delete()
            return True
        else:
            # The item is a portal item or file resource
            # Get the collection-ui node
            root_node = self._properties["root"]
            ui_node = self._properties["nodes"][root_node]["children"][0]
            ui = self._properties["nodes"][ui_node]

            # Find the item in the collection-ui node, it will be a resource item
            for i, item in enumerate(ui["data"]["items"]):
                if "resourceId" in item:
                    # will be a resource item
                    if i == index:
                        # will be the same index since the list of content and the list of items
                        # in the collection-ui node are the same
                        # Remove the resource from the collection
                        del self._properties["resources"][item["resourceId"]]
                        # Remove the item from the collection-ui node
                        del self._properties["nodes"][ui_node]["data"]["items"][i]
                        return True
        return False

    # ----------------------------------------------------------------------
    def add(
        self,
        item: content.Image | content.Video | content.Embed | arcgis.gis.Item | str,
        title: str | None = None,
        thumbnail: str | None = None,
        position: int | None = None,
    ) -> object:
        """
        Add an item to the collection. Specify this item with the item object.
        The item can be a portal item, file resource, or a story content of type
        Image, Video, or Embed.

        ===============     ====================================================================
        **Parameter**        **Description**
        ---------------     --------------------------------------------------------------------
        item                Required object. Either an Image, Video, or Embed content type object.
        ---------------     --------------------------------------------------------------------
        title               Optional string. The title of the item to add under the thumbnail in the collection.
        ---------------     --------------------------------------------------------------------
        thumbnail           Optional string. The image file path to use as the thumbnail for the item.
        ---------------     --------------------------------------------------------------------
        position            Optional integer. The position in the collection to add the item.
                            If none is specified, the item is added to the end of the collection.
        ===============     ====================================================================
        """
        root_node = self._properties["root"]
        ui_node = self._properties["nodes"][root_node]["children"][0]
        if position is None:
            # If no position is specified, add the item to the end of the collection
            position = len(self._properties["nodes"][ui_node]["data"]["items"])
        # If the item is an Image, Video or Embed, add the node to the collection
        if isinstance(item, (content.Image, content.Video, content.Embed)):
            item._add_to_story(story=self)
            item_dict = {"nodeId": item._node}
            extra = self._add_custom_properties(title, thumbnail)
            item_dict.update(extra)

            # add the node to the collection-ui node
            self._properties["nodes"][ui_node]["data"]["items"].insert(
                position, item_dict
            )
        else:
            resource_node = "r-" + uuid.uuid4().hex[0:6]
            # The item is a portal item or file resource
            # If item, add the item id to the resources
            if isinstance(item, arcgis.gis.Item):
                self._properties["resources"][resource_node] = {
                    "type": "portal-item",
                    "data": {"itemId": item.itemid},
                }
            else:
                # The item is a file resource
                name = os.path.basename(item).replace(".pdf", "")
                utils._add_resource(self, file=item, resource_name=name)
                resources = self._item.resources.list()
                for resource in resources:
                    if resource["resource"] == name:
                        self._properties["resources"][resource_node] = {
                            "type": "file-item",
                            "data": {
                                "resourceId": resource["resource"],
                                "provider": "item-resource",
                            },
                        }
            item_dict = {"resourceId": resource_node}
            extra = self._add_custom_properties(title, thumbnail)
            item_dict.update(extra)
            # add the resource to the collection-ui node
            self._properties["nodes"][ui_node]["data"]["items"].insert(
                position, item_dict
            )

    def _add_custom_properties(self, title, thumbnail):
        """
        Add extra properties to the items in a collection. As of now only title and thumbnail
        """
        new_item = {}
        if title:
            new_item["customTitle"] = title
        if thumbnail:
            resource_node = "r-" + uuid.uuid4().hex[0:6]
            # The item is a file resource
            name = os.path.basename(thumbnail).replace(".pdf", "")
            utils._add_resource(self, file=thumbnail, resource_name=name)
            resources = self._item.resources.list()
            for resource in resources:
                if resource["resource"] == name:
                    self._properties["resources"][resource_node] = {
                        "type": "image",
                        "data": {
                            "resourceId": resource["resource"],
                            "provider": "item-resource",
                        },
                    }

            new_item["customThumbnail"] = resource_node
        return new_item
