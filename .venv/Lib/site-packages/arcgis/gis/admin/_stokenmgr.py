from __future__ import annotations
from arcgis.gis import GIS, Item, User
from arcgis.auth import EsriSession
from typing import Iterable, Union
from arcgis.gis._impl._dataclasses._contentds import ItemTypeEnum, ItemProperties
from arcgis.gis._impl._content_manager.folder import Folder
import datetime as _dt
from enum import Enum
import json
import warnings

__all__ = ["TokenPrivilege", "DeveloperCredentialManager", "DeveloperCredential"]


###########################################################################
class TokenPrivilege(Enum):
    PORTAL_USER_VIEWORGUSERS = "portal:user:viewOrgUsers"  # Grants the ability to view members of the organization.
    PORTAL_USER_CREATEGROUP = "portal:user:createGroup"  # Grants the ability for a member to create, edit, and delete their own groups.
    PORTAL_USER_JOINGROUP = "portal:user:joinGroup"  # Grants the ability to join groups in the organization.
    PORTAL_USER_JOINNONORGGROUP = "portal:user:joinNonOrgGroup"  # Grants the ability to join groups external to the organization.
    PORTAL_USER_VIEWORGGROUPS = "portal:user:viewOrgGroups"  # Grants the ability to view groups shared with the organization.
    PORTAL_USER_INVITEPARTNEREDCOLLABORATIONMEMBERS = "portal:user:invitePartneredCollaborationMembers"  # Grants the ability to invite members from partnered collaboration organizations to groups.
    PORTAL_USER_ADDEXTERNALMEMBERSTOGROUP = "portal:user:addExternalMembersToGroup"  # Grants the ability to create groups that allow members from other organizations, as well as invite external members to groups.
    PORTAL_USER_CREATEITEM = "portal:user:createItem"  # Grants the ability for a member to create, edit, and delete their own content.
    PORTAL_PUBLISHER_PUBLISHFEATURES = "portal:publisher:publishFeatures"  # Grants the ability to publish hosted feature layers from shapefiles, CSV files, and so on.
    PORTAL_PUBLISHER_PUBLISHTILES = "portal:publisher:publishTiles"  # Grants the ability to publish hosted tile layers from tile packages, features, and so on.
    PORTAL_PUBLISHER_PUBLISHSCENES = "portal:publisher:publishScenes"  # Grants the ability to publish hosted scene layers.
    PORTAL_PUBLISHER_PUBLISHTILEDIMAGERY = "portal:publisher:publishTiledImagery"  # Grants the ability to publish hosted tiled imagery layers from a single image or collection of images, and allows members to export a tile package from a hosted tiled imagery layer. This privilege requires an ArcGIS Image for ArcGIS Online user type extension license.
    PORTAL_PUBLISHER_PUBLISHDYNAMICIMAGERY = "portal:publisher:publishDynamicImagery"  # Grants the ability to publish hosted dynamic imagery layers from a single image or collection of images.
    PORTAL_USER_VIEWORGITEMS = "portal:user:viewOrgItems"  # Grants the ability to view content shared with the organization.
    PREMIUM_PUBLISHER_CREATENOTEBOOKS = "premium:publisher:createNotebooks"  # Grants the ability to create and edit interactive notebook documents.
    PREMIUM_PUBLISHER_SCHEDULENOTEBOOKS = "premium:publisher:scheduleNotebooks"  # Grants the ability to schedule notebooks.
    PORTAL_USER_VIEWTRACKS = "portal:user:viewTracks"  # Grants the ability to view members' location tracks via shared track views when location tracking is enabled.
    PORTAL_PUBLISHER_PUBLISHFEEDS = "portal:publisher:publishFeeds"  # Grants the ability to publish feeds to collect and display real-time data using ArcGIS Velocity. This privilege applies only to the organizations with ArcGIS Velocity license.
    PORTAL_PUBLISHER_PUBLISHREALTIMEANALYTICS = "portal:publisher:publishRealTimeAnalytics"  # Grants the ability to publish real-time analytics to analyze and process real-time data using ArcGIS Velocity. This privilege applies only to the organizations with ArcGIS Velocity license.
    PORTAL_PUBLISHER_PUBLISHBIGDATAANALYTICS = "portal:publisher:publishBigDataAnalytics"  # Grants the ability to publish big data analytics to analyze historical observation data using ArcGIS Velocity. This privilege applies only to the organizations with ArcGIS Velocity license.
    PORTAL_USER_REASSIGNITEMS = "portal:user:reassignItems"  # Grants a user the ability to reassign only their content to another member with the privilege to receive content.
    PORTAL_USER_RECEIVEITEMS = "portal:user:receiveItems"  # Grants a user the ability to receive content that is reassigned to them by another member with the privilege to reassign content.
    PORTAL_PUBLISHER_CREATEDATAPIPELINES = "portal:publisher:createDataPipelines"  # Grants the ability to create, edit, and run data pipelines.
    PORTAL_USER_GENERATEAPITOKENS = "portal:user:generateApiTokens"  # Grants a member the ability to generate API keys.
    PORTAL_USER_ASSIGNPRIVILEGESTOAPPS = "portal:user:assignPrivilegesToApps"  # Grants a member the ability to assign privileges to OAuth2.0 applications.
    PORTAL_USER_SHARETOGROUP = (
        "portal:user:shareToGroup"  # Grants the ability to share content to groups.
    )
    PORTAL_USER_SHARETOORG = "portal:user:shareToOrg"  # Grants the ability to share content to the organization.
    PORTAL_USER_SHARETOPUBLIC = "portal:user:shareToPublic"  # Grants the ability to share content to all users of the portal.
    PORTAL_USER_SHAREGROUPTOORG = "portal:user:shareGroupToOrg"  # Grants the ability to make groups discoverable by the organization.
    PORTAL_USER_SHAREGROUPTOPUBLIC = "portal:user:shareGroupToPublic"  # Grants the ability to make groups discoverable by all users of the portal.
    OPENDATA_USER_DESIGNATEGROUP = "opendata:user:designateGroup"  # Grants the ability to designate groups in the organization as being available for use in Open Data.
    PREMIUM_USER_GEOCODE = "premium:user:geocode"  # Grants the ability to perform large-volume geocoding tasks with the Esri World Geocoder, such as publishing a CSV file of addresses as a hosted feature layer.
    PREMIUM_USER_NETWORKANALYSIS = "premium:user:networkanalysis"  # Grants the ability to perform network analysis tasks such as routing and drive-time areas.
    PREMIUM_USER_SPATIALANALYSIS = "premium:user:spatialanalysis"  # Grants the ability to perform spatial analysis tasks.
    PREMIUM_USER_GEOENRICHMENT = (
        "premium:user:geoenrichment"  # Grants the ability to geoenrich features.
    )
    PREMIUM_USER_DEMOGRAPHICS = "premium:user:demographics"  # Grants the ability to make use of premium demographic data.
    PREMIUM_PUBLISHER_RASTERANALYSIS = "premium:publisher:rasteranalysis"  # Grants the ability to perform imagery and raster analysis tasks such as calculating slope. This requires an ArcGIS Image for ArcGIS Online user type extension license.
    PREMIUM_USER_FEATUREREPORT = "premium:user:featurereport"  # Grants the ability to create feature reports in ArcGIS Survey123.
    PREMIUM_PUBLISHER_CREATEADVANCEDNOTEBOOKS = "premium:publisher:createAdvancedNotebooks"  # Grants the ability to import and use ArcPy modules in notebooks.
    PORTAL_USER_RUNWEBTOOL = "portal:user:runWebTool"  # Grants the ability to run web tools published from notebooks.
    PREMIUM_USER_PLACES = "premium:user:places"  # Grants the ability to perform local place, or point of interest search with the new places-service (beta). Available for developer subscriptions only.
    FEATURES_USER_EDIT = "features:user:edit"  # Grants the ability to edit features in editable layers, according to the edit options enabled on the layer.
    FEATURES_USER_FULLEDIT = "features:user:fullEdit"  # Grants the ability to add, delete, and update features in a hosted feature layer regardless of the editing options enabled on the layer.
    PORTAL_ADMIN_VIEWUSERS = "portal:admin:viewUsers"  # Grants the ability to view full member account information in the organization.
    PORTAL_ADMIN_UPDATEUSERS = "portal:admin:updateUsers"  # Grants the ability to update member account information and categorize members in the organization.
    PORTAL_ADMIN_DELETEUSERS = "portal:admin:deleteUsers"  # Grants the ability to delete member accounts in the organization.
    PORTAL_ADMIN_INVITEUSERS = "portal:admin:inviteUsers"  # Grants the ability to invite members to the organization.
    PORTAL_ADMIN_DISABLEUSERS = "portal:admin:disableUsers"  # Grants the ability to enable and disable member accounts in the organization.
    PORTAL_ADMIN_CHANGEUSERROLES = "portal:admin:changeUserRoles"  # Grants the ability to change the role a member is assigned in the organization; however, it does not grant the ability to promote a member to, or demote a member from, the Administrator role. That privilege is reserved for the Administrator role alone.
    PORTAL_ADMIN_MANAGELICENSES = "portal:admin:manageLicenses"  # Grants the ability to assign licenses to members of the organization.
    PORTAL_ADMIN_UPDATEMEMBERCATEGORYSCHEMA = "portal:admin:updateMemberCategorySchema"  # Grants the ability to configure the organization member category schema.
    PORTAL_ADMIN_VIEWGROUPS = "portal:admin:viewGroups"  # Grants the ability to view all groups in the organization.
    PORTAL_ADMIN_UPDATEGROUPS = "portal:admin:updateGroups"  # Grants the ability to update groups in the organization.
    PORTAL_ADMIN_DELETEGROUPS = "portal:admin:deleteGroups"  # Grants the ability to delete groups in the organization.
    PORTAL_ADMIN_REASSIGNGROUPS = "portal:admin:reassignGroups"  # Grants the ability to reassign groups to other members in the organization.
    PORTAL_ADMIN_ASSIGNTOGROUPS = "portal:admin:assignToGroups"  # Grants the ability to assign members to, and remove members from, groups in the organization.
    PORTAL_ADMIN_MANAGEENTERPRISEGROUPS = "portal:admin:manageEnterpriseGroups"  # Grants the ability to link group membership to organization-specific groups.
    PORTAL_ADMIN_CREATELEAVINGDISALLOWEDGROUP = "portal:admin:createLeavingDisallowedGroup"  # Grants the ability to create and own groups that do not allow members to leave (administrative groups).
    PORTAL_ADMIN_CREATEUPDATECAPABLEGROUP = "portal:admin:createUpdateCapableGroup"  # Grants the ability to create and own groups with item update capabilities.
    PORTAL_ADMIN_VIEWITEMS = "portal:admin:viewItems"  # Grants the ability to view all content in the organization.
    PORTAL_ADMIN_UPDATEITEMS = "portal:admin:updateItems"  # Grants the ability to update and categorize content in the organization and edit hosted feature layers in your organization.
    PORTAL_ADMIN_DELETEITEMS = "portal:admin:deleteItems"  # Grants the ability to delete content in the organization.
    PORTAL_ADMIN_REASSIGNITEMS = "portal:admin:reassignItems"  # Grants the ability to reassign content to other members in the organization.
    PORTAL_ADMIN_UPDATEITEMCATEGORYSCHEMA = "portal:admin:updateItemCategorySchema"  # Grants the ability to configure the organization content category schema.
    PORTAL_PUBLISHER_PUBLISHSERVERGPSERVICES = "portal:publisher:publishServerGPServices"  # Grants the ability to publish web tools to an organization.
    PORTAL_ADMIN_SHARETOORG = "portal:admin:shareToOrg"  # Grants the ability to share other members' content to the organization.
    PORTAL_ADMIN_SHARETOPUBLIC = "portal:admin:shareToPublic"  # Grants the ability to share other members' content to all users of the portal.
    PORTAL_ADMIN_CREATEREPORTS = "portal:admin:createReports"  # Grants the ability to create and manage administrative reports for the organization.
    MARKETPLACE_ADMIN_MANAGE = "marketplace:admin:manage"  # Grants the ability to create listings and list items and manage subscriptions in ArcGIS Marketplace.
    MARKETPLACE_ADMIN_PURCHASE = "marketplace:admin:purchase"  # Grants the ability to request purchase information about apps and data in ArcGIS Marketplace.
    MARKETPLACE_ADMIN_STARTTRIAL = "marketplace:admin:startTrial"  # Grants the ability to start trial subscriptions in ArcGIS Marketplace.
    PORTAL_ADMIN_MANAGESECURITY = "portal:admin:manageSecurity"  # Grants the ability to manage the organization's security and infrastructure settings.
    PORTAL_ADMIN_MANAGEWEBSITE = "portal:admin:manageWebsite"  # Grants the ability to manage the organization's website settings.
    PORTAL_ADMIN_MANAGECOLLABORATIONS = "portal:admin:manageCollaborations"  # Grants the ability to manage the organization's collaborations.
    PORTAL_ADMIN_MANAGECREDITS = "portal:admin:manageCredits"  # Grants the ability to manage the organization's credit budgeting settings.
    PORTAL_ADMIN_MANAGEROLES = "portal:admin:manageRoles"  # Grants the ability to manage the organization's member roles.
    PORTAL_ADMIN_MANAGEUTILITYSERVICES = "portal:admin:manageUtilityServices"  # Grants the ability to manage the organization's utility service settings.
    OPENDATA_USER_OPENDATAADMIN = "opendata:user:openDataAdmin"  # Grants the ability to manage Open Data Sites for the organization.
    PORTAL_PUBLISHER_PUBLISHSERVERSERVICES = "portal:publisher:publishServerServices"  # Grants the ability to publish ArcGIS Server web layers to ArcGIS Server sites that are federated with the portal. These services often reference registered data from geodatabases or file-based data sources. This privilege is also required for members who will bulk publish layers from a data store item.
    PORTAL_PUBLISHER_PUBLISHKNOWLEDGEGRAPH = "portal:publisher:publishKnowledgeGraph"  # Grants the ability to publish hosted knowledge graphs in ArcGIS Pro. This privilege is only visible if an ArcGIS Knowledge Server is configured for your organization.
    PORTAL_PUBLISHER_REGISTERDATASTORES = "portal:publisher:registerDataStores"  # Grants the ability to add data store items to the portal.
    PORTAL_PUBLISHER_BULKPUBLISHFROMDATASTORES = "portal:publisher:bulkPublishFromDataStores"  # Grants the owner of a database data store item the ability to publish feature and map layers from all feature classes and tables that can be accessed in the database.
    PORTAL_PUBLISHER_PUBLISHVIDEO = "portal:publisher:PublishVideo"  # Introduced at ArcGIS Enterprise 11.2. Grants a user the ability to publish hosted video layers from video file and supported video metadata files.
    PORTAL_PUBLISHER_PUBLISHLIVESTREAMVIDEO = "portal:publisher:publishLivestreamVideo"  # Introduced at ArcGIS Enterprise 11.2. Grants a user the ability to publish hosted livestream video layers sourced from network video broadcasts and streams.
    PREMIUM_PUBLISHER_GEOANALYTICS = "premium:publisher:geoanalytics"  # Grants the ability to perform GeoAnalytics tasks.LegacyThe ArcGIS GeoAnalytics Server extension is deprecated with the release of ArcGIS Enterprise 11.4. The last ArcGIS Enterprise release with support for GeoAnalytics Server is 11.3.
    FEATURES_USER_MANAGEVERSIONS = "features:user:manageVersions"  # Grants the ability to view, alter, and delete all branch versions accessed through an ArcGIS Server web feature layer, as well as the ability to manage version locks.NoteIf this privilege is assigned in the front-end of ArcGIS Enterprise portal, the following privileges are assigned by default. Users assigned the features:user:manageVersions privilege and those from the list below are considered to be version administrators. features:user:edit features:user:fullEdit
    PORTAL_PUBLISHER_CREATEFEATUREWEBHOOK = "portal:publisher:createFeatureWebhook"  # Grants the ability to create, edit, and delete their own feature layer webhooks.
    PORTAL_ADMIN_CREATEGPWEBHOOK = "portal:admin:createGPWebhook"  # Grants the ability to create, edit, and delete geoprocessing webhooks.
    PORTAL_ADMIN_MANAGESERVERS = "portal:admin:manageServers"  # Grants the ability to manage the portal's server settings.
    PORTAL_ADMIN_MANAGEWEBHOOKS = "portal:admin:manageWebhooks"  # Grants the ability to create, edit, and delete organizational webhooks and manage all webhooks within the portal.


###########################################################################
class DeveloperCredential:
    """
    A class to represents developer credentials for a user's GIS.
    """

    _session: EsriSession | None = None
    token: str
    expiration: int
    privileges: list
    item: Item

    def __init__(self, item: Item):
        self._item = item
        self._gis: GIS = item._gis
        self._session: EsriSession = item._gis.session

    @property
    def session(self) -> EsriSession:
        """returns the session for the DeveloperCredential"""
        if self._session is None:
            self._session = self._item._gis.session
        return self._session

    def __str__(self):
        return '<%s Item ID:"%s">' % (type(self).__name__, self._item.id)

    def __repr__(self):
        return '<%s Item ID:"%s">' % (type(self).__name__, self._item.id)

    # ----------------------------------------------------------------------
    def delete(self) -> bool:
        """
        Deletes the scoped token.
        """
        if self._item is not None:
            return self._item.delete(permanent=True)
        return False

    # ----------------------------------------------------------------------
    def update(
        self,
        redirect_uris: list[str] | None = None,
        referers: list[str] | None = None,
        privileges: list[str] | None = None,
    ) -> bool:
        """
        Updates the scoped token with new privileges.
        """
        if redirect_uris is None and referers is None and privileges is None:
            return False
        privileges = privileges or self._item.app_info["privileges"]
        if isinstance(privileges, str):
            privileges = json.load(privileges)
        token_privileges: list[str] = []
        for priv in privileges:
            if isinstance(priv, str):
                token_privileges.append(priv)
            elif isinstance(priv, TokenPrivilege):
                token_privileges.append(priv.value)

        params = {
            "f": "json",
            "client_id": self._item.app_info["client_id"],
            "httpReferrers": referers
            or json.dumps(self._item.app_info["httpReferrers"]),
            "privileges": json.dumps(token_privileges),
        }

        url: str = (
            f"{self._gis.url}/sharing/rest/oauth2/apps/{self._item.app_info['client_id']}/update"
        )

        resp = self.session.post(url=url, data=params)
        resp.raise_for_status()
        data: dict = resp.json()
        if "error" in data:
            raise Exception(str(data))
        return data["apiToken1Active"] == False and data["apiToken2Active"] == False

    # ----------------------------------------------------------------------
    def revoke(self, slot: int = 1) -> bool:
        """
        Revokes the scoped token.
        """
        app_info: dict = self._item.app_info
        params: dict = {
            "f": "json",
            "client_id": app_info.get("client_id", ""),
            "client_secret": app_info.get("client_secret", ""),
            "apiToken": slot,
        }
        url: str = f"{self._gis.url}/sharing/rest/oauth2/revokeToken"
        resp = self.session.post(url=url, data=params)
        resp.raise_for_status()
        return resp.json().get("success", False)

    # ----------------------------------------------------------------------
    def generate_token(
        self, slot: int = 1, expiration: _dt.datetime | None = None
    ) -> dict | None:
        """
        Generates a new token for the scoped token.

        ================  ===============================================================
        **Parameter**      **Description**
        ----------------  ---------------------------------------------------------------
        slot              Optional int. API keys support 2 API tokens.  The `slot` allows
                          users to specify which API Key to create or regenerate. The
                          values can be 1 or 2. The default is 1.
        ----------------  ---------------------------------------------------------------
        expiration        Optional datetime.datetime. The time when the expiration expires.
                          The maximum value is 1 year from the time you create the API Key.
                          The default is 26 weeks from today.
        ================  ===============================================================

        :returns: dict | None

        """
        if expiration is None:
            expiration = _dt.datetime.now() + _dt.timedelta(weeks=26)
        if self._item is not None:
            return self._generate_api_token(slot, False, expiration)
        else:
            raise ValueError(
                "The Scoped Token not longer exists, please verify that the Item exists."
            )

    # ----------------------------------------------------------------------
    def regenerate_token(
        self, slot: int = 1, expiration: _dt.datetime | None = None
    ) -> dict | None:
        """
        Regenerates the token for the scoped token.

        ================  ===============================================================
        **Parameter**      **Description**
        ----------------  ---------------------------------------------------------------
        slot              Optional int. API keys support 2 API tokens.  The `slot` allows
                          users to specifiy which API Key to create or regenerate. The
                          values can be 1 or 2. The default is 1.
        ----------------  ---------------------------------------------------------------
        expiration        Optional datetime.datetime. The time when the expiration expires.
                          The maximum value is 1 year from the time you create the API Key.
                          The default is 26 weeks from today.
        ================  ===============================================================

        :returns: dict | None

        """
        if expiration is None:
            expiration = _dt.datetime.now() + _dt.timedelta(weeks=26)
        if self._item is not None:
            return self._generate_api_token(slot, True, expiration)
        else:
            raise ValueError(
                "The Scoped Token not longer exists, please verify that the Item exists."
            )

    # ----------------------------------------------------------------------
    def _generate_api_token(
        self,
        slot: int = 1,
        regenerate: bool = False,
        expiration: _dt.datetime | None = None,
    ) -> dict:
        """
        Generates a Developer Token from an Developer Token Item.

        ================  ===============================================================
        **Parameter**      **Description**
        ----------------  ---------------------------------------------------------------
        slot              Optional int. API keys support 2 API tokens.  The `slot` allows
                          users to specifiy which API Key to create or regenerate. The
                          values can be 1 or 2. The default is 1.
        ----------------  ---------------------------------------------------------------
        regenerate        Optional bool. When True, this will re-create the API token.
                          The default is False.
        ----------------  ---------------------------------------------------------------
        expiration        Optional datetime.datetime. The time when the expiration expires.
                          The maximum value is 1 year from the time you create the API Key.
        ================  ===============================================================

        :returns: dict
        """
        import datetime as _dt

        if not self._gis.version >= [2025, 1]:
            raise Exception(
                "The `GIS` does not support Developer Credentials, please use Enterprise 11.5+ or ArcGIS Online."
            )

        app_info: dict = self._item.app_info
        max_date: _dt.datetime = _dt.datetime.now() + _dt.timedelta(weeks=52)
        if expiration and expiration > max_date:
            raise ValueError(
                "The expiration value cannot be longer than one year from today."
            )
        if self._item.type != "Application":
            raise ValueError(
                "This item is not allowed to create developer tokens, please select the proper item type."
            )
        if app_info is None:
            raise Exception(
                "Please register your application before generating developer api keys."
            )
        if not slot in [1, 2]:
            raise ValueError("The `slot` value must be 1 or 2.")
        url: str = f"{self._gis.url}/sharing/rest/oauth2/token"
        client_id, client_secret = app_info.get("client_id"), app_info.get(
            "client_secret"
        )

        slot_key: str = f"apiToken{slot}ExpirationDate"

        if regenerate and expiration is None:
            expiration: _dt.datetime = _dt.datetime.now() + _dt.timedelta(weeks=26)
            warnings.warn(
                f"The `expiration` was not set, setting the new expiration to be {expiration.strftime('%B %d, %Y %I:%M %p')}"
            )
            self.update({slot_key: int(expiration.timestamp() * 1000)})
        if getattr(self, slot_key, -1) < 0:
            self.update({slot_key: int(expiration.timestamp() * 1000)})
        self._item.update({slot_key: int(expiration.timestamp() * 1000)})
        params: dict = {
            "f": "json",
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "apiToken": slot,
            "regenerateApiToken": json.dumps(regenerate),
        }
        resp = self.session.post(url, data=params)
        resp.raise_for_status()
        data = resp.json()
        self._item = self._gis.content.get(self._item.itemid)
        return data


###########################################################################
class DeveloperCredentialManager:
    """
    A class to manage developer crednetials for a user's GIS.
    """

    _gis: GIS
    session: EsriSession

    def __init__(self, gis: GIS):
        self._gis: GIS = gis
        self.session: EsriSession = gis.session

    def create(
        self,
        title: str,
        privileges: list[Union[TokenPrivilege, str]],
        referers: list[str],
        expiration: _dt.datetime,
        items: list[Item] | None = None,
        redirect_uris: list[str] | None = None,
        tags: list[str] | str | None = None,
        snippet: str | None = None,
        folder: Folder | None = None,
    ) -> DeveloperCredential:
        """
        Creates Developer Credentials on a given GIS.

        ================  ===============================================================
        **Parameter**      **Description**
        ----------------  ---------------------------------------------------------------
        title             Required str. The name of the developer credential item
        ----------------  ---------------------------------------------------------------
        privileges:       Required List[Union[TokenPrivilege, str]]. List of privileges
                          for the developer credential.
        ----------------  ---------------------------------------------------------------
        referers          Required List[str]. List of referers.
        ----------------  ---------------------------------------------------------------
        expiration        Required _dt.datetime. The date the developer credentials
                          expire.  This cannot be more than 1 year from date of creation.
        ----------------  ---------------------------------------------------------------
        items             Optional list[Item]. A list of items to restrict access to.
        ----------------  ---------------------------------------------------------------
        redirect_uris     Optional list[str]. Allowed list of redirect uris.
        ----------------  ---------------------------------------------------------------
        tags              Optional list[str] | str | None. The tags for the item.
        ----------------  ---------------------------------------------------------------
        snippet           Optional String. The snippet of the developer credential item.
        ----------------  ---------------------------------------------------------------
        folder            Optional Folder. The save location for the developer credentials.
        ================  ===============================================================

        """
        then: _dt.datetime = _dt.datetime.now() + _dt.timedelta(weeks=52)

        if folder is None:
            folder: Folder = self._gis.content.folders.get()
        if redirect_uris is None:
            redirect_uris = ["urn:ietf:wg:oauth:2.0:oob"]

        if expiration > then:
            raise ValueError(
                "The `expiration` cannot be greater than one year from now."
            )
        if not isinstance(redirect_uris, list) or not all(
            isinstance(i, str) for i in redirect_uris
        ):
            raise ValueError("`redirect_uris` must be a list containing only strings")
        if not isinstance(referers, list) or not all(
            isinstance(i, str) for i in referers
        ):
            raise ValueError("`referers` must be a list containing only strings")
        ip: ItemProperties = ItemProperties(
            item_type=ItemTypeEnum.APPLICATION,
            title=title,
            tags=tags,
            snippet=snippet,
            type_keywords=[],
            api_token1_expiration=expiration,
        )
        item: Item = folder.add(item_properties=ip).result()
        # Check if Personal Token
        #
        non_personal_privileges: tuple = {
            "premium:user:basemaps",
            "premium:user:geoenrichment",
            "premium:user:geocode",
            "premium:user:networkanalysis",
            "premium:user:spatialanalysis",
            "portal:publisher:publishDynamicImagery",
            "portal:publisher:publishTiledImagery",
            "premium:publisher:rasteranalysis",
            "portal:publisher:publishFeatures",
            "portal:user:createItem",
        }
        is_personal_token: bool = False
        is_updated = False
        token_priveleges: list[str] = []
        for privilege in privileges:
            if isinstance(privilege, TokenPrivilege):
                privilege = privilege.value
            if not isinstance(privilege, str):
                raise ValueError(
                    "The `privileges` must be a list of strings or TokenPrivilege Enum values."
                )
            if not privilege in token_priveleges:
                token_priveleges.append(privilege)

            if (
                is_updated == False
                and self._gis._is_arcgisonline
                and privilege not in non_personal_privileges
            ):
                is_personal_token = True
                is_updated = True
        result = item.register(
            app_type="multiple",
            redirect_uris=redirect_uris,
            http_referers=referers,
            privileges=token_priveleges,
            personal_token=is_personal_token,
        )
        assert result
        assert item.update(
            item_properties={
                "apiToken1ExpirationDate": int(expiration.timestamp() * 1000)
            }
        )
        return DeveloperCredential(item=item)

    def get(self, itemid: str) -> DeveloperCredential:
        """gets a scoped token by the item id"""
        item: Item = self._gis.content.get(itemid)
        if item.type == "Application" and "APIToken" in item.typeKeywords:
            return DeveloperCredential(item)

    def list(self, owner: User | None = None) -> Iterable[DeveloperCredential]:
        """
        Lists all the scoped tokens for a user.

        ================  ===============================================================
        **Parameter**      **Description**
        ----------------  ---------------------------------------------------------------
        owner             Optional User. The user to list the scoped tokens for. The default
                          is the current logged in user.
        ================  ===============================================================

        :returns: list of DeveloperCredential objects
        """
        if owner is None:
            owner = self._gis.users.me
        username = owner.username
        query_string: str = (
            f'owner: {username}  -typekeywords:("MapAreaPackage") -type:("Map Area" OR "Indoors Map Configuration" OR "Code Attachment") '
            '(type:("Application" AND typekeywords:"Registered App" AND typekeywords:"APIToken"))'
        )
        result = self._gis.content.advanced_search(query_string, max_items=-1)

        for result in result["results"]:

            yield DeveloperCredential(item=result)
