from __future__ import annotations
import json
from arcgis.gis.kubernetes._admin._base import _BaseKube
from ._deployment import DeploymentManager
from ._upgrades import UpgradeManager
from ._recovery import RecoveryManager
from ._content import LanguageManager, ExternalContentManager
from ._architecture import ArchitectureManager
from ._tasks import TaskManager
from ._adaptors import WebAdaptorManager
from ._license import LicenseManager
from typing import List, Dict, Any, Tuple, Optional
from arcgis.gis.admin import LivingAtlas
from arcgis.auth import EsriSession


class Server(_BaseKube):
    """Represents a single kubernetes service"""

    # ----------------------------------------------------------------------
    def update(self, server_json):
        """
        Updates the server JSON

        :return: Boolean

        """
        url = f"{self._url}/edit"
        params = {"f": "json", "serverJson": server_json}
        res = self._con.post(url, params)
        if "status" in res:
            return res["status"] == "success"
        return res

    # ----------------------------------------------------------------------
    @property
    def status(self):
        """returns the status of the server"""
        url = f"{self._url}/status"
        params = {"f": "json"}
        return self._con.get(url, params)

    # ----------------------------------------------------------------------
    def stop(self):
        """returns the status of the server"""
        url = f"{self._url}/stop"
        params = {"f": "json"}
        res = self._con.post(url, params)
        if "status" in res:
            return res["status"] == "success"
        return res

    # ----------------------------------------------------------------------
    def start(self):
        """returns the status of the server"""
        url = f"{self._url}/start"
        params = {"f": "json"}
        res = self._con.post(url, params)
        if "status" in res:
            return res["status"] == "success"
        return res


###########################################################################
class ServerDefaults(_BaseKube):
    """Represents the server default values"""

    @property
    def properties(self):
        url = self._url
        params = {"f": "json"}
        return self._con.get(url, params)

    @properties.setter
    def properties(self, value):
        if self.properties != value:
            url = self._url + "/edit"
            params = {"f": "json", "propertyJson": value}
            self._con.post(url, params)


###########################################################################
class ServerManager(_BaseKube):
    """Manages the Registered Servers"""

    _gis = None
    _con = None
    _properties = None

    # ----------------------------------------------------------------------
    @property
    def list(self):
        """
        Allows for the modification of Servers

        :return: List of Server objects

        """
        servers = []
        if "servers" in self.properties:
            for server in self.properties.servers:
                url = f"{self._url}/{server.id}"
                servers.append(Server(url, self._gis))
        return servers

    # ----------------------------------------------------------------------
    @property
    def defaults(self):
        """
        Returns the default properties for each server type.

        :return: list of `ServerDefault`

        """
        d = []
        url = f"{self._url}/properties"
        params = {"f": "json"}
        res = self._con.get(url, params)
        if "properties" in res:
            for i in res["properties"]:
                purl = f"{url}/{i['id']}"
                d.append(ServerDefaults(url=purl, gis=self._gis))
        return d


###########################################################################
class Indexer(_BaseKube):
    """
    This resource contains connection information to the default indexing service.
    """

    def reconfigure(self) -> bool:
        """
        This operation recreates the index service metadata, schema, and data in the event it becomes corrupted.

        :returns: Boolean
        """
        params = {"f": "json"}
        url = f"{self._url}/reconfigure"
        res = self._con.post(url, params)
        return res.get("status", "failed") == "success"

    @property
    def status(self):
        """
        `status` allows you to view the status of the indexing service. You
        can view the number of users, groups, and search items in both the
        database (store) and the index. If the database and index do not
        match, indexing is either in progress or there is a problem with
        the index. It is recommended that you reindex to correct any
        issues. If indexing is in progress, you can monitor the status by
        refreshing the page.

        :return: dict

        """
        params = {"f": "json"}
        url = f"{self._url}/status"
        return self._con.get(url, params)

    def reindex(self, mode, includes=None):
        """
        The operation allows you to generate or update the indexes for content, such as users, groups, and items stored in the database store.

        ===============     ====================================================================
        **Parameter**        **Description**
        ---------------     --------------------------------------------------------------------
        mode                Required String. The mode in which the indexer should run.
                            Values: USER_MODE, GROUP_MODE, SEARCH_MODE, or FULL_MODE
        ---------------     --------------------------------------------------------------------
        includes            Optional String. A comma separated list of elements to include in
                            the index. This is useful if you want to only index certain items
                            or user accounts.
        ===============     ====================================================================

        :return: Boolean

        """
        url = f"{self._url}/reindex"
        params = {"f": "json", "mode": mode, "includes": includes}
        res = self._con.post(url, params)
        if "status" in res:
            return res["status"] == "success"
        return res


class Container:
    """
    A single representation of a registered container.
    """

    _properties = None
    _url = None
    _con = None
    _gis = None

    def __init__(self, url: str, gis: "GIS"):
        self._url = url
        self._gis = gis
        self._con = gis._con

    def properties(self):
        """returns the properties of the endpoint"""
        return self._con.get(self._url, {"f": "json"})

    def edit(self, value: dict) -> dict:
        """
        Allows certain container registry properties to be updated after your organization has been configured.

        ===============     ====================================================================
        **Parameter**        **Description**
        ---------------     --------------------------------------------------------------------
        value               Required dict. A dictionary of registry properties.
        ===============     ====================================================================



        :returns: bool
        """
        url = f"{self._url}/edit"
        params = {"f": "json", "containerRegistryJson": value}
        return self._con.post(url, params).get("status", "failed") == "success"


###########################################################################
class SystemManager:
    """
    The system resource is a collection of system-wide resources for a
    deployment, such as the configuration store and deployment-wide
    security.
    """

    _recovery = None
    _indexer = None
    _sm = None
    _deployments = None
    _upgrades = None
    _license = None

    # ----------------------------------------------------------------------
    def __init__(self, url, gis, initialize=False):
        """Constructor


        ==================     ====================================================================
        **Parameter**           **Description**
        ------------------     --------------------------------------------------------------------
        url                    Required string. The machine URL.
        ------------------     --------------------------------------------------------------------
        gis                    Optional string. The GIS or Server object..
        ==================     ====================================================================

        """

        self._url = url
        self._gis = gis
        self._con = gis._con
        if initialize:
            self._init(gis._con)

    # ----------------------------------------------------------------------
    def __str__(self) -> str:
        return "<%s at %s>" % (type(self).__name__, self._url)

    # ----------------------------------------------------------------------
    def __repr__(self) -> str:
        return "<%s at %s>" % (type(self).__name__, self._url)

    # ----------------------------------------------------------------------
    def _init(self, connection=None) -> None:
        """loads the properties into the class"""
        if connection is None:
            connection = self._con
        params = {"f": "json"}
        try:
            result = connection.get(path=self._url, params=params)
            if isinstance(result, dict):
                self._json_dict = result
                self._properties = result
            else:
                self._json_dict = {}
                self._properties = {}
        except:
            self._json_dict = {}
            self._properties = {}

    # ----------------------------------------------------------------------
    @property
    def url(self) -> str:
        """gets/sets the service url"""
        return self._url

    # ----------------------------------------------------------------------
    @url.setter
    def url(self, value) -> None:
        """gets/sets the service url"""
        self._url = value
        self._refresh()

    # ----------------------------------------------------------------------
    def __iter__(self):
        """creates iterable for classes properties"""
        for k, v in self._json_dict.items():
            yield k, v

    # ----------------------------------------------------------------------
    def _refresh(self) -> None:
        """reloads all the properties of a given service"""
        self._init()

    # ----------------------------------------------------------------------
    @property
    def properties(self) -> Dict[str, Any]:
        """
        returns the object properties
        """
        if self._properties is None:
            self._init()
        return self._properties

    # ----------------------------------------------------------------------
    @property
    def living_atlas(self) -> LivingAtlas:
        """provides functionality to manage living atlas content"""
        return LivingAtlas(url=f"{self._url}/content/livingatlas", gis=self._gis)

    # ----------------------------------------------------------------------
    @property
    def containers(self) -> list:
        """
        Returns the registered containers.

        :returns: list
        """
        url = f"{self._url}/containerregistries"
        params = {"f": "json"}
        return [
            Container(url=f"{url}/{container['id']}", gis=self._gis)
            for container in self._con.get(url, params).get("containerRegistries", [])
        ]

    # ----------------------------------------------------------------------
    @property
    def deployments(self) -> DeploymentManager:
        """Manages the deployment settings for enterprise"""
        url = f"{self._url}/deployments"
        if self._deployments is None:
            self._deployments = DeploymentManager(url=url, gis=self._gis)
        return self._deployments

    # ----------------------------------------------------------------------
    @property
    def upgrades(self) -> UpgradeManager:
        """
        The upgrades resource provides access to child operations and
        resources that are used to manage the release and patch updates that
        can be applied to an ArcGIS Enterprise on Kubernetes deployment.
        During the upgrade process, this resource returns detailed,
        real-time messages regarding the status and progress of the
        upgrade. While an upgrade is in progress, child operations and
        resources for this resource will be inaccessible. Once completed,
        all child endpoints will be operational, and the JSON view of the
        resource will contain a log of the job messages and the completion
        status.
        """
        url = f"{self._url}/upgrades"
        if self._upgrades is None:
            self._upgrades = UpgradeManager(url=url, gis=self._gis)
        return self._upgrades

    # ----------------------------------------------------------------------
    @property
    def container_images(self) -> dict:
        """
        Returns a list of the container images that have been pulled and
        used to deploy applications for ArcGIS Enterprise on Kubernetes.

        :returns: dict

        """
        url: str = f"{self._url}/containerimages"
        params: dict = {"f": "json"}
        return self._con.get(url, params=params)

    # ----------------------------------------------------------------------
    @property
    def recovery(self) -> RecoveryManager:
        """
        This resource allows an administrator the ability to manage
        disaster recovery settings.

        :return: RecoveryManager
        """
        if self._recovery is None:
            url = f"{self._url}/disasterrecovery"
            self._recovery = RecoveryManager(url=url, gis=self._gis)
        return self._recovery

    # ----------------------------------------------------------------------
    @property
    def web_adaptors(self) -> WebAdaptorManager:
        """
        The webadaptors resource lists the ArcGIS Enterprise on Kubernetes Web Adaptor configured your deployment. The web adaptor can be configured using the config operation.
        """
        url = f"{self._url}/webadaptors"
        return WebAdaptorManager(url=url, gis=self._gis)

    # ----------------------------------------------------------------------
    @property
    def licenses(self) -> List[Dict[str, Any]]:
        """
        The licenses resource lists the current license level of ArcGIS Server and all authorized extensions.

        :return: List[Dict[str, Any]]
        """
        url = f"{self._url}/licenses"
        return LicenseManager(url=url, gis=self._gis)

    # ----------------------------------------------------------------------
    @property
    def external_content(self) -> ExternalContentManager:
        """ """
        return ExternalContentManager(url=f"{self._url}/content", gis=self._gis)

    # ----------------------------------------------------------------------
    @property
    def language(self) -> LanguageManager:
        """
        The content resource provides access to the languages resource.
        The languages resource provides a list of current languages for an
        organization.

        :return: LanguageManager

        """
        return LanguageManager(url=f"{self._url}/content", gis=self._gis)

    # ----------------------------------------------------------------------
    @property
    def enterprise_functions(self) -> "EnterpriseFunctions":
        """
        Returns the manager for working with enterprise functions.
        """
        url: str = f"{self._url}/enterprisefunctions"
        return EnterpriseFunctions(url, gis=self._gis)

    # ----------------------------------------------------------------------
    @property
    def tasks(self) -> TaskManager:
        """
        This resource returns a list of tasks (CleanGPJobs, BackupRetentionCleaner, CreateBackup) that exist within your deployment.
        """
        url = f"{self._url}/tasks"
        return TaskManager(url=url, gis=self._gis)

    # ----------------------------------------------------------------------
    @property
    def architecture_profiles(self) -> ArchitectureManager:
        """
        This resource returns the architecture profile that is set when an organization is configured and provides access to all three architecture
        profile resources: development, standard-availability, and enhanced-availability.
        """
        url = f"{self._url}/architectureprofiles"
        return ArchitectureManager(url=url, gis=self._gis)

    # ----------------------------------------------------------------------
    @property
    def properties(self) -> dict:
        """
        Gets/Sets the system properties resource list system properties
        that have been modified to control the portal's environment.

        ===============     ====================================================================
        **Parameter**        **Description**
        ---------------     --------------------------------------------------------------------
        value               Required dict. A dictionary of registry properties.

                            Allowed Key/Value:

                            ==========================================     ====================================================================
                            **Parameter**                                   **Description**
                            ------------------------------------------     --------------------------------------------------------------------
                            versionManifestURL                             The URL to the version manifest used in the upgrade process. This property should not be modified.
                            ------------------------------------------     --------------------------------------------------------------------
                            containerStartUpTimeoutSeconds                 The timeout (in seconds) for the start-up of containers during the upgrade process. The default value is 900.
                            ------------------------------------------     --------------------------------------------------------------------
                            allowGPAndExtensionPublishingToPublishers      Introduced at 11.0. When set as true, this property allows administrators and publishers to publish geoprocessing services and extensions. By default, only administrators can publish extensions and geoprocessing services.
                            ==========================================     ====================================================================

        ===============     ====================================================================

        :return: dict
        """
        url = f"{self._url}/properties"
        params = {"f": "json"}
        return self._con.get(url, params)

    # ----------------------------------------------------------------------
    @properties.setter
    def properties(self, value: dict) -> None:
        """
        Gets/Sets the system properties resource list system properties
        that have been modified to control the portal's environment.


        ===============     ====================================================================
        **Parameter**        **Description**
        ---------------     --------------------------------------------------------------------
        value               Required dict. A dictionary of registry properties.

                            Allowed Key/Value:

                            ==========================================     ====================================================================
                            **Parameter**                                   **Description**
                            ------------------------------------------     --------------------------------------------------------------------
                            versionManifestURL                             The URL to the version manifest used in the upgrade process. This property should not be modified.
                            ------------------------------------------     --------------------------------------------------------------------
                            containerStartUpTimeoutSeconds                 The timeout (in seconds) for the start-up of containers during the upgrade process. The default value is 900.
                            ------------------------------------------     --------------------------------------------------------------------
                            allowGPAndExtensionPublishingToPublishers      Introduced at 11.0. When set as true, this property allows administrators and publishers to publish geoprocessing services and extensions. By default, only administrators can publish extensions and geoprocessing services.
                            ==========================================     ====================================================================

        ===============     ====================================================================


        :return: dict
        """
        url = f"{self._url}/properties/update"
        params = {"f": "json", "properties": value}
        return self._con.post(url, params)

    # ----------------------------------------------------------------------
    @property
    def indexer(self):
        """
        Allows user to manage the site's indexer

        :return: `Indexer`
        """
        if self._indexer is None:
            url = f"{self._url}/indexer"
            self._indexer = Indexer(url=url, gis=self._gis)
        return self._indexer

    # ----------------------------------------------------------------------
    @property
    def servers(self) -> ServerManager:
        """Returns a manager to work with ArcGIS Servers registerd with Kubernetes"""
        if self._sm is None:
            self._sm = ServerManager(url=f"{self._url}/servers", gis=self._gis)
        return self._sm


class EnterpriseFunctions:
    """
    The EnterpriseFunctions resource returns the premium capabilities that
    are enabled for an organization. Currently, administrators can enable
    it if the organization has been licensed to use these premium
    capabilities.
    """

    _url: str | None = None
    _gis: "GIS" | None = None
    url: str
    session: EsriSession

    def __init__(self, url: str, gis: "GIS" = None) -> None:
        """class initializer"""
        super()
        self._gis = gis
        self.session = gis.session
        self.url = url
        self._url = url

    # ----------------------------------------------------------------------
    def __str__(self) -> str:
        return "<%s at %s>" % (type(self).__name__, self._url)

    # ----------------------------------------------------------------------
    def __repr__(self) -> str:
        return "<%s at %s>" % (type(self).__name__, self._url)

    # ----------------------------------------------------------------------
    @property
    def properties(self) -> Dict[str, Any]:
        """
        returns the object properties
        """

        resp = self.session.get(
            url=self.url,
            params={
                "f": "json",
            },
        )
        resp.raise_for_status()
        self._properties = resp.json()
        return self._properties

    @property
    def enabled_functions(self) -> list[str]:
        """returns a list of enabled functionality on the deployment"""
        return self.properties["enterpriseFunctionsEnabled"]

    @property
    def licensed(self) -> list[str]:
        """returns a list of available licensed functionality"""
        return self.properties["enterpriseFunctionsLicensed"]

    def disable(self, function: str) -> dict:
        """
        Disables licensed functionality on a kubernetes deployment

        ===============     ====================================================================
        **Parameter**        **Description**
        ---------------     --------------------------------------------------------------------
        function            Required String. The service function to disable on the kubernetes site.
        ===============     ====================================================================

        """

        if function in self.licensed and not function in self.enabled_functions:
            raise Exception(f"The function {function} is already disabled.")
        elif not function in self.licensed:
            raise ValueError(f"The value must be {','.join(self.licensed)}")

        url: str = f"{self._url}/remove"
        params: dict = {
            "f": "json",
            "enterpriseFunction": function,
        }
        status_ends = ["COMPLETED", "FAILED", "ERROR"]
        req = self.session.post(url, data=params)
        req.raise_for_status()
        response: dict = req.json()
        if "jobsUrl" in response:
            job_url: str = response.get("jobsUrl")
            if not job_url:
                raise Exception(response)
            status_response = self.session.get(job_url, params={"f": "json"})
            i: int = 1
            import time

            while not status_response.json().get("status") in status_ends:
                time.sleep(i)
                if i <= 10:
                    i += 1
                status_response = self.session.get(job_url, params={"f": "json"})
            return status_response.json()
        else:
            return response

    def enable(self, function: str) -> dict:
        """
        Enables the select licensed functionality

        ===============     ====================================================================
        **Parameter**        **Description**
        ---------------     --------------------------------------------------------------------
        function            Required String. The service function to enable on the kubernetes site.
        ===============     ====================================================================

        """
        if function in self.licensed and function in self.enabled_functions:
            raise Exception(f"The function {function} is already enabled.")
        elif not function in self.licensed:
            raise ValueError(f"The value must be {','.join(self.licensed)}")

        url: str = f"{self._url}/add"
        params: dict = {
            "f": "json",
            "enterpriseFunction": function,
        }
        status_ends = ["COMPLETED", "FAILED", "ERROR"]
        req = self.session.post(url, data=params)
        req.raise_for_status()
        response: dict = req.json()
        if "jobsUrl" in response:
            job_url: str = response.get("jobsUrl")
            if not job_url:
                raise Exception(response)
            status_response = self.session.get(job_url, params={"f": "json"})
            i: int = 1
            import time

            while not status_response.json().get("status") in status_ends:
                time.sleep(i)
                if i <= 10:
                    i += 1
                status_response = self.session.get(job_url, params={"f": "json"})
            return status_response.json()
        else:
            return response
