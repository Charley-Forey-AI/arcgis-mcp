from __future__ import annotations
from arcgis.gis.kubernetes._admin._base import _BaseKube
from arcgis.gis import GIS
from typing import Dict, Any, Optional, List


class Deployment:
    """
    This class represents a single microservice *deployment*. Objects of this class
    are not meant to be initialized directly, but instead are returned by the
    :meth:`~arcgis.gis.kubernetes.DeploymentManager.search` and the
    :meth:`~arcgis.gis.kubernetes.DeploymentManager.get` methods of the
    :class:`~arcgis.gis.kubernetes.DeploymentManager` class.

    .. code-block:: python

        # Usage Example: Getting a Deployment object

        >>> from arcgis.gis import GIS
        >>> gis = GIS(profile="your_kubernetes_admin_profile)

        >>> deployment_manager = gis.admin.deployments
        >>> object_store = deployment_manager.search(name="object-store")[0]
        >>> object_store

        <arcgis.gis.kubernetes._admin._deployment.Deployment object at <mem_addr>]

    """

    _url = None
    _gis = None

    def __init__(self, url: str, gis: GIS) -> None:
        self._url = url
        self._gis = gis
        self._con = gis._con

    @property
    def properties(self) -> Dict[str, Any]:
        """
        Returns the properties of the :class:`~arcgis.gis.kubernetes.Deployment`
        object.

        :return:
            A Python dictionary of the properties for the object.

        .. code-block:: python

            # Usage Example: Get a deployment object's properties

            >>> from arcgis.gis import GIS
            >>> kube_admin = GIS(profile="your_kubernetes_admin_profile").admin

            >>> deployment_mgr = kube_admin.deployments
            >>> object_store_deployment = deployment_mgr.search(name="object-store")[0]
            >>> ostore_deployment_props = object_store_deployment.properties

            >>> list(ostore_deployment_props.keys())

            ['mode',
            'configuredState',
            'provider',
            'deploymentId',
            'name',
            'type',
            'spec',
            'labels',
            'revision']

            >>> ostore_deployment_props["type"]
            'ObjectStore'

        """
        return self._con.get(self._url, {"f": "json"})

    def edit(self, props: Dict[str, Any]) -> bool:
        """
        This operation allows editing of the scaling (replicas) and resource
        allocation (resources) for a specific microservice within your
        deployment.

        See `Edit(Deployment) <https://developers.arcgis.com/rest/enterprise-administration/enterprise/edit-deployment/>`_
        documentation for full details on concepts and format criteria for the
        dictionary to use as the *props* argument.

        ==================     ============================================================
        **Parameter**           **Description**
        ------------------     ------------------------------------------------------------
        props                  Required dictionary. The microservice properties to modify.
        ==================     ============================================================

        :return:
            Boolean. *True* if successful, *False* if not.

        """
        url = f"{self._url}/edit"
        params = {"f": "json", "deploymentJson": props}
        res = self._con.post(url, params)
        return res.get("status", "failed") == "success"

    def refresh(self) -> bool:
        """
        The `refresh` operation can be used to troubleshoot microservices
        and pods that may be unresponsive or may be malfunctioning.
        Performing this operation will restart the corresponding pods and
        recreate the microservice.

        :return:
            Boolean. *True* if successful, *False* if it fails.
        """
        url = f"{self._url}/refresh"
        params = {"f": "json"}
        res = self._con.post(url, params)
        return res.get("status", "failed") == "success"

    def status(self) -> Dict[str, Any]:
        """
        Returns the the current state of the microstervice resource for the
        :class:`~arcgis.gis.kubernetes.Deployment` as well as information
        about the number of active and associated pods.

        :return:
           A Python dictionary with pertinent information on the current status
           of the *deployment*.

        .. code-block:: python

            # Usage Example: Getting current status of a deployment
            >>> from arcgis.gis import GIS
            >>> gis = GIS(profile="your_kubernetes_admin_profile")

            >>> deployment_mgr = gis.admin.deployments
            >>> ing_controller = deployment_mgr.search(name="ingress-controller")[0]
            >>> ing_controller.status()

                {
                    'name': 'ingress-controller',
                    'readyReplicas': 2,
                    'kubernetesResourceKind': 'Deployment',
                    'id': 'kx....4tmr',
                    'currentState': 'STARTED',
                    'kubernetesResourceName': 'arcgis-ingress-controller',
                    'totalReplicas': 2
                }
        """
        url = f"{self._url}/status"
        params = {"f": "json"}
        return self._con.get(url, params)


class DeploymentProperty:
    """
    This class provides access to default templates used for creating and
    managing both GIS-service and non-service related microservices.

    Objects of this class are not meant to initialized directly, but
    instead are returned by the
    :attr:`~arcgis.gis.kubernetes.DeploymentManager.deployment_properties`
    property of :class:`~arcgis.gis.kubernetes.DeploymentManager` objects.

    .. code-block:: python

        # Initializing a DeploymentProperty object
        >>> from arcgis.gis import GIS
        >>> kube_gis = GIS(profile="your_kubernetes_admin_profile")

        >>> deploy_mgr = kube_gis.admin.deployments
        >>> deploy_props = deploy_mgr.deployment_properties

        <arcgis.gis.kubernetes._admin._deployment.DeploymentProperty object <mem_addr>>

    See `Deployment Properties <https://developers.arcgis.com/rest/enterprise-administration/enterprise/deployment-default-properties/>`_
    for full details and explanations.
    """

    _url = None
    _gis = None

    def __init__(self, url: str, gis: GIS) -> None:
        self._url = url
        self._gis = gis
        self._con = gis._con

    @property
    def properties(self) -> Dict[str, Any]:
        """
        Returns a list of default templates and their properties for both
        GIS-service and non-service related microservices. The properties for
        each template can be updated using the
        :meth:`~arcgis.gis.kubernetes.DeploymentProperty.edit` method.

        .. note:
            Starting with ArcGIS Enterprise 11.2 on Kubernetes, the default
            property template supports having node affinity and tolerations
            that are applied to the pods of newly created GIS service
            deployments.

        .. code-block:: python

            # Usage Example: Getting a list of template ID values

            >>> from arcgis.gis import GIS
            >>> gis = GIS(profile="your_kubernetes_admin_profile")

            >>> depl_prop_mgr = gis.admin.deployments.deployment_properties

            # Returns a list of dictionaries representing each template.
            # Each dictionary has the same keys:
            >>>list(dmgr_dep_props.properties[0].keys())

            ['mode', 'provider', 'id', 'type', 'spec', 'revision']

            # Use Python comprehensions to retrieve specific information
            # about each template. For example, get each 'id':

            >>> [template["id"] for template in depl_prop_mgr.properties]

                ['pzrzabematwd9j8mlcx3p',
                'pqi21u4qrx3zwjl2jgxym',
                 ...
                'px4zjramkvgf084ve2m10']
        """
        params = {"f": "json"}
        return self._con.get(self._url, params)["properties"]

    def get(self, template_id: str) -> Dict[str, Any]:
        """
        Gets the default template for microservices matching *template_id*
        argument.

        ==================     ====================================================================
        **Parameter**           **Description**
        ------------------     --------------------------------------------------------------------
        template_id            Required String.  The unique ID of the microservice template.
        ==================     ====================================================================

        :return:
           Returns a Python dictionary with detailed information on the specific template.

        .. code-block:: python

            # Usage Example: Getting a specific microservice template

            >>> from arcgis.gis import GIS
            >>> gis = GIS(profile="your_kubernetes_admin_profile")

            >>> dep_mgr = gis.admin.deployments
            >>> template_property_mgr = dep_mgr.deployment_properties

            >>> msvc_template = template_property_mgr.get("pqi21u4qrx3zwjl2jgxym")

            {'mode': 'Dedicated',
            'provider': 'ArcObjects11',
            'id': 'pqi21u4qrx3zwjl2jgxym',
            'type': 'GeometryServer',
            'spec': {'replicas': {'min': 1, 'max': 1, 'scalingMode': 'manual'},
                     'containers': [{'name': 'main-container',
                                     'resources': {'memoryMin': '500Mi',
                                                   'memoryMax': '2Gi',
                                                   'cpuMin': '0.125',
                                                   'customResources': {},
                                                   'cpuMax': '1'},
                                     'containerImageKey': 'GEOMETRY_SERVER'},
                                    {'name': 'fluent-bit',
                                     'resources': {'memoryMin': '32Mi',
                                                   'memoryMax': '150Mi',
                                                   'cpuMin': '0.05',
                                                   'customResources': {},
                                                   'cpuMax': '0.25'},
                                     'containerImageKey': 'FLUENT_BIT'}]},
            'revision': 1752137406148}

        """
        url = f"{self._url}/{template_id}"
        params = {"f": "json"}
        return self._con.get(url, params)

    def edit(self, template_id: str, props: Dict[str, Any]) -> bool:
        """
        This operation modifies the default scaling and resource allocation
        properties of a specific microservice within your organization.
        Subsequent microservices matching the *type*, *provider*, and *mode* of
        the default template will have these updated properties.

        .. note::
            Any previously existing :class:`deployments <arcgis.gis.kubernetes.Deployment>`
            must be edited individually to match these new properties.

        See `Edit (Default Properties) <https://developers.arcgis.com/rest/enterprise-administration/enterprise/edit-deployment-properties/>`_
        for full details on concepts and formatting of *props* dictionary.

        ==================     ====================================================================
        **Parameter**           **Description**
        ------------------     --------------------------------------------------------------------
        template_id            Required String.  The unique ID of the property template.
        ------------------     --------------------------------------------------------------------
        props                  Required dictionary. A dictionary representing the properties any
                               any new microservice of the *type* represented by the ID will have.
                               Refer to `Edit (Default Properties) <https://developers.arcgis.com/rest/enterprise-administration/enterprise/edit-deployment-properties/>`_
                               for details and formatting.
        ==================     ====================================================================

        :return:
            Returns a Boolean object, *True* if successful, *False* if not.

        """
        url = f"{self._url}/{template_id}/edit"
        params = {
            "f": "json",
            "propertyJson": props,
        }
        return self._con.post(url, params).get("status", "failed") == "success"


class DeploymentManager(_BaseKube):
    """
    This class provides properties and methods to search, retrieve and edit
    the microservices used to host or run your organization's services, as well
    as information on those not directly related to services such as the
    *ArcGIS Enterprise Admin API*, *Portal Sharing*, and *ingress controller*
    microservices. Each microservice may correspond to one or more pods in the
    ArcGIS Enterprise on Kubernetes deployment.

    Objects of this class are not meant to be initialized directly, but instead
    accessed through the :attr:`~arcgis.gis.kubernetes.KubernetesAdmin.deployments`
    property on a :class:`~arcgis.gis.kubernetes.KubernetesAdmin` object.

    .. code-block:: python

        # Initializing a DeploymentManager object
        >>> from arcgis.gis import GIS
        >>> kube_gis = GIS(profile="your_kubernetes_admin_profile")
        >>> kube_admin = kube_gis.admin

        >>> deploy_mgr = kube_admin.deployments

        <DeploymentManager at <organization_url>/<web adaptor>/admin/system/deployments>

    """

    _dp = None
    _con = None
    _gis = None
    _url = None

    def __init__(
        self,
        url: str,
        gis: GIS = None,
    ) -> None:
        """class initializer"""
        super()
        self._url = url
        self._gis = gis
        self._con = gis._con

    # ---------------------------------------------------------------------
    def search(
        self,
        name: Optional[str] = None,
        filter_type: Optional[str] = None,
        filter_id: Optional[str] = None,
        provider: Optional[str] = None,
        mode: Optional[str] = None,
    ) -> List[Deployment]:
        """
        The search operation queries and returns a list of
        :class:`~arcgis.gis.kubernetes.Deployment`
        objects representing the microservies within a Kubernetes deployment.
        Search criteria can be fine-tuned by specifying the *name*, *type*, *ID*,
        *provider*, and/or *mode* of the service you're searching for. These
        filters are optional, if no filter is applied all microservices are returned
        by the operation.

        ==================     ====================================================================
        **Parameter**           **Description**
        ------------------     --------------------------------------------------------------------
        name                   Optional String. The name of the microservice.
        ------------------     --------------------------------------------------------------------
        filter_type            Optional String. The microservice type.

                               Allowed Values:

                               * `FeatureServer`
                               * `GeometryServer`
                               * `GPServer`
                               * `GPSyncServer`
                               * `MapServer`
                               * `TileServer`
                               * `ImageServer`
                               * `System`
                               * `InMemoryStore`
                               * `ObjectStore`
                               * `SpatiotemporalIndexStore`
                               * `QueueServer`
                               * `RelationalStore`
                               * `WebhookProcessor`
        ------------------     --------------------------------------------------------------------
        filter_id              Optional String. The microservice ID.
        ------------------     --------------------------------------------------------------------
        provider               Optional String. The microservice provider. Only microservices
                               related to an ArcGIS service type will have a provider type. A
                               provider type of Undefined is used for non-service related
                               microservices (Admin API, Portal Sharing, ingress controller,
                               etc.).

                               Values:

                               * `SDS`
                               * `ArcObjects11`
                               * `DMaps`
                               * `Undefined`
                               * `Postgres`,
                               * `Tiles`
                               * `Ignite`
                               * `MinIO`
                               * `Elasticsearch`
                               * `RabbitMQ`
        ------------------     --------------------------------------------------------------------
        mode                   Optional String. The microservice mode. A mode type of Undefined is
                               used when the microservices is system related (for example, the
                               Admin API, Portal Sharing, ingress controller, etc.). Only
                               microservices related to an ArcGIS service type use either the
                               *Dedicated* or *Shared* mode.

                               Values:

                               * `Shared`
                               * `Dedicated`
                               * `Undefined`
                               * `Primary`
                               * `Standby`
                               * `Coordinator`
        ==================     ====================================================================

        :return:
            A list of :class:`~arcgis.gis.kubernetes.Deployment` objects

        .. code-block:: python

            # Usage Example #1: Searching all Deployments

            >>> from arcgis.gis import GIS
            >>> kube_admin = GIS(profile="your_kubernetes_admin_profile").admin

            >>> deployment_mgr = kube_admin.deployments
            >>> all_deployments = deployment_mgr.search()
            >>> all_deployments
            [
               <arcgis.gis.kubernetes._admin._deployment.Deployment object at 0x166c0ee40>,
               <arcgis.gis.kubernetes._admin._deployment.Deployment object at 0x166b19810>,
               ...
               <arcgis.gis.kubernetes._admin._deployment.Deployment object at 0x166cba050>
            ]

            # Usage Example #2: Searching for a Deployment by name

            >>> deployment_mgr = kube_admin.deployments
            >>> object_store = deployment_mgr.search(name="object-store")[0]
            >>> list(object_store.properties.keys())

            ['mode',
            'configuredState',
            'provider',
            'deploymentId',
            'name',
            'type',
            'spec',
            'labels',
            'revision']

            >>> object_store["configuredState"]
            'STARTED'

            # Usage Example #3: Searching by deployment type

            >>> deployment_mgr = kube_admin.deployments
            >>> gp_server_deployments = deployment_mgr.search(filter_type="GPServer")
            >>> len(gp_server_deployements)
            14
            >>> gp_server_deployments[0].properties["provider]
            'ArcObjects11'

        """
        url = f"{self._url}/findDeploymentIds"
        params = {
            "filterName": name,
            "filterType": filter_type,
            "filterId": filter_id,
            "filterProvider": provider,
            "filterMode": mode,
            "f": "json",
        }
        params = {k: v for k, v in params.items() if v}
        deployment_ids = self._gis._con.post(url, params).get(
            "filteredDeploymentIds", []
        )
        return [self.get(deploy_id) for deploy_id in deployment_ids]

    def get(self, deployment_id: str) -> Deployment:
        """
        Returns a :class:`~arcgis.gis.kubernetes.Deployment` object based on its
        specific *id* value.

        ==================     =================================================
        **Parameter**           **Description**
        ------------------     -------------------------------------------------
        deployment_id          Required string. The *id* value for the specific
                               *deployment*
        ==================     =================================================

        :return:
            :class:`~arcgis.gis.kubernetes.Deployment` object

        .. code-block:: python

            # Usage Example: Getting a specific Deployment

            >>> from arcgis.gis import GIS
            >>> kube_admin = GIS(profile="your_kubernetes_admin_profile").admin

            >>> deployment_mgr = kube_admin.deployments
            >>> deployment_ids =[
                       dply.properties["depoloymentId"]
                       for dply in deployment_mgr.search()
                ]

                ['ked412f10oqiy1wmqu4xl',
                'kb4tehbrw1dvb34mpp12g',
                ...
                'kzz1a1ay9d7vs4kga2akl',
                'kilgjk429p8rocnj4xam1']


            >>> deployment_obj = deployment_mgr.get('ked412f10oqiy1wmqu4xl')
            >>> deployment_obj.properties

            {'mode': 'Undefined',
            'configuredState': 'STARTED',
            'provider': 'Undefined',
            'deploymentId': 'ked412f10oqiy1wmqu4xl',
            'name': 'enterprise-apps',
            'type': 'System',
            'spec': {
             },
             ...
            'revision': 1754472392103}

        """
        url = f"{self._url}/{deployment_id}"
        return Deployment(url=url, gis=self._gis)

    @property
    def deployment_properties(self) -> DeploymentProperty:
        """
        Provides administrators with an object to retrieve and manage the
        organization's service templates and their properties.

        :return:
            Returns a :class:`~arcgis.gis.kubernetes.DeploymentProperty` object.

        .. code-block:: python

            # Usage Example: Getting a DeploymentProperty object

            >>> from arcgis.gis import GIS
            >>> gis = GIS(profile="your_kubernetes_admin_profile")

            >>> deployment_mgr = gis.admin.deployments
            >>> deployment_property_mgr = deployment_mgr.deployment_properties
            >>> deployment_property_mgr

            <arcgis.gis.kubernetes._admin._deployment.DeploymentProperty object at <mem_addr>>

        """
        if self._dp is None:
            url = f"{self._url}/properties"
            self._dp = DeploymentProperty(url=url, gis=self._gis)
        return self._dp
