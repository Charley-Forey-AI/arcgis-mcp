"""
This is the ArcGIS Notebook Server API Framework
"""

from arcgis.gis import GIS
from arcgis.auth import EsriSession
from ._dataaccess import KubeNotebookDataAccess
from ._nbm import KubeNotebookManager

__all__ = ["KubernetesNotebook"]


########################################################################
class KubernetesNotebook:
    """
    Provides access to the notebook functionality on a kubernetes site
    """

    _da = None
    _gis = None
    _url = None
    _properties = None
    _logs = None
    _system = None
    _machine = None
    _notebook = None
    _security = None
    _services = None
    _version = None
    _sitemanager = None
    _session: EsriSession | None = None

    # ----------------------------------------------------------------------
    def __init__(self, url, gis):
        """Constructor"""
        if isinstance(gis, GIS):
            self._gis = gis
        else:
            raise ValueError("Invalid GIS object")
        if self._gis._is_kubernetes == False:
            raise ValueError("The GIS provided is not for a Kubernetes deployment.")
        if url.lower().endswith("/notebooks") == False:
            url += "/notebooks"
        self._url = url

    # ----------------------------------------------------------------------
    @property
    def url(self):
        """The URL of the notebook server."""
        return self._url

    # ----------------------------------------------------------------------
    @property
    def session(self) -> EsriSession:
        if self._session is None:
            self._session = self._gis.session
        return self._session

    # ----------------------------------------------------------------------
    def _init(self) -> dict:
        """loads the properties"""
        try:
            params = {"f": "json"}
            res = self._gis._con.get(self._url, params)
            self._properties = res
        except Exception as ex:
            raise Exception(str(ex))

    # ----------------------------------------------------------------------
    def __str__(self):
        return "< Kubernetes Notebook @ {url} >".format(url=self._url)

    # ----------------------------------------------------------------------
    def __repr__(self):
        return "< Kubernetes Notebook @ {url} >".format(url=self._url)

    # ----------------------------------------------------------------------
    @property
    def properties(self) -> dict:
        """Properties of the object"""
        if self._properties is None:
            self._init()
        return self._properties

    # ----------------------------------------------------------------------
    @property
    def version(self) -> list | None:
        """
        Returns the notebook server version

        :return: List
        """
        if self._version is None and self._gis:
            self._version = self._gis.version
        return self._version

    # ----------------------------------------------------------------------
    @property
    def data_access(self) -> KubeNotebookDataAccess:
        """Provides access to managing files stored on notebook server.

        :return:
            :class:`~arcgis.gis.nb._dataaccess.NotebookDataAccess` object

        """
        if self._da is None:
            try:
                url: str = (
                    self._gis.users.me.generate_direct_access_url("notebook")
                    .get("url")
                    .replace("/notebookworkspace", "")
                )
            except:
                url = self._url + "/dataaccess"
            self._da = KubeNotebookDataAccess(url, self._gis)
        return self._da

    # ----------------------------------------------------------------------
    @property
    def notebooks(self) -> KubeNotebookManager:
        """
        Provides access to managing the ArcGIS Notebook Server's
        Notebooks

        :return: :class:`~arcgis.gis.nb.NotebookManager`
        """
        if self._notebook is None:
            url = self._url
            self._notebook = KubeNotebookManager(url=url, gis=self._gis, nbs=self)
        return self._notebook
