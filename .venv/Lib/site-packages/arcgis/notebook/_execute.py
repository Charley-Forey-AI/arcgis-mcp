from __future__ import annotations
import concurrent.futures
from arcgis.gis import GIS, Item
from arcgis.gis.nb import NotebookManager
from arcgis.gis.agonb import AGOLNotebookManager


def _list_instances(gis: GIS = None) -> list[dict[str:str]]:
    """
    Returns a list of avaialable Machine Instances.

    :returns: list[dict[str:str]]
    """

    if gis is None:
        from arcgis import env

        gis = env.active_gis
        assert gis
    mgrs = gis.notebook_server
    if len(mgrs) > 0:
        if gis._portal.is_arcgisonline:
            mgr = mgrs[0]
            ip = mgr.instance_preferences
            return ip.instances.get("instanceTypePreferences", [])
    return []


def list_runtimes(gis: GIS = None) -> list:
    """
    Returns a list of avaialable runtimes.

    :returns: list[dict[str:str]]
    """

    if gis is None:
        from arcgis import env

        gis = env.active_gis
        assert gis
    mgrs = gis.notebook_server
    if len(mgrs) > 0:
        if gis._portal.is_arcgisonline:
            mgr = mgrs[0]
            rt = mgr.runtimes
            return rt.list()
        else:
            mgr = mgrs[0]
            nb = mgr.notebooks
            rt = nb.runtimes
            return [r.properties for r in rt]
    return []


def execute_notebook(
    item: Item,
    *,
    timeout: int = 50,
    update_portal_item: bool = True,
    parameters: list = None,
    save_parameters: bool = False,
    server_index: int = 0,
    gis: GIS = None,
    future: bool = False,
) -> dict | concurrent.futures.Future:
    """

    The Execute Notebook operation allows administrators and users with
    the `Create and Edit Notebooks` privilege to remotely run a notebook
    that they own.  The notebook specified in the operation will be run
    with all cells in order.

    Using this operation, you can schedule the execution of a notebook,
    either once or with a regular occurrence. This allows you to
    automate repeating tasks such as data collection and cleaning,
    content updates, and portal administration. On Linux machines, use
    a cron job to schedule the executeNotebook operation; on Windows
    machines, you can use the Task Scheduler app.

    .. note::
        To run this operation in ArcGIS Enterprise, you must log in with
        an Enterprise account. You cannot execute notebooks using the
        ArcGIS Notebook Server primary site administrator account.

    .. note::
        ArcGIS Online has additional parameters, as noted in the parameter
        table below.

    You can specify parameters to be used in the notebook at execution
    time. If you've specified one or more parameters, they'll be
    inserted into the notebook as a new cell. This cell will be placed
    at the beginning of the notebook, unless you have added the tag
    parameters to a cell.

    ====================    ====================================================================
    **Parameter**            **Description**
    --------------------    --------------------------------------------------------------------
    item                    Required :class:`~arcgis.gis.Item`. Opens an existing portal item.
    --------------------    --------------------------------------------------------------------
    update_portal_item      Optional Boolean. Specifies whether you want to update the
                            notebook's portal item after execution. The default is true. You may
                            want to specify true when the notebook you're executing contains
                            information that needs to be updated, such as a workflow that
                            collects the most recent version of a dataset. It may not be
                            important to update the portal item if the notebook won't store any
                            new information after executing, such as an administrative notebook
                            that emails reminders to inactive users.
    --------------------    --------------------------------------------------------------------
    parameters              Optional Dictionary. Defines the parameters to add to the
                            notebook for this execution. The parameters will be inserted as a
                            new cell directly after the cell you have tagged *parameters*.
                            Separate parameters with a comma. Use format of:

                            * "x":1 when defining number parameters
                            * "y":"text" when defining string parameters

                            See `Prepare the Notebook <https://enterprise.arcgis.com/en/notebook/latest/use/windows/prepare-a-notebook-for-automated-execution.htm#GUID-74ECC731-D8D3-4E63-A22C-38027407A209>`_
                            for detailed explanation.
    --------------------    --------------------------------------------------------------------
    save_parameters         Optional Boolean.  Specifies whether the notebook parameters cell
                            should be saved in the notebook for future use. The default is
                            *False*.
    --------------------    --------------------------------------------------------------------
    timeout                 Optional Int. The number of minutes to run the instance before timeout.

                            .. note::
                                This is only available in ArcGIS Online.
    --------------------    --------------------------------------------------------------------
    future                  Optional boolean.

                            * If *True*, a Job object will be returned and the process runs
                              asynchronously, allowing for other work to be done while
                              processing completes.T
                            * If *False*, which is the default, the process waits for results
                              before continuing.
    ====================    ====================================================================

    :return:
        * If *future=False*, a Python dictionary
        * If *future = True*, then the result is a
          `concurrent.futures.Future <https://docs.python.org/3/library/concurrent.futures.html>`_
          object. Call *result()* on the object to get the response

    .. code-block:: python

        #Usage example: Inserting parameters at execution time
        >>> from arcgis.gis import GIS
        >>> from arcgis.notebook import execute_notebook

        >>> gis = GIS(
                      profile="your_online_admin_profile",
                      verify_cert=False
                  )

        >>> nb_item = gis.content.search(
                       query="air_quality_regular_updates",
                       item_type="Notebook"
                      )[0]

        # In the notebook cell tagged as parameters, the variables defined
        # with the below key values will be replaced by the value
        >>> execute_notebook(
                        item=nb_item,
                        parameters={
                            "file_path": r"/arcgis/home/aqi_data/",
                            "num": 2,
                        },
                        save_parameters=True
            )

    """
    if gis is None:
        from arcgis import env

        gis = env.active_gis or item._gis
        assert gis

    mgrs = gis.notebook_server
    if len(mgrs) > 0:
        if gis._is_arcgisonline:
            instance_type = None
            mgr = gis.notebook_server[0]
            assert isinstance(mgr, AGOLNotebookManager)
            return mgr.notebooksmanager.execute_notebook(
                item=item,
                update_portal_item=update_portal_item,
                parameters=parameters,
                save_parameters=save_parameters,
                instance_type=instance_type,
                timeout=timeout,
                future=future,
            )
        else:
            mgr = gis.notebook_server[server_index].notebooks
            assert hasattr(mgr, "execute_notebook")
            return mgr.execute_notebook(
                item=item,
                update_portal_item=update_portal_item,
                parameters=parameters,
                save_parameters=save_parameters,
                future=future,
            )

    return
