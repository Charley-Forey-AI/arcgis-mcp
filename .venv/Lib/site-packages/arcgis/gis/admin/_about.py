from __future__ import annotations
import os
import json
import tempfile
from arcgis.auth import EsriSession
from requests import Response

__all__ = ["AboutManager"]


###########################################################################
class AboutManager:
    """
    The about resource compiles information, such as hardware details
    (CPU, RAM, disk usage, etc.) and licenses, for each component that
    makes up an ArcGIS Enterprise deployment, including all servers
    federated with the deployment.
    """

    _properties: dict | None = None
    url: str
    session: EsriSession

    def __init__(self, url: str, session: EsriSession) -> None:
        self.url = url
        self.session = session

    @property
    def properties(self) -> dict:
        """gets the about properties"""
        if self._properties is None:
            resp: Response = self.session.get(
                url=self.url,
                params={
                    "f": "json",
                },
            )
            resp.raise_for_status()
            self._properties = resp.json()
        return self._properties

    def report(self, redact: bool = False, save_file: str | None = None) -> str:
        """
        The `report` method generates and downloads a .json file that contains
        the hardware and licensing information for each component of an
        Enterprise component. The information included in the report is the
        same information returned by the About resource compiled into a
        single file.

        ===============     ====================================================
        **Parameter**        **Description**
        ---------------     ----------------------------------------------------
        redact              Optional Boolean. When set to true, all hostnames
                            included as part of URLs, or as values for the
                            machineName properties, included in the report are
                            replaced with placeholder values. If set to false,
                            or not included in the request, hostnames will be
                            appear as-is in the report.
        ---------------     ----------------------------------------------------
        save_file           Optional string. The save path of the .json file.
        ===============     ====================================================

        :returns: Path to the save file.

        """
        params = {
            "redact": json.dumps(redact),
        }
        url: str = f"{self.url}/report"
        resp: Response = self.session.post(url=url, data=params)
        resp.raise_for_status()
        if save_file is None:
            save_file = os.path.join(tempfile.gettempdir(), "about.json")
        with open(save_file, "wb") as writer:
            writer.write(resp.content)
        return save_file
