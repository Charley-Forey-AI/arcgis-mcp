from __future__ import annotations
import os
import time
from arcgis.auth import EsriSession
from typing import Iterable
from requests import Response


__all__ = ["HealthCheckManager", "Report", "ReportManager", "Suite", "SuitesManager"]


###########################################################################
class ReportJob:
    def __init__(self, url: str, session: EsriSession) -> None:
        self.url: str = url
        self.session: EsriSession = session

    # ---------------------------------------------------------------------
    def __str__(self) -> str:
        return f"< ReportJob @ {self.url} >"

    # ---------------------------------------------------------------------
    def __repr__(self) -> str:
        return self.__str__()

    # ---------------------------------------------------------------------
    @property
    def properties(self) -> dict:
        resp: Response = self.session.get(
            url=self.url,
            params={
                "f": "json",
            },
        )
        resp.raise_for_status()
        return resp.json()

    # ---------------------------------------------------------------------
    @property
    def status(self) -> str:
        """returns the job status"""
        return self.properties.get("status")

    # ---------------------------------------------------------------------
    def result(self):
        pause: int = 1
        while not self.properties.get("status") in ["COMPLETED", "FAILED", "CANCELLED"]:
            if pause > 5:
                pause = 5
            else:
                pause += 1
            time.sleep(pause)
        return self.properties


###########################################################################
class Report:
    _properties: dict | None = None

    def __init__(self, url: str, session: EsriSession) -> None:
        self.url: str = url
        self.session: EsriSession = session

    def __str__(self) -> str:
        return f"< Report >"

    def __repr__(self) -> str:
        return self.__str__()

    @property
    def properties(self) -> dict:
        """returns the report properties"""
        resp: Response = self.session.get(
            self.url,
            params={
                "f": "json",
            },
        )
        resp.raise_for_status()
        return resp.json()

    def delete(self) -> bool:
        """Deletes the Report"""
        url: str = f"{self.url}/delete"
        params = {
            "f": "json",
        }
        resp: Response = self.session.post(url=url, data=params)
        resp.raise_for_status()
        return True


###########################################################################
class ReportManager:
    def __init__(self, url: str, session: EsriSession) -> None:
        self.url: str = url
        self.session: EsriSession = session

    # ---------------------------------------------------------------------
    def __str__(self) -> str:
        return f"< ReportManager >"

    # ---------------------------------------------------------------------
    def __repr__(self) -> str:
        return self.__str__()

    # ---------------------------------------------------------------------
    def query(self, result_type: str | None = None) -> Iterable[Report]:
        """
        The query operation returns all current health check reports for
        the organization, returning name of the operation, the start and
        end time of the health check in UTC format, and whether the
        organization passed or failed the check, as well as the IDs
        associated with each report that are needed when using the Reports
        level Export and Delete operations.
        """
        url: str = f"{self.url}/query"
        if result_type is None:
            result_type = "passed,failed,warning,unknown"
        num: int = 2  # 1000
        params: dict = {
            "f": "json",
            "start": 1,
            "num": num,
            "runResult": result_type,
        }
        resp: Response = self.session.get(url=url, params=params)
        resp.raise_for_status()
        data: dict = resp.json()
        while data.get("nextStart", -1) > 0:
            for r in data.get("runs", []):
                report_url: str = f"{self.url}/{r.get('id')}"
                yield Report(url=report_url, session=self.session)
            params["start"] = data["nextStart"]
            resp: Response = self.session.get(url=url, params=params)
            resp.raise_for_status()
            data: dict = resp.json()

    # ---------------------------------------------------------------------
    def run(self, suite: Suite | list[Suite], name: str) -> ReportJob | dict:
        """ """
        if isinstance(suite, Suite):
            suite = [suite]
        ids = [s.properties.get("id") for s in suite]
        params: dict = {
            "f": "json",
            "async": "true",
            "runName": name,
            "suiteIds": ",".join(ids),
        }
        url: str = f"{os.path.dirname(self.url)}/run"
        resp: Response = self.session.post(url=url, data=params)
        data: dict = resp.json()
        if "jobsUrl" in data:
            return ReportJob(data.get("jobsUrl"), session=self.session)
        return data


###########################################################################
class Suite:
    """
    The `Suite` resource returns the name of, and short description for,
    the tests included in a specific suite. A suite is a grouping of
    predefined tests that are performed against the organization during a
    health check. Administrators cannot change the tests or choose which
    tests are performed from a specific suite. At ArcGIS Enterprise 11.2 on
    Kubernetes, administrators have access to a basic health check suite.
    Basic health check reports conduct a suite of functional and
    availability health checks to validate overall organization health.
    """

    _properties: dict | None = None

    def __init__(self, url: str, session: EsriSession) -> None:
        self.url: str = url
        self.session: EsriSession = session

    @property
    def properties(self) -> dict:
        if self._properties is None:
            resp: Response = self.session.get(url=self.url, params={"f": "json"})
            resp.raise_for_status()
            self._properties = resp.json()
        return self._properties

    def __str__(self) -> str:
        return f'< Suite {self.properties.get("name")} - {self.properties.get("id")} >'

    def __repr__(self) -> str:
        return self.__str__()

    @property
    def suite_id(self) -> str:
        """returns the Suite Id"""
        return self.properties.get("id")

    @property
    def name(self) -> str:
        """returns the Suite Id"""
        return self.properties.get("name")


###########################################################################
class SuitesManager:
    """
    The `SuitesManager` class returns a list of all suites that administrators
    can use to generate health check reports for their organization. Each
    suite is a grouping of predefined tests that are performed against the
    organization during a health check. Administrators cannot change the
    tests or choose which tests are performed from a specific suite. At
    ArcGIS Enterprise 11.2 on Kubernetes, administrators have access to a
    basic health check suite.
    """

    def __init__(self, url: str, session: EsriSession) -> None:
        self.url: str = url
        self.session: EsriSession = session

    def __str__(self) -> str:
        return f"< SuitesManager >"

    def __repr__(self) -> str:
        return self.__str__()

    def list(self) -> Iterable[Suite]:
        """provides a list of registered test suites"""
        url: str = self.url
        resp: Response = self.session.get(
            url=url,
            params={
                "f": "json",
            },
        )
        resp.raise_for_status()
        suites: list = resp.json().get("userSuites", [])
        for suite in suites:
            suite_url: str = f"{self.url}/{suite.get('id')}"
            yield Suite(url=suite_url, session=self.session)

    def get(self, name: str) -> Suite | None:
        """obtains the Suite by it's name"""
        for suite in self.list():
            if (
                suite.name.lower() == name.lower()
                or suite.suite_id.lower() == name.lower()
            ):
                return suite


###########################################################################
class HealthCheckManager:
    """
    This manager helps administrators monitor the health of their organization.
    """

    _suites: SuitesManager | None = None
    _reports: ReportManager | None = None

    def __init__(self, url: str, session: EsriSession) -> None:
        self.url: str = url
        self.session: EsriSession = session

    def __str__(self) -> str:
        return f"< HealthCheckManager >"

    def __repr__(self) -> str:
        return self.__str__()

    @property
    def suites(self) -> SuitesManager:
        """
        Returns a manager to work with the existing set of suites on a Kubernetes organization
        """
        if self._suites is None:
            url: str = f"{self.url}/suites"
            self._suites = SuitesManager(url=url, session=self.session)
        return self._suites

    @property
    def reports(self) -> ReportManager:
        """Provides access to past reports on the kubernetes organization"""

        if self._reports is None:
            url: str = f"{self.url}/reports"
            self._reports = ReportManager(url=url, session=self.session)
        return self._reports
