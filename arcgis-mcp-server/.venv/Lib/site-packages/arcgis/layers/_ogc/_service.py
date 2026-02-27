from __future__ import annotations
import os, copy
from typing import Dict, Any, Iterator
import pandas as pd
import requests
from arcgis.gis import GIS
from arcgis.geometry import Geometry
from arcgis import env as _env
from functools import lru_cache


###########################################################################
class OGCCollection:
    """
    Represents a single OGC dataset

    ================  ===============================================================================
    **Parameter**      **Description**
    ----------------  -------------------------------------------------------------------------------
    url               Required String. The web address endpoint.
    ----------------  -------------------------------------------------------------------------------
    gis               Optional :class:`~arcgis.gis.GIS`. The connection object.
    ================  ===============================================================================

    """

    _gis = None
    _url = None
    _properties = None

    # ---------------------------------------------------------------------
    def __init__(self, url: str, gis: GIS = None) -> None:
        """Constructor"""

        if gis is None:
            gis = _env.active_gis or GIS()
        self._gis = gis
        self._url = url
        self._session = gis.session

        self._is_esri_ogc = (
            str(url).lower().find("ogcfeatureserver") > -1
            and os.path.basename(url).isdigit()
        )

    # ---------------------------------------------------------------------
    @property
    def properties(self) -> dict:
        """returns the service properties"""
        params = {"f": "json"}
        try:
            resp: requests.Response = self._session.get(url=self._url)
            resp.raise_for_status()
            return resp.json()
        except:
            resp: requests.Response = self._session.post(url=self._url, data=params)
            resp.raise_for_status()
            return resp.json()

    # ---------------------------------------------------------------------
    def _process_row(self, feature: dict[str, Any]) -> Dict[str, Any]:
        """Converts the GeoJSON Geometry to Esri JSON and format the row accordingly"""
        row = {
            "SHAPE": (Geometry(feature["geometry"]) if feature["geometry"] else None)
        }
        row.update(feature["properties"])
        return row

    # ---------------------------------------------------------------------
    def query(
        self,
        query: str | None = None,
        limit: int = 10000,
        bbox: list[float] | None = None,
        bbox_sr: int | None = None,
        time_filter: str | None = None,
        return_all: bool = False,
        **kwargs,
    ) -> dict[str, Any] | pd.DataFrame:
        """
        Queries the :class:`~arcgis.layers.ogc.OGCFeatureService` Layer and returns back the information as a Spatially Enabled DataFrame.

        ================  ===============================================================================
        **Parameter**      **Description**
        ----------------  -------------------------------------------------------------------------------
        query             Optional String. A SQL based query applied to the service.
        ----------------  -------------------------------------------------------------------------------
        limit             Optional Integer. The number of records to limit to.  The default is 10,000.
        ----------------  -------------------------------------------------------------------------------
        bbox              Optional List[float]. The bounding box to limit search in.
        ----------------  -------------------------------------------------------------------------------
        bbox_sr           Optional Integer. The coordinate reference system as a WKID.
        ----------------  -------------------------------------------------------------------------------
        time_filter       Optional String. The dates to filter time by.
        ================  ===============================================================================

        :return: Union[Dict[str, Any], pd.DataFrame]
        """
        url = f"{self._url}/items"
        params = {"f": "json"}
        if query:
            params["filter"] = query
        if isinstance(limit, int) and limit >= 0:
            params["limit"] = limit
        if bbox and bbox_sr:
            params["bbox"] = bbox
            params["bbox-crs"] = bbox_sr
        if kwargs.get("crs", None):
            params["crs"] = kwargs.pop("crs")
        params["offset"] = kwargs.pop("offset", 0)
        if time_filter:
            params["datetime"] = time_filter
        for k, v in kwargs.items():
            if not self._is_esri_ogc and k == "as_dict":
                continue
            params[k] = v
        as_dict = kwargs.pop("as_dict", False)
        if as_dict == False:  # returns all records as sedf
            resp: requests.Response = self._session.get(url=url, params=params)
            resp.raise_for_status()
            res = resp.json()
            if len(res["features"]) == 0:
                return pd.DataFrame([])
            results = [
                self._process_row(feature) for feature in res["features"] if feature
            ]
            params["offset"] += limit
            while True:
                try:
                    resp: requests.Response = self._session.get(url=url, params=params)
                    resp.raise_for_status()
                    res = resp.json() or resp.text
                except:
                    # with new session get an error is returned rather than an empty response for some services
                    if resp.text == "":
                        # If text empty then json is empty
                        break
                    raise Exception(resp.text)
                if res["numberReturned"] == 0:
                    break
                elif return_all == False and len(results) >= limit:
                    results = results[:limit]
                    break
                elif res["numberReturned"] < limit:
                    r1 = [
                        self._process_row(feature)
                        for feature in res["features"]
                        if feature
                    ]
                    results.extend(r1)
                    break
                else:
                    r1 = [
                        self._process_row(feature)
                        for feature in res["features"]
                        if feature
                    ]
                    results.extend(r1)
                params["offset"] += limit
            df = pd.DataFrame(results)
            return df
        else:
            resp: requests.Response = self._session.get(url=url, params=params)
            resp.raise_for_status()
            results = resp.json()
            if len(results["features"]) == 0:
                return results
            res = copy.deepcopy(results)
            params["offset"] += limit
            while res["numberReturned"] > 0:
                try:
                    resp: requests.Response = self._session.get(url=url, params=params)
                    resp.raise_for_status()
                    res = resp.json()
                except:
                    # with new session get an error is returned rather than an empty response for some services
                    if resp.text == "":
                        # If text empty then json is empty
                        break
                    raise Exception(resp.text)
                if res["numberReturned"] == 0:
                    break

                results["features"].extend(res["features"])
                if return_all == False and len(results["features"]) >= limit:
                    results["features"] = results["features"][:limit]
                    break
                elif res["numberReturned"] < limit:
                    break
                params["offset"] += limit
            return results

    # ---------------------------------------------------------------------
    def get(self, feature_id: int) -> Dict[str, Any]:
        """
        Gets an individual feature on the service. Needs to correspond
        to an id of the feature.

        :return: Dict[str, Any]
        """
        assert isinstance(feature_id, int)
        url = f"{self._url}/items/{feature_id}"
        params = {"f": "json"}
        resp: requests.Response = self._session.get(url=url, params=params)
        resp.raise_for_status()
        return resp.json()


###########################################################################
class OGCFeatureService:
    """
    Represents the Hosted OGC Feature Server

    ================  ===============================================================================
    **Parameter**      **Description**
    ----------------  -------------------------------------------------------------------------------
    url               Required String. The web address endpoint.
    ----------------  -------------------------------------------------------------------------------
    gis               Optional :class:`~arcgis.gis.GIS`. The connection object.
    ================  ===============================================================================

    """

    _gis = None
    _url = None
    _properties = None

    # ---------------------------------------------------------------------
    def __init__(self, url: str, gis: GIS = None) -> "OGCFeatureService":
        """Constructor"""
        if gis is None:
            gis = _env.active_gis or GIS()
        self._gis = gis
        self._url = url
        if hasattr(gis, "_session"):

            self._session = gis._session
        else:
            self._session = gis.session
        self._is_ogc_feature_server = str(url).lower().endswith("ogcfeatureserver")

    # ---------------------------------------------------------------------
    @property
    def properties(self) -> dict:
        """returns the service properties"""
        params = {"f": "json"}
        if self._properties is None:
            try:
                resp: requests.Response = self._session.get(url=self._url)
                resp.raise_for_status()
                res = resp.json()
                self._properties = res
            except:
                resp: requests.Response = self._session.post(url=self._url, data=params)
                resp.raise_for_status()
                res = resp.json()
                self._properties = res
        return self._properties

    # ---------------------------------------------------------------------
    @property
    @lru_cache(maxsize=100)
    def conformance(self) -> Dict[str, Any]:
        """
        Provides the API conformance with the OGC standard.

        :return: Dict[str, Any]
        """
        url = f"{self._url}/conformance"
        resp: requests.Response = self._session.get(url=url)
        resp.raise_for_status()
        return resp.json()

    # ---------------------------------------------------------------------
    @property
    def collections(self) -> Iterator[OGCCollection]:
        """
        Yields all the OGC Feature Service Layers within the service.

        :return: Iterator[:class:`~arcgis.layers.ogc.OGCCollection`]
        """
        url = f"{self._url}/collections"
        resp: requests.Response = self._session.get(url=url, params={"f": "json"})
        resp.raise_for_status()
        resp_json = resp.json()
        if "collections" not in resp_json:
            return []
        collections = resp_json["collections"]
        for _, lyr in enumerate(collections):
            service_url = f"{url}/{lyr['id']}"
            yield OGCCollection(url=service_url, gis=self._gis)
