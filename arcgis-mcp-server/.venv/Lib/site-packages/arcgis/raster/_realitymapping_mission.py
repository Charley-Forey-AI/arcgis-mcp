from __future__ import annotations
import logging
from typing import Any, Optional, Union

import arcgis
from arcgis.features.layer import FeatureLayer
from arcgis.gis import GIS, Item
from arcgis.raster.realitymapping import Project, _generate_reality_url
from ._util import (
    _update_settings,
    _validate_settings,
    get_request,
    post_request,
    _flatten_adjust_settings,
    _nestify_context,
)

_LOGGER = logging.getLogger(__name__)


class Mission:
    """

    Mission represents a mission in an Realitymapping Project.

    .. note :: This class is not created by users directly. An instance of this class is returned as output for
      get_mission() and create_mission() methods or the missions property on the Project class of arcgis.raster.realitymapping module.

    ====================================     ====================================================================
    **Parameter**                             **Description**
    ------------------------------------     --------------------------------------------------------------------
    mission_name                             Required string representing the mission name.

                                             Example:

                                                mission_name='Mission_Yucaipa'
    ------------------------------------     --------------------------------------------------------------------
    project                                  Required Project object or an Realitymapping Project portal item. The realitymapping project to which the mission belongs to.
    ====================================     ====================================================================

    .. code-block:: python

        # Example Usage 1

        rm_item = gis.content.get("85a54236c6364a88a7c7c2b1a31fd901")
        project = Project(rm_item, gis=gis)

        mission_1 = project.add_mission(image_list,
                                        mission_name="mission_name",
                                        image_collection="img_collection",
                                        raster_type_name="UAV/UAS",
                                        raster_type_params=raster_type_params)

        # Example Usage 2

        rm_item = gis.content.get("85a54236c6364a88a7c7c2b1a31fd901")
        project = Project(rm_item, gis=gis)

        mission_1 = project.get_mission("mission_name")

    """

    def __init__(self, mission_name, mission_id, project):
        self._mission_name = mission_name
        self._mission_id = mission_id
        if isinstance(project, Project):
            self._project = project
        elif isinstance(project, Item):
            if project.type == "Reality Mapping Project":  # "Reality Mapping Project"
                self._project = Project(project, gis=project._gis)

        self._project_item = project._project_item
        self._gis = project._gis
        self._collection = None
        self._prod_to_id_map = {}
        self._reality_url = _generate_reality_url(self._gis)
        self._workspace = self._mission_json.get("workspace", None)

    @property
    def _mission_json(self):
        return self._get_mission_json()

    @property
    def mission_id(self):
        """
        The ``mission_id`` property returns the ID of the mission.

        :return: A string representing the mission ID.
        """
        return self._mission_id

    @property
    def products(self):
        """
        The ``products`` property returns all the products associated with the mission.

        :return: A list of products of the mission.
        """
        mission_products = {}
        self._prod_to_id_map = {}
        dataprod_mapping = {
            "orthoDEM": "dem",
            "qualityReport": "report",
            "mesh": "mesh",
            "DTM": "dtm",
            "mosaicDataset": "image_collection",
            "DSM": "dsm",
            "trueOrtho": "true_ortho",
            "DSMMesh": "dsm_mesh",
            "orthoMosaic": "ortho",
            "pointCloud": "point_cloud",
        }

        url = f"{self._reality_url}/missions/{self._mission_id}/dataproducts"
        token = self._gis._con._create_token(self._reality_url)
        headers = {"Authorization": f"Bearer {token}"}
        products = get_request(url, headers=headers)
        if products is None:
            _LOGGER.warning("No products found for this mission.")
            return mission_products

        for product in products:
            prod_type = product["interpretation"]
            prod_type = (
                dataprod_mapping[prod_type]
                if prod_type in dataprod_mapping
                else prod_type
            )
            self._prod_to_id_map[prod_type] = product["id"]
            try:
                mission_products[prod_type] = Item(
                    self._gis, product["arcgisItem"]["itemId"]
                )
            except:
                pass

        return mission_products

    @property
    def image_count(self):
        """
        The ``image_count`` property returns the number of images in the mission.

        :return: An integer representing the number of images.
        """
        ic = self.image_collection
        lyr = ic.layers[0]
        image_count = lyr.query(return_count_only=True)
        return image_count

    @property
    def mission_date(self):
        """
        The ``mission_date`` property returns the creation date & time of the mission.

        :return: A datetime object representing the mission date & time.
        """
        from datetime import datetime

        ts = self._mission_json["created"]
        dt_obj = datetime.fromisoformat(ts.rstrip("Z"))
        return dt_obj

    @property
    def image_collection(self):
        """
        The ``image_collection`` property returns the image collection associated with the mission.

        :return: image collection item.
        """
        if self._collection is not None:
            return self._collection
        else:
            products = self.products
            self._collection = products.get("image_collection", None)
            if self._collection is None:
                _LOGGER.warning("No image collection found for this mission.")
        return self._collection

    @property
    def workspace(self):
        """
        The ``workspace`` property returns the workspace created for the reality mapping mission on the server.

        :return: A string representing the workspace name.
        """
        if self._workspace is not None:
            return self._workspace

        import json

        metadata = {}
        try:
            if "metadata" in self._mission_json:
                metadata = json.loads(self._mission_json["metadata"])
                self._workspace = metadata.get("workspace", None)
        except:
            pass
        return self._workspace

    @property
    def settings(self):
        """
        The ``settings`` property returns the processing settings of the project.

        :return: A dictionary representing the processing settings of the project.
        """
        return self._mission_json.get("processingSettings", {})

    @settings.setter
    def settings(self, new_settings):
        """
        The ``settings`` method updates the properties of the mission.
        """
        if new_settings is None:
            raise ValueError("new_settings cannot be None")

        current_settings = self.settings
        is_valid = _validate_settings(current_settings, new_settings)

        if not is_valid:
            raise ValueError("Invalid settings provided.")

        # update the current settings with the new settings
        _update_settings(current_settings, new_settings)
        payload = {"processingSettings": current_settings}

        url = f"{self._reality_url}/missions/{self.mission_id}/update"
        token = self._gis._con._create_token(self._reality_url)
        headers = {"Authorization": f"Bearer {token}"}
        resp = post_request(url, payload=payload, headers=headers)
        if resp is None:
            raise RuntimeError("Failed to update mission settings.")

    def _get_mission_json(self):
        url = f"{self._reality_url}/missions/{self._mission_id}"
        token = self._gis._con._create_token(self._reality_url)
        headers = {"Authorization": f"Bearer {token}"}
        resp = get_request(url=url, headers=headers)
        if resp is None:
            raise RuntimeError(f"Failed to retrieve settings.")
        return resp

    def delete(self):
        """
        The ``delete`` method deletes the Mission and all the associated products.

        :return: A boolean indicating whether the deletion was successful or not
        """
        return self._gis._tools.realitymapping.delete_mission(self, future=False)

    def add_image(
        self,
        input_rasters: list,
        raster_type_name: Optional[str] = None,
        raster_type_params: Optional[dict[str, Any]] = None,
        context: Optional[dict[str, Any]] = None,
        *,
        gis: Optional[GIS] = None,
        future: bool = False,
        **kwargs,
    ):
        """
        Add a collection of images to existing image collection of the mission. It provides
        provision to specify image collection properties through context parameter.

        It can be used when new data is available to be included in the same mission of the
        realitymapping project. When new data is added to the image collection
        the entire image collection must be reset to the original state.

        ==================          ====================================================================
        **Parameter**               **Description**
        ------------------          --------------------------------------------------------------------
        input_rasters               Required, the list of input images to be added to
                                    the image collection being created. This parameter can
                                    be a list of image paths or a path to a folder containing the images

                                    The function can create hosted imagery layers on enterprise from 
                                    local raster datasets by uploading the data to the server.  
        ------------------          --------------------------------------------------------------------
        raster_type_name            Optional string. The name of the raster type to use for adding data to
                                    the image collection.


                                    Choice list:

                                        | [
                                        | "Aerial", "ASTER", "DMCII", "DubaiSat-2", "GeoEye-1", "GF-1 PMS", "GF-1 WFV",
                                        | "GF-2 PMS", "GRIB", "HDF", "IKONOS", "Jilin-1", "KOMPSAT-2", "KOMPSAT-3",
                                        | "Landsat 1-5 MSS", "Landsat 4-5 TM", "Landsat 7 ETM+", "Landsat 8", "Landsat 9",
                                        | "NetCDF", "PlanetScope", "Pleiades-1", "Pleiades NEO", "QuickBird", "RapidEye",
                                        | "Raster Dataset", "ScannedAerial", "Sentinel-2", "SkySat", "SPOT 5", "SPOT 6",
                                        | "SPOT 7", "Superview-1", "Tiled Imagery Layer", "UAV/UAS", "WordView-1",
                                        | "WordView-2", "WordView-3", "WordView-4", "ZY3-SASMAC", "ZY3-CRESDA"
                                        | ]
                                

                                    Example:

                                    "QuickBird"
        ------------------          --------------------------------------------------------------------
        raster_type_params          Optional dict. Additional ``raster_type`` specific parameters.
        
                                    The process of add rasters to the image collection can be \
                                    controlled by specifying additional raster type arguments.

                                    The raster type parameters argument is a dictionary.
                                
                                    Syntax:

                                        {"gps": [["image1.jpg", "10", "2", "300"], ["image2.jpg", "10", "3", "300"], ["image3.jpg", "10", "4", "300"]],
                                        "cameraProperties": {"Maker": "Canon", "Model": "5D Mark II", "FocalLength": 20, "PixelSize": 10, "x0": 0, "y0": 0, "columns": 4000, "rows": 3000},
                                        "constantZ": 300,"isAltitudeFlightHeight": "True","dem": {"url": ``https://...``}

                                    The dictionary can contain productType, processingTemplate, \
                                    pansharpenType, Filter, pansharpenWeights, ConstantZ, \
                                    dem, zoffset, CorrectGeoid, ZFactor, StretchType, \
                                    ScaleFactor, ValidRange

                                    Please check the table below (Supported Raster Types), \
                                    for more details about the product types, \
                                    processing templates, pansharpen weights for each raster type. 

                                    - Possible values for pansharpenType - ["Mean", "IHS", "Brovey", "Esri", "Mean", "Gram-Schmidt"]
                                    - Possible values for filter - [None, "Sharpen", "SharpenMore"]
                                    - Value for StretchType dictionary can be as follows:

                                    - "None"
                                    - "MinMax; <min>; <max>"
                                    - "PercentMinMax; <MinPercent>; <MaxPercent>"
                                    - "StdDev; <NumberOfStandardDeviation>"

                                    Example: {"StretchType": "MinMax; <min>; <max>"}
                                    - Value for ValidRange dictionary can be as follows:

                                    - "<MaskMinValue>, <MaskMaxValue>"

                                    Example: {"ValidRange": "10, 200"}

                                    Example:

                                    {"productType":"All","processingTemplate":"Pansharpen",
                                    "pansharpenType":"Gram-Schmidt","filter":"SharpenMore",
                                    "pansharpenWeights":"0.85 0.7 0.35 1","constantZ":-9999}
        ------------------          --------------------------------------------------------------------
        context                     Optional dict. The context parameter is used to provide additional input parameters.

                                    Syntax:

                                    {"image_collection_properties": {"imageCollectionType":"Satellite"},"byref":'True'}
                                
                                    Use ``image_collection_properties`` key to set value for imageCollectionType.


                                    .. note::

                                       The "imageCollectionType" property is important for image collection \
                                       that will later on be adjusted by realitymapping system service. \
                                       Based on the image collection type, the realitymapping system \
                                       service will choose different algorithm for adjustment. \
                                       Therefore, if the image collection is created by reference, \
                                       the requester should set this property based on the type of images \
                                       in the image collection using the following keywords. \
                                       If the imageCollectionType is not set, it defaults to "UAV/UAS"

                                    If byref is set to 'True', the data will not be uploaded. If it is not set, the default is 'False'
        ==================          ====================================================================

        :return: The imagery layer url


        """
        image_collection = self.image_collection

        from arcgis.raster.analytics import add_image

        gis = self._gis

        context = context or {}
        context["mission"] = self.mission_id
        context["workspace"] = self.workspace

        gpjob = add_image(
            image_collection=image_collection,
            input_rasters=input_rasters,
            raster_type_name=raster_type_name,
            raster_type_params=raster_type_params,
            context=context,
            gis=gis,
            future=future,
            **kwargs,
        )

        return image_collection.url

    def delete_image(
        self,
        where: str,
        context: Optional[dict] = None,
        *,
        gis: Optional[GIS] = None,
        future: bool = False,
        estimate: Optional[bool] = False,
        **kwargs,
    ):
        """

        ``delete_image`` allows users to remove existing images from the image collection (mosaic dataset) of a mission.

        ==================                   ====================================================================
        **Parameter**                         **Description**
        ------------------                   --------------------------------------------------------------------
        where                                Required string. A SQL ``where`` clause for selecting the images
                                             to be deleted from the image collection
        ==================                   ====================================================================

        :return: The imagery layer url

        """

        image_collection = self.image_collection

        from arcgis.raster.analytics import delete_image

        gis = self._gis

        context = {"mission": self.mission_id}

        gpjob = delete_image(
            image_collection=image_collection,
            where=where,
            context=context,
            gis=gis,
            future=future,
            estimate=estimate,
            **kwargs,
        )

        return image_collection.url

    def __repr__(self):
        return "<%s - %s>" % (type(self).__name__, self._mission_name)

    def compute_sensor_model(
        self,
        mode: str = "Quick",
        location_accuracy: str = "High",
        context: Optional[dict[str, Any]] = None,
        *,
        gis: Optional[GIS] = None,
        future: bool = False,
        **kwargs,
    ):
        """
        compute_sensor_model computes the bundle block adjustment for the image collection
        and applies the frame xform to the images. It will also generate the control point
        table, solution table, solution points table and flight path table.
        These tables will not be published as Portal items.

        ==================      ====================================================================
        **Parameter**           **Description**
        ------------------      --------------------------------------------------------------------
        mode                    Optional string.  the mode to be used for bundle block adjustment
                                Only the following modes are supported:

                                - 'Quick' : Computes tie points and adjustment at 8x of the source imagery resolution

                                - 'Full'  : adjust the images in Quick mode then at 1x of the source imagery resolution

                                - 'Refine' : adjust the image at 1x of the source imagery resolution

                                By default, 'Quick' mode is applied to compute the sensor model.
        ------------------      --------------------------------------------------------------------
        location_accuracy       Optional string. this option allows users to specify the GPS location accuracy level of the
                                source image. It determines how far the underline tool will search for neighboring
                                matching images, then calculate tie points and compute adjustments.

                                Possible values for location_accuracy are:

                                - 'VeryHigh': Imagery was collected with a high-accuracy, differential GPS, such as RTK or PPK. This option will hold image locations fixed during block adjustment

                                - 'High'    : GPS accuracy is 0 to 10 meters, and the tool uses a maximum of 4 by 3 images

                                - 'Medium'  : GPS accuracy of 10 to 20 meters, and the tool uses a maximum of 4 by 6 images

                                - 'Low'     : GPS accuracy of 20 to 50 meters, and the tool uses a maximum of 4 by 12 images

                                - 'VeryLow' : GPS accuracy is more than 50 meters, and the tool uses a maximum of 4 by 20 images

                                The default location_accuracy is 'High'
        ------------------      --------------------------------------------------------------------
        context                 Optional dictionary. The context parameter is used to configure additional client settings
                                for block adjustment. The supported configurable parameters are for compute mosaic dataset
                                candidates after the adjustment.

                                Example:

                                    {
                                    "computeCandidate": False,
                                    "maxoverlap": 0.6,
                                    "maxloss": 0.05,
                                    }
        ------------------      --------------------------------------------------------------------
        gis                     Optional :class:`~arcgis.gis.GIS` . The GIS on which this tool runs. If not specified, the active GIS is used.
        ==================      ====================================================================

        :return:
            The imagery layer url

        """

        gis = arcgis.env.active_gis if gis is None else gis

        image_collection = self.image_collection
        settings = {}

        try:
            project = self._project
            project_adj_settings = project.settings
            if (
                isinstance(project_adj_settings, dict)
                and "adjustSettings" in project_adj_settings.keys()
            ):
                project_adj_settings = project_adj_settings["adjustSettings"]
            keys_to_pop = ["parallelProcessingFactor"]

            if isinstance(context, dict):
                adjust_options = context.pop("adjustOptions", [])
                adjust_options = _flatten_adjust_settings(adjust_options)
                # context is flattened
                context.update(adjust_options)
                # update adj dict with all the params from context
                project_adj_settings.update(context)
                # pop the keys that are not relevant to the adj settings
                for key in keys_to_pop:
                    project_adj_settings.pop(key, None)
                # update context with default values from project_adj_settings if they are not present in context
                context.update(project_adj_settings)
                _nestify_context(context)

                if (
                    project_adj_settings["locationAccuracy"].lower()
                    != location_accuracy.lower()
                ):
                    project_adj_settings.update({"locationAccuracy": location_accuracy})
            elif context is None:
                context = dict(project_adj_settings)
                _nestify_context(context)
            # update the settings to update flight json
            settings = project_adj_settings
        except:
            adj_dict = {}
            if isinstance(context, dict):
                context_new = {k.lower(): v for k, v in context.items()}
                adj_keys = [
                    "computeCandidate",
                    "maxOverlap",
                    "maxLoss",
                    "maxResidual",
                    "initPointResolution",
                    "k",
                    "p",
                    "principalPoint",
                    "focalLength",
                ]
                adj_dict = {
                    k: context_new[k.lower()]
                    for k in adj_keys
                    if k.lower() in context_new
                }
                adj_dict.update({"locationAccuracy": location_accuracy})
                settings = adj_dict

        settings.update({"mode": mode})
        if context:
            context["mission"] = self.mission_id
        else:
            context = {"mission": self.mission_id}

        return gis._tools.realitymapping.compute_sensor_model(
            image_collection=image_collection,
            mode=mode,
            location_accuracy=location_accuracy,
            context=context,
            future=future,
            **kwargs,
        )

    ###################################################################################################
    ## Alter processing states
    ###################################################################################################
    def alter_processing_states(
        self,
        new_states: dict[str, Any],
        *,
        gis: Optional[GIS] = None,
        future: bool = False,
        **kwargs,
    ):
        """
        Alter the processing states of the image collection.
        The states are stored as key property "Orthomapping".
        The content of the state is a dictionary including
        several properties which can be set based on the process
        done on the image collection.

        ==================      ====================================================================
        **Parameter**           **Description**
        ------------------      --------------------------------------------------------------------
        new_states              Required dictionary. The state to set on the image_collection

                                This a dictionary of states that should be set on the image collection
                                The new states that can be set on the image collection are:
                                blockadjustment, dem, gcp, seamlines, colorcorrection, adjust_index, imagetype

                                Example:

                                    | {"blockadjustment": "raw",
                                    |  "dem": "Dense_Natual_Neighbor",
                                    |  "seamlines":"VORONOI",
                                    |  "colorcorrection":"SingleColor",
                                    |  "imagetype": "UAV/UAS",
                                    |  "adjust_index": 0}
        ------------------      --------------------------------------------------------------------
        gis                     Optional :class:`~arcgis.gis.GIS` . The GIS on which this tool runs. If not specified, the active GIS is used.
        ==================      ====================================================================

        :return:
            The result will be the newly set states dictionary

        """
        gis = arcgis.env.active_gis if gis is None else gis

        image_collection = self.image_collection

        return gis._tools.realitymapping.alter_processing_states(
            image_collection=image_collection,
            new_states=new_states,
            future=future,
            **kwargs,
        )

    ###################################################################################################
    ## Get processing states
    ###################################################################################################
    def get_processing_states(
        self, *, gis: Optional[GIS] = None, future: bool = False, **kwargs
    ):
        """
        Retrieve the processing states of the image collection

        ==================      ====================================================================
        **Parameter**           **Description**
        ------------------      --------------------------------------------------------------------
        gis                     Optional :class:`~arcgis.gis.GIS` . The GIS on which this tool runs.
                                If not specified, the active GIS is used.
        ==================      ====================================================================

        :return:
            The result will be the current states dictionary

        """

        gis = arcgis.env.active_gis if gis is None else gis

        image_collection = self.image_collection

        return gis._tools.realitymapping.get_processing_states(
            image_collection=image_collection, future=future, **kwargs
        )

    ###################################################################################################
    ## Match control points
    ###################################################################################################
    def match_control_points(
        self,
        control_points: list[dict[str, Any]],
        similarity: str = "High",
        context: Optional[dict[str, Any]] = None,
        *,
        gis: Optional[GIS] = None,
        future: bool = False,
        **kwargs,
    ):
        """
        The match_control_points is a function that takes a collection of ground control points
        as input (control points to be specified as a list of dictionary objects), and each of the 
        ground control points needs at least one matching tie point in the control point sets. 
        The function will compute the remaining matching tie points for all control point sets.
        
        ==================      ====================================================================
        **Parameter**           **Description**
        ------------------      --------------------------------------------------------------------
        control_points          Required, a list of control point sets objects.

                                The schema of control points follows the schema 
                                of the mosaic dataset control point table. 

                                The control point object should contain the point geometry, pointID, type, status and the
                                imagePoints. (the imagePoints attribute inside the control points object lists the imageIDs)

                                -- pointID (int) - The ID of the point within the control point table.

                                -- type (int)    - The type of the control point as determined by its numeric value

                                                        1: Tie Point 
                                                        2: Ground Control Point.
                                                        3: Check Point

                                -- status (int)  - The status of the point. A value of 0 indicates that the point will not be used in computation. A non-zero value indicates otherwise.

                                -- imageID (int) - Image identification using the ObjectID from the mosaic dataset footprint table.

                                Example:

                                    | [{
                                    | "status": 1,
                                    | "type": 2,
                                    | "x": -117.0926538,
                                    | "y": 34.00704253,
                                    | "z": 634.2175,
                                    | "spatialReference": {
                                    |     "wkid": 4326
                                    | }, // default WGS84
                                    | "imagePointSpatialReference": {}, // default ICS
                                    | "pointId": 1,
                                    | "xyAccuracy": "0.008602325",
                                    | "zAccuracy": "0.015",
                                    | "imagePoints": [{
                                    |     "imageID": 1,
                                    |     "x": 2986.5435987557084,
                                    |     "y": -2042.5193648409431,
                                    |     "u": 3057.4580682832734,
                                    |     "v": -1909.1506872159698
                                    | },
                                    | {
                                    |     "imageID": 2,
                                    |     "x": 1838.2814361401108,
                                    |     "y": -2594.5280063817972,
                                    |     "u": 3059.4079724863363,
                                    |     "v": -2961.292545463305
                                    | },
                                    | {
                                    |     "imageID": 12,
                                    |     "x": 5332.855578204663,
                                    |     "y": -2533.2805429751907,
                                    |     "u": 614.2338676573158,
                                    |     "v": -165.10836768947297
                                    | },
                                    | {
                                    |     "imageID": 13,
                                    |     "x": 4932.0895715254455,
                                    |     "y": -1833.8401744114287,
                                    |     "u": 616.9396928182223,
                                    |     "v": -1243.1445126959693
                                    | }]
                                    | },
                                    | …
                                    | …
                                    | ] 
        ------------------      --------------------------------------------------------------------
        similarity              Optional string. Choose the tolerance level for your control point matching. 

                                - Low - The similarity tolerance for finding control points will be low. \
                                This option will produce the most control points, \
                                but some may have a higher level of error. 
                                - Medium - The similarity tolerance for finding control points will be medium.
                                - High - The similarity tolerance for finding control points will be high. \
                                
                                This option will produce the least number of control points, \
                                but each matching pair will have a lower level of error. This is the default. 
        ------------------      --------------------------------------------------------------------
        context                 Optional dictionary.Additional settings such as the input control points 
                                spatial reference can be specified here. 

                                Example:

                                    {"groundControlPointsSpatialReference": {"wkid": 3459}, "imagePointSpatialReference": {"wkid": 3459}}

                                Note: The ground control points spatial reference and image point spatial reference 
                                spatial reference set in the context parameter is to decide the returned point set's 
                                ground control points spatial reference and image point spatial reference. 
                                If these two parameters are not set here, the tool will use the spatial reference 
                                defined in the input point set. And if no spatial reference is defined in the point set,
                                then the default ground control points coordinates are in lon/lat and image points 
                                coordinates are in image coordinate system. 
        ------------------      --------------------------------------------------------------------
        gis                     Optional :class:`~arcgis.gis.GIS` . The GIS on which this tool runs. If not specified, the active GIS is used.
        ==================      ====================================================================

        :return:
            A list of dictionary objects

        """
        gis = arcgis.env.active_gis if gis is None else gis

        image_collection = self.image_collection
        context = context or {}
        context["mission"] = self.mission_id

        return gis._tools.realitymapping.match_control_points(
            image_collection=image_collection,
            control_points=control_points,
            similarity=similarity,
            context=context,
            future=future,
            **kwargs,
        )

    ###################################################################################################
    ## Compute Control Points
    ###################################################################################################
    def compute_control_points(
        self,
        reference_image=None,
        image_location_accuracy: str = "High",
        context: Optional[dict[str, Any]] = None,
        *,
        gis: Optional[GIS] = None,
        future: bool = False,
        **kwargs,
    ):
        """
        This service tool is used for computing matching control points between images
        within an image collection and/or matching control points between the image 
        collection images and the reference image.
        `Compute Control Points <https://pro.arcgis.com/en/pro-app/tool-reference/data-management/compute-control-points.htm>`_
        
        ====================================    ====================================================================
        **Parameter**                           **Description**
        ------------------------------------    --------------------------------------------------------------------
        reference_image                         This is the reference image service that can be used to generate ground control 
                                                points set with the image service. 
                                                It can be a portal Item or an image service URL or a URI
        ------------------------------------    --------------------------------------------------------------------
        image_location_accuracy                 Optional string. This option allows you to specify the GPS location accuracy 
                                                level of the source image. It determines how far the tool will search for 
                                                neighboring matching images for calculating tie points and block adjustments. 
                                                
                                                The following are the available options:
                                                Low, Medium, High

                                                - Low- GPS accuracy of 20 to 50 meters, and the tool uses a maximum of 4 by 12 images. 

                                                - Medium- GPS accuracy of 10 to 20 meters, and the tool uses a maximum of 4 by 6 images. 

                                                - High- GPS accuracy of 0 to 10 meters, and the tool uses a maximum of 4 by 3 images.

                                                If the image collection is created from satellite data, it will be automatically switched 
                                                to use RPC adjustment mode. In this case, the mode need not be explicitly set by the user.

                                                Default is High
        ------------------------------------    --------------------------------------------------------------------
        context                                 Optional dictionary. Context contains additional environment settings that affect 
                                                output control points generation. 
                                                
                                                Possible keys and their possible values are: 

                                                pointSimilarity- Sets LOW, MEDIUM, or HIGH tolerance for computing control points with varying levels of potential error.
                                                                
                                                - LOW tolerance will produce the most control point, but may have a higher \
                                                level of error.

                                                - HIGH tolerance will produce the least number of control point, \
                                                but each matching pair will have a lower level of error.

                                                - MEDIUM tolerance will set the similarity tolerance to medium.

                                                pointDensity- Sets the number of tie points (LOW, MEDIUM, or HIGH), to be created. 
                                                            
                                                - LOW point density will create the fewest number of tie points. \

                                                - MEDIUM point density will create a moderate number of tie points. \

                                                - HIGH point density will create the highest number of tie points. \

                                                pointDistribution- Randomly generates points that are better for overlapping areas with irregular shapes.
                                                                
                                                - RANDOM- will generate points that are better for overlapping areas \
                                                with irregular shapes.

                                                - REGULAR- will generate points based on a \
                                                fixed pattern and uses the point density to determine how frequently to create points.

                                                Example:

                                                    {
                                                    "pointSimilarity":"MEDIUM",
                                                    "pointDensity": "MEDIUM",
                                                    "pointDistribution": "RANDOM"
                                                    }
        ------------------------------------    --------------------------------------------------------------------
        gis                                     Optional :class:`~arcgis.gis.GIS` . the GIS on which this tool runs. If not specified, the active GIS is used.
        ====================================    ====================================================================

        :return:
            The imagery layer url

        """
        gis = arcgis.env.active_gis if gis is None else gis

        image_collection = self.image_collection
        if context:
            context["mission"] = self.mission_id
        else:
            context = {"mission": self.mission_id}

        return gis._tools.realitymapping.compute_control_points(
            image_collection=image_collection,
            reference_image=reference_image,
            image_location_accuracy=image_location_accuracy,
            context=context,
            future=future,
            **kwargs,
        )

    ###################################################################################################
    ## Edit control points
    ###################################################################################################
    def edit_control_points(
        self,
        control_points: list[dict[str, Any]],
        *,
        gis: Optional[GIS] = None,
        future: bool = False,
        **kwargs,
    ):
        """
        This service can be used to append additional ground control point sets to
        the image collection's control points. It is recommended that a ground control point (GCP) set
        should contain one ground control point and multiple tie points.
        The service tool can also be used to edit tie point sets.
        The input control points dictionary will always replace the points in the tie points
        table if the point IDs already exist.

        ==================      ====================================================================
        **Parameter**           **Description**
        ------------------      --------------------------------------------------------------------
        control_points          Required, a list of control point sets objects.

                                The schema of control points follows the schema
                                of the mosaic dataset control point table.

                                The control point object should contain the point geometry, pointID, type, status and the
                                imagePoints. (the imagePoints attribute inside the control points object lists the imageIDs)

                                -- pointID (int) - The ID of the point within the control point table.

                                -- type (int)    - The type of the control point as determined by its numeric value

                                                        1: Tie Point
                                                        2: Ground Control Point.
                                                        3: Check Point

                                -- status (int)  - The status of the point. A value of 0 indicates that the point will not be used in computation. A non-zero value indicates otherwise.

                                -- imageID (int) - Image identification using the ObjectID from the mosaic dataset footprint table.

                                Example:

                                    | [{
                                    | "status": 1,
                                    | "type": 2,
                                    | "x": -117.0926538,
                                    | "y": 34.00704253,
                                    | "z": 634.2175,
                                    | "spatialReference": {
                                    |     "wkid": 4326
                                    | }, // default WGS84
                                    | "imagePointSpatialReference": {}, // default ICS
                                    | "pointId": 1,
                                    | "xyAccuracy": "0.008602325",
                                    | "zAccuracy": "0.015",
                                    | "imagePoints": [{
                                    |     "imageID": 1,
                                    |     "x": 2986.5435987557084,
                                    |     "y": -2042.5193648409431,
                                    |     "u": 3057.4580682832734,
                                    |     "v": -1909.1506872159698
                                    | },
                                    | {
                                    |     "imageID": 2,
                                    |     "x": 1838.2814361401108,
                                    |     "y": -2594.5280063817972,
                                    |     "u": 3059.4079724863363,
                                    |     "v": -2961.292545463305
                                    | },
                                    | {
                                    |     "imageID": 12,
                                    |     "x": 5332.855578204663,
                                    |     "y": -2533.2805429751907,
                                    |     "u": 614.2338676573158,
                                    |     "v": -165.10836768947297
                                    | },
                                    | {
                                    |     "imageID": 13,
                                    |     "x": 4932.0895715254455,
                                    |     "y": -1833.8401744114287,
                                    |     "u": 616.9396928182223,
                                    |     "v": -1243.1445126959693
                                    | }]
                                    | },
                                    | …
                                    | …
                                    | ]

        ------------------      --------------------------------------------------------------------
        gis                     Optional :class:`~arcgis.gis.GIS` . The GIS on which this tool runs. If not specified, the active GIS is used.
        ==================      ====================================================================

        :return:
            The Imagery layer url

        """

        gis = arcgis.env.active_gis if gis is None else gis

        image_collection = self.image_collection
        context = {"mission": self.mission_id}

        return gis._tools.realitymapping.edit_control_points(
            image_collection=image_collection,
            input_control_points=control_points,
            future=future,
            context=context,
            **kwargs,
        )

    ###################################################################################################
    ## Generate orthomosaic
    ###################################################################################################
    def generate_orthomosaic(
        self,
        out_ortho=None,
        regen_seamlines: bool = True,
        recompute_color_correction: bool = True,
        context: Optional[dict[str, Any]] = None,
        *,
        gis: Optional[GIS] = None,
        future: bool = False,
        **kwargs,
    ):
        """
        Function can be used for generating single ortho-rectified mosaicked image from image collection after
        the block adjustment.

        ===================================     ====================================================================
        **Parameter**                           **Description**
        -----------------------------------     --------------------------------------------------------------------
        out_ortho                               Required String or dict. This is the ortho-mosaicked image converted from the image
                                                collection after the block adjustment.
                                                It can be a url, uri, portal item, or string representing the name of output dem
                                                (either existing or to be created.)
                                                If passed as a dict, the following keys can be set:
                                                - service_name: The name for the output ortho-mosaicked image service.
                                                - portal_name: The name for the portal item for this image service.
                                                If this product has already been created, the tool will overwrite it instead.
                                                Like Raster Analysis services, the service can be an existing multi-tenant service URL.
        -----------------------------------     --------------------------------------------------------------------
        regen_seamlines                         Optional, boolean.
                                                Choose whether to apply seamlines before the orthomosaic image generation or not.
                                                The seamlines will always be regenerated if this parameter is set to True.
                                                The user can set the seamline options through the context parameter.
                                                If the seamline generation options are not set, the default will be used.

                                                Default value is True
        -----------------------------------     --------------------------------------------------------------------
        recompute_color_correction              Optional, boolean.
                                                Choose whether to apply color correction settings to the output ortho-image or not.
                                                Color correction will always be recomputed if this option is set to True.
                                                The user can configure the compute color correction settings through the context parameter.
                                                If there is no color collection setting, the default will be used.

                                                Default value is True
        -----------------------------------     --------------------------------------------------------------------
        context                                 Optional dictionary. Context contains additional environment settings that affect output
                                                image. The supported environment settings for this tool are:

                                                1. Output Spatial Reference (outSR)-the output features will
                                                    be projected into the output spatial reference.

                                                2. Extent (extent) - extent that would clip or expand the output image

                                                3. Cell Size (cellSize) - The output raster will have the resolution specified by cell size.

                                                4. Compute Seamlines (seamlinesMethod) - Default.

                                                5. Clipping Geometry (clippingGeometry) - Clips the orthomosaic image to an area of
                                                    interest defined by the geometry.

                                                6. Orthomosaic As Overview (orthoMosaicAsOvr) - Adds the orthomosaic as an overview of the image collection.

                                                7. Compute Color Correction (colorcorrectionMethod) — Default.

                                                Example:

                                                    | {
                                                    |   "outSR": {"wkid": 3516},
                                                    |   "extent": {"xmin": 470614.263139,
                                                    |             "ymin": 8872849.409968,
                                                    |             "xmax": 532307.351827,
                                                    |             "ymax": 8920205.372412,
                                                    |             "spatialReference": {"wkid": 32628}},
                                                    |   "clippingGeometry": {},
                                                    |   "orthoMosaicAsOvr": False,
                                                    |   "seamlinesMethod": "VORONOI",
                                                    |   "minRegionSize": 100,
                                                    |   "pixelSize": "",
                                                    |   "blendType": "Both",
                                                    |   "blendWidth": None,
                                                    |   "blendUnit": "Pixels",
                                                    |   "requestSize": 1000,
                                                    |   "minThinnessRatio": 0.05,
                                                    |   "maxSliverSize": 20
                                                    |   "colorCorrectionMethod": "DODGING",
                                                    |   "dodgingSurface": "Single_Color",
                                                    |   "referenceImg": {"url": "``https://...``"},
                                                    |   "skipRows": 10,
                                                    |   "skipCols": 10,
                                                    |   "reCalculateSats": "OVERWRITE"
                                                    |  }

        -----------------------------------     --------------------------------------------------------------------
        gis                                     Optional :class:`~arcgis.gis.GIS` . The GIS on which this tool runs. If not specified, the active GIS is used.
        ===================================     ====================================================================

        :return:
            The Orthomosaicked Imagery layer item

        """
        gis = arcgis.env.active_gis if gis is None else gis

        image_collection = self.image_collection

        if self.workspace:
            if context:
                context["workspace"] = self.workspace
            else:
                context = {"workspace": self.workspace}

        products = self.products
        if "ortho" in products:
            out_ortho = products["ortho"]
        if "ortho" in self._prod_to_id_map:
            context["dataproduct_id"] = self._prod_to_id_map["ortho"]

        context["mission"] = self.mission_id
        group = self._project.group
        context["group"] = group.id

        if kwargs is not None:
            if "folder" in kwargs:
                folder = kwargs["folder"]
            else:
                for f in gis.users.me.folders:
                    if f._fid == image_collection.ownerFolder:
                        folder = f.properties
                        break
            kwargs.update({"folder": folder})

        color_balance_keys = [
            "targetRaster",
            "skipX",
            "skipY",
            "overwriteStats",
            "dodgingSurface",
            "colorCorrectionMethod",
        ]
        color_balance_dict = {}
        color_balance_dict.update(
            {
                "colorBalance": {
                    "skipX": 0,
                    "skipY": 0,
                    "overwriteStats": "SKIP_EXISTING",
                    "colorCorrectionMethod": "DODGING",
                    "dodgingSurface": "SINGLE_COLOR",
                    "targetImage": "",
                }
            }
        )

        ortho_dict = {}
        if isinstance(context, dict):
            context_new = {k.lower(): v for k, v in context.items()}
            color_balance_dict["colorBalance"].update(
                {
                    k: context_new[k.lower()]
                    for k in color_balance_keys
                    if k.lower() in context_new
                }
            )
            if "colorCorrectionMethod" in color_balance_dict["colorBalance"]:
                color_balance_dict["colorBalance"]["method"] = color_balance_dict[
                    "colorBalance"
                ].pop("colorCorrectionMethod")
            if "dodgingSurface" in color_balance_dict["colorBalance"]:
                color_balance_dict["colorBalance"]["surfaceType"] = color_balance_dict[
                    "colorBalance"
                ].pop("dodgingSurface")

            seamline_keys = [
                "computeCandidate",
                "maxOverlap",
                "maxLoss",
                "pixelSize",
                "blendType",
                "blendUnit",
                "requestSizeType",
                "requestSize",
                "minThinnessRatio",
                "maxSliverSize",
                "seamlinesMethod",
            ]
            seamline_dict = {}
            seamline_dict.update(
                {
                    "seamline": {
                        "seamlinesMethod": "DISPARITY",
                        "minRegionSize": 100,
                        "pixelSize": "",
                        "blendType": "Both",
                        "blendWidth": None,
                        "blendUnit": "Pixels",
                        "requestSizeType": "Pixels",
                        "requestSize": 1000,
                        "minThinnessRatio": 0.05,
                        "maxSliverSize": 20,
                    }
                }
            )

            seamline_dict["seamline"].update(
                {
                    k: context_new[k.lower()]
                    for k in seamline_keys
                    if k.lower() in context_new
                }
            )
            if "seamlinesMethod" in seamline_dict["seamline"]:
                seamline_dict["seamline"]["method"] = seamline_dict["seamline"].pop(
                    "seamlinesMethod"
                )

            ortho_mosaic_as_ovr = context.get("orthoMosaicAsOvr", False)
            ortho_dict = {"ortho": {"orthoMosaicAsOvr": ortho_mosaic_as_ovr}}

            if regen_seamlines:
                ortho_dict.update(seamline_dict)
            if recompute_color_correction:
                ortho_dict.update(color_balance_dict)

        return gis._tools.realitymapping.generate_orthomosaic(
            image_collection=image_collection,
            output_ortho_image=out_ortho,
            regen_seamlines=regen_seamlines,
            recompute_color_correction=recompute_color_correction,
            context=context,
            future=future,
            **kwargs,
        )

    ###################################################################################################
    ## Generate report
    ###################################################################################################
    def generate_report(
        self,
        report_format: str = "PDF",
        *,
        gis: Optional[GIS] = None,
        future: bool = False,
        **kwargs,
    ):
        """
        This function is used to generate realitymapping report with image collection
        that has been block adjusted. The report would contain information about
        the quality of the adjusted images, the distribution of the control points, etc.
        The output of this service tool is a downloadable html page.

        ===================    ====================================================================
        **Parameter**          **Description**
        -------------------    --------------------------------------------------------------------
        report_format          Type of the format to be generated. Possible PDF, HTML. Default - PDF
        -------------------    --------------------------------------------------------------------
        gis                    Optional :class:`~arcgis.gis.GIS` . The GIS on which this tool runs. If not specified, the active GIS is used.
        ===================    ====================================================================

        :return:
            The URL of a single html webpage that is a formatted realitymapping report

        """
        gis = arcgis.env.active_gis if gis is None else gis

        image_collection = self.image_collection
        context = {"mission": self.mission_id}

        return gis._tools.realitymapping.generate_report(
            image_collection=image_collection,
            report_format=report_format,
            future=future,
            context=context,
            **kwargs,
        )

    ###################################################################################################
    ## query camera info
    ###################################################################################################
    def query_camera_info(
        camera_query: Optional[str] = None,
        *,
        gis: Optional[GIS] = None,
        future: bool = False,
        **kwargs,
    ):
        """
        This service tool is used to query specific or the entire digital camera
        database. The digital camera database contains the specs
        of digital camera sensors that were used to capture drone images.

        ==================      ====================================================================
        **Parameter**           **Description**
        ------------------      --------------------------------------------------------------------
        camera_query            Required String. This is a SQL query statement that can
                                be used to filter a portion of the digital camera
                                database.
                                Digital camera database can be queried using the fields Make, Model,
                                Focallength, Columns, Rows, PixelSize.

                                Example:

                                    "Make='Rollei' and Model='RCP-8325'"
        ------------------      --------------------------------------------------------------------
        gis                     Optional :class:`~arcgis.gis.GIS` . The GIS on which this tool runs.
                                If not specified, the active GIS is used.
        ==================      ====================================================================


        :return:
            Data Frame representing the camera database

        """
        gis = arcgis.env.active_gis if gis is None else gis

        return gis._tools.realitymapping.query_camera_info(
            camera_query=camera_query, future=future, **kwargs
        )

    ###################################################################################################
    ## query control points
    ###################################################################################################
    def query_control_points(
        self,
        query: str,
        *,
        gis: Optional[GIS] = None,
        future: bool = False,
        **kwargs,
    ):
        """
        Query for control points in an image collection. It allows users to query
        among certain control point sets that has ground control points inside.

        ==================      ====================================================================
        **Parameter**           **Description**
        ------------------      --------------------------------------------------------------------
        query                   Required string. a SQL statement used for querying the point

                                Example:

                                "pointID > 100"
        ------------------      --------------------------------------------------------------------
        gis                     Optional :class:`~arcgis.gis.GIS` . The GIS on which this tool runs.
                                If not specified, the active GIS is used.
        ==================      ====================================================================


        :return:
            A dictionary object

        """
        gis = arcgis.env.active_gis if gis is None else gis

        image_collection = self.image_collection

        return gis._tools.realitymapping.query_control_points(
            image_collection=image_collection,
            where=query,
            future=future,
            **kwargs,
        )

    ###################################################################################################
    ## Reset image collection
    ###################################################################################################
    def reset_image_collection(
        self, *, gis: Optional[GIS] = None, future: bool = False, **kwargs
    ):
        """
        Reset the image collection. It is used to reset the image collection to its
        original state. The image collection could be adjusted during the orthomapping
        workflow and if the user is not satisfied with the result, they will be able
        to clear any existing adjustment settings and revert the images back to
        un-adjusted state

        ==================      ====================================================================
        **Parameter**           **Description**
        ------------------      --------------------------------------------------------------------
        gis                     Optional :class:`~arcgis.gis.GIS` . The GIS on which this tool runs.
                                If not specified, the active GIS is used.
        ==================      ====================================================================

        :return:
            A boolean indicating whether the reset was successful or not

        """
        gis = arcgis.env.active_gis if gis is None else gis

        image_collection = self.image_collection
        context = {"mission": self.mission_id}

        return gis._tools.realitymapping.reset_image_collection(
            image_collection=image_collection,
            future=future,
            context=context,
            **kwargs,
        )

    ###################################################################################################
    ## Query exif info
    ###################################################################################################
    def query_exif_info(
        input_images, *, gis: Optional[GIS] = None, future: bool = False, **kwargs
    ):
        """
        The `query_exif_info` reads the Exif header metadata from single or
        multiple images in shared data store. The Exif metadata is usually stored
        in drone image files. Some common Exif metadata information are GPS
        locations, camera model, focal length, and more.

        ==================      ====================================================================
        **Parameter**           **Description**
        ------------------      --------------------------------------------------------------------
        input_images            Required String/list of Strings.  The input images could be a single image path, list of image paths,
                                or a folder path, or a list of folder paths. The image file paths can also be server data store path.

                                Eg:

                                - "\\servername\drone\imagefolder\image_file.jpg"
                                - "/cloudStores/S3DataStore/yvwd13"
                                - "/fileShares/drones/SampleEXIF/YUN_0040.jpg"
                                - ["/fileShares/drones/SampleEXIF/DJI_0002.JPG", "/fileShares/drones/SampleEXIF/YUN_0040.jpg"]
                                - ["/cloudStores/S3DataStore/yvwd13", "/cloudStores/S3DataStore/BogotaFarm"]
        ------------------      --------------------------------------------------------------------
        gis                     Optional :class:`~arcgis.gis.GIS` . The GIS on which this tool runs.
                                If not specified, the active GIS is used.
        ==================      ====================================================================

        :return:
            A dictionary object

        """
        gis = arcgis.env.active_gis if gis is None else gis

        return gis._tools.realitymapping.query_exif_info(
            input_images=input_images, future=future, **kwargs
        )

    ###################################################################################################
    ## Reconstruct surface
    ###################################################################################################
    def reconstruct_surface(
        self,
        scenario: Optional[str] = "DRONE",
        forward_overlap: Optional[int] = None,
        sideward_overlap: Optional[int] = None,
        quality: Optional[str] = "ULTRA",
        area_of_interest: Optional[Union[str, FeatureLayer]] = "AUTO",
        waterbody_features: Optional[FeatureLayer] = None,
        correction_feature: Optional[FeatureLayer] = None,
        reconstruct_options: Optional[str] = None,
        output_dsm_name: Optional[str] = None,
        output_true_ortho_name: Optional[str] = None,
        output_dsm_mesh_name: Optional[str] = None,
        output_point_cloud_name: Optional[str] = None,
        output_mesh_name: Optional[str] = None,
        output_dtm_name: Optional[str] = None,
        context: Optional[dict[str, Any]] = None,
        *,
        gis: Optional[GIS] = None,
        future: bool = False,
        **kwargs,
    ):
        """
        The `reconstruct_surface` generates a digital surface model (DSM), true
        orthos, 2.5D meshes, 3D meshes, and point clouds from adjusted imagery.

        =========================================================================   ===========================================================================
        **Parameter**                                                                **Description**
        -------------------------------------------------------------------------   ---------------------------------------------------------------------------
        scenario                                                                    Optional String. Specifies the type of imagery that will be used to generate the output products.

                                                                                    - DRONE: The input imagery will be defined as having been acquired with drones or terrestrial cameras.
                                                                                    - AERIAL_NADIR: The input imagery will be defined as having been acquired with large, photogrammetric camera systems.
                                                                                    - AERIAL_OBLIQUE: The input imagery will be defined as having been acquired with oblique camera systems.
                                                                                    - SATELLITE: The input imagery will be defined as having been acquired with a satellite.
        -------------------------------------------------------------------------   ---------------------------------------------------------------------------
        forward_overlap                                                             Optional Integer. The forward (in-strip) overlap percentage that will be used between the images.
                                                                                    The default is 60.
                                                                                    This parameter is enabled when the scenario parameter is set to AERIAL_NADIR.
        -------------------------------------------------------------------------   ---------------------------------------------------------------------------
        sideward_overlap                                                            Optional Integer. The sideward (cross-strip) overlap percentage that will be used between the images.
                                                                                    The default is 30.
                                                                                    This parameter is enabled when the scenario parameter is set to AERIAL_NADIR.
        -------------------------------------------------------------------------   ---------------------------------------------------------------------------
        quality                                                                     Optional String. Specifies the quality of the final product.

                                                                                    - ULTRA - Input images will be used at their original (full) resolution.
                                                                                    - HIGH - Input images will be downsampled two times.
        -------------------------------------------------------------------------   ---------------------------------------------------------------------------
        area_of_interest                                                            Optional :class:`~arcgis.features.FeatureLayer` or String. The area of interest that will
                                                                                    be used to select images for processing. The area of interest can be computed automatically
                                                                                    or defined using an input feature.
                                                                                    If the value contains 3D geometries, the z-component will be ignored. If the value includes
                                                                                    overlapping features, the union of these features will be computed.

                                                                                    - NONE - All images will be used in processing.
                                                                                    - AUTO - The processing extent will be calculated automatically. This is the default.
        -------------------------------------------------------------------------   ---------------------------------------------------------------------------
        waterbody_features                                                          Optional :class:`~arcgis.features.FeatureLayer`. A polygon that will define the extent of large water bodies.
                                                                                    For the best results, use a 3D feature.
        -------------------------------------------------------------------------   ---------------------------------------------------------------------------
        correction_features                                                         Optional :class:`~arcgis.features.FeatureLayer`. A polygon that will define the extent of all surfaces that are not water bodies.
                                                                                    The value must be a 3D feature.
        -------------------------------------------------------------------------   ---------------------------------------------------------------------------
        reconstruction_options                                                      Optional dict or shared data path (this path must be accessible by the server).
                                                                                    This specifies the values for the tool parameters. If this parameter is specified, the properties of
                                                                                    the file or dictionary will set the default values for the remaining optional parameters.
                                                                                    The list of keywords and an example of this JSON can be found here:
                                                                                    `Reconstruct Surface tool <https://pro.arcgis.com/en/pro-app/latest/tool-reference/reality-mapping/reconstruct-surface.htm>`_
        -------------------------------------------------------------------------   ---------------------------------------------------------------------------
        output_dsm_name                                                             Optional String. You can pass in the name of the output Image Service that should be created by this method to be
                                                                                    used as the output for the tool.
                                                                                    If this product has already been created, the tool will overwrite it instead.

                                                                                    A RuntimeError is raised if a service by that name already exists.
        -------------------------------------------------------------------------   ---------------------------------------------------------------------------
        output_true_ortho_name                                                      Optional String. You can pass in the name of the output Image Service that should be created by this method to be
                                                                                    used as the output for the tool.
                                                                                    If this product has already been created, the tool will overwrite it instead.

                                                                                    A RuntimeError is raised if a service by that name already exists.
        -------------------------------------------------------------------------   ---------------------------------------------------------------------------
        output_dsm_mesh_name                                                        Optional String. You can pass in the name of the output Image Service that should be created by this method to be
                                                                                    used as the output for the tool.
                                                                                    If this product has already been created, the tool will overwrite it instead.

                                                                                    A RuntimeError is raised if a service by that name already exists.
        -------------------------------------------------------------------------   ---------------------------------------------------------------------------
        output_point_cloud_name                                                     Optional String. You can pass in the name of the output Image Service that should be created by this method to be
                                                                                    used as the output for the tool.
                                                                                    If this product has already been created, the tool will overwrite it instead.

                                                                                    A RuntimeError is raised if a service by that name already exists.
        -------------------------------------------------------------------------   ---------------------------------------------------------------------------
        output_mesh_name                                                            Optional String. You can pass in the name of the output Image Service that should be created by this method to be
                                                                                    used as the output for the tool.
                                                                                    If this product has already been created, the tool will overwrite it instead.

                                                                                    A RuntimeError is raised if a service by that name already exists.
        -------------------------------------------------------------------------   ---------------------------------------------------------------------------
        output_dtm_name                                                             Optional String. You can pass in the name of the output Image Service that should be created by this method to be
                                                                                    used as the output for the tool.
                                                                                    If this product has already been created, the tool will overwrite it instead.

                                                                                    A RuntimeError is raised if a service by that name already exists.
        -------------------------------------------------------------------------   ---------------------------------------------------------------------------
        context                                                                     Context contains additional settings that affect task execution.

                                                                                    context parameter overwrites values set through arcgis.env parameter

                                                                                    This function has the following settings:

                                                                                    - Extent (extent): A bounding box that defines the analysis area.

                                                                                        Example:

                                                                                            | {"extent": {"xmin": -122.68,
                                                                                            | "ymin": 45.53,
                                                                                            | "xmax": -122.45,
                                                                                            | "ymax": 45.6,
                                                                                            | "spatialReference": {"wkid": 4326}}}

                                                                                    - Cell Size (cellSize): The output raster will have the resolution
                                                                                    specified by cell size.

                                                                                        Example:

                                                                                            {'cellSize': 11} or {'cellSize': {'url': <image_service_url>}}  or {'cellSize': 'MaxOfIn'}

                                                                                    - Parallel Processing Factor (parallelProcessingFactor): controls
                                                                                    Raster Processing (CPU) service instances.

                                                                                        Example:

                                                                                        Syntax example with a specified number of processing instances:

                                                                                            {"parallelProcessingFactor": "2"}

                                                                                        Syntax example with a specified percentage of total
                                                                                        processing instances:

                                                                                            {"parallelProcessingFactor": "60%"}

                                                                                    - Output DSM product settings: controls
                                                                                    the environment variables for creating the DSM product.

                                                                                        Example:

                                                                                        Syntax example with a specified number of processing instances:

                                                                                            {key: {"outputType": "Tiled", "compression": "JPEG 75", "resamplingMethod": "NEAREST", "cellSize": 10, "noData": 0}}

                                                                                    - Output True Ortho product settings: controls
                                                                                    the environment variables for creating the DSM product.

                                                                                        Example:

                                                                                        Syntax example with a specified number of processing instances:

                                                                                            {"true_ortho": {"outputType": "Mosaic", "compression": "JPEG 75", "resamplingMethod": "NEAREST", "cellSize": 10, "noData": 0}}
        -------------------------------------------------------------------------   ---------------------------------------------------------------------------
        gis                                                                         Optional, the GIS on which this tool runs. If not specified, the active GIS is used.
        -------------------------------------------------------------------------   ---------------------------------------------------------------------------
        future                                                                      Optional boolean. If True, the result will be a GPJob object and results will be returned asynchronously.
        =========================================================================   ===========================================================================

        :return: Named Tuple

        """
        gis = arcgis.env.active_gis if gis is None else gis

        image_collection = self.image_collection
        products = self.products

        if output_dsm_name:
            if "dsm" in products:
                output_dsm_name = products["dsm"]
        if output_true_ortho_name:
            if "true_ortho" in products:
                output_true_ortho_name = products["true_ortho"]
        if output_dsm_mesh_name:
            if "dsm_mesh" in products:
                output_dsm_mesh_name = products["dsm_mesh"]
        if output_point_cloud_name:
            if "point_cloud" in products:
                output_point_cloud_name = products["point_cloud"]
        if output_mesh_name:
            if "mesh" in products:
                output_mesh_name = products["mesh"]
        if output_dtm_name:
            if "dtm" in products:
                output_dtm_name = products["dtm"]

        if self.workspace:
            if context:
                context["workspace"] = self.workspace
            else:
                context = {"workspace": self.workspace}

        context["mission"] = self.mission_id
        group = self._project.group
        context["group"] = group.id

        prod_types = ["dtm", "dsm", "true_ortho", "dsm_mesh", "point_cloud", "mesh"]
        dataproduct_ids = {
            k: v for k, v in self._prod_to_id_map.items() if k in prod_types
        }
        context["dataproduct_id"] = dataproduct_ids

        if kwargs is not None:
            if "folder" in kwargs:
                folder = kwargs["folder"]
            else:
                for f in gis.users.me.folders:
                    if f._fid == image_collection.ownerFolder:
                        folder = f.properties
                        break
            kwargs.update({"folder": folder})

        return gis._tools.realitymapping.reconstruct_surface(
            image_collection=image_collection,
            scenario=scenario,
            forward_overlap=forward_overlap,
            sideward_overlap=sideward_overlap,
            quality=quality,
            area_of_interest=area_of_interest,
            waterbody_features=waterbody_features,
            correction_feature=correction_feature,
            reconstruct_options=reconstruct_options,
            output_dsm_name=output_dsm_name,
            output_true_ortho_name=output_true_ortho_name,
            output_dsm_mesh_name=output_dsm_mesh_name,
            output_point_cloud_name=output_point_cloud_name,
            output_mesh_name=output_mesh_name,
            output_dtm_name=output_dtm_name,
            context=context,
            future=future,
            **kwargs,
        )
