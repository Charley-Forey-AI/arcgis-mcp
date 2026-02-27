"""
The orthomapping python API allows automating orthomapping tasks in the server environment.

For more information about orthomapping workflows in ArcGIS, please visit the help documentation at
`Block adjustment for mosaic datasets <https://desktop.arcgis.com/en/arcmap/10.4/manage-data/raster-and-images/block-adjustment-for-mosaic-datasets.htm>`_

"""

from __future__ import annotations
from typing import Any, Optional, Union
import arcgis
import json
from arcgis.gis import GIS, Item
import collections
from ._util import _set_context
import string as _string
import random as _random

from arcgis.geoprocessing._support import (
    _analysis_job,
    _analysis_job_results,
    _analysis_job_status,
    _layer_input,
)

###################################################################################################
###
### INTERNAL FUNCTIONS
###
###################################################################################################


def _execute_task(gis, taskname, params):
    gptool_url = gis.properties.helperServices.orthoMapping.url
    gptool = arcgis.gis._GISResource(gptool_url, gis)
    task = taskname

    task_url, job_info, job_id = _analysis_job(gptool, task, params)
    # print ('task url is ', task_url)

    job_info = _analysis_job_status(gptool, task_url, job_info)
    job_values = _analysis_job_results(gptool, task_url, job_info, job_id)

    item_properties = {
        "properties": {
            "jobUrl": task_url + "/jobs/" + job_info["jobId"],
            "jobType": "GPServer",
            "jobId": job_info["jobId"],
            "jobStatus": "completed",
        }
    }
    return job_values


def _id_generator(size=6, chars=_string.ascii_uppercase + _string.digits):
    return "".join(_random.choice(chars) for _ in range(size))


###################################################################################################
###################################################################################################
def _set_image_collection_param(gis, params, image_collection):
    if isinstance(image_collection, str):
        if "http:" in image_collection or "https:" in image_collection:
            params["imageCollection"] = json.dumps({"url": image_collection})
        else:
            params["imageCollection"] = json.dumps({"uri": image_collection})
    elif isinstance(image_collection, Item):
        params["imageCollection"] = json.dumps({"itemId": image_collection.itemid})
    else:
        raise TypeError("image_collection should be a string (service name) or Item")

    return


def _create_output_image_service(gis, output_name, task):
    ok = gis.content.is_service_name_available(output_name, "Image Service")
    if not ok:
        raise RuntimeError(
            "An Image Service by this name already exists: " + output_name
        )

    create_parameters = {
        "name": output_name,
        "description": "",
        "capabilities": "Image",
        "properties": {"path": "@", "description": "", "copyright": ""},
    }

    output_service = gis.content.create_service(
        output_name,
        create_params=create_parameters,
        service_type="imageService",
    )
    description = "Image Service generated from running the " + task + " tool."
    item_properties = {
        "description": description,
        "tags": "Analysis Result, " + task,
        "snippet": "Analysis Image Service generated from " + task,
    }
    output_service.update(item_properties)
    return output_service


def _get_collection_item(project_item=None, flight_name=None, gis=None):
    rm = project_item.resources
    res_list = rm.list()

    try:
        last_res = res_list[-1]
        props_json = last_res["properties"]
        props = json.loads(props_json)
        items_list = props["items"]
        for ele in items_list:
            if ele["product"] == "imageCollection":
                item_id = ele["id"]
        image_collection_item = gis.content.get(item_id)
        return image_collection_item, last_res
    except:
        raise RuntimeError("Unable to retrieve the flight information")


def _update_flight_info(
    flight, project_item, processing_states={}, item_name="", gis=None
):
    # job_info=output_item.properties
    # job_response={}
    # if "jobUrl" in job_info :
    # Get the url of the Analysis job to track the status.

    # job_url = job_info.get("jobUrl")
    # params = {"f": "json"}
    # job_response = gis._con.post(job_url, params)

    resource = flight["resource"]

    rm = project_item.resources
    flight_json = rm.get(resource)

    # flight_json["items"].update({item_name:{"itemId": output_item.itemid, "url":output_item.url}})

    # flight_json['jobs'].update({item_name:{"messages": job_response["messages"], "checked": True, "progress": 100,"success": True}})
    flight_json["processingSettings"].update({item_name: processing_states})

    properties = json.loads(flight["properties"])

    # properties = json.loads(flight['properties'])
    # properties_items =properties["items"]
    # properties_items.append({"product": item_name, "id": output_item.itemid, "created": True})
    # properties.update({"items": properties_items})

    import tempfile, uuid, os

    fname = resource.split("/")[1]
    temp_dir = tempfile.gettempdir()
    temp_file = os.path.join(temp_dir, fname)
    with open(temp_file, "w") as writer:
        json.dump(flight_json, writer)
    del writer

    try:
        rm.update(
            file=temp_file,
            text=flight_json,
            folder_name="flights",
            file_name=fname,
            properties=properties,
        )
    except:
        raise RuntimeError("Error updating the flight resource")


def _create_project(
    name: str,
    definition: Optional[dict[str, Any]] = None,
    *,
    gis: Optional[GIS] = None,
    **kwargs,
):
    """
    Creates a new orthomapping project item on your enterprise.
    This project item can be specified as input to the orthomapping functions as value to the
    image_collection parameter.

    The orthomapping project item can be open in Ortho Maker web app.
    The Project includes all project inputs, ancillary data such as image footprints and block adjustment reports,
    intermediate products such as image collections, quick block adjustment results, final products,
    and status at each stage of processing.

    The create_project method also creates a new folder and adds the orthomapping project item to it.
    All the orthomapping products such as the image collection, orthomosaic products etc will be added in the
    same folder. The folder name will be same the project name with the prefix "_orthomapping_"

    ==================     ====================================================================
    **Parameter**           **Description**
    ------------------     --------------------------------------------------------------------
    name                   Required string. The name of the project item to be created.
    ------------------     --------------------------------------------------------------------
    definition             Optional dictionary.  The project definition dictionary.
                           the definition contais the template informatios such as adjustSettings,
                           processingStates, rasterType, information about the flights.
    ------------------     --------------------------------------------------------------------
    gis                    Optional :class:`~arcgis.gis.GIS` . The GIS on which this tool runs. If not specified, the active GIS is used.
    ==================     ====================================================================

    :return:
        The orthomapping project item

    """

    gis = arcgis.env.active_gis if gis is None else gis
    folder = None
    folderId = None

    if folder is None:
        folder = "_orthomapping " + name
    owner = gis.properties.user.username
    try:
        folder_item = gis.content.folders.create(folder, owner)
        folder_dict = folder_item.properties
    except:
        raise RuntimeError(
            "Unable to create folder for Orthomapping Project Item. The project name is not available."
        )
    folder = folder_dict["title"]
    folderId = folder_dict["id"]

    item_properties = {
        "title": name,
        "type": "Ortho Mapping Project",
        "properties": {"flightCount": 0, "status": "inProgress"},
    }
    if definition is None:
        definition = {}

    item_properties["text"] = json.dumps(definition)
    folder = gis.content.folders.get(folder)
    item = folder.add(item_properties).result()
    return item


def _add_mission(
    project,
    image_list: list,
    mission_name: Optional[str] = None,
    image_collection: Optional[str] = None,
    raster_type_name: Optional[str] = None,
    raster_type_params: Optional[dict[str, Any]] = None,
    out_sr: Optional[dict[str, Any]] = None,
    context: Optional[dict[str, Any]] = None,
    *,
    gis: Optional[GIS] = None,
    **kwargs,
):
    """
    Add flights to the orthomapping project item. You can add imagery from one or more drone flights 
    to your orthomapping project item.

    ======================               ====================================================================
    **Parameter**                        **Description**
    ----------------------               --------------------------------------------------------------------
    project_item                         Required Item. The orthomapping project item to which the flight has to be added
    ----------------------               --------------------------------------------------------------------
    image_list                           Required, the list of input images to be added to
                                         the image collection being created. This parameter can
                                         be a list of image paths or a path to a folder containing the images

                                         The function can create hosted imagery layers on enterprise from 
                                         local raster datasets by uploading the data to the server.    
    ----------------------               --------------------------------------------------------------------
    flight_name                          Optional string. The name of the flight.
    ----------------------               --------------------------------------------------------------------
    image_collection                     Optional string, the name of the image collection to create.
                  
                                         The image collection can be an existing image service, in \
                                         which the function will create a mosaic dataset and the existing \
                                         hosted image service will then point to the new mosaic dataset.

                                         If the image collection does not exist, a new multi-tenant \
                                         service will be created.

                                         This parameter can be the Item representing an existing image_collection \
                                         or it can be a string representing the name of the image_collection \
                                         (either existing or to be created.)

                                         The image collection will be created in the same folder as the one created
                                         by the create_project method
    ----------------------               --------------------------------------------------------------------
    raster_type_name                     Optional string. The name of the raster type to use for adding data to \
                                         the image collection. Default is "UAV/UAS"

                                         Example:

                                            "UAV/UAS"
    ----------------------               --------------------------------------------------------------------
    raster_type_params                   Optional dict. Additional ``raster_type`` specific parameters.
        
                                         The process of add rasters to the image collection can be \
                                         controlled by specifying additional raster type arguments.

                                         The raster type parameters argument is a dictionary.

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
    ----------------------               --------------------------------------------------------------------
    out_sr                               Optional integer. Additional parameters of the service.
                            
                                         The following additional parameters can be specified:

                                         - Spatial reference of the image_collection; The well-known ID of \
                                         the spatial reference or a spatial reference dictionary object for the \
                                         input geometries.

                                         If the raster type name is set to "UAV/UAS", the spatial reference of the
                                         output image collection will be determined by the raster type parameters defined.
    ----------------------               --------------------------------------------------------------------
    context                              Optional dict. The context parameter is used to provide additional input parameters.
    
                                         Syntax: {"image_collection_properties": {"imageCollectionType":"Satellite"},"byref":True}
                                        
                                         Use ``image_collection_properties`` key to set value for imageCollectionType.

                                         .. note::

                                            The "imageCollectionType" property is important for image collection that will later on be adjusted by orthomapping system service. 
                                            Based on the image collection type, the orthomapping system service will choose different algorithm for adjustment. 
                                            Therefore, if the image collection is created by reference, the requester should set this 
                                            property based on the type of images in the image collection using the following keywords. 
                                            If the imageCollectionType is not set, it defaults to "UAV/UAS"

                                         If ``byref`` is set to 'True', the data will not be uploaded. If it is not set, the default is 'False'

                                         The context parameter can also be used to specify whether to build overviews, \
                                         build footprints, to specify pixel value that represents the NoData etc.


                                         Example:

                                            | {"buildFootprints":True,                                            
                                            | "footprintsArguments":{"method":"RADIOMETRY","minValue":1,"maxValue":5,
                                            | "shrinkDistance":50,"skipOverviews":True,"updateBoundary":True,
                                            | "maintainEdge":False,"simplification":None,"numVertices":20,
                                            | "minThinnessRatio":0.05,"maxSliverSize":20,"requestSize":2000,
                                            | "minRegionSize":100},
                                            | "defineNodata":True,                                            
                                            | "noDataArguments":{"noDataValues":[500],"numberOfBand":99,"compositeValue":True},                                            
                                            | "buildOverview":True}

                                         The context parameter can be used to add new fields when creating \
                                         the image collection.


                                         Example:

                                            | {"fields": [{"name": "cloud_cover", "type": "Long"},
                                            | {"name": "cloud_shadow_count", "type": "Long"}]}
    ----------------------               --------------------------------------------------------------------
    gis                                  Keyword only parameter. Optional :class:`~arcgis.gis.GIS` object. The GIS on which this tool runs. If not specified, the active GIS is used.
    ----------------------               --------------------------------------------------------------------
    future                               Keyword only parameter. Optional boolean. If True, the result will be a GPJob object and 
                                         results will be returned asynchronously.
    ======================               ====================================================================

    :return: The imagery layer item

    """
    gis = arcgis.env.active_gis if gis is None else gis
    project_item = project._project_item
    resource_manager = project_item.resources
    oid = project.mission_count

    for f in gis.users.me.folders:
        if f._fid == project_item.ownerFolder:
            folder = f.properties
            break

    from arcgis.raster.analytics import create_image_collection

    if image_collection is None:
        image_collection = "image_collection" + "_" + _id_generator()

    if raster_type_name is None:
        raster_type_name = "UAV/UAS"

    output_collection = create_image_collection(
        image_collection=image_collection,
        input_rasters=image_list,
        raster_type_name=raster_type_name,
        raster_type_params=raster_type_params,
        out_sr=out_sr,
        context=context,
        gis=gis,
        folder=folder,
    )

    try:
        job_info = output_collection.properties
        job_response = {}
        if "jobUrl" in job_info:
            # Get the url of the Analysis job to track the status.

            job_url = job_info.get("jobUrl")
            params = {"f": "json"}
            job_response = gis._con.post(job_url, params)

        mission_json = {
            "items": {"imageCollection": {}},
            "jobs": {
                "imageCollection": {
                    "checked": True,
                    "progress": 100,
                    "success": True,
                },
                "adjustment": {"checked": False, "mode": "Quick"},
                "ortho": {"checked": False},
                "matchControlPoint": {"checked": False},
                "queryControlPoints": {"checked": False},
                "computeControlPoints": {"checked": False},
                "appendControlPoints": {"checked": False},
            },
            "rasterType": "UAV/UAS",
            "cameraInfo": {},
            "sourceData": {},
            "spatialReference": {},
            "adjustSettings": {},
            "processingSettings": {},
            "mapping": {
                "basemap": {
                    "title": "Topographic",
                    "baseMapLayers": [
                        {
                            "layerType": "ArcGISTiledMapServiceLayer",
                            "opacity": 1,
                            "visibility": True,
                            "url": "https://services.arcgisonline.com/ArcGIS/rest/services/World_Topo_Map/MapServer",
                        }
                    ],
                },
                "referenceData": [],
                "layers": {"visibilities": {"footprint": False}},
            },
            "projectVersion": 2,
            "createTS": "",
            "oid": oid,
            "gcsExtent": {},
        }

        mission_json.update(
            {
                "items": {
                    "imageCollection": {
                        "itemId": output_collection.itemid,
                        "url": output_collection.url,
                    }
                }
            }
        )

        mission_json["jobs"]["imageCollection"].update(
            {"messages": job_response["messages"]}
        )

        mission_json.update({"rasterType": raster_type_name})

        def get_camera_props(cam_props):
            cam_props_lower = dict((k.lower(), v) for k, v in cam_props.items())
            cam_dict = {
                "make": cam_props_lower.get("maker", ""),
                "focalLength": cam_props_lower.get("focallength", ""),
                "model": cam_props_lower.get("model", ""),
                "cols": cam_props_lower.get("columns", ""),
                "rows": cam_props_lower.get("rows", ""),
                "pixelSize": cam_props_lower.get("pixelsize", ""),
            }
            return cam_dict

        cam_props = {}
        try:
            if "cameraProperties" in raster_type_params:
                cam_props = get_camera_props(raster_type_params["cameraProperties"])
            else:
                try:
                    lyr = output_collection.layers[0]
                    cam_info = lyr.query_gps_info()
                    cam_props = cam_info["cameras"][0]
                except:
                    # older servers may not have query gps info rest end point
                    pass
        except:
            pass

        mission_json.update({"cameraInfo": cam_props})

        gps_data = []
        gps_info_list = ["name", "lat", "long", "alt", "acq"]

        if (
            raster_type_params is not None
            and isinstance(raster_type_params, dict)
            and "gps" in raster_type_params
        ):
            for ele in raster_type_params["gps"]:
                dict_gps = dict(zip(gps_info_list, ele))
                gps_data.append(dict_gps)
        if not gps_data:
            try:
                lyr = output_collection.layers[0]
                gps_info = lyr.query_gps_info()["images"]
                for img_info in gps_info:
                    from arcgis.raster._util import _to_datetime

                    acq = _to_datetime(img_info["acquisitionDate"]).isoformat()
                    gps = img_info["gps"]
                    name = img_info["name"]
                    lat = gps["latitude"]
                    long = gps["longitude"]
                    alt = gps["altitude"]
                    gps_val = [name, lat, long, alt, acq]
                    dict_gps = dict(zip(gps_info_list, gps_val))
                    gps_data.append(dict_gps)
            except:
                # older servers may not have query gps info rest end point
                pass
        from datetime import datetime

        try:
            lyr = output_collection.layers[0]
            query_output = lyr.query(
                where="OBJECTID=1",
                out_fields="AcquisitionDate",
                return_all_records=False,
                return_geometry=False,
            )
            acq_date = query_output["features"][0]["attributes"]["AcquisitionDate"]
            from datetime import datetime

            ts = int(acq_date) / 1000
            flight_date = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
        except:
            dt = datetime.now()
            flight_date = dt.strftime("%Y-%m-%d")

        image_count = output_collection.layers[0].query(return_count_only=True)

        mission_json.update(
            {
                "sourceData": {
                    "gps": gps_data,
                    "imageCount": image_count,
                    "flightDate": flight_date,
                }
            }
        )

        project_id = datetime.now().strftime("%Y%m%d%H%M%S")
        mission_json.update({"projectId": project_id})

        ts = int(datetime.timestamp(datetime.now())) * 1000
        mission_json.update({"createTS": ts})

        ## Set extent
        try:
            gcs_extent = {}
            extent_arr = output_collection.extent
            if extent_arr is not None:
                gcs_extent = {
                    "xmin": extent_arr[0][0],
                    "ymin": extent_arr[0][1],
                    "xmax": extent_arr[1][0],
                    "ymax": extent_arr[1][1],
                    "spatialReference": {"wkid": 4326},
                }
        except:
            gcs_extent = {}

        projected_extent = {}
        try:
            projected_extent = dict(output_collection.layers[0].extent)
        except:
            projected_extent = {}

        mission_json.update(
            {"gcsExtent": gcs_extent, "projectedExtent": projected_extent}
        )

        try:
            coverage_area = output_collection.layers[0].query_boundary()["area"]
            mission_json.update({"coverage": coverage_area})
        except:
            pass

        import uuid

        if mission_name is None:
            mission_name = "mission" + "_" + _id_generator()
        fname = "%s.json" % mission_name

        try:
            resource_manager.add(
                file_name=fname,
                text=mission_json,
                folder_name="flights",
                properties={
                    "oid": oid,
                    "imageCount": image_count,
                    "createTS": ts,
                    "items": [
                        {
                            "product": "imageCollection",
                            "id": output_collection.id,
                            "created": True,
                        }
                    ],
                    "gcpItems": [],
                    "baStatus": "succeeded",
                },
            )
            prj_data = project_item.get_data()
            flights_list = prj_data.get("flights", [])
            flights_list.append({"oid": oid, "createTS": ts})
            prj_data.update({"flights": flights_list})
            prj_data.update({"projectVersion": 2, "rasterType": raster_type_name})

            project_properties = project_item.properties
            flight_count = project_properties.get("flightCount", 0)
            flight_count = flight_count + 1

            image_count_ex = project_properties.get("imageCount", 0)
            image_count_ex = image_count + image_count_ex

            project_properties.update(
                {"flightCount": flight_count, "imageCount": image_count}
            )
            project_item.update(
                item_properties={"properties": project_properties},
                data=json.dumps(prj_data),
            )

            # project_item.update(data=json.dumps(prj_data))
        except:
            raise RuntimeError("Error adding the mission")
    except:
        raise RuntimeError("Error updating the mission JSON")
    return output_collection, mission_name


###################################################################################################
###
### PUBLIC API
###
###################################################################################################
def is_supported(gis=None):
    """
    Returns True if the GIS supports orthomapping. If a gis isn't specified,
    checks if :meth:`~arcgis.env.active_gis` supports raster analytics
    """
    gis = arcgis.env.active_gis if gis is None else gis
    if "orthoMapping" in gis.properties.helperServices:
        return True
    else:
        return False


###################################################################################################
## Compute Sensor model
###################################################################################################
def compute_sensor_model(
    image_collection,
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

    ==================     ====================================================================
    **Parameter**           **Description**
    ------------------     --------------------------------------------------------------------
    image_collection       Required, the input image collection on which to compute
                           the sensor model.
                           The image_collection can be a Mission object, an image service URL or portal Item or a datastore URI.

                           The image_collection must exist.
    ------------------     --------------------------------------------------------------------
    mode                   Optional string.  the mode to be used for bundle block adjustment
                           Only the following modes are supported:

                           - 'Quick' : Computes tie points and adjustment at 8x of the source imagery resolution

                           - 'Full'  : adjust the images in Quick mode then at 1x of the source imagery resolution

                           - 'Refine' : adjust the image at 1x of the source imagery resolution

                           By default, 'Quick' mode is applied to compute the sensor model.
    ------------------     --------------------------------------------------------------------
    location_accuracy      Optional string. this option allows users to specify the GPS location accuracy level of the
                           source image. It determines how far the underline tool will search for neighboring
                           matching images, then calculate tie points and compute adjustments.

                           Possible values for location_accuracy are:

                           - 'VeryHigh'    : Imagery was collected with a high-accuracy, differential GPS, such as RTK or PPK. This option will hold image locations fixed during block adjustment

                           - 'High'    : GPS accuracy is 0 to 10 meters, and the tool uses a maximum of 4 by 3 images

                           - 'Medium'  : GPS accuracy of 10 to 20 meters, and the tool uses a maximum of 4 by 6 images

                           - 'Low'     : GPS accuracy of 20 to 50 meters, and the tool uses a maximum of 4 by 12 images

                           - 'VeryLow' : GPS accuracy is more than 50 meters, and the tool uses a maximum of 4 by 20 images

                           The default location_accuracy is 'High'
    ------------------     --------------------------------------------------------------------
    context                Optional dictionary. The context parameter is used to configure additional client settings
                           for block adjustment. The supported configurable parameters are for compute mosaic dataset
                           candidates after the adjustment.

                           The possible keys for the context dictionary are:

                           - parallelProcessingFactor : The specified number or percentage of processes will be used for the analysis. The default value is "50%".

                           - computeCandidate : Indicates whether Compute Mosaic Candidates will run inside the service task. Default value is False.

                           - maxOverlap : Specifies the maximum area overlap for running the Compute Mosaic Candidates tool inside the task. The default value is 0.6 .

                           - maxLoss : Specifies the maximum area loss allowed for running the Compute Mosaic Candidates tool inside the task. The default value is 0.05 .

                           - initPointResolution : Specifies the initial tie point resolution for running the Compute Camera Model tool inside the task. The default value is 8.0 .

                           - maxResidual : Specifies the maximum residual for running the Compute Block Adjustment and Compute Camera Model tools inside the task. The default value is 5.0 .

                           - adjustOptions : Specifies the adjustment options for running the Compute Block Adjustment tool inside the task. The default value is empty.

                           - pointSimilarity : Specifies the similarity for running the Compute Tie Points tool inside the task. The default value is MEDIUM.

                           - pointDensity : Specifies the point density for running the Compute Tie Points tool inside the task. The default value is MEDIUM.

                           - pointDistribution : Specifies the point distribution for running the Compute Tie Points tool inside the task. The default value is RANDOM.

                           - polygonMask : Specifies the input mask for running the Compute Tie Points tool inside the task. Default value is empty.

                           - regenTiepoints : Indicates whether Compute Tie Points will rerun inside the service task if tie points feature class exists. The default value is True.

                           Example:

                               {
                               "computeCandidate": False,
                               "maxOverlap": 0.6,
                               "maxLoss": 0.05,
                               }
    ------------------     --------------------------------------------------------------------
    gis                    Optional :class:`~arcgis.gis.GIS` . The GIS on which this tool runs. If not specified, the active GIS is used.
    ==================     ====================================================================

    :return:
        The imagery layer url

    """

    gis = arcgis.env.active_gis if gis is None else gis
    update_flight_json = False
    flight_json_details = {}
    from ._mission import Mission

    if isinstance(image_collection, Mission):
        mission = image_collection
        image_collection = image_collection.image_collection
        update_flight_json = True

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
                k: context_new[k.lower()] for k in adj_keys if k.lower() in context_new
            }
            adj_dict.update({"locationAccuracy": location_accuracy})
        adj_dict.update({"mode": mode})
        flight_json_details = {
            "update_flight_json": update_flight_json,
            "mission": mission,
            "item_name": "adjustment",
            "adjust_settings": adj_dict,
        }

    return gis._tools.orthomapping.compute_sensor_model(
        image_collection=image_collection,
        mode=mode,
        location_accuracy=location_accuracy,
        context=context,
        future=future,
        flight_json_details=flight_json_details,
        **kwargs,
    )


###################################################################################################
## Alter processing states
###################################################################################################
def alter_processing_states(
    image_collection,
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

    ==================     ====================================================================
    **Parameter**           **Description**
    ------------------     --------------------------------------------------------------------
    image_collection       Required, This is the image collection that will be adjusted.

                           The image_collection can be a Mission object, an image service URL or portal Item or a datastore URI.

                           The image_collection must exist.
    ------------------     --------------------------------------------------------------------
    new_states             Required dictionary. The state to set on the image_collection

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
    ------------------     --------------------------------------------------------------------
    gis                    Optional :class:`~arcgis.gis.GIS` . The GIS on which this tool runs. If not specified, the active GIS is used.
    ==================     ====================================================================

    :return:
        The result will be the newly set states dictionary

    """
    gis = arcgis.env.active_gis if gis is None else gis
    from ._mission import Mission

    if isinstance(image_collection, Mission):
        mission = image_collection
        image_collection = image_collection.image_collection

    return gis._tools.orthomapping.alter_processing_states(
        image_collection=image_collection,
        new_states=new_states,
        future=future,
        **kwargs,
    )


###################################################################################################
## Get processing states
###################################################################################################
def get_processing_states(
    image_collection,
    *,
    gis: Optional[GIS] = None,
    future: bool = False,
    **kwargs,
):
    """
    Retrieve the processing states of the image collection

    ==================     ====================================================================
    **Parameter**           **Description**
    ------------------     --------------------------------------------------------------------
    image_collection       Required, This is the image collection that will be adjusted.

                           The image_collection can be a Mission object, an image service URL or portal Item or a datastore URI.

                           The image_collection must exist.
    ------------------     --------------------------------------------------------------------
    gis                    Optional :class:`~arcgis.gis.GIS` . The GIS on which this tool runs. If not specified, the active GIS is used.
    ==================     ====================================================================

    :return:
        The result will be the newly set states dictionary

    """

    gis = arcgis.env.active_gis if gis is None else gis
    from ._mission import Mission

    if isinstance(image_collection, Mission):
        mission = image_collection
        image_collection = image_collection.image_collection

    return gis._tools.orthomapping.get_processing_states(
        image_collection=image_collection, future=future, **kwargs
    )


###################################################################################################
## Match control points
###################################################################################################
def match_control_points(
    image_collection,
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
    
    ==================     ====================================================================
    **Parameter**           **Description**
    ------------------     --------------------------------------------------------------------
    image_collection       Required, the input image collection that will be adjusted.

                           The image_collection can be a Mission object, an image service URL or portal Item or a datastore URI.
                            
                           The image_collection must exist.
    ------------------     --------------------------------------------------------------------
    control_points         Required, a list of control point sets objects.

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
    ------------------     --------------------------------------------------------------------
    similarity             Optional string. Choose the tolerance level for your control point matching. 

                           - Low- The similarity tolerance for finding control points will be low. \
                           This option will produce the most control points, \
                           but some may have a higher level of error. 

                           - Medium - The similarity tolerance for finding control points will be medium.
                           
                           - High - The similarity tolerance for finding control points will be high. \
                           This option will produce the least number of control points, \
                           but each matching pair will have a lower level of error. This is the default. 
    ------------------     --------------------------------------------------------------------
    context                Optional dictionary.Additional settings such as the input control points 
                           spatial reference can be specified here. 

                           For Example:

                                {"groundControlPointsSpatialReference": {"wkid": 3459}, "imagePointSpatialReference": {"wkid": 3459}}

                           Note: The ground control points spatial reference and image point spatial reference 
                           spatial reference set in the context parameter is to decide the returned point set's 
                           ground control points spatial reference and image point spatial reference. 
                           If these two parameters are not set here, the tool will use the spatial reference 
                           defined in the input point set. And if no spatial reference is defined in the point set,
                           then the default ground control points coordinates are in lon/lat and image points 
                           coordinates are in image coordinate system. 
    ------------------     --------------------------------------------------------------------
    gis                    Optional :class:`~arcgis.gis.GIS` . The GIS on which this tool runs. If not specified, the active GIS is used.
    ==================     ====================================================================

    :return:
        A list of dictionary objects

    """
    gis = arcgis.env.active_gis if gis is None else gis
    update_flight_json = False
    flight_json_details = {}
    from ._mission import Mission

    if isinstance(image_collection, Mission):
        mission = image_collection
        image_collection = image_collection.image_collection
        update_flight_json = True

        flight_json_details = {
            "update_flight_json": update_flight_json,
            "mission": mission,
            "item_name": "matchControlPoint",
        }

    return gis._tools.orthomapping.match_control_points(
        image_collection=image_collection,
        control_points=control_points,
        similarity=similarity,
        context=context,
        future=future,
        flight_json_details=flight_json_details,
        **kwargs,
    )


###################################################################################################
## Color Correction
###################################################################################################
def color_correction(
    image_collection,
    color_correction_method: str,
    dodging_surface_type: str,
    target_image=None,
    context: Optional[dict[str, Any]] = None,
    *,
    gis: Optional[GIS] = None,
    future: bool = False,
    **kwargs,
):
    """
    Color balance the image collection. 
    Refer to the `Color Balance Mosaic Dataset <https://pro.arcgis.com/en/pro-app/tool-reference/data-management/color-balance-mosaic-dataset.htm>`_ GP tool for
    documentation on color balancing mosaic datasets.


    ====================================     ====================================================================
    **Parameter**                             **Description**
    ------------------------------------     --------------------------------------------------------------------
    image_collection                         Required. This is the image collection that will be adjusted.

                                             The image_collection can be a Mission object, an image service URL or portal Item or a datastore URI.
                            
                                             The image_collection must exist.
    ------------------------------------     --------------------------------------------------------------------
    color_correction_method                  Required string. This is the method that will be used for color
                                             correction computation. The available options are:

                                             - Dodging-Change each pixel's value toward a target color. \
                                             With this technique, you must also choose \
                                             the type of target color surface, which \
                                             affects the target color. Dodging tends \
                                             to give the best result in most cases. 

                                             - Histogram-Change each pixel's value according \
                                             to its relationship with a target histogram. \
                                             The target histogram can be derived from \
                                             all of the rasters, or you can specify a \
                                             raster. This technique works well when \
                                             all of the rasters have a similar histogram.
                                    
                                             - Standard_Deviation-Change each of the pixel's \
                                             values according to its relationship with the \
                                             histogram of the target raster, within one \
                                             standard deviation. The standard deviation can be \
                                             calculated from all of the rasters in the mosaic \
                                             dataset, or you can specify a target raster. \
                                             This technique works best when all of the \
                                             rasters have normal distributions.
    ------------------------------------     --------------------------------------------------------------------
    dodging_surface_type                     Required string.When using the Dodging balance method, 
                                             each pixel needs a target color, which is determined by 
                                             the surface type.

                                             - Single_Color-Use when there are only a small \
                                             number of raster datasets and a few different \
                                             types of ground objects. If there are too many \
                                             raster datasets or too many types of ground \
                                             surfaces, the output color may become blurred. \
                                             All the pixels are altered toward a single \
                                             color point-the average of all pixels. 
                                    
                                             - Color_Grid- Use when you have a large number \
                                             of raster datasets, or areas with a large \
                                             number of diverse ground objects. Pixels \
                                             are altered toward multiple target colors, \
                                             which are distributed across the mosaic dataset. 

                                             - First_Order- This technique tends to create a \
                                             smoother color change and uses less storage in \
                                             the auxiliary table, but it may take longer to \
                                             process compared to the color grid surface. \
                                             All pixels are altered toward many points obtained \
                                             from the two-dimensional polynomial slanted plane. 

                                             - Second_Order-This technique tends to create a \
                                             smoother color change and uses less storage in \
                                             the auxiliary table, but it may take longer to \
                                             process compared to the color grid surface. \
                                             All input pixels are altered toward a set of \
                                             multiple points obtained from the two-dimensional \
                                             polynomial parabolic surface. 

                                             - Third_Order-This technique tends to create a \
                                             smoother color change and uses less storage in \
                                             the auxiliary table, but it may take longer to \
                                             process compared to the color grid surface. \
                                             All input pixels are altered toward multiple \
                                             points obtained from the cubic surface.
    ------------------------------------     --------------------------------------------------------------------
    target_image                             Optional. The image service you want to use to color balance 
                                             the images in the image collection.
                                             It can be a portal Item or an image service URL or a URI
    ------------------------------------     --------------------------------------------------------------------
    context                                  Optional dictionary. It contains additional settings that allows
                                             users to customize the statistics computation settings.

                                             Example:

                                                {"skipRows": 10, "skipCols": 10, "reCalculateStats": "OVERWRITE"}
    ------------------------------------     --------------------------------------------------------------------
    gis                                      Optional :class:`~arcgis.gis.GIS` . the GIS on which this tool runs. If not specified, the active GIS is used.
    ====================================     ====================================================================

    :return:
        The imagery layer url

    """
    gis = arcgis.env.active_gis if gis is None else gis
    update_flight_json = False
    flight_json_details = {}
    from ._mission import Mission

    if isinstance(image_collection, Mission):
        mission = image_collection
        image_collection = image_collection.image_collection
        update_flight_json = True

        flight_json_details = {
            "update_flight_json": update_flight_json,
            "mission": mission,
            "item_name": "colorCorrection",
        }

    return gis._tools.orthomapping.compute_color_correction(
        image_collection=image_collection,
        color_correction_method=color_correction_method,
        dodging_surface=dodging_surface_type,
        target_image=target_image,
        context=context,
        future=future,
        flight_json_details=flight_json_details,
        **kwargs,
    )


###################################################################################################
## Compute Control Points
###################################################################################################
def compute_control_points(
    image_collection,
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
    **Parameter**                            **Description**
    ------------------------------------    --------------------------------------------------------------------
    image_collection                        Required. This is the image collection that will be adjusted.

                                            The image_collection can be a Mission object, an image service URL or portal Item or a datastore URI.
                            
                                            The image_collection must exist.
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
    update_flight_json = False
    flight_json_details = {}
    from ._mission import Mission

    if isinstance(image_collection, Mission):
        mission = image_collection
        image_collection = image_collection.image_collection
        update_flight_json = True

        flight_json_details = {
            "update_flight_json": update_flight_json,
            "mission": mission,
            "item_name": "computeControlPoints",
        }

    return gis._tools.orthomapping.compute_control_points(
        image_collection=image_collection,
        reference_image=reference_image,
        image_location_accuracy=image_location_accuracy,
        context=context,
        future=future,
        flight_json_details=flight_json_details,
        **kwargs,
    )


###################################################################################################
## Compute Seamlines
###################################################################################################
def compute_seamlines(
    image_collection,
    seamlines_method: str,
    context: Optional[dict[str, Any]] = None,
    *,
    gis: Optional[GIS] = None,
    future: bool = False,
    **kwargs,
):
    """
    Compute seamlines on the image collection. This service tool is used to compute
    seamlines for the image collection, usually after the image collection has been
    block adjusted. Seamlines are helpful for generating the seamless mosaicked 
    display of overlapped images in image collection. The seamlines are computed
    only for candidates that will eventually be used for generating the result
    ortho-mosaicked image.

    `Build Seamlines <https://pro.arcgis.com/en/pro-app/tool-reference/data-management/build-seamlines.htm>`_

    ==================     ====================================================================
    **Parameter**           **Description**
    ------------------     --------------------------------------------------------------------
    image_collection       Required, the input image collection that will be adjusted.
                           The image_collection can be a Mission object, an image service URL or portal Item or a datastore URI.

                           The image_collection must exist.
    ------------------     --------------------------------------------------------------------
    seamlines_method       Required string. These are supported methods for generated seamlines for the image collection.
    
                           - VORONOI-Generate seamlines using the area Voronoi diagram.

                           - DISPARITY-Generate seamlines based on the disparity images of stereo pairs.

                           - GEOMETRY - Generate seamlines for overlapping areas based on the intersection \
                             of footprints. Areas with no overlapping imagery will merge the footprints.

                           - RADIOMETRY - Generate seamlines based on the spectral patterns of features \
                             within the imagery.

                           - EDGE_DETECTION - Generate seamlines over intersecting areas based on the \
                             edges of features in the area.

                             This method can avoid seamlines cutting through buildings.
    ------------------     --------------------------------------------------------------------
    context                Optional dictionary. Context contains additional settings that allows users to customize
                           the seamlines generation.
                           
                           The possible keys for the context dictionary are:

                           - parallelProcessingFactor : The specified number or percentage of processes will be used for the analysis.
                           
                           - computeCandidate : Indicates whether “Compute Mosaic Candidates” will run inside the service task. Default value is False.
                           
                           - maxOverlap : Defines the Maximum Area Overlap for running Compute Mosaic Candidates tool inside the task. Default value is 0.6.
                            
                           - maxLoss : It is Maximum Area Loss Allowed for running Compute Mosaic Candidates tool inside the task. Default value is 0.05.
                           
                           - minRegionSize : Any seamline polygons smaller than this specified threshold will be removed in the seamline result.
                            
                           - pixelSize : Generates seamlines for raster datasets that fall within the specified spatial resolution size.
                           
                           - blendType : Determine how to blend one image into another (Both , Inside , or Outside ) over the seamlines. Inside blends pixels inside the seamline, while Outside blends outside the seamline. Both will blend pixels on either side of the seamline.
                           
                           - blendWidth : Specifies how many pixels will be blended relative to the seamline. Blending (feathering) occurs along a seamline between pixels of overlapping images.
                           
                           - blendUnit : Specifies the unit of measurement for blendWidth . Pixels measures using the number of pixels, and Ground measures using the same units as the image collection.
                           
                           - requestSizeType : Sets the units for requestSize . Pixels modifies requestSize based on the pixel size. This resamples the closest image based on the raster pixel size. Pixel scaling factor modifiers requestSize by specifying a scaling factor. This operation resamples the closest image by multiplying the raster pixel size with the pixel size factor.
                           
                           - requestSize : Specifies the number of columns and rows for resampling. Though the maximum value is 5,000, this value can increase or decreased based on the complexity of your raster data. A greater image resolution provides more detail in the raster dataset but increases the processing time.
                           
                           - minThinnessRatio : Defines how thin a polygon can be before its considered a sliver. This is based on a scale from 0 to 1.0, where a value of 0.0 represents a polygon that's almost a straight line, and a value of 1.0 represents a polygon that's a circle.
                           
                           - maxSliverSize : Defines how large a Sliver can be before its considered a polygon. This uses the same scale as minThinnessRatio .
                           
                           Example:

                               {"minRegionSize": 100,
                               "pixelSize": "",
                               "blendType": "Both",
                               "blendWidth": None,
                               "blendUnit": "Pixels",
                               "requestSizeType": "Pixels",
                               "requestSize": 1000,
                               "minThinnessRatio": 0.05,
                               "maxSilverSize": 20
                               }

                           Allowed keys are:
                           "minRegionSize", "pixelSize", "blendType", "blendWidth", 
                           "blendUnit", "requestSizeType", "requestSize", 
                           "minThinnessRatio", "maxSilverSize"
    ------------------     --------------------------------------------------------------------
    gis                    Optional :class:`~arcgis.gis.GIS` . The GIS on which this tool runs. If not specified, the active GIS is used.
    ==================     ====================================================================

    :return:
        The Imagery layer url

    """

    gis = arcgis.env.active_gis if gis is None else gis
    update_flight_json = False
    flight_json_details = {}
    from ._mission import Mission

    if isinstance(image_collection, Mission):
        mission = image_collection
        image_collection = image_collection.image_collection
        update_flight_json = True

        flight_json_details = {
            "update_flight_json": update_flight_json,
            "mission": mission,
            "item_name": "seamline",
        }

    return gis._tools.orthomapping.compute_seamlines(
        image_collection=image_collection,
        seamlines_method=seamlines_method,
        context=context,
        future=future,
        flight_json_details=flight_json_details,
        **kwargs,
    )


###################################################################################################
## Edit control points
###################################################################################################
def edit_control_points(
    image_collection,
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

    ==================     ====================================================================
    **Parameter**           **Description**
    ------------------     --------------------------------------------------------------------
    image_collection       Required.
                           The image_collection can be a Mission object, an image service URL or portal Item or a datastore URI.

                           The image_collection must exist.
    ------------------     --------------------------------------------------------------------
    control_points         Required, a list of control point sets objects.

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


    ------------------     --------------------------------------------------------------------
    gis                    Optional :class:`~arcgis.gis.GIS` . The GIS on which this tool runs. If not specified, the active GIS is used.
    ==================     ====================================================================

    :return:
        The Imagery layer url

    """

    gis = arcgis.env.active_gis if gis is None else gis
    update_flight_json = False
    flight_json_details = {}
    from ._mission import Mission

    if isinstance(image_collection, Mission):
        mission = image_collection
        image_collection = image_collection.image_collection
        update_flight_json = True

        flight_json_details = {
            "update_flight_json": update_flight_json,
            "mission": mission,
            "item_name": "appendControlPoints",
        }

    return gis._tools.orthomapping.edit_control_points(
        image_collection=image_collection,
        input_control_points=control_points,
        future=future,
        flight_json_details=flight_json_details,
        **kwargs,
    )


###################################################################################################
## Generate DEM
###################################################################################################
def generate_dem(
    image_collection,
    out_dem: str,
    cell_size: dict[str, int],
    surface_type: str,
    matching_method: Optional[str] = None,
    context: Optional[dict[str, Any]] = None,
    classify_ground_options: Optional[dict[str, Any]] = None,
    *,
    gis: Optional[GIS] = None,
    future: bool = False,
    **kwargs,
):
    """
    Generate a DEM from the image collection. Refer to `Interpolate From Point Cloud <http://pro.arcgis.com/en/pro-app/tool-reference/data-management/interpolate-from-point-cloud.htm>`_
    GP tool for more documentation

    
    =======================     ====================================================================
    **Parameter**               **Description**
    -----------------------     --------------------------------------------------------------------
    image_collection            Required. The input image collection that will be used
                                to generate the DEM from.
                                The image_collection can be a Mission object, an image service URL or portal Item or a datastore URI.

                                The image_collection must exist.
    -----------------------     --------------------------------------------------------------------
    out_dem                     This is the output digital elevation model.
                                It can be a url, uri, portal item, or string representing the name of output dem 
                                (either existing or to be created.)
                                Like Raster Analysis services, the service can be an existing multi-tenant service URL.
    -----------------------     --------------------------------------------------------------------
    cell_size                   Required, The cell size of the output raster dataset. This is a single numeric input. 
                                Rectangular cell size such as {"x": 10, "y": 10} is not supported. 
                                The cell size unit will be the unit used by the image collection's spatial reference.
    -----------------------     --------------------------------------------------------------------
    surface_type                Required string. Create a digital terrain model or a digital surface model. Refer
                                to "surface_type" parameter of the GP tool.
                                
                                The available choices are:

                                - DTM - Digital Terrain Model, the elevation is only the elevation of the bare earth, not including structures above the surface.

                                - DSM - Digital Surface Model, the elevation includes the structures above the surface, for example, buildings, trees, bridges.
    -----------------------     --------------------------------------------------------------------
    matching_method             Optional string. The method used to generate 3D points. 

                                - ETM-A feature-based stereo matching that uses the Harris operator to \
                                detect feature points. It is recommended for DTM generation.  

                                - SGM- Produces more points and more detail than the ETM method. It is \
                                suitable for generating a DSM for urban areas. This is more \
                                computationally intensive than the ETM method1.  

                                - MVM (Multi-view image matching (MVM) - is based on the SGM matching method followed by a fusion step in which \
                                the redundant depth estimations across single stereo model are merged. \
                                It produces dense 3D points and is computationally efficient

                                References:  
                                Heiko Hirschmuller et al., "Memory Efficient Semi-Global Matching," 
                                ISPRS Annals of the Photogrammetry, Remote Sensing and Spatial 
                                Information Sciences, Volume 1-3, (2012): 371-376. 

                                Refer to the documentation
                                of "matching_method" parameter of the `Generate Point Cloud <http://pro.arcgis.com/en/pro-app/tool-reference/data-management/generate-point-cloud.htm>`_
                                GP tool
    -----------------------     --------------------------------------------------------------------
    context                     Optional dictionary. Additional allowed point cloud generation parameter and DEM 
                                interpolation parameter can be assigned here.  
                                
                                This dictionary can contain the following keys:

                                - parallelProcessingFactor : Specifies the number or percentage of
                                    processes will be used for the analysis. The default value is 50% .
                                
                                - maxObjectSize : A search radium within surface objects, such as
                                    buildings and trees, will be identified. It's the linear size in map units.
                                
                                - groundSpacing : The ground spacing, in meters, at which the 3D points are generated.
                                
                                - minAngle : The value, in degrees, that defines the minimum
                                    intersection angle the stereo pair must meet.
                                
                                - maxAngle : The value, in degrees, that defines the maximum
                                    intersection angle the stereo pair must meet.
                                
                                - minOverlap : Specifies a minimum overlap threshold that is acceptable,
                                    which is a percentage of overlap between a pair of images. Image pairs
                                    with overlap areas smaller than this threshold will receive a score of 0
                                    for this criteria and will descend in the ordered list.
                                    The range of values is from 0 to 1.
                                
                                - maxOmegaPhiDif : Specifies the maximum threshold for the Omega/Phi
                                difference between the image pair. The Omega and Phi values for the image
                                pair are compared, and a difference greater than this threshold will receive
                                a score of 0 and will descend in the ordered list.
                                
                                - maxGSDDif : Specifies the maximum allowable threshold for the ground sample
                                distance (GSD) between two images in a pair. The resolution ration between the
                                two images will be compared with the threshold value. Image pairs with a GSD
                                greater than this threshold will receive a score of 0 and will descend in the ordered list.
                                
                                - numImagePairs : The number of pairs used to generate 3D points.
                                
                                - adjQualityThreshold : Specifies the minimum acceptable adjustment quality.
                                The threshold value will be compared to the quality value stored within the
                                stereo model. Image pairs with an adjustment quality less than the specified
                                threshold will receive a score of 0 and will descend in the ordered list.
                                The range of values for the threshold is between 0 and 1.
                                
                                - regenPointCloud : Regenerates the 3D point cloud when set to True.   
                                
                                - pointCloudFolder : The point cloud folder to use. This can be one of "DSM", "DTM", "LAS".
                                
                                For Example:

                                        | Point cloud generation parameters -  
                                        | {"maxObjectSize": 50, 
                                        | "groundSpacing": None, 
                                        | "minAngle": 10, 
                                        | "maxAngle": 70, 
                                        | "minOverlap": 0.6, 
                                        | "maxOmegaPhiDif": 8, 
                                        | "maxGSDDif": 2, 
                                        | "numImagePairs": 2, 
                                        | "adjQualityThreshold": 0.2, 
                                        | "regenPointCloud": False,
                                        | } 
                                        | 
                                        | DEM interpolation parameters -  
                                        | {"method": "TRIANGULATION", 
                                        | "smoothingMethod": "GAUSS5x5", 
                                        | "applyToOrtho": True, 
                                        | "fillDEM": "``https://....``"
                                        | "pointCloudFolder": "DSM"
                                        | } 
        
                                Note:  
                                The "applyToOrtho" flag can apply the generated DEM back into the 
                                mosaic dataset's geometric function to achieve more accurate 
                                orthorectification result.  
                                The "fillDEM" flag allows the user to specify an elevation service URL as 
                                background elevation to fill the area when elevation model pixels cannot be 
                                interpolated from the point cloud.  
    -----------------------     --------------------------------------------------------------------
    classify_ground_options     Optional dict. Classify ground points from the input LAS data. This can be used when the surface type is DTM.
                                
                                The dictionary can contain the following keys:

                                - Classify : the method to use to detect ground points. This value can be one of "standard", "conservative" or "aggressive".
                                  
                                  - standard: This method has a tolerance for slope variation that allows it to capture gradual undulations in the ground's
                                    topography that would typically be missed by the conservative option but not capture the type of sharp reliefs that
                                    would be captured by the aggressive option. This is the default.
                                  
                                  - conservative: When compared to other options, this method uses a tighter restriction on the variation of the ground's
                                    slope that allows it to differentiate the ground from low-lying vegetation such as grass and shrubbery. It is best suited
                                    for topography with minimal curvature.
                                  
                                  - aggressive: This method detects ground areas with sharper reliefs, such as ridges and hill tops, that may be ignored by
                                    the standard option. This method is best used in a second iteration of this tool with the ReuseGround option set to
                                    1. Avoid using this method in urban areas or flat, rural areas, as it may result in the misclassification of taller
                                    objects — such as utility towers, vegetation, and portions of buildings — as ground.

                                - LowNoise : the distance below the ground that will be used to classify the point to be low-noise points. The unit is meter.
                                  The default value is 0.25 meter.

                                - HighNoise : the distance above the ground that will be used to classify the point to be high-noise points. The unit is meter.
                                  The default value is 100 meter.

                                - ReuseGround : specifies whether existing ground points will be reclassified or reused. 0 mean reclassify and 1 indicating reuse.
                                  The default value is 0.

                                - ReuseLowNoise : specifies whether existing low-noise points will be reused or reclassified. 0 mean reclassify and 1 mean reuse.
                                  The default value is 0.
                                
                                - ReuseHighNoise : specifies whether existing high-noise points will be reused or reclassified. 0 mean reclassify and 1 mean reuse.
                                  The default value is 0.
    -----------------------     --------------------------------------------------------------------
    gis                         Optional :class:`~arcgis.gis.GIS` . The GIS on which this tool runs. If not specified, the active GIS is used.
    =======================     ====================================================================

    :return:
        The DEM layer item

    """

    gis = arcgis.env.active_gis if gis is None else gis
    update_flight_json = False
    flight_json_details = {}
    from ._mission import Mission

    if isinstance(image_collection, Mission):
        mission = image_collection
        image_collection = image_collection.image_collection
        update_flight_json = True

        if kwargs is not None:
            if "folder" in kwargs:
                folder = kwargs["folder"]
            else:
                for f in gis.users.me.folders:
                    if f._fid == image_collection.ownerFolder:
                        folder = f.properties
                        break
            kwargs.update({"folder": folder})

        dem_dict = {}
        if isinstance(context, dict):
            context_new = {k.lower(): v for k, v in context.items()}

            point_cloud_keys = [
                "maxObjectSize",
                "groundSpacing",
                "minAngle",
                "maxAngle",
                "minOverlap",
                "maxOmegaPhiDif",
                "maxGSDDif",
                "numImagePairs",
                "adjQualityThreshold",
            ]
            point_cloud_dict = {
                "pointCloud": {
                    k: context_new[k.lower()]
                    for k in point_cloud_keys
                    if k.lower() in context_new
                }
            }
            point_cloud_dict["pointCloud"].update({"method": matching_method})
            interpolation_keys = [
                "pixelSize",
                "pixelSizeUnit",
                "method",
                "smoothingMethod",
            ]
            interpolation_dict = {
                "interpolation": {
                    k: context_new[k.lower()]
                    for k in interpolation_keys
                    if k.lower() in context_new
                }
            }

            apply_to_ortho = context_new.get("applytoortho", False)
            dem_dict = {"applyToOrtho": apply_to_ortho}
            dem_dict.update(point_cloud_dict)
            dem_dict.update(interpolation_dict)
        flight_json_details = {
            "update_flight_json": update_flight_json,
            "mission": mission,
            "item_name": surface_type.lower(),
            "processing_states": dem_dict,
        }

    return gis._tools.orthomapping.generate_dem(
        image_collection=image_collection,
        cell_size=cell_size,
        output_dem=out_dem,
        surface_type=surface_type,
        matching_method=matching_method,
        context=context,
        future=future,
        flight_json_details=flight_json_details,
        classify_ground_options=classify_ground_options,
        **kwargs,
    )


###################################################################################################
## Generate orthomosaic
###################################################################################################
def generate_orthomosaic(
    image_collection,
    out_ortho,
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

    ===================================    ====================================================================
    **Parameter**                           **Description**
    -----------------------------------    --------------------------------------------------------------------
    image_collection                       Required. The input image collection that will be used
                                           to generate the ortho-mosaic from.
                                           The image_collection can be a Mission object, an image service URL or portal Item or a datastore URI.

                                           The image_collection must exist.
    -----------------------------------    --------------------------------------------------------------------
    out_ortho                               Required. This is the ortho-mosaicked image converted from the image
                                            collection after the block adjustment.
                                            It can be a url, uri, portal item, or string representing the name of output dem
                                            (either existing or to be created.)
                                            Like Raster Analysis services, the service can be an existing multi-tenant service URL.
    -----------------------------------    --------------------------------------------------------------------
    regen_seamlines                        Optional, boolean.
                                           Choose whether to apply seamlines before the orthomosaic image generation or not.
                                           The seamlines will always be regenerated if this parameter is set to True.
                                           The user can set the seamline options through the context parameter.
                                           If the seamline generation options are not set, the default will be used.

                                           Default value is True
    -----------------------------------    --------------------------------------------------------------------
    recompute_color_correction              Optional, boolean.
                                            Choose whether to apply color correction settings to the output ortho-image or not.
                                            Color correction will always be recomputed if this option is set to True.
                                            The user can configure the compute color correction settings through the context parameter.
                                            If there is no color collection setting, the default will be used.

                                            Default value is True
    -----------------------------------    --------------------------------------------------------------------
    context                                Optional dictionary. Context contains additional environment settings that affect output
                                           image. The supported environment settings for this tool are:

                                           The possible key for the context dictionary are:

                                           - parallelProcessingFactor : The specified number or percentage of processes will be used for the analysis. The default value is 50%.

                                           - orthoMosaicAsOvr : Determines whether to apply the generated orthomosaic image as overview of input image collection. The default value is False.

                                           - clippingGeometry : Specifies the extent or clippinggeometry parameter for the Clip raster function. It is used for setting the extent of the output orthomosaic image.The default value is empty.

                                           - cellSize : The output raster will have the resolution specified by cell size. The cellSize is specified using the MAXOF , MINOF , or number. The default value is MAXOF.

                                           - resamplingMethod : Choose which resampling method to use when creating the raster dataset for download.
                                             Available resampling types include: NEARESTNEIGHBOR , BILINEAR , CUBIC , MAJORITY , BILINEAR_PLUS , BILINEAR_GAUSSBLUR , BILINEAR_GAUSSBLUR_PLUS , AVERAGE , MINIMUM , MAXIMUM , VECTOR_AVERAGE . The default value is NEARESTNEIGHBOR .

                                           - outSR : The output raster will be projected into the output spatial reference.

                                           - seamlinesMethod : Specifies the computation method for running the Build Seamlines tool inside the task. The default value is DISPARITY.

                                           - minRegionSize : Any seamline polygons smaller than this specified threshold will be removed in the seamline result.

                                           - pixelSize : Generates seamlines for raster datasets that fall within the specified spatial resolution size.

                                           - blendType : Determine how to blend one image into another (Both , Inside , or Outside ) over the seamlines. Inside blends pixels inside the seamline, while Outside blends outside the seamline. Both will blend pixels on either side of the seamline.

                                           - blendWidth : Specifies how many pixels will be blended relative to the seamline. Blending (feathering) occurs along a seamline between pixels of overlapping images.

                                           - blendUnit : Specifies the unit of measurement for blendWidth . Pixels measures using the number of pixels, and Ground measures using the same units as the image collection.

                                           - requestSizeType : Sets the units for requestSize . Pixels modifies requestSize based on the pixel size. This resamples the closest image based on the raster pixel size. Pixel scaling factor modifiers requestSize by specifying a scaling factor. This operation resamples the closest image by multiplying the raster pixel size with the pixel size factor.

                                           - requestSize : Specifies the number of columns and rows for resampling. Though the maximum value is 5,000, this value can increase or decreased based on the complexity of your raster data. A greater image resolution provides more detail in the raster dataset but increases the processing time.

                                           - minThinnessRatio : Defines how thin a polygon can be before its considered a sliver. This is based on a scale from 0 to 1.0, where a value of 0.0 represents a polygon that's almost a straight line, and a value of 1.0 represents a polygon that's a circle.

                                           - maxSliverSize : Defines how large a Sliver can be before its considered a polygon. This uses the same scale as minThinnessRatio .

                                           - skipX and skipY : Specifies the X skip factor and the Y skip factor for running the Build Pyramids And Statistics tool inside the task as part of color correction workflow. The default values are both 1.

                                           - overwriteStats : Specifies the Skip Existing parameter for running the Build Pyramids And Statistics tool inside the task as part of color correction workflow. The default value isFalse , and statistics will not be recalculated if they exist.

                                           - colorCorrectionMethod : Specifies the balance method for running the Color Balance Mosaic Dataset tool inside the task. The default value is DODGING .

                                           - dodgingSurface : Specifies the color surface type for running the Color Balance Mosaic Dataset tool inside the task. The default value is SINGLE_COLOR .

                                           - targetImage : Specifies the target raster for running the Color Balance Mosaic Dataset tool inside the task. The default value is empty.

                                           - applyColorCorrection : Indicates whether or not to apply color correction to the image collection. The default value is True .

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
                                               |   "skipX": 10,
                                               |   "skipY": 10,
                                               |   "overwriteStats": "OVERWRITE"
                                               |  }
    -----------------------------------    --------------------------------------------------------------------
    gis                                    Optional :class:`~arcgis.gis.GIS` . The GIS on which this tool runs. If not specified, the active GIS is used.
    ===================================    ====================================================================

    :return:
        The Orthomosaicked Imagery layer item

    """

    gis = arcgis.env.active_gis if gis is None else gis

    update_flight_json = False
    from ._mission import Mission

    flight_json_details = {}

    if isinstance(image_collection, Mission):
        mission = image_collection
        image_collection = image_collection.image_collection
        update_flight_json = True

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

        flight_json_details = {
            "update_flight_json": update_flight_json,
            "mission": mission,
            "item_name": "ortho",
            "processing_states": ortho_dict,
        }

    return gis._tools.orthomapping.generate_orthomosaic(
        image_collection=image_collection,
        output_ortho_image=out_ortho,
        regen_seamlines=regen_seamlines,
        recompute_color_correction=recompute_color_correction,
        context=context,
        future=future,
        flight_json_details=flight_json_details,
        **kwargs,
    )


###################################################################################################
## Generate report
###################################################################################################
def generate_report(
    image_collection,
    report_format: str = "PDF",
    *,
    gis: Optional[GIS] = None,
    future: bool = False,
    **kwargs,
):
    """
    This function is used to generate orthomapping report with image collection
    that has been block adjusted. The report would contain information about
    the quality of the adjusted images, the distribution of the control points, etc.
    The output of this service tool is a downloadable html page.

    ===================    ====================================================================
    **Parameter**           **Description**
    -------------------    --------------------------------------------------------------------
    image_collection       Required. The input image collection that should be
                           used to generate a report from.

                           The image_collection can be a Mission object, an image service URL or portal Item or a datastore URI.

                           The image_collection must exist.
    -------------------    --------------------------------------------------------------------
    report_format          Type of the format to be generated. Possible PDF, HTML. Default - PDF
    -------------------    --------------------------------------------------------------------
    gis                    Optional :class:`~arcgis.gis.GIS` . The GIS on which this tool runs. If not specified, the active GIS is used.
    ===================    ====================================================================

    :return:
        The URL of a single html webpage that is a formatted orthomapping report

    """

    gis = arcgis.env.active_gis if gis is None else gis
    update_flight_json = False
    flight_json_details = {}
    from ._mission import Mission

    if isinstance(image_collection, Mission):
        mission = image_collection
        image_collection = image_collection.image_collection
        update_flight_json = True

        flight_json_details = {
            "update_flight_json": update_flight_json,
            "mission": mission,
            "item_name": "report",
        }

    return gis._tools.orthomapping.generate_report(
        image_collection=image_collection,
        report_format=report_format,
        future=future,
        flight_json_details=flight_json_details,
        **kwargs,
    )


###################################################################################################
## query camera info
###################################################################################################
def query_camera_info(
    camera_query: Optional[Union[dict[str, Any], str]] = None,
    *,
    gis: Optional[GIS] = None,
    future: bool = False,
    **kwargs,
):
    """
    This service tool is used to query specific or the entire digital camera
    database. The digital camera database contains the specs
    of digital camera sensors that were used to capture drone images.

    ==================     ====================================================================
    **Parameter**           **Description**
    ------------------     --------------------------------------------------------------------
    camera_query           Optional Dictionary or String. A dictionary or a string representing
                           the SQL query statement to query the specifications of digital
                           camera sensors that are used to capture drone images.
                           The digital camera database can be queried using the fields Make, Model,
                           Focallength, Columns, Rows, PixelSize.

    ------------------     --------------------------------------------------------------------
    gis                    Optional :class:`~arcgis.gis.GIS` . The GIS on which this tool runs. If not specified, the active GIS is used.
    ==================     ====================================================================


    :return:
        Dictionary/Data Frame representing the camera database

    .. code-block:: python

        # Example 1: Query camera properties for camera Rollei RCP-8325 in dictionary format.

        camera_info = query_camera_info(camera_query={"Make":"Rollei", "Model":"RCP-8325"})


        # Example 2: Query camera properties for camera Rollei RCP-8325 in string format.

        camera_info = query_camera_info(camera_query="Make='Rollei' and Model='RCP-8325'")


    """
    gis = arcgis.env.active_gis if gis is None else gis

    return gis._tools.orthomapping.query_camera_info(
        camera_query=camera_query, future=future, **kwargs
    )


###################################################################################################
## query control points
###################################################################################################
def query_control_points(
    image_collection,
    query: str,
    *,
    gis: Optional[GIS] = None,
    future: bool = False,
    **kwargs,
):
    """
    Query for control points in an image collection. It allows users to query
    among certain control point sets that has ground control points inside.

    ==================     ====================================================================
    **Parameter**           **Description**
    ------------------     --------------------------------------------------------------------
    image_collection       Required, the input image collection on which to query
                           the the control points.

                           The image_collection can be a Mission object, an image service URL or portal Item or a datastore URI.

                           The image_collection must exist.
    ------------------     --------------------------------------------------------------------
    query                  Required string. a SQL statement used for querying the point;

                           Example:

                            "pointID > 100"
    ------------------     --------------------------------------------------------------------
    gis                    Optional :class:`~arcgis.gis.GIS` . The GIS on which this tool runs. If not specified, the active GIS is used.
    ==================     ====================================================================


    :return:
        A dictionary object

    """

    gis = arcgis.env.active_gis if gis is None else gis
    update_flight_json = False
    flight_json_details = {}
    from ._mission import Mission

    if isinstance(image_collection, Mission):
        mission = image_collection
        image_collection = image_collection.image_collection
        update_flight_json = True

        flight_json_details = {
            "update_flight_json": update_flight_json,
            "mission": mission,
            "item_name": "queryControlPoints",
        }

    return gis._tools.orthomapping.query_control_points(
        image_collection=image_collection,
        where=query,
        future=future,
        flight_json_details=flight_json_details,
        **kwargs,
    )


###################################################################################################
## Reset image collection
###################################################################################################
def reset_image_collection(
    image_collection,
    *,
    gis: Optional[GIS] = None,
    future: bool = False,
    **kwargs,
):
    """
    Reset the image collection. It is used to reset the image collection to its
    original state. The image collection could be adjusted during the orthomapping
    workflow and if the user is not satisfied with the result, they will be able
    to clear any existing adjustment settings and revert the images back to
    un-adjusted state

    ==================     ====================================================================
    **Parameter**           **Description**
    ------------------     --------------------------------------------------------------------
    image_collection       Required, the input image collection to reset
                           The image_collection can be a Mission object, an image service URL or portal Item or a datastore URI.

                           The image_collection must exist.
    ------------------     --------------------------------------------------------------------
    gis                    Optional :class:`~arcgis.gis.GIS` . The GIS on which this tool runs. If not specified, the active GIS is used.
    ==================     ====================================================================

    :return:
        A boolean indicating whether the reset was successful or not

    """

    gis = arcgis.env.active_gis if gis is None else gis
    from ._mission import Mission

    flight_json_details = {}

    if isinstance(image_collection, Mission):
        mission = image_collection
        image_collection = image_collection.image_collection
        update_flight_json = True

        flight_json_details = {
            "update_flight_json": update_flight_json,
            "mission": mission,
            "item_name": "reset",
        }

    return gis._tools.orthomapping.reset_image_collection(
        image_collection=image_collection,
        future=future,
        flight_json_details=flight_json_details,
        **kwargs,
    )


def compute_spatial_reference_factory_code(latitude: float, longitude: float):
    """
    Computes spatial reference factory code. This value may be used as out_sr value in create image collection function

    ==================     ====================================================================
    **Parameter**           **Description**
    ------------------     --------------------------------------------------------------------
    latitude               latitude value in decimal degrees that will be used to compute UTM zone
    ------------------     --------------------------------------------------------------------
    longitude              longitude value in decimal degrees that will be used to compute UTM zone
    ==================     ====================================================================

    :return:
        factory_code : spatial reference factory code
    """
    from math import isnan, fabs, floor

    zone = 0
    if (
        isnan(longitude)
        or isnan(latitude)
        or fabs(longitude) > 180.0
        or fabs(latitude) > 90.0
    ):
        raise RuntimeError("Incorrect latitude or longitude value")

    zone = floor((longitude + 180) / 6) + 1
    if latitude >= 56.0 and latitude < 64.0 and longitude >= 3.0 and longitude < 12.0:
        zone = 32

    if latitude >= 72.0 and latitude < 84.0:
        if longitude >= 0.0 and longitude < 9.0:
            zone = 31
        elif longitude >= 9.0 and longitude < 21.0:
            zone = 33
        elif longitude >= 21.0 and longitude < 33.0:
            zone = 35
        elif longitude >= 33.0 and longitude < 42.0:
            zone = 37

    if latitude >= 0:
        srid = 32601
    else:
        srid = 32701

    factory_code = srid + zone - 1

    return factory_code


###################################################################################################
## Query Exif Info
###################################################################################################
def query_exif_info(
    input_images, *, gis: Optional[GIS] = None, future: bool = False, **kwargs
):
    r"""
    The `query_exif_info` reads the Exif header metadata from single or
    multiple images in shared data store. The Exif metadata is usually stored
    in drone image files. Some common Exif metadata information are GPS
    locations, camera model, focal length, and more.

    ==================     ====================================================================
    **Parameter**           **Description**
    ------------------     --------------------------------------------------------------------
    input_images           Required String/list of Strings.  The input images could be a single image path, list of image paths,
                           or a folder path, or a list of folder paths. The image file paths can also be server data store path.

                           Eg:

                           - "\\servername\drone\imagefolder\image_file.jpg"
                           - "/cloudStores/S3DataStore/yvwd13"
                           - "/fileShares/drones/SampleEXIF/YUN_0040.jpg"
                           - ["/fileShares/drones/SampleEXIF/DJI_0002.JPG", "/fileShares/drones/SampleEXIF/YUN_0040.jpg"]
                           - ["/cloudStores/S3DataStore/yvwd13", "/cloudStores/S3DataStore/BogotaFarm"]
    ------------------     --------------------------------------------------------------------
    gis                    Optional :class:`~arcgis.gis.GIS` . The GIS on which this tool runs. If not specified, the active GIS is used.
    ==================     ====================================================================

    :return:
        A dictionary object

    """
    gis = arcgis.env.active_gis if gis is None else gis

    return gis._tools.orthomapping.query_exif_info(
        input_images=input_images, future=future, **kwargs
    )


class Project:
    """

    Project represents an Orthomapping Project Item in the portal.

    Usage: ``arcgis.raster.Project(project, gis=gis)``

    ====================================     ====================================================================
    **Parameter**                             **Description**
    ------------------------------------     --------------------------------------------------------------------
    project                                  Required string or Orthomapping Project Item

                                             Example:

                                                | project = "OM_project"
                                                | om_item = gis.content.get("85a54236c6364a88a7c7c2b1a31fd901")
                                                | project = om_item
    ------------------------------------     --------------------------------------------------------------------
    definition                               Optional dictionary. Custom project definition.
    ------------------------------------     --------------------------------------------------------------------
    gis                                      Optional  :class:`~arcgis.gis.GIS` . Repesents the GIS object of the Orthomapping
                                             Project item.
    ====================================     ====================================================================

    .. code-block:: python

        # Example Usage

        project = Project('om_proj', gis=gis)

        # Example Usage

        om_item = gis.content.get("85a54236c6364a88a7c7c2b1a31fd901")
        project = Project(om_item, gis=gis)

    """

    def __init__(
        self,
        project=None,
        definition=None,
        *,
        gis: Optional[GIS] = None,
        **kwargs,
    ):
        if not isinstance(project, Item):
            try:
                project = _create_project(name=project, definition=definition, gis=gis)
            except:
                raise RuntimeError("Creation of orthomapping project failed.")

        if project.type == "Ortho Mapping Project":
            self._project_item = project
        else:
            raise RuntimeError(
                "Invalid project. Project is not of type Ortho Mapping Project"
            )
        try:
            self._project_name = self._project_item.title
        except:
            self._project_name = self._project_item.name

        self._mission_list = []
        gis = arcgis.env.active_gis if gis is None else gis
        self._gis = gis

        content = self._gis.content
        fm = content.folders
        for folder in fm.list():
            if folder.properties["id"] == self._project_item.ownerFolder:
                self._folder = folder
                break

    @property
    def missions(self):
        """
        The ``missions`` property returns all the missions associated with the project

        :return: A list of missions of the orthomapping project
        """
        from ._mission import Mission

        res_list = self._project_item.resources.list()
        self._mission_list = []
        for resource in res_list:
            full_res_name = resource["resource"]
            res_name = full_res_name[
                full_res_name.find("/") + 1 : full_res_name.find(".")
            ]
            self._mission_list.append(Mission(mission_name=res_name, project=self))

        return self._mission_list

    @property
    def mission_count(self):
        """
        The ``count`` property returns the number of missions associated with the project

        :return: An integer representing the number of missions
        """
        res_list = self._project_item.resources.list()
        return len(res_list)

    @property
    def item(self):
        """
        The ``item`` property returns the portal item associated with the Project.

        :return: A portal item
        """
        return self._project_item

    def delete(self):
        """
        The ``delete`` method deletes the project item from the portal and all the associated products.

        :return: A boolean indicating whether the deletion was successful or not
        """
        deleted = self._folder.delete()
        return deleted

    # def create_project(self, name, definition: Optional[dict[str, Any]] = None):
    #    try:
    #        project_item = _create_project(name=name,
    #                                       definition=definition
    #                                       )
    #        self._project_item = project_item
    #        return True
    #    except:
    #        raise RuntimeError("Creation of orthompping project failed.")

    def add_mission(
        self,
        image_list: list,
        mission_name: Optional[str] = None,
        image_collection: Optional[str] = None,
        raster_type_name: Optional[str] = None,
        raster_type_params: Optional[dict[str, Any]] = None,
        out_sr: Optional[dict[str, Any]] = None,
        context: Optional[dict[str, Any]] = None,
    ):
        """
        Add missions to the orthomapping project item. You can add imagery from one or more drone flights 
        to your orthomapping project item.

        ======================               ====================================================================
        **Parameter**                        **Description**
        ----------------------               --------------------------------------------------------------------
        image_list                           Required, the list of input images to be added to
                                             the image collection being created. This parameter can
                                             be a list of image paths or a path to a folder containing the images

                                             The function can create hosted imagery layers on enterprise from 
                                             local raster datasets by uploading the data to the server.    
        ----------------------               --------------------------------------------------------------------
        mission_name                         Optional string. The name of the mission.
        ----------------------               --------------------------------------------------------------------
        image_collection                     Optional string, the name of the image collection to create.
                  
                                             The image collection can be an existing image service, in \
                                             which the function will create a mosaic dataset and the existing \
                                             hosted image service will then point to the new mosaic dataset.

                                             If the image collection does not exist, a new multi-tenant \
                                             service will be created.

                                             This parameter can be the Item representing an existing image_collection \
                                             or it can be a string representing the name of the image_collection \
                                             (either existing or to be created.)

                                             The image collection will be created in the same folder as the one created
                                             by the create_project method
        ----------------------               --------------------------------------------------------------------
        raster_type_name                     Optional string. The name of the raster type to use for adding data to \
                                             the image collection.

                                             Example:

                                                "UAV/UAS"
        ----------------------               --------------------------------------------------------------------
        raster_type_params                   Optional dict. Additional ``raster_type`` specific parameters.
        
                                             The process of add rasters to the image collection can be \
                                             controlled by specifying additional raster type arguments.

                                             The raster type parameters argument is a dictionary.

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
        ----------------------               --------------------------------------------------------------------
        context                              Optional dict. The context parameter is used to provide additional input parameters.
    
                                             Syntax: {"image_collection_properties": {"imageCollectionType":"Satellite"},"byref":True}
                                        
                                             Use ``image_collection_properties`` key to set value for imageCollectionType.

                                             .. note::

                                                The "imageCollectionType" property is important for image collection that will later on be adjusted by orthomapping system service. 
                                                Based on the image collection type, the orthomapping system service will choose different algorithm for adjustment. 
                                                Therefore, if the image collection is created by reference, the requester should set this 
                                                property based on the type of images in the image collection using the following keywords. 
                                                If the imageCollectionType is not set, it defaults to "UAV/UAS"

                                             If ``byref`` is set to 'True', the data will not be uploaded. If it is not set, the default is 'False'

                                             The context parameter can also be used to specify whether to build overviews, \
                                             build footprints, to specify pixel value that represents the NoData etc.


                                             Example:

                                                | {"buildFootprints":True,                                            
                                                | "footprintsArguments":{"method":"RADIOMETRY","minValue":1,"maxValue":5,
                                                | "shrinkDistance":50,"skipOverviews":True,"updateBoundary":True,
                                                | "maintainEdge":False,"simplification":None,"numVertices":20,
                                                | "minThinnessRatio":0.05,"maxSliverSize":20,"requestSize":2000,
                                                | "minRegionSize":100},
                                                | "defineNodata":True,                                            
                                                | "noDataArguments":{"noDataValues":[500],"numberOfBand":99,"compositeValue":True},                                            
                                                | "buildOverview":True}

                                             The context parameter can be used to add new fields when creating \
                                             the image collection.


                                             Example:

                                                | {"fields": [{"name": "cloud_cover", "type": "Long"},
                                                | {"name": "cloud_shadow_count", "type": "Long"}]}
        ======================               ====================================================================

        :return: Mission object

        """

        try:
            from ._mission import Mission

            collection, mission_name = _add_mission(
                project=self,
                image_list=image_list,
                mission_name=mission_name,
                image_collection=image_collection,
                raster_type_name=raster_type_name,
                raster_type_params=raster_type_params,
                out_sr=out_sr,
                context=context,
            )
            return Mission(mission_name=mission_name, project=self)

        except:
            raise RuntimeError("Failed to add the mission to the project")

    def get_mission(self, name):
        """
        Returns a Mission object with the name specified using the name parameter.

        ==================                   ====================================================================
        **Parameter**                         **Description**
        ------------------                   --------------------------------------------------------------------
        name                                 Required string. The name of the Mission.
        ==================                   ====================================================================

        :return: Mission object


        """
        from ._mission import Mission

        res_list = self._project_item.resources.list()
        for resource in res_list:
            full_res_name = resource["resource"]
            res_name = full_res_name[
                full_res_name.find("/") + 1 : full_res_name.find(".")
            ]
            if name == res_name:
                return Mission(mission_name=name, project=self)

    def __repr__(self):
        return "<%s - %s>" % (type(self).__name__, self._project_name)
