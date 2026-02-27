from __future__ import annotations
import logging
from arcgis.auth.tools import LazyLoader
import copy
from traitlets import link
from typing import Optional, Union, Any
import base64
from functools import cached_property


# Import methods from modules
auth = LazyLoader("arcgis.auth._auth._schain")
arcgis = LazyLoader("arcgis")
arcgismapping = LazyLoader("arcgis.map")
basemapdef = LazyLoader("arcgis.map.definitions._basemap_definitions")
basemapdef3d = LazyLoader("arcgis.map.definitions._3d_basemap_definitions")
rm = LazyLoader("arcgis.map.definitions._renderer_metaclass")
env = LazyLoader("arcgis.env")
features = LazyLoader("arcgis.features")
json = LazyLoader("json")
arcgis_layers = LazyLoader("arcgis.layers")
pd = LazyLoader("pandas")
geocoding = LazyLoader("arcgis.geocoding")
realtime = LazyLoader("arcgis.realtime")
raster = LazyLoader("arcgis.raster")
uuid = LazyLoader("uuid")
_gis_mod = LazyLoader("arcgis.gis")
os = LazyLoader("os")
arcgis_utils = LazyLoader("arcgis._impl.common._utils")
arcgis_cm = LazyLoader("arcgis._impl._content_manager")
geoprocessing = LazyLoader("arcgis.geoprocessing")
urllib = LazyLoader("urllib")
_models = LazyLoader("arcgis.map.dataclasses.models")
_model_enums = LazyLoader("arcgis.map.dataclasses.enums")

_DEFAULT_SPATIAL_REFERENCE = {
    "latestWkid": 3857,
    "wkid": 102100,
}


class RefreshableProperty:
    def __init__(self, func):
        self.func = func
        self.attr_name = f"_refreshable_{func.__name__}"

    def __get__(self, instance, owner):
        if instance is None:
            return self  # Access via class returns descriptor itself
        if not hasattr(instance, self.attr_name):
            self._refresh(instance)
        return getattr(instance, self.attr_name)

    def _refresh(self, instance):
        setattr(instance, self.attr_name, self.func(instance))

    def clear(self, instance):
        """Clear the cached value."""
        if hasattr(instance, self.attr_name):
            delattr(instance, self.attr_name)


def refreshable_property(func):
    return RefreshableProperty(func)


class _HelperMethods:
    """
    The Helper Methods class defines the methods that are used in both the Map and Scene classes. This facilitates
    upkeep since many similarities can be found and we store them in one place.

    The class is instantiated with either an instance of Map or Scene and the spec used depends on this.
    Not all layers are in the webmap spec and vise versa. So it is up to the documentation in the respective classes
    to clearly state what layers can and cannot be added. This is an example of a minor difference but there are others as well.

    When making edits, consult the documentation for each method found in each widget class as well as their respective specs.

    There are four main properties on the class that are used throughout the methods:
    - spec: The spec used to validate depends on the widget type. This is either the webmap spec or the webscene spec.
    - _source: The class instance which is either Map or Scene. This is important to grab methods and properties from there such as layers, extent, gis, etc.
    - pydantic_class: This represents the pydantic dataclass we are manipulating throughout each property and method. This can be found in the Map class as _webmap and in the Scene class as _webscene.
    - is_map: A boolean to determine if the widget is a Map or Scene. This is used to determine which spec to use and some other minor differences.
    """

    def __init__(self, widget: arcgismapping.Map | arcgismapping.Scene) -> None:
        # Map and Scene will be referred to as widget since they share commonality of being DOMWidgets.
        self._source: arcgismapping.Map | arcgismapping.Scene = widget

        # The spec used to validate depends on the widget type
        if isinstance(widget, arcgismapping.Map):
            self.is_map = True
        elif isinstance(widget, arcgismapping.Scene):
            self.is_map = False
        else:
            raise ValueError("Invalid widget type")

    ############################## Map and Scene Setup Methods ##############################
    def _set_widget_definition(self, definition):
        """
        This is a method to avoid circular reference. Once the definition has been created in the widget, this
        method is called and the definition is assigned. This method is called in the widget constructors.

        This represents the pydantic dataclass we are manipulating throughout each property and method.
        For a Map it is the pydantic Webmap class and for a Scene it is the pydantic Webscene class, each found
        in their respective spec files.

        In the map this property is called `_webmap` and in the scene it is called `_webscene`.
        """
        self.pydantic_class: _models.Webmap | _models.Webscene = definition

    def _setup_gis_properties(self, gis):
        if gis is None:
            # If no active gis then login as anonymous user
            gis = env.active_gis if env.active_gis else _gis_mod.GIS(set_active=False)
        # gis property on the widget
        self._source._gis = gis

        # determine authentication mode and relay to the widget
        auth = getattr(gis, "_session", None)
        token = getattr(getattr(auth, "auth", None), "token", None)
        if token:
            self._source._portal_token = str(token)
            self._source._auth_mode = "tokenBased"
        elif isinstance(
            getattr(auth, "auth", None),
            tuple(
                ty
                for ty in (
                    getattr(auth, "_MultiAuth", None),
                    getattr(auth, "SupportMultiAuth", None),
                )
                if isinstance(ty, type)
            ),
        ):
            tokens = [
                getattr(mode, "token")
                for mode in auth.auth.authentication_modes
                if hasattr(mode, "token")
            ]
            if tokens:
                self._source._portal_token = str(tokens[0])  # First available token
                self._source._auth_mode = "tokenBased"
            else:
                self._source._portal_token = ""
                self._source._auth_mode = "anonymous"
        else:
            self._source._portal_token = ""
            self._source._auth_mode = "anonymous"

        # Set the properties that aren't dependent on auth mode
        self._source._portal_rest_url = self._get_portal_url()
        self._source._username = str(gis._username)
        self._source._proxy_rule = self._get_proxy_rule()
        self._check_js_cdn_variable()

    def _check_js_cdn_variable(self):
        """
        Check if the JS API CDN variable is set and if not, set it.
        """
        # Look if env variable is set
        self._source.js_api_path = os.getenv("JSAPI_CDN", "")

    def _get_portal_url(self):
        """
        Get the portal url to be used.
        """
        try:
            # public rest url
            return self._source._gis._public_rest_url
        except Exception:
            pass
        # gis url
        return self._source._gis.url

    def _get_proxy_rule(self):
        """
        Get the proxy configuration to be used.
        """
        return self._source._gis.session.proxies or {}

    def _setup_location_properties(self, location, geocoder):
        """
        Set up the widget properties to be used.
        """
        if location:
            # If user gave location, geocode and find the correct points to set
            self._geocode_location(location, geocoder)
        elif self._init_from_initial_state() or self._init_from_org_settings():
            # Extent was set from initial state
            pass
        else:
            # No extent found, creating default
            if self.is_map:
                self._source.extent = {
                    "spatialReference": _DEFAULT_SPATIAL_REFERENCE,
                    "xmin": -13034068.816148141,
                    "ymin": 4021158.323305902,
                    "xmax": -13014692.029477874,
                    "ymax": 4036445.728962917,
                }
            else:
                # Create default camera dataclass for scene
                camera = _models.Camera(
                    tilt=0.0,
                    position={
                        "spatialReference": _DEFAULT_SPATIAL_REFERENCE,
                        "x": -13024380.422,
                        "y": 4028802.0,
                        "z": 3000000,
                    },
                    heading=0.0,
                )
                # set on traitlet
                self._source.camera = camera.dict()

        self._fix_pydantic_initial_state()

    def _init_from_initial_state(self):
        initial = self.pydantic_class.initial_state
        if self.is_map:
            if not initial or not hasattr(initial.viewpoint, "target_geometry"):
                return False
            geom = initial.viewpoint.target_geometry
            if not getattr(geom, "xmin", None):
                return False
            if (
                not hasattr(geom, "spatial_reference")
                or geom.spatial_reference.wkid is None
            ):
                geom.spatial_reference = _models.SpatialReference(
                    **_DEFAULT_SPATIAL_REFERENCE
                )
            self._source.extent = geom.dict()
            return True
        else:
            if not initial or not hasattr(initial.viewpoint, "camera"):
                return False
            geom = initial.viewpoint.camera
            if not getattr(geom.position, "x", None):
                return False
            self._source.camera = geom.dict()
            return True

    def _init_from_org_settings(self):

        org_extent = self._source._gis.org_settings.get("defaultExtent")
        if not org_extent:
            return False

        org_extent = dict(org_extent)  # Avoid mutating original
        if "spatialReference" not in org_extent:
            org_extent["spatialReference"] = _DEFAULT_SPATIAL_REFERENCE
        if self.is_map:
            self._source.extent = org_extent
            return True
        else:
            # Create default camera dataclass for scene
            camera = _models.Camera(
                tilt=0.0,
                position={
                    "spatialReference": _DEFAULT_SPATIAL_REFERENCE,
                    "x": (org_extent["xmin"] + org_extent["xmax"]) / 2,
                    "y": (org_extent["ymin"] + org_extent["ymax"]) / 2,
                    "z": 3000000,
                },
                heading=0.0,
            )
            # set on traitlet
            self._source.camera = camera.dict()
            return True

    def _geocode_location(self, geo_location, geocoder):
        """
        If a location was given, geocode and find the correct points to set
        """
        # get the geocoder(s)
        geocoders = (
            [geocoder]
            if geocoder and isinstance(geocoder, geocoding.Geocoder)
            else geocoding.get_geocoders(self._source._gis)
        )

        # geocode
        for gc in geocoders:
            try:
                # geocode the location
                locations = geocoding.geocode(
                    geo_location,
                    out_sr=_DEFAULT_SPATIAL_REFERENCE.get("wkid"),
                    max_locations=1,
                    geocoder=gc,
                )
            except Exception as e:
                # if geocoding fails, try the next geocoder
                logging.debug(f"Geocoding failed: {e}")
                continue

            # if no locations found, continue to next geocoder
            if not locations:
                continue

            # set properties based on location
            geo_extent = locations[0].get("extent")
            geo_location = locations[0].get("location")
            if self.is_map and (geo_extent or geo_location):
                # if no extent, continue to next geocoder
                if geo_extent:
                    # Add the spatial reference if not present
                    if "spatialReference" not in geo_extent:
                        geo_extent["spatialReference"] = _DEFAULT_SPATIAL_REFERENCE
                    # Set the extent traitlet
                    self._source.extent = geo_extent
                elif geo_location:
                    # If no extent, default to using the center [lat, long]
                    self._source.center = [geo_location["y"], geo_location["x"]]
            # For a scene set the camera position as well
            elif not self.is_map and geo_location:
                self._source.camera = {
                    "position": {
                        "spatialReference": _DEFAULT_SPATIAL_REFERENCE,
                        "x": geo_location["x"],
                        "y": geo_location["y"],
                        "z": 3000000,  # Default altitude
                    },
                    "tilt": 0.0,
                    "heading": 0.0,
                }
            return

    def _fix_pydantic_initial_state(self):
        """
        Fix the initial state of the webmap if it is None.
        """
        # The extent observe will do this as well but we need to keep incase user only uses python script.
        # reference _webmap or _webscene initial state class
        initial_state = self.pydantic_class.initial_state
        if self.is_map:
            # Fix the extent
            extent = self._source.extent
            if (
                not initial_state
                or not initial_state.viewpoint
                or not getattr(initial_state.viewpoint, "target_geometry", None)
                or getattr(initial_state.viewpoint.target_geometry, "xmin", None)
                is None
            ):
                # Map with no viewpoint or target geometry
                viewpoint = _models.MapViewpoint(rotation=0, targetGeometry=extent)
                if not initial_state:
                    # create the initial state class
                    self.pydantic_class.initial_state = _models.MapInitialState(
                        viewpoint=viewpoint
                    )
                else:
                    # set target geometry extent to the extent of the map
                    self.pydantic_class.initial_state.viewpoint.target_geometry = (
                        _models.Extent(**extent)
                    )
                return
        if not self.is_map:
            # Fix the camera
            if getattr(initial_state.viewpoint, "camera", None):
                self._source.camera = initial_state.viewpoint.camera.dict()
                # If camera is already set, we don't need to change it
                return
            # set on _webscene dataclass
            self.pydantic_class.initial_state.viewpoint = _models.SceneViewpoint(
                camera=self._source.camera.dict()
            )
            return

    ############################## MapContent Methods ##############################
    @property
    def _tables(self):
        """
        A list of tables that can be found on the scene.
        """
        tables = []
        # Get the tables from the dataclass
        map_tables = self.pydantic_class.tables or []
        # Create Table objects
        for table in map_tables:
            if isinstance(table, _models.SubtypeGroupTable):
                # If subtype group table, create a GroupLayer
                t = arcgismapping.SubtypeGroupTable(table, self._source, self._source)
            else:
                t = features.Table(table.url)
            tables.append(t)
        return tables

    @property
    def _layers(self):
        """
        A list of layers that can be found on the scene. This is called when
        the Webscene is initialized if pre-existing layers are in the Webscene.
        After that the layers will be added and removed from the `layers` property.
        """
        layers = []
        operational_layers = self.pydantic_class.operational_layers or []
        for index, layer in enumerate(operational_layers):
            # index needed when dealing with group layers
            l = self._infer_layer(layer, index)
            if l:
                layers.append(l)
        return layers

    def _infer_layer(self, layer, index=None, url=None):
        """
        Infer the layer instance to be created and added to the list.
        """

        # layer mapping for group layer creation
        layer_type_mapping = {
            "GroupLayer": lambda *args, **kwargs: arcgismapping.GroupLayer(
                self.pydantic_class.operational_layers[index],
                self._source,
                self._source,
            ),
            "SubtypeGroupTable": lambda *args, **kwargs: arcgismapping.SubtypeGroupTable(
                self.pydantic_class.operational_layers[index],
                self._source,
                self._source,
            ),
            "SubtypeGroupLayer": lambda *args, **kwargs: arcgismapping.SubtypeGroupLayer(
                self.pydantic_class.operational_layers[index],
                self._source,
                self._source,
            ),
        }

        # define the needed variables
        layer_instance = None
        layer = layer.dict() if not isinstance(layer, dict) else layer

        layer_type = layer.get("layerType")
        layer_url = layer.get("url") or url
        item_id = layer.get("itemId")

        if "Group" in layer_type:
            # Group Layers
            layer_class = layer_type_mapping.get(layer_type, _gis_mod.Layer)
            return layer_class(layer_url, gis=self._source._gis)

        if layer_url:
            return arcgis_layers.Service(layer_url, server=self._source._gis)
        if item_id:
            try:
                item_url = self._source._gis.content.get(item_id).url
                return arcgis_layers.Service(item_url, server=self._source._gis)
            except Exception as e:
                try:
                    item = self._source._gis.content.get(item_id)
                    if item is None:
                        raise ValueError(f"Item with id {item_id} not found.")
                except Exception as e:
                    raise Exception(f"Error: {e}")
                raise Exception(f"Error: {e}")
        if (
            not layer_instance
            and layer_type == "ArcGISFeatureLayer"
            and layer.get("featureCollection")
        ):
            # Feature layer created from a feature collection
            layer_instance = features.FeatureCollection(
                layer.get("featureCollection", {})
            )
        return layer_instance

    ############################### Add Feature Collection Methods ##############################
    def _fix_fields_type(self, layer_def):
        field_types = {
            "blob": "esriFieldTypeBlob",
            "date": "esriFieldTypeDate",
            "double": "esriFieldTypeDouble",
            "geometry": "esriFieldTypeGeometry",
            "global_id": "esriFieldTypeGlobalID",
            "guid": "esriFieldTypeGUID",
            "integer": "esriFieldTypeInteger",
            "oid": "esriFieldTypeOID",
            "raster": "esriFieldTypeRaster",
            "single": "esriFieldTypeSingle",
            "small_integer": "esriFieldTypeSmallInteger",
            "string": "esriFieldTypeString",
            "xml": "esriFieldTypeXML",
            "esriFieldTypeBigInteger": "esriFieldTypeDouble",
            "esriFieldTypeDateOnly": "esriFieldTypeDate",
            "esriFieldTypeTimeOnly": "esriFieldTypeString",
            "esriFieldTimestampOffset": "esriFieldTypeString",
        }
        for field in layer_def["fields"]:
            field["type"] = field_types.get(field["type"], field["type"])
        return layer_def

    def _normalize_feature_collection(self, fc):
        """
        Normalize the feature collection to ensure it is in the correct format.
        Depending on how the feature collection was created, it may have different
        schemas. This method ensures that the feature collection is in the correct
        format for further processing.
        """
        # Ensure consistent format
        if "featureSet" in fc.properties:
            fc.properties = {
                "layers": [
                    {
                        "featureSet": dict(fc.properties["featureSet"]),
                        "layerDefinition": dict(fc.properties["layerDefinition"]),
                    }
                ]
            }
        else:
            fc.properties = dict(fc.properties)
        return fc

    def _add_from_feature_collection(self, fc, drawing_info, popup_info, options):
        """
        This methods goes through the steps of taking a feature collection and creating
        a layer that will be added to the map.

        What is important to know:
            - Feature Collections have various schemas depending on what they were created from
            - We need to create a feature layer out of the feature collection
            - Feature Collection will be added to the `layers` property.

        """
        # Normalize the feature collection to ensure it is in the correct format.
        fc = self._normalize_feature_collection(fc)

        # Set the title
        title = fc.properties["layers"][0]["layerDefinition"].get(
            "name", uuid.uuid4().hex[0:7]
        )

        # Create pydantic feature collection and layer definition
        # need to add a screening for fields of type 'esriFieldTypeBigInteger', 'esriFieldTypeDateOnly', 'esriFieldTypeTimeOnly', and 'esriFieldTimestampOffset'
        layers = fc.properties.get("layers", [])
        for layer in layers:
            layer_def = layer["layerDefinition"]
            layer_def_fixed = self._fix_fields_type(layer_def)
            layer["layerDefinition"] = layer_def_fixed

        # Create pydantic classes
        fc = _models.FeatureCollection(layers=layers)
        ld = self._create_layer_definition(fc, drawing_info)
        popup_info = self._create_popup_dataclass(fc, popup_info)

        # Create pydantic Feature Layer with the feature collection and layer definition
        geometry_mapping = {
            "esriGeometryPoint": "point",
            "esriGeometryMultipoint": "multipoint",
            "esriGeometryPolyline": "polyline",
            "esriGeometryPolygon": "polygon",
        }
        sr: dict = {
            "latestWkid": 3857,
            "wkid": 102100,
        }  # sets default SpatialReference

        for layer in fc.layers:
            if (
                hasattr(layer, "layer_definition")
                and hasattr(layer.layer_definition, "spatial_reference")
                and getattr(layer.layer_definition, "spatial_reference", None)
                is not None
            ):
                sr: dict = layer.layer_definition.spatial_reference.model_dump(
                    mode="python",
                    include=None,
                    exclude=None,
                    by_alias=True,
                    exclude_unset=False,
                    exclude_defaults=False,
                    exclude_none=True,
                    round_trip=False,
                    warnings=True,
                )
        # Create pydantic Feature Layer with the feature collection and layer definition
        layer = _models.FeatureLayer(
            featureCollection=fc,
            layerDefinition=ld,
            popupInfo=popup_info,
            title=title,
            id=uuid.uuid4().hex[0:12],
            geometryType=geometry_mapping[ld.get("geometry_type", "esriGeometryPoint")],
            fields=ld.get("fields", []),
            objectIdField=ld.get("object_id_field"),
            spatialReference=sr,
            source=fc.layers[0].feature_set.features,
        )
        self._postprocess_item(layer, options)

    ################################ Add Imagery Layer Methods ##############################
    def _get_rendering_rule(self, layer):
        """
        Get the rendering rule for the layer. This is used for imagery layers.
        """
        _lyr = layer._lyr_json
        if "options" in _lyr:
            lyr_options = json.loads(_lyr["options"])
            if "imageServiceParameters" in lyr_options:
                if "renderingRule" in lyr_options["imageServiceParameters"]:
                    if lyr_options["imageServiceParameters"]["renderingRule"] == {}:
                        return None
                    rr = lyr_options["imageServiceParameters"]["renderingRule"]
                    if "function" in rr:
                        ## if it is a Raster function template (case from RFT class), then we need to set the RFT as a value for the rasterFunctionDefinition key
                        rr = {"rasterFunctionDefinition": rr}
                    return rr

        return None

    def _get_mosaic_rule(self, layer):
        """
        Get the mosaic rule for the layer. This is used for imagery layers.
        """
        _lyr = layer._lyr_json
        if "options" in _lyr:
            lyr_options = json.loads(_lyr["options"])
            if "imageServiceParameters" in lyr_options:
                if "mosaicRule" in lyr_options["imageServiceParameters"]:
                    if lyr_options["imageServiceParameters"]["mosaicRule"] == {}:
                        return None
                    return lyr_options["imageServiceParameters"]["mosaicRule"]

        return None

    def _get_datastore_raster(self, layer):
        _lyr = layer._lyr_json
        if "options" in _lyr:
            lyr_options = json.loads(_lyr["options"])
            if "imageServiceParameters" in lyr_options:
                if "raster" in lyr_options["imageServiceParameters"]:
                    ras = lyr_options["imageServiceParameters"]["raster"]
                    if isinstance(ras, dict):
                        # if the layer is a tiles only service or if allow raster function is set to False but allow analysis is true, the if a raster function is applied then the input will be a dictionary which needs to be base 64 encoded
                        import base64

                        encoded_dict = str(ras).encode("utf-8")
                        ras = base64.b64encode(encoded_dict)
                    return ras
        return None

    def _get_base64_data_url(self, layer):
        img = layer.export_image(
            size=[400, 400], export_format="PNG", bbox=layer.extent
        )
        base64_data = base64.b64encode(img.data).decode("utf-8")
        return f"data:image/png;base64,{base64_data}"

    def _add_local_raster(self, layer, options):
        # trigger the traitlet to update the JS code
        opacity = 1
        if "opacity" in options.keys():
            opacity = options["opacity"]

        self._source._local_image_data = {
            "image": self._get_base64_data_url(layer),
            "extent": layer.extent,
            "opacity": opacity,
        }
        return None

    ############################### Add Service / Layer Methods ##############################
    def _should_preprocess(self, layer) -> bool:
        """
        Determines whether the input should be routed through _preprocess_item
        rather than directly through _add_from_service.

        Preprocessing is required for:
        - lists of layers (user wants to create a GroupLayer)
        - Items with embedded layers/tables
        - Group Layer items
        - Map Service items (special case: layers are stored server-side)
        """
        # list of layers → always preprocess
        if isinstance(layer, list):
            return True

        # Portal Item with layer structure
        if isinstance(layer, _gis_mod.Item):
            if layer.type in ("Group Layer", "Map Service"):
                return True
            # Items that have .layers or .tables also require preprocess
            if hasattr(layer, "layers") and layer.layers:
                return True
            if hasattr(layer, "tables") and layer.tables:
                return True

        return False

    def _create_group_layer(self, layers_or_item, drawing_info, popup_info, options):
        """
        Normalize any of these inputs into a proper GroupLayer:
        - a list of layers
        - a Group Layer Item
        - nested group definitions inside Item data
        """

        # ---------------------------------------------------------
        # Case 1 — already a list → recurse create each layer
        # ---------------------------------------------------------
        if isinstance(layers_or_item, list):
            children = [
                self._add_from_service(lyr, drawing_info, popup_info, options)
                for lyr in layers_or_item
            ]
            title = options.get("title", "Custom Group Layer")
            return _models.GroupLayer(layers=children, title=title)

        # ---------------------------------------------------------
        # Case 2 — a Portal Item representing a group layer
        # ---------------------------------------------------------
        item = layers_or_item
        data = item.get_data() or {}

        raw_layers = data.get("layers", []) or []
        children = []

        for lyr_json in raw_layers:
            if "url" in lyr_json:
                service = arcgis_layers.Service(lyr_json["url"])
                children.append(
                    self._add_from_service(service, drawing_info, popup_info, options)
                )

            elif "itemId" in lyr_json:
                child_item = self._source._gis.content.get(lyr_json["itemId"])
                children.append(
                    self._add_from_service(
                        child_item, drawing_info, popup_info, options
                    )
                )

            elif lyr_json.get("layerType") == "GroupLayer":
                # recursive group layer
                children.append(
                    self._create_group_layer(
                        lyr_json, drawing_info, popup_info, options
                    )
                )

        # Copy over any extra operationalLayer keys from the item's JSON
        for key, val in data.items():
            if key not in ("layers", "layerType") and key not in options:
                options[key] = val

        title = options.get("title") or data.get("title") or item.title

        return _models.GroupLayer(layers=children, title=title)

    def _add_options(self, pydantic_layer, options):
        """
        Applies any leftover `options` to the final model.
        """
        if not options:
            return pydantic_layer

        for key, value in options.items():
            # normalize to snake_case
            snake = "".join("_" + c.lower() if c.isupper() else c for c in key).lstrip(
                "_"
            )

            if hasattr(pydantic_layer, snake):
                setattr(pydantic_layer, snake, value)

        return pydantic_layer

    def _postprocess_item(self, pydantic_layer, options):
        """
        Insert the newly-created layer/table into the webmap's operationalLayers/tables.
        Also applies any leftover `options` to the final model.
        """
        if pydantic_layer is None:
            raise ValueError("The item could not be added to the map.")

        index = options.pop("index", None)
        is_table = isinstance(
            pydantic_layer, (_models.Table, _models.SubtypeGroupTable)
        )

        if self.pydantic_class.tables is None and is_table:
            self.pydantic_class.tables = []

        if self.pydantic_class.operational_layers is None and not is_table:
            self.pydantic_class.operational_layers = []

        target_list = (
            self.pydantic_class.tables
            if is_table
            else self.pydantic_class.operational_layers
        )

        # compute insertion index
        if index is None:
            index = len(target_list)

        # Apply options AFTER building the pydantic model
        if options:
            pydantic_layer = self._add_options(pydantic_layer, options)

        # Insert it
        target_list.insert(index, pydantic_layer)

    def _preprocess_item(self, layer, drawing_info, popup_info, options):
        """
        Handles any input that represents a *container of layers*, not a single layer.

        Cases covered:
        - list of layers → create GroupLayer directly
        - Group Layer Item → create GroupLayer from item.layers
        - Map Service Item → wrap as a single Service instance
        - Item with .layers and/or .tables → expand into multiple webmap layers

        This method always:
        - Resolves all underlying layers
        - Creates the appropriate Pydantic layer models
        - Calls _postprocess_item() for each created layer
        - Returns the *final primary layer* when appropriate
        """

        # ------------------------------------------------------
        # 1. LIST OF LAYERS (explicit group creation)
        # ------------------------------------------------------
        if isinstance(layer, list):
            group = self._create_group_layer(layer, drawing_info, popup_info, options)
            self._postprocess_item(group, options)
            return group

        # From here onward, we know layer is an Item
        options["itemId"] = layer.id

        # ------------------------------------------------------
        # 2. GROUP LAYER ITEM
        # ------------------------------------------------------
        if isinstance(layer, _gis_mod.Item) and layer.type == "Group Layer":
            group = self._create_group_layer(layer, drawing_info, popup_info, options)
            self._postprocess_item(group, options)
            return group

        # ------------------------------------------------------
        # 3. MAP SERVICE ITEM
        #    (always represented as a single Service URL)
        # ------------------------------------------------------
        if isinstance(layer, _gis_mod.Item) and layer.type == "Map Service":
            service = arcgis_layers.Service(layer.url)
            result = self._create_layer_from_service(
                service, drawing_info, popup_info, options, postprocess=False
            )
            self._postprocess_item(result, options)
            return result

        # ------------------------------------------------------
        # 4. ITEM WITH LAYERS (FeatureService, MapImageLayer item, etc.)
        # ------------------------------------------------------
        primary_layer = None

        if hasattr(layer, "layers") and layer.layers:
            layer_list = list(layer.layers)

            if len(layer_list) > 1:
                primary_layer = self._create_group_layer(
                    layer_list, drawing_info, popup_info, options
                )
            else:
                primary_layer = self._create_layer_from_service(
                    layer_list[0], drawing_info, popup_info, options, postprocess=False
                )

            self._postprocess_item(primary_layer, options)

        # ------------------------------------------------------
        # 5. ITEM WITH TABLES (FeatureService etc.)
        # ------------------------------------------------------
        if hasattr(layer, "tables") and layer.tables:
            for t in layer.tables:
                tbl_layer = self._create_layer_from_service(
                    t, drawing_info, popup_info, options, postprocess=False
                )
                self._postprocess_item(tbl_layer, options)

        # Return
        return primary_layer

    def _unwrap_engine_raster(self, layer):
        """
        Normalizes raster inputs so the rest of the pipeline only deals with:
            - ImageServerRaster
            - Arcpy raster
            - Standard Raster

        The raster module wraps engine rasters internally; this helper cleanly
        unwraps those engine instances when appropriate, but NEVER unwraps
        non-engine Raster objects.
        """
        # Not a Raster -> return unchanged
        if not isinstance(layer, raster.Raster):
            return layer

        engine = getattr(layer, "_engine_obj", None)

        # If no engine or is Arcpy Raster -> nothing to unwrap
        if engine is None or (isinstance(engine, raster._ArcpyRaster)):
            return layer

        # ImageServerRaster or future types -> should always be unwrapped
        if isinstance(engine, raster._ImageServerRaster) or (
            hasattr(engine, "url") or hasattr(engine, "_url")
        ):
            return engine

        # Default: leave unchanged
        return layer

    def _extract_properties(self, layer) -> dict:
        """
        Returns a clean dict for any supported layer type.
        The returned dict is safe to mutate.
        """
        try:
            props = dict(layer.properties)
        except Exception:
            props = {}
        return props.copy()

    def _extract_item_id(self, props: dict, options: dict) -> str | None:
        """
        Determine the service-level itemId for the service
        """
        return (
            props.get("serviceItemId")
            or props.get("id")
            or (options.get("itemId") if options else None)
        )

    def _assign_unique_id(self, props: dict) -> None:
        """
        Assigns a unique id in
        """
        props["id"] = uuid.uuid4().hex[0:12]
        return props

    def _resolve_layer_url(self, layer) -> str:
        """
        Determines the correct URL for a layer or table being added.
        Handles private to public conversion, MapImageLayer child layers,
        subtype group parents, and non-service layers
        """
        # 1. Local Rasters (arcpy) have a catalog path
        if isinstance(layer, raster.Raster):
            if isinstance(layer._engine_obj, raster._ArcpyRaster):
                return layer.catalog_path
            if isinstance(layer._engine_obj, raster._ImageServerRaster):
                return layer._engine_obj._url

        # 2. Service based layers always have _url
        url = getattr(layer, "_url", None)

        # 3. Subtype Group Layers/Tables have no but have a parent
        if url is None and hasattr(layer, "_group_layer"):
            # Use the parent layer URL
            parent = layer._group_layer
            return getattr(parent, "url", None)

        # 4. If still no URL, try 'url' property
        if url is None:
            url = getattr(layer, "url", None)

        # 5. WMTS, WMS, KML, etc. may have 'operational layer url'
        if url is None and hasattr(layer, "operational_layer_json"):
            url = layer._operational_layer_json.get("url", None)

        # 6. If AGOL -> no need for private to public conversion
        if self._source._gis._is_agol:
            return url

        # 7. Private to public URL conversion if needed
        if self._source._gis._use_private_url_only:
            if isinstance(layer, arcgis_layers.MapImageLayer):
                return url
            return self._private_to_public_url(layer)

        # 8. Default: return as is
        return url

    def _get_ld_as_dict(self, layer) -> dict:
        """
        Extract layer definition from layer.properties or from item data.
        """
        props = dict(layer.properties)

        # If layer came from an item with serviceItemId, merge service LD
        if props.get("serviceItemId"):
            item = self._source._gis.content.get(props["serviceItemId"])
            item_def = item.get_data() if item else None

            if item_def and "layers" in item_def:
                for lyr_json in item_def["layers"]:
                    if lyr_json.get("id") == props.get("id"):
                        base = {**props, **lyr_json.get("layerDefinition", {})}
                        return copy.deepcopy(base)

        # fallback
        if "layerDefinition" in props:
            return copy.deepcopy(props["layerDefinition"])

        # final fallback: layer.properties
        return copy.deepcopy(props)

    def _normalize_layer_definition(self, ld):
        """
        Apply core spec-normalization on layerDefinition dict.
        """
        # Make sure type is valid (webmap requirement)
        if ld.get("type") not in ("Feature Layer", "Table"):
            ld["type"] = "Feature Layer"

        # Fix fields list (esriFieldTypeBigInteger, DateOnly, etc.)
        if "fields" in ld:
            ld = self._fix_fields_type(ld)

        # Fix htmlPopupType
        if ld.get("htmlPopupType") == "esriServerHTMLPopupTypeNull":
            ld["htmlPopupType"] = "esriServerHTMLPopupTypeNone"

        # Remove non-int id (e.g., GeoJSON layers)
        if "id" in ld and not isinstance(ld["id"], int):
            ld.pop("id")

        return ld

    def _merge_drawing_info(self, ld, drawing_info):
        """
        Merges input drawing_info into the LD.
        Converts renderer dataclasses to dicts and normalizes structure.
        """
        drawing = dict(drawing_info)

        # Convert renderer dataclass → dict → normalized dataclass
        if isinstance(drawing.get("renderer"), _models.BaseModel):
            drawing["renderer"] = drawing["renderer"].dict()

        if isinstance(drawing.get("renderer"), dict):
            drawing["renderer"] = rm.FactoryWorker(
                renderer_type=drawing["renderer"]["type"],
                renderer=drawing["renderer"],
            )

        # LD may store drawingInfo as drawingInfo or drawing_info
        if "drawingInfo" in ld:
            ld["drawingInfo"] = {**ld["drawingInfo"], **drawing}
        elif "drawing_info" in ld:
            ld["drawingInfo"] = {**ld["drawing_info"], **drawing}
            ld.pop("drawing_info")
        else:
            ld["drawingInfo"] = drawing

    def _normalize_time_info(self, ld):
        if "timeInfo" not in ld or not ld["timeInfo"]:
            return

        time_info = ld["timeInfo"]

        units = time_info.get("timeIntervalUnits")
        if units:
            try:
                time_info["timeIntervalUnits"] = _model_enums.TimeIntervalUnits(units)
            except Exception:
                time_info["timeIntervalUnits"] = (
                    _model_enums.TimeIntervalUnits.esri_time_units_unknown
                )

    def _create_layer_definition(self, layer, drawing_info):
        """
        Build a layer definition dict for FeatureLayer/Table/CSV/GEOJSON/...

        Returns a mutable dict suitable for further processing.
        """
        # Step 1: Get base layer definition
        if isinstance(layer, _models.FeatureCollection):
            ld = layer.layers[0].layer_definition.model_dump(
                mode="python",
                exclude_none=True,
                by_alias=True,
            )
        else:
            ld = self._get_ld_as_dict(layer)

        # Step 2: Normalize the definition
        ld = self._normalize_layer_definition(ld)

        # Step 3: Merge drawing info
        if drawing_info:
            self._merge_drawing_info(ld, drawing_info)

        # Step 4: Time info normalization
        self._normalize_time_info(ld)

        # Step 5: Capabilities normalization
        if isinstance(ld.get("capabilities"), list):
            ld["capabilities"] = ",".join(ld["capabilities"])

        return ld

    def _extract_builtin_popup(self, layer):
        """
        Extract popupInfo from:
        - layer.properties
        - itemData.layers[*].popupInfo (if serviceItemId present)
        """
        props = dict(getattr(layer, "properties", {}))
        popup = props.get("popupInfo") or {}

        # Look up item-level popup if serviceItemId is present
        service_id = props.get("serviceItemId")
        if service_id:
            item = self._source._gis.content.get(service_id)
            data = item.get_data() if item else None
            if data and "layers" in data:
                for lyr in data["layers"]:
                    if lyr.get("id") == props.get("id") and "popupInfo" in lyr:
                        popup = lyr["popupInfo"]
                        break

        # FeatureCollection pydantic (SEDf/GeoJSON/OGC)
        if isinstance(layer, _models.FeatureCollection):
            layer0 = layer.layers[0]
            if layer0.popup_info:
                popup = layer0.popup_info.model_dump(
                    mode="python",
                    exclude_none=True,
                    by_alias=True,
                )

        return popup or {}

    def _auto_generate_popup(self, layer):
        """
        Create a minimal PopupInfo from the layer fields.
        """
        try:
            if isinstance(layer, _models.FeatureCollection):
                ld = layer.layers[0].layer_definition
                fields = ld.fields
                title = ld.name or "Layer"
                object_id = ld.object_id_field
            else:
                props = dict(layer.properties)
                fields = props.get("fields", [])
                title = props.get("name", "Layer")
                object_id = props.get("objectIdField")

            field_infos = []
            for f in fields:
                name = f if isinstance(f, str) else f.get("name")
                alias = name if isinstance(f, str) else f.get("alias", name)
                field_infos.append(
                    _models.FieldInfo(
                        field_name=name,
                        label=alias,
                        visible=(name != object_id),
                    )
                )

            return _models.PopupInfo(field_infos=field_infos, title=title)

        except Exception:
            return None

    def _create_popup_dataclass(self, layer, popup_info):
        """
        Build a PopupInfo dataclass for the layer/table.
        """
        # Step 1: Collect original popup candidates
        orig = self._extract_builtin_popup(layer)

        # Step 2: Normalize provided popup_info
        if isinstance(popup_info, _models.PopupInfo):
            new_pi = popup_info.model_dump(
                mode="python",
                exclude_none=True,
                by_alias=True,
            )
        elif isinstance(popup_info, dict):
            new_pi = popup_info
        else:
            new_pi = {}

        merged = {**orig, **new_pi} if new_pi else orig

        # Step 3: If merged is non-empty, create PopupInfo dataclass
        if merged:
            return _models.PopupInfo(**merged)

        # Step 4: Try to auto-generate basic popup info from fields
        return self._auto_generate_popup(layer)

    def _is_subtype_layer(self, props: dict) -> bool:
        """
        Returns True if a layer (FeatureLayer or Table) supports subtypes.
        Subtype layers require BOTH subtypeField and subtypes list to be present.
        """
        if not isinstance(props, dict):
            return False

        field = props.get("subtypeField")
        subtypes = props.get("subtypes")

        # Must have a subtype field AND a non-empty subtype list
        if not field:
            return False
        if not isinstance(subtypes, list) or len(subtypes) == 0:
            return False

        return True

    def _get_subtype_layer_definition(self, layer) -> dict:
        """
        Get the layer definition for the whole subtype group layer.
        """
        props = dict(layer.properties)
        service_item_id = props.get("serviceItemId")
        if not service_item_id:
            # return generic layer definition
            return {}

        # Get the item definition
        item = self._source._gis.content.get(props["serviceItemId"])
        item_def = item.get_data() if item else None

        if not item_def:
            # return generic layer definition
            return {}

        # Decide if we are looking in layers or tables
        if isinstance(layer, features.Table):
            # find the table definition by the table id
            if "tables" in item_def:
                for tbl_json in item_def["tables"]:
                    if tbl_json.get("id") == props.get("id"):
                        return copy.deepcopy(tbl_json)
        if isinstance(layer, features.FeatureLayer):
            # find the layer definition by the layer id
            if "layers" in item_def:
                for lyr_json in item_def["layers"]:
                    if lyr_json.get("id") == props.get("id"):
                        return copy.deepcopy(lyr_json)
        return {}

    def _create_subtype_group_layer(self, layer, url, item_id, props):
        subtype_field = props.get("subtypeField", None)
        subtype_defs = props.get("subtypes", []) or []
        subtype_group_layer_def = self._get_subtype_layer_definition(layer)
        parent_title = subtype_group_layer_def.get(
            "title", props.get("name", uuid.uuid4().hex[0:7])
        )

        # Build children subtype layers
        children = []
        for s in subtype_defs:
            code = s.get("code")
            name = s.get("name", f"Subtype {code}")
            ld = {}
            popup = {}
            visibility = True
            opacity = 1.0
            disable_popup = False
            for lyr in subtype_group_layer_def.get("layers", []):
                if lyr.get("subtypeCode") == code:
                    ld = lyr.get("layerDefinition", {})
                    popup = lyr.get("popupInfo", {})
                    visibility = lyr.get("visibility", True)
                    opacity = lyr.get("opacity", 1.0)
                    name = lyr.get("title", name)
                    disable_popup = lyr.get("disablePopup", False)
                    break

            # Create subtype layer dataclass
            children.append(
                _models.SubtypeLayer(
                    id=str(uuid.uuid4().hex[:12]),
                    title=name,
                    subtypeCode=code,
                    subtype_field=subtype_field,
                    layerType="ArcGISFeatureLayer",
                    popupInfo=popup,
                    layerDefinition=ld,
                    visibility=visibility,
                    opacity=opacity,
                    disablePopup=disable_popup,
                )
            )

        return _models.SubtypeGroupLayer(
            **props,
            title=subtype_group_layer_def.get("title", parent_title),
            url=url,
            itemId=item_id,
            layerType="SubtypeGroupLayer",
            layers=children,
        )

    def _create_subtype_group_table(self, layer, url, item_id, props):
        subtype_field = props.get("subtypeField", None)
        subtype_defs = props.get("subtypes", []) or []
        subtype_group_table_def = self._get_subtype_layer_definition(layer)
        parent_title = subtype_group_table_def.get(
            "title", props.get("name", uuid.uuid4().hex[0:7])
        )

        # Build children subtype layers
        children = []
        for s in subtype_defs:
            code = s.get("code")
            name = s.get("name", f"Subtype {code}")
            ld = {}
            popup = {}
            visibility = True
            opacity = 1.0
            disable_popup = False
            for lyr in subtype_group_table_def.get("layers", []):
                if lyr.get("subtypeCode") == code:
                    ld = lyr.get("layerDefinition", {})
                    popup = lyr.get("popupInfo", {})
                    visibility = lyr.get("visibility", True)
                    opacity = lyr.get("opacity", 1.0)
                    name = lyr.get("title", name)
                    disable_popup = lyr.get("disablePopup", False)
                    break
            children.append(
                _models.SubtypeTable(
                    id=str(uuid.uuid4().hex[:12]),
                    title=name,
                    subtypeCode=code,
                    subtype_field=subtype_field,
                    layerType="ArcGISTable",
                    popupInfo=popup,
                    layerDefinition=ld,
                    visibility=visibility,
                    opacity=opacity,
                    disablePopup=disable_popup,
                )
            )

        return _models.SubtypeGroupTable(
            title=parent_title,
            url=url,
            itemId=item_id,
            layerType="SubtypeGroupTable",
            tables=children,
        )

    # --------------------------------------------------------------------
    # Service / Layer Dispatcher
    # --------------------------------------------------------------------
    def _handle_feature_layer(self, layer, props, url, item_id, ld, popup, options):
        return _models.FeatureLayer(
            **props,
            url=url,
            layerDefinition=ld,
            popupInfo=popup,
            itemId=item_id,
            title=props.get("name", uuid.uuid4().hex[:7]),
        )

    def _handle_table(self, layer, props, url, item_id, ld, popup, options):
        return _models.Table(
            **props,
            url=url,
            layerDefinition=ld,
            popupInfo=popup,
            itemId=item_id,
            title=props.get("name", uuid.uuid4().hex[:7]),
        )

    def _handle_vector_tile_layer(self, layer, props, url, item_id, ld, popup, options):
        style_url = f"{url}/resources/styles/root.json"
        return _models.VectorTileLayer(
            **props,
            url=url,
            itemId=item_id,
            title=props.get("name", uuid.uuid4().hex[:7]),
            styleUrl=style_url,
            isReference=False,
        )

    def _handle_csv_layer(self, layer, props, url, item_id, ld, popup, options):
        props.pop("layerDefinition", None)
        return _models.CSVLayer(
            **props,
            layerDefinition=ld,
            popupInfo=popup,
            itemId=item_id,
        )

    def _handle_georss_layer(self, layer, props, url, item_id, ld, popup, options):
        return _models.GeoRSSLayer(
            **props,
            itemId=item_id,
        )

    def _handle_geojson_layer(self, layer, props, url, item_id, ld, popup, options):
        return _models.GeoJSONLayer(
            **props,
            layerDefinition=ld,
            popupInfo=popup,
            itemId=item_id,
        )

    def _handle_ogc_fs(self, layer, props, url, item_id, ld, popup, options):
        # First collection
        coll = list(layer.collections)[0]
        return self._handle_ogc_collection(
            coll, props, url, item_id, ld, popup, options
        )

    def _handle_ogc_collection(self, layer, props, url, item_id, ld, popup, options):
        sedf = layer.query("1=1").spatial
        fs = sedf.to_featureset()
        fc = features.FeatureCollection.from_featureset(fs)
        fc = _models.FeatureCollection(layers=fc.layer["layers"])

        ld = self._create_layer_definition(fc, options.get("drawing_info"))
        popup = self._create_popup_dataclass(fc, options.get("popup_info"))

        return _models.OGCFeatureLayer(
            url=url,
            collectionId=layer.properties["id"],
            title=layer.properties["title"],
            layerDefinition=ld,
            popupInfo=popup,
        )

    def _handle_map_service_layer(self, layer, props, url, item_id, ld, popup, options):
        layer_type = (
            _models.TiledMapServiceLayer
            if "TilesOnly" in props.get("capabilities", [])
            else _models.MapServiceLayer
        )
        title = props.get("name", props.get("mapName", "Map Service"))
        return layer_type(**props, url=url, itemId=item_id, title=title)

    def _handle_oriented_imagery(self, layer, props, url, item_id, ld, popup, options):
        title = props.get("name", uuid.uuid4().hex[:7])
        return _models.OrientedImageryLayer(
            **props,
            layerDefinition=ld,
            popupInfo=popup,
            itemId=item_id,
            url=url,
            title=title,
        )

    def _handle_kml_layer(self, layer, props, url, item_id, ld, popup, options):
        return _models.KMLLayer(**props, itemId=item_id)

    def _handle_wms_layer(self, layer, props, url, item_id, ld, popup, options):
        return _models.WMSLayer(**layer._operational_layer_json)

    def _handle_wmts_layer(self, layer, props, url, item_id, ld, popup, options):
        if options and "layer" in options:
            lyr_json = layer.operational_layer_json(options["layer"])
        else:
            lyr_json = layer._operational_layer_json

        info = lyr_json.get("wmtsInfo", {})
        layer_id = info.get("layerIdentifier")

        title = (
            lyr_json["title"]["#text"]
            if isinstance(lyr_json["title"], dict)
            else layer_id
        )

        wmts_info = _models.WebMapTileServiceInfo(
            url=info["url"],
            layer_identifier=layer_id,
            tile_matrix_set=info["tileMatrixSet"][0],
        )

        tile_info = lyr_json.get("tileInfo", {})
        tile_info["spatialReference"] = {"wkid": layer._spatial_reference}

        return _models.WebTiledLayer(
            **props,
            item_id=layer._id,
            title=title,
            wmts_info=wmts_info,
            tile_info=_models.TileInfo(**tile_info),
            url=url,
            min_scale=layer._min_scale,
            max_scale=layer._max_scale,
            opacity=layer._opacity,
            template_url=lyr_json.get("templateUrl"),
            full_extent=lyr_json.get("fullExtent"),
        )

    def _handle_scene_layer(self, layer, props, url, item_id, ld, popup, options):
        title = props.get("name", props.get("serviceName", "Scene Layer"))
        props.pop("layerType", None)  # JS API quirk
        return _models.SceneLayer(
            **props,
            url=url,
            layerDefinition=ld,
            itemId=item_id,
            title=title,
        )

    def _handle_building_layer(self, layer, props, url, item_id, ld, popup, options):
        title = props.get("name", props.get("serviceName", "Building Layer"))
        return _models.BuildingSceneLayer(
            **props,
            url=url,
            layerDefinition=ld,
            itemId=item_id,
            title=title,
            sublayers=props.get("layers", []),
        )

    def _handle_pointcloud_layer(self, layer, props, url, item_id, ld, popup, options):
        title = props.get("name", props.get("serviceName", "Point Cloud Layer"))
        return _models.PointCloudLayer(
            **props,
            url=url,
            layerDefinition=ld,
            popupInfo=popup,
            itemId=item_id,
            title=title,
        )

    def _handle_integrated_mesh_layer(
        self, layer, props, url, item_id, ld, popup, options
    ):
        title = props.get("name", props.get("serviceName", "Integrated Mesh Layer"))
        return _models.IntegratedMeshLayer(
            **props,
            url=url,
            layerDefinition=ld,
            itemId=item_id,
            title=title,
        )

    def _handle_voxel_layer(self, layer, props, url, item_id, ld, popup, options):
        ld_voxel = _models.VoxelLayerDefinition(**ld)
        title = props.get("name", props.get("serviceName", "Voxel Layer"))
        return _models.VoxelLayer(
            **props,
            url=url,
            layerDefinition=ld_voxel,
            popupInfo=popup,
            itemId=item_id,
            title=title,
        )

    def _handle_map_image_layer(self, layer, props, url, item_id, ld, popup, options):
        # Determine tiled vs dynamic
        try:
            is_tiled = (
                layer.container is not None
                and "TilesOnly" in layer.container.properties.get("capabilities", [])
            )
        except Exception:
            is_tiled = "TilesOnly" in props.get("capabilities", [])

        layer_type = (
            _models.TiledMapServiceLayer if is_tiled else _models.MapServiceLayer
        )

        title = props.get("name", props.get("mapName", "Map Image Layer"))

        return layer_type(
            **props,
            url=url,
            itemId=item_id,
            title=title,
        )

    def _handle_map_raster_layer(self, layer, props, url, item_id, ld, popup, options):
        title = props.get("name", uuid.uuid4().hex[:7])
        return _models.TiledMapServiceLayer(
            **props,
            url=url,
            itemId=item_id,
            title=title,
        )

    def _handle_imagery_layer(self, layer, props, url, item_id, ld, popup):
        # Rendering + mosaic rules
        rr = self._get_rendering_rule(layer)
        mr = self._get_mosaic_rule(layer)

        datastore_raster = self._get_datastore_raster(layer)
        custom_params = {"Raster": datastore_raster} if datastore_raster else None

        layer_type = (
            _models.TiledImageServiceLayer
            if layer.tiles_only
            else _models.ImageServiceLayer
        )

        title = props.get("name", uuid.uuid4().hex[:7])

        return layer_type(
            **props,
            url=url,
            layerDefinition=ld,
            popupInfo=popup,
            itemId=item_id,
            title=title,
            noData=0,
            rendering_rule=rr,
            mosaic_rule=mr,
            custom_parameters=custom_params,
        )

    def _handle_general_raster(self, layer, props, url, item_id, ld, popup, options):
        # If the engine is arcpy → use the helper
        if isinstance(layer._engine_obj, raster._ArcpyRaster):
            return self._add_local_raster(layer, options)

        # Otherwise treat as ImageServiceLayer using catalog_path
        rr = self._get_rendering_rule(layer)
        mr = self._get_mosaic_rule(layer)

        title = layer.name or uuid.uuid4().hex[:7]

        return _models.ImageServiceLayer(
            **props,
            url=layer.catalog_path,
            layerDefinition=ld,
            popupInfo=popup,
            itemId=item_id,
            title=title,
            noData=0,
            rendering_rule=rr,
            mosaic_rule=mr,
        )

    _SERVICE_DISPATCH = {
        # Vector & Feature Layers
        features.FeatureLayer: _handle_feature_layer,
        features.Table: _handle_table,
        arcgis_layers.MapFeatureLayer: _handle_feature_layer,
        # Tile & Vector Tile
        arcgis_layers.VectorTileLayer: _handle_vector_tile_layer,
        # CSV / GeoRSS / GeoJSON
        arcgis_layers.CSVLayer: _handle_csv_layer,
        arcgis_layers.GeoRSSLayer: _handle_georss_layer,
        arcgis_layers.GeoJSONLayer: _handle_geojson_layer,
        # OGC
        arcgis_layers.OGCFeatureService: _handle_ogc_fs,
        arcgis_layers.OGCCollection: _handle_ogc_collection,
        # MapService / ImageService Layers
        arcgis_layers.MapServiceLayer: _handle_map_service_layer,
        arcgis_layers.MapImageLayer: _handle_map_image_layer,
        arcgis_layers.MapRasterLayer: _handle_map_raster_layer,
        # Oriented Imagery
        features.layer.OrientedImageryLayer: _handle_oriented_imagery,
        # KML / WMS / WMTS
        arcgis_layers.KMLLayer: _handle_kml_layer,
        arcgis_layers.WMSLayer: _handle_wms_layer,
        arcgis_layers.WMTSLayer: _handle_wmts_layer,
        # Scene / 3D Layers
        arcgis_layers.SceneLayer: _handle_scene_layer,
        arcgis_layers.Object3DLayer: _handle_scene_layer,
        arcgis_layers.Point3DLayer: _handle_scene_layer,
        arcgis_layers.BuildingLayer: _handle_building_layer,
        arcgis_layers.PointCloudLayer: _handle_pointcloud_layer,
        arcgis_layers.IntegratedMeshLayer: _handle_integrated_mesh_layer,
        arcgis_layers.VoxelLayer: _handle_voxel_layer,
    }

    # --------------------------------------------------------------------
    # Main Service / Layer Handler
    # --------------------------------------------------------------------
    def _create_layer_from_service(
        self,
        layer,
        drawing_info=None,
        popup_info=None,
        options=None,
        *,
        postprocess: bool = True,
    ):
        """
        Public entry point for turning a service/layer/item into a web map layer model.

        - For container-like inputs (Items / lists), delegates to `_preprocess_item`,
        which is responsible for calling `_postprocess_item` on each created layer.
        - For simple layer objects (FeatureLayer, Table, Raster, etc.), uses
        `_add_from_service` to build a single Pydantic model and, unless
        `postprocess=False`, inserts it into the web map via `_postprocess_item`.
        """

        # 1) Container inputs → let `_preprocess_item` own postprocessing
        if self._should_preprocess(layer):
            return self._preprocess_item(layer, drawing_info, popup_info, options)

        # 2) Simple layer → build model, optionally postprocess here
        model = self._add_from_service(
            layer,
            drawing_info=drawing_info,
            popup_info=popup_info,
            options=options,
        )

        if postprocess and model is not None:
            self._postprocess_item(model, options)

        return model

    def _add_from_service(
        self,
        layer,
        drawing_info=None,
        popup_info=None,
        options=None,
    ):
        """
        Convert a Service/Layer/Item/list into the correct Pydantic layer model.
        """

        # ----------------------------------------------------------
        # Step 1: Handle list or Item (Group Layer / Multi-layer Items)
        # ----------------------------------------------------------
        if self._should_preprocess(layer):
            return self._preprocess_item(layer, drawing_info, popup_info, options)

        # ----------------------------------------------------------
        # Step 2: Normalize special raster cases
        # ----------------------------------------------------------
        layer = self._unwrap_engine_raster(layer)

        # ----------------------------------------------------------
        # Step 3: Extract base metadata
        # ----------------------------------------------------------
        props = self._extract_properties(layer)
        item_id = self._extract_item_id(props, options)
        self._assign_unique_id(props)

        # ----------------------------------------------------------
        # Step 4: Resolve URL (public/private)
        # ----------------------------------------------------------
        url = self._resolve_layer_url(layer)

        # ----------------------------------------------------------
        # Step 5: Subtype handling (central)
        # ----------------------------------------------------------
        if self._is_subtype_layer(props):
            if isinstance(layer, features.Table):
                return self._create_subtype_group_table(layer, url, item_id, props)
            else:
                return self._create_subtype_group_layer(layer, url, item_id, props)

        # ----------------------------------------------------------
        # Step 6: Build layerDefinition + popup
        # ----------------------------------------------------------
        ld = self._create_layer_definition(layer, drawing_info)
        popup = self._create_popup_dataclass(layer, popup_info)

        # ----------------------------------------------------------
        # Step 7: Dispatch to correct handler by type
        # ----------------------------------------------------------
        handler = self._SERVICE_DISPATCH.get(type(layer))
        if handler:
            return handler(self, layer, props, url, item_id, ld, popup, options)

        # ----------------------------------------------------------
        # Step 8: Raster or unsupported type
        # ----------------------------------------------------------
        if isinstance(layer, raster.ImageryLayer):
            return self._handle_imagery_layer(layer, props, url, item_id, ld, popup)

        if isinstance(layer, raster.Raster):
            return self._handle_general_raster(
                layer, props, url, item_id, ld, popup, options
            )

        raise ValueError(
            f"Layer type {type(layer)} not supported or incorrectly formatted."
        )

    def update_layer(
        self,
        index=None,
        labeling_info=None,
        renderer=None,
        scale_symbols=None,
        transparency=None,
        options=None,
        form=None,
        **kwargs,
    ):
        """
        This method can be used to update certain properties on a layer that is in your scene.
        """
        # was this method called from the group layer class?
        group_layer = kwargs.pop("group_layer", False)

        if not group_layer:
            if index is None:
                raise ValueError("Must specify index parameter.")

            # Get layer from list (should not be pydantic)
            # We will edit pydantic layer after, this needs to be passed into method
            layer = self._source.content.layers[index]
            # Error check
            if isinstance(layer, arcgismapping.BaseGroup):
                raise ValueError(
                    "The layer cannot be of type Group Layer. Use the `update_layer` method found in the Group Layer class."
                )
        if not options and not (
            isinstance(layer, features.FeatureCollection)
            or isinstance(layer, features.FeatureLayer)
            or isinstance(layer, arcgis_layers.GeoJSONLayer)
            or isinstance(layer, arcgis_layers.CSVLayer)
        ):
            raise ValueError(
                "Only Feature Collections, Feature Layers, GeoJSON Layers, and CSV Layers can have their drawing info edited."
            )

        if renderer or scale_symbols or labeling_info or transparency:
            # Retrieve the existing drawing info
            existing_drawing_info = (
                self.pydantic_class.operational_layers[index]
                .dict()
                .get("layerDefinition", {})
                .get("drawingInfo", {})
            )

            # Update the existing drawing info with new properties
            if renderer:
                existing_drawing_info["renderer"] = renderer
            if scale_symbols in [True, False]:
                existing_drawing_info["scale_symbols"] = scale_symbols
            if labeling_info:
                existing_drawing_info["labeling_info"] = labeling_info
            if transparency is not None:
                existing_drawing_info["transparency"] = transparency

            # Assign the updated drawing info back to the layer definition
            if self.pydantic_class.operational_layers[index].layer_definition is None:
                # Create a new LayerDefinition if it doesn't exist
                self.pydantic_class.operational_layers[index].layer_definition = (
                    _models.LayerDefinition(
                        drawing_info=_models.DrawingInfo(**existing_drawing_info)
                    )
                )
            else:
                # Update the existing LayerDefinition with the new drawing info
                self.pydantic_class.operational_layers[
                    index
                ].layer_definition.drawing_info = _models.DrawingInfo(
                    **existing_drawing_info
                )

        if options is not None:
            # handle layer definition separately
            if "layerDefinition" in options:
                # get existing layer definition
                existing_layer_definition = layer.layer_definition
                # update the layer definition with the new options
                existing_layer_definition.update(options["layerDefinition"])
                # set the layer definition back to the layer but on the webmap
                self._definition.operational_layers[index].layer_definition = (
                    existing_layer_definition
                )
                # remove from options so we don't set it again
                del options["layerDefinition"]

            # make the edits straight in the webmap definition
            layer = self.pydantic_class.operational_layers[index]
            # if an options dictionary was passed in, set the available attributes
            for key, value in options.items():
                # make sure key is in snake case
                key = "".join(
                    ["_" + c.lower() if c.isupper() else c for c in key]
                ).lstrip("_")
                if hasattr(layer, key):
                    setattr(self.pydantic_class.operational_layers[index], key, value)

        if form is not None:
            # Assign the new FormInfo
            form_info = _models.FormInfo(**form.dict())
            self.pydantic_class.operational_layers[index].form_info = form_info

        # Update the webmap dict on the widget so layer changes are reflected
        self._source._update_source()

    def remove_layer(self, index: int | None = None):
        """
        Remove a layer from the scene either by specifying the index or passing in the layer dictionary.

        ==================      =====================================================================
        **Parameter**           **Description**
        ------------------      ---------------------------------------------------------------------
        index                   Optional integer specifying the index for the layer you want to remove.
                                To see a list of layers use the layers property.
        ==================      =====================================================================
        """
        if index is None:
            raise ValueError(
                "Must specify index parameter. You can see a list of all your layers by calling the `layers` property."
            )

        # Remove from pydantic dataclass
        try:
            del self.pydantic_class.operational_layers[index]
        except Exception:
            logging.error("Layer index not found.")
            return
        # Remove from layers property
        del self._source.content.layers[index]

    def remove_all(self):
        """
        Remove all layers and tables from the map.
        """
        # Remove from pydantic dataclass
        # check that the operational layers and tables exist
        if hasattr(self.pydantic_class, "operational_layers") and isinstance(
            self.pydantic_class.operational_layers, (list, tuple)
        ):
            self.pydantic_class.operational_layers.clear()
        if hasattr(self.pydantic_class, "tables") and isinstance(
            self.pydantic_class.tables, (list, tuple)
        ):
            self.pydantic_class.tables.clear()
        # Remove from properties
        self._source.content.layers.clear()
        self._source.content.tables.clear()

    def remove_table(self, index: int | None = None):
        """
        Remove a table from the map either by specifying the index or passing in the table object.

        ==================      =====================================================================
        **Parameter**           **Description**
        ------------------      ---------------------------------------------------------------------
        index                   Optional integer specifying the index for the table you want to remove.
                                To see a list of tables use the `tables` property.
        ==================      =====================================================================
        """
        if index is None:
            raise ValueError(
                "You must provide a table index. See your map's tables by calling the `tables` property."
            )

        # Remove from pydantic dataclass
        del self.pydantic_class.tables[index]
        # Remove from tables property
        del self._source.content.tables[index]

    ###################### Map and Scene Methods ######################

    def _save(
        self,
        item_properties: dict[str, Any],
        thumbnail: Optional[str] = None,
        metadata: Optional[str] = None,
        owner: Optional[str] = None,
        folder: Optional[str] = None,
    ):
        # Check a user is logged in
        if not self._source._gis.users.me:
            raise RuntimeError("You must be logged in to save a webmap or webscene.")

        # check item props are there
        if (
            "title" not in item_properties
            or "snippet" not in item_properties
            or "tags" not in item_properties
        ):
            raise RuntimeError(
                "title, snippet and tags are required in item_properties dictionary"
            )

        # fix the tags to be a string of comma separated values
        if isinstance(item_properties["tags"], list):
            item_properties["tags"] = ",".join(item_properties["tags"])

        # make sure authoring app and version are correct
        self.pydantic_class.authoring_app = "ArcGIS Python API"
        self.pydantic_class.authoring_app_version = arcgis.__version__

        if self.is_map:
            # Refresh to make sure all changes are in the webmap_dict
            # We exclude none values when saving to avoid saving unneccessary properties
            self._source._update_source()
            # Add to item properties
            item_properties["type"] = "Web Map"

            # check extent spatial reference
            self._check_extent_sr()

            # Set the extent to the current map extent
            item_properties["extent"] = self._source.extent

            # Update the initial state so that the mapviewer has the correct extent to render
            self._source._webmap_dict["initialState"]["viewpoint"][
                "targetGeometry"
            ] = self._source.extent
            self._source._webmap_dict["initialState"]["viewpoint"]["targetGeometry"][
                "spatialReference"
            ] = self._source._webmap_dict["spatialReference"]

            item_properties["text"] = json.dumps(
                self._source._webmap_dict, default=arcgis_utils._date_handler
            )
        else:
            item_properties["type"] = "Web Scene"
            # Set to current camera view if applicable
            self.pydantic_class.initial_state.viewpoint.camera = (
                _models.Camera(**self._source.camera)
                if self._source.camera
                else _models.Camera(
                    **{
                        "position": {
                            "spatialReference": {
                                "latestWkid": 3857,
                                "wkid": 102100,
                            },
                            "x": -0.00044117777277823567,
                            "y": -42336.301402091056,
                            "z": 20266096.34006851,
                        },
                        "heading": 1.7075472925031877e-06,
                        "tilt": 0.11968932646564065,
                    }
                )
            )
            # Refresh to make sure all changes are in the webscene_dict
            self._source._update_source()
            # Add to item properties

        if "typeKeywords" not in item_properties:
            item_properties["typeKeywords"] = self._eval_type_keywords()

        # Add as a new item to the portal
        # if the folder is a string, it will either get or create it
        # otherwise get the root folder
        if isinstance(folder, str):
            folder = self._source._gis.content.folders._get_or_create(folder, owner)
        elif folder is None:
            folder = self._source._gis.content.folders.get()  # root folder
        elif not isinstance(folder, arcgis_cm.Folder):
            raise ValueError("Folder must be a Folder object.")

        item_properties["thumbnail"] = thumbnail
        item_properties["metadata"] = metadata

        # add in folder class
        future_item = folder.add(
            item_properties,
            text=(self.pydantic_class.dict()),
        )
        new_item = future_item.result()

        # set to item property
        self._source.item = new_item

        return new_item

    def _update(
        self,
        item_properties: Optional[dict[str, Any]] = None,
        thumbnail: Optional[str] = None,
        metadata: Optional[str] = None,
    ):
        if self._source.item is not None:
            self.pydantic_class.authoring_app = "ArcGIS Python API"
            self.pydantic_class.authoring_app_version = arcgis.__version__
            self._source._update_source()

            if item_properties is None:
                item_properties = {}
            elif "tags" in item_properties and isinstance(
                item_properties["tags"], list
            ):
                item_properties["tags"] = ",".join(item_properties["tags"])

            if self.is_map:
                # Make sure all changes are in webmapdict
                # Set exclude none to True to avoid saving unnecessary parameters

                # Check the extent spatial reference of the map
                self._check_extent_sr()

                # Update the initial state so that the mapviewer has the correct extent to render
                self.pydantic_class.initial_state.viewpoint.target_geometry = (
                    _models.Extent(**self._source.extent)
                )

                item_properties["extent"] = self._source.extent

            item_properties["text"] = json.dumps(
                self.pydantic_class.dict(),
                default=arcgis_utils._date_handler,
            )
            if "typeKeywords" not in item_properties:
                item_properties["typeKeywords"] = self._eval_type_keywords()
            if "type" in item_properties:
                item_properties.pop("type")  # type should not be changed.
            return self._source.item.update(
                item_properties=item_properties,
                thumbnail=thumbnail,
                metadata=metadata,
            )
        else:
            raise RuntimeError(
                "Item object missing, you should use `save()` method if you are creating a "
                "new web map item"
            )

    def _check_extent_sr(self):
        """
        Check the extent spatial reference and make sure it matches that of the map before saving and updating.
        This method is important when a user has set the extent and the map is not rendered. When a
        map is rendered, the extent is automatically updated to match the spatial reference of the map.
        """
        if self._source.extent is not None or self._source.extent != {}:
            if (
                self._source.extent.get("spatialReference")
                != self.pydantic_class.spatial_reference
            ):
                self._source.extent = self._reproject_extent(
                    [self._source.extent],
                    self.pydantic_class.spatial_reference.dict(),
                )

    def _eval_type_keywords(self) -> list[str]:
        """
        Evaluate and return updated type keywords for the map item based on its capabilities.

        - Adds or removes 'Offline' if the map is offline-capable.
        - Adds or removes 'Collector' and 'Data Editing' if the map is collector-ready.
        """
        type_keywords = (
            set(self._source.item.typeKeywords) if self._source.item else set()
        )
        try:
            # Offline capability evaluation
            if "OfflineDisabled" not in type_keywords:
                (
                    type_keywords.add("Offline")
                    if self._is_offline_capable_map()
                    else type_keywords.discard("Offline")
                )
        except Exception:
            pass
        try:
            # Collector capability evaluation
            if "CollectorDisabled" not in type_keywords:
                (
                    type_keywords.update({"Collector", "Data Editing"})
                    if self._is_collector_ready_map()
                    else type_keywords.difference_update({"Collector", "Data Editing"})
                )
        except Exception:
            pass
        # Add other keywords
        type_keywords.add("ArcGIS API for Python")
        type_keywords.add("ArcGIS API for JavaScript")

        return list(type_keywords)

    def _is_offline_capable_map(self, layers=None) -> bool:
        """
        Determine whether all layers in the map are offline-capable.

        A map is offline-capable if:
        - All feature layers are sync-enabled.
        - All tiled layers allow tile export.
        - At least one tiled layer exists and allows export.
        - All group layers' sublayers also meet these conditions.
        """
        layers = layers or getattr(self._source.content, "layers", [])

        for layer in layers:
            if isinstance(layer, arcgismapping.GroupLayer):
                if not self._is_offline_capable_map(layer.layers):
                    return False

            elif isinstance(layer, features.FeatureLayer):
                capabilities = getattr(layer.properties, "capabilities", "")
                if "Sync" not in capabilities:
                    return False

            elif not getattr(layer.properties, "exportTilesAllowed", False):
                return False

        # At least one exportable layer is required
        return True

    def _is_collector_ready_map(self, layers=None) -> bool:
        """
        Determine whether at least one layer in the map is collector-ready.

        A map is collector-ready if it contains at least one editable feature layer
        (with Create, Update, Delete, or Editing capability).
        """
        editable_capabilities = {"Create", "Update", "Delete", "Editing"}
        layers = layers or getattr(self._source.content, "layers", [])
        collector_ready_found = False
        for layer in layers:
            # Handle group layers recursively
            if isinstance(layer, arcgismapping.GroupLayer):
                if self._is_collector_ready_map(layer.layers):
                    collector_ready_found = True
            else:
                capabilities = getattr(layer.properties, "capabilities", "")
                if any(cap in capabilities for cap in editable_capabilities):
                    collector_ready_found = True

        return collector_ready_found

    def _zoom_to_layer(self, item):
        # Get the target extent from the item passed in
        target_extent = self._get_extent(item)

        # Get the widget's spatial reference so we can project the extent
        target_sr = self.pydantic_class.spatial_reference.dict()

        # Transform target extent if needed
        if isinstance(target_extent, list):
            target_extent = self._flatten_list(target_extent)
            if len(target_extent) > 1:
                target_extent = self._get_master_extent(target_extent, target_sr)
            else:
                target_extent = target_extent[0]

        # Check if need to re-project
        if not (target_extent.get("spatialReference") == target_sr):
            target_extent = self._reproject_extent(target_extent, target_sr)

        # Sometimes setting extent will not work for the same target extent if we do it multiple times, doing this fixes that issue.
        # self._source.extent = self._source.extent
        self._source.extent = target_extent

    def _get_extent(self, item):
        if isinstance(item, raster.Raster):
            if isinstance(item._engine_obj, raster._ImageServerRaster):
                item = item._engine_obj
            elif isinstance(item._engine_obj, raster._ArcpyRaster):
                return dict(item.extent)
        if isinstance(item, _gis_mod.Item):
            return list(map(self._get_extent, item.layers))
        elif isinstance(item, list):
            return list(map(self._get_extent, item))
        elif isinstance(item, pd.DataFrame):
            return self._get_extent_of_dataframe(item)
        elif isinstance(item, features.FeatureSet):
            return self._get_extent(item.sdf)
        elif isinstance(item, features.FeatureCollection):
            props = dict(item.properties)
            return props.get("layerDefinition", {}).get("extent")
        elif isinstance(item, _gis_mod.Layer):
            try:
                if "extent" in item.properties:
                    return dict(item.properties.extent)
                elif "fullExtent" in item.properties:
                    return dict(item.properties["fullExtent"])
                elif "initialExtent" in item.properties:
                    return dict(item.properties["initialExtent"])
            except Exception:
                ext = item.extent
                return {
                    "spatialReference": {"wkid": 4326, "latestWkid": 4326},
                    "xmin": ext[0][1],
                    "ymin": ext[0][0],
                    "xmax": ext[1][1],
                    "ymax": ext[1][0],
                }
        else:
            raise Exception("Could not infer layer type")

    def _flatten_list(self, *unpacked_list):
        return_list = []
        for x in unpacked_list:
            if isinstance(x, (list, tuple)):
                return_list.extend(self._flatten_list(*x))
            else:
                return_list.append(x)
        return return_list

    def _get_extent_of_dataframe(self, sdf):
        if hasattr(sdf, "spatial"):
            sdf_ext = sdf.spatial.full_extent
            return {
                "spatialReference": sdf.spatial.sr,
                "xmin": sdf_ext[0],
                "ymin": sdf_ext[1],
                "xmax": sdf_ext[2],
                "ymax": sdf_ext[3],
            }
        else:
            raise Exception(
                "Could not add get extent of DataFrame it is not a spatially enabled DataFrame."
            )

    def _get_master_extent(self, list_of_extents, target_sr=None):
        if target_sr is None:
            target_sr = {"wkid": 102100, "latestWkid": 3857}
        # Check if any extent is different from one another
        varying_spatial_reference = False
        for extent in list_of_extents:
            if not target_sr == extent.get("spatialReference"):
                varying_spatial_reference = True
        if varying_spatial_reference:
            list_of_extents = self._reproject_extent(list_of_extents, target_sr)

        # Calculate master_extent
        master_extent = list_of_extents[0]
        for extent in list_of_extents:
            master_extent["xmin"] = min(master_extent["xmin"], extent["xmin"])
            master_extent["ymin"] = min(master_extent["ymin"], extent["ymin"])
            master_extent["xmax"] = max(master_extent["xmax"], extent["xmax"])
            master_extent["ymax"] = max(master_extent["ymax"], extent["ymax"])
        return master_extent

    def _reproject_extent(
        self, extents, target_sr={"wkid": 102100, "latestWkid": 3857}
    ):
        """Reproject Extent

        ==================      ====================================================================
        **Parameter**           **Description**
        ------------------      --------------------------------------------------------------------
        extents                 Extent or list of extents you want to project.
        ------------------      --------------------------------------------------------------------
        target_sr               The target Spatial Reference you want to get your extent in.
                                default is {'wkid': 102100, 'latestWkid': 3857}
        ==================      ====================================================================

        """
        if not isinstance(extents, list):
            extents = [extents]

        extents_to_reproject = {}
        for i, extent in enumerate(extents):
            current_sr = extent.get("spatialReference", {})
            if current_sr and not current_sr == target_sr:
                current_sr_str = str(current_sr)
                if current_sr_str not in extents_to_reproject:
                    extents_to_reproject[current_sr_str] = {}
                    extents_to_reproject[current_sr_str]["spatialReference"] = extent[
                        "spatialReference"
                    ]
                    extents_to_reproject[current_sr_str]["extents"] = []
                    extents_to_reproject[current_sr_str]["indexes"] = []
                extents_to_reproject[current_sr_str]["extents"].extend(
                    [
                        {"x": extent["xmin"], "y": extent["ymin"]},
                        {"x": extent["xmax"], "y": extent["ymax"]},
                    ]
                )
                extents_to_reproject[current_sr_str]["indexes"].append(i)

        for current_sr_str in extents_to_reproject:  # Re-project now
            reprojected_extents = arcgis.geometry.project(
                extents_to_reproject[current_sr_str]["extents"],
                in_sr=extents_to_reproject[current_sr_str]["spatialReference"],
                out_sr=target_sr,
                gis=self._source._gis,
            )
            for i in range(0, len(reprojected_extents), 2):
                source_idx = extents_to_reproject[current_sr_str]["indexes"][int(i / 2)]
                extents[source_idx] = {
                    "xmin": reprojected_extents[i]["x"],
                    "ymin": reprojected_extents[i]["y"],
                    "xmax": reprojected_extents[i + 1]["x"],
                    "ymax": reprojected_extents[i + 1]["y"],
                    "spatialReference": target_sr,
                }

        if len(extents) == 1:
            return extents[0]
        return extents

    def _sync_navigation(self, mapview):
        """
        The ``sync_navigation`` method synchronizes the navigation from one rendered Map/Scene to
        another rendered Map/Scene instance so panning/zooming/navigating in one will update the other.

        .. note::
            Users can sync more than two instances together by passing in a list of Map/Scene instances to
            sync. The syncing will be remembered

        ==================      ===================================================================
        **Parameter**           **Description**
        ------------------      -------------------------------------------------------------------
        mapview                 Either a single Map/Scene instance, or a list of ``Map`` or ``Scene``
                                instances to synchronize to.
        ==================      ===================================================================

        """
        if isinstance(mapview, list):
            # append the current Map/Scene to the list so it gets linked as well
            mapview.append(self._source)
            # Iterate through if list of maps and link all of them
            for i in range(len(mapview) - 1):
                for j in range(i + 1, len(mapview)):
                    # Extent is linked to zoom and scale on js side so we only need to link this.
                    l = link((mapview[i], "_view_state"), (mapview[j], "_view_state"))
                    # Keep track of links for each mapview
                    mapview[i]._linked_maps.append(l)
                    mapview[j]._linked_maps.append(l)
        elif self.is_map and isinstance(mapview, arcgismapping.Map):
            l = link((self._source, "_view_state"), (mapview, "_view_state"))
            self._source._linked_maps.append(l)
            mapview._linked_maps.append(l)
        elif self.is_map is False and isinstance(mapview, arcgismapping.Scene):
            l = link((self._source, "_view_state"), (mapview, "_view_state"))
            self._source._linked_maps.append(l)
            mapview._linked_maps.append(l)
        else:
            raise ValueError(
                "Please provide a valid list of Map instances or a single Map instance to link to the current Map."
            )

    def _unsync_navigation(self, mapview=None):
        """
        The ``unsync_navigation`` method unsynchronizes connections made to other rendered Map/Scene instances
        made via the sync_navigation method.

        ==================     ===================================================================
        **Parameter**           **Description**
        ------------------     -------------------------------------------------------------------
        mapview                Optional, either a single `Map` or `Scene` instance, or a list of
                               `Map` or `Scene` instances to unsynchronize. If not specified, will
                               unsynchronize all synced `Map` or `Scene` instances.
        ==================     ===================================================================
        """
        # Unlink all
        if mapview is None:
            for link_obj in self._source._linked_maps:
                link_obj.unlink()
                # clear list of links for source and target
                link_obj.source[0]._linked_maps.clear()
                link_obj.target[0]._linked_maps.clear()
                return

        if self.is_map:
            # Unlink some
            if isinstance(mapview, arcgismapping.Map):
                # Make a list since the logic will be the same as list
                mapview = [mapview]
        else:
            # Unlink some
            if isinstance(mapview, arcgismapping.Scene):
                # Make a list since the logic will be the same as list
                mapview = [mapview]

        if isinstance(mapview, list):
            # Iterate through if list of maps and link all of them
            for widget in mapview:
                # We want to unlink the list of widgets from this one
                widgets = [self._source, widget]
                for link_obj in self._source._linked_maps:
                    source_widget = link_obj.source[0]
                    target_widget = link_obj.target[0]

                    if source_widget in widgets and target_widget in widgets:
                        link_obj.unlink()
                        source_widget._linked_maps.remove(link_obj)
                        target_widget._linked_maps.remove(link_obj)

    def _export_to_html(
        self,
        path_to_file,
        title=None,
    ):
        """
        The ``export_to_html`` method takes the current state of the rendered map and exports it to a
        standalone HTML file that can be viewed in any web browser.

        By default, only publicly viewable layers will be visible in any
        exported html map. Specify ``credentials_prompt=True`` to have a user
        be prompted for their credentials when opening the HTML page to view
        private content.

        .. warning::
            Follow best security practices when sharing any HTML page that
            prompts a user for a password.

        .. note::
            You cannot successfully authenticate if you open the HTML page in a
            browser locally like file://path/to/file.html. The credentials
            prompt will only properly function if served over a HTTP/HTTPS
            server.

        ==================     ====================================================================
        **Parameter**           **Description**
        ------------------     --------------------------------------------------------------------
        path_to_file           Required string. The path to save the HTML file on disk.
        ------------------     --------------------------------------------------------------------
        title                  Optional string. The HTML title tag used for the HTML file.
        ==================     ====================================================================
        """
        js_api_path = self._source.js_api_path or "https://js.arcgis.com/4.30/"
        html_template = """
        <html>
            <head>
                <meta charset="utf-8" />
                <meta name="viewport" content="initial-scale=1, maximum-scale=1, user-scalable=no" />
                <title>{title}</title>

                <style>
                    html,
                    body,
                    #viewDiv {{
                        padding: 0;
                        margin: 0;
                        height: 100%;
                        width: 100%;
                    }}
                </style>

                <link rel="stylesheet" href="{js_api_path}esri/themes/light/main.css">
                <script src="{js_api_path}"></script>

                <script>
                    require(["esri/config", "esri/WebMap", "esri/views/MapView"], function(esriConfig, WebMap, MapView) {{


                        const map = WebMap.fromJSON({state});

                        const view = new MapView({{
                            map: map,
                            extent: {extent},
                            container: "viewDiv"
                        }});

                    }});
                </script>
            </head>
            <body>
                <div id="viewDiv"></div>
            </body>
        </html>
        """
        # Title
        if title is None:
            title = "Exported ArcGIS Map from Python API"

        # WebMap state to export
        state = json.dumps(self.pydantic_class.dict())
        extent = json.dumps(self._source.extent)

        # Create template
        rendered_template = html_template.format(
            title=title,
            js_api_path=js_api_path,
            state=state,
            extent=extent,
        )

        # Write to file
        with open(path_to_file, "w") as fp:
            fp.write(rendered_template)

        return True

    def _print(
        self,
        file_format: str,
        extent: dict[str, Any],
        dpi: int = 92,
        output_dimensions: tuple[float] = (500, 500),
        scale: Optional[float] = None,
        rotation: Optional[float] = None,
        spatial_reference: Optional[dict[str, Any]] = None,
        layout_template: str = "MAP_ONLY",
        time_extent: Optional[Union[tuple[int], list[int]]] = None,
        layout_options: Optional[dict[str, Any]] = None,
    ):
        # compose map options
        map_options: dict = {
            "extent": extent,
            "scale": scale,
            "rotation": rotation,
            "spatialReference": spatial_reference,
            "time": time_extent,
        }

        # compose export options
        export_options: dict = {"dpi": dpi, "outputSize": output_dimensions}

        map_dict: dict = self.pydantic_class.dict()
        # compose combined JSON
        print_options: dict = {
            "mapOptions": map_options,
            "operationalLayers": map_dict["operationalLayers"],
            "baseMap": map_dict["baseMap"],
            "exportOptions": export_options,
        }

        # add token parameter to the operational layers if token present
        if self._source._gis._session.auth.token is not None:
            for i in range(len(print_options["operationalLayers"])):
                print_options["operationalLayers"][i][
                    "token"
                ] = self._source._gis._session.auth.token

        if layout_options:
            print_options["layoutOptions"] = layout_options

        # execute printing, result is a DataFile
        result = self._export_map(
            web_map_as_json=print_options,
            format=file_format,
            layout_template=layout_template,
            gis=self._source._gis,
        )

        # process output
        return result.url

    def _export_map(
        self,
        web_map_as_json: Optional[dict] = None,
        format: str = """PDF""",
        layout_template: str = """MAP_ONLY""",
        gis=None,
        **kwargs,
    ):
        """
        The ``export_map`` function takes the state of the :class:`~arcgis.map.Map` object (for example, included services, layer visibility
        settings, client-side graphics, and so forth) and returns either (a) a page layout or
        (b) a map without page surrounds of the specified area of interest in raster or vector format.
        The input for this function is a piece of text in JavaScript object notation (JSON) format describing the layers,
        graphics, and other settings in the web map. The JSON must be structured according to the Map specification
        in the ArcGIS Help.
        .. note::
            The ``export_map`` tool is shipped with ArcGIS Server to support web services for printing, including the
            pre-configured service named ``PrintingTools``.
        ==================     ====================================================================
        **Parameter**           **Description**
        ------------------     --------------------------------------------------------------------
        web_map_as_json        Web Map JSON along with export options. See the
                            `Export Web Map Specifications <https://developers.arcgis.com/rest/services-reference/exportwebmap-specification.htm>`_
                            for more information on structuring this JSON.
        ------------------     --------------------------------------------------------------------
        format                 Format (str). Optional parameter.  The format in which the map image
                            for printing will be delivered. The following strings are accepted:
                            For example:
                                    PNG8
                            Choice list:
                                    ['PDF', 'PNG32', 'PNG8', 'JPG', 'GIF', 'EPS', 'SVG', 'SVGZ']
        ------------------     --------------------------------------------------------------------
        layout_template        Layout Template (str). Optional parameter.  Either a name of a
                            template from the list or the keyword MAP_ONLY. When MAP_ONLY is chosen
                            or an empty string is passed in, the output map does not contain any
                            page layout surroundings.
                            For example - title, legends, scale bar, and so forth
                            Choice list:
                                | ['A3 Landscape', 'A3 Portrait',
                                | 'A4 Landscape', 'A4 Portrait', 'Letter ANSI A Landscape',
                                | 'Letter ANSI A Portrait', 'Tabloid ANSI B Landscape',
                                | 'Tabloid ANSI B Portrait', 'MAP_ONLY'].
                            You can get the layouts configured with your GIS by calling the :meth:`get_layout_templates <arcgis.mapping.get_layout_templates>` function
        ------------------     --------------------------------------------------------------------
        gis                    The :class:`~arcgis.gis.GIS` to use for printing. Optional
                            parameter. When not specified, the active GIS will be used.
        ==================     ====================================================================
        Returns:
            A dictionary with URL to download the output file.
        """

        verbose = kwargs.pop("verbose", False)

        if gis is None:
            gis = arcgis.env.active_gis
        params = {
            "web_map_as_json": web_map_as_json,
            "format": format,
            "layout_template": layout_template,
            "gis": gis,
            "future": False,
        }
        params.update(kwargs)

        url = os.path.dirname(gis.properties.helperServices.printTask.url)
        tbx = geoprocessing.import_toolbox(url, gis=gis, verbose=verbose)
        basename = os.path.basename(gis.properties.helperServices.printTask.url)
        basename = geoprocessing._tool._camelCase_to_underscore(
            urllib.parse.unquote_plus(urllib.parse.unquote(basename))
        )

        fn = getattr(tbx, basename)
        return fn(**params)

    def _layer_info(self):
        """
        Return a table with three columns. One with the layer index, one with the layer title, and one with the class instance of the layer.

        :return: A pandas DataFrame with the layer information.
        """
        layers = self._source.content.layers
        layer_info = []
        for i, layer in enumerate(layers):
            title = self.pydantic_class.operational_layers[
                i
            ].title  # title is in the properties unlike with the python classes
            layer_info.append([i, title, layer])
        return pd.DataFrame(
            layer_info, columns=["Layer Index", "Layer Title", "Layer Class"]
        )

    def _table_info(self):
        """
        Return a table with three columns. One with the table index, one with the table title, and one with the class instance of the table.

        :return: A pandas DataFrame with the table information.
        """
        tables = self._source.content.tables
        table_info = []
        for i, table in enumerate(tables):
            title = self.pydantic_class.tables[
                i
            ].title  # title is in the properties unlike with the python classes
            table_info.append([i, title, table])
        return pd.DataFrame(
            table_info, columns=["Table Index", "Table Title", "Table Class"]
        )

    def _get_layer(self, title, is_table=False):
        """
        Get the layer object by title.

        :param title: The title of the layer.
        :return: The layer object.
        """
        # use the pydantic class operational layers to get the layer index since title is in every layer property
        if is_table:
            tables = self.pydantic_class.tables
            for i, table in enumerate(tables):
                if table.title == title:
                    return self._source.content.tables[i]
        layers = self.pydantic_class.operational_layers
        for i, layer in enumerate(layers):
            if layer.title == title:
                return self._source.content.layers[i]

    def _js_requirement(self):
        return "This version of arcgis-mapping requires the ArcGIS Maps SDK for JavaScript version 4.33. You can download it from https://developers.arcgis.com/javascript/latest/downloads/ ."

    ############################################## Basemap Methods ##############################################
    @property
    def basemaps(self):
        # List of basemaps from the basemap def file
        return list(basemapdef.basemap_dict.keys())

    @property
    def basemaps3d(self):
        # List of basemaps from the basemap def file
        return list(basemapdef3d.basemap_dict.keys())

    @property
    def basemap(self):
        # Current basemap in the widget
        # Set exclude none to True to avoid returning unnecessary properties
        return self.pydantic_class.base_map.dict()

    def _move_to_basemap(self, index: int):
        # Types accepted as basemap
        layer_types = [
            "ArcGISTiledMapServiceLayer",
            "ArcGISImageServiceLayer",
            "ArcGISImageServiceVectorLayer",
            "ArcGISMapServiceLayer",
            "ArcGISTiledImageServiceLayer",
            "VectorTileLayer",
            "WMS",
            "WebTiledLayer",
        ]

        # Get the pydantic layer
        layer = self.pydantic_class.operational_layers[index]
        # Check the type
        if layer.layer_type in layer_types:
            # Store initial states
            initial_operational_layers = list(self.pydantic_class.operational_layers)
            initial_base_map_layers = list(self.pydantic_class.base_map.base_map_layers)
            initial_widget_layers = list(self._source.content.layers)

            try:
                # Add to basemap layers
                self.pydantic_class.base_map.base_map_layers.append(layer)
                # Remove from operational layers
                del self.pydantic_class.operational_layers[index]
                # Remove from layers property
                del self._source.content.layers[index]
            except Exception as e:
                # Revert to initial state
                self.pydantic_class.operational_layers = initial_operational_layers
                self.pydantic_class.base_map.base_map_layers = initial_base_map_layers
                self._source.content.layers = initial_widget_layers

                # Raise the exception again to notify the caller
                raise e
        else:
            raise ValueError(
                "This layer type cannot be added as a basemap. See method description to know what layer types can be moved to basemap."
            )

    def _move_from_basemap(self, index: int):
        try:
            # Store initial states
            initial_base_map_layers = list(self.pydantic_class.base_map.base_map_layers)
            initial_operational_layers = list(self.pydantic_class.operational_layers)
            initial_widget_layers = list(self._source.content.layers)

            if len(initial_base_map_layers) == 1:
                raise ValueError(
                    "You only have one basemap layer present. You cannot remove the layer since you must always have at least one layer."
                )
            # Get the layer from basemap layers
            layer = self.pydantic_class.base_map.base_map_layers[index]
            # Add to the pydantic dataclass
            self.pydantic_class.operational_layers.append(layer)
            # Add to the layers property
            self._source.content.layers.append(self._infer_layer(layer))
            # Remove from the basemap layers
            del self.pydantic_class.base_map.base_map_layers[index]
            first_layer = self.pydantic_class.base_map.base_map_layers[
                0
            ]  # need extra line incase fails
            self._check_service_spatial_reference(first_layer)
        except Exception as e:
            # Revert to initial state
            self.pydantic_class.base_map.base_map_layers = initial_base_map_layers
            self.pydantic_class.operational_layers = initial_operational_layers
            self._source.content.layers = initial_widget_layers

            # Raise the exception again to notify the caller
            raise e

    def _basemap_title_format(self, title):
        return title.replace("-", " ").replace("_", " ")

    def _set_basemap_title(self, title: str):
        """
        Set the basemap title.

        =====================       ===================================================================
        **Parameter**                **Definition**
        ---------------------       -------------------------------------------------------------------
        title                       Required string. The title to set for the basemap.
        =====================       ===================================================================
        """
        title = self._basemap_title_format(title)
        self.pydantic_class.base_map.title = title

    def _remove_basemap_layer(self, index: int):
        if len(self.pydantic_class.base_map.base_map_layers) > 1:
            if index == 0:
                # If removing first basemap layer, need to check the spatial reference of new first layer
                # We know there has to be at least one layer following since cannot delete all basemap layers
                self._check_service_spatial_reference(
                    self.pydantic_class.base_map.base_map_layers[1]
                )
            # Remove the basemap layer at the specific layer
            del self.pydantic_class.base_map.base_map_layers[index]
            # Refresh the basemap property
            self._source._update_source()
        else:
            raise ValueError(
                "You only have one basemap layer present. You cannot remove the layer since you must always have at least one layer."
            )

    def _set_basemap_from_definition(self, basemap):
        """Set basemap from the hardcoded basemap definitions in the basemapdef file or the basemap gallery."""
        # set basemap_dict
        basemap_dict = None
        # reset the spatial reference if it was changed for other basemap
        if self.pydantic_class.spatial_reference.wkid != 102100:
            self.pydantic_class.spatial_reference = _models.SpatialReference(
                wkid=102100
            )
        if basemap in self.basemaps:
            basemap_dict = {
                "baseMapLayers": basemapdef.basemap_dict[basemap],
                "title": self._basemap_title_format(basemap.title()),
            }
        elif basemap in self.basemaps3d:
            # Only on webscene
            basemap_dict = basemapdef3d.basemap_dict[basemap]
        elif basemap in self.basemap_gallery:
            # Pass in a dictionary representation of basemap to widget
            basemap_dict = self._get_basemap_from_gallery(basemap)
            self._check_service_spatial_reference(basemap_dict)
        if not basemap_dict:
            raise ValueError(
                "Basemap not found. Please check the basemap name and try again."
            )
        return basemap_dict

    def _set_basemap_from_map_scene_item(self, basemap):
        self._check_service_spatial_reference(basemap)
        # Set the basemap to an existing widget's basemap
        # Pass in a dictionary representation of basemap to widget
        orig_dict = basemap.get_data()
        # Check spatial reference
        return orig_dict["baseMap"]

    def _set_basemap_from_item(self, basemap):
        if isinstance(basemap, _gis_mod.Item):
            basemap = basemap.layers[0]
        # Add the layer to the widget and then move it to basemap
        self._source.content.add(basemap, index=0)
        try:
            # Since we set the index when adding, we know which layer it is to move
            self._move_to_basemap(0)
        except Exception as e:
            # Maybe the layer type was not correct or there was another error.
            # Remove the layer and return exception
            # We added at index 0 so ok to say it is there
            self._source.content.remove(index=0)
            raise e
        # Remove the old basemap layer(s).
        # Replacing old layers entirely so need to remove all except one we just added.
        while len(self.pydantic_class.base_map.base_map_layers) != 1:
            # Layers keep moving forward.
            # We know recent one was added at end so keep removing first until only recent one left.
            self._remove_basemap_layer(index=0)
        # Finally update the basemap title
        self._set_basemap_title(self.pydantic_class.base_map.base_map_layers[0].title)

    def _get_basemap_from_gallery(self, basemap_name):
        """Get the basemap from the basemap gallery."""
        basemap_gallery = self._basemap_gallery_dict()
        # The key in the dictionary is the title of the basemap, the value is the basemap definition we need
        return basemap_gallery.get(basemap_name)

    def _basemap_gallery_dict(self):
        basemap_gallery = self._source.basemap._basemap_gallery

        if len(basemap_gallery) <= 1:
            # If the only loaded basemap_gallery is 'default', load the rest
            basemap_group_query = self._source._gis.properties[
                "basemapGalleryGroupQuery"
            ]
            basemap_groups = self._source._gis.groups.search(
                basemap_group_query, outside_org=True
            )

            if len(basemap_groups) == 1:
                # Get the basemaps from the group
                for basemap in basemap_groups[0].content():
                    if basemap.type.lower() in ["web map", "web scene"]:
                        item_data = basemap.get_data()
                        basemap_title = basemap.title.lower().replace(" ", "_")
                        basemap_gallery[basemap_title] = item_data["baseMap"]
        return basemap_gallery

    @cached_property
    def basemap_gallery(self):
        """
        The ``basemap_gallery`` property allows for viewing of your portal's custom basemap group.

        :returns: list of basemap names
        """
        basemap_gallery = self._basemap_gallery_dict()
        return list(basemap_gallery.keys())

    def _check_service_spatial_reference(self, service):
        """
        Find the spatial reference being used by the widget and the first basemap layer and make sure they match.
        Otherwise throw an error.

        This method is used when switching the basemap to a new basemap. This can occur when a user sets
        the basemap using the `basemap` property or when they remove the first basemap layer using the
        `remove_basemap_layer` method.

        What can be checked here:
        - Existing Web Map Item that is being used to set basemap
        - Existing Web Scene Item that is being used to set basemap
        - Pydantic classes when a user is removing the first basemap layer and we must check the next in line is compatible
        - Layer coming from the arcgis Item class's `layers` property.
        - Dictionary of basemap from basemap file or gallery basemaps
        """
        # Get the spatial reference from the widget
        map_sr = self.pydantic_class.spatial_reference.wkid

        # Determine spatial reference of the service
        layer_sr = self._get_service_spatial_reference(service)

        # Check if spatial references match
        if layer_sr and map_sr != layer_sr:
            # Update widget's spatial reference and log a warning
            self.pydantic_class.spatial_reference = _models.SpatialReference(
                wkid=layer_sr
            )
            self._check_extent_sr()
            logging.warning(
                "The first basemap layer's spatial reference does not match the current spatial reference of the webmap or webscene. "
                "The spatial reference of the webmap or webscene will be updated but this might "
                "affect the rendering of other layers depending on their spatial reference."
            )

    def _get_service_spatial_reference(self, service):
        """
        Attempts to retrieve the spatial reference WKID from a variety of service types.
        Returns None if it cannot be determined.
        """

        def _get_layer_sr_from_properties(properties):
            for key in ["spatialReference", "extent", "fullExtent", "tileInfo"]:
                sr = (
                    properties.get(key, {}).get("spatialReference")
                    if isinstance(properties.get(key), dict)
                    else properties.get("spatialReference")
                )
                if sr:
                    return sr
            return None

        def _normalize_sr(sr):
            if isinstance(sr, dict):
                return sr.get("wkid")
            if isinstance(sr, str):
                return int(sr)
            if sr and not isinstance(sr, int):
                return dict(sr).get("wkid")
            return sr

        def _extract_layer_from_basemap_dict(service_dict):
            base_map_layers = service_dict.get("baseMapLayers", [])
            if not base_map_layers:
                return None
            base_map_layer = base_map_layers[0]

            if "itemId" in base_map_layer:
                item = self._source._gis.content.get(base_map_layer["itemId"])
                try:
                    return item.layers[0]
                except Exception:
                    return None
            elif "url" in base_map_layer:
                return arcgis_layers.Service(base_map_layer["url"])
            return None

        # Step 1: Handle dictionary-based services
        if isinstance(service, dict) and not isinstance(service, _gis_mod.Item):
            resolved = _extract_layer_from_basemap_dict(service)
            return self._check_basemap_layer_sr(resolved) if resolved else None

        # Step 2: Handle known ArcGIS model types
        if isinstance(service, _models.WMSLayer):
            return _normalize_sr(service.spatial_references[0])

        if isinstance(service, _models.VectorTileLayer):
            if hasattr(service, "full_extent") and service.full_extent:
                sr = service.full_extent.dict().get("spatialReference")
                return _normalize_sr(sr)
            return None

        if isinstance(service, _models.WebTiledLayer):
            return _normalize_sr(service.tile_info.spatial_reference.wkid)

        # Step 3: Handle Items and Tiled Map Services
        if isinstance(service, (_gis_mod.Item, _models.TiledMapServiceLayer)):
            if isinstance(service, _gis_mod.Item):
                service = (
                    service.get_data().get("baseMap", {}).get("baseMapLayers", [])[0]
                )

            item_id = (
                service.get("itemId")
                if isinstance(service, dict)
                else getattr(service, "item_id", None)
            )

            if item_id:
                item = self._source._gis.content.get(item_id)
                try:
                    if item.type == "Map Service":
                        service = arcgis_layers.Service(item.url)
                    else:
                        service = item.layers[0]
                except Exception:
                    return None
            elif "url" in service or hasattr(service, "url"):
                url = service["url"] if isinstance(service, dict) else service.url
                service = arcgis_layers.Service(url)
            else:
                return None

            return self._check_basemap_layer_sr(service)

        # Step 4: Default property-based SR extraction
        if hasattr(service, "properties"):
            sr = _get_layer_sr_from_properties(service.properties)
            return _normalize_sr(sr)
        return None

    def _private_to_public_url(self, layer) -> str:
        """
        In certain cases urls set on Feature Layer and other services are private urls.
        Need to find the service's public url through the Item class and create public url.
        """
        # On the source._gis, there is a property called: _use_private_url_only. This is checked as a precursor to this method call.
        # If this property is True, then the Layers being read in are using a private url, we need to find the public url and return it

        # We will write each service out first and then combine later
        # Feature Layer, MapFeatureLayer, MapRasterLayer, Table, TiledImageryLayer, ElevationLayer:
        # If serviceItemId is present, we can find the service url through the Item class
        # Caveat: If layer is coming from another gis instance, we cannot find the service url
        if layer.properties.get("serviceItemId"):
            layer_idx = os.path.basename(layer.url)
            service_item = self._source._gis.content.get(
                layer.properties["serviceItemId"]
            )
            if service_item:
                return service_item.url + "/" + layer_idx
        # Scene Layers and Vector Tile Layer
        if hasattr(layer, "_parent_url") and layer._parent_url:
            if isinstance(layer, _models.VectorTileLayer):
                return layer._parent_url
            # 3D Layers need to add /layers/<their idx> to the url if they came from Item class
            return layer._parent_url + "/layers/" + os.path.basename(layer.url)
        # default
        return layer.url
