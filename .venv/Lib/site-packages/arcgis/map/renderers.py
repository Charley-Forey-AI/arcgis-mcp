from __future__ import annotations
import json
import time
from arcgis.layers import Service
from arcgis.map import smart_mapping
from arcgis.map.dataclasses.models import FeatureLayer, LayerDefinition, DrawingInfo
from arcgis.map.dataclasses.enums.layers import BlendMode
from arcgis.map.dataclasses.models.renderers import (
    ColorRamp,
    AuthoringInfoVisualVariable,
    AuthoringInfoStatistics,
    AuthoringInfoClassBreakInfo,
    AuthoringInfoField,
    AuthoringInfo,
    HeatmapColorStop,
    LegendOptions,
    ColorStop,
    ColorInfoVisualVariable,
    RotationInfoVisualVariable,
    SizeStop,
    Size,
    SizeInfoVisualVariable,
    TransparencyStop,
    TransparencyInfoVisualVariable,
    UniqueValueInfo,
    AttributeColorInfo,
    HeatmapRenderer,
    UniqueValueClass,
    UniqueValueGroup,
    UniqueValueRenderer,
    ClassBreakInfo,
    SimpleRenderer,
    DotDensityRenderer,
    StretchRenderer,
    FlowRenderer,
    PredominanceRenderer,
    ClassBreaksRenderer,
    ExpressionInfo,
    DictionaryRenderer,
    OthersThresholdColorInfo,
    PieChartRenderer,
    VectorFieldRenderer,
    TemporalRenderer,
    ColormapInfo,
    RasterShadedReliefRenderer,
    RasterColorMapRenderer,
    ColorClassBreakInfo,
    ColorModulationInfo,
    RendererLegendOptions,
    PointCloudFixedSizeAlgorithm,
    PointCloudSplatAlgorithm,
    PointCloudClassBreaksRenderer,
    PointCloudRGBRenderer,
    PointCloudStretchRenderer,
    ColorUniqueValueInfo,
    PointCloudUniqueValueRenderer,
)

from arcgis.map.dataclasses.enums.renderers import (
    RampAlgorithm,
    ColorRampType,
    HillshadeType,
    ScalingType,
    RatioStyle,
    ValueRepresentation,
    FieldTransformType,
    Axis,
    LengthUnit,
    Theme,
    VisualVariableType,
    TimeUnits,
    UnivariateSymbolStyle,
    StandardDeviationInterval,
    Focus,
    FlowTheme,
    ClassificationMethod,
    RendererType,
    UnivariateTheme,
    LegendOrder,
    StretchType,
    TrailCap,
    FlowRepresentation,
    NormalizationType,
    InputOutputUnit,
    VectorFieldStyle,
)

__all__ = [
    "ColorRamp",
    "AuthoringInfoVisualVariable",
    "AuthoringInfoStatistics",
    "AuthoringInfoClassBreakInfo",
    "AuthoringInfoField",
    "AuthoringInfo",
    "HeatmapColorStop",
    "LegendOptions",
    "ColorStop",
    "ColorInfoVisualVariable",
    "RotationInfoVisualVariable",
    "SizeStop",
    "Size",
    "SizeInfoVisualVariable",
    "TransparencyStop",
    "TransparencyInfoVisualVariable",
    "UniqueValueInfo",
    "AttributeColorInfo",
    "HeatmapRenderer",
    "UniqueValueClass",
    "UniqueValueGroup",
    "UniqueValueRenderer",
    "ClassBreakInfo",
    "SimpleRenderer",
    "DotDensityRenderer",
    "StretchRenderer",
    "FlowRenderer",
    "PredominanceRenderer",
    "ClassBreaksRenderer",
    "ExpressionInfo",
    "DictionaryRenderer",
    "OthersThresholdColorInfo",
    "PieChartRenderer",
    "VectorFieldRenderer",
    "TemporalRenderer",
    "ColormapInfo",
    "RasterShadedReliefRenderer",
    "RasterColorMapRenderer",
    "ColorClassBreakInfo",
    "ColorModulationInfo",
    "RendererLegendOptions",
    "PointCloudFixedSizeAlgorithm",
    "PointCloudSplatAlgorithm",
    "PointCloudClassBreaksRenderer",
    "PointCloudRGBRenderer",
    "PointCloudStretchRenderer",
    "ColorUniqueValueInfo",
    "PointCloudUniqueValueRenderer",
    "RampAlgorithm",
    "ColorRampType",
    "HillshadeType",
    "ScalingType",
    "RatioStyle",
    "ValueRepresentation",
    "FieldTransformType",
    "Axis",
    "LengthUnit",
    "Theme",
    "VisualVariableType",
    "TimeUnits",
    "UnivariateSymbolStyle",
    "StandardDeviationInterval",
    "Focus",
    "FlowTheme",
    "ClassificationMethod",
    "RendererType",
    "UnivariateTheme",
    "LegendOrder",
    "StretchType",
    "TrailCap",
    "FlowRepresentation",
    "NormalizationType",
    "InputOutputUnit",
    "VectorFieldStyle",
    "BlendMode",
]


class RendererManager:
    """
    A class that defines the renderer found on a layer.
    Through this class you can edit the renderer and get information on it.


    .. note::
        This class should not be created by a user but rather called through the `renderer` method on
        a MapContent or GroupLayer instance.
    """

    def __init__(self, **kwargs) -> None:
        # The pydantic layer, this hooks it to the main webmap and tracks changes made
        self._layer = kwargs.pop("layer")
        self._source = kwargs.pop("source")

    def __str__(self) -> str:
        return "Renderer Manager for: " + self._layer.title

    def __repr__(self) -> str:
        return f"RendererManager(layer={self._layer.title})"

    def _get_renderer(self):
        lyr_dict = self._layer.dict()
        renderer = (
            lyr_dict.get("layerDefinition", {}).get("drawingInfo", {}).get("renderer")
        )

        if renderer is None:
            # The map could have been initialized with layers where renderer in layer info
            # get the service and find renderer from there if feature layer
            if isinstance(self._layer, FeatureLayer):
                if self._layer.url:
                    fl = Service(self._layer.url)
                    renderer = dict(fl.renderer)
        return renderer

    @property
    def renderer(
        self,
    ) -> (
        SimpleRenderer
        | HeatmapRenderer
        | PredominanceRenderer
        | DotDensityRenderer
        | FlowRenderer
        | ClassBreaksRenderer
        | DictionaryRenderer
        | PieChartRenderer
        | VectorFieldRenderer
    ):
        """
        Get an instance of the Renderer dataclass found in the layer.
        :return: Renderer dataclass for the layer specified.
        """
        # Return initialized instance of the class
        # Must pass in pydantic class to edit
        renderer = self._get_renderer()

        if not renderer:
            return None

        # pass the renderer through our renderer classes so there are no inconsistencies
        rtype = renderer["type"] if isinstance(renderer, dict) else renderer.type
        renderer_class_mapping = {
            "simple": SimpleRenderer,
            "heatmap": HeatmapRenderer,
            "uniqueValue": PredominanceRenderer,
            "dotDensity": DotDensityRenderer,
            "flowRenderer": FlowRenderer,
            "classBreaks": ClassBreaksRenderer,
            "dictionary": DictionaryRenderer,
            "pieChart": PieChartRenderer,
            "vectorField": VectorFieldRenderer,
        }
        if rtype in renderer_class_mapping:
            # if came from smart_mapping it is a dict else dataclass
            if not isinstance(renderer, dict):
                renderer = renderer.dict()
            return renderer_class_mapping[rtype](**renderer)
        else:
            return renderer

    @renderer.setter
    def renderer(self, new_renderer):
        """
        Set the renderer for the layer.
        :param renderer: The renderer to set for the layer.
        :return: None
        """
        rtype = new_renderer.type

        # Turn it into the corresponding spec class
        if rtype == "simple":
            new_renderer = SimpleRenderer(**new_renderer.dict())
        elif rtype == "heatmap":
            new_renderer = HeatmapRenderer(**new_renderer.dict())
        elif rtype == "uniqueValue":
            new_renderer = PredominanceRenderer(**new_renderer.dict())
        elif rtype == "dotDensity":
            new_renderer = DotDensityRenderer(**new_renderer.dict())
        elif rtype == "flowRenderer":
            new_renderer = FlowRenderer(**new_renderer.dict())
        elif rtype == "classBreaks":
            new_renderer = ClassBreaksRenderer(**new_renderer.dict())
        elif rtype == "dictionary":
            new_renderer = DictionaryRenderer(**new_renderer.dict())
        elif rtype == "pieChart":
            new_renderer = PieChartRenderer(**new_renderer.dict())
        elif rtype == "vectorField":
            new_renderer = VectorFieldRenderer(**new_renderer.dict())
        else:
            raise ValueError("The renderer type provided is not supported.")
        # Set the renderer
        if self._layer.layer_definition is None:
            self._layer.layer_definition = LayerDefinition()
        if self._layer.layer_definition.drawing_info is None:
            self._layer.layer_definition.drawing_info = DrawingInfo()
        self._layer.layer_definition.drawing_info.renderer = new_renderer
        # Update the webmap to reflect the changes
        self._source._update_source()

    @property
    def blend_mode(self) -> BlendMode | None:
        """
        Get or set the blend mode for the layer.
        :return: BlendMode enum value for the layer.
        """
        if hasattr(self._layer, "blend_mode"):
            return self._layer.blend_mode or BlendMode.normal
        return None

    @blend_mode.setter
    def blend_mode(self, mode: BlendMode):
        """
        Set the blend mode for the layer.
        :param mode: The BlendMode enum value to set for the layer.
        :return: None
        """
        if not isinstance(mode, BlendMode):
            raise ValueError("mode must be an instance of BlendMode enum.")
        self._layer.blend_mode = mode
        # Update the webmap to reflect the changes
        self._source._update_source()

    def smart_mapping(self) -> smart_mapping.SmartMappingManager:
        """
        Returns a SmartMappingManager object that can be used to create
        smart mapping visualizations.

        .. note::
            Requires the Map to be rendered in a Jupyter environment.
        """
        return smart_mapping.SmartMappingManager(source=self._source, layer=self._layer)

    def _find_non_serializable(self, obj, path="root"):
        errors = []
        if isinstance(obj, dict):
            for k, v in obj.items():
                errors += self._find_non_serializable(v, f"{path}.{k}")
        elif isinstance(obj, list):
            for i, v in enumerate(obj):
                errors += self._find_non_serializable(v, f"{path}[{i}]")
        else:
            try:
                json.dumps(obj)
            except (TypeError, OverflowError):
                errors.append(path)
        return errors

    def to_template(self, path: str | None = None) -> bool:
        """
        This method will take the current renderer and save it as an item resource on the Item.
        This will allow the renderer to be used in other web maps and applications. You can also
        share the renderer with other users. Use the Item's Resource Manager to export the renderer
        to a file.

        ==========================      =================================================================
        **Parameter**                   **Description**
        --------------------------      ---------------------------------------------------------------
        path                            Optional string. The file path where the renderer template JSON file will be saved.
                                        If not provided, the renderer will be saved as an item resource.
        ==========================      =================================================================

        :return: If added to item resources, returns the resource name. If saved to path, returns the path.

        .. code-block:: python
            from arcgis.gis import GIS
            from arcgis.map import Map
            gis = GIS(profile="geosaurus")
            m1 = Map(item="<item_id>")
            renderer_manager_layer1 = m1.content.renderer(0)
            resource_name = renderer_manager_layer1.to_template()
            print(resource_name)
            >>> simple_renderer_renderer_1627891234567.json

            # Get the resource from the item
            resource = m1.item.resources.get(resource_name)
            print(resource)
            >>> "C:/path/to/resource/simple_renderer_renderer_1627891234567.json"
        """
        if not hasattr(self._source, "item") or self._source.item is None:
            # Check that item is not None, else tell user to save map/scene
            raise ValueError(
                "The Map or Scene must be saved as an item before the renderer can be saved as a template."
            )
        # Get the renderer dictionary and dump to json
        # Add new resource_name with time in milliseconds
        resource_name = (
            self.renderer.type + "_renderer_" + str(int(time.time() * 1000)) + ".json"
        )
        renderer_dict = self.renderer.dict()
        bad_fields = self._find_non_serializable(renderer_dict)
        if bad_fields:
            raise ValueError(
                f"The renderer contains non-serializable fields: {bad_fields}"
            )
        json_str = json.dumps(self.renderer.dict(), ensure_ascii=False)

        # If path is provided, save to that path instead of item resources
        if path:
            # Save to specified path
            with open(path, "w", encoding="utf-8") as f:
                f.write(json_str)
            return path
        # Save the renderer dictionary to a json file, add to resources
        resource_manager = self._source.item.resources

        # Add the json to the resources
        resource_manager.add(file_name=resource_name, text=json_str, access="inherit")
        return resource_name

    def from_template(self, template: str | dict) -> bool:
        """
        This method will take a json file that defines a renderer and set the layer's
        renderer to the one defined in the template.

        ==========================      =================================================================
        **Parameter**                   **Description**
        --------------------------      ---------------------------------------------------------------
        template                        Required string or dictionary. The path to the json file that defines the renderer.
                                        If a dictionary is provided, it will be used as the template directly.

                                        Example of content in the json file:
                                        {
                                            "authoringInfo": {
                                                "colorRamp": {
                                                    "colorRamps": [
                                                        {
                                                            "algorithm": "esriHSVAlgorithm",
                                                            "fromColor": [56, 168, 0, 255],
                                                            "toColor": [255, 255, 0, 255],
                                                            "type": "algorithmic"
                                                        },
                                                        {
                                                            "algorithm": "esriHSVAlgorithm",
                                                            "fromColor": [255, 255, 0, 255],
                                                            "toColor": [255, 0, 0, 255],
                                                            "type": "algorithmic"}],
                                                            "type": "multipart"
                                                        }
                                                },
                                                "type": "uniqueValue",
                                                "uniqueValueInfos": [{
                                                    "label": "National Park or Forest",
                                                    "symbol": {
                                                        "color": [0.0, 0.0, 0.0, 64.0],
                                                        "outline": {
                                                            "color": [0, 0, 0, 255],
                                                            "style": "esriSLSSolid",
                                                            "type": "esriSLS",
                                                            "width": 1
                                                        },
                                                        "style": "esriSFSSolid",
                                                        "type": "esriSFS"
                                                    },
                                                    "value": "National park or forest"
                                                },
                                                {
                                                    "label": "State Park or Forest",
                                                    "symbol": {
                                                        "color": [0.0, 0.0, 0.0, 64.0],
                                                        "outline": {
                                                            "color": [0, 0, 0, 255],
                                                            "style": "esriSLSSolid",
                                                            "type": "esriSLS",
                                                            "width": 1
                                                        },
                                                        "style": "esriSFSSolid",
                                                        "type": "esriSFS"
                                                    },
                                                    "value": "State park or forest"
                                                },
                                                {
                                                    "label": "County Park",
                                                    "symbol": {
                                                        "color": [0.0, 0.0, 0.0, 64.0],
                                                        "outline": {
                                                            "color": [0, 0, 0, 255],
                                                            "style": "esriSLSSolid",
                                                            "type": "esriSLS",
                                                            "width": 1
                                                        },
                                                        "style": "esriSFSSolid",
                                                        "type": "esriSFS"
                                                    },
                                                    "value": "County park"
                                                },
                                                {
                                                    "label": "Local Park",
                                                    "symbol": {
                                                        "color": [0.0, 0.0, 0.0, 64.0],
                                                        "outline": {
                                                            "color": [0, 0, 0, 255],
                                                            "style": "esriSLSSolid",
                                                            "type": "esriSLS",
                                                            "width": 1
                                                        },
                                                        "style": "esriSFSSolid",
                                                        "type": "esriSFS"
                                                    },
                                                    "value": "Local park"
                                                }
                                            ]
                                        }
        ==========================      =================================================================

        :return: The renderer dataclass that was set on the layer. Further edits can be made to this renderer and assigned to the renderer property.

        .. code-block:: python
            from arcgis.gis import GIS
            from arcgis.map import Map
            gis = GIS(profile="geosaurus")
            m1 = Map(item="<item_id>")
            renderer_manager_layer1 = m1.content.renderer(0)
            predominance_rend = renderer_manager_layer1.from_template("path/to/predominance_renderer_template.json")
            print(predominance_rend)
            >>> PredominanceRenderer(...)

            # make further edits and update again
            predominance_rend.unique_value_infos[0].label = "New Label"
            renderer_manager_layer1.renderer = predominance_rend
        """
        # First check that template is a json file
        data = None
        if isinstance(template, dict):
            data = template
        else:
            with open(template, "r") as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError as e:
                    raise ValueError(
                        f"Failed to parse JSON: {e}. Check the file contains a valid JSON object representing a renderer."
                    )

            if not isinstance(data, dict):
                raise ValueError(
                    "Renderer template must be a JSON object (dictionary) at the top level."
                )

            if "type" not in data:
                raise ValueError(
                    "Renderer template JSON is missing the required 'type' field."
                )

        # Pass the renderer into a dataclass depending on type
        if data["type"] == "simple":
            self.renderer = SimpleRenderer(**data)
        elif data["type"] == "heatmap":
            self.renderer = HeatmapRenderer(**data)
        elif data["type"] == "uniqueValue":
            self.renderer = PredominanceRenderer(**data)
        elif data["type"] == "dotDensity":
            self.renderer = DotDensityRenderer(**data)
        elif data["type"] == "flowRenderer":
            self.renderer = FlowRenderer(**data)
        elif data["type"] == "classBreaks":
            self.renderer = ClassBreaksRenderer(**data)
        elif data["type"] == "dictionary":
            self.renderer = DictionaryRenderer(**data)
        elif data["type"] == "pieChart":
            self.renderer = PieChartRenderer(**data)
        elif data["type"] == "vectorField":
            self.renderer = VectorFieldRenderer(**data)
        else:
            raise ValueError("The renderer type provided is not supported.")

        return self.renderer
