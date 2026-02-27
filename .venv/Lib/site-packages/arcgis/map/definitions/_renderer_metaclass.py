from __future__ import annotations
import logging
from pydantic import ValidationError
from arcgis.auth.tools import LazyLoader

_models = LazyLoader("arcgis.map.dataclasses.models")


class RendererMetaclassObject(type):
    def __call__(cls, **kwargs):
        renderer_type = kwargs.get("renderer_type")
        renderer = kwargs.get("renderer")

        renderer_class_mapping = {
            "heatmap": _models.HeatmapRenderer,
            "class breaks": _models.ClassBreaksRenderer,
            "classbreaks": _models.ClassBreaksRenderer,
            "unique value": _models.UniqueValueRenderer,
            "uniquevalue": _models.UniqueValueRenderer,
            "simple": _models.SimpleRenderer,
            "dot density": _models.DotDensityRenderer,
            "dotdensity": _models.DotDensityRenderer,
            "dictionary": _models.DictionaryRenderer,
            "flow": _models.FlowRenderer,
            "piechart": _models.PieChartRenderer,
            "pie chart": _models.PieChartRenderer,
            "temporal": _models.TemporalRenderer,
            "vector field": _models.VectorFieldRenderer,
            "vectorfield": _models.VectorFieldRenderer,
            "predominance": _models.PredominanceRenderer,
        }

        # Look up the renderer class from the spec
        renderer_class = renderer_class_mapping.get(
            renderer_type.lower(), _models.SimpleRenderer
        )

        try:
            return renderer_class(**renderer)
        except ValidationError as e:
            logging.error(
                f"Renderer of type {renderer_type} could not be created. Error: {e}"
            )
            return None


class FactoryWorker(metaclass=RendererMetaclassObject):
    def __init__(self, renderer_type, renderer, is_map):
        self.renderer_type = renderer_type
        self.renderer = renderer
