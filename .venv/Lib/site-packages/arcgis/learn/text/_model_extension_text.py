import os
import json
import traceback
from pathlib import Path
from typing import Dict, List
from importlib.util import spec_from_loader, module_from_spec
from importlib.machinery import SourceFileLoader

try:
    from ..models._arcgis_model import ArcGISModel
    from .._utils.common import _get_emd_path
except Exception as e:
    import_exception = "\n".join(
        traceback.format_exception(type(e), e, e.__traceback__)
    )
    HAS_FASTAI = False


class TextModelExtension:
    def __init__(self, emd_path: str | Path, model):
        self.emd_path: Path = emd_path
        self.model = model
        self.model_loaded = False

    @classmethod
    def from_model(cls, emd_path: str | Path, **kwargs):
        emd_path = _get_emd_path(emd_path)
        with open(emd_path) as f:
            emd_json = json.load(f)

        # Till here we are not aware of the type of the task. Either we infer it from emd or ask the upstream to supply
        # it explicitly. Currently, upstream will only call predict method and parsing will be task specific

        inference_function, inference_function_full_path = (
            TextModelExtension._get_inference_function_details(emd_json, emd_path)
        )
        load_class_name = os.path.basename(inference_function_full_path).split(".")[0]
        if inference_function:
            # dynamically load the module
            try:
                spec = spec_from_loader(
                    load_class_name,
                    SourceFileLoader(load_class_name, inference_function_full_path),
                )
                model = module_from_spec(spec)
                spec.loader.exec_module(model)
                model = getattr(model, load_class_name)
            except FileNotFoundError:
                raise Exception(
                    f"Inference function file {inference_function_full_path} is not present in the dlpk."
                )
            except AttributeError:
                raise Exception(
                    f"Class {load_class_name} is not present in the provided inference file."
                )
            except:
                raise Exception("Inference function has encountered an issue.")
            # add the additional kwargs
            # Check for key overlap
            emd_keys = set(list(emd_json.keys()))
            user_keys = set(list(kwargs.keys()))
            common_keys = emd_keys.intersection(user_keys)

            if common_keys.__len__():
                print(
                    f"A key with the same name is detected between EMD parameters and user-defined parameters, "
                    f"the value of the common key: {common_keys} in the user-defined parameters will take "
                    f"precedence."
                )
            # prepare a payload JSON
            payload_json = {"model": emd_path}
            payload_json.update(kwargs)
            model = model()
            model.initialize(**payload_json)
            cls_object = cls(emd_path, model)
            cls_object.model_loaded = True
        else:
            cls_object = cls({}, None)

        return cls_object

    @staticmethod
    def _get_inference_function_details(emd: dict, emd_path: str | Path):
        """
        This can act as helper function to check whether the inference function is dependent on the inference function or no.
        If it is dependent then the caller module can call it.
        """
        inference_function = emd.get("InferenceFunction", None)
        # generate full path for inference function. Since it will load from Resource folder so commenting this out

        inference_function_full_path = ""
        # generate full path for inference function
        base_path = os.path.dirname(emd_path)
        if inference_function:
            inference_function_full_path = os.path.join(base_path, inference_function)

        return inference_function, inference_function_full_path
