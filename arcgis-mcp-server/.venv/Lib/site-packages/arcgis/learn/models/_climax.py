from ._codetemplate import super_resolution
import json
import traceback
import numpy as np
import pandas as pd


from .._data import _raise_fastai_import_error
from ._arcgis_model import ArcGISModel, _EmptyData

try:
    from IPython.display import display
    from ._climax_utils import lat_weighted_mse
    from ._climax_utils import climaX
    from .._utils.climax import load_pretrained_path

    from .._data_utils.climax_data import show_results, lat_weighted_rmse

    # from .._data_utils.psetae_data import show_results
    from .._utils.common import _get_emd_path
    from pathlib import Path
    from fastai.vision import Learner, partial, optim

    HAS_FASTAI = True
except Exception as e:
    import_exception = "\n".join(
        traceback.format_exception(type(e), e, e.__traceback__)
    )
    HAS_FASTAI = False


class ClimaX(ArcGISModel):
    """
    Creates ClimaX model object: a foundational model for
    weather and climate forecasting tasks.

    =====================   ===========================================
    **Parameter**            **Description**
    ---------------------   -------------------------------------------
    data                    Required fastai Databunch. Returned data object from
                            `prepare_data` function.
    ---------------------   -------------------------------------------
    backbone                Optional string. pretrained foundational models
                            as backbone. Compatible backbones: '5.625deg',
                            '1.40625deg'. Default set to '5.625deg'.
    ---------------------   -------------------------------------------
    pretrained_path         Optional string. Path where pre-trained model is
                            saved.
    =====================   ===========================================

    **Keyword Arguments**

    =====================   ===========================================
    **Parameter**            **Description**
    ---------------------   -------------------------------------------
    patch_size              Optional int. Patch size for generating patch
                            embeddings. Default: 4
    ---------------------   -------------------------------------------
    embed_dim               Optional int. Dimension of embeddings.
                            Default: 1024
    ---------------------   -------------------------------------------
    depth                   Optional int. Depth of model.
                            Default: 8
    ---------------------   -------------------------------------------
    num_heads               Optional int. Number of attention heads.
                            Default: 16
    ---------------------   -------------------------------------------
    mlp_ratio               Optional float. Ratio of MLP.
                            Default: 4.0
    ---------------------   -------------------------------------------
    decoder_depth           Optional int. Depth of decoder.
                            Default: 2
    ---------------------   -------------------------------------------
    drop_path               Optional float. stochastic depth or randomly
                            drops entire layers. Default: 0.1
    ---------------------   -------------------------------------------
    drop_rate               Optional float. randomly drops neurons.
                            Default: 0.1
    ---------------------   -------------------------------------------
    parallel_patch_embed    Optional bol. parallel embdedding of patches.
                            Default: True
    =====================   ===========================================

    :return: `ClimaX` Object
    """

    def __init__(self, data, backbone=None, pretrained_path=None, *args, **kwargs):
        super().__init__(data, backbone, pretrained_path=None, *args, **kwargs)

        self.kwargs = kwargs
        self._backbone = backbone
        if backbone:
            kwargs["parallel_patch_embed"] = False

        climax = climaX(
            default_vars=data._variables,
            out_variables=data._out_variables,
            img_size=data.chp_size,
            **kwargs,
        )

        self.learn = Learner(
            data,
            climax,
            loss_func=lat_weighted_mse(climax),
            opt_func=partial(optim.Adam, betas=(0.9, 0.99)),
        )

        self.learn.model = self.learn.model.to(self._device)

        if backbone:
            load_pretrained_path(self.learn.model, data, backbone)

        self.learn.model._device = self._device
        self._slice_lr = False
        if pretrained_path is not None:
            self.load(pretrained_path)
        self._code = super_resolution

        def __str__(self):
            return self.__repr__()

        def __repr__(self):
            return "<%s>" % (type(self).__name__)

    def _get_emd_params(self, save_inference_file):
        _emd_template = {}
        _emd_template["Framework"] = "arcgis.learn.models._inferencing"
        _emd_template["ModelConfiguration"] = "_climax"
        _emd_template["Kwargs"] = self.kwargs
        if save_inference_file:
            _emd_template["InferenceFunction"] = "ArcGISSuperResolution.py"
        else:
            _emd_template["InferenceFunction"] = (
                "[Functions]System\\DeepLearning\\ArcGISLearn\\ArcGISSuperResolution.py"
            )
        _emd_template["ModelType"] = "ImageClassification"
        _emd_template["n_channel"] = self._data._n_channels
        _emd_template["train_valid_years"] = (
            self._data._trainperiod,
            self._data._validperiod,
        )
        _emd_template["variables"], _emd_template["out_variables"] = (
            self._data._variables,
            self._data._out_variables,
        )
        _emd_template["lead_times"] = self._data._leadtimes.tolist()
        _emd_template["ImageHeight"] = self._data.chp_size[0]
        _emd_template["ImageWidth"] = self._data.chp_size[1]
        _emd_template["ImageSpaceUsed"] = self._data._imagespace
        _emd_template["mean_norm_stats"] = list(
            (self._data._norm_mean).astype(np.float64)
        )
        _emd_template["std_norm_stats"] = list(
            (self._data._norm_std).astype(np.float64)
        )
        _emd_template["IsMultidimensional"] = True

        return _emd_template

    @classmethod
    def from_model(cls, emd_path, data=None):
        """
        Creates a ClimaX object from an Esri Model Definition (EMD) file.

        =====================   ===========================================
        **Parameter**            **Description**
        ---------------------   -------------------------------------------
        emd_path                Required string. Path to Deep Learning Package
                                (DLPK) or Esri Model Definition(EMD) file.
        ---------------------   -------------------------------------------
        data                    Required fastai Databunch or None. Returned data
                                object from `prepare_data` function or None for
                                inferencing.
        =====================   ===========================================

        :return: `ClimaX` Object
        """
        if not HAS_FASTAI:
            _raise_fastai_import_error(import_exception=import_exception)

        emd_path = _get_emd_path(emd_path)
        with open(emd_path) as f:
            emd = json.load(f)

        model_file = Path(emd["ModelFile"])

        if not model_file.is_absolute():
            model_file = emd_path.parent / model_file

        # model_params = emd["ModelParameters"]
        chip_h = emd["ImageHeight"]
        kwargs = emd.get("Kwargs", {})
        backbone = emd.get("ModelParameters")["backbone"]

        if data is None:
            data = _EmptyData(
                path=emd_path.parent, loss_func=None, c=2, chip_size=chip_h
            )
            data._n_channel = emd.get("n_channel", None)
            data._trainperiod, data._validperiod = emd.get("train_valid_years")
            data.chp_size = [emd.get("ImageHeight"), emd.get("ImageWidth")]
            data._variables, data._out_variables = emd.get("variables"), emd.get(
                "out_variables"
            )
            data._mean_norm_stats = emd.get("mean_norm_stats", None)
            data._std_norm_stats = emd.get("std_norm_stats", None)
            data.emd_path = emd_path
            data.emd = emd
            data._is_empty = True

        model_obj = cls(data, backbone, pretrained_path=str(model_file), **kwargs)
        model_obj._model_emd = emd

        return model_obj

    @property
    def _model_metrics(self):
        return self.compute_metrics()

    @property
    def supported_datasets(self):
        """Supported dataset types for this model."""
        return ClimaX._supported_datasets()

    @staticmethod
    def _supported_datasets():
        return ["Export_Tiles"]

    @staticmethod
    def _supported_backbones():
        return ["5.625deg", "1.40625deg"]

    def show_results(self, rows=5, variable="", **kwargs):
        """
        Displays the results of a trained model on a part of the validation set.

        =====================   ===========================================
        **Parameter**            **Description**
        ---------------------   -------------------------------------------
        rows                    Optional int. Number of rows of results
                                to be displayed.
        ---------------------   -------------------------------------------
        total_sample_size       Optional int. Number of rows of results
                                to be displayed.
        ---------------------   -------------------------------------------
        variable_no             Optional int. variable count to be displayed
        =====================   ===========================================

        """
        show_results(self, rows, variable, **kwargs)

    def compute_metrics(self):
        """
        Computes latitude weighted root mean squared error on validation set.

        """

        if not hasattr(self._data, "load_empty"):
            raise Exception("Dataset is required for compute metrics")
        return lat_weighted_rmse(self)
