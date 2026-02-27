import os
import sys
import traceback


HAS_BACKEND_SET = False
ARCGIS_ENABLE_TF_BACKEND = os.environ.get("ARCGIS_ENABLE_TF_BACKEND") == "1"
_LAMBDA_TEXT_CLASSIFICATION = os.environ.get("_LAMBDA_TEXT_CLASSIFICATION") == "1"

if os.environ.get("TF_CPP_MIN_LOG_LEVEL", None) is None:
    os.environ["TF_CPP_MIN_LOG_LEVEL"] = "1"

HAS_TENSORFLOW = False
tf_import_exception = None


class FakeImport:
    def __getattr__(self, attr):
        return self

    def __call__(self, *args, **kwargs):
        return self


if _LAMBDA_TEXT_CLASSIFICATION:
    default_module = FakeImport()
    missing_modules = [
        "scipy",
        "scipy.stats",
        "spacy",
        "spacy.symbols",
        "spacy.blank",
        "matplotlib",
        "matplotlib.pyplot",
        "matplotlib.patches",
        "matplotlib.cm",
        "scipy.special",
        "PIL",
    ]
    for module_name in missing_modules:
        sys.modules[module_name] = default_module

## Fastai Imports #######

HAS_FASTAI = False
fastai_import_exception = None


def do_fastai_imports():
    global HAS_FASTAI
    global fastai_import_exception

    try:
        import fastai
        import torch
        import torchvision
        import skimage

        HAS_FASTAI = True
    except Exception as e:
        fastai_import_exception = traceback.format_exc()
        pass

    try:
        from .patches import precondition
    except:
        pass


def fastai_installation_command():
    installation_steps = "Install them using the ArcGIS installer"

    return installation_steps


def raise_fastai_import_error(
    import_exception=fastai_import_exception, installation_steps=None, message=None
):
    if installation_steps is None:
        installation_steps = fastai_installation_command()
    if message is None:
        message = "This module requires a compatible ArcGIS deep-learning-environment"
    raise Exception(f"""{import_exception} \n\n{message}\n{installation_steps}""")


HAS_GDAL = False
gdal_import_exception = None
GDAL_INSTALL_MESSAGE = f"""
\nPlease install gdal using the following command
\nconda install gdal -c esri
""".strip()

try:
    from osgeo import gdal

    HAS_GDAL = True
except Exception as e:
    gdal_import_exception = traceback.format_exc()
    pass


def raise_gdal_import_error(import_exception=gdal_import_exception):
    message = "gdal is required to work with multispectral datasets."
    raise Exception(f"""{import_exception} \n\n{message}\n{GDAL_INSTALL_MESSAGE}""")


## Ipython inside ArcGIS Pro
import sys

_IS_ARCGISPRONOTEBOOK = None
using_mpl_inline = False


def is_arcgispronotebook():
    global using_mpl_inline
    if _IS_ARCGISPRONOTEBOOK is not None:
        if _IS_ARCGISPRONOTEBOOK:
            patch_arcgis_notebook()
        else:
            using_mpl_inline = False
        return _IS_ARCGISPRONOTEBOOK
    if os.path.basename(sys.executable) == "ArcGISPro.exe":
        ### This code will be execute only once in ArcGIS Pro notebooks
        if not using_mpl_inline:
            patch_arcgis_notebook()
            using_mpl_inline = True
        ###
        ### This following code block will be replaced by a flag exposed by Pro's Team (Vinay Vijayan)
        return True
        ###


def reload_IPython():
    if "IPython" in sys.modules:
        del sys.modules["IPython"]
    import IPython

    return IPython


def patch_arcgis_notebook():
    if reload_IPython().get_ipython() is not None:
        reload_IPython().get_ipython().run_line_magic("matplotlib", "inline")


is_arcgispronotebook()
