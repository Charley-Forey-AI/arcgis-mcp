# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import os
from pathlib import Path
import json
import datetime
import torch
import arcgis
import glob
import types
from osgeo import gdal

gdal.UseExceptions()
from math import ceil

import numpy as np
from .._utils.climax import (
    Forecast,
    IndividualForecastDataIter,
    NpyReader,
    ShuffleIterableDataset,
    NoShuffleIterableDataset,
    ClimaxDataBunch,
)
from torchvision.transforms import transforms
from torch.utils.data import DataLoader
from fastai.vision import subplots, plt
from .._utils.common import ArcGISMSImage
from .._data import _prepare_working_dir
from .._utils.common import get_nbatches, get_top_padding


def get_device():
    if getattr(arcgis.env, "_processorType", "") == "GPU" and torch.cuda.is_available():
        device = torch.device("cuda")
    elif getattr(arcgis.env, "_processorType", "") == "CPU":
        device = torch.device("cpu")
    else:
        device = (
            torch.device("cuda") if torch.cuda.is_available() else torch.device("cpu")
        )
    return device


def collate_fn(batch):
    inp = torch.stack([batch[i][0] for i in range(len(batch))])
    out = torch.stack([batch[i][1] for i in range(len(batch))])
    lead_times = torch.stack([batch[i][2] for i in range(len(batch))])
    variables = batch[0][3]
    out_variables = batch[0][4]
    return (
        inp,
        out,
        lead_times,
        [v for v in variables],
        [v for v in out_variables],
    )


def GlobalForecastData(
    root_dir,
    variables,
    buffer_size,
    out_variables=None,
    imagespace=None,
    predict_range: int = 6,
    time_each_step: int = 1,
    batch_size: int = 64,
    num_workers: int = 0,
    pin_memory: bool = False,
    ts_type: str = "monthly",
):
    if isinstance(out_variables, str):
        out_variables = [out_variables]

    def get_normalize(root_dir, variables=None):
        normalize_mean = dict(np.load(os.path.join(root_dir, "normalize_mean.npz")))
        mean = []
        for var in variables:
            if var != "total_precipitation":
                mean.append(normalize_mean[var])
            else:
                mean.append(np.array([0.0]))
        normalize_mean = np.concatenate(mean)
        normalize_std = dict(np.load(os.path.join(root_dir, "normalize_std.npz")))
        normalize_std = np.concatenate([normalize_std[var] for var in variables])
        return (
            transforms.Normalize(normalize_mean, normalize_std),
            normalize_mean,
            normalize_std,
        )

    train_dir, val_dir = os.path.join(root_dir, "train"), os.path.join(root_dir, "val")

    transfrms, norm_mean, norm_std = get_normalize(root_dir, variables=variables)
    out_transfrms, outnorm_mean, outnorm_std = get_normalize(
        root_dir, variables=out_variables
    )

    n_chips_train = np.array(
        [
            int(i.split(".")[0][5:])
            for i in os.listdir(train_dir)
            if not i.startswith("climatology")
        ]
    ).max()
    n_chips_val = np.array(
        [
            int(i.split(".")[0][5:])
            for i in os.listdir(val_dir)
            if not i.startswith("climatology")
        ]
    ).max()

    def chips_shuffle_paths_dict(n_chips, path):
        chips_list = {}
        for i in range(n_chips + 1):
            lst = []
            for j in [i for i in os.listdir(path) if not i.startswith("climatology")]:
                if i == int(j.split(".")[0][5:]):
                    lst.append(os.path.join(path, j))
            chips_list[i] = lst
        return chips_list

    train_chips_dict = chips_shuffle_paths_dict(n_chips_train, train_dir)
    val_chips_dict = chips_shuffle_paths_dict(n_chips_val, val_dir)

    data_train = ShuffleIterableDataset(
        IndividualForecastDataIter(
            Forecast(
                NpyReader(
                    file_dicts=train_chips_dict,
                    start_idx=0,
                    end_idx=1,
                    variables=variables,
                    out_variables=out_variables,
                    shuffle=True,
                    multi_dataset_training=False,
                ),
                max_predict_range=predict_range,
                random_lead_time=False,
                hrs_each_step=time_each_step,
                ts_type=ts_type,
            ),
            transforms=transfrms,
            output_transforms=out_transfrms,
        ),
        buffer_size=buffer_size,
    )

    data_val = NoShuffleIterableDataset(
        IndividualForecastDataIter(
            Forecast(
                NpyReader(
                    file_dicts=val_chips_dict,
                    start_idx=0,
                    end_idx=1,
                    variables=variables,
                    out_variables=out_variables,
                    shuffle=False,
                    multi_dataset_training=False,
                ),
                max_predict_range=predict_range,
                random_lead_time=False,
                hrs_each_step=time_each_step,
                ts_type=ts_type,
            ),
            transforms=transfrms,
            output_transforms=out_transfrms,
        )
    )

    train_dataloader = DataLoader(
        data_train,
        batch_size=batch_size,
        drop_last=False,
        num_workers=num_workers,
        pin_memory=pin_memory,
        collate_fn=collate_fn,
    )

    val_dataloader = DataLoader(
        data_val,
        batch_size=batch_size,
        shuffle=False,
        drop_last=False,
        num_workers=num_workers,
        pin_memory=pin_memory,
        collate_fn=collate_fn,
    )

    return [
        (train_dataloader, val_dataloader),
        variables,
        out_variables,
        imagespace,
        (norm_mean, norm_std),
        (outnorm_mean, outnorm_std),
    ]


# HOURS_PER_YEAR = 12  # 8760  # 365-day year


def nc2np(
    path, dim_name, variables, years, save_dir, partition, num_shards_per_year, **kwargs
):

    lenallyear = len(years)
    multi_imgs = {}
    img_shp = kwargs.get("img_shp")

    if kwargs.get("ts_type") == "monthly":
        HOURS_PER_YEAR = 12
    else:
        HOURS_PER_YEAR = 8760

    os.makedirs(os.path.join(save_dir, partition), exist_ok=True)

    if partition == "train":
        normalize_mean = {}
        normalize_std = {}
        norm_lats = {}
        lat_chips = {}

        for var in variables:
            emd_path = os.path.join(path, var, dim_name, "esri_model_definition.emd")
            with open(emd_path) as f:
                emd_stats = json.load(f)
            normalize_mean[var] = np.mean(
                np.array([i["Mean"] for i in emd_stats["AllTilesStats"]])[
                    : lenallyear * HOURS_PER_YEAR
                ].reshape(-1, HOURS_PER_YEAR),
                axis=1,
            )[:, None]
            normalize_std[var] = np.mean(
                np.array(
                    [i["StdDev"] for i in emd_stats["AllTilesStats"]], dtype=np.float32
                )[: lenallyear * HOURS_PER_YEAR][:, None].reshape(-1, 12),
                axis=1,
            )[:, None]

    constant_values = {}
    constant_fields = ["lattitude"]  # ["orography", "lattitude"]

    imgs_var_lst = [
        glob.glob(os.path.join(path, var, dim_name, "images", "*.tif"))
        for var in variables
    ]
    tfw_var_lst = [
        glob.glob(os.path.join(path, var, dim_name, "images", "*.tfw"))
        for var in variables
    ]

    for n, (img, tfw) in enumerate(zip(imgs_var_lst[0], tfw_var_lst[0])):
        climatology = {}
        for year, i in zip(years, range(lenallyear)):
            np_vars = {}
            for var in variables:
                img_path = Path(img)
                parts = list(img_path.parts)
                parts[-4] = var
                new_img_path = Path(*parts)
                ds = gdal.Open(str(new_img_path))

                img_arr = ds.ReadAsArray()

                width = ds.RasterXSize
                height = ds.RasterYSize
                gt = ds.GetGeoTransform()

                miny = gt[3] + width * gt[4] + height * gt[5]
                maxy = gt[3]

                lat = np.linspace(maxy, miny, height)
                constant_values["lattitude"] = np.tile(lat[:, None], (1, width))[
                    None, None
                ].repeat(HOURS_PER_YEAR, axis=0)

                for f in constant_fields:
                    np_vars[f] = constant_values[f]

                    if partition == "train":
                        lat_chips[f] = np.tile(lat[:, None], (1, img_shp[1]))

                # code = NAME_TO_VAR[var]

                arr = np.expand_dims(img_arr, 1)

                if partition == "val":
                    arr = arr[-(HOURS_PER_YEAR * lenallyear) :]

                st_id = i * HOURS_PER_YEAR
                end_id = st_id + HOURS_PER_YEAR
                img_save_arr = arr[st_id:end_id]

                np_vars[var] = img_save_arr

                clim_yearly = np_vars[var].mean(axis=0)
                if var not in climatology:
                    climatology[var] = [clim_yearly]
                else:
                    climatology[var].append(clim_yearly)

            np.savez(
                os.path.join(save_dir, partition, f"{year}_{n}.npz"),
                **np_vars,
            )

        for var in climatology.keys():
            climatology[var] = np.stack(climatology[var], axis=0)
        climatology = {k: np.mean(v, axis=0) for k, v in climatology.items()}
        np.savez(
            os.path.join(save_dir, partition, f"climatology_{n}.npz"),
            **climatology,
        )

        if partition == "train":
            norm_lats[os.path.basename(img)[:12]] = lat_chips["lattitude"]

    if partition == "train":
        normalize_mean["lattitude"] = np.array([j for i, j in norm_lats.items()]).mean(
            axis=(0, 1, 2)
        )[None]
        normalize_std["lattitude"] = np.array([j for i, j in norm_lats.items()]).std(
            axis=(0, 1, 2)
        )[None]

        for var in normalize_mean.keys():  # aggregate over the years
            if var not in constant_fields:
                mean, std = normalize_mean[var], normalize_std[var]
                # var(X) = E[var(X|Y)] + var(E[X|Y])
                variance = (
                    (std**2).mean(axis=0)
                    + (mean**2).mean(axis=0)
                    - mean.mean(axis=0) ** 2
                )
                std = np.sqrt(variance)
                # E[X] = E[E[X|Y]]
                mean = mean.mean(axis=0)
                normalize_mean[var] = mean
                normalize_std[var] = std

        np.savez(
            os.path.join(save_dir, "normalize_mean.npz"),
            **normalize_mean,
        )
        np.savez(
            os.path.join(save_dir, "normalize_std.npz"),
            **normalize_std,
        )


def create_data(
    root_dir,
    dim_name,
    save_dir,
    variables,
    start_train_year,
    start_val_year,
    end_year,
    num_shards,
    ts_type,
    img_shp,
):
    assert start_val_year > start_train_year and end_year > start_val_year
    train_years = range(start_train_year, start_val_year)
    val_years = range(start_val_year, end_year)

    os.makedirs(save_dir, exist_ok=True)

    nc2np(
        root_dir,
        dim_name,
        variables,
        train_years,
        save_dir,
        "train",
        num_shards,
        ts_type=ts_type,
        img_shp=img_shp,
    )
    nc2np(
        root_dir,
        dim_name,
        variables,
        val_years,
        save_dir,
        "val",
        num_shards,
        ts_type=ts_type,
        img_shp=img_shp,
    )


def check_timeseries_type(realdates):
    checkmonth = all(j == realdates[0].month for j in [i.month for i in realdates[:3]])
    checkyear = all(j == realdates[0].year for j in [i.year for i in realdates[:3]])
    if checkmonth and checkyear:
        return "hourly"
    else:
        return "monthly"


def create_train_val_sets(path, val_split_pct, working_dir, batch_size, **kwargs):
    path = Path(path)

    if working_dir is not None:
        save_path = working_dir
    else:
        save_path = path

    isExist_data = os.path.exists(os.path.join(save_path, "DATA"))
    isExist_train_data = os.path.exists(os.path.join(save_path, "DATA", "train"))
    isExist_valid_data = os.path.exists(os.path.join(save_path, "DATA", "val"))

    folds = os.listdir(path)

    varfolds = [i for i in folds if i not in ["DATA", "models"]]
    dim_name = [i for i in os.walk(os.path.join(path, varfolds[0]))][0][1][0]

    emd_path = os.path.join(path, varfolds[0], dim_name, "esri_model_definition.emd")
    with open(emd_path) as f:
        emd_stats = json.load(f)
    ImageHeight, ImageWidth = emd_stats.get("ImageHeight"), emd_stats.get("ImageWidth")
    IsMultidimensional = emd_stats.get("IsMultidimensional", False)
    serialDates = emd_stats.get("DimensionValues")
    imagespace = emd_stats.get("ImageSpaceUsed")

    realdates = [datetime.datetime.fromordinal(i + 693593) for i in serialDates]
    years = sorted(set([date.year for date in realdates]))

    ts_type = check_timeseries_type(realdates)

    valsplit = int(val_split_pct * len(years))
    train_dates, val_dates = years[:-valsplit], years[-valsplit:]

    if not isExist_data:
        os.makedirs(os.path.join(save_path, "DATA"))
        if not isExist_train_data:
            os.makedirs(os.path.join(save_path, "DATA", "train"))
        if not isExist_valid_data:
            os.makedirs(os.path.join(save_path, "DATA", "val"))

        create_data(
            path,
            dim_name,
            os.path.join(save_path, "DATA"),
            varfolds,
            start_train_year=train_dates[0],
            start_val_year=train_dates[-1] + 1,
            end_year=val_dates[-1] + 1,
            num_shards=1,
            ts_type=ts_type,
            img_shp=(ImageHeight, ImageWidth),
        )

    if not kwargs.get("out_variables", None):
        out_variables = kwargs.get("out_variables", varfolds)
        out_variables = sorted(out_variables, key=lambda x: varfolds.index(x))
    else:
        out_variables = kwargs.get("out_variables")

    data = GlobalForecastData(
        root_dir=os.path.join(save_path, "DATA"),
        variables=varfolds,
        buffer_size=10000,
        out_variables=out_variables,
        imagespace=imagespace,
        predict_range=kwargs.get("forecast_timesteps", 1),
        time_each_step=kwargs.get("hrs_each_step", 1),
        batch_size=batch_size,
        num_workers=0,
        pin_memory=False,
        ts_type=ts_type,
    )

    data.append((train_dates, val_dates))

    return data


def prepare_climax_data(
    path,
    batch_size,
    val_split_pct,
    working_dir,
    **kwargs,
):
    train_val_dls = create_train_val_sets(
        path, val_split_pct, working_dir, batch_size, **kwargs
    )

    train_dl, valid_dl = train_val_dls[0]

    device = get_device()
    data = ClimaxDataBunch(train_dl, valid_dl, device=device)

    if working_dir is not None:
        path = Path(os.path.abspath(working_dir))
    data.path = Path(path)
    data._temp_folder = _prepare_working_dir(path)

    shape, leadtimes = [(i[0].shape, i[1]) for i, j in data.train_dl][0]
    data._leadtimes = leadtimes

    data._dataset_type = "ClimaX"
    data._variables = train_val_dls[1]
    data._out_variables = train_val_dls[2]
    data._n_channels = len(data._variables)
    data._imagespace = train_val_dls[3]
    data.chp_size = [shape[2], shape[3]]
    data.show_batch = types.MethodType(show_batch, data)
    data._norm_mean, data._norm_std = train_val_dls[4]
    data._outnorm_mean, data._outnorm_std = train_val_dls[5]
    data._trainperiod, data._validperiod = train_val_dls[6]

    return data


def show_results(self, rows, variable, **kwargs):
    variable = self._data._out_variables[0] if variable == "" else variable
    if len(self._data._out_variables) != 1:
        variable_no_x = {i.lower(): n for n, i in enumerate(self._data._out_variables)}[
            variable.lower()
        ]
        variable_no_y = variable_no_x
    else:
        variable_no_x = {i.lower(): n for n, i in enumerate(self._data._variables)}[
            variable.lower()
        ]
        variable_no_y = 0
    from .._data_utils.pix2pix_data import display_row
    from fastai.vision import image2np

    device = next(self.learn.model.parameters()).device.type

    self.learn.model.eval()
    activ = []
    top = get_top_padding(title_font_size=16, nrows=rows, imsize=5)

    denormfunc = lambda btch_arr, mean, std: (btch_arr * std) + mean

    x_batch, y_batch = get_nbatches(
        self._data.valid_dl, ceil(rows / self._data.batch_size)
    )

    x_A = torch.cat([x_batch[i][0] for i in range(len(x_batch))])
    x_B = torch.cat([y_batch[0] for i in range(len(y_batch))])

    for i in range(0, x_A.shape[0], self._data.batch_size):
        leadtime = self._data._leadtimes[: x_A[i : i + self._data.batch_size].shape[0]]
        preds = self.learn.model(x_A[i : i + self._data.batch_size], leadtime, 0, 0, 0)
        activ.append(preds[0])
    activations = torch.cat(activ)

    x_A = denormfunc(
        x_A.cpu(),
        self._data._norm_mean[None, :, None, None],
        self._data._norm_std[None, :, None, None],
    )[:, variable_no_x, None, :, :]
    x_B = denormfunc(
        x_B.cpu(),
        self._data._norm_mean[None, :, None, None],
        self._data._norm_std[None, :, None, None],
    )[:, variable_no_y, None, :, :]
    activations = denormfunc(
        activations.detach().cpu(),
        self._data._norm_mean[None, :, None, None],
        self._data._norm_std[None, :, None, None],
    )[:, variable_no_y, None, :, :]

    rows = min(rows, x_A.shape[0])

    fig, axs = plt.subplots(
        nrows=rows, ncols=3, figsize=(4 * 5, rows * 5), squeeze=False
    )

    def display_row(axes, display, rgb_bands=None):
        for i, ax in enumerate(axes):
            ax.imshow(display[i], cmap="twilight")
            ax.axis("off")

            plt.subplots_adjust(top=top)

            axs[0, 0].set_title(f"Input State", fontsize=15)
            axs[0, 1].set_title(f"Target State", fontsize=15)
            axs[0, 2].set_title(f"Forecasted State", fontsize=15)

    for r in range(rows):
        display_row(
            axs[r],
            (
                image2np(x_A[r].detach()),
                image2np(x_B[r].detach()),
                image2np(activations[r].detach()),
            ),
        )


def show_batch(self, rows=4, variable="", **kwargs):
    """
    This function randomly picks a few training chips and visualizes them.

    =====================   ===========================================
    **Parameter**            **Description**
    ---------------------   -------------------------------------------
    rows                    Optional int. Number of rows of results
                            to be displayed.
    ---------------------   -------------------------------------------
    variable                Optional int. Seriel number of variable
                            to be displayed.
    =====================   ===========================================
    """
    xs, ys, years = [], [], []
    variable = self._out_variables[0] if variable == "" else variable
    if not len(self._out_variables) == 1:
        variable_no_x = {i.lower(): n for n, i in enumerate(self._out_variables)}[
            variable.lower()
        ]
        variable_no_y = variable_no_x
    else:
        variable_no_x = {i.lower(): n for n, i in enumerate(self._variables)}[
            variable.lower()
        ]
        variable_no_y = 0
    for n, imgs in enumerate(self.train_dl):
        if n != rows:
            xs.append(imgs[0][0][:, None, variable_no_x, :, :])
            ys.append(imgs[1][:, None, variable_no_y, :, :])
            years.extend(imgs[0][3])
        else:
            break

    class ClimaxArcGISImage(ArcGISMSImage):
        def show(
            self,
            ax=None,
            show_axis=False,
            title=None,
            return_ax=False,
        ):
            if ax is None:
                ax = plt.subplot(1, 1, 1)
            symbology_data = self.data.cpu().repeat(1, 3, 1, 1)

            data_to_plot = symbology_data[:, 0, :, :].permute(1, 2, 0)
            if not show_axis:
                ax.axis("off")
            ax.imshow(data_to_plot, cmap="twilight")
            if title is not None:
                ax.set_title(title)
            if return_ax:
                return ax

    axs = subplots(len(xs), 2, figsize=(20, rows * 5), squeeze=False)
    for i, (x, y, year) in enumerate(zip(xs, ys, years)):
        for k in range(x.shape[0]):
            if x.data.shape[0] != 1:
                x, y = ClimaxArcGISImage(x.data[k, None]), ClimaxArcGISImage(
                    y.data[k, None]
                )
            x.show(ax=axs[i, 0], **kwargs)
            y.show(ax=axs[i, 1], **kwargs)

            axs[i, 0].set_title(f"Input State - {year}", fontsize=15)
            axs[i, 1].set_title(f"Future State - {year}", fontsize=15)


def remove_nans(pred: torch.Tensor, gt: torch.Tensor):
    # pred and gt are two flattened arrays
    pred_nan_ids = torch.isnan(pred) | torch.isinf(pred)
    pred = pred[~pred_nan_ids]
    gt = gt[~pred_nan_ids]

    gt_nan_ids = torch.isnan(gt) | torch.isinf(gt)
    pred = pred[~gt_nan_ids]
    gt = gt[~gt_nan_ids]

    return pred, gt


def lat_weighted_rmse(self, forecast_steps=10):
    """Latitude weighted root mean squared error

    Args:
        y: [B, V, H, W]
        pred: [B, V, H, W]
        vars: list of variable names
        lat: H
    """

    from scipy import stats
    from skimage.metrics import structural_similarity as ssim

    denormfunc = lambda btch_arr, mean, std: (btch_arr * std) + mean

    preds, y, lats = [], [], []

    for i, j in self._data.valid_dl:
        self.learn.model.eval()
        with torch.no_grad():
            pred = self.learn.model(i[0], i[1], 0, 0, 0)
        preds.append(pred[0])
        y.append(j)
        lats.append(i[2])

    preds = torch.cat(preds, axis=0)
    ys = torch.cat(y, axis=0)
    lat = torch.cat(lats, axis=0).detach().cpu().numpy()

    pred = denormfunc(
        preds.detach().cpu(),
        self._data._norm_mean[None, :, None, None],
        self._data._norm_std[None, :, None, None],
    )
    y = denormfunc(
        ys.detach().cpu(),
        self._data._norm_mean[None, :, None, None],
        self._data._norm_std[None, :, None, None],
    )

    error = (pred - y) ** 2  # [B, V, H, W]

    # lattitude weights
    w_lat = np.cos(np.deg2rad(lat))
    w_lat = w_lat / w_lat.mean()  # (H, )
    w_lat = (
        torch.from_numpy(w_lat)
        .unsqueeze(0)
        .unsqueeze(-1)
        .to(dtype=error.dtype, device=error.device)
    )

    loss_dict = {}
    for i, var in enumerate(self._data._out_variables):
        loss_dict[f"lat_weighted_rmse_{var}"] = round(
            (
                torch.mean(torch.sqrt(torch.mean(error[:, i] * w_lat, dim=(-2, -1))))
            ).item(),
            4,
        )
        pred_pers, y_pers = pred[:, i].flatten(), y[:, i].flatten()
        pred_ssim, y_ssim = pred[:, i, None], y[:, i, None]

        pred_pers, y_pers = remove_nans(pred_pers, y_pers)

        loss_dict[f"pearsonr_{var}"] = round(
            stats.pearsonr(pred_pers.cpu().numpy(), y_pers.cpu().numpy())[0], 4
        )
        data_range = (
            max(pred_ssim.max(), y_ssim.max()) - min(pred_ssim.min(), y_ssim.min())
        ).item()
        loss_dict[f"SSIM_{var}"] = round(
            ssim(
                pred_ssim.detach().cpu().numpy(),
                y_ssim.detach().cpu().numpy(),
                channel_axis=1,
                data_range=data_range,
                full=True,
            )[0],
            4,
        )

    return loss_dict
