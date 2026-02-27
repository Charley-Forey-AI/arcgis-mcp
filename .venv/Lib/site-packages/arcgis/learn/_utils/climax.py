# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import math
import os
import random
from typing import Collection

from fastai.torch_core import DataLoader, data_collate
import numpy as np
import torch
from torch.utils.data import IterableDataset
from fastai.data_block import DataBunch
from fastai.torch_core import *


class ClimaxDataBunch(DataBunch):
    def __init__(
        self,
        train_dl: DataLoader,
        valid_dl: DataLoader,
        fix_dl: DataLoader = None,
        test_dl: Optional[DataLoader] = None,
        device: torch.device = None,
        dl_tfms: Optional[Collection[Callable]] = None,
        path: PathOrStr = ".",
        collate_fn: Callable = data_collate,
        no_check: bool = False,
    ):
        super().__init__(
            train_dl,
            valid_dl,
            fix_dl,
            test_dl,
            device,
            dl_tfms,
            path,
            collate_fn,
            no_check,
        )

    def _grab_dataset(self, dl: DataLoader):
        ds = dl.dl.dataset
        return ds


def interpolate_pos_embed(model, checkpoint_model, new_size=(64, 128)):
    if "pos_embed" in checkpoint_model:
        pos_embed_checkpoint = checkpoint_model["pos_embed"]
        embedding_size = pos_embed_checkpoint.shape[-1]
        orig_num_patches = pos_embed_checkpoint.shape[-2]
        patch_size = model.patch_size
        w_h_ratio = 2
        orig_h = int((orig_num_patches // w_h_ratio) ** 0.5)
        orig_w = w_h_ratio * orig_h
        orig_size = (orig_h, orig_w)
        new_size = (new_size[0] // patch_size, new_size[1] // patch_size)
        if orig_size[0] != new_size[0]:
            # print(
            #     "Interpolate PEs from %dx%d to %dx%d"
            #     % (orig_size[0], orig_size[1], new_size[0], new_size[1])
            # )
            pos_tokens = pos_embed_checkpoint.reshape(
                -1, orig_size[0], orig_size[1], embedding_size
            ).permute(0, 3, 1, 2)
            new_pos_tokens = torch.nn.functional.interpolate(
                pos_tokens,
                size=(new_size[0], new_size[1]),
                mode="bicubic",
                align_corners=False,
            )
            new_pos_tokens = new_pos_tokens.permute(0, 2, 3, 1).flatten(1, 2)
            checkpoint_model["pos_embed"] = new_pos_tokens


def load_pretrained_path(model, data, backbone):
    if backbone == "5.625deg":
        checkpoint = torch.hub.load_state_dict_from_url(
            "https://huggingface.co/tungnd/climax/resolve/main/5.625deg.ckpt"
        )
    else:
        checkpoint = torch.hub.load_state_dict_from_url(
            "https://huggingface.co/tungnd/climax/resolve/main/1.40625deg.ckpt"
        )

    checkpoint = {
        "state_dict": {
            key.split(".", 1)[1]: value
            for key, value in checkpoint["state_dict"].items()
        }
    }

    checkpoint_model = checkpoint["state_dict"]
    # interpolate positional embedding
    interpolate_pos_embed(model, checkpoint_model, new_size=data.chp_size)

    state_dict = model.state_dict()
    if model.parallel_patch_embed:
        if "token_embeds.proj_weights" not in checkpoint_model.keys():
            raise ValueError(
                "Pretrained checkpoint does not have token_embeds.proj_weights for parallel processing. Please convert the checkpoints first or disable parallel patch_embed tokenization."
            )
    for k in list(checkpoint_model.keys()):
        if "channel" in k:
            checkpoint_model[k.replace("channel", "var")] = checkpoint_model[k]
            del checkpoint_model[k]
    for k in list(checkpoint_model.keys()):
        if (
            k not in state_dict.keys()
            or checkpoint_model[k].shape != state_dict[k].shape
        ):
            del checkpoint_model[k]

    # load pre-trained model
    return model.load_state_dict(checkpoint_model, strict=False)


class NpyReader(IterableDataset):
    def __init__(
        self,
        file_dicts,
        start_idx=None,
        end_idx=None,
        variables=None,
        out_variables=None,
        shuffle: bool = False,
        multi_dataset_training=False,
    ) -> None:
        super().__init__()
        self.random_file_dicts = file_dicts
        self.start_idx = start_idx
        self.end_idx = end_idx
        self.variables = variables
        self.out_variables = out_variables if out_variables is not None else variables
        self.shuffle = shuffle
        self.multi_dataset_training = multi_dataset_training
        # self.lat = 0  # lat

    def __iter__(self):
        random_key = random.randint(0, len(self.random_file_dicts) - 1)
        fle_list = [i for i in self.random_file_dicts[random_key]]
        start_idx = int(self.start_idx * len(fle_list))
        end_idx = int(self.end_idx * len(fle_list))
        fle_list = fle_list[start_idx:end_idx]
        fle_list = [f for f in fle_list if "climatology" not in f]

        if self.shuffle:
            random.shuffle(fle_list)
        iter_start = 0
        iter_end = len(fle_list)

        for idx in range(iter_start, iter_end):
            path = fle_list[idx]
            data = np.load(path)
            year = int(os.path.basename(path)[:4])
            tile_num = os.path.basename(path)[5]
            clim_path = os.path.join(
                os.path.dirname(path), f"climatology_{tile_num}.npz"
            )
            clim = np.load(clim_path)

            yield {
                k: data[k] for k in self.variables
            }, self.variables, self.out_variables, data["lattitude"][
                0, 0, :, 0
            ], year, {
                k: clim[k] for k in self.variables
            }


class Forecast(IterableDataset):
    def __init__(
        self,
        dataset: NpyReader,
        max_predict_range: int = 6,
        random_lead_time: bool = False,
        hrs_each_step: int = 1,
        ts_type: str = "monthly",
    ) -> None:
        super().__init__()
        self.dataset = dataset
        self.max_predict_range = max_predict_range
        self.random_lead_time = random_lead_time
        self.hrs_each_step = hrs_each_step
        self.ts_type = ts_type

    def __len__(self):
        return len([i for i in self.dataset])

    def __iter__(self):
        for data, variables, out_variables, lat, year, clim in self.dataset:
            x = np.concatenate(
                [data[k].astype(np.float32) for k in data.keys()], axis=1
            )
            x = torch.from_numpy(x)
            y = np.concatenate(
                [data[k].astype(np.float32) for k in out_variables], axis=1
            )
            y = torch.from_numpy(y)

            inputs = x[: -self.max_predict_range]  # N, C, H, W

            if self.random_lead_time:
                predict_ranges = torch.randint(
                    low=1, high=self.max_predict_range, size=(inputs.shape[0],)
                )
            else:
                predict_ranges = (
                    torch.ones(inputs.shape[0]).to(torch.long) * self.max_predict_range
                )
            lead_times = self.hrs_each_step * predict_ranges / 100

            lead_times = lead_times.to(inputs.dtype)
            output_ids = torch.arange(inputs.shape[0]) + predict_ranges
            outputs = y[output_ids]

            yield inputs, outputs, lead_times, variables, out_variables, lat, year, clim


class IndividualForecastDataIter(IterableDataset):
    def __init__(
        self,
        dataset,
        transforms: torch.nn.Module,
        output_transforms: torch.nn.Module,
        region_info=None,
    ):
        super().__init__()
        self.dataset = dataset
        self.transforms = transforms
        self.output_transforms = output_transforms
        self.region_info = region_info

    def __len__(self):
        return len(
            [(inp, out) for inp, out, led, var, outvar, lat, year, clim in self.dataset]
        )

    def __repr__(self):
        ds = [
            (inp, out) for inp, out, led, var, outvar, lat, year, clim in self.dataset
        ]
        inp, out = ds[0][0], ds[0][1]
        return f"{self.__class__.__name__}, Input timeseries : {inp.shape}, Output forecast : {out.shape}, items : {len(self)}"

    def __iter__(self):
        for (
            inp,
            out,
            lead_times,
            variables,
            out_variables,
            lat,
            year,
            clim,
        ) in self.dataset:
            assert inp.shape[0] == out.shape[0]
            for i in range(inp.shape[0]):
                yield (
                    self.transforms(inp[i]),
                    lead_times[i],
                    lat,
                    year,
                    clim,
                ), self.output_transforms(out[i])


class NoShuffleIterableDataset(IterableDataset):
    def __init__(self, dataset) -> None:
        super().__init__()
        self.dataset = dataset

    def __len__(self):
        return len([(i, j) for i, j in self.dataset])

    def __repr__(self):
        ds = [(i, j) for i, j in self.dataset]
        inp, out = ds[0][0][0], ds[0][1]
        return f"{self.__class__.__name__}, Input timeseries : {inp.shape}, Output forecast : {out.shape}, items : {len(self)}"

    def __iter__(self):
        for i, j in self.dataset:
            yield (
                i[0],
                i[1],
                i[2],
                i[3],
                i[4],
            ), j


class ShuffleIterableDataset(IterableDataset):
    def __init__(self, dataset, buffer_size: int) -> None:
        super().__init__()
        assert buffer_size > 0
        self.dataset = dataset
        self.buffer_size = buffer_size

    def __len__(self):
        return len([(i, j) for i, j in self.dataset])

    def __repr__(self):
        ds = [(i, j) for i, j in self.dataset]
        inp, out = ds[0][0][0], ds[0][1]
        return f"{self.__class__.__name__}, Input timeseries : {inp.shape}, Output forecast : {out.shape}, items : {len(self)}"

    def __iter__(self):
        buf = []
        for x in self.dataset:
            if len(buf) == self.buffer_size:
                idx = random.randint(0, self.buffer_size - 1)
                yield buf[idx]
                buf[idx] = x
            else:
                buf.append(x)
        random.shuffle(buf)
        while buf:
            yield buf.pop()
