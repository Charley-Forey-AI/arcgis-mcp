import os
import math
from pathlib import Path

import torch
from torch import nn
import torch.nn.functional as F

from einops import rearrange
from arcgis import GIS
from ._spherical_harmonics_ylm import SH as SH_analytic
from arcgis.learn._utils.common import _temp_dlpk


def weight_download_url(item_id):
    """
    Returns a function that downloads the weights from the given URL.
    """
    # check Arcgis pro cache dir
    cache_dir = Path.home() / "AppData\Local\ESRI\DeepLearning\SatClip"
    gis = GIS("home")
    online_model = gis.content.get(item_id)
    # download the model using arcgis python
    if not os.path.exists(cache_dir / online_model.name):
        download_path = online_model.download(save_path=cache_dir)
    else:
        download_path = cache_dir / online_model.name
    extracted_path = _temp_dlpk(download_path)
    return extracted_path


def exists(val):
    return val is not None


def cast_tuple(val, repeat=1):
    return val if isinstance(val, tuple) else ((val,) * repeat)


class Sine(nn.Module):
    def __init__(self, w0=1.0):
        super().__init__()
        self.w0 = w0

    def forward(self, x):
        return torch.sin(self.w0 * x)


class Siren(nn.Module):
    def __init__(
        self,
        dim_in,
        dim_out,
        w0=1.0,
        c=6.0,
        is_first=False,
        use_bias=True,
        activation=None,
        dropout=False,
    ):
        super().__init__()
        self.dim_in = dim_in
        self.is_first = is_first
        self.dim_out = dim_out
        self.dropout = dropout

        weight = torch.zeros(dim_out, dim_in)
        bias = torch.zeros(dim_out) if use_bias else None
        self.init_(weight, bias, c=c, w0=w0)

        self.weight = nn.Parameter(weight)
        self.bias = nn.Parameter(bias) if use_bias else None
        self.activation = Sine(w0) if activation is None else activation

    def init_(self, weight, bias, c, w0):
        dim = self.dim_in

        w_std = (1 / dim) if self.is_first else (math.sqrt(c / dim) / w0)
        weight.uniform_(-w_std, w_std)

        if exists(bias):
            bias.uniform_(-w_std, w_std)

    def forward(self, x):
        out = F.linear(x, self.weight, self.bias)
        if self.dropout:
            out = F.dropout(out, training=self.training)
        out = self.activation(out)
        return out


class SirenNet(nn.Module):
    def __init__(
        self,
        dim_in,
        dim_hidden,
        dim_out,
        num_layers,
        w0=1.0,
        w0_initial=30.0,
        use_bias=True,
        final_activation=None,
        degreeinput=False,
        dropout=True,
    ):
        super().__init__()
        self.num_layers = num_layers
        self.dim_hidden = dim_hidden
        self.degreeinput = degreeinput

        self.layers = nn.ModuleList([])
        for ind in range(num_layers):
            is_first = ind == 0
            layer_w0 = w0_initial if is_first else w0
            layer_dim_in = dim_in if is_first else dim_hidden

            self.layers.append(
                Siren(
                    dim_in=layer_dim_in,
                    dim_out=dim_hidden,
                    w0=layer_w0,
                    use_bias=use_bias,
                    is_first=is_first,
                    dropout=dropout,
                )
            )

        final_activation = (
            nn.Identity() if not exists(final_activation) else final_activation
        )
        self.last_layer = Siren(
            dim_in=dim_hidden,
            dim_out=dim_out,
            w0=w0,
            use_bias=use_bias,
            activation=final_activation,
            dropout=False,
        )

    def forward(self, x, mods=None):
        # do some normalization to bring degrees in a -pi to pi range
        if self.degreeinput:
            x = torch.deg2rad(x) - torch.pi

        mods = cast_tuple(mods, self.num_layers)

        for layer, mod in zip(self.layers, mods):
            x = layer(x)

            if exists(mod):
                x *= rearrange(mod, "d -> () d")

        return self.last_layer(x)


class SphericalHarmonics(nn.Module):
    def __init__(self, legendre_polys: int = 40):
        """
        legendre_polys: determines the number of legendre polynomials.
                        more polynomials lead more fine-grained resolutions
        calculation of spherical harmonics:
            analytic uses pre-computed equations. This is exact, but works only up to degree 50,
            closed-form uses one equation but is computationally slower (especially for high degrees)
        """
        super(SphericalHarmonics, self).__init__()
        self.L, self.M = int(legendre_polys), int(legendre_polys)
        self.embedding_dim = self.L * self.M
        self.SH = SH_analytic

    def forward(self, lonlat):
        lon, lat = lonlat[:, 0], lonlat[:, 1]

        # convert degree to rad
        phi = torch.deg2rad(lon + 180)
        theta = torch.deg2rad(lat + 90)

        Y = []
        for l in range(self.L):
            for m in range(-l, l + 1):
                y = self.SH(m, l, phi, theta)
                if isinstance(y, float):
                    y = y * torch.ones_like(phi)
                Y.append(y)

        return torch.stack(Y, dim=-1)


class SatClipLocationEncoder(torch.nn.Module):
    def __init__(self, posenc: nn.Module, nnet: SirenNet):
        """
        posenc: positional encoding module, e.g., SphericalHarmonics
        nnet: neural network module, e.g., SirenNet
        """
        super(SatClipLocationEncoder, self).__init__()
        self.posenc = posenc
        self.nnet = nnet

    def forward(self, x):
        x = self.posenc(x)
        return self.nnet(x)
