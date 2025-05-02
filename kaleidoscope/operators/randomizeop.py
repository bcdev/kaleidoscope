#  Copyright (c) Brockmann Consult GmbH, 2025
#  License: MIT

"""
This module provides the randomize operator.
"""
import json
from argparse import Namespace
from importlib import resources
from typing import Any

import dask.array as da
import numpy as np
from typing_extensions import override
from xarray import DataArray
from xarray import Dataset

from ..algorithms.codec import Decode
from ..algorithms.codec import Encode
from ..algorithms.randomize import Randomize
from ..interface.logging import Logging
from ..interface.operator import Operator
from ..logger import get_logger


def _hash(name: str) -> int:
    h = 1
    for c in name:
        h = 31 * h + ord(c)
    return h


def _decode(
    x: da.Array, a: dict[str:Any], dtype: np.dtype = np.single
) -> da.Array:
    f = Decode(dtype, x.ndim)
    y = f.apply_to(
        x,
        add_offset=a.get("add_offset", None),
        scale_factor=a.get("add_offset", None),
        fill_value=a.get("_FillValue", None),
        valid_min=a.get("valid_min", None),
        valid_max=a.get("valid_max", None),
    )
    return y


def _encode(x: da.Array, a: dict[str:Any], dtype: np.dtype) -> da.Array:
    f = Encode(dtype, x.ndim)
    y = f.apply_to(
        x,
        add_offset=a.get("add_offset", None),
        scale_factor=a.get("add_offset", None),
        fill_value=a.get("_FillValue", None),
        valid_min=a.get("valid_min", None),
        valid_max=a.get("valid_max", None),
    )
    return y


class RandomizeOp(Operator):
    """The randomize operator."""

    _args: Namespace
    """The configuration parameters."""

    def __init__(self, args: Namespace):
        """
        Creates a new forecast operator instance.

        :param args: The configuration parameters.
        """
        self._args = args

    @property
    @override
    def name(self) -> str:  # noqa: D102
        return "randomize"

    @override
    def run(self, source: Dataset) -> Dataset:  # noqa: D102
        """
        Runs the operator.

        :param source: The source dataset.
        :return: The result dataset.
        """
        target = Dataset(
            data_vars=source.data_vars,
            coords=source.coords,
            attrs=source.attrs,
        )
        config: dict[str : dict[str:Any]] = self.config.get(
            self._args.product_type, {}
        )
        for v, x in target.data_vars.items():
            if v not in config:
                continue

            get_logger().info(f"starting graph for variable: {v}")

            a: dict[str:Any] = config[v]
            f = Randomize(
                np.single,
                x.ndim,
                dist=a["distribution"],
                entropy=self.entropy(v),
            )
            if "uncertainty" in a:
                u = (
                    target[a["uncertainty"]]
                    if isinstance(a["uncertainty"], str)
                    else DataArray(
                        data=da.full(
                            x.shape, a["uncertainty"], chunks=x.chunks
                        ),
                        coords=x.coords,
                        dims=x.dims,
                        attrs={},
                    )
                )
                z = f.apply_to(
                    _decode(x.data, x.attrs),
                    _decode(u.data, u.attrs),
                    coverage=a.get("coverage", 1.0),
                    relative=a.get("relative", False),
                    clip=a.get("clip", None),
                )
            else:
                b = target[a["bias"]]
                r = target[a["rmsd"]]
                z = f.apply_to(
                    _decode(x.data, x.attrs),
                    _decode(r.data, r.attrs),
                    _decode(b.data, b.attrs),
                    clip=a.get("clip", None),
                )

            target[v] = DataArray(
                data=_encode(
                    z,
                    x.attrs,
                    x.dtype,
                ),
                coords=x.coords,
                dims=x.dims,
                attrs=x.attrs,
            )
            if "actual_range" in target[v].attrs:
                target[v].attrs["actual_range"] = np.array(
                    [
                        da.nanmin(z).compute(),
                        da.nanmax(z).compute(),
                    ],
                    dtype=x.dtype,
                )

            if get_logger().is_enabled(Logging.DEBUG):
                get_logger().debug(f"min:  {da.nanmin(z).compute() :.6f}")
                get_logger().debug(f"max:  {da.nanmax(z).compute() :.6f}")
                get_logger().debug(f"mean: {da.nanmean(z).compute() :.6f}")
                get_logger().debug(f"std:  {da.nanstd(z).compute() :.6f}")
            get_logger().info(f"finished graph for variable: {v}")
        return target

    @property
    def config(self) -> dict[str : dict[str:Any]]:
        """Returns the product type configuration."""
        package = "kaleidoscope.config"
        name = "config.random.json"
        with resources.path(package, name) as resource:
            get_logger().debug(f"reading resource: {resource}")
            with open(resource) as r:
                config = json.load(r)
        return config

    def entropy(self, v: str) -> list[int]:
        """
        Returns the entropy of the seed sequence used for a given variable.

        :param v: The name of the variable.
        :return: The entropy.
        """
        return [
            self._args.selector,
            _hash(v),
            _hash(self._args.source_file.stem),
        ]
