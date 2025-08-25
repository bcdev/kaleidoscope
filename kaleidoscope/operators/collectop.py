#  Copyright (c) Brockmann Consult GmbH, 2025
#  License: MIT

"""
This module provides the collect operator.
"""
import json
from argparse import Namespace
from importlib import resources
from typing import Any

import dask.array as da
import numpy as np
from xarray import DataArray
from xarray import Dataset

from ..algorithms.codec import decode
from ..algorithms.codec import encode
from ..algorithms.filter import Gaussian
from ..interface.logging import Logging
from ..interface.operator import Operator
from ..logger import get_logger


def _filter(x: da.Array, dims: tuple) -> da.Array:
    """
    Applies a lateral low pass filter to suppress statistical
    fluctuations caused by finite sampling of error probability
    density functions.
    """
    return Gaussian(dtype=x.dtype, m=x.ndim, n=x.ndim).apply_to(
        x, dims=dims, fwhm=4.0
    )


def _std(x: da.Array, dims: tuple, filtered: bool = False) -> da.Array:
    """
    Returns the standard deviation of simulated errors.

    Here, the standard deviation corresponds to the root
    mean squared error, since the mean simulated error
    vanishes. The standard deviation can be low-pass filtered
    to suppress statistical fluctuations resulting from the
    Monte Carlo method.
    """
    return da.sqrt(
        _filter(_mse(x[1:] - x[:1]), dims)
        if filtered
        else _mse(x[1:] - x[:1])
    )


def _mse(errors: da.Array) -> da.Array:
    """Returns the mean squared error."""
    return da.where(
        da.count_nonzero(da.isfinite(errors), axis=0) > 0,
        da.nanmean(da.square(errors), axis=0),
        np.nan,
    )


def _set_coordinate_attr(x: DataArray):
    """Sets the coordinate attribute, if applicable."""
    if x.coords.keys():
        x.attrs["coordinates"] = " ".join(map(str, x.coords.keys()))


def _set_standard_name_attr(x: DataArray):
    """Sets the standard name attribute, if applicable"""
    if "standard_name" in x.attrs:
        standard_name = x.attrs["standard_name"]
        x.attrs["standard_name"] = f"{standard_name} standard_error"


def _set_title_attr(x: DataArray):
    """Sets the title attribute, if applicable."""
    if "title" in x.attrs:
        title = x.attrs["title"]
        x.attrs["title"] = f"standard uncertainty of {title}"


class CollectOp(Operator):
    """The collect operator."""

    _args: Namespace
    """The configuration parameters."""

    def __init__(self, args: Namespace):
        """
        Creates a new operator instance.

        :param args: The configuration parameters.
        """
        self._args = args

    @property
    def name(self) -> str:  # noqa: D102
        return "collect"

    def run(self, source: Dataset) -> Dataset:  # noqa: D102
        """
        Runs the operator.

        :param source: The source dataset.
        :return: The result dataset.
        """
        config: dict[str : dict[str:Any]] = self.config
        target: Dataset = source.isel(i=0)
        for v, x in target.data_vars.items():
            if v not in config:
                continue
            self.add_uncertainty(target, source, v, x)
            if config[v].get("filter", False):
                self.add_uncertainty(target, source, v, x, filtered=True)
        return target

    def add_uncertainty(self, target, source, v, x, filtered: bool = False):
        """
        Adds an uncertainty variable to the target dataset.

        :param target: The target dataset.
        :param source: The source dataset.
        :param v: The name of the source variable.
        :param x: The data of the source variable.
        :param filtered: To apply a low-pass filter to the error variance.
        """
        config: dict[str : dict[str:Any]] = self.config
        v_unc = config[v].get("uncertainty", f"{v}_unc")
        if filtered:
            v_unc = f"{v_unc}_filtered"
        if v_unc in target:
            return
        get_logger().info(f"starting graph for variable: {v_unc}")
        x_unc = _std(decode(source[v].data, x.attrs), x.dims, filtered)
        get_logger().info(f"finished graph for variable: {v_unc}")
        target[v_unc] = DataArray(
            data=encode(x_unc, x.attrs, x.dtype),
            coords=x.coords,
            dims=x.dims,
            attrs=x.attrs,
        )
        if "actual_range" in target[v_unc].attrs:
            target[v_unc].attrs["actual_range"] = np.array(
                [
                    da.nanmin(x_unc).compute(),
                    da.nanmax(x_unc).compute(),
                ],
                dtype=x_unc.dtype,
            )
        if filtered:
            target[v_unc].attrs[
                "comment"
            ] = "filtered variant of standard uncertainty"
        _set_coordinate_attr(target[v_unc])
        _set_standard_name_attr(target[v_unc])
        _set_title_attr(target[v_unc])
        for attr in config[v].get("attrs_pop", []):
            target[v_unc].attrs.pop(attr, None)
        target[v_unc].attrs.update(config[v].get("attrs", {}))
        if get_logger().is_enabled(Logging.DEBUG):
            get_logger().debug(f"min:  {da.nanmin(x_unc).compute() :.3f}")
            get_logger().debug(f"max:  {da.nanmax(x_unc).compute() :.3f}")
            get_logger().debug(f"mean: {da.nanmean(x_unc).compute() :.3f}")
            get_logger().debug(f"std:  {da.nanstd(x_unc).compute() :.3f}")

    @property
    def config(self) -> dict[str : dict[str:Any]]:
        """Returns the randomization configuration."""
        package = "kaleidoscope.config"
        name = "config.collect.json"
        with resources.path(package, name) as resource:
            get_logger().debug(f"reading resource: {resource}")
            with open(resource) as r:
                config = json.load(r)
        return config
