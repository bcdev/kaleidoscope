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
from ..interface.logging import Logging
from ..interface.operator import Operator
from ..logger import get_logger


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
        subset: Dataset = source.isel(i=slice(1, None))
        for v, x in target.data_vars.items():
            if v not in config:
                continue
            v_unc = config[v].get("uncertainty", f"{v}_unc")
            if v_unc in target:
                continue
            get_logger().info(f"starting graph for variable: {v_unc}")
            x_unc = da.nanstd(decode(subset[v].data, x.attrs), axis=0)
            get_logger().info(f"finished graph for variable: {v_unc}")
            target[v_unc] = DataArray(
                data=encode(x_unc, x.attrs, x.dtype),
                coords=x.coords,
                dims=x.dims,
                attrs=x.attrs,
            )
            for name in config[v].get("attrs_pop", []):
                target[v_unc].attrs.pop(name, None)
            target[v_unc].attrs.update(config[v].get("attrs", {}))
            if "actual_range" in target[v_unc].attrs:
                target[v_unc].attrs["actual_range"] = np.array(
                    [
                        da.nanmin(x_unc).compute(),
                        da.nanmax(x_unc).compute(),
                    ],
                    dtype=x_unc.dtype,
                )
            if "standard_name" in target[v_unc].attrs:
                standard_name = target[v_unc].attrs["standard_name"]
                target[v_unc].attrs[
                    "standard_name"
                ] = f"{standard_name} standard_error"
            if get_logger().is_enabled(Logging.DEBUG):
                get_logger().debug(f"min:  {da.nanmin(x_unc).compute() :.3f}")
                get_logger().debug(f"max:  {da.nanmax(x_unc).compute() :.3f}")
                get_logger().debug(
                    f"mean: {da.nanmean(x_unc).compute() :.3f}"
                )
                get_logger().debug(f"std:  {da.nanstd(x_unc).compute() :.3f}")
        return target

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
