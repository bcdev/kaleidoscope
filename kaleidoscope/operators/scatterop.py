#  Copyright (c) Brockmann Consult GmbH, 2025
#  License: MIT

"""
This module provides the scatter operator.
"""
import json
import uuid
from argparse import Namespace
from importlib import resources
from typing import Any

import dask.array as da
import numpy as np
from xarray import DataArray
from xarray import Dataset

from .. import __name__
from .. import __version__
from ..algorithms.codec import decode
from ..algorithms.codec import encode
from ..algorithms.randomize import Randomize
from ..generators import DefaultGenerator
from ..interface.logging import Logging
from ..interface.operator import Operator
from ..logger import get_logger


class ScatterOp(Operator):
    """The scatter operator."""

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
        return "scatter"

    def run(self, source: Dataset) -> Dataset:  # noqa: D102
        """
        Runs the operator.

        :param source: The source dataset.
        :return: The result dataset.
        """
        config: dict[str : dict[str:Any]] = self.config.get(
            self._args.source_type, {}
        )
        target: Dataset = Dataset(
            data_vars=source.data_vars,
            coords=source.coords,
            attrs=source.attrs,
        )
        for v, x in target.data_vars.items():
            if v not in config or self._args.selector == 0:
                continue
            get_logger().info(f"starting graph for variable: {v}")
            self.randomize(source, target, v, x, config[v])
            get_logger().info(f"finished graph for variable: {v}")
        target.attrs["monte_carlo_software"] = __name__
        target.attrs["monte_carlo_software_version"] = __version__
        return target

    @property
    def config(self) -> dict[str : dict[str:Any]]:
        """Returns the randomization configuration."""
        package = "kaleidoscope.config"
        name = "config.scatter.json"
        with resources.path(package, name) as resource:
            get_logger().debug(f"reading resource: {resource}")
            with open(resource) as r:
                config = json.load(r)
        return config

    def randomize(
        self,
        source: Dataset,
        target: Dataset,
        v: str,
        x: DataArray,
        config: dict[str:Any],
    ):
        """
        Creates the graph to randomize a variable.

        :param source: The source dataset.
        :param target: The target dataset.
        :param v: The name of the variable.
        :param x: The data of the variable.
        :param config: The randomization configuration.
        """
        if "total" in config:
            s = None
            z = decode(x.data, x.attrs)
            for ref in config["total"]:
                a = decode(target[ref].data, target[ref].attrs)
                b = decode(source[ref].data, source[ref].attrs)
                z = z + (a - b)
            if "clip" in config:
                z = da.clip(z, config["clip"][0], config["clip"][1])
        elif "uncertainty" in config:
            s = self.seed(self.uuid(v))
            f = Randomize(
                m=x.ndim,
                dist=config["distribution"],
                seed=s,
                antithetic=self._args.antithetic,
            )
            u = (
                target[config["uncertainty"]]
                if isinstance(config["uncertainty"], str)
                else DataArray(
                    data=da.full(
                        x.shape, config["uncertainty"], chunks=x.chunks
                    ),
                    coords=x.coords,
                    dims=x.dims,
                    attrs={},
                )
            )
            z = f.apply_to(
                decode(x.data, x.attrs),
                decode(u.data, u.attrs),
                coverage=config.get("coverage", 1.0),
                relative=config.get("relative", False),
                clip=config.get("clip", None),
            )
        else:
            s = self.seed(self.uuid(v))
            f = Randomize(
                m=x.ndim,
                dist=config["distribution"],
                seed=s,
                antithetic=self._args.antithetic,
            )
            b = target[config["bias"]]
            r = target[config["rmsd"]]
            z = f.apply_to(
                decode(x.data, x.attrs),
                decode(r.data, r.attrs),
                decode(b.data, b.attrs),
                clip=config.get("clip", None),
            )
        target[v] = DataArray(
            data=encode(z, x.attrs, x.dtype),
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
                dtype=z.dtype,
            )
        if s is not None:
            target[v].attrs["seed"] = s
        if get_logger().is_enabled(Logging.DEBUG):
            get_logger().debug(f"seed: {s}")
            get_logger().debug(f"min:  {da.nanmin(z).compute() :.3f}")
            get_logger().debug(f"max:  {da.nanmax(z).compute() :.3f}")
            get_logger().debug(f"mean: {da.nanmean(z).compute() :.3f}")
            get_logger().debug(f"std:  {da.nanstd(z).compute() :.3f}")

    # noinspection PyShadowingNames
    def seed(self, uuid: uuid.UUID, n: int = 4) -> np.ndarray:
        """
        Returns the seed sequence used for a given variable.

        The seed sequence is generated using the Philox bit generator,
        which produces truly independent sequences of random numbers for
        different values of the seed.

        :param uuid: The variable and dataset UUID.
        :param n: The length of the seed sequence.
        :return: The seed sequence.
        """
        from numpy.random import Philox

        seed = uuid.int + (
            self._args.selector
            if not self._args.antithetic
            else (self._args.selector + 1) // 2
        )
        g = DefaultGenerator(Philox(seed))
        return np.array([g.next() for _ in range(n)], dtype=np.int64)

    def uuid(self, v: str) -> uuid.UUID:
        """
        Returns a UUID constructed from the variable name and the
        basename of the source file.
        """
        return uuid.uuid5(
            uuid.NAMESPACE_URL, f"{v}-{self._args.source_file.stem}"
        )
