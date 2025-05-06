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
from ..algorithms.randomize import Randomize
from ..generators import DefaultGenerator
from ..interface.logging import Logging
from ..interface.operator import Operator
from ..logger import get_logger


def _hash(name: str) -> int:
    """
    Daniel J. Bernstein hash function.

    Returns a positive hash value.
    """
    h = 5381
    for c in name:
        h = ((h << 5) + h) + ord(c)
    return h


def _decode(
    x: da.Array, a: dict[str:Any], dtype: np.dtype = np.double
) -> da.Array:
    f = Decode(dtype, x.ndim)
    y = f.apply_to(
        x,
        add_offset=a.get("add_offset", None),
        scale_factor=a.get("scale_factor", None),
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
        source_id = source.attrs.get(
            "tracking_id",
            source.attrs.get("uuid", self._args.source_file.stem),
        )
        target: Dataset = Dataset(
            data_vars=source.data_vars,
            coords=source.coords,
            attrs=source.attrs,
        )
        config: dict[str : dict[str:Any]] = self.config.get(
            self._args.source_type, {}
        )
        for v, x in target.data_vars.items():
            if v not in config or self._args.selector == 0:
                continue
            get_logger().info(f"starting graph for variable: {v}")
            a: dict[str:Any] = config[v]
            f = Randomize(
                m=x.ndim,
                dist=a["distribution"],
                entropy=self.entropy(v, source_id),
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
            if get_logger().is_enabled(Logging.DEBUG):
                get_logger().debug(f"min:  {da.nanmin(z).compute() :.3f}")
                get_logger().debug(f"max:  {da.nanmax(z).compute() :.3f}")
                get_logger().debug(f"mean: {da.nanmean(z).compute() :.3f}")
                get_logger().debug(f"std:  {da.nanstd(z).compute() :.3f}")
            target[v] = DataArray(
                data=z, coords=x.coords, dims=x.dims, attrs=x.attrs
            )
            # target[v].attrs.pop("valid_min", None)
            # target[v].attrs.pop("valid_max", None)
            target[v].attrs["dtype"] = x.dtype
            target[v].attrs["actual_range"] = np.array(
                [
                    da.nanmin(z).compute(),
                    da.nanmax(z).compute(),
                ],
                dtype=z.dtype,
            )
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

    def entropy(self, vid: str, did: str, n: int = 8) -> list[int]:
        """
        Returns the entropy of the seed sequence used for a given variable.

        Entropy is generated using the Philox bit generator, which produces
        truly independent sequences for different values of the seed.

        :param vid: The variable ID.
        :param did: The dataset ID.
        :param n: The length of the seed sequence.
        :return: The entropy.
        """
        from numpy.random import Philox

        seed = _hash(f"{vid}-{did}") + self._args.selector
        g = DefaultGenerator(Philox(seed))
        return [g.next() for _ in range(n)]
