#  Copyright (c) Brockmann Consult GmbH, 2025
#  License: MIT

"""
This module provides the randomize operator.
"""

from argparse import Namespace
from typing import Any

import dask.array as da
from typing_extensions import override
from xarray import DataArray
from xarray import Dataset

from ..algorithms.randomize import Randomize
from ..interface.logging import Logging
from ..interface.operator import Operator
from ..logger import get_logger


def _hash(name: str) -> int:
    h = 1
    for c in name:
        h = 31 * h + ord(c)
    return h


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
            attrs: dict[str:Any] = config[v]
            f = Randomize(
                x.dtype,
                x.ndim,
                dist=attrs["distribution"],
                entropy=self.entropy(),
            )
            if "uncertainty" in attrs:
                u = (
                    target[attrs["uncertainty"]]
                    if isinstance(attrs["uncertainty"], str)
                    else DataArray(
                        data=da.full(
                            x.shape, attrs["uncertainty"], chunks=x.chunks
                        ),
                        coords=x.coords,
                        dims=x.dims,
                    )
                )
                target[v] = DataArray(
                    data=f.apply_to(
                        x.data,
                        u.data,
                        coverage=attrs.get("coverage", 1.0),
                        relative=attrs.get("relative", False),
                        clip=attrs.get("clip", None),
                    ),
                    coords=x.coords,
                    dims=x.dims,
                    name=x.name,
                    attrs=x.attrs,
                )
            elif "bias" in attrs and "rmsd" in attrs:
                b = target[attrs["bias"]]
                r = target[attrs["rmsd"]]
                target[v] = DataArray(
                    data=f.apply_to(
                        x.data,
                        r.data,
                        b.data,
                        clip=attrs.get("clip", [None, None]),
                    ),
                    coords=x.coords,
                    dims=x.dims,
                    name=x.name,
                    attrs=x.attrs,
                )
            if get_logger().is_enabled(Logging.DEBUG):
                get_logger().debug(
                    f"source[{v}] min:  "
                    f"{source[v].quantile(0.0001).values.item() :.6f}"
                )
                get_logger().debug(
                    f"target[{v}] min:  "
                    f"{target[v].quantile(0.0001).values.item() :.6f}"
                )
                get_logger().debug(
                    f"source[{v}] max:  "
                    f"{source[v].quantile(0.9999).values.item() :.6f}"
                )
                get_logger().debug(
                    f"target[{v}] max:  "
                    f"{target[v].quantile(0.9999).values.item() :.6f}"
                )
                get_logger().debug(
                    f"source[{v}] mean: "
                    f"{source[v].mean().values.item() :.6f}"
                )
                get_logger().debug(
                    f"target[{v}] mean: "
                    f"{target[v].mean().values.item() :.6f}"
                )
                get_logger().debug(
                    f"source[{v}] std:  "
                    f"{source[v].std().values.item() :.6f}"
                )
                get_logger().debug(
                    f"target[{v}] std:  "
                    f"{target[v].std().values.item() :.6f}"
                )
            get_logger().info(f"finished graph for variable: {v}")
        return target

    @property
    def config(self) -> dict[str : dict[str:Any]]:
        """Returns the product type configuration."""
        return {
            "esa-cci-oc": {  # the product type
                "Rrs_412": {  # the variable to randomize
                    # the associated bias variable
                    "bias": "Rrs_412_bias",
                    # the associated RMSD variable
                    "rmsd": "Rrs_412_rmsd",
                    # the error distribution
                    "distribution": "lognormal",
                },
                "Rrs_443": {
                    "bias": "Rrs_443_bias",
                    "rmsd": "Rrs_443_rmsd",
                    "distribution": "lognormal",
                },
                "Rrs_490": {
                    "bias": "Rrs_490_bias",
                    "rmsd": "Rrs_490_rmsd",
                    "distribution": "lognormal",
                },
                "Rrs_510": {
                    "bias": "Rrs_510_bias",
                    "rmsd": "Rrs_510_rmsd",
                    "distribution": "lognormal",
                },
                "Rrs_560": {
                    "bias": "Rrs_560_bias",
                    "rmsd": "Rrs_560_rmsd",
                    "distribution": "lognormal",
                },
                "Rrs_665": {
                    "bias": "Rrs_665_bias",
                    "rmsd": "Rrs_665_rmsd",
                    "distribution": "normal",
                },
                "adg_412": {
                    "bias": "adg_412_bias",
                    "rmsd": "adg_412_rmsd",
                    "distribution": "lognormal",
                },
                "adg_443": {
                    "bias": "adg_443_bias",
                    "rmsd": "adg_443_rmsd",
                    "distribution": "lognormal",
                },
                "adg_490": {
                    "bias": "adg_490_bias",
                    "rmsd": "adg_490_rmsd",
                    "distribution": "lognormal",
                },
                "adg_510": {
                    "bias": "adg_510_bias",
                    "rmsd": "adg_510_rmsd",
                    "distribution": "lognormal",
                },
                "adg_560": {
                    "bias": "adg_560_bias",
                    "rmsd": "adg_560_rmsd",
                    "distribution": "lognormal",
                },
                "adg_665": {
                    "bias": "adg_665_bias",
                    "rmsd": "adg_665_rmsd",
                    "distribution": "lognormal",
                },
                "aph_412": {
                    "bias": "aph_412_bias",
                    "rmsd": "aph_412_rmsd",
                    "distribution": "lognormal",
                },
                "aph_443": {
                    "bias": "aph_443_bias",
                    "rmsd": "aph_443_rmsd",
                    "distribution": "lognormal",
                },
                "aph_490": {
                    "bias": "aph_490_bias",
                    "rmsd": "aph_490_rmsd",
                    "distribution": "lognormal",
                },
                "aph_510": {
                    "bias": "aph_510_bias",
                    "rmsd": "aph_510_rmsd",
                    "distribution": "lognormal",
                },
                "aph_560": {
                    "bias": "aph_560_bias",
                    "rmsd": "aph_560_rmsd",
                    "distribution": "lognormal",
                },
                "aph_665": {
                    "bias": "aph_665_bias",
                    "rmsd": "aph_665_rmsd",
                    "distribution": "lognormal",
                },
                "kd_490": {
                    "bias": "kd_490_bias",
                    "rmsd": "kd_490_rmsd",
                    "distribution": "lognormal",
                },
                "chlor_a": {
                    "bias": "chlor_a_log10_bias",
                    "rmsd": "chlor_a_log10_rmsd",
                    "distribution": "chlorophyll",
                },
            },
            "esa-scope-exchange": {
                "fco2": {
                    "uncertainty": "fco2_tot_unc",
                    # the uncertainty interval coverage factor
                    "coverage": 2.0,
                    "distribution": "lognormal",
                },
                "flux": {
                    "uncertainty": "flux_unc",
                    # uncertainty is stated in relative terms
                    "relative": True,
                    "coverage": 2.0,
                    "distribution": "normal",
                },
                "ta": {
                    "uncertainty": "ta_tot_unc",
                    "coverage": 2.0,
                    "distribution": "lognormal",
                    # clip to interval
                    "clip": (None, 3400.0),
                },
                "dic": {
                    "uncertainty": "dic_tot_unc",
                    "coverage": 2.0,
                    "distribution": "lognormal",
                    "clip": (None, 3200.0),
                },
                "pH": {
                    "uncertainty": "pH_tot_unc",
                    "coverage": 2.0,
                    "distribution": "normal",
                },
                "saturation_aragonite": {
                    "uncertainty": "saturation_aragonite_tot_unc",
                    "coverage": 2.0,
                    "distribution": "lognormal",
                    "clip": (None, 6.0),
                },
            },
            "ghrsst": {
                "analysed_sst": {
                    # the associated uncertainty variable or a constant value
                    "uncertainty": "analysed_sst_uncertainty",
                    "distribution": "normal",
                },
            },
        }

    def entropy(self) -> list[int]:
        return [self._args.selector, _hash(self._args.source_file.stem)]
