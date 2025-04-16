#  Copyright (c) Brockmann Consult GmbH, 2024
#  License: MIT

"""
This module provides the randomize operator.
"""

from argparse import Namespace

from typing_extensions import override
from xarray import DataArray
from xarray import Dataset

from ..algorithms.randomize import Randomize
from ..interface.operator import Operator


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

        for name, array in source.data_vars.items():
            if array.dtype.kind != "f":
                continue
            f = Randomize(array.dtype, array.ndim, array.ndim)
            source[name] = DataArray(
                data=f.apply_to(array.data, test=self._args.test),
                coords=array.coords,
                dims=array.dims,
                name=array.name,
                attrs=array.attrs,
            )
        return source
