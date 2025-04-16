#  Copyright (c) Brockmann Consult, 2024
#  License: MIT

"""
This module provides the algorithm to randomize data.
"""

import dask.array as da
import numpy as np
from typing_extensions import override

from ..interface.algorithm import BlockAlgorithm


class Randomize(BlockAlgorithm):
    """
    The algorithm to randomize data.
    """

    @override
    def chunks(self, *inputs: da.Array) -> tuple[int, ...] | None:
        return None

    @property
    @override
    def created_axes(self) -> list[int] | None:
        return [0]

    @property
    @override
    def dropped_axes(self) -> list[int]:
        return [0]

    # noinspection PyMethodMayBeStatic
    def randomize(
        self, data: np.ndarray, *, test: bool = False
    ) -> np.ndarray:
        """
        Randomizes data.

        :param data: The data.
        :param test: Run in test mode.
        :return: The randomized data.
        """
        return data if test else self.simulate(data, data)

    compute_block = randomize

    # noinspection PyMethodMayBeStatic
    def simulate(self, x: np.ndarray, u: np.ndarray):
        """
        Simulates measurement errors.

        :param x: The measurement.
        :param u: The measurement uncertainty.
        :return: The simulated measurements.
        """
        return x

    @property
    @override
    def name(self) -> str:
        return "randomize"
