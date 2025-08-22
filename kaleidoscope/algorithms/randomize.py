#  Copyright (c) Brockmann Consult GmbH, 2025
#  License: MIT

"""
This module provides the algorithm to randomize data.
"""
from typing import Any
from typing import Literal

import dask.array as da
import numpy as np

from ..generators import DefaultNormal
from ..interface.algorithm import InformedBlockAlgorithm
from ..interface.generating import Normal


class Randomize(InformedBlockAlgorithm):
    """
    The algorithm to randomize data.
    """

    _dist: str
    """The type of measurement error distribution."""

    _root_seed: np.ndarray
    """The root seed."""

    def __init__(
        self,
        dtype: np.dtype = np.single,
        m: int = 2,
        dist: Literal["normal", "lognormal", "chlorophyll"] = "normal",
        seed: np.ndarray | None = None,
        antithetic: bool = False,
    ):
        """
        Creates a new algorithm instance.

        :param dtype: The result data type.
        :param m: The number of input data dimensions.
        :param dist: The type of measurement error distribution.
        :param seed: The root seed.
        :param antithetic: To generate antithetic pairs of random numbers.
        """
        super().__init__(dtype, m, m)
        self._dist = dist
        self._root_seed = seed
        self._antithetic = antithetic

    def chunks(self, *inputs: da.Array) -> tuple[int, ...] | None:
        return None

    @property
    def created_axes(self) -> list[int] | None:
        return None

    @property
    def dropped_axes(self) -> list[int]:
        return []

    def randomize(
        self,
        *data: np.ndarray,
        coverage: Any = 1.0,
        relative: bool = False,
        clip: tuple[Any, Any] | None = None,
        **kwargs,
    ) -> np.ndarray:
        """
        Randomizes data.

        :param data: The data.
        :param coverage: The uncertainty coverage factor.
        :param relative: Uncertainty is given in relative terms.
        :param clip: Where to clip measurement errors.
        :return: The measurement values randomized.
        """
        seed = self.block_seed(kwargs["block_id"])

        x = data[0]
        u = (
            data[1]
            if len(data) == 2
            else np.sqrt(np.square(data[1]) - np.square(data[2]))
        )
        if coverage != 1.0:
            u = u / coverage
        if relative:
            u = u * x
        match self._dist:
            case "normal":
                y = self._normal(seed, x, u)
            case "lognormal":
                y = self._lognormal(seed, x, u)
            case "chlorophyll":
                y = self._chlorophyll(seed, x, u)
            case _:
                y = x
        if clip is not None:
            y = np.clip(y, clip[0], clip[1])
        return np.where(np.isfinite(y), y, x)

    compute_block = randomize

    def block_seed(self, block_id: tuple[int, ...]) -> np.ndarray:
        """Returns the block seed."""
        return np.array([i for i in block_id] + [i for i in self._root_seed])

    def _chlorophyll(
        self, seed: np.ndarray, x: np.ndarray, u: np.ndarray
    ) -> np.ndarray:
        """
        Returns randomized values for ESA CCI ocean colour chlorophyll.

        Uses ESA CCI OC PUG (Equation 2.10).
        """
        return self._lognormal(
            seed, x, x * np.sqrt(np.exp(np.square(np.log(10.0) * u)) - 1.0)
        )

    def _lognormal(
        self, seed: np.ndarray, x: np.ndarray, u: np.ndarray
    ) -> np.ndarray:
        """
        Returns randomized values for log-normally distributed errors.
        """
        v = np.log(1.0 + np.square(u / x))
        m = np.log(x) - 0.5 * v
        return np.exp(self._normal(seed, m, np.sqrt(v)))

    # noinspection PyMethodMayBeStatic
    def _normal(
        self, seed: np.ndarray, x: np.ndarray, u: np.ndarray
    ) -> np.ndarray:
        """
        Returns randomized values for normally distributed errors.
        """
        z: Normal = DefaultNormal(seed, self._antithetic)
        return x + u * z.randoms(np.empty(x.shape, x.dtype))

    @property
    def name(self) -> str:
        return "randomize"
