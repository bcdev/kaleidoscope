#  Copyright (c) Brockmann Consult GmbH, 2025
#  License: MIT

"""
This module provides the algorithm to randomize data.
"""
from typing import Any
from typing import Literal

import dask.array as da
import numpy as np
from numpy.random import SeedSequence
from typing_extensions import override

from ..generators import DefaultNormal
from ..interface.algorithm import InformedBlockAlgorithm
from ..interface.generating import Normal


def _block_seed(
    block_id: tuple[int, ...], root_seed: np.ndarray
) -> np.ndarray:
    """Returns a random seed array for a given block."""
    work_seed = SeedSequence(_hash(block_id)).generate_state(1)
    return np.array([i for i in work_seed] + [i for i in root_seed])


def _hash(block_id: tuple[int, ...]) -> int:
    """Returns a positive hash value."""
    h = 1
    for i in block_id:
        h = 31 * h + i
    return h


def _chlorophyll(
    seed: np.ndarray, x: np.ndarray, u: np.ndarray
) -> np.ndarray:
    """
    Returns randomized values for ESA CCI ocean colour chlorophyll.

    Uses ESA CCI OC PUG (Equation 2.10).
    """
    return _lognormal(
        seed, x, x * np.sqrt(np.exp(np.square(np.log(10.0) * u)) - 1.0)
    )


def _lognormal(seed: np.ndarray, x: np.ndarray, u: np.ndarray) -> np.ndarray:
    """Returns randomized values for log-normally distributed errors."""
    v = np.log(1.0 + np.square(u / x))
    m = np.log(x) - 0.5 * v
    return np.exp(_normal(seed, m, np.sqrt(v)))


def _normal(seed: np.ndarray, x: np.ndarray, u: np.ndarray) -> np.ndarray:
    """Returns randomized values for normally distributed errors."""
    z: Normal = DefaultNormal(seed)
    return x + u * z.randoms(np.empty(x.shape, x.dtype))


class Randomize(InformedBlockAlgorithm):
    """
    The algorithm to randomize data.
    """

    _dist: Literal["normal", "lognormal", "chlorophyll"] | str
    """The type of measurement error distribution."""

    _root_seed: np.ndarray
    """The root seed."""

    def __init__(
        self,
        dtype: np.dtype,
        m: int,
        dist: Literal["normal", "lognormal", "chlorophyll"] | str = "normal",
        entropy: int | list[int] | None = None,
    ):
        """
        Creates a new algorithm instance.

        :param dtype: The result data type.
        :param m: The number of input array dimensions.
        :param dist: The type of measurement error distribution.
        :param entropy: The entropy to create the seed sequence.
        """
        super().__init__(dtype, m, m)
        self._dist = dist
        self._root_seed = SeedSequence(entropy).generate_state(8)

    @override
    def chunks(self, *inputs: da.Array) -> tuple[int, ...] | None:
        return None

    @property
    @override
    def created_axes(self) -> list[int] | None:
        return None

    @property
    @override
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
        seed = _block_seed(kwargs["block_id"], self._root_seed)

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
                y = _normal(seed, x, u)
            case "lognormal":
                y = _lognormal(seed, x, u)
            case "chlorophyll":
                y = _chlorophyll(seed, x, u)
            case _:
                y = x
        if clip is not None:
            y = np.clip(y, a_min=clip[0], a_max=clip[1])
        return y

    compute_block = randomize

    @property
    @override
    def name(self) -> str:
        return "randomize"
