#  Copyright (c) Brockmann Consult GmbH, 2025
#  License: MIT

"""
This module provides random number generators.
"""
from typing import Literal

import numpy as np
from numpy import ndarray
from numpy.random import BitGenerator
from numpy.random import Generator

from .interface.generating import Generating
from .interface.generating import Normal
from .interface.generating import Uniform


def default_bit_generator(
    seed: int | np.ndarray | None = None,
) -> BitGenerator:
    """
    Returns the default bit generator.

    :param seed: The seed.
    :return: The default bit generator.
    """
    from numpy.random import PCG64DXSM

    return PCG64DXSM(seed)


def default_generator(seed) -> Generator:
    """
    Returns the default generator.

    :param seed: The seed.
    :return: The default generator.
    """
    return Generator(
        seed
        if isinstance(seed, BitGenerator)
        else default_bit_generator(seed)
    )


class DefaultGenerator(Generating):
    """The default random number generator."""

    _g: Generator

    def __init__(self, seed: int | np.ndarray | BitGenerator | None = None):
        """
        Creates a new random number generator.

        :param seed: The seed.
        """
        self._g = default_generator(seed)

    # noinspection PyTypeChecker
    def next(self) -> int:
        return self._g.integers(0x8000000000000000)


class DefaultUniform(Uniform):
    """The default uniform random deviate."""

    _g: Generator

    def __init__(self, seed: int | np.ndarray | BitGenerator | None = None):
        """
        Creates a new random deviate.

        :param seed: The seed.
        """
        self._g = default_generator(seed)

    def random(self) -> float:
        return self._g.random()

    def randoms(self, randoms: np.ndarray) -> np.ndarray:
        self._g.random(dtype=randoms.dtype, out=randoms)
        return randoms


class DefaultNormal(Normal):
    """The default normal random deviate."""

    _g: Generator

    def __init__(self, seed: int | np.ndarray | BitGenerator | None = None):
        """
        Creates a new random deviate.

        :param seed: The seed.
        """
        self._g = default_generator(seed)

    def random(self) -> float:
        return self._g.standard_normal()

    def randoms(self, randoms: np.ndarray) -> np.ndarray:
        self._g.standard_normal(dtype=randoms.dtype, out=randoms)
        return randoms


class DecileNormal(Normal):
    """
    The decile normal random deviate.

    Generates random deviates, which are uniformly distributed in a
    selected decile of the standard normal distribution.
    """

    _g: Generator

    _q: ndarray[float, float]
    """The deciles of the normal distribution."""
    _s: int | Literal[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    """The decile selector."""

    def __init__(
        self, s: int, seed: int | np.ndarray | BitGenerator | None = None
    ):
        """
        Creates a new random deviate.

        :param s: The decile selector.
        :param seed: The seed.
        """
        self._s = s % 10
        self._g = default_generator(seed)
        self._q = np.ndarray(
            [
                -3.09023,
                -1.28155,
                -0.841621,
                -0.524401,
                -0.253347,
                0.0,
                0.253347,
                0.524401,
                0.841621,
                1.28155,
                3.09023,
            ]
        )

    def random(self) -> float:
        return (self.sup - self.inf) * self._g.random() + self.inf

    def randoms(self, randoms: np.ndarray) -> np.ndarray:
        self._g.random(dtype=randoms.dtype, out=randoms)
        randoms *= self.sup - self.inf
        randoms += self.inf
        return randoms

    @property
    def sup(self) -> float:
        """Returns the supremum of the selected decile."""
        return self._q[self._s + 1]

    @property
    def inf(self) -> float:
        """Returns the infimum of the selected decile."""
        return self._q[self._s]
