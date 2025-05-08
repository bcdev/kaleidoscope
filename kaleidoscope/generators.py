#  Copyright (c) Brockmann Consult GmbH, 2025
#  License: MIT

"""
This module provides random number generators.
"""

import numpy as np
from numpy.random import BitGenerator
from numpy.random import Generator

from .interface.generating import Generating
from .interface.generating import Normal
from .interface.generating import Uniform
from .interface.generating import Univariate


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

    def next(self) -> int:
        return self._g.integers(0x8000000000000000)


class DefaultUniform(Uniform):
    """The default uniform random deviate."""

    _g: Generator

    def __init__(self, seed: int | np.ndarray | BitGenerator | None = None):
        """
        Creates a new random variate.

        :param seed: The seed.
        """
        self._g = default_generator(seed)

    def get(self, i: int) -> Univariate:
        return self

    def random(self) -> float:
        return self._g.random()

    def randoms(self, randoms: np.ndarray) -> np.ndarray:
        self._g.standard_normal(dtype=randoms.dtype, out=randoms)
        return randoms


class DefaultNormal(Normal):
    """The default normal random variate."""

    _g: Generator

    def __init__(self, seed: int | np.ndarray | BitGenerator | None = None):
        """
        Creates a new random variate.

        :param seed: The seed.
        """
        self._g = default_generator(seed)

    def get(self, i: int) -> Univariate:
        return self

    def random(self) -> float:
        return self._g.standard_normal()

    def randoms(self, randoms: np.ndarray) -> np.ndarray:
        self._g.standard_normal(dtype=randoms.dtype, out=randoms)
        return randoms
