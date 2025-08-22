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


def conditional_negate(
    randoms: np.ndarray | float, condition: bool
) -> np.ndarray | float:
    """
    Negates the given random numbers when a condition is met.

    :param randoms: The random numbers.
    :param condition: The condition.
    :return: The negated or original random numbers.
    """
    if condition:
        if isinstance(randoms, np.ndarray):
            randoms[:] = -randoms[:]
        else:
            randoms = -randoms
    return randoms


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
    _antithetic: bool

    def __init__(
        self,
        seed: int | np.ndarray | BitGenerator | None = None,
        antithetic: bool = False,
    ):
        """
        Creates a new random deviate.

        :param seed: The seed.
        :param antithetic: To generate antithetic pairs of random numbers.
        """
        self._g = default_generator(seed)
        self._antithetic = antithetic

    def random(self) -> float:
        return conditional_negate(self._g.standard_normal(), self._antithetic)

    def randoms(self, randoms: np.ndarray) -> np.ndarray:
        self._g.standard_normal(dtype=randoms.dtype, out=randoms)
        return conditional_negate(randoms, self._antithetic)
