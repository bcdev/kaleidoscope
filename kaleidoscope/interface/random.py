#  Copyright (c) Brockmann Consult GmbH, 2025
#  License: MIT

"""
This module provides interfaces for generating random numbers.
"""

from abc import ABC
from abc import abstractmethod

import numpy as np


class Generating(ABC):
    """Random number generating interface."""

    @abstractmethod
    def random(self) -> int:
        """
        Returns a new integer random number.

        Generated random numbers are uniformly distributed in the
        interval [0, 18446744073709551615].

        :return: The new integer random number.
        """


class Univariate(ABC):
    """Univariate random variates."""

    @abstractmethod
    def random(self) -> float:
        """
        Returns a new random number.

        :return: The new random number.
        """

    @abstractmethod
    def randoms(self, randoms: np.ndarray) -> np.ndarray:
        """
        Returns a sequence of newly generated random numbers.

        :param randoms: To store the sequence.
        :return: The sequence of newly generated random numbers.
        """


class Multivariate(ABC):
    """Multivariate random variates."""

    @abstractmethod
    def get(self, i: int) -> Univariate:
        """
        Returns a univariate random variate for a given dimension.

        :param i: The dimension.
        :return: The univariate random variate for the given dimension.
        """


class Normal(Univariate, Multivariate, ABC):
    """
    Normal random variates.

    Generated random numbers are standard normally distributed.
    """


class Uniform(Univariate, Multivariate, ABC):
    """
    Uniform random variates.

    Generated random numbers are uniformly distributed in [0, 1).
    """
