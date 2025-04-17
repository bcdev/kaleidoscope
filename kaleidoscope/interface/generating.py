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
    def next_int64(self) -> int:
        """
        Returns a new integer random number.

        Generated random numbers are uniformly distributed in the
        interval [0, 9223372036854775807].

        :return: The new integer random number.
        """


class Univariate(ABC):
    """Univariate random deviates."""

    @abstractmethod
    def next_double(self) -> float:
        """
        Returns a new random number.

        :return: The new random number.
        """

    @abstractmethod
    def next_doubles(self, doubles: np.ndarray) -> np.ndarray:
        """
        Returns a sequence of newly generated random numbers.

        :param doubles: To store the sequence.
        :return: The sequence of newly generated random numbers.
        """


class Multivariate(ABC):
    """Multivariate random deviates."""

    @abstractmethod
    def get(self, i: int) -> Univariate:
        """
        Returns a univariate random variate for a given dimension.

        :param i: The dimension.
        :return: The univariate random variate for the given dimension.
        """


class Normal(Univariate, Multivariate, ABC):
    """
    Normal random deviates.

    Generated random numbers are standard normally distributed.
    """


class Uniform(Univariate, Multivariate, ABC):
    """
    Uniform random deviates.

    Generated random numbers are uniformly distributed in [0, 1).
    """
