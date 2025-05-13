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
    def next(self) -> int:
        """
        Returns a new integer random number.

        Generated random numbers are uniformly distributed in the
        interval [0, 9223372036854775807].

        :return: The new integer random number.
        """


class Deviate(ABC):
    """Random deviates."""

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


class Normal(Deviate, ABC):
    """
    Normal random deviates.

    Generated random numbers are standard normally distributed.
    """


class Uniform(Deviate, ABC):
    """
    Uniform random deviates.

    Generated random numbers are uniformly distributed in [0, 1).
    """
