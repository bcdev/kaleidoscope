#  Copyright (c) Brockmann Consult GmbH, 2024
#  License: MIT

"""
This module defines the metric interface.
"""

from abc import ABCMeta
from abc import abstractmethod
from numbers import Number

from xarray import DataArray


class Metric(metaclass=ABCMeta):
    """
    The metric interface.

    A metric evaluates time series forecasts against reference data.
    """

    @abstractmethod
    def total(self, ref: DataArray, pre: DataArray, **kwargs) -> Number:
        """
        Evaluates the total metric score.

        :param ref: The reference data.
        :param pre: The predicted data.
        :param kwargs: Any keyword arguments.
        :return: The total metric score.
        """

    @abstractmethod
    def map(self, ref: DataArray, pre: DataArray, **kwargs) -> DataArray:
        """
        Evaluates the temporally averaged metric score.

        :param ref: The reference data.
        :param pre: The predicted data.
        :param kwargs: Any keyword arguments.
        :return: The temporally averaged metric score, which is a map.
        """

    @abstractmethod
    def series(self, ref: DataArray, pre: DataArray, **kwargs) -> DataArray:
        """
        Evaluates the laterally averaged metric score.

        :param ref: The reference data.
        :param pre: The predicted data.
        :param kwargs: Any keyword arguments.
        :return: The laterally averaged metric score, which is a time series.
        """
