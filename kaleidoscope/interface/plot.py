#  Copyright (c) Brockmann Consult GmbH, 2025
#  License: MIT

"""
This module defines plot interface.
"""

from abc import ABCMeta
from abc import abstractmethod
from typing import Any

from matplotlib.figure import Figure
from xarray import DataArray


class Plot(metaclass=ABCMeta):
    """The plot interface."""

    @abstractmethod
    def plot(
        self,
        data: DataArray | tuple[DataArray, DataArray] | None,
        xlabel: str | None = None,
        ylabel: str | None = None,
        xlim: tuple[Any, Any] | None = None,
        ylim: tuple[Any, Any] | None = None,
        title: str | None = None,
        fn: str | None = None,
        plot_size: tuple[Any, Any] | None = None,
        show: bool = False,
        **kwargs,
    ) -> Figure:
        """
        Plots the given data.

        :param data: The data to plot.
        :param xlabel: The label for the x-axis.
        :param ylabel: The label for the y-axis.
        :param xlim: The limits for the x-axis.
        :param ylim: The limits for the y-axis.
        :param title: The title of the plot.
        :param fn: The file name to save the figure.
        :param plot_size: The size of the plot.
        :param show: Show the figure.
        :param kwargs: Additional keyword arguments.
        :return: The plot.
        """
