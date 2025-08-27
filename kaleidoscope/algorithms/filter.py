#  Copyright (c) Brockmann Consult GmbH, 2024
#  License: MIT

"""
This module provides the algorithm to apply low-pass filters.
"""

from numbers import Real
from typing import Literal

from dask import array as da
from dask_image.ndfilters import gaussian_filter
from dask_image.ndfilters import median_filter
from dask_image.ndfilters import uniform_filter
from typing_extensions import override

from ..interface.algorithm import Algorithm


class Gaussian(Algorithm):
    """
    Applies a lateral Gaussian filter to data.

    The implementation does not propagate NaN.
    """

    @override
    def apply_to(
        self, data: da.Array, *, dims: tuple[str, ...], fwhm: Real = 1.0
    ) -> da.Array:
        """
        Applies a lateral Gaussian filter to data.

        :param data: The data.
        :param dims: The dimension names.
        :param fwhm: The full width at half maximum of the Gaussian
        kernel (pixels).
        :return: The filtered data.
        """
        sigma = [
            (
                fwhm / 2.35482
                if dim.startswith("lat") or dim.startswith("lon")
                else 0.0
            )
            for dim in dims
        ]
        modes = [
            "wrap" if dim.startswith("lon") else "constant" for dim in dims
        ]
        m = da.isnan(data)
        v = da.where(m, 0.0, data)
        w = da.where(m, 0.0, 1.0)
        v = gaussian_filter(v, sigma, mode=modes)
        w = gaussian_filter(w, sigma, mode=modes)
        return da.where(m, data, v / w)

    @property
    @override
    def name(self) -> str:
        return "gaussian_filter"


class Median(Algorithm):
    """
    Applies a lateral median filter to data.

    The implementation does not propagate NaN.
    """

    @override
    def apply_to(
        self,
        data: da.Array,
        *,
        dims: tuple[str, ...],
        size: int = 3,
        mode: Literal["constant", "wrap"] = "constant",
    ) -> da.Array:
        """
        Applies a lateral median filter to data.

        :param data: The data.
        :param dims: The dimension names.
        :param size: The size of the uniform kernel (pixels).
        :param mode: How to extend the image.
        :return: The filtered data.
        """
        sizes = [
            (size if dim.startswith("lat") or dim.startswith("lon") else 1)
            for dim in dims
        ]
        m = da.isnan(data)
        v = da.where(m, 0.0, data)
        w = da.where(m, 0.0, 1.0)
        v = median_filter(v, sizes, mode=mode)
        w = median_filter(w, sizes, mode=mode)
        return da.where(m, data, v / w)

    @property
    @override
    def name(self) -> str:
        return "median_filter"


class Uniform(Algorithm):
    """
    Applies a lateral uniform (i.e., mean) filter to data.

    The implementation does not propagate NaN.
    """

    @override
    def apply_to(
        self, data: da.Array, *, dims: tuple[str, ...], size: int = 3
    ) -> da.Array:
        """
        Applies a lateral uniform (i.e., mean) filter to data.

        :param data: The data.
        :param dims: The dimension names.
        :param size: The size of the uniform kernel (pixels).
        :return: The filtered data.
        """
        sizes = [
            (size if dim.startswith("lat") or dim.startswith("lon") else 1)
            for dim in dims
        ]
        modes = [
            "wrap" if dim.startswith("lon") else "constant" for dim in dims
        ]
        m = da.isnan(data)
        v = da.where(m, 0.0, data)
        w = da.where(m, 0.0, 1.0)
        v = uniform_filter(v, sizes, mode=modes)
        w = uniform_filter(w, sizes, mode=modes)
        return da.where(m, data, v / w)

    @property
    @override
    def name(self) -> str:
        return "uniform_filter"
