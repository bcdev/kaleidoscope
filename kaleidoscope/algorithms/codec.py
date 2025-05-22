#  Copyright (c) Brockmann Consult GmbH, 2025
#  License: MIT

"""
This module provides algorithms to encode and decode data
according to CF conventions.
"""
from typing import Any

import dask.array as da
import numpy as np

from ..interface.algorithm import BlockAlgorithm


def decode(x: da.Array, a: dict[str:Any]) -> da.Array:
    """Returns decoded data."""
    f = Decode(np.single if x.dtype == np.single else np.double, x.ndim)
    y = f.apply_to(
        x,
        add_offset=a.get("add_offset", None),
        scale_factor=a.get("scale_factor", None),
        fill_value=a.get("_FillValue", None),
        valid_min=a.get("valid_min", None),
        valid_max=a.get("valid_max", None),
    )
    return y


def encode(x: da.Array, a: dict[str:Any], dtype: np.dtype) -> da.Array:
    """Returns encoded data."""
    f = Encode(dtype, x.ndim)
    y = f.apply_to(
        x,
        add_offset=a.get("add_offset", None),
        scale_factor=a.get("scale_factor", None),
        fill_value=a.get("_FillValue", None),
        valid_min=a.get("valid_min", None),
        valid_max=a.get("valid_max", None),
    )
    return y


class Decode(BlockAlgorithm):
    """
    The algorithm to decode data according to CF conventions.
    """

    def __init__(self, dtype: np.dtype, m: int):
        """
        Creates a new algorithm instance.

        :param dtype: The result data type.
        :param m: The number of input data dimensions.
        """
        super().__init__(dtype, m, m)

    def chunks(self, *inputs: da.Array) -> tuple[int, ...] | None:
        return None

    @property
    def created_axes(self) -> list[int] | None:
        return None

    @property
    def dropped_axes(self) -> list[int]:
        return []

    # noinspection PyMethodMayBeStatic
    def decode(
        self,
        x: np.ndarray,
        *,
        add_offset: Any = None,
        scale_factor: Any = None,
        fill_value: Any = None,
        valid_min: Any = None,
        valid_max: Any = None,
    ) -> np.ndarray:
        """
        Decodes data.

        :param x: The data.
        :param add_offset: The add-offset.
        :param scale_factor: The scale factor.
        :param fill_value: The fill value.
        :param valid_min: The valid minimum.
        :param valid_max: The valid maximum.
        :return: The decoded data.
        """
        if (
            fill_value is None
            and add_offset is None
            and scale_factor is None
            and valid_min is None
            and valid_max is None
        ):
            y = x
        else:
            y = x.astype(self.dtype)
            if fill_value is not None:
                y[x == fill_value] = np.nan
            if valid_min is not None:
                y[x < valid_min] = np.nan
            if valid_max is not None:
                y[x > valid_max] = np.nan
            if scale_factor is not None:
                y = y * scale_factor
            if add_offset is not None:
                y = y + add_offset
        return y

    compute_block = decode

    @property
    def name(self) -> str:
        return "decode"


class Encode(BlockAlgorithm):
    """
    The algorithm to encode data according to CF conventions.
    """

    def __init__(self, dtype: np.dtype, m: int):
        """
        Creates a new algorithm instance.

        :param dtype: The result data type.
        :param m: The number of input data dimensions.
        """
        super().__init__(dtype, m, m)

    def chunks(self, *inputs: da.Array) -> tuple[int, ...] | None:
        return None

    @property
    def created_axes(self) -> list[int] | None:
        return None

    @property
    def dropped_axes(self) -> list[int]:
        return []

    # noinspection PyMethodMayBeStatic
    def encode(
        self,
        x: np.ndarray,
        *,
        add_offset: Any = None,
        scale_factor: Any = None,
        fill_value: Any = None,
        valid_min: Any = None,
        valid_max: Any = None,
    ) -> np.ndarray:
        """
        Encodes data.

        :param x: The data.
        :param add_offset: The add-offset.
        :param scale_factor: The scale factor.
        :param fill_value: The fill value.
        :param valid_min: The valid minimum.
        :param valid_max: The valid maximum.
        :return: The encoded data.
        """
        if (
            fill_value is None
            and add_offset is None
            and scale_factor is None
            and valid_min is None
            and valid_max is None
        ):
            y = x
        else:
            y = x.astype(np.double)
            if add_offset is not None:
                y = y - add_offset
            if scale_factor is not None:
                y = y / scale_factor
            if valid_max is not None:
                y[y > valid_max] = valid_max
            if valid_min is not None:
                y[y < valid_min] = valid_min
            if fill_value is not None:
                y[np.isnan(x)] = fill_value
        return y

    compute_block = encode

    @property
    def name(self) -> str:
        return "encode"
