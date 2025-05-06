#  Copyright (c) Brockmann Consult GmbH, 2025
#  License: MIT

"""
This module provides algorithms to encode and decode data
according to CF conventions.
"""
from typing import Any

import dask.array as da
import numpy as np
from typing_extensions import override

from ..interface.algorithm import BlockAlgorithm


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

    @override
    def chunks(self, *inputs: da.Array) -> tuple[int, ...] | None:
        return None

    @property
    @override
    def created_axes(self) -> list[int] | None:
        return None

    @property
    @override
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
    @override
    def name(self) -> str:
        return "decode"
