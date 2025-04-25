#  Copyright (c) Brockmann Consult GmbH, 2025
#  License: MIT

"""
This module defines algorithm interfaces.
"""

from abc import ABCMeta
from abc import abstractmethod
from typing import Literal

import dask.array as da
import numpy as np
from typing_extensions import override


class Algorithm(metaclass=ABCMeta):
    """The algorithm interface."""

    _dtype: np.dtype
    """The result data type."""
    _m: int
    """The number of input array dimensions."""
    _n: int
    """The number of output array dimensions."""

    def __init__(self, dtype: np.dtype, m: int = 2, n: int = 2):
        """
        Creates a new algorithm instance.

        @param[in] dtype The result data type.
        @param[in] m The number of input array dimensions.
        @param[in] n The number of output array dimensions.
        """
        self._dtype = dtype
        self._m = m
        self._n = n

    @property
    def dtype(self) -> np.dtype:
        """Returns the result data type."""
        return self._dtype

    @property
    @abstractmethod
    def name(self) -> str:
        """Returns the name of the algorithm."""

    @abstractmethod
    def apply_to(self, *inputs: da.Array, **kwargs) -> da.Array:
        """
        Applies an algorithm to the given inputs and returns the result.

        @param[in] inputs The input Dask arrays.
        @param[in] kwargs Any additional keyword arguments of the algorithm.
        :return: A result Dask array.
        """

    @property
    def meta(self) -> np.ndarray:
        """
        Returns a surrogate instance of the result.

        The surrogate instance represents the result array type
        and data type.
        """
        return np.zeros(0, self._dtype)

    @property
    def nan(self):
        """Returns not-a-number (NaN) of appropriate result data type."""
        if self.dtype == np.dtype("int8"):
            nan = np.int8(-127)
        elif self.dtype == np.dtype("int16"):
            nan = np.int16(-32767)
        elif self.dtype == np.dtype("int32"):
            nan = np.int32(-2147483647)
        elif self.dtype == np.dtype("int64"):
            nan = np.int64(-9223372036854775807)
        else:  # unsigned integral and real types are fine with
            nan = np.full(1, np.nan, dtype=self.dtype).item()
        return nan

    @property
    def category(self) -> Literal["kaleidoscope"]:
        """Returns the algorithm category name."""
        return "kaleidoscope"

    def mark(self, result: da.Array) -> da.Array:
        """
        A convenience method.

        Shall be called by implementing classes to mark the result
        array properly, if Dask functions do not consider the name
        keyword argument. The mark is useful for monitoring and
        timing computations.

        :param result: A result array.
        :return: The renamed result array.
        """
        return result.map_blocks(
            lambda x: x, name=f"{self.category}-{self.name}"
        )


class BlockAlgorithm(Algorithm, metaclass=ABCMeta):
    """
    Interface class for an algorithm that constitutes a mapping of
    blocks without overlap between blocks.
    """

    def __init__(self, dtype: np.dtype, m: int = 2, n: int = 2):
        """
        Creates a new algorithm instance.

        :param dtype: The result data type.
        :param m: The number of input array dimensions.
        :param n: The number of output array dimensions.
        """
        super().__init__(dtype, m, n)

    @abstractmethod
    def chunks(self, *inputs: da.Array) -> tuple[int, ...] | None:
        """
        Returns the shape of computed blocks, if not preserved.

        If preserved, `None` is returned.
        """

    @property
    @abstractmethod
    def created_axes(self) -> list[int] | None:
        """
        Returns the list of dimensions created.

        If no dimensions are created, `None` is returned.
        """

    @property
    @abstractmethod
    def dropped_axes(self) -> list[int]:
        """
        Returns the list of dimensions annihilated.

        If no dimensions are annihilated, an empty list is returned.
        """

    @abstractmethod
    def compute_block(self, *inputs: np.ndarray, **kwargs) -> np.ndarray:
        """
        Evaluates the algorithm for a single block of data and returns
        the result.

        :param inputs: The input data.
        :param kwargs: Any additional keyword arguments of the computation.
        :return: The result of the computation.
        """

    def compute_block_typed(
        self, *inputs: np.ndarray, **kwargs
    ) -> np.ndarray:
        """
        Evaluates the algorithm for a single block of data and converts
        the result into the correct type, if necessary.

        :param inputs: The input data.
        :param kwargs: Any additional keyword arguments of the computation.
        :return: The result of the computation.
        """
        result = self.compute_block(*inputs, **kwargs)
        assert result.ndim == self._n, (
            f"algorithm '{self.name}' returned array of invalid dimension "
            f"{result.ndim} != {self._n}"
        )
        return result.astype(self.dtype, copy=False)

    @override
    def apply_to(self, *inputs: da.Array, **kwargs) -> da.Array:  # noqa: D102
        return da.map_blocks(
            self.compute_block_typed,
            *inputs,
            name=f"{self.category}-{self.name}",
            dtype=self.dtype,
            chunks=self.chunks(*inputs),
            drop_axis=self.dropped_axes,
            new_axis=self.created_axes,
            meta=self.meta,
            **kwargs,
        )


class InformedBlockAlgorithm(Algorithm, metaclass=ABCMeta):
    """
    Interface class for an algorithm that constitutes a mapping of
    blocks without overlap between blocks and uses information on
    where a block is within a Dask array.
    """

    def __init__(self, dtype: np.dtype, m: int = 2, n: int = 2):
        """
        Creates a new algorithm instance.

        :param dtype: The result data type.
        :param m: The number of input array dimensions.
        :param n: The number of output array dimensions.
        """
        super().__init__(dtype, m, n)

    @abstractmethod
    def chunks(self, *inputs: da.Array) -> tuple[int, ...] | None:
        """
        Returns the shape of computed blocks if `compute_blocks` does
        not preserve shape. If the shape is preserved, `None` is returned.
        """

    @property
    @abstractmethod
    def created_axes(self) -> list[int] | None:
        """
        Returns the list of dimensions created by `compute_blocks`.
        If no dimensions are created, `None` is returned.
        """

    @property
    @abstractmethod
    def dropped_axes(self) -> list[int]:
        """
        Returns the list of dimensions annihilated by `compute_blocks`.
        If no dimensions are annihilated, an empty list is returned.
        """

    @abstractmethod
    def compute_block(self, *inputs: np.ndarray, **kwargs) -> np.ndarray:
        """
        Evaluates the algorithm for a single block of data and returns
        the result.

        :param inputs: The input data.
        :param kwargs: Any additional keyword arguments of the computation.
        :return: The result of the computation.
        """

    def compute_block_typed(
        self, *inputs: np.ndarray, block_id=None, **kwargs
    ) -> np.ndarray:
        """
        Evaluates the algorithm for a single block of data and converts
        the result into the correct type, if necessary.

        :param inputs: The input data.
        :param block_id: The block identifier.
        :param kwargs: Any additional keyword arguments of the computation.
        :return: The result of the computation.
        """
        result = self.compute_block(*inputs, block_id=block_id, **kwargs)
        assert result.ndim == self._n, (
            f"Algorithm {self.name} returned array of invalid dimension "
            f"{result.ndim} != {self._n}"
        )
        return result.astype(self.dtype, copy=False)

    @override
    def apply_to(self, *inputs: da.Array, **kwargs) -> da.Array:  # noqa: D102
        result = da.map_blocks(
            self.compute_block_typed,
            *inputs,
            name=f"{self.category}_{self.name}",
            dtype=self.dtype,
            chunks=self.chunks(*inputs),
            drop_axis=self.dropped_axes,
            new_axis=self.created_axes,
            meta=self.meta,
            **kwargs,
        )
        if self.created_axes is not None:
            chunks: list = [None for _ in range(result.ndim)]
            for i in self.created_axes:
                chunks[i] = 1
            result = result.rechunk(chunks)
        return result
