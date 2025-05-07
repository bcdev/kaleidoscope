#  Copyright (c) Brockmann Consult GmbH, 2025
#  License: MIT

"""
This module provides the target dataset writer.
"""

from pathlib import Path
from typing import Any
from typing import Literal

import numpy as np
from typing_extensions import override
from xarray import Dataset

from .interface.writing import Writing
from .progress import Progress

_KEY_CHUNKS: str = "config.writer.chunks"
"""
The key to configure chunking of data. The default is an empty
dictionary.
"""

_KEY_ENGINE: str = "config.writer.engine"
"""
The key to configure the writer engine. Possible engines are
`h5netcdf`, `netcdf4`, and `zarr`. The default is `h5netcdf`.
"""

_KEY_ZLIB: str = "config.writer.zlib"
"""
The key to configure whether the data will be compressed using zlib.
The default is `true`.
"""

_KEY_COMPLEVEL: str = "config.writer.complevel"
"""
The key to configure the compression level. The default is `1`.
"""

_KEY_SHUFFLE: str = "config.writer.shuffle"
"""
The key to configure shuffling. The default is `true`.
"""


class Writer(Writing):
    """! The target dataset writer."""

    _config: dict[str, Any]
    """! The writer configuration."""

    _progress: bool
    """Displays a progress bar on the console, if set."""

    def __init__(
        self,
        config: dict[str, Any],
        chunks: dict[str:int] = None,
        engine: Literal["h5netcdf", "netcdf4", "zarr"] | None = None,
        progress: bool = False,
    ):
        """
        Creates a new writer instance.

        :param config: The writer configuration.
        :param chunks: An explicit configuration of chunks. Overrides the
        writer configuration.
        :param engine: An explicit specification of the writer engine.
        Overrides the writer configuration.
        :param progress: Displays a progress bar on the console, if set.
        """
        self._config = {
            _KEY_CHUNKS: {},
            _KEY_ENGINE: "h5netcdf",
            _KEY_ZLIB: "true",
            _KEY_COMPLEVEL: 1,
            _KEY_SHUFFLE: "true",
        }
        self._config.update(config)
        if chunks is not None:
            self._config[_KEY_CHUNKS].update(chunks)
        if engine is not None:
            self._config[_KEY_ENGINE] = engine
        self._progress = progress

    @override
    def write(
        self, dataset: Dataset, data_id: str | Path, **kwargs
    ):  # noqa: D102
        to_zarr = self._auto_engine(data_id) == "zarr"
        encoding = self._encode(dataset, to_zarr)

        with dataset as ds:
            if to_zarr:
                with Progress(self._progress):
                    ds.to_zarr(data_id, encoding=encoding)
            else:
                with Progress(self._progress):
                    # noinspection PyTypeChecker
                    ds.to_netcdf(
                        data_id,
                        encoding=encoding,
                        format=self._nc_format,
                        engine=self._auto_engine(data_id),
                    )

    def _encode(self, dataset: Dataset, to_zarr: bool = True):
        """This method does not belong to public API."""
        encodings: dict[str, dict[str, Any]] = {}

        for name, array in dataset.data_vars.items():
            data = array.data
            dims: list = list(array.dims)
            if array.ndim == 0:  # not an array
                continue
            if name in dims:  # a coordinate dimension
                continue
            chunks: list[int] = []
            for i, dim in enumerate(dims):
                if dim in self._chunks:
                    chunk_size = self._chunks[dim]
                    assert isinstance(chunk_size, int), (
                        f"Invalid chunk size specified for "
                        f"dimension '{dim}'"
                    )
                    if chunk_size == -1:
                        chunk_size = data.shape[i]
                    if chunk_size == 0:
                        chunk_size = data.chunksize[i]
                    chunks.append(chunk_size)
                else:
                    chunks.append(data.chunksize[i])
            encodings[name] = self._encode_compress(
                data.dtype, chunks, to_zarr
            )
        return encodings

    def _auto_engine(self, path: str | Path) -> str:
        """This method does not belong to public API."""
        if f"{path}".endswith(".zarr"):
            return "zarr"
        if f"{path}".endswith(".nc"):
            return self._engine if self._engine != "zarr" else "h5netcdf"
        return self._engine

    @property
    def _chunks(self) -> dict[str, int]:
        """This method does not belong to public API."""
        return self._config[_KEY_CHUNKS]

    @property
    def _engine(self) -> Literal["h5netcdf", "netcdf4", "zarr"]:
        """This method does not belong to public API."""
        return self._config[_KEY_ENGINE]

    @property
    def _nc_format(self) -> Literal["NETCDF4"]:
        """This method does not belong to public API."""
        return "NETCDF4"

    @property
    def _zlib(self) -> bool:
        """This method does not belong to public API."""
        return self._config[_KEY_ZLIB] == "true"

    @property
    def _complevel(self) -> int:
        """This method does not belong to public API."""
        return self._config[_KEY_COMPLEVEL]

    @property
    def _shuffle(self) -> bool:
        """This method does not belong to public API."""
        return self._config[_KEY_SHUFFLE] == "true"

    def _encode_compress(
        self,
        dtype: np.dtype,
        chunks: list[int],
        to_zarr: bool = True,
    ) -> dict[str, Any]:
        """This method does not belong to public API."""
        enc = {"dtype": dtype}
        if chunks:
            if to_zarr:
                enc["chunks"] = tuple(chunks)
            else:
                enc["chunksizes"] = tuple(chunks)
        if to_zarr:
            pass
        else:
            enc["zlib"] = self._zlib
            enc["complevel"] = self._complevel
            enc["shuffle"] = self._shuffle
        return enc
