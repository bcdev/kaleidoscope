#  Copyright (c) Brockmann Consult GmbH, 2025
#  License: MIT

"""
This module provides the source dataset reader.
"""

from pathlib import Path
from typing import Any

import numpy as np
import xarray as xr
from typing_extensions import override
from xarray import Dataset

from .interface.reading import Reading

_KEY_CHUNKS: str = "config.reader.chunks"
"""
The key to configure chunking. The default is `{}`.
"""

_KEY_ENGINE: str = "config.reader.engine"
"""
The key to configure the reader engine. Possible engines are `h5netcdf`,
`netcdf4`, and `zarr`. The default is `h5netcdf`.
"""

_KEY_DECODE_CF: str = "config.reader.decode_cf"
"""
The key to configure whether to decode variables, assuming they were
saved according to CF conventions. The default is `true`.
"""

_KEY_DECODE_COORDS: str = "config.reader.decode_coords"
"""
The key to configure which variables are set as coordinate variables.
If `true` variables referred to in the coordinates attribute of the
datasets or individual variables are considered coordinate variables.
The default is `true`."""

_KEY_DECODE_TIMES: str = "config.reader.decode_times"
"""
The key to configure whether to decode times encoded in the standard
netCDF datetime format into datetime objects. The default is `false`.
"""

_KEY_DECODE_TIMEDELTA: str = "config.reader.decode_timedelta"
"""
The key to configure whether to decode variables and coordinates with
time units in {"days", "hours", "minutes", "seconds", "milliseconds",
"microseconds"} into timedelta objects. The default is `false`.
"""

_KEY_USE_CFTIME: str = "config.reader.use_cftime"
"""
The key to configure whether to decode times into `cftime.datetime`
objects or into `np.datetime64[ns]` objects. The default is `false`.
"""

_KEY_CONCAT_CHARACTERS: str = "config.reader.concat_characters"
"""
The key to configure concatenation along the last dimension of
character arrays to form string arrays. The default is `true`.
"""

_KEY_INLINE_ARRAY: str = "config.reader.inline_array"
"""
The key to configure whether to inline arrays directly in the dask
task graph. The default is `false`.
"""


class Reader(Reading):
    """The source dataset reader."""

    _config: dict[Any, Any]
    """The reader configuration."""

    def __init__(self, config: dict[str:Any] | None = None):
        """
        Creates a new reader instance.

        :param config: The reader configuration.
        """
        self._config = {
            _KEY_CHUNKS: {},
            _KEY_ENGINE: "h5netcdf",
            _KEY_DECODE_CF: "true",
            _KEY_DECODE_COORDS: "true",
            _KEY_DECODE_TIMES: "false",
            _KEY_DECODE_TIMEDELTA: "false",
            _KEY_USE_CFTIME: "false",
            _KEY_CONCAT_CHARACTERS: "true",
            _KEY_INLINE_ARRAY: "false",
        }
        if config is not None:
            self._config.update(config)

    @override
    def read(
        self,
        data_id: str | Path,
        **kwargs,
    ) -> Dataset:
        """
        Reads a dataset.
        """
        return self._open(data_id)

    def _open(self, data_id: str | Path) -> Dataset:
        """This method does not belong to public API."""
        kwargs = {}
        return xr.open_dataset(
            data_id,
            chunks=self._chunks,
            engine=self._auto_engine(data_id),
            mask_and_scale=True,
            decode_cf=self._decode_cf,
            decode_coords=self._decode_coords,
            decode_times=self._decode_times,
            decode_timedelta=self._decode_timedelta,
            use_cftime=self._use_cftime,
            concat_characters=self._concat_characters,
            inline_array=self._inline_array,
            backend_kwargs=kwargs,
        ).astype(np.single, copy=False)

    def _auto_engine(self, data_id: str | Path) -> str:
        """This method does not belong to public API."""
        if f"{data_id}".endswith(".zarr"):
            return "zarr"
        if f"{data_id}".endswith(".nc"):
            return self._engine if self._engine != "zarr" else "h5netcdf"
        return self._engine

    @property
    def _chunks(self) -> dict[str:int]:
        """This method does not belong to public API."""
        return self._config[_KEY_CHUNKS]

    @property
    def _engine(self) -> str:
        """This method does not belong to public API."""
        return self._config[_KEY_ENGINE]

    @property
    def _decode_cf(self) -> bool:
        """This method does not belong to public API."""
        return self._config[_KEY_DECODE_CF] == "true"

    @property
    def _decode_coords(self) -> bool:
        """This method does not belong to public API."""
        return self._config[_KEY_DECODE_COORDS] == "true"

    @property
    def _decode_times(self) -> bool:
        """This method does not belong to public API."""
        return self._config[_KEY_DECODE_TIMES] == "true"

    @property
    def _decode_timedelta(self) -> bool:
        """This method does not belong to public API."""
        return self._config[_KEY_DECODE_TIMEDELTA] == "true"

    @property
    def _use_cftime(self) -> bool:
        """This method does not belong to public API."""
        return self._config[_KEY_USE_CFTIME] == "true"

    @property
    def _concat_characters(self) -> bool:
        """This method does not belong to public API."""
        return self._config[_KEY_CONCAT_CHARACTERS] == "true"

    @property
    def _inline_array(self) -> bool:
        """This method does not belong to public API."""
        return self._config[_KEY_INLINE_ARRAY] == "true"
