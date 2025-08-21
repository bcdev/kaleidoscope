#  Copyright (c) Brockmann Consult GmbH, 2025
#  License: MIT

"""
This module provides the source dataset reader.
"""

from pathlib import Path
from typing import Any

import xarray as xr
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

_KEY_MASK_AND_SCALE: str = "config.reader.mask_and_scale"
"""
The key to configure whether to mask and scale variables. The default
is `false`.
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
            _KEY_MASK_AND_SCALE: "false",
            _KEY_USE_CFTIME: "false",
            _KEY_CONCAT_CHARACTERS: "true",
            _KEY_INLINE_ARRAY: "false",
        }
        if config is not None:
            self._config.update(config)

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
        if isinstance(data_id, str) and "*" in data_id:
            pr = Preprocessor()
            ds = xr.open_mfdataset(
                data_id,
                chunks=self._chunks,
                engine=self._auto_engine(data_id),
                mask_and_scale=self._mask_and_scale,
                decode_cf=self._decode_cf,
                decode_coords=self._decode_coords,
                decode_times=self._decode_times,
                decode_timedelta=self._decode_timedelta,
                use_cftime=self._use_cftime,
                concat_characters=self._concat_characters,
                inline_array=self._inline_array,
                backend_kwargs=kwargs,
                combine="nested",
                concat_dim="i",
                preprocess=pr,
            )
            ds = pr.drop(ds)
        else:
            ds = xr.open_dataset(
                data_id,
                chunks=self._chunks,
                engine=self._auto_engine(data_id),
                mask_and_scale=self._mask_and_scale,
                decode_cf=self._decode_cf,
                decode_coords=self._decode_coords,
                decode_times=self._decode_times,
                decode_timedelta=self._decode_timedelta,
                use_cftime=self._use_cftime,
                concat_characters=self._concat_characters,
                inline_array=self._inline_array,
                backend_kwargs=kwargs,
            )
        return ds

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
    def _mask_and_scale(self) -> bool:
        """This method does not belong to public API."""
        return self._config[_KEY_MASK_AND_SCALE] == "true"

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


class Preprocessor:
    """
    A preprocessor to find the names of global attributes and data
    variables, which are not common to all datasets.
    """

    _all_attrs: list[str]
    """The list of all global attributes."""
    _all_vars: list[str]
    """The list of all data variables."""
    _drop_attrs: list[str]
    """The list of non-common global attributes to drop."""
    _drop_vars: list[str]
    """The list of non-common data variables to drop."""

    def __init__(self):
        """Creates a preprocessor instance."""
        self._all_vars = []
        self._all_attrs = []
        self._drop_vars = []
        self._drop_attrs = []

    def __call__(self, ds: Dataset) -> Dataset:
        """
        Returns the dataset supplied as argument unmodified.

        When consecutively called for multiple datasets, finds the names
        of global attributes and data variables, which are not common to
        all datasets.
        """
        self._process_attrs(ds)
        self._process_vars(ds)
        return ds

    def _process_attrs(self, ds):
        """This method does not belong to public API."""
        if self._all_attrs:
            for a in self._all_attrs:
                if a not in ds.attrs and a not in self._drop_attrs:
                    self._drop_attrs.append(a)
            for a, _ in ds.attrs.items():
                if a not in self._all_attrs:
                    self._all_attrs.append(a)
                    if a not in self._drop_attrs:
                        self._drop_attrs.append(a)
        else:
            for a, _ in ds.attrs.items():
                self._all_attrs.append(a)

    def _process_vars(self, ds):
        """This method does not belong to public API."""
        if self._all_vars:
            for v in self._all_vars:
                if v not in ds.data_vars and v not in self._drop_vars:
                    self._drop_vars.append(v)
            for v, _ in ds.data_vars.items():
                if v not in self._all_vars:
                    self._all_vars.append(v)
                    if v not in self._drop_vars:
                        self._drop_vars.append(v)
        else:
            for v, _ in ds.data_vars.items():
                self._all_vars.append(v)

    def drop(self, ds: Dataset) -> Dataset:
        """
        Returns a dataset with all non-common attributes
        and data variables dropped.
        """
        return self.drop_attrs(self.drop_vars(ds))

    def drop_attrs(self, ds: Dataset) -> Dataset:
        """
        Returns a dataset with all non-common attributes
        dropped.
        """
        for attr in self._drop_attrs:
            ds.attrs.pop(attr, None)
        return ds

    def drop_vars(self, ds: Dataset) -> Dataset:
        """
        Returns a dataset with all non-common data variables
        dropped.
        """
        return ds.drop_vars(self._drop_vars) if self._drop_vars else ds
