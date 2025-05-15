#  Copyright (c) Brockmann Consult GmbH, 2025
#  License: MIT

"""
This module provides unit-level tests for the target product writer.
"""

import json
import unittest
import warnings
from importlib import resources
from pathlib import Path
from typing import Any

import numpy as np
from dask.array import Array
from xarray import Dataset

from kaleidoscope.reader import Reader
from kaleidoscope.util.ncbin import ncdump
from kaleidoscope.util.ncbin import ncgen
from kaleidoscope.writer import Writer

warnings.filterwarnings("ignore")


class WriterTest(unittest.TestCase):
    """Tests the target product writer."""

    reader_config: dict[str:Any]
    """The reader configuration."""

    writer_config: dict[str:Any]
    """The writer configuration."""

    def setUp(self):
        """Initializes the test."""
        package = "kaleidoscope.config"
        name = "config.reader.json"
        with resources.path(package, name) as resource:
            with open(resource) as r:
                self.reader_config = json.load(r)
        name = "config.writer.json"
        with resources.path(package, name) as resource:
            with open(resource) as r:
                self.writer_config = json.load(r)
        self.generate_source_files()

    def tearDown(self):
        """Cleans up the test."""
        for source_file in self.source_files:
            source_file.unlink()

    def generate_source_files(self):
        """Generate source datasets from template CDL files."""
        self.source_files = []
        with resources.path("kaleidoscope", "templates") as resource:
            for f in resource.iterdir():
                if f.name.endswith(".cdl"):
                    self.source_files.append(ncgen(f))

    def test_write_esa_cci_oc(self):
        """Tests writing a generated target dataset."""
        self.assert_write("esa-cci-oc")

    def test_write_esa_scope_cs(self):
        """Tests writing a generated target dataset."""
        self.assert_write("esa-scope-cs")

    def test_write_ghrsst(self):
        """Tests writing a generated target dataset."""
        self.assert_write("ghrsst")

    def test_write_glorys(self):
        """Tests writing a generated target dataset."""
        self.assert_write("glorys")

    def assert_write(self, source_type):
        """This method does not belong to public API."""
        for source_file in self.source_files:
            if source_file.stem.replace("_", "-") != source_type:
                continue

            source = None
            target = None
            target_dump = None
            target_file = None

            try:
                target_file = Path(
                    f"{source_file}".replace(".nc", ".randomized.nc")
                )
                reader = self.create_reader(source_type)
                source = reader.read(source_file)

                # can be written?
                writer = self.create_writer(source_type)
                writer.write(source, target_file)
                self.assertTrue(target_file.exists())

                # can be read?
                target = reader.read(target_file)
                self.assert_shapes(source, target)

                # can be dumped?
                target_dump = ncdump(target_file)
                self.assertTrue(target_dump.exists())
            finally:
                if target is not None:
                    target.close()
                if source is not None:
                    source.close()
                if target_dump is not None:
                    target_dump.unlink()
                if target_file is not None:
                    target_file.unlink()

    def create_reader(self, source_type):
        """This method does not belong to public API."""
        chunks = self.reader_config["config.reader.chunks"]
        for k, v in chunks.get(source_type, chunks["_"]).items():
            chunks[k] = v
        return Reader(self.reader_config)

    def create_writer(self, source_type):
        """This method does not belong to public API."""
        chunks = self.writer_config["config.writer.chunks"]
        for k, v in chunks.get(source_type, chunks["_"]).items():
            chunks[k] = v
        return Writer(self.writer_config, engine="h5netcdf")

    def assert_almost_no_difference(
        self, source: Dataset, target: Dataset, delta=0.0, q=1.0
    ):
        """This method does not belong to public API."""
        for name, _ in source.variables.items():
            a: np.ndarray = source[name].values
            b: np.ndarray = target[name].values
            d = np.nanquantile(np.abs(a - b), q)
            self.assertAlmostEqual(0.0, d, delta=delta, msg=f"{name}: {d}")

    def assert_shapes(self, source: Dataset, target: Dataset):
        """This method does not belong to public API."""
        for name, _ in source.variables.items():
            a: Array = source[name].data
            b: Array = target[name].data
            self.assertEqual(a.shape, b.shape, msg=f"{name}: {a}, {b}")


if __name__ == "__main__":
    unittest.main()
